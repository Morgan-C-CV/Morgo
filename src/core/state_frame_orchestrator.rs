use crate::bootstrap::model_profiles::ModelProfileRegistry;
use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::evidence_scope::evidence_refs_have_anchor_scope;
use crate::core::state_frame::{
    ActorRole, CompletionEvidenceStatus, StageExecutionContract, StateFrame,
};
use crate::core::state_frame_loop::{
    DecisionLoopConfig, LoopOutcome, LoopUsage, StateFrameToolRuntime, run_decision_loop,
    run_decision_loop_with_tools,
};
use crate::core::state_frame_model_resolver::resolve_step_model;
use crate::core::state_frame_model_router::{ModelRoute, route_model_tier};
use crate::core::state_frame_projection::{
    collect_projection_diagnostics, project_state_frame, project_state_frame_with_st_mode,
};
use crate::core::state_frame_router::{apply_route, route_toolset};
use crate::service::api::client::{ModelPricing, ModelProviderClient};
use crate::service::observability::ServiceObservabilityTracker;
use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;
use crate::tool::registry::{
    ToolAssemblyContext, ToolContractMismatch, ToolContractPreflightSpec, ToolRegistrySnapshot,
};
use crate::{bootstrap::InteractionSurface, bootstrap::SessionMode};
use serde::{Deserialize, Serialize};

/// Outcome of a single step execution via the StateFrame orchestrator seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepFailureClassification {
    GenericFailure,
    UnsupportedRequest,
    RepairableRecovery,
    VerificationRepairContinuation,
    TrueExternalBlocker,
}

impl StepFailureClassification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GenericFailure => "generic_failure",
            Self::UnsupportedRequest => "unsupported_request",
            Self::RepairableRecovery => "repairable_recovery",
            Self::VerificationRepairContinuation => "verification_repair_continuation",
            Self::TrueExternalBlocker => "true_external_blocker",
        }
    }
}

#[derive(Debug, Clone)]
pub enum StepOutcome {
    Completed {
        usage: LoopUsage,
        tool_registry_snapshot: Option<ToolRegistrySnapshot>,
    },
    Failed {
        reason: String,
        failure_classification: StepFailureClassification,
        usage: Option<LoopUsage>,
        tool_registry_snapshot: Option<ToolRegistrySnapshot>,
        tool_contract_mismatch: Option<ToolContractMismatch>,
    },
}

#[derive(Debug, Clone)]
pub struct RoutedStateFrame {
    pub frame: StateFrame,
    pub model_route: ModelRoute,
    pub projection_mismatch_count: usize,
}

fn tool_assembly_context_for_role(role: ActorRole) -> ToolAssemblyContext {
    match role {
        ActorRole::ExecutorB => {
            ToolAssemblyContext::executor_b(InteractionSurface::Cli, SessionMode::Headless)
        }
        ActorRole::Worker | ActorRole::Verifier | ActorRole::Summarizer => {
            ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless)
        }
        ActorRole::DesignerA => {
            ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Headless)
        }
    }
}

fn has_any_marker(items: &[String], markers: &[&str]) -> bool {
    items.iter().any(|item| {
        let lowered = item.to_ascii_lowercase();
        markers.iter().any(|marker| lowered.contains(marker))
    })
}

fn infer_preflight_requirements_from_state_frame(frame: &StateFrame) -> ToolContractPreflightSpec {
    let mut spec = ToolContractPreflightSpec {
        required_visible_tools: Vec::new(),
        required_allowed_actions: Vec::new(),
        permission_probe_tools: Vec::new(),
        permission_probe_paths: std::collections::BTreeMap::new(),
    };
    if matches!(
        frame.state,
        crate::core::state_frame::AgentState::Blocked | crate::core::state_frame::AgentState::Done
    ) {
        return spec;
    }
    if frame.role == ActorRole::ExecutorB {
        spec.required_visible_tools.push("Agent".into());
        spec.required_allowed_actions.push("spawn_agent".into());
        spec.permission_probe_tools.push("Agent".into());
    }

    let readonly_contract = matches!(
        frame.required_output_schema.as_deref(),
        Some("readonly_audit_4_paragraphs_v1")
    );
    let artifact_requires_write =
        |artifact: &crate::core::state_frame::DeclaredArtifactContract| {
            artifact.required_actions.iter().any(|action| {
                matches!(
                    action.as_str(),
                    "write_file" | "edit_file" | "create" | "write"
                )
            })
        };
    let requires_write = !readonly_contract
        && frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .any(artifact_requires_write);
    let requires_command_execution = !readonly_contract
        && (frame.stage_execution_contract.tests.iter().any(|test| {
            test.required_actions
                .iter()
                .any(|action| action == "run_command")
        }) || frame
            .stage_execution_contract
            .verifications
            .iter()
            .any(|verification| {
                verification
                    .required_actions
                    .iter()
                    .any(|action| action == "run_command")
            }));
    let writable_probe_path = frame
        .stage_execution_contract
        .declared_artifacts
        .iter()
        .find(|artifact| {
            artifact_requires_write(artifact)
                && std::path::Path::new(artifact.path.as_str())
                    .extension()
                    .is_some()
        })
        .map(|artifact| artifact.path.clone())
        .or_else(|| {
            frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .find(|artifact| artifact_requires_write(artifact) && artifact.kind == "directory")
                .map(|artifact| format!("{}/README.md", artifact.path.trim_end_matches('/')))
        });

    if requires_write {
        spec.required_allowed_actions.push("write_file".into());
        spec.permission_probe_tools.push("Edit".into());
        spec.permission_probe_tools.push("Bash".into());
        spec.required_visible_tools.push("Bash".into());
        if let Some(path) = writable_probe_path {
            spec.permission_probe_paths.insert("Edit".into(), path);
        }
    }
    if requires_command_execution {
        spec.required_allowed_actions.push("run_command".into());
        spec.permission_probe_tools.push("Bash".into());
        spec.required_visible_tools.push("Bash".into());
    }
    spec.required_visible_tools.sort();
    spec.required_visible_tools.dedup();
    spec.required_allowed_actions.sort();
    spec.required_allowed_actions.dedup();
    spec.permission_probe_tools.sort();
    spec.permission_probe_tools.dedup();
    spec
}

fn direct_worker_preflight_spec(frame: &StateFrame) -> ToolContractPreflightSpec {
    infer_preflight_requirements_from_state_frame(frame)
}

async fn apply_tool_registry_contract(
    frame: &mut StateFrame,
    runtime: &StateFrameToolRuntime,
) -> anyhow::Result<ToolRegistrySnapshot> {
    let mut assembly_context = tool_assembly_context_for_role(frame.role);
    assembly_context.include_deferred_tools = runtime.permissions.include_deferred_tools;
    assembly_context.include_interactive_tools = runtime.permissions.include_interactive_tools;
    assembly_context.boss_actor_policy = runtime.permissions.boss_actor_policy;
    let actor_registry = runtime.registry.assemble(assembly_context);
    let snapshot = actor_registry
        .snapshot(
            &runtime.permissions,
            frame
                .toolset_id
                .clone()
                .unwrap_or_else(|| "unrouted".into()),
            format!("{:?}", frame.role).to_ascii_lowercase(),
            runtime.cwd.clone(),
            runtime.config_root.clone(),
        )
        .await;
    frame.allowed_tools = snapshot.visible_tools.clone();
    frame.allowed_actions = snapshot.allowed_actions.clone();
    Ok(snapshot)
}

/// The current StateFrame loop can decide, summarize, and request context, but it does not
/// execute read/write/shell tool calls. Until tool dispatch is wired, direct LisM execution must
/// reject tasks whose success depends on filesystem or command side effects.
pub fn requires_external_tool_execution(
    frame: &StateFrame,
    direct_tool_runtime_available: bool,
) -> bool {
    let preflight = infer_preflight_requirements_from_state_frame(frame);
    frame.role == ActorRole::Worker
        && !direct_tool_runtime_available
        && !matches!(
            frame.required_output_schema.as_deref(),
            Some("readonly_audit_4_paragraphs_v1")
        )
        && (!preflight.required_visible_tools.is_empty()
            || !preflight.required_allowed_actions.is_empty())
}

fn external_tool_execution_unsupported() -> StepOutcome {
    StepOutcome::Failed {
            reason: "StateFrame direct execution cannot yet perform required filesystem or command side effects; use full worker path or wire tool dispatch before enabling LisM for this step".into(),
            failure_classification: StepFailureClassification::UnsupportedRequest,
            usage: None,
            tool_registry_snapshot: None,
            tool_contract_mismatch: None,
        }
}

#[test]
fn verification_repair_continuation_with_missing_evidence_is_not_generic_failure() {
    let usage = LoopUsage {
        recovery_tier: Some("verification_repair_continuation".into()),
        completion_evidence_status: Some(
            crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence,
        ),
        ..LoopUsage::default()
    };

    assert_eq!(
        classify_usage_failure(Some(&usage)),
        StepFailureClassification::VerificationRepairContinuation
    );
}

fn classify_usage_failure(usage: Option<&LoopUsage>) -> StepFailureClassification {
    let Some(usage) = usage else {
        return StepFailureClassification::GenericFailure;
    };
    if usage.recovery_tier.as_deref() == Some("verification_repair_continuation")
        && matches!(
            usage.completion_evidence_status,
            Some(crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence)
        )
    {
        return StepFailureClassification::VerificationRepairContinuation;
    }
    if usage.terminal_blocker_kind.as_deref() == Some("true_external_blocker") {
        return StepFailureClassification::TrueExternalBlocker;
    }
    if usage.terminal_blocker_kind.as_deref() == Some("unsupported_selector")
        || usage.recovery_outcome.as_deref() == Some("unsupported_selector")
    {
        return StepFailureClassification::UnsupportedRequest;
    }
    if usage.recovery_outcome.as_deref() == Some("repair_turn_injected") {
        if matches!(
            usage.completion_evidence_status,
            Some(crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence)
        ) {
            return StepFailureClassification::VerificationRepairContinuation;
        }
        return StepFailureClassification::RepairableRecovery;
    }
    StepFailureClassification::GenericFailure
}

pub fn build_routed_state_frame(
    plan: &BossPlan,
    stage: BossStage,
    step_id: usize,
    role: ActorRole,
) -> StateFrame {
    build_routed_state_frame_with_model_route(plan, stage, step_id, role).frame
}

pub fn build_routed_state_frame_with_model_route(
    plan: &BossPlan,
    stage: BossStage,
    step_id: usize,
    role: ActorRole,
) -> RoutedStateFrame {
    build_routed_state_frame_with_model_route_and_st_mode(plan, stage, step_id, role, false)
}

pub fn build_routed_state_frame_with_model_route_and_st_mode(
    plan: &BossPlan,
    stage: BossStage,
    step_id: usize,
    role: ActorRole,
    st_mode_enabled: bool,
) -> RoutedStateFrame {
    let mut frame = if st_mode_enabled {
        project_state_frame_with_st_mode(plan, stage, Some(step_id), role, true)
    } else {
        project_state_frame(plan, stage, Some(step_id), role)
    };
    let projection_mismatch_count = collect_projection_diagnostics(&frame).mismatch_count;
    let route = route_toolset(&frame);
    apply_route(&mut frame, route);
    let model_route = route_model_tier(frame.budget.effort, frame.role, frame.state);
    RoutedStateFrame {
        frame,
        model_route,
        projection_mismatch_count,
    }
}

/// Run a single plan step through the StateFrame decision loop.
///
/// Pure orchestrator seam — no AppState, no session actors, no BossCoordinator mutation.
/// Callers are responsible for persisting the outcome back to the plan.
#[derive(Debug, Clone)]
pub struct StepRuntimeResolutionContext<'a> {
    pub inherited_snapshot: &'a ActiveModelRuntimeSnapshot,
    pub model_registry: Option<&'a ModelProfileRegistry>,
    pub observability: ServiceObservabilityTracker,
    pub tool_runtime: Option<StateFrameToolRuntime>,
}

#[derive(Debug, Clone)]
pub struct ResolvedRoutedStep {
    pub routed: RoutedStateFrame,
    pub resolved_snapshot: ActiveModelRuntimeSnapshot,
    pub tool_runtime: Option<StateFrameToolRuntime>,
    pub tool_registry_snapshot: Option<ToolRegistrySnapshot>,
}

pub async fn run_step_with_state_frame(
    client: &ModelProviderClient,
    plan: &BossPlan,
    stage: BossStage,
    step_id: usize,
    role: ActorRole,
    config: DecisionLoopConfig,
) -> anyhow::Result<StepOutcome> {
    let routed = build_routed_state_frame_with_model_route(plan, stage, step_id, role);
    if requires_external_tool_execution(&routed.frame, false) {
        return Ok(external_tool_execution_unsupported());
    }
    let stage_execution_contract = routed.frame.stage_execution_contract.clone();
    let outcome = run_decision_loop(client, routed.frame, config).await?;
    Ok(map_loop_outcome(outcome, &stage_execution_contract, None))
}

pub async fn run_step_with_state_frame_and_runtime<'a>(
    plan: &BossPlan,
    stage: BossStage,
    step_id: usize,
    role: ActorRole,
    config: DecisionLoopConfig,
    runtime: StepRuntimeResolutionContext<'a>,
) -> anyhow::Result<StepOutcome> {
    let routed = build_routed_state_frame_with_model_route(plan, stage, step_id, role);
    if requires_external_tool_execution(&routed.frame, runtime.tool_runtime.is_some()) {
        return Ok(external_tool_execution_unsupported());
    }
    let resolved = resolve_routed_step_runtime(routed, runtime).await?;
    if let (Some(tool_runtime), Some(snapshot)) = (
        resolved.tool_runtime.as_ref(),
        resolved.tool_registry_snapshot.as_ref(),
    ) {
        let actor_registry = tool_runtime
            .registry
            .assemble(tool_assembly_context_for_role(resolved.routed.frame.role));
        if let Err(mismatch) = actor_registry
            .preflight_contract(
                &tool_runtime.permissions,
                snapshot,
                &direct_worker_preflight_spec(&resolved.routed.frame),
            )
            .await
        {
            return Ok(StepOutcome::Failed {
                reason: format!(
                    "ToolContractMismatch: {}",
                    serde_json::to_string(&mismatch)?
                ),
                failure_classification: StepFailureClassification::UnsupportedRequest,
                usage: None,
                tool_registry_snapshot: Some(snapshot.clone()),
                tool_contract_mismatch: Some(mismatch),
            });
        }
    }
    let stage_execution_contract = resolved.routed.frame.stage_execution_contract.clone();
    let outcome = run_decision_loop_with_tools(
        &resolved.resolved_snapshot.client,
        resolved.routed.frame,
        config,
        resolved.tool_runtime,
    )
    .await?;
    Ok(map_loop_outcome_with_pricing(
        outcome,
        &stage_execution_contract,
        &resolved.resolved_snapshot.config.pricing,
        resolved.tool_registry_snapshot,
    ))
}

pub async fn resolve_routed_step_runtime<'a>(
    mut routed: RoutedStateFrame,
    runtime: StepRuntimeResolutionContext<'a>,
) -> anyhow::Result<ResolvedRoutedStep> {
    let tool_registry_snapshot = match runtime.tool_runtime.as_ref() {
        Some(tool_runtime) => {
            Some(apply_tool_registry_contract(&mut routed.frame, tool_runtime).await?)
        }
        None => None,
    };
    let resolved = resolve_step_model(
        &routed.model_route,
        runtime.inherited_snapshot,
        runtime.model_registry,
        runtime.observability,
    )?;
    Ok(ResolvedRoutedStep {
        routed,
        resolved_snapshot: resolved.snapshot,
        tool_runtime: runtime.tool_runtime,
        tool_registry_snapshot,
    })
}

pub async fn run_routed_step_with_runtime<'a>(
    routed: RoutedStateFrame,
    config: DecisionLoopConfig,
    runtime: StepRuntimeResolutionContext<'a>,
) -> anyhow::Result<StepOutcome> {
    if requires_external_tool_execution(&routed.frame, runtime.tool_runtime.is_some()) {
        return Ok(external_tool_execution_unsupported());
    }
    let resolved = resolve_routed_step_runtime(routed, runtime).await?;
    if let (Some(tool_runtime), Some(snapshot)) = (
        resolved.tool_runtime.as_ref(),
        resolved.tool_registry_snapshot.as_ref(),
    ) {
        let actor_registry = tool_runtime
            .registry
            .assemble(tool_assembly_context_for_role(resolved.routed.frame.role));
        if let Err(mismatch) = actor_registry
            .preflight_contract(
                &tool_runtime.permissions,
                snapshot,
                &direct_worker_preflight_spec(&resolved.routed.frame),
            )
            .await
        {
            return Ok(StepOutcome::Failed {
                reason: format!(
                    "ToolContractMismatch: {}",
                    serde_json::to_string(&mismatch)?
                ),
                failure_classification: StepFailureClassification::UnsupportedRequest,
                usage: None,
                tool_registry_snapshot: Some(snapshot.clone()),
                tool_contract_mismatch: Some(mismatch),
            });
        }
    }
    let stage_execution_contract = resolved.routed.frame.stage_execution_contract.clone();
    let outcome = run_decision_loop_with_tools(
        &resolved.resolved_snapshot.client,
        resolved.routed.frame,
        config,
        resolved.tool_runtime,
    )
    .await?;
    Ok(map_loop_outcome_with_pricing(
        outcome,
        &stage_execution_contract,
        &resolved.resolved_snapshot.config.pricing,
        resolved.tool_registry_snapshot,
    ))
}

fn map_loop_outcome(
    outcome: LoopOutcome,
    stage_execution_contract: &StageExecutionContract,
    tool_registry_snapshot: Option<ToolRegistrySnapshot>,
) -> StepOutcome {
    match outcome {
        LoopOutcome::Done { usage, .. } => {
            if let Some((reason, failure_classification)) =
                completion_gate_failure(stage_execution_contract, &usage)
            {
                StepOutcome::Failed {
                    reason,
                    failure_classification,
                    usage: Some(usage),
                    tool_registry_snapshot,
                    tool_contract_mismatch: None,
                }
            } else {
                StepOutcome::Completed {
                    usage,
                    tool_registry_snapshot,
                }
            }
        }
        LoopOutcome::Rejected { reason, usage } => StepOutcome::Failed {
            reason,
            failure_classification: classify_usage_failure(Some(&usage)),
            usage: Some(usage),
            tool_registry_snapshot,
            tool_contract_mismatch: None,
        },
        LoopOutcome::MaxIterationsReached { last_state, usage } => StepOutcome::Failed {
            reason: format!("max iterations reached; last state: {last_state:?}"),
            failure_classification: classify_usage_failure(Some(&usage)),
            usage: Some(usage),
            tool_registry_snapshot,
            tool_contract_mismatch: None,
        },
        LoopOutcome::NoProgress {
            last_state,
            reason,
            usage,
        } => StepOutcome::Failed {
            reason: format!("{reason}; last state: {last_state:?}"),
            failure_classification: classify_usage_failure(Some(&usage)),
            usage: Some(usage),
            tool_registry_snapshot,
            tool_contract_mismatch: None,
        },
        LoopOutcome::ToolDispatchFailed {
            last_state,
            reason,
            usage,
        } => StepOutcome::Failed {
            reason: format!("tool dispatch failed: {reason}; last state: {last_state:?}"),
            failure_classification: classify_usage_failure(Some(&usage)),
            usage: Some(usage),
            tool_registry_snapshot,
            tool_contract_mismatch: None,
        },
        LoopOutcome::RepairExhausted {
            reason,
            raw_json,
            usage,
        } => StepOutcome::Failed {
            reason: format!("repair exhausted: {reason}; raw: {raw_json}"),
            failure_classification: classify_usage_failure(Some(&usage)),
            usage: Some(usage),
            tool_registry_snapshot,
            tool_contract_mismatch: None,
        },
    }
}

fn map_loop_outcome_with_pricing(
    outcome: LoopOutcome,
    stage_execution_contract: &StageExecutionContract,
    pricing: &ModelPricing,
    tool_registry_snapshot: Option<ToolRegistrySnapshot>,
) -> StepOutcome {
    match map_loop_outcome(outcome, stage_execution_contract, tool_registry_snapshot) {
        StepOutcome::Completed {
            mut usage,
            tool_registry_snapshot,
        } => {
            usage.estimated_cost_micros_usd = estimate_loop_usage_cost_micros(&usage, pricing);
            StepOutcome::Completed {
                usage,
                tool_registry_snapshot,
            }
        }
        StepOutcome::Failed {
            reason,
            failure_classification,
            usage: Some(mut usage),
            tool_registry_snapshot,
            tool_contract_mismatch,
        } => {
            usage.estimated_cost_micros_usd = estimate_loop_usage_cost_micros(&usage, pricing);
            StepOutcome::Failed {
                reason,
                failure_classification,
                usage: Some(usage),
                tool_registry_snapshot,
                tool_contract_mismatch,
            }
        }
        failed => failed,
    }
}

fn estimate_loop_usage_cost_micros(usage: &LoopUsage, pricing: &ModelPricing) -> u64 {
    let estimated_cost_usd = (usage.uncached_input_tokens as f64 / 1_000_000.0)
        * pricing.input_per_million_usd
        + (usage.output_tokens as f64 / 1_000_000.0) * pricing.output_per_million_usd
        + (usage.cache_write_tokens as f64 / 1_000_000.0) * pricing.cache_write_per_million_usd
        + (usage.cache_read_tokens as f64 / 1_000_000.0) * pricing.cache_read_per_million_usd;
    (estimated_cost_usd * 1_000_000.0).round() as u64
}

fn stage_execution_contract_requires_verification(
    stage_execution_contract: &StageExecutionContract,
) -> bool {
    !stage_execution_contract.verifications.is_empty()
        || stage_execution_contract
            .required_actions
            .iter()
            .any(|action| {
                matches!(
                    action.as_str(),
                    "verify" | "verify_artifact" | "run_verification"
                )
            })
}

fn completion_gate_failure(
    stage_execution_contract: &StageExecutionContract,
    usage: &LoopUsage,
) -> Option<(String, StepFailureClassification)> {
    if !stage_execution_contract_requires_verification(stage_execution_contract) {
        return None;
    }
    let completion_status = usage.completion_evidence_status.as_ref();
    let report = usage.worker_report.as_ref();
    let gaps = report
        .map(|report| report.completion_evidence_gaps.as_slice())
        .unwrap_or(&[]);
    let has_gaps = !gaps.is_empty();
    let missing_verification_gap = gaps.iter().any(|gap| gap.missing_verification_evidence);
    let worker_verification_verified = report
        .map(|report| report.verification_status.as_str() == "verified")
        .unwrap_or(false);
    let worker_completion_sufficient = report.is_some_and(|report| {
        report.completion_evidence_status == CompletionEvidenceStatus::Sufficient
    });
    let verification_first_read_anchor_closed = report.is_some_and(|report| {
        worker_report_has_target_scoped_read_anchor(stage_execution_contract, report)
    });
    let worker_verification_satisfied =
        worker_verification_verified || verification_first_read_anchor_closed;
    let evidence_bound = report.is_some_and(|report| {
        worker_report_has_target_scoped_evidence(stage_execution_contract, report)
            || verification_first_read_anchor_closed
    });
    let source_evidence_satisfied = report.is_some_and(|report| {
        worker_report_has_required_source_evidence(stage_execution_contract, report)
    });
    let completion_sufficient = matches!(
        completion_status,
        Some(CompletionEvidenceStatus::Sufficient)
    ) && worker_verification_satisfied
        && worker_completion_sufficient
        && evidence_bound
        && source_evidence_satisfied;

    if completion_sufficient && !has_gaps {
        return None;
    }

    let failure_classification = if missing_verification_gap
        || !worker_verification_satisfied
        || !worker_completion_sufficient
        || !evidence_bound
        || !source_evidence_satisfied
        || matches!(
            completion_status,
            Some(CompletionEvidenceStatus::MissingVerificationEvidence)
        ) {
        StepFailureClassification::VerificationRepairContinuation
    } else {
        StepFailureClassification::RepairableRecovery
    };
    Some((
        "completion gate rejected direct completion: verification contract remains unsatisfied"
            .into(),
        failure_classification,
    ))
}

fn artifact_like_paths(contract: &StageExecutionContract) -> Vec<&str> {
    let mut paths: Vec<&str> = contract
        .declared_artifacts
        .iter()
        .map(|artifact| artifact.path.as_str())
        .collect();
    for target_path in contract
        .verifications
        .iter()
        .filter_map(|verification| verification.target_path.as_deref())
    {
        if !paths.iter().any(|existing| *existing == target_path) {
            paths.push(target_path);
        }
    }
    paths
}

fn required_non_artifact_evidence_targets(contract: &StageExecutionContract) -> Vec<&str> {
    let artifact_paths = artifact_like_paths(contract);
    let mut targets = Vec::new();
    for target in contract
        .required_evidence
        .iter()
        .chain(
            contract
                .verifications
                .iter()
                .flat_map(|verification| verification.required_evidence.iter()),
        )
        .map(|value| value.as_str())
    {
        if artifact_paths.iter().any(|artifact| *artifact == target)
            || targets.iter().any(|existing| *existing == target)
        {
            continue;
        }
        targets.push(target);
    }
    targets
}

fn evidence_ref_mentions_target(evidence_ref: &str, target: &str) -> bool {
    evidence_ref.contains(target)
}

fn evidence_ref_is_artifact_presence_only(evidence_ref: &str, artifact_paths: &[&str]) -> bool {
    if evidence_ref.starts_with("artifact:") {
        return true;
    }
    artifact_paths.iter().any(|artifact| {
        evidence_ref.contains(artifact)
            && (evidence_ref.starts_with("read:")
                || evidence_ref.starts_with("write:")
                || evidence_ref.starts_with("artifact:"))
    })
}

fn worker_report_has_target_scoped_evidence(
    contract: &StageExecutionContract,
    report: &crate::core::state_frame::WorkerStructuredReport,
) -> bool {
    if report.evidence_refs.is_empty() {
        return false;
    }
    let artifact_paths = artifact_like_paths(contract);
    let required_targets = required_non_artifact_evidence_targets(contract);
    if !required_targets.is_empty() {
        return required_targets.iter().all(|target| {
            report
                .evidence_refs
                .iter()
                .any(|evidence_ref| evidence_ref_mentions_target(evidence_ref, target))
        });
    }
    report
        .evidence_refs
        .iter()
        .any(|evidence_ref| !evidence_ref_is_artifact_presence_only(evidence_ref, &artifact_paths))
}

fn worker_report_has_required_source_evidence(
    contract: &StageExecutionContract,
    report: &crate::core::state_frame::WorkerStructuredReport,
) -> bool {
    if contract.content_evidence_targets.is_empty() {
        return true;
    }
    if report.evidence_refs.is_empty() {
        return false;
    }
    contract
        .content_evidence_targets
        .iter()
        .all(|target| evidence_refs_have_anchor_scope(&report.evidence_refs, "read", target))
}

fn worker_report_has_target_scoped_read_anchor(
    contract: &StageExecutionContract,
    report: &crate::core::state_frame::WorkerStructuredReport,
) -> bool {
    let verification_targets = contract
        .verifications
        .iter()
        .filter_map(|verification| {
            verification.target_path.clone().or_else(|| {
                contract
                    .declared_artifact_by_ref(&verification.target_ref)
                    .map(|artifact| artifact.path.clone())
            })
        })
        .collect::<Vec<_>>();
    if verification_targets.is_empty() {
        return false;
    }
    verification_targets.iter().all(|target| {
        if contract
            .declared_artifacts
            .iter()
            .any(|artifact| artifact.path == *target && artifact.kind == "directory")
        {
            let prefix = format!("{}/", target.trim_end_matches('/'));
            let child_files = contract
                .declared_artifacts
                .iter()
                .filter(|artifact| {
                    artifact.kind != "directory" && artifact.path.starts_with(&prefix)
                })
                .map(|artifact| artifact.path.as_str())
                .collect::<Vec<_>>();
            if !child_files.is_empty() {
                return child_files.iter().all(|child_path| {
                    let read_anchor = format!("read:{child_path}");
                    report
                        .evidence_refs
                        .iter()
                        .any(|evidence_ref| evidence_ref == &read_anchor)
                });
            }
        }
        report
            .evidence_refs
            .iter()
            .any(|evidence_ref| evidence_ref == &format!("read:{target}"))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DecisionLoopConfig, LoopOutcome, LoopUsage, RoutedStateFrame, StepFailureClassification,
        StepOutcome, StepRuntimeResolutionContext, apply_tool_registry_contract,
        infer_preflight_requirements_from_state_frame, map_loop_outcome,
        requires_external_tool_execution, run_routed_step_with_runtime,
        tool_assembly_context_for_role,
    };
    use crate::core::boss_state::{BossActorRole, BossStage};
    use crate::core::state_frame::{
        ActorRole, AgentState, StageExecutionContract, StateBudget, StateFrame,
    };
    use crate::core::state_frame_model_router::{ModelRoute, ModelTier};
    use crate::service::api::client::{ModelProviderClient, ModelProviderConfig};
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;
    use crate::state::app_state::{ActiveModelProfileSource, ActiveModelProviderSummary};
    use crate::state::permission_context::{
        BossActorPolicy, PermissionMode, ToolPermissionContext,
    };
    use crate::tool::builtin::agent::AgentTool;
    use crate::tool::builtin::bash::BashTool;
    use crate::tool::builtin::file_edit::FileEditTool;
    use crate::tool::builtin::file_read::FileReadTool;
    use crate::tool::registry::ToolRegistry;
    use std::sync::Arc;

    fn worker_frame(objective: &str) -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: objective.into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: Vec::new(),
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: Vec::new(),
            allowed_actions: vec![],
            allowed_tools: vec![],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
            runtime_open_items: Vec::new(),
        }
    }

    fn verification_contract() -> StageExecutionContract {
        StageExecutionContract {
            verifications: vec![crate::core::state_frame::VerificationContract {
                target_ref: "artifact:step0:0".into(),
                target_path: Some("/tmp/report.md".into()),
                required_actions: vec!["verify_artifact".into()],
                required_evidence: vec!["/tmp/report.md".into()],
            }],
            required_actions: vec!["verify_artifact".into()],
            required_evidence: vec!["/tmp/report.md".into()],
            ..StageExecutionContract::default()
        }
    }

    fn directory_verification_contract() -> StageExecutionContract {
        StageExecutionContract {
            declared_artifacts: vec![
                crate::core::state_frame::DeclaredArtifactContract {
                    ref_id: "artifact:step0:dir".into(),
                    path: "/tmp/demo".into(),
                    kind: "directory".into(),
                    required_actions: vec!["create".into(), "write".into()],
                    required_evidence: vec!["artifact:step0:dir".into(), "/tmp/demo".into()],
                },
                crate::core::state_frame::DeclaredArtifactContract {
                    ref_id: "artifact:step0:readme".into(),
                    path: "/tmp/demo/README.md".into(),
                    kind: "file".into(),
                    required_actions: vec!["create".into(), "write".into()],
                    required_evidence: vec![
                        "artifact:step0:readme".into(),
                        "/tmp/demo/README.md".into(),
                    ],
                },
                crate::core::state_frame::DeclaredArtifactContract {
                    ref_id: "artifact:step0:runtime".into(),
                    path: "/tmp/demo/runtime.py".into(),
                    kind: "file".into(),
                    required_actions: vec!["create".into(), "write".into()],
                    required_evidence: vec![
                        "artifact:step0:runtime".into(),
                        "/tmp/demo/runtime.py".into(),
                    ],
                },
            ],
            verifications: vec![
                crate::core::state_frame::VerificationContract {
                    target_ref: "artifact:step0:dir".into(),
                    target_path: Some("/tmp/demo".into()),
                    required_actions: vec!["verify".into()],
                    required_evidence: vec!["artifact:step0:dir".into(), "/tmp/demo".into()],
                },
                crate::core::state_frame::VerificationContract {
                    target_ref: "artifact:step0:readme".into(),
                    target_path: Some("/tmp/demo/README.md".into()),
                    required_actions: vec!["verify".into()],
                    required_evidence: vec![
                        "artifact:step0:readme".into(),
                        "/tmp/demo/README.md".into(),
                    ],
                },
                crate::core::state_frame::VerificationContract {
                    target_ref: "artifact:step0:runtime".into(),
                    target_path: Some("/tmp/demo/runtime.py".into()),
                    required_actions: vec!["verify".into()],
                    required_evidence: vec![
                        "artifact:step0:runtime".into(),
                        "/tmp/demo/runtime.py".into(),
                    ],
                },
            ],
            required_actions: vec!["create".into(), "write".into(), "verify".into()],
            required_evidence: vec![
                "artifact:step0:dir".into(),
                "/tmp/demo".into(),
                "artifact:step0:readme".into(),
                "/tmp/demo/README.md".into(),
                "artifact:step0:runtime".into(),
                "/tmp/demo/runtime.py".into(),
            ],
            ..StageExecutionContract::default()
        }
    }

    fn verification_contract_with_required_input(input_path: &str) -> StageExecutionContract {
        StageExecutionContract {
            verifications: vec![crate::core::state_frame::VerificationContract {
                target_ref: "artifact:step0:0".into(),
                target_path: Some("/tmp/report.md".into()),
                required_actions: vec!["verify_artifact".into()],
                required_evidence: vec!["/tmp/report.md".into(), input_path.into()],
            }],
            content_evidence_targets: vec![input_path.into()],
            required_actions: vec!["verify_artifact".into()],
            required_evidence: vec!["/tmp/report.md".into(), input_path.into()],
            ..StageExecutionContract::default()
        }
    }

    fn test_snapshot() -> ActiveModelRuntimeSnapshot {
        ActiveModelRuntimeSnapshot {
            config: ModelProviderConfig::default(),
            client: ModelProviderClient::with_scripted_turns(Vec::new()),
            active_profile_name: None,
            active_level: None,
            source: ActiveModelProfileSource::BootstrapDefault,
            summary: ActiveModelProviderSummary {
                provider_id: "test".into(),
                protocol: "messages_api".into(),
                compatibility_profile: "messages_api".into(),
                base_url_host: "localhost".into(),
                model: "test-model".into(),
                auth_status: "no_auth".into(),
            },
        }
    }

    fn test_runtime_paths() -> (std::path::PathBuf, Option<std::path::PathBuf>) {
        let cwd = std::env::temp_dir().join("state_frame_orchestrator_tests");
        let config_root = Some(cwd.join(".morgo"));
        (cwd, config_root)
    }

    #[test]
    fn external_tool_gate_blocks_worker_only_without_runtime() {
        let frame = worker_frame("修改文件 src/core/boss.rs 并运行命令 cargo test");
        assert!(requires_external_tool_execution(&frame, false));
        assert!(!requires_external_tool_execution(&frame, true));
    }

    #[test]
    fn external_tool_gate_skips_readonly_contract() {
        let mut frame = worker_frame("read-only review run");
        frame.required_output_schema = Some("readonly_audit_4_paragraphs_v1".into());
        assert!(!requires_external_tool_execution(&frame, false));
    }

    #[tokio::test]
    async fn direct_worker_tool_snapshot_matches_registry() {
        let registry = ToolRegistry::new()
            .register(Arc::new(AgentTool))
            .register(Arc::new(BashTool))
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool));
        let permissions =
            ToolPermissionContext::new(PermissionMode::Default).with_interactive_tools(true);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry: registry.clone(),
            permissions: permissions.clone(),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut frame = worker_frame("修改文件 src/lib.rs 并运行命令 cargo test");
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        let assembled = registry.assemble(tool_assembly_context_for_role(ActorRole::Worker));
        let expected_visible = assembled
            .visible_tools(&permissions)
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<Vec<_>>();
        let expected_actions = assembled
            .derive_allowed_actions(&permissions, &snapshot.cwd)
            .await;
        assert_eq!(snapshot.visible_tools, expected_visible);
        assert_eq!(snapshot.allowed_actions, expected_actions);
        assert_eq!(frame.allowed_tools, snapshot.visible_tools);
        assert_eq!(frame.allowed_actions, snapshot.allowed_actions);
        assert!(!snapshot.visible_tools.iter().any(|tool| tool == "Agent"));
    }

    #[tokio::test]
    async fn direct_worker_preflight_fails_on_missing_write_tool() {
        let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
        let permissions =
            ToolPermissionContext::new(PermissionMode::Default).with_interactive_tools(true);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry,
            permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut frame = worker_frame("修改文件 src/lib.rs 并写入目标文件 /tmp/out.txt");
        frame.stage_execution_contract.declared_artifacts.push(
            crate::core::state_frame::DeclaredArtifactContract {
                ref_id: "artifact:step0:0".into(),
                path: "/tmp/out.txt".into(),
                kind: "file".into(),
                required_actions: vec!["write_file".into()],
                required_evidence: vec!["artifact:step0:0".into(), "/tmp/out.txt".into()],
            },
        );
        frame.stage_execution_contract.required_actions = vec!["write_file".into()];
        frame.stage_execution_contract.required_evidence =
            vec!["artifact:step0:0".into(), "/tmp/out.txt".into()];
        let routed = RoutedStateFrame {
            frame,
            model_route: ModelRoute {
                tier: ModelTier::Low,
                provider_profile_id: None,
            },
            projection_mismatch_count: 0,
        };
        let inherited_snapshot = test_snapshot();
        let outcome = run_routed_step_with_runtime(
            routed,
            DecisionLoopConfig::default(),
            StepRuntimeResolutionContext {
                inherited_snapshot: &inherited_snapshot,
                model_registry: None,
                observability: ServiceObservabilityTracker::default(),
                tool_runtime: Some(runtime),
            },
        )
        .await
        .expect("outcome");
        match outcome {
            StepOutcome::Failed {
                usage,
                tool_registry_snapshot: Some(snapshot),
                tool_contract_mismatch: Some(mismatch),
                ..
            } => {
                assert!(usage.is_none(), "preflight should fail before model loop");
                assert!(snapshot.visible_tools.iter().any(|tool| tool == "Read"));
                assert!(
                    mismatch
                        .missing_allowed_actions
                        .iter()
                        .any(|action| action == "write_file")
                );
            }
            other => panic!("expected preflight mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn direct_worker_preflight_uses_declared_artifact_path_for_edit_probe() {
        let registry = ToolRegistry::new()
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry: registry.clone(),
            permissions: permissions.clone(),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let temp_dir = std::env::temp_dir().join("state_frame_preflight_edit_probe");
        let _ = std::fs::create_dir_all(&temp_dir);
        let artifact_path = temp_dir.join("README.md");
        runtime
            .permissions
            .add_delegated_write_path(artifact_path.as_path());
        let mut frame = worker_frame("finish the assigned artifact");
        frame.stage_execution_contract.declared_artifacts.push(
            crate::core::state_frame::DeclaredArtifactContract {
                ref_id: "artifact:step0:0".into(),
                path: artifact_path.display().to_string(),
                kind: "file".into(),
                required_actions: vec!["write_file".into()],
                required_evidence: vec!["artifact:step0:0".into()],
            },
        );
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        let assembled = registry.assemble(tool_assembly_context_for_role(frame.role));
        let mismatch = assembled
            .preflight_contract(
                &runtime.permissions,
                &snapshot,
                &infer_preflight_requirements_from_state_frame(&frame),
            )
            .await
            .expect_err(
                "write path still lacks bash/write capability, but Edit should not be denied",
            );
        let _ = std::fs::remove_dir_all(&temp_dir);
        assert!(
            !mismatch
                .permission_denied_tools
                .iter()
                .any(|tool| tool == "Edit"),
            "declared artifact path should clear Edit permission probe: {mismatch:?}"
        );
    }

    #[tokio::test]
    async fn direct_worker_preflight_uses_readme_fallback_for_directory_edit_probe() {
        let registry = ToolRegistry::new()
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry: registry.clone(),
            permissions: permissions.clone(),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let artifact_dir = std::env::temp_dir().join("state_frame_preflight_dir_probe");
        let _ = std::fs::create_dir_all(&artifact_dir);
        let readme_path = artifact_dir.join("README.md");
        runtime
            .permissions
            .add_delegated_write_path(readme_path.as_path());
        let mut frame = worker_frame("finish the assigned directory artifact");
        frame.stage_execution_contract.declared_artifacts.push(
            crate::core::state_frame::DeclaredArtifactContract {
                ref_id: "artifact:step0:0".into(),
                path: artifact_dir.display().to_string(),
                kind: "directory".into(),
                required_actions: vec!["write_file".into()],
                required_evidence: vec!["artifact:step0:0".into()],
            },
        );
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        let assembled = registry.assemble(tool_assembly_context_for_role(frame.role));
        let mismatch = assembled
            .preflight_contract(
                &runtime.permissions,
                &snapshot,
                &infer_preflight_requirements_from_state_frame(&frame),
            )
            .await
            .expect_err(
                "directory write still lacks bash/write capability, but Edit should use README.md probe",
            );
        let _ = std::fs::remove_dir_all(&artifact_dir);
        assert!(
            !mismatch
                .permission_denied_tools
                .iter()
                .any(|tool| tool == "Edit"),
            "directory artifact should probe Edit with README.md fallback: {mismatch:?}"
        );
    }

    #[tokio::test]
    async fn worker_prompt_does_not_advertise_unavailable_actions() {
        let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry,
            permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut frame = worker_frame("修改文件 src/lib.rs");
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        let prompt_json = serde_json::to_string(&frame).expect("state frame json");
        assert_eq!(snapshot.visible_tools, vec!["Read".to_string()]);
        assert!(!prompt_json.contains("write_file"));
        assert!(!prompt_json.contains("run_command"));
        assert!(!frame.allowed_tools.iter().any(|tool| tool == "Edit"));
    }

    #[tokio::test]
    async fn direct_worker_preflight_requires_write_from_artifact_fact_even_without_keyword_marker()
    {
        let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry,
            permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut frame = worker_frame("finish the assigned artifact");
        frame.stage_execution_contract.declared_artifacts.push(
            crate::core::state_frame::DeclaredArtifactContract {
                ref_id: "artifact:step0:0".into(),
                path: "/tmp/p0-artifact/report.md".into(),
                kind: "file".into(),
                required_actions: vec!["write_file".into()],
                required_evidence: vec![
                    "artifact:step0:0".into(),
                    "/tmp/p0-artifact/report.md".into(),
                ],
            },
        );
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        let assembled = runtime
            .registry
            .assemble(tool_assembly_context_for_role(frame.role));
        let mismatch = assembled
            .preflight_contract(
                &runtime.permissions,
                &snapshot,
                &infer_preflight_requirements_from_state_frame(&frame),
            )
            .await
            .expect_err("artifact fact should force write requirement");
        assert!(
            mismatch
                .missing_allowed_actions
                .iter()
                .any(|action| action == "write_file")
        );
    }

    #[tokio::test]
    async fn direct_worker_preflight_does_not_infer_bash_from_objective_keywords_without_contract()
    {
        let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry,
            permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut frame = worker_frame("修改文件 src/lib.rs 并运行命令 cargo test");
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        let assembled = runtime
            .registry
            .assemble(tool_assembly_context_for_role(frame.role));
        let spec = infer_preflight_requirements_from_state_frame(&frame);
        assert!(
            spec.required_allowed_actions.is_empty(),
            "preflight must stay contract-first when no typed contract demands commands"
        );
        assert!(
            assembled
                .preflight_contract(&runtime.permissions, &snapshot, &spec)
                .await
                .is_ok(),
            "objective keywords alone must not force a Bash contract mismatch"
        );
    }

    #[tokio::test]
    async fn tool_registry_snapshot_uses_runtime_cwd_and_config_root() {
        let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        let runtime_cwd = std::env::temp_dir().join("snapshot_runtime_cwd");
        let runtime_config_root = runtime_cwd.join(".claude");
        let runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry,
            permissions,
            cwd: runtime_cwd.clone(),
            config_root: Some(runtime_config_root.clone()),
        };
        let mut frame = worker_frame("read one file");
        let snapshot = apply_tool_registry_contract(&mut frame, &runtime)
            .await
            .expect("snapshot");
        assert_eq!(snapshot.cwd, runtime_cwd);
        assert_eq!(snapshot.config_root, Some(runtime_config_root));
    }

    #[tokio::test]
    async fn agent_tool_visibility_remains_executor_b_only() {
        let registry = ToolRegistry::new()
            .register(Arc::new(AgentTool))
            .register(Arc::new(FileReadTool));

        let exec_permissions = ToolPermissionContext::new(PermissionMode::Default)
            .with_interactive_tools(true)
            .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));
        exec_permissions.add_always_allow_rule("Agent");
        let exec_runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry: registry.clone(),
            permissions: exec_permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut exec_frame = worker_frame("spawn implement worker");
        exec_frame.role = ActorRole::ExecutorB;
        let exec_snapshot = apply_tool_registry_contract(&mut exec_frame, &exec_runtime)
            .await
            .expect("executor snapshot");
        assert!(
            exec_snapshot
                .visible_tools
                .iter()
                .any(|tool| tool == "Agent")
        );
        assert!(
            exec_snapshot
                .allowed_actions
                .iter()
                .any(|action| action == "spawn_agent")
        );

        let child_permissions = ToolPermissionContext::new(PermissionMode::Default)
            .with_interactive_tools(true)
            .with_boss_actor_policy(BossActorPolicy::child(
                BossActorRole::ImplementChild,
                1,
                BossStage::Execution,
            ));
        let child_runtime = crate::core::state_frame_loop::StateFrameToolRuntime {
            registry,
            permissions: child_permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let mut child_frame = worker_frame("implement task");
        let child_snapshot = apply_tool_registry_contract(&mut child_frame, &child_runtime)
            .await
            .expect("child snapshot");
        assert!(
            !child_snapshot
                .visible_tools
                .iter()
                .any(|tool| tool == "Agent")
        );
        assert!(
            !child_snapshot
                .allowed_actions
                .iter()
                .any(|action| action == "spawn_agent")
        );
    }

    #[test]
    fn step_outcome_completed_is_rejected_when_worker_report_still_has_verification_gap() {
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Read".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "unverified".into(),
                    stage_execution_contract: verification_contract(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["read:/tmp/report.md".into()],
                    completion_evidence_gaps: vec![crate::core::state_frame::CompletionEvidenceGap {
                        target_ref: "artifact:step0:0".into(),
                        target_path: Some("/tmp/report.md".into()),
                        missing_artifact_evidence: false,
                        missing_test_evidence: false,
                        missing_verification_evidence: true,
                        recommended_action: "verify_artifact".into(),
                    }],
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &verification_contract(), None) {
            StepOutcome::Failed {
                failure_classification,
                ..
            } => {
                assert_eq!(
                    failure_classification,
                    StepFailureClassification::VerificationRepairContinuation
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn directory_verification_child_read_anchors_pass_orchestrator_gate() {
        let contract = directory_verification_contract();
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Read".into()),
                    files_changed: vec!["/tmp/demo/README.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "verified".into(),
                    stage_execution_contract: contract.clone(),
                    stage_continuation_context: None,
                    evidence_refs: vec![
                        "read:/tmp/demo/README.md".into(),
                        "read:/tmp/demo/runtime.py".into(),
                    ],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &contract, None) {
            StepOutcome::Completed { .. } => {}
            other => panic!("expected completed outcome, got {other:?}"),
        }
    }

    #[test]
    fn verification_required_contract_cannot_finish_with_empty_missing_targets_only_by_done_state()
    {
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: None,
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Write".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "unverified".into(),
                    stage_execution_contract: verification_contract(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["write:/tmp/report.md".into()],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &verification_contract(), None) {
            StepOutcome::Failed { .. } => {}
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn unverified_worker_report_cannot_pass_completion_gate() {
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Write".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "unverified".into(),
                    stage_execution_contract: verification_contract(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["write:/tmp/report.md".into()],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &verification_contract(), None) {
            StepOutcome::Failed {
                failure_classification,
                ..
            } => {
                assert_eq!(
                    failure_classification,
                    StepFailureClassification::VerificationRepairContinuation
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn missing_verification_evidence_forces_failed_not_completed() {
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Write".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "unverified".into(),
                    stage_execution_contract: verification_contract(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["write:/tmp/report.md".into()],
                    completion_evidence_gaps: vec![crate::core::state_frame::CompletionEvidenceGap {
                        target_ref: "artifact:step0:0".into(),
                        target_path: Some("/tmp/report.md".into()),
                        missing_artifact_evidence: false,
                        missing_test_evidence: false,
                        missing_verification_evidence: true,
                        recommended_action: "verify_artifact".into(),
                    }],
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &verification_contract(), None) {
            StepOutcome::Failed {
                failure_classification,
                ..
            } => {
                assert_eq!(
                    failure_classification,
                    StepFailureClassification::VerificationRepairContinuation
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn generic_report_without_target_evidence_anchor_cannot_pass_completion_gate() {
        let contract = verification_contract_with_required_input("/tmp/source.md");
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Write".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "verified".into(),
                    stage_execution_contract: contract.clone(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["write:/tmp/report.md".into()],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &contract, None) {
            StepOutcome::Failed {
                failure_classification,
                ..
            } => {
                assert_eq!(
                    failure_classification,
                    StepFailureClassification::VerificationRepairContinuation
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn write_then_read_output_only_cannot_complete_content_derived_task() {
        let contract = verification_contract_with_required_input("/tmp/source.md");
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Read".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "verified".into(),
                    stage_execution_contract: contract.clone(),
                    stage_continuation_context: None,
                    evidence_refs: vec![
                        "write:/tmp/report.md".into(),
                        "read:/tmp/report.md".into(),
                    ],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &contract, None) {
            StepOutcome::Failed {
                failure_classification,
                ..
            } => {
                assert_eq!(
                    failure_classification,
                    StepFailureClassification::VerificationRepairContinuation
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn content_derived_task_completes_after_required_source_reads() {
        let contract = verification_contract_with_required_input("/tmp/source.md");
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Read".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "verified".into(),
                    stage_execution_contract: contract.clone(),
                    stage_continuation_context: None,
                    evidence_refs: vec![
                        "read:/tmp/source.md".into(),
                        "write:/tmp/report.md".into(),
                        "read:/tmp/report.md".into(),
                    ],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &contract, None) {
            StepOutcome::Completed { .. } => {}
            other => panic!("expected completed outcome, got {other:?}"),
        }
    }

    #[test]
    fn report_with_only_artifact_presence_evidence_cannot_finish_verification_contract() {
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Write".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "verified".into(),
                    stage_execution_contract: verification_contract(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["write:/tmp/report.md".into()],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &verification_contract(), None) {
            StepOutcome::Failed {
                failure_classification,
                ..
            } => {
                assert_eq!(
                    failure_classification,
                    StepFailureClassification::VerificationRepairContinuation
                );
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
    }

    #[test]
    fn verification_first_read_anchor_can_pass_completion_gate_without_verified_status() {
        let outcome = LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: LoopUsage {
                completion_evidence_status: Some(
                    crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                ),
                worker_report: Some(crate::core::state_frame::WorkerStructuredReport {
                    worker_state: AgentState::Done,
                    last_tool_action: Some("Read".into()),
                    files_changed: vec!["/tmp/report.md".into()],
                    tests_run: Vec::new(),
                    artifact_status: "verified".into(),
                    test_status: "not_required".into(),
                    verification_status: "unverified".into(),
                    stage_execution_contract: verification_contract(),
                    stage_continuation_context: None,
                    evidence_refs: vec!["read:/tmp/report.md".into()],
                    completion_evidence_gaps: Vec::new(),
                    remaining_risks: Vec::new(),
                    completion_evidence_status:
                        crate::core::state_frame::CompletionEvidenceStatus::Sufficient,
                }),
                ..LoopUsage::default()
            },
        };

        match map_loop_outcome(outcome, &verification_contract(), None) {
            StepOutcome::Completed { .. } => {}
            other => panic!("expected completed outcome, got {other:?}"),
        }
    }
}

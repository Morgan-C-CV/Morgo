use crate::bootstrap::config_root::resolve_config_root;
use crate::bootstrap::model_profiles::load_model_profiles_registry_from_root;
use crate::core::boss_acceptance::{extract_artifact_expectations, verify_artifact_expectations};
use crate::core::boss_actor_runtime::{
    BossActorEvent, BossActorRegistry, DesignerACommand, ExecutorBCommand,
};
use crate::core::boss_context_brief::{
    BossContextBrief, BossContextStrategy, BossStateFrame, PermissionScopeView, RelevantFileHandle,
    TargetArtifact, assemble_brief_prompt,
};
use crate::core::boss_runtime::{BossControlRuntime, BossRuntimeOwner};
use crate::core::boss_state::{
    BossActorHandle, BossActorStatus, BossControlRequest, BossControlResponse, BossLisMPolicy,
    BossObservabilitySummary, BossPlan, BossPlanStep, BossPlanStepStatus, BossReportPayload,
    BossRolloutPolicyDecision, BossRolloutTargetDecision, BossSession, BossStage, BossStatus,
    BossStepMetrics, BossStepReport, BossStepRoutedMetadata, BossStopOutcome, BossStopStage,
    CompressionStrategy, ContextMode, ExecutorBStageMemory, ExecutorBStageMemoryContinuity,
};
use crate::core::boss_test_readiness::BossTestRunOutcome;
use crate::core::context::WorkerLisMPolicy;
use crate::core::lism_ab_sample::SharedLisMAbSampleSink;
use crate::core::prompt_budget::{BudgetDecision, evaluate_message_budget};
use crate::core::state_frame::{
    ActorRole, CompletionEvidenceStatus, DeclaredArtifactContract, StageExecutionContract,
    VerificationContract,
};
use crate::core::state_frame_loop::{DecisionLoopConfig, StateFrameToolRuntime};
use crate::core::state_frame_model_router::ModelTier;
use crate::core::state_frame_orchestrator::{
    StepFailureClassification, StepOutcome, StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
    requires_external_tool_execution, run_routed_step_with_runtime,
};
use crate::history::session::SessionHistory;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::state::app_state::WorkerRole;
use crate::task::manager::TaskManager;
use crate::task::types::{TaskEvent, TaskStatus, TaskUsageSummary};
use crate::tool::definition::{ObservableInput, ObservableInputSource, Tool, ToolCall};
use crate::tool::result::{
    ToolBatchContext, ToolExecutionOutcomeKind, ToolExecutionRecord, ToolReportModifier,
};
use serde_json::json;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct BossCoordinator {
    pub status: Arc<RwLock<BossStatus>>,
    /// Placed here so the planner can hold and modify it in memory before flushing
    pub plan: Arc<RwLock<Option<BossPlan>>>,

    /// Structured actor topology — replaces the former loose a/b session_id/cancel fields.
    pub session: Arc<RwLock<Option<BossSession>>>,

    /// Live actor runtimes for A and B — bootstrapped on restore/init.
    pub actor_registry: Arc<RwLock<Option<BossActorRegistry>>>,

    pub auto_advance_app_state: Arc<RwLock<Option<Arc<crate::state::app_state::AppState>>>>,
    routed_step_metadata: Arc<RwLock<std::collections::HashMap<usize, BossStepRoutedMetadata>>>,
    runtime_key: Arc<RwLock<Option<String>>>,
    runtime_owner: Arc<BossRuntimeOwner>,
    lism_policy: Arc<RwLock<BossLisMPolicy>>,
    worker_lism_policy: Arc<RwLock<WorkerLisMPolicy>>,
    full_worker_dispatch_fallback_enabled: Arc<RwLock<bool>>,
    lism_ab_sink: Option<SharedLisMAbSampleSink>,
}

fn step_artifact_verification_error(step: &BossPlanStep) -> Option<String> {
    verify_artifact_expectations(step.objective())
        .err()
        .map(|reason| format!("artifact verification failed: {reason}"))
}

fn step_requires_verification_evidence(step: &BossPlanStep) -> bool {
    !step.stage_execution_contract.verifications.is_empty()
        || step
            .stage_execution_contract
            .required_actions
            .iter()
            .any(|action| matches!(action.as_str(), "verify" | "verify_artifact" | "run_verification"))
}

fn step_completion_gate_error(
    step: &BossPlanStep,
    metadata: Option<&BossStepRoutedMetadata>,
) -> Option<(String, StepFailureClassification)> {
    if let Some(reason) = step_artifact_verification_error(step) {
        return Some((reason, StepFailureClassification::RepairableRecovery));
    }
    if !step_requires_verification_evidence(step) {
        return None;
    }
    let metadata = metadata?;
    let completion_sufficient = matches!(
        metadata.completion_evidence_status.as_deref(),
        Some("sufficient")
    ) && metadata
        .worker_report
        .as_ref()
        .is_some_and(|report| report.completion_evidence_status == CompletionEvidenceStatus::Sufficient);
    if completion_sufficient && metadata.completion_evidence_gaps.is_empty() {
        return None;
    }
    let classification = if metadata
        .completion_evidence_gaps
        .iter()
        .any(|gap| gap.missing_verification_evidence)
        || matches!(
            metadata.completion_evidence_status.as_deref(),
            Some("missing_verification_evidence")
        ) {
        StepFailureClassification::VerificationRepairContinuation
    } else {
        StepFailureClassification::RepairableRecovery
    };
    Some((
        "completion gate rejected direct completion: verification evidence still missing".into(),
        classification,
    ))
}

fn primary_declared_artifact_path(step: &BossPlanStep) -> Option<String> {
    step.stage_execution_contract
        .declared_artifacts
        .first()
        .map(|artifact| artifact.path.clone())
        .or_else(|| {
            extract_artifact_expectations(step.objective())
                .into_iter()
                .next()
                .map(|expectation| expectation.path.display().to_string())
        })
}

fn build_artifact_repair_instruction(step: &BossPlanStep, missing_reason: &str) -> Option<String> {
    let expectation = step
        .stage_execution_contract
        .declared_artifacts
        .first()
        .map(|artifact| (artifact.path.clone(), artifact.kind.clone()))
        .or_else(|| {
            extract_artifact_expectations(step.objective())
                .into_iter()
                .next()
                .map(|expectation| {
                    let kind = match expectation.kind {
                        crate::core::boss_acceptance::BossArtifactKind::File => "file".to_string(),
                        crate::core::boss_acceptance::BossArtifactKind::Directory => {
                            "directory".to_string()
                        }
                    };
                    (expectation.path.display().to_string(), kind)
                })
        })?;
    let target_path = expectation.0;
    let parent_dir = std::path::Path::new(&target_path)
        .parent()
        .map(|path| path.display().to_string())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| ".".into());
    let recommended_write_strategy = match expectation.1.as_str() {
        "file" => "write_exact_target_file",
        "directory" => "create_directory_then_write_files",
        _ => "write_exact_target_file",
    };
    Some(format!(
        "repair artifact evidence for target_path={target_path} parent_dir={parent_dir} missing_reason={missing_reason} recommended_write_strategy={recommended_write_strategy}"
    ))
}

fn build_verification_repair_instruction(step: &BossPlanStep) -> Option<String> {
    let target_path = primary_declared_artifact_path(step)?;
    Some(format!(
        "re-verify artifact evidence for target_path={}",
        target_path
    ))
}

fn has_only_verification_evidence_gap(step: &BossPlanStep) -> bool {
    step.tool_execution_records
        .iter()
        .filter(|record| record.tool_name == "ArtifactVerify")
        .any(|record| {
            record.kind == ToolExecutionOutcomeKind::Interrupted
                && record
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("artifact verification status=missing_or_invalid"))
        })
        && step.stage_continuation_context.as_ref().is_some_and(|context| {
            context
                .next_action
                .as_deref()
                .is_some_and(|action| {
                    action.eq_ignore_ascii_case("verify_artifact")
                        || action.eq_ignore_ascii_case("run_verification")
                })
        })
}

fn continuation_verified_facts(step: &BossPlanStep) -> Vec<String> {
    step.tool_execution_records
        .iter()
        .map(|record| record.summary.clone())
        .take(5)
        .collect()
}

fn is_verification_first_continuation(step: &BossPlanStep) -> bool {
    step.executor_b_stage_memory
        .as_ref()
        .and_then(|memory| memory.continuity)
        == Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated)
        || step
            .stage_continuation_context
            .as_ref()
            .and_then(|context| context.next_action.as_deref())
            .is_some_and(|action| {
                action.eq_ignore_ascii_case("verify_artifact")
                    || action.eq_ignore_ascii_case("run_verification")
            })
        || step
            .last_correction
            .as_deref()
            .is_some_and(|correction| correction.eq_ignore_ascii_case("verify_artifact"))
}

fn verification_first_output_contract() -> String {
    "Return exactly four labeled lines:
verified_target: <exact target path>
verification_result: <verified|blocked>
minimal_evidence: <1-3 short facts>
remaining_blocker: <none|short blocker>
Forbidden: Files changed, Minimal verification steps, next_action for coordinator, further reading suggestions, file-reading plans, truncation notes, roadmap, replan prose, or extended next steps."
        .into()
}

fn general_worker_output_contract() -> String {
    "return a unified diff or file edits".into()
}

fn target_scoped_verification_evidence(step: &BossPlanStep) -> Vec<String> {
    let mut evidence = Vec::new();
    for record in &step.tool_execution_records {
        if record.kind != ToolExecutionOutcomeKind::Success {
            continue;
        }
        if matches!(
            record.tool_name.as_str(),
            "ArtifactVerify" | "Read" | "Bash" | "Write" | "Glob"
        ) && !record.summary.trim().is_empty()
            && !evidence.iter().any(|existing| existing == &record.summary)
        {
            evidence.push(record.summary.clone());
        }
        if evidence.len() >= 3 {
            break;
        }
    }
    evidence
}

fn build_brief_verification_review_summary(step: &BossPlanStep, source: &str) -> String {
    let verified_target = primary_declared_artifact_path(step).unwrap_or_else(|| "unknown".into());
    let evidence = target_scoped_verification_evidence(step);
    let evidence_line = if evidence.is_empty() { "none".to_string() } else { evidence.join("; ") };
    format!(
        "verified_target: {verified_target}\nverification_result: verified\nminimal_evidence: {evidence_line}\nremaining_blocker: none"
    )
}

fn build_verification_first_brief_objective(step: &BossPlanStep) -> String {
    let target = primary_declared_artifact_path(step).unwrap_or_else(|| "unknown".into());
    format!(
        "Verify target artifact only: {target}. Return a short verification result only."
    )
}

fn build_verification_first_acceptance(step: &BossPlanStep) -> Vec<String> {
    let target = primary_declared_artifact_path(step).unwrap_or_else(|| "unknown".into());
    vec![
        format!("verified_target: {target}"),
        "verification_result: verified|blocked".into(),
    ]
}

fn shape_verification_first_result_text(step: &BossPlanStep, text: &str) -> String {
    let target = primary_declared_artifact_path(step).unwrap_or_else(|| "unknown".into());
    let mut verification_result = None;
    let mut minimal_evidence = None;
    let mut remaining_blocker = None;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        if verification_result.is_none()
            && (lower.starts_with("verification result:")
                || lower.starts_with("verification_result:"))
        {
            verification_result = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
        if minimal_evidence.is_none()
            && (lower.starts_with("minimal evidence:")
                || lower.starts_with("minimal_evidence:"))
        {
            minimal_evidence = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
        if remaining_blocker.is_none()
            && (lower.starts_with("remaining blocker:")
                || lower.starts_with("remaining_blocker:")
                || lower.starts_with("remaining blockers"))
        {
            remaining_blocker = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
    }

    let evidence = minimal_evidence.unwrap_or_else(|| {
        let facts = target_scoped_verification_evidence(step);
        if facts.is_empty() {
            "none recorded".into()
        } else {
            facts.join("; ")
        }
    });
    let result = verification_result.unwrap_or_else(|| "verified".into());
    let blocker = remaining_blocker.unwrap_or_else(|| "none".into());

    format!(
        "verified_target: {target}\nverification_result: {result}\nminimal_evidence: {evidence}\nremaining_blocker: {blocker}"
    )
}

fn sync_legacy_correction_from_continuation(step: &mut BossPlanStep) {
    if let Some(context) = step.stage_continuation_context.as_ref() {
        step.last_correction = context
            .next_action
            .clone()
            .or_else(|| context.failed_target.clone())
            .or_else(|| {
                context
                    .repair_intent
                    .as_ref()
                    .and_then(|intent| intent.next_action.clone().or(intent.failed_target.clone()))
            });
    }
}

fn update_step_continuation_context(
    step: &mut BossPlanStep,
    mode: crate::core::state_frame::ContinuityMode,
    failed_target: Option<String>,
    next_action: Option<String>,
    verified_facts: Vec<String>,
) {
    let effective_failed_target = failed_target.or_else(|| {
        step.stage_continuation_context
            .as_ref()
            .and_then(|context| context.failed_target.clone())
    });
    let effective_next_action = next_action.or_else(|| {
        step.stage_continuation_context
            .as_ref()
            .and_then(|context| context.next_action.clone())
    });
    let effective_verified_facts = if verified_facts.is_empty() {
        step.stage_continuation_context
            .as_ref()
            .map(|context| context.verified_facts.clone())
            .unwrap_or_default()
    } else {
        verified_facts
    };
    let context = crate::core::state_frame::StageContinuationContext {
        repair_intent: Some(crate::core::state_frame::RepairIntent {
            failed_target: effective_failed_target.clone(),
            verified_facts: effective_verified_facts.clone(),
            next_action: effective_next_action.clone(),
            continuity_mode: Some(mode.clone()),
        }),
        failed_target: effective_failed_target,
        verified_facts: effective_verified_facts,
        next_action: effective_next_action,
        continuity_mode: Some(mode),
    };
    step.stage_continuation_context = Some(context);
    sync_legacy_correction_from_continuation(step);
}

fn clear_step_continuation_context(step: &mut BossPlanStep) {
    step.stage_continuation_context = None;
    step.last_correction = None;
    step.executor_b_stage_memory = None;
}

fn build_stage_execution_contract(
    step: &BossPlanStep,
    target_artifacts: &[TargetArtifact],
) -> StageExecutionContract {
    let declared_artifacts = target_artifacts
        .iter()
        .map(|artifact| DeclaredArtifactContract {
            ref_id: artifact.path.clone(),
            path: artifact.path.clone(),
            kind: artifact.kind.clone(),
            required_actions: vec!["create".into(), "write".into()],
            required_evidence: vec![artifact.path.clone()],
        })
        .collect::<Vec<_>>();
    let verifications = target_artifacts
        .iter()
        .map(|artifact| VerificationContract {
            target_ref: artifact.path.clone(),
            target_path: Some(artifact.path.clone()),
            required_actions: vec!["verify".into()],
            required_evidence: vec![artifact.path.clone()],
        })
        .collect::<Vec<_>>();
    let tests = step
        .acceptance
        .iter()
        .filter(|item| {
            let lowered = item.to_ascii_lowercase();
            lowered.contains("test") || lowered.contains("verify")
        })
        .map(|item| crate::core::state_frame::TestContract {
            name: item.clone(),
            required_actions: vec!["run_test".into()],
            required_evidence: vec![item.clone()],
        })
        .collect::<Vec<_>>();
    let mut required_actions = vec!["create".into(), "write".into(), "verify".into()];
    if !tests.is_empty() {
        required_actions.push("run_test".into());
    }
    let mut required_evidence = target_artifacts
        .iter()
        .map(|artifact| artifact.path.clone())
        .collect::<Vec<_>>();
    required_evidence.extend(tests.iter().map(|item| item.name.clone()));
    StageExecutionContract {
        declared_artifacts,
        verifications,
        tests,
        required_actions,
        required_evidence,
    }
}

fn inject_declared_writable_artifact_paths(
    permissions: &crate::state::permission_context::ToolPermissionContext,
    contract: &StageExecutionContract,
) {
    for artifact in &contract.declared_artifacts {
        let path = artifact.path.trim();
        if path.is_empty() {
            continue;
        }
        if artifact.required_actions.iter().any(|action| {
            matches!(
                action.as_str(),
                "write_file" | "edit_file" | "create" | "write"
            )
        }) {
            permissions.add_delegated_write_path(path);
        }
    }
}

fn seed_step_acceptance(task: &str) -> Vec<String> {
    let mut acceptance = vec!["Task completed successfully.".to_string()];
    for expectation in extract_artifact_expectations(task) {
        let line = match expectation.kind {
            crate::core::boss_acceptance::BossArtifactKind::File => {
                format!(
                    "target file exists and is non-empty: {}",
                    expectation.path.display()
                )
            }
            crate::core::boss_acceptance::BossArtifactKind::Directory => {
                format!(
                    "target directory exists and is non-empty: {}",
                    expectation.path.display()
                )
            }
        };
        if !acceptance.iter().any(|item| item == &line) {
            acceptance.push(line);
        }
    }
    acceptance
}

fn classify_relevant_file_handle(path: &str, line: &str) -> String {
    if line.contains("目标目录") || path.ends_with('/') {
        "target_directory".to_string()
    } else if line.contains("目标文件") {
        "target_file".to_string()
    } else if path.ends_with(".rs") {
        "source_file".to_string()
    } else if path.ends_with(".md") {
        "document".to_string()
    } else if path.ends_with(".jsonl") || path.ends_with(".json") || path.ends_with(".log") {
        "data_or_log".to_string()
    } else {
        "path".to_string()
    }
}

fn build_file_handle_relevance(kind: &str, line: &str, path: &str) -> String {
    if line.contains("目标文件") {
        format!("explicit target file for this step: {path}")
    } else if line.contains("目标目录") {
        format!("explicit target directory for this step: {path}")
    } else {
        format!("referenced in step objective as {kind}: {path}")
    }
}

fn classify_step_success(metadata: Option<&BossStepRoutedMetadata>) -> Option<crate::core::boss_state::BossSuccessClassification> {
    let metadata = metadata?;
    let worker_report = metadata.worker_report.as_ref();
    let completion = metadata.completion_evidence_status.as_deref();
    let has_success_gaps = metadata
        .completion_evidence_gaps
        .iter()
        .any(|gap| gap.missing_artifact_evidence || gap.missing_test_evidence || gap.missing_verification_evidence);
    let via_full_worker_dispatch = matches!(
        metadata.recovery_tier.as_deref(),
        Some("full_worker_dispatch")
    ) || matches!(
        metadata.fallback_tier.as_deref(),
        Some("full_worker_dispatch")
    ) || matches!(
        metadata.recovery_outcome.as_deref(),
        Some("full_worker_dispatch_success")
    );
    let via_verification_first = matches!(
        metadata.recovery_tier.as_deref(),
        Some("verification_first")
    ) || matches!(
        metadata.fallback_tier.as_deref(),
        Some("verification_first")
    ) || matches!(
        metadata.recovery_outcome.as_deref(),
        Some("verification_first_success")
    );
    let via_recovery = metadata.recovery_attempted.unwrap_or(false)
        || metadata.recovery_outcome.is_some()
        || metadata.terminal_blocker_kind.is_some();
    let achieved_artifact = worker_report
        .map(|report| report.artifact_status.as_str() == "verified")
        .unwrap_or(false)
        || completion == Some("sufficient");
    let passed_verification = worker_report
        .map(|report| report.verification_status.as_str() == "verified")
        .unwrap_or(false)
        || completion == Some("sufficient");

    if metadata.terminal_blocker_kind.as_deref() == Some("true_external_blocker") {
        return Some(crate::core::boss_state::BossSuccessClassification::TrueExternalBlocker);
    }
    if via_full_worker_dispatch && achieved_artifact && passed_verification {
        return Some(
            crate::core::boss_state::BossSuccessClassification::FullWorkerDispatchSuccess,
        );
    }
    if via_verification_first && achieved_artifact && passed_verification {
        return Some(crate::core::boss_state::BossSuccessClassification::FallbackSuccess);
    }
    if via_recovery && achieved_artifact && passed_verification {
        return Some(crate::core::boss_state::BossSuccessClassification::RecoveredSuccess);
    }
    if has_success_gaps && achieved_artifact && passed_verification {
        return Some(crate::core::boss_state::BossSuccessClassification::FallbackSuccess);
    }
    if achieved_artifact && passed_verification {
        return Some(crate::core::boss_state::BossSuccessClassification::DirectSuccess);
    }
    None
}

#[derive(Debug, Clone)]
struct ExecutorBAssignmentContract {
    brief: BossContextBrief,
    state_frame: BossStateFrame,
    allowed_tools: Vec<String>,
    lism_policy: String,
    worker_role: WorkerRole,
    assignment_fingerprint: String,
}

#[derive(Debug, Clone)]
struct ContinuePayloadBuild {
    payload: String,
    assignment_fingerprint: String,
    plan_version: String,
    step_revision: String,
}

#[derive(Debug, Clone)]
struct SpawnPayloadBuild {
    payload: String,
    assignment_fingerprint: String,
    plan_version: String,
    step_revision: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
struct ContinuationPayload {
    #[serde(default)]
    failed_target: Option<String>,
    #[serde(default)]
    verified_facts: Vec<String>,
    #[serde(default)]
    next_action: Option<String>,
    #[serde(default)]
    continuity_mode: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
struct StageMemoryPayload {
    #[serde(default)]
    recent_reads: Vec<String>,
    #[serde(default)]
    recent_edits: Vec<String>,
    #[serde(default)]
    recent_test_refs: Vec<String>,
    #[serde(default)]
    recent_verification_refs: Vec<String>,
    #[serde(default)]
    failed_targets: Vec<String>,
    #[serde(default)]
    verified_targets: Vec<String>,
    #[serde(default)]
    continuity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StepRolloutExecutionPolicy {
    forced_worker_lism_policy: WorkerLisMPolicy,
    fallback_tier: &'static str,
    fallback_reason: &'static str,
    worker_role: WorkerRole,
    force_fresh_spawn: bool,
    affected_gaps: Vec<crate::core::state_frame::CompletionEvidenceGap>,
}

fn assignment_fingerprint(material: &serde_json::Value) -> String {
    let mut hasher = DefaultHasher::new();
    material.to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn is_verification_first_assignment_contract(contract: &ExecutorBAssignmentContract) -> bool {
    contract.worker_role == WorkerRole::Verify
        && contract
            .state_frame
            .executor_b_stage_memory
            .as_ref()
            .and_then(|memory| memory.continuity)
            == Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated)
}

fn verification_first_contract_target(contract: &ExecutorBAssignmentContract) -> String {
    contract
        .state_frame
        .stage_execution_contract
        .declared_artifacts
        .first()
        .map(|artifact| artifact.path.clone())
        .or_else(|| contract.brief.target_files.first().cloned())
        .or_else(|| contract.brief.target_artifacts.first().map(|artifact| artifact.path.clone()))
        .unwrap_or_else(|| contract.brief.objective.clone())
}

fn verification_first_contract_facts(contract: &ExecutorBAssignmentContract) -> Vec<String> {
    let mut facts = contract
        .state_frame
        .stage_continuation_context
        .as_ref()
        .map(|context| context.verified_facts.clone())
        .unwrap_or_default();
    if facts.is_empty() {
        facts = contract
            .state_frame
            .recent_local_facts
            .iter()
            .take(2)
            .cloned()
            .collect();
    }
    facts.truncate(3);
    facts
}

fn verification_first_contract_blocker(contract: &ExecutorBAssignmentContract) -> String {
    contract
        .state_frame
        .stage_continuation_context
        .as_ref()
        .and_then(|context| {
            context
                .next_action
                .clone()
                .or_else(|| {
                    context
                        .repair_intent
                        .as_ref()
                        .and_then(|intent| intent.next_action.clone())
                })
        })
        .unwrap_or_else(|| "none".into())
}

fn build_verification_first_task_message(contract: &ExecutorBAssignmentContract) -> String {
    let target = verification_first_contract_target(contract);
    let acceptance = contract
        .brief
        .acceptance
        .first()
        .cloned()
        .unwrap_or_else(|| "target-scoped verification".into());
    let facts = verification_first_contract_facts(contract);
    let evidence = if facts.is_empty() {
        "none".into()
    } else {
        facts.join("; ")
    };
    let blocker = verification_first_contract_blocker(contract);
    format!(
        "verified_target: {target}\nacceptance: {acceptance}\nminimal_evidence: {evidence}\nremaining_blocker: {blocker}"
    )
}

fn build_continuation_payload(contract: &ExecutorBAssignmentContract) -> ContinuationPayload {
    let typed_context = contract.state_frame.stage_continuation_context.as_ref();
    if is_verification_first_assignment_contract(contract) {
        return ContinuationPayload {
            failed_target: typed_context
                .and_then(|context| {
                    context
                        .failed_target
                        .clone()
                        .or_else(|| {
                            context
                                .repair_intent
                                .as_ref()
                                .and_then(|intent| intent.failed_target.clone())
                        })
                })
                .or_else(|| {
                    contract
                        .state_frame
                        .stage_execution_contract
                        .declared_artifacts
                        .first()
                        .map(|artifact| artifact.path.clone())
                }),
            verified_facts: typed_context
                .map(|context| context.verified_facts.iter().take(3).cloned().collect())
                .unwrap_or_else(|| {
                    contract
                        .state_frame
                        .recent_local_facts
                        .iter()
                        .take(2)
                        .cloned()
                        .collect()
                }),
            next_action: typed_context
                .and_then(|context| {
                    context.next_action.clone().or_else(|| {
                        context
                            .repair_intent
                            .as_ref()
                            .and_then(|intent| intent.next_action.clone())
                    })
                })
                .or_else(|| Some("verify_artifact".into())),
            continuity_mode: Some(
                typed_context
                    .and_then(|context| context.continuity_mode.as_ref())
                    .map(|mode| match mode {
                        crate::core::state_frame::ContinuityMode::Continue => "continue",
                        crate::core::state_frame::ContinuityMode::Repair => "repair",
                    })
                    .unwrap_or("repair")
                    .into(),
            ),
        };
    }
    ContinuationPayload {
        failed_target: typed_context
            .and_then(|context| {
                context
                    .failed_target
                    .clone()
                    .or_else(|| {
                        context
                            .repair_intent
                            .as_ref()
                            .and_then(|intent| intent.failed_target.clone())
                    })
            })
            .or_else(|| {
                contract
                    .state_frame
                    .stage_execution_contract
                    .declared_artifacts
                    .first()
                    .map(|artifact| artifact.path.clone())
            })
            .or_else(|| Some(contract.brief.objective.clone())),
        verified_facts: typed_context
            .map(|context| context.verified_facts.clone())
            .filter(|facts| !facts.is_empty())
            .unwrap_or_else(|| {
                contract
                    .state_frame
                    .recent_local_facts
                    .iter()
                    .take(5)
                    .cloned()
                    .collect()
            }),
        next_action: typed_context
            .and_then(|context| {
                context.next_action.clone().or_else(|| {
                    context
                        .repair_intent
                        .as_ref()
                        .and_then(|intent| intent.next_action.clone())
                })
            })
            .or_else(|| contract.state_frame.allowed_actions.first().cloned())
            .or_else(|| contract.allowed_tools.first().cloned()),
        continuity_mode: Some(
            typed_context
                .and_then(|context| context.continuity_mode.as_ref())
                .map(|mode| match mode {
                    crate::core::state_frame::ContinuityMode::Continue => "continue",
                    crate::core::state_frame::ContinuityMode::Repair => "repair",
                })
                .unwrap_or("continue")
                .into(),
        ),
    }
}

fn build_stage_memory_payload(contract: &ExecutorBAssignmentContract) -> Option<StageMemoryPayload> {
    let memory = contract.state_frame.executor_b_stage_memory.as_ref()?;
    Some(StageMemoryPayload {
        recent_reads: memory.recent_reads.clone(),
        recent_edits: memory.recent_edits.clone(),
        recent_test_refs: memory.recent_test_refs.clone(),
        recent_verification_refs: memory.recent_verification_refs.clone(),
        failed_targets: memory.failed_targets.clone(),
        verified_targets: memory.verified_targets.clone(),
        continuity: memory
            .continuity
            .as_ref()
            .map(|value| format!("{value:?}").to_ascii_lowercase()),
    })
}

fn build_stage_continuation_context(
    step: &BossPlanStep,
) -> Option<crate::core::state_frame::StageContinuationContext> {
    step.stage_continuation_context.clone().or_else(|| {
        let verified_facts = continuation_verified_facts(step);
        if verified_facts.is_empty() && step.last_correction.is_none() {
            return None;
        }
        Some(crate::core::state_frame::StageContinuationContext {
            repair_intent: Some(crate::core::state_frame::RepairIntent {
                failed_target: step.last_correction.clone(),
                verified_facts: verified_facts.clone(),
                next_action: step.last_correction.clone(),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            failed_target: step.last_correction.clone(),
            verified_facts,
            next_action: step.last_correction.clone(),
            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
        })
    })
}

fn classify_repairable_failure(failure_classification: StepFailureClassification) -> bool {
    matches!(
        failure_classification,
        StepFailureClassification::RepairableRecovery
            | StepFailureClassification::VerificationRepairContinuation
    )
}

fn apply_step_failure_classification(
    step: &mut BossPlanStep,
    failure_classification: StepFailureClassification,
    reason: &str,
) {
    step.completed = false;
    step.last_review_summary = Some(reason.to_string());
    if classify_repairable_failure(failure_classification) {
        step.status = BossPlanStepStatus::Rejected;
        update_step_continuation_context(
            step,
            crate::core::state_frame::ContinuityMode::Repair,
            primary_declared_artifact_path(step),
            Some(reason.to_string()),
            continuation_verified_facts(step),
        );
    } else {
        step.status = BossPlanStepStatus::Failed;
    }
}

fn should_emit_terminal_aborted_sample(repair_continuation_dispatched: bool) -> bool {
    !repair_continuation_dispatched
}

fn should_continue_repairable_failure(
    failure_classification: StepFailureClassification,
    step_status: BossPlanStepStatus,
) -> bool {
    classify_repairable_failure(failure_classification)
        && step_status == BossPlanStepStatus::Rejected
}

fn push_unique_memory_item(items: &mut Vec<String>, item: Option<String>, limit: usize) {
    let Some(item) = item.map(|value| value.trim().to_string()) else {
        return;
    };
    if item.is_empty() || items.iter().any(|existing| existing == &item) {
        return;
    }
    items.push(item);
    if items.len() > limit {
        let drop_count = items.len() - limit;
        items.drain(0..drop_count);
    }
}

fn continuity_for_step_memory(
    step: &BossPlanStep,
    metadata: Option<&BossStepRoutedMetadata>,
) -> ExecutorBStageMemoryContinuity {
    match metadata.and_then(|meta| meta.fallback_tier.as_deref()) {
        Some("verification_first") => ExecutorBStageMemoryContinuity::VerificationFirstIsolated,
        Some("full_context") => {
            if matches!(step.status, BossPlanStepStatus::Running | BossPlanStepStatus::Rejected) {
                ExecutorBStageMemoryContinuity::FullContextReuse
            } else {
                ExecutorBStageMemoryContinuity::FullContextFresh
            }
        }
        Some("full_worker_dispatch") => {
            if matches!(step.status, BossPlanStepStatus::Running | BossPlanStepStatus::Rejected) {
                ExecutorBStageMemoryContinuity::FullWorkerDispatchReuse
            } else {
                ExecutorBStageMemoryContinuity::FullWorkerDispatchFresh
            }
        }
        _ => {
            if matches!(step.status, BossPlanStepStatus::Running | BossPlanStepStatus::Rejected) {
                ExecutorBStageMemoryContinuity::ReuseWithinStep
            } else {
                ExecutorBStageMemoryContinuity::FreshStep
            }
        }
    }
}

fn project_executor_b_stage_memory(
    step: &BossPlanStep,
    metadata: Option<&BossStepRoutedMetadata>,
) -> Option<ExecutorBStageMemory> {
    let mut memory = step.executor_b_stage_memory.clone().unwrap_or_default();
    memory.continuity = Some(continuity_for_step_memory(step, metadata));

    for record in &step.tool_execution_records {
        match record.tool_name.as_str() {
            "Read" => {
                push_unique_memory_item(&mut memory.recent_reads, observable_path_local(record), 5);
            }
            "Edit" | "Write" => {
                push_unique_memory_item(&mut memory.recent_edits, observable_path_local(record), 5);
            }
            "Bash" => {
                push_unique_memory_item(
                    &mut memory.recent_test_refs,
                    observable_bash_command_local(record),
                    5,
                );
            }
            "ArtifactVerify" => {
                let path = observable_path_local(record)
                    .or_else(|| record.summary.split(':').next_back().map(str::trim).map(str::to_string));
                push_unique_memory_item(
                    &mut memory.recent_verification_refs,
                    Some(record.summary.clone()),
                    5,
                );
                if record.summary.contains("missing_or_invalid")
                    || record.summary.contains("target file missing")
                    || record.summary.contains("artifact verification failed")
                {
                    push_unique_memory_item(&mut memory.failed_targets, path, 5);
                } else {
                    push_unique_memory_item(&mut memory.verified_targets, path, 5);
                }
            }
            _ => {}
        }
    }

    if let Some(context) = step.stage_continuation_context.as_ref() {
        push_unique_memory_item(&mut memory.failed_targets, context.failed_target.clone(), 5);
        for fact in &context.verified_facts {
            push_unique_memory_item(&mut memory.recent_verification_refs, Some(fact.clone()), 5);
        }
    }

    let has_memory = !memory.recent_reads.is_empty()
        || !memory.recent_edits.is_empty()
        || !memory.recent_test_refs.is_empty()
        || !memory.recent_verification_refs.is_empty()
        || !memory.failed_targets.is_empty()
        || !memory.verified_targets.is_empty();
    has_memory.then_some(memory)
}

fn strip_to_path_start(candidate: &str) -> &str {
    if candidate.starts_with("./") || candidate.starts_with("../") || candidate.starts_with('/') {
        return candidate;
    }
    candidate
        .find('/')
        .map(|idx| &candidate[idx..])
        .unwrap_or(candidate)
}

fn is_probably_filesystem_hint(candidate: &str) -> bool {
    let trimmed = candidate.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return false;
    }
    let lowered = trimmed.to_ascii_lowercase();
    if matches!(
        lowered.as_str(),
        "/boss" | "/mcp" | "/skills" | "/lism" | "/effort" | "/status"
    ) {
        return false;
    }
    if trimmed.starts_with('/') {
        let path = Path::new(trimmed);
        let slash_count = trimmed.matches('/').count();
        let has_extension = path.extension().is_some();
        let looks_like_dir = trimmed.ends_with('/');
        let under_known_root = [
            "/tmp/",
            "/private/tmp/",
            "/Users/",
            "/private/var/",
            "/var/",
            "/etc/",
            "/usr/",
        ]
        .iter()
        .any(|prefix| trimmed.starts_with(prefix));
        if (slash_count < 2 && !has_extension && !looks_like_dir)
            || (!path.exists() && !has_extension && !looks_like_dir && !under_known_root)
        {
            return false;
        }
    }
    if trimmed.contains('`') {
        return false;
    }
    true
}

fn relevant_file_handle_rank(kind: &str) -> usize {
    match kind {
        "target_directory" => 5,
        "target_file" => 4,
        "source_file" => 3,
        "document" => 2,
        "data_or_log" => 1,
        _ => 0,
    }
}

fn extract_relevant_file_handles(text: &str, step_revision: &str) -> Vec<RelevantFileHandle> {
    let mut handles: Vec<RelevantFileHandle> = Vec::new();
    let cwd = std::env::current_dir().ok();
    for line in text.lines() {
        let trimmed = line.trim();
        if !(trimmed.starts_with('-')
            || trimmed.starts_with("目标文件")
            || trimmed.starts_with("目标目录"))
        {
            continue;
        }
        for token in trimmed.split_whitespace() {
            let candidate = token
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('-')
                .trim_matches('：')
                .trim_end_matches(['，', ',', '。', '.', ';', '；', ')', '）', ']']);
            let candidate = strip_to_path_start(candidate);
            let candidate = candidate
                .rsplit_once(['：', ':'])
                .map(|(_, suffix)| suffix)
                .filter(|suffix| suffix.contains('/'))
                .unwrap_or(candidate);
            if candidate.is_empty()
                || candidate == "/"
                || !candidate.contains('/')
                || !is_probably_filesystem_hint(candidate)
            {
                continue;
            }
            if !(candidate.ends_with(".rs")
                || candidate.ends_with(".md")
                || candidate.starts_with('/')
                || candidate.starts_with("./")
                || candidate.starts_with("../"))
            {
                continue;
            }
            let candidate = normalize_relevant_file_hint(candidate, cwd.as_deref())
                .unwrap_or_else(|| candidate.to_string());
            let kind = classify_relevant_file_handle(&candidate, trimmed);
            if let Some(existing) = handles
                .iter_mut()
                .find(|existing| existing.path == candidate)
            {
                if relevant_file_handle_rank(&kind) > relevant_file_handle_rank(&existing.kind) {
                    existing.kind = kind.clone();
                    existing.why_relevant = build_file_handle_relevance(&kind, trimmed, &candidate);
                    existing.step_revision = step_revision.to_string();
                }
                continue;
            }
            if !handles.iter().any(|existing| existing.path == candidate) {
                handles.push(RelevantFileHandle {
                    path: candidate.clone(),
                    kind: kind.clone(),
                    source: "boss_step_objective".to_string(),
                    freshness: "current".to_string(),
                    why_relevant: build_file_handle_relevance(&kind, trimmed, &candidate),
                    step_revision: step_revision.to_string(),
                });
            }
        }
    }
    handles
}

fn normalize_relevant_file_hint(candidate: &str, cwd: Option<&Path>) -> Option<String> {
    let cwd = cwd?;
    let candidate_path = Path::new(candidate);
    if candidate_path.is_absolute() {
        return Some(candidate.to_string());
    }

    let mut attempts: Vec<PathBuf> = vec![cwd.join(candidate_path)];
    if candidate.starts_with("src/") {
        attempts.push(cwd.join("RustAgent/Agent").join(candidate_path));
    }
    if let Some(rest) = candidate.strip_prefix("../docs/") {
        attempts.push(cwd.join("RustAgent/docs").join(rest));
    }

    for attempt in attempts {
        if attempt.exists() {
            if let Ok(relative) = attempt.strip_prefix(cwd) {
                return Some(relative.to_string_lossy().replace('\\', "/"));
            }
            return Some(attempt.to_string_lossy().replace('\\', "/"));
        }
    }

    None
}

fn summarize_acceptance_items(step: &BossPlanStep) -> String {
    if step.acceptance.is_empty() {
        "- none".to_string()
    } else {
        step.acceptance
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn collect_recent_decisions(plan: &BossPlan, current_step_id: usize) -> Vec<String> {
    let mut decisions = plan
        .steps
        .iter()
        .filter(|step| step.id < current_step_id)
        .filter_map(|step| {
            if let Some(summary) = step.last_review_summary.as_ref() {
                Some(format!("step {} review: {}", step.id, summary))
            } else if step.status == BossPlanStepStatus::Completed {
                Some(format!("step {} completed: {}", step.id, step.objective()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if decisions.len() > 3 {
        decisions = decisions.split_off(decisions.len() - 3);
    }
    decisions
}

fn collect_target_files(relevant_file_handles: &[RelevantFileHandle]) -> Vec<String> {
    let mut target_files = Vec::new();
    for handle in relevant_file_handles {
        if matches!(
            handle.kind.as_str(),
            "target_file" | "target_directory" | "source_file" | "document"
        ) && !target_files.iter().any(|path| path == &handle.path)
        {
            target_files.push(handle.path.clone());
        }
    }
    target_files
}

fn collect_target_artifacts(step: &BossPlanStep, target_files: &[String]) -> Vec<TargetArtifact> {
    let mut artifacts = Vec::new();
    for expectation in extract_artifact_expectations(step.objective()) {
        let kind = match expectation.kind {
            crate::core::boss_acceptance::BossArtifactKind::File => "file",
            crate::core::boss_acceptance::BossArtifactKind::Directory => "directory",
        };
        artifacts.push(TargetArtifact {
            path: expectation.path.display().to_string(),
            kind: kind.to_string(),
            required_state: "exists_non_empty".to_string(),
            source: "artifact_expectation".to_string(),
        });
    }
    for path in target_files {
        if !artifacts.iter().any(|artifact| artifact.path == *path) {
            artifacts.push(TargetArtifact {
                path: path.clone(),
                kind: if path.ends_with('/') {
                    "directory".to_string()
                } else {
                    "file".to_string()
                },
                required_state: "referenced_for_step".to_string(),
                source: "target_file_handle".to_string(),
            });
        }
    }
    artifacts
}

fn default_allowed_tools() -> Vec<String> {
    vec![
        "Read".into(),
        "Edit".into(),
        "Glob".into(),
        "Grep".into(),
        "LS".into(),
        "Bash".into(),
    ]
}

fn render_workspace_capability_scope() -> String {
    "inherited_runtime_scope".to_string()
}

fn collect_blocked_items(step: &BossPlanStep) -> Vec<String> {
    let mut blocked = Vec::new();
    if matches!(step.status, BossPlanStepStatus::WaitingForApproval) {
        blocked.push("waiting for approval before implementation may proceed".to_string());
    }
    if matches!(
        step.status,
        BossPlanStepStatus::Rejected | BossPlanStepStatus::Failed
    ) {
        if let Some(summary) = step
            .last_review_summary
            .as_ref()
            .filter(|text| !text.trim().is_empty())
        {
            blocked.push(summary.clone());
        }
    }
    blocked
}

fn collect_recent_local_facts(step: &BossPlanStep, limit: usize) -> Vec<String> {
    let mut facts = Vec::new();
    for (idx, record) in step.tool_execution_records.iter().enumerate().rev() {
        match record.tool_name.as_str() {
            "Read" => {
                if let Some(path) = observable_path_local(record) {
                    facts.push(format!("recent_read path={path}"));
                }
            }
            "Edit" | "Write" => {
                if let Some(path) = observable_path_local(record) {
                    facts.push(format!("recent_edit path={path}"));
                }
            }
            "Bash" => {
                if let Some(command) = observable_bash_command_local(record) {
                    facts.push(format!(
                        "recent_test command={}",
                        trim_runtime_excerpt(&command, 120)
                    ));
                }
            }
            _ => {}
        }
        if let Some(output_fact) = recent_large_output_fact(step.id, idx, record) {
            facts.push(output_fact);
        }
        if facts.len() >= limit {
            break;
        }
    }
    facts.reverse();
    facts
}

fn recent_large_output_fact(
    step_id: usize,
    record_index: usize,
    record: &ToolExecutionRecord,
) -> Option<String> {
    let detail = record.detail.as_ref()?.trim();
    if detail.is_empty() {
        return None;
    }
    let is_large = matches!(record.kind, ToolExecutionOutcomeKind::ResultTooLarge)
        || detail.len() > 160
        || detail.lines().count() > 4;
    if !is_large {
        return None;
    }
    let source_event_id = format!("tool-output:{step_id}:{record_index}");
    let ref_id = format!("output:step{step_id}:{record_index}");
    Some(format!(
        "recent_output_ref ref={ref_id} source_event_id={source_event_id} excerpt={}",
        trim_runtime_excerpt(detail, 140)
    ))
}

fn render_recent_local_facts_section(facts: &[String]) -> String {
    if facts.is_empty() {
        String::new()
    } else {
        let mut lines = vec!["recent_local_facts:".to_string()];
        for fact in facts {
            lines.push(format!("  - {fact}"));
        }
        format!("\n{}", lines.join("\n"))
    }
}

fn trim_runtime_excerpt(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let mut iter = trimmed.chars();
    let excerpt = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{excerpt}...")
    } else {
        excerpt
    }
}

fn observable_path_local(record: &ToolExecutionRecord) -> Option<String> {
    record.observable_input.as_ref().and_then(|input| {
        serde_json::from_str::<serde_json::Value>(&input.value)
            .ok()
            .and_then(|value| {
                value
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
    })
}

fn observable_bash_command_local(record: &ToolExecutionRecord) -> Option<String> {
    record.observable_input.as_ref().and_then(|input| {
        serde_json::from_str::<serde_json::Value>(&input.value)
            .ok()
            .and_then(|value| {
                value
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
    })
}

fn store_step_result_diff(step: &mut BossPlanStep, primary: &str, fallback: Option<&str>) {
    let candidate = if primary.trim().is_empty() {
        fallback.unwrap_or_default()
    } else {
        primary
    };
    if candidate.trim().is_empty() {
        return;
    }
    let stored = if is_verification_first_continuation(step) {
        shape_verification_first_result_text(step, candidate.trim())
    } else {
        candidate.trim().to_string()
    };
    step.result_diff = Some(stored);
}

fn sync_step_tool_execution_records(
    step: &mut BossPlanStep,
    tasks: Option<&TaskManager>,
    task_id: &str,
) {
    step.tool_execution_records = tasks
        .map(|manager| manager.tool_execution_records(task_id))
        .unwrap_or_default();
}

fn append_step_runtime_record(step: &mut BossPlanStep, record: ToolExecutionRecord) {
    let duplicate = step.tool_execution_records.iter().any(|existing| {
        existing.tool_name == record.tool_name
            && existing.kind == record.kind
            && existing.summary == record.summary
            && existing.detail == record.detail
            && existing.observable_input == record.observable_input
    });
    if !duplicate {
        step.tool_execution_records.push(record);
    }
}

fn observable_input_json(value: serde_json::Value) -> ObservableInput {
    ObservableInput {
        value: value.to_string(),
        source: ObservableInputSource::Raw,
    }
}

fn append_review_runtime_record(
    step: &mut BossPlanStep,
    verdict: &str,
    summary: &str,
    correction: Option<&str>,
) {
    append_step_runtime_record(
        step,
        ToolExecutionRecord {
            tool_name: "BossReview".into(),
            outcome: "Text".into(),
            kind: ToolExecutionOutcomeKind::Success,
            summary: format!("Boss review verdict: {verdict}"),
            detail: Some(summary.to_string()),
            pending_approval: None,
            report_modifier: ToolReportModifier::None,
            observable_input: Some(observable_input_json(json!({
                "step_id": step.id,
                "verdict": verdict,
                "correction": correction,
            }))),
            batch_context: ToolBatchContext {
                batch_index: 0,
                batch_size: 1,
                executed_in_batch: false,
            },
        },
    );
}

fn append_artifact_verification_runtime_records(
    step: &mut BossPlanStep,
    status: &str,
    summary_prefix: &str,
) {
    for expectation in extract_artifact_expectations(step.objective()) {
        let path = expectation.path.to_string_lossy().to_string();
        let kind = match expectation.kind {
            crate::core::boss_acceptance::BossArtifactKind::File => "file",
            crate::core::boss_acceptance::BossArtifactKind::Directory => "directory",
        };
        let summary = format!("{summary_prefix}: {path}");
        append_step_runtime_record(
            step,
            ToolExecutionRecord {
                tool_name: "ArtifactVerify".into(),
                outcome: "Text".into(),
                kind: if status == "missing_or_invalid" {
                    ToolExecutionOutcomeKind::Interrupted
                } else {
                    ToolExecutionOutcomeKind::Success
                },
                summary,
                detail: Some(format!("artifact verification status={status} path={path}")),
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: Some(observable_input_json(json!({
                    "step_id": step.id,
                    "path": path,
                    "kind": kind,
                    "status": status,
                }))),
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            },
        );
    }
}

fn build_step_review_summary(
    step: &BossPlanStep,
    source: &str,
    details: &[(&str, &str)],
) -> String {
    if is_verification_first_continuation(step) {
        return build_brief_verification_review_summary(step, source);
    }
    let mut sections = vec![
        format!("{source} reported boss step {} complete.", step.id),
        format!("Objective: {}", step.objective()),
        "Acceptance:".to_string(),
        summarize_acceptance_items(step),
    ];
    for (label, value) in details {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            sections.push(format!("{label}: {trimmed}"));
        }
    }
    sections.join("\n")
}

impl BossCoordinator {
    /// State-only constructor — no callbacks wired. Use in tests or low-level assembly only.
    /// Production code must call `new_with_runtime_owner` followed by
    /// `bootstrap_actor_registry_with_app_state`.
    #[doc(hidden)]
    pub fn new() -> Self {
        Self::new_with_runtime_owner(Arc::new(BossRuntimeOwner::default()))
    }

    pub fn new_with_runtime_owner(runtime_owner: Arc<BossRuntimeOwner>) -> Self {
        Self {
            status: Arc::new(RwLock::new(BossStatus::default())),
            plan: Arc::new(RwLock::new(None)),
            session: Arc::new(RwLock::new(None)),
            actor_registry: Arc::new(RwLock::new(None)),
            auto_advance_app_state: Arc::new(RwLock::new(None)),
            routed_step_metadata: Arc::new(RwLock::new(std::collections::HashMap::new())),
            runtime_key: Arc::new(RwLock::new(None)),
            runtime_owner,
            lism_policy: Arc::new(RwLock::new(BossLisMPolicy::Inherit)),
            worker_lism_policy: Arc::new(RwLock::new(WorkerLisMPolicy::ForceOn)),
            full_worker_dispatch_fallback_enabled: Arc::new(RwLock::new(true)),
            lism_ab_sink: None,
        }
    }

    /// Returns the raw pointer address of the coordinator's `BossRuntimeOwner` Arc.
    /// Test-only seam for verifying owner identity without exposing the Arc itself.
    #[doc(hidden)]
    pub fn runtime_owner_ptr(&self) -> usize {
        Arc::as_ptr(&self.runtime_owner) as usize
    }

    /// Test-only seam: exposes `parse_a_review_decision` as a public associated function.
    #[doc(hidden)]
    pub fn parse_a_review_decision_pub(
        response: &str,
        summary: &str,
    ) -> crate::core::boss_actor_runtime::ReviewDecision {
        Self::parse_a_review_decision(response, summary)
    }

    /// Test-only seam: builds and returns the ReviewFn for this coordinator.
    #[doc(hidden)]
    pub fn review_fn_for_test(&self) -> crate::core::boss_actor_runtime::ReviewFn {
        Self::build_review_fn(self)
    }

    /// Test-only seam: exposes `record_b_session_id` for direct state mutation in tests.
    #[doc(hidden)]
    pub async fn record_b_session_id_pub(&self, task_id: &str) {
        self.record_b_session_id(task_id).await;
    }

    /// Test-only seam: pre-seeds designer_a.session_id so ensure_a_session skips spawning.
    #[doc(hidden)]
    pub async fn record_a_session_id_pub(&self, task_id: &str) {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.designer_a.session_id = task_id.to_string();
            session.designer_a.task_id = Some(task_id.to_string());
            session.designer_a.status = crate::core::boss_state::BossActorStatus::Active;
        }
    }

    /// Test-only seam: reads `executor_b.session_id` for assertion in tests.
    #[doc(hidden)]
    pub async fn b_session_id(&self) -> String {
        let guard = self.session.read().await;
        guard
            .as_ref()
            .map(|s| s.executor_b.session_id.clone())
            .unwrap_or_default()
    }

    /// Test-only seam: reads `executor_b.task_id` for assertion in tests.
    #[doc(hidden)]
    pub async fn b_task_id(&self) -> Option<String> {
        let guard = self.session.read().await;
        guard.as_ref().and_then(|s| s.executor_b.task_id.clone())
    }

    #[doc(hidden)]
    pub async fn current_step_worker_task_id(&self) -> Option<String> {
        let current_step = self.status.read().await.current_step?;
        let plan = self.plan.read().await;
        plan.as_ref()?
            .steps
            .iter()
            .find(|step| step.id == current_step)
            .and_then(|step| step.worker_task_id.clone())
    }

    /// Full-mode constructor — wires A+B callbacks immediately.
    /// Prefer `BossRuntimeHost::build_coordinator` in production so the host's
    /// `BossRuntimeOwner` is used. This method is the building block used by the host.
    pub async fn new_with_app_state(
        runtime_owner: Arc<BossRuntimeOwner>,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> Self {
        let coordinator = Self::new_with_runtime_owner(runtime_owner);
        coordinator
            .bootstrap_actor_registry_with_app_state(app_state)
            .await;
        coordinator
    }

    pub async fn attach_app_state_for_report_testing(
        &self,
        app_state: Arc<crate::state::app_state::AppState>,
    ) {
        let mut auto = self.auto_advance_app_state.write().await;
        *auto = Some(app_state);
    }

    pub async fn current_runtime_key(&self) -> Option<String> {
        self.runtime_key.read().await.clone()
    }

    pub async fn runtime_is_closed_for_testing(&self, key: &str) -> bool {
        self.runtime_owner
            .get_runtime(key)
            .map(|runtime| runtime.is_closed())
            .unwrap_or(true)
    }

    pub fn shutdown_all_runtime_instances(&self) {
        self.runtime_owner.shutdown_all_runtimes();
    }

    pub fn shutdown_runtime_owner(&self) {
        self.runtime_owner.shutdown_owner();
    }

    pub fn restart_runtime_owner(&self) {
        self.runtime_owner.restart_owner();
    }

    pub async fn has_control_runtime(&self) -> bool {
        self.runtime_key
            .read()
            .await
            .as_ref()
            .and_then(|key| self.runtime_owner.get_runtime(key))
            .is_some()
    }

    pub async fn ensure_control_runtime(&self) {
        if self.runtime_owner.is_closed() {
            return;
        }
        let mut runtime_key = self.runtime_key.write().await;
        if runtime_key
            .as_ref()
            .and_then(|key| self.runtime_owner.get_runtime(key))
            .is_some()
        {
            return;
        }
        let plan_id = self
            .plan
            .read()
            .await
            .as_ref()
            .map(|plan| plan.plan_id.clone())
            .unwrap_or_else(|| "boss-default".into());
        let key = self.runtime_owner.fresh_runtime_key(&plan_id);
        let runtime = BossControlRuntime::spawn(self.clone_for_runtime());
        self.runtime_owner.bind_runtime(key.clone(), runtime);
        *runtime_key = Some(key);
    }

    pub async fn rebind_control_runtime(&self) {
        let mut runtime_key = self.runtime_key.write().await;
        if let Some(key) = runtime_key.as_ref() {
            let _ = self.runtime_owner.shutdown_runtime(key);
        }
        let plan_id = self
            .plan
            .read()
            .await
            .as_ref()
            .map(|plan| plan.plan_id.clone())
            .unwrap_or_else(|| "boss-default".into());
        let key = self.runtime_owner.fresh_runtime_key(&plan_id);
        let runtime = BossControlRuntime::spawn(self.clone_for_runtime());
        self.runtime_owner.bind_runtime(key.clone(), runtime);
        *runtime_key = Some(key);
    }

    async fn send_control_request(
        &self,
        request: BossControlRequest,
        tasks: Arc<TaskManager>,
        dispatcher: NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        self.ensure_control_runtime().await;
        if self.runtime_owner.is_closed() {
            anyhow::bail!("boss runtime owner is closed");
        }
        let key = self
            .runtime_key
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("boss control runtime key unavailable"))?;
        let runtime = self
            .runtime_owner
            .get_runtime(&key)
            .ok_or_else(|| anyhow::anyhow!("boss control runtime unavailable"))?;
        runtime.request(request, tasks, dispatcher).await
    }

    fn clone_for_runtime(&self) -> Self {
        Self {
            status: self.status.clone(),
            plan: self.plan.clone(),
            session: self.session.clone(),
            actor_registry: self.actor_registry.clone(),
            auto_advance_app_state: self.auto_advance_app_state.clone(),
            routed_step_metadata: self.routed_step_metadata.clone(),
            runtime_key: self.runtime_key.clone(),
            runtime_owner: self.runtime_owner.clone(),
            lism_policy: self.lism_policy.clone(),
            worker_lism_policy: self.worker_lism_policy.clone(),
            full_worker_dispatch_fallback_enabled: self
                .full_worker_dispatch_fallback_enabled
                .clone(),
            lism_ab_sink: self.lism_ab_sink.clone(),
        }
    }

    /// If the file doesn't exist, it falls back to a fresh coordinator.
    /// State-only restore — no callbacks wired. Prefer `restore_or_init_with_app_state` in production.
    #[doc(hidden)]
    pub async fn restore_or_init(path: &std::path::Path) -> anyhow::Result<Self> {
        Self::restore_or_init_with_owner(path, Arc::new(BossRuntimeOwner::default())).await
    }

    /// Restore (or init) and immediately bootstrap with full A+B callbacks.
    /// After this call the registry is in full mode — no lazy upgrade on first production entry.
    /// Full-mode restore — restores from file (or creates fresh) and bootstraps A+B callbacks.
    /// Prefer `BossRuntimeHost::restore_or_init_coordinator` in production so the host's
    /// `BossRuntimeOwner` is used. This static helper creates a throwaway owner.
    #[doc(hidden)]
    pub async fn restore_or_init_with_app_state(
        path: &std::path::Path,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Self> {
        let coordinator =
            Self::restore_or_init_with_owner(path, Arc::new(BossRuntimeOwner::default())).await?;
        coordinator
            .bootstrap_actor_registry_with_app_state(app_state)
            .await;
        Ok(coordinator)
    }

    pub async fn restore_or_init_with_owner(
        path: &std::path::Path,
        runtime_owner: Arc<BossRuntimeOwner>,
    ) -> anyhow::Result<Self> {
        let coordinator = Self::new_with_runtime_owner(runtime_owner);

        if path.exists() {
            let loaded_plan = load_plan(path).await?;

            // Determine stage based on plan progress
            let mut stage = BossStage::Documentation;
            if loaded_plan.accepted_by_user {
                let all_completed =
                    !loaded_plan.steps.is_empty() && loaded_plan.steps.iter().all(|s| s.completed);
                if all_completed {
                    stage = BossStage::Completed;
                } else {
                    stage = BossStage::Execution;
                }
            }

            // Figure out the current step (first uncompleted)
            let mut current_step = None;
            let total_steps = Some(loaded_plan.steps.len());
            if loaded_plan.accepted_by_user {
                current_step = loaded_plan
                    .steps
                    .iter()
                    .find(|s| !s.completed)
                    .map(|s| s.id);
            }

            {
                let mut status = coordinator.status.write().await;
                status.stage = stage;
                status.planning_file = Some(path.to_string_lossy().into_owned());
                status.current_step = current_step;
                status.total_steps = total_steps;
            }

            {
                let mut plan_guard = coordinator.plan.write().await;
                *plan_guard = Some(loaded_plan.clone());
            }

            // Init actor session — prefer persisted snapshot so A/B identity
            // (session_id / task_id) survives restart. Fallback to deterministic
            // placeholder when no snapshot exists (new plan or old plan file).
            {
                let mut session_guard = coordinator.session.write().await;
                *session_guard = Some(
                    loaded_plan
                        .session_snapshot
                        .clone()
                        .unwrap_or_else(|| BossSession::from_plan_id(&loaded_plan.plan_id, stage)),
                );
            }

            // Bootstrap actor runtimes for A and B.
            coordinator.bootstrap_actor_registry().await;
        } else {
            let mut status = coordinator.status.write().await;
            status.planning_file = Some(path.to_string_lossy().into_owned());
        }

        Ok(coordinator)
    }

    pub async fn get_stage(&self) -> BossStage {
        self.status.read().await.stage
    }

    /// Ensures a BossSession exists for the given plan_id, creating one if absent.
    /// Idempotent: if a session already exists for the same plan_id it is returned unchanged.
    pub async fn ensure_actor_session(&self, plan_id: &str, stage: BossStage) {
        let mut guard = self.session.write().await;
        if guard.as_ref().map(|s| s.plan_id.as_str()) != Some(plan_id) {
            *guard = Some(BossSession::from_plan_id(plan_id, stage));
        }
    }

    /// Spawn fresh A and B actor runtimes (state-only, no execution callback).
    /// Low-level / test-only. Production code must use `bootstrap_actor_registry_with_app_state`.
    #[doc(hidden)]
    pub async fn bootstrap_actor_registry(&self) {
        let registry = BossActorRegistry::bootstrap();
        let mut guard = self.actor_registry.write().await;
        if let Some(old) = guard.take() {
            old.shutdown_all();
        }
        *guard = Some(registry);
    }

    /// Bootstrap A and B with all callbacks wired in one shot.
    /// No-op if the registry already has both executor and A callbacks.
    /// This is the preferred production path — call once when AppState is available.
    pub async fn bootstrap_actor_registry_with_app_state(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) {
        let already_full = {
            let guard = self.actor_registry.read().await;
            guard
                .as_ref()
                .map(|r| r.has_executor && r.has_a_callbacks)
                .unwrap_or(false)
        };
        if already_full {
            return;
        }
        // Store app_state for auto path (finalize/approval may call it without app_state param).
        {
            let mut guard = self.auto_advance_app_state.write().await;
            *guard = Some(app_state.clone());
        }
        let exec_fn = Self::build_exec_fn(self, app_state);
        let spec_review_fn = Self::build_spec_review_fn(self, app_state);
        let review_fn = Self::build_review_fn(self);
        let doc_fn = Self::build_doc_fn(self, app_state);
        let registry = crate::core::boss_actor_runtime::bootstrap_with_all_callbacks(
            exec_fn,
            spec_review_fn,
            review_fn,
            doc_fn,
        );
        let mut guard = self.actor_registry.write().await;
        if let Some(old) = guard.take() {
            old.shutdown_all();
        }
        *guard = Some(registry);
    }

    fn build_exec_fn(
        coordinator: &Self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> crate::core::boss_actor_runtime::ExecutionFn {
        let c = coordinator.clone_for_runtime();
        let app = app_state.clone();
        Arc::new(move |payload: String| {
            let c = c.clone_for_runtime();
            let app = app.clone();
            Box::pin(async move {
                {
                    let mut guard = c.status.write().await;
                    guard.last_b_dispatch_payload = Some(payload.clone());
                }
                // Invoke AgentTool (Spawn or Continue — payload already encodes which).
                // Write the returned task_id back to executor_b so subsequent dispatches
                // can reuse the same B session via Continue.
                match c.invoke_agent_tool_with_task_id(&app, &payload).await {
                    Ok(task_id) => {
                        c.record_b_session_id(&task_id).await;
                        Ok(task_id)
                    }
                    Err(e) => Err(e),
                }
            })
        })
    }

    /// Build B's spec-review callback for the Documentation stage.
    /// B receives the spec, calls its LLM session, and returns review feedback.
    fn build_spec_review_fn(
        coordinator: &Self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> crate::core::boss_actor_runtime::SpecReviewFn {
        let c = coordinator.clone_for_runtime();
        let app = app_state.clone();
        Arc::new(move |spec: String| {
            let c = c.clone_for_runtime();
            let app = app.clone();
            Box::pin(async move {
                c.ensure_b_session(&app, 0).await;
                let msg = format!(
                    "Please review the following spec for feasibility, risk, and testability. \
                     Respond with LGTM if acceptable, or FEEDBACK: <your feedback> if changes are needed.\n\n{spec}"
                );
                match c.ask_b_session(&app, msg).await {
                    Ok(response) => Ok(response),
                    Err(_) => Ok("LGTM".to_string()),
                }
            })
        })
    }

    /// Ask A to draft a technical spec from `task_description`.
    /// Calls `ensure_a_session` then `ask_a_session`; returns A's response as the draft spec.
    /// Returns `Err` if A's session is unavailable or times out.
    pub async fn draft_spec_with_a(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        task_description: &str,
    ) -> anyhow::Result<String> {
        self.ensure_a_session(app_state).await;
        let msg = format!(
            "Draft a technical specification for the following task. \
             Include objectives, acceptance criteria, and a high-level approach. \
             Task: {task_description}"
        );
        self.ask_a_session(app_state, msg).await
    }

    /// Write a real B task id back to BossSession.executor_b after a successful spawn/continue.
    async fn record_b_session_id(&self, task_id: &str) {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.session_id = task_id.to_string();
            session.executor_b.task_id = Some(task_id.to_string());
            session.executor_b.status = crate::core::boss_state::BossActorStatus::Active;
        }
    }

    async fn record_step_dispatch_task_id(&self, step_id: usize, task_id: &str) {
        {
            let mut guard = self.session.write().await;
            if let Some(session) = guard.as_mut() {
                session.executor_b.task_id = Some(task_id.to_string());
                session.executor_b.status = BossActorStatus::Active;
            }
        }
        {
            let mut plan = self.plan.write().await;
            if let Some(plan) = plan.as_mut() {
                if let Some(step) = plan.steps.iter_mut().find(|step| step.id == step_id) {
                    step.worker_task_id = Some(task_id.to_string());
                }
            }
        }
    }

    /// Ensure Executor B has a real LLM session. On first call, spawns a B session via
    /// AgentTool and writes the task id back to BossSession.executor_b.session_id.
    /// Subsequent calls are no-ops if the session id is already a real task id.
    async fn ensure_b_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        step_id: usize,
    ) {
        if app_state.permission_context.task_manager.is_none() {
            return;
        }
        let is_placeholder = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| {
                    let placeholder = format!("boss-{}-b", s.plan_id);
                    s.executor_b.session_id == placeholder || s.executor_b.session_id.is_empty()
                })
                .unwrap_or(true)
        };
        if !is_placeholder {
            return;
        }

        let parent_session_id = app_state.active_session_id.clone();
        let b_actor_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.executor_b.actor_id.clone())
                .unwrap_or_else(|| "boss-unknown-b".into())
        };
        let spawn_build = match self
            .build_step_spawn_payload_internal(step_id, &parent_session_id, &b_actor_id)
            .await
        {
            Ok(build) => build,
            Err(_) => return,
        };
        self.record_b_assignment_contract(
            &spawn_build.assignment_fingerprint,
            &spawn_build.plan_version,
            &spawn_build.step_revision,
        )
        .await;
        let payload = spawn_build.payload;

        if let Ok(task_id) = self
            .invoke_agent_tool_with_task_id(app_state, &payload)
            .await
        {
            self.record_b_session_id(&task_id).await;
        }
    }

    fn build_review_fn(coordinator: &Self) -> crate::core::boss_actor_runtime::ReviewFn {
        let c = coordinator.clone_for_runtime();
        Arc::new(
            move |step_id, accepted, summary: String, correction: Option<String>| {
                let c = c.clone_for_runtime();
                Box::pin(async move {
                    let app_state = {
                        let guard = c.auto_advance_app_state.read().await;
                        guard.clone()
                    };
                    if let Some(app) = app_state {
                        c.ensure_a_session(&app).await;
                        let verdict_hint = if accepted { "accepted" } else { "rejected" };
                        let msg = match correction.as_deref() {
                            Some(corr) => format!(
                                "Review step {step_id}: coordinator verdict={verdict_hint}. Summary: {summary}. Correction: {corr}. Please respond with ACCEPT, REJECT, or REPLAN_STEP. If REJECT include CORRECTION: <your correction>. If REPLAN_STEP include REASON: <why this step needs replanning>."
                            ),
                            None => format!(
                                "Review step {step_id}: coordinator verdict={verdict_hint}. Summary: {summary}. Please respond with ACCEPT, REJECT, or REPLAN_STEP. If REJECT include CORRECTION: <your correction>. If REPLAN_STEP include REASON: <why this step needs replanning>."
                            ),
                        };
                        match c.ask_a_session(&app, msg).await {
                            Ok(response) => {
                                let decision = Self::parse_a_review_decision(&response, &summary);
                                c.apply_review_verdict(step_id, &decision).await?;
                                return Ok(decision);
                            }
                            Err(_) => {}
                        }
                    }
                    let decision = if accepted {
                        crate::core::boss_actor_runtime::ReviewDecision::Accept {
                            summary: summary.clone(),
                        }
                    } else {
                        crate::core::boss_actor_runtime::ReviewDecision::Correct {
                            summary: summary.clone(),
                            correction,
                        }
                    };
                    c.apply_review_verdict(step_id, &decision).await?;
                    Ok(decision)
                })
            },
        )
    }

    fn build_doc_fn(
        coordinator: &Self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> crate::core::boss_actor_runtime::DocumentationFn {
        let c = coordinator.clone_for_runtime();
        let app = app_state.clone();
        Arc::new(move |signal: String| {
            let c = c.clone_for_runtime();
            let app = app.clone();
            Box::pin(async move {
                c.ensure_a_session(&app).await;
                let msg = format!(
                    "Documentation signal: {signal}. Please acknowledge with ACCEPT or provide CORRECTION: <feedback>."
                );
                let effective_signal = match c.ask_a_session(&app, msg).await {
                    Ok(response) => {
                        let decision = Self::parse_a_review_decision(&response, &signal);
                        match decision {
                            crate::core::boss_actor_runtime::ReviewDecision::Accept { .. } => {
                                signal.clone()
                            }
                            crate::core::boss_actor_runtime::ReviewDecision::Correct {
                                correction,
                                ..
                            } => correction.unwrap_or_else(|| signal.clone()),
                            crate::core::boss_actor_runtime::ReviewDecision::ReplanStep {
                                reason,
                                ..
                            } => reason,
                        }
                    }
                    Err(_) => signal.clone(),
                };
                c.apply_documentation_signal(&app, &effective_signal).await
            })
        })
    }

    /// Ensure actor runtimes exist with a real execution callback wired to B.
    /// If the registry already has an executor, this is a no-op.
    #[deprecated(note = "use bootstrap_actor_registry_with_app_state directly")]
    pub async fn ensure_actor_registry_with_executor(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) {
        // Prefer the full bootstrap — wires A and B in one shot.
        self.bootstrap_actor_registry_with_app_state(app_state)
            .await;
    }

    /// Ensure actor runtimes exist; bootstrap if not yet initialized.
    /// State-only fallback — no callbacks wired. Prefer `bootstrap_actor_registry_with_app_state`.
    #[doc(hidden)]
    pub async fn ensure_actor_registry(&self) {
        let needs_bootstrap = self.actor_registry.read().await.is_none();
        if needs_bootstrap {
            self.bootstrap_actor_registry().await;
        }
    }

    /// Ensure A's callbacks are wired using the stored auto_advance_app_state.
    /// No-op if already fully bootstrapped or if no app_state is available.
    pub async fn ensure_actor_registry_with_a_callbacks_auto(&self) {
        let app_state = self.auto_advance_app_state.read().await.clone();
        if let Some(app) = app_state {
            self.bootstrap_actor_registry_with_app_state(&app).await;
        } else {
            self.ensure_actor_registry().await;
        }
    }

    /// Ensure A's review and documentation callbacks are wired.
    /// Delegates to bootstrap_actor_registry_with_app_state — no-op if already fully bootstrapped.
    #[deprecated(note = "use bootstrap_actor_registry_with_app_state directly")]
    pub async fn ensure_actor_registry_with_a_callbacks(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) {
        self.bootstrap_actor_registry_with_app_state(app_state)
            .await;
    }

    /// Returns a point-in-time snapshot of all tracked actor handles (A, B, and children).
    pub async fn actor_registry_snapshot(&self) -> Vec<BossActorHandle> {
        let guard = self.session.read().await;
        let Some(session) = guard.as_ref() else {
            return Vec::new();
        };
        let mut handles = vec![session.designer_a.clone(), session.executor_b.clone()];
        handles.extend(session.active_children.iter().cloned());
        handles
    }

    /// Updates the status of a tracked actor by actor_id.
    pub async fn update_actor_status(&self, actor_id: &str, status: BossActorStatus) {
        let mut guard = self.session.write().await;
        let Some(session) = guard.as_mut() else {
            return;
        };
        for handle in std::iter::once(&mut session.designer_a)
            .chain(std::iter::once(&mut session.executor_b))
            .chain(session.active_children.iter_mut())
        {
            if handle.actor_id == actor_id {
                handle.status = status;
                handle.last_snapshot = Some(std::time::SystemTime::now());
                return;
            }
        }
    }

    /// Enforces a strict DAG state transition to prevent invalid lifecycle jumps.
    pub async fn transition_to(&self, new_stage: BossStage) -> anyhow::Result<()> {
        let mut status = self.status.write().await;
        // Verify valid transition
        let valid = match (status.stage, new_stage) {
            (BossStage::Documentation, BossStage::WaitingForApproval) => true,
            (BossStage::WaitingForApproval, BossStage::Execution) => true,
            (BossStage::WaitingForApproval, BossStage::Documentation) => true, // Rejected by user
            (BossStage::Execution, BossStage::Completed) => true,
            (BossStage::Documentation, BossStage::Documentation) => true, // Re-entering valid
            (BossStage::Execution, BossStage::Documentation) => true,     // Fallback/Fatal failure
            _ => false,
        };

        if !valid {
            anyhow::bail!(
                "Invalid BossStage transition from {:?} to {:?}",
                status.stage,
                new_stage
            );
        }

        status.stage = new_stage;
        Ok(())
    }

    /// Returns the default path for the immutable planning cache.
    pub fn default_plan_path(root: &std::path::Path) -> std::path::PathBuf {
        root.join(crate::bootstrap::config_root::PRIMARY_CONFIG_DIR)
            .join("boss")
            .join("planning.json")
    }

    /// Records one Documentation-stage red/blue loop pass.
    ///
    /// A drafts the spec, B reviews feasibility/risk/testability, then A revises.
    /// The revised spec becomes the immutable planning content and the coordinator
    /// transitions to `WaitingForApproval`.
    pub async fn finalize_documentation_loop(
        &self,
        draft_spec: &str,
        review_feedback: &str,
        revision_notes: &str,
        final_document_spec: &str,
        final_pseudo_code: &str,
    ) -> anyhow::Result<()> {
        // If no draft_spec was supplied, ask A to generate one from the plan's task_description.
        let effective_draft_spec: String;
        let draft_spec = if draft_spec.is_empty() {
            let task_description = {
                let guard = self.plan.read().await;
                guard
                    .as_ref()
                    .map(|p| p.task_description.clone())
                    .unwrap_or_default()
            };
            let app_state = self.auto_advance_app_state.read().await.clone();
            effective_draft_spec = if let Some(app) = app_state {
                self.draft_spec_with_a(&app, &task_description).await?
            } else {
                anyhow::bail!("draft_spec is empty and no app_state available for A session");
            };
            effective_draft_spec.as_str()
        } else {
            draft_spec
        };

        // Ask B to review the draft spec before finalizing.
        // B's feedback is stored alongside A's revision notes.
        self.ensure_actor_registry_with_a_callbacks_auto().await;
        let b_feedback = {
            let registry_guard = self.actor_registry.read().await;
            if let Some(registry) = registry_guard.as_ref() {
                let mailbox = registry.b_mailbox().clone();
                drop(registry_guard);
                match mailbox
                    .request(
                        crate::core::boss_actor_runtime::ExecutorBCommand::ReviewSpec {
                            spec: draft_spec.to_string(),
                        },
                    )
                    .await
                {
                    Ok(crate::core::boss_actor_runtime::BossActorEvent::SpecReviewed {
                        feedback,
                    }) => Some(feedback),
                    _ => None,
                }
            } else {
                drop(registry_guard);
                None
            }
        };

        // Use B's feedback if caller didn't supply one, otherwise keep caller's value.
        let effective_review_feedback = if review_feedback.is_empty() {
            b_feedback.as_deref().unwrap_or("LGTM")
        } else {
            review_feedback
        };

        // Mutate plan state first (coordinator owns the data).
        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            plan.draft_spec = Some(draft_spec.to_string());
            plan.review_feedback = Some(effective_review_feedback.to_string());
            plan.revision_notes = Some(revision_notes.to_string());
            plan.document_spec = final_document_spec.to_string();
            plan.pseudo_code = final_pseudo_code.to_string();
            plan.finalized = true;
            plan.accepted_by_user = false;
        }

        let path_to_save = self.status.read().await.planning_file.clone();
        if let Some(path_str) = path_to_save {
            let path = std::path::PathBuf::from(path_str);
            self.save_plan_with_session(&path).await?;
        }

        // Send FinalizeDocumentation to A mailbox — A's handler drives the stage transition.
        if let Some(registry) = self.actor_registry.read().await.as_ref() {
            let _ = registry
                .a_mailbox()
                .request(DesignerACommand::FinalizeDocumentation {
                    signal: "finalize".to_string(),
                })
                .await;
        }

        // Fallback: if A's callback is not wired, coordinator transitions directly.
        let has_a_callbacks = self
            .actor_registry
            .read()
            .await
            .as_ref()
            .map(|r| r.has_a_callbacks)
            .unwrap_or(false);
        if !has_a_callbacks {
            self.transition_to(BossStage::WaitingForApproval).await?;
        }
        Ok(())
    }

    /// Records user feedback while in the documentation/approval loop.
    /// Any non-confirmation input during `WaitingForApproval` reopens Documentation,
    /// preserving a feedback trail for the next A/B revision pass.
    pub async fn record_documentation_feedback(&self, feedback: &str) -> anyhow::Result<()> {
        let trimmed = feedback.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            plan.documentation_feedback.push(trimmed.to_string());
            plan.accepted_by_user = false;
            plan.finalized = false;
        }

        let path_to_save = self.status.read().await.planning_file.clone();
        if let Some(path_str) = path_to_save {
            let path = std::path::PathBuf::from(path_str);
            self.save_plan_with_session(&path).await?;
        }

        self.transition_to(BossStage::Documentation).await?;
        Ok(())
    }

    /// Handles the user confirmation for transitioning from Documentation -> Execution.
    /// MUST only be called when in WaitingForApproval.
    /// Returns true if user confirmed (Y/enter), false if they provided feedback (re-enter Documentation).
    pub async fn handle_user_approval(&self, user_input: &str) -> anyhow::Result<bool> {
        let path_to_save = {
            let status = self.status.read().await;
            if status.stage != BossStage::WaitingForApproval {
                tracing::warn!(
                    "handle_user_approval called in wrong state: {:?}",
                    status.stage
                );
                return Ok(false);
            }
            status.planning_file.clone()
        };

        let approved = user_input.trim().to_uppercase() == "Y" || user_input.trim().is_empty();

        if approved {
            {
                let mut plan_guard = self.plan.write().await;
                if let Some(plan) = plan_guard.as_mut() {
                    if !plan.finalized {
                        tracing::warn!(
                            "handle_user_approval called before documentation loop finalized"
                        );
                        return Ok(false);
                    }
                    plan.accepted_by_user = true;
                }
            }
            if let Some(path_str) = path_to_save {
                let path = std::path::PathBuf::from(path_str);
                self.save_plan_with_session(&path).await?;
            }
        }

        // Send UserApproval to A mailbox — A's handler drives the stage transition.
        // Wire A's callbacks via auto path (uses stored auto_advance_app_state).
        self.ensure_actor_registry_with_a_callbacks_auto().await;
        let a_approved = if let Some(registry) = self.actor_registry.read().await.as_ref() {
            match registry
                .a_mailbox()
                .request(DesignerACommand::UserApproval {
                    input: user_input.to_string(),
                })
                .await
            {
                Ok(BossActorEvent::ApprovalHandled { approved: a }) => a,
                _ => approved,
            }
        } else {
            approved
        };

        // Fallback: if A's callback is not wired, coordinator transitions directly.
        let has_a_callbacks = self
            .actor_registry
            .read()
            .await
            .as_ref()
            .map(|r| r.has_a_callbacks)
            .unwrap_or(false);
        if !has_a_callbacks {
            if a_approved {
                self.transition_to(BossStage::Execution).await?;
            } else {
                self.record_documentation_feedback(user_input).await?;
            }
        }

        Ok(a_approved)
    }

    pub async fn handle_control_request(
        &self,
        request: BossControlRequest,
        tasks: &TaskManager,
        dispatcher: &NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        self.send_control_request(request, Arc::new(tasks.clone()), dispatcher.clone())
            .await
    }

    pub(crate) async fn handle_control_request_direct(
        &self,
        request: BossControlRequest,
        tasks: &TaskManager,
        dispatcher: &NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        match request {
            BossControlRequest::Report => Ok(BossControlResponse::Report(
                self.report_progress(tasks).await?,
            )),
            BossControlRequest::Stop {
                requester_session_id,
                deadline_ms,
            } => Ok(BossControlResponse::Stop(
                self.stop(tasks, &requester_session_id, dispatcher, deadline_ms)
                    .await?,
            )),
        }
    }

    pub async fn routed_step_metadata_snapshot(
        &self,
    ) -> std::collections::HashMap<usize, BossStepRoutedMetadata> {
        self.routed_step_metadata.read().await.clone()
    }

    async fn persist_plan_if_configured(&self) -> anyhow::Result<()> {
        let plan_path = self.status.read().await.planning_file.clone();
        if let Some(path) = plan_path {
            self.save_plan_with_session(std::path::Path::new(&path))
                .await?;
        }
        Ok(())
    }

    async fn trigger_review_for_completed_step(
        &self,
        step_id: usize,
        review_summary: String,
    ) -> anyhow::Result<()> {
        self.update_current_step(Some(step_id)).await;
        self.persist_plan_if_configured().await?;
        self.on_review_event(step_id, true, &review_summary, None)
            .await
    }

    pub async fn set_lism_policy(&self, policy: BossLisMPolicy) {
        *self.lism_policy.write().await = policy;
    }

    /// Synchronous policy initializer — only safe to call before the coordinator is Arc-wrapped.
    pub fn init_lism_policy(&mut self, policy: BossLisMPolicy) {
        if let Ok(mut guard) = self.lism_policy.try_write() {
            *guard = policy;
        }
    }

    pub async fn lism_policy(&self) -> BossLisMPolicy {
        *self.lism_policy.read().await
    }

    pub async fn set_worker_lism_policy(&self, policy: WorkerLisMPolicy) {
        *self.worker_lism_policy.write().await = policy;
    }

    /// Synchronous policy initializer — only safe to call before the coordinator is Arc-wrapped.
    pub fn init_worker_lism_policy(&mut self, policy: WorkerLisMPolicy) {
        if let Ok(mut guard) = self.worker_lism_policy.try_write() {
            *guard = policy;
        }
    }

    pub async fn worker_lism_policy(&self) -> WorkerLisMPolicy {
        *self.worker_lism_policy.read().await
    }

    pub async fn set_full_worker_dispatch_fallback_enabled(&self, enabled: bool) {
        *self.full_worker_dispatch_fallback_enabled.write().await = enabled;
    }

    pub fn init_full_worker_dispatch_fallback_enabled(&mut self, enabled: bool) {
        if let Ok(mut guard) = self.full_worker_dispatch_fallback_enabled.try_write() {
            *guard = enabled;
        }
    }

    pub async fn full_worker_dispatch_fallback_enabled(&self) -> bool {
        *self.full_worker_dispatch_fallback_enabled.read().await
    }

    /// Attach a LisM A/B sample sink. Call before the first `advance_plan`.
    pub fn with_lism_ab_sink(mut self, sink: SharedLisMAbSampleSink) -> Self {
        self.lism_ab_sink = Some(sink);
        self
    }

    /// Attach a LisM A/B sample sink in place (for post-construction wiring).
    pub fn set_lism_ab_sink(&mut self, sink: SharedLisMAbSampleSink) {
        self.lism_ab_sink = Some(sink);
    }

    /// Accessor for the LisM A/B sink — callers can record rolled_back outcomes.
    pub fn lism_ab_sink(&self) -> Option<&SharedLisMAbSampleSink> {
        self.lism_ab_sink.as_ref()
    }

    /// Inject a single-step execution plan for non-interactive `--boss-task` runs.
    /// Sets stage to Execution and current_step to 0 so `advance_plan` dispatches immediately.
    /// Safe to call after bootstrap_coordinator but before advance_plan.
    pub async fn seed_plan_for_task(&self, task: &str) {
        let plan_id = format!(
            "boss-task-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        let plan = BossPlan {
            plan_id: plan_id.clone(),
            task_description: task.to_string(),
            document_spec: task.to_string(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: true,
            documentation_feedback: vec![],
            steps: vec![BossPlanStep {
                id: 0,
                description: task.to_string(),
                objective: Some(task.to_string()),
                acceptance: seed_step_acceptance(task),
                requires_approval: false,
                status: BossPlanStepStatus::Pending,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: true,
            session_snapshot: None,
        };
        {
            let mut plan_guard = self.plan.write().await;
            *plan_guard = Some(plan);
        }
        {
            let mut status = self.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
            status.total_steps = Some(1);
        }
        {
            let mut session_guard = self.session.write().await;
            *session_guard = Some(BossSession::from_plan_id(&plan_id, BossStage::Execution));
        }
    }

    /// Stable run identifier derived from plan_id, or a timestamp fallback.
    pub(crate) async fn current_run_id(&self) -> String {
        self.session
            .read()
            .await
            .as_ref()
            .map(|s| s.plan_id.clone())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| format!("boss-run-{}", d.as_millis()))
                    .unwrap_or_else(|_| "boss-run-unknown".to_string())
            })
    }

    pub(crate) async fn emit_lism_sample_once(
        &self,
        run_id: &str,
        lism_enabled: bool,
        outcome: BossTestRunOutcome,
        pending_approval_count: usize,
    ) {
        let should_emit = {
            let mut status = self.status.write().await;
            if status.lism_sample_emitted {
                false
            } else {
                status.lism_sample_emitted = true;
                true
            }
        };
        if should_emit {
            self.emit_lism_sample(run_id, lism_enabled, outcome, pending_approval_count)
                .await;
        }
    }

    pub async fn has_terminal_failure(&self) -> bool {
        self.plan.read().await.as_ref().is_some_and(|plan| {
            plan.steps
                .iter()
                .any(|step| step.status.is_terminal_failure())
        })
    }

    fn add_task_usage_to_observability(
        summary: &mut BossObservabilitySummary,
        usage: &TaskUsageSummary,
    ) {
        summary.total_input_tokens += usage.input_tokens;
        summary.total_uncached_input_tokens += usage.uncached_input_tokens;
        summary.total_output_tokens += usage.output_tokens;
        summary.total_cache_read_tokens += usage.cache_read_input_tokens;
        summary.total_cache_write_tokens += usage.cache_creation_input_tokens;
        summary.total_original_chars += usage.original_prompt_chars;
        summary.total_sent_chars += usage.sent_prompt_chars;
        summary.estimated_cost_micros_usd += usage.estimated_cost_micros_usd;
    }

    fn routed_metadata_has_usage(m: &BossStepRoutedMetadata) -> bool {
        m.input_tokens.unwrap_or(0) > 0
            || m.uncached_input_tokens.unwrap_or(0) > 0
            || m.output_tokens.unwrap_or(0) > 0
            || m.cache_read_tokens.unwrap_or(0) > 0
            || m.cache_write_tokens.unwrap_or(0) > 0
            || m.original_prompt_chars.unwrap_or(0) > 0
            || m.sent_prompt_chars.unwrap_or(0) > 0
            || m.estimated_cost_micros_usd.unwrap_or(0) > 0
    }

    fn apply_loop_usage_to_routed_metadata(
        routed_metadata: &mut BossStepRoutedMetadata,
        usage: &crate::core::state_frame_loop::LoopUsage,
    ) {
        routed_metadata.input_tokens = Some(usage.input_tokens);
        routed_metadata.uncached_input_tokens = Some(usage.uncached_input_tokens);
        routed_metadata.output_tokens = Some(usage.output_tokens);
        routed_metadata.cache_read_tokens = Some(usage.cache_read_tokens);
        routed_metadata.cache_write_tokens = Some(usage.cache_write_tokens);
        routed_metadata.original_prompt_chars = Some(usage.original_prompt_chars);
        routed_metadata.sent_prompt_chars = Some(usage.sent_prompt_chars);
        routed_metadata.estimated_cost_micros_usd = Some(usage.estimated_cost_micros_usd);
        routed_metadata.fallback_count = Some(usage.fallback_count);
        routed_metadata.fallback_tier = usage.fallback_tier.clone();
        routed_metadata.fallback_reason = usage.fallback_reason.clone();
        routed_metadata.hydration_count = Some(usage.hydration_count);
        routed_metadata.hydration_from_contract_count = Some(usage.hydration_from_contract_count);
        routed_metadata.hydration_from_ledger_count = Some(usage.hydration_from_ledger_count);
        routed_metadata.stale_ref_count = Some(usage.stale_ref_count);
        routed_metadata.hydration_ref_missing = Some(usage.hydration_ref_missing);
        routed_metadata.hydration_miss_unsupported_count =
            Some(usage.hydration_miss_unsupported_count);
        routed_metadata.hydration_miss_stale_count = Some(usage.hydration_miss_stale_count);
        routed_metadata.hydration_miss_no_match_count = Some(usage.hydration_miss_no_match_count);
        routed_metadata.tool_dispatch_count = Some(usage.tool_dispatch_count);
        routed_metadata.tool_dispatch_success_count = Some(usage.tool_dispatch_success_count);
        routed_metadata.tool_dispatch_failure_count = Some(usage.tool_dispatch_failure_count);
        routed_metadata.tool_dispatch_ref_write_count = Some(usage.tool_dispatch_ref_write_count);
        routed_metadata.tool_dispatch_failure_taxonomy =
            usage.tool_dispatch_failure_taxonomy.clone();
        routed_metadata.last_effective_tool_action = usage.last_effective_tool_action.clone();
        if let Some(outcome) = usage.last_failure_outcome.as_ref() {
            routed_metadata.last_failure_kind = Some(outcome.kind.as_str().to_string());
            routed_metadata.last_failure_recoverable = Some(outcome.recoverable);
            routed_metadata.last_recommended_repair = outcome.recommended_next_action.clone();
            routed_metadata.last_failure_evidence_ref = outcome.evidence_ref.clone();
            routed_metadata.last_failure_bounded_excerpt = outcome.bounded_excerpt.clone();
            routed_metadata.last_failure_truncated = Some(outcome.truncated);
        } else {
            routed_metadata.last_failure_kind = None;
            routed_metadata.last_failure_recoverable = None;
            routed_metadata.last_recommended_repair = None;
            routed_metadata.last_failure_evidence_ref = None;
            routed_metadata.last_failure_bounded_excerpt = None;
            routed_metadata.last_failure_truncated = None;
        }
        routed_metadata.recovery_attempted = Some(usage.recovery_attempted);
        routed_metadata.recovery_tier = usage.recovery_tier.clone();
        routed_metadata.recovery_outcome = usage.recovery_outcome.clone();
        routed_metadata.terminal_blocker_kind = usage.terminal_blocker_kind.clone();
        routed_metadata.step_failure_classification = match usage.terminal_blocker_kind.as_deref()
        {
            Some("true_external_blocker") => Some(StepFailureClassification::TrueExternalBlocker),
            Some("unsupported_selector") => Some(StepFailureClassification::UnsupportedRequest),
            _ if usage.recovery_outcome.as_deref() == Some("repair_turn_injected")
                && matches!(
                    usage.completion_evidence_status,
                    Some(crate::core::state_frame::CompletionEvidenceStatus::MissingVerificationEvidence)
                ) =>
            {
                Some(StepFailureClassification::VerificationRepairContinuation)
            }
            _ if usage.recovery_outcome.as_deref() == Some("repair_turn_injected") => {
                Some(StepFailureClassification::RepairableRecovery)
            }
            _ if usage.recovery_outcome.as_deref() == Some("unsupported_selector") => {
                Some(StepFailureClassification::UnsupportedRequest)
            }
            _ if usage.terminal_blocker_kind.is_some() || usage.recovery_outcome.is_some() => {
                Some(StepFailureClassification::GenericFailure)
            }
            _ => None,
        };
        routed_metadata.completion_evidence_status = usage
            .completion_evidence_status
            .as_ref()
            .map(|status| status.as_str().to_string());
        routed_metadata.worker_report = usage.worker_report.clone();
        routed_metadata.completion_evidence_gaps = usage
            .worker_report
            .as_ref()
            .map(|report| report.completion_evidence_gaps.clone())
            .unwrap_or_default();
        routed_metadata.success_classification = classify_step_success(Some(routed_metadata));
    }

    async fn mark_routed_metadata_artifact_recovery(
        &self,
        step_id: usize,
        recovery_outcome: &str,
        terminal_blocker_kind: Option<&str>,
    ) {
        let mut routed_step_metadata = self.routed_step_metadata.write().await;
        if let Some(metadata) = routed_step_metadata.get_mut(&step_id) {
            metadata.recovery_attempted = Some(true);
            metadata.recovery_tier = Some("boss_artifact_repair".into());
            metadata.recovery_outcome = Some(recovery_outcome.into());
            metadata.terminal_blocker_kind = terminal_blocker_kind.map(str::to_string);
        }
    }

    fn derive_rollout_policy_decision(
        steps: &[BossStepReport],
    ) -> Option<BossRolloutPolicyDecision> {
        use std::collections::{BTreeMap, BTreeSet};

        #[derive(Default)]
        struct AggregateGap {
            target_ref: String,
            target_path: Option<String>,
            missing_evidence_kinds: BTreeSet<String>,
        }

        let mut aggregates: BTreeMap<(String, Option<String>), AggregateGap> = BTreeMap::new();
        for step in steps {
            let Some(metadata) = step.routed_metadata.as_ref() else {
                continue;
            };
            for gap in &metadata.completion_evidence_gaps {
                if !gap.missing_artifact_evidence
                    && !gap.missing_test_evidence
                    && !gap.missing_verification_evidence
                {
                    continue;
                }
                let key = (gap.target_ref.clone(), gap.target_path.clone());
                let aggregate = aggregates.entry(key).or_insert_with(|| AggregateGap {
                    target_ref: gap.target_ref.clone(),
                    target_path: gap.target_path.clone(),
                    missing_evidence_kinds: BTreeSet::new(),
                });
                if gap.missing_artifact_evidence {
                    aggregate
                        .missing_evidence_kinds
                        .insert("artifact_evidence".into());
                }
                if gap.missing_test_evidence {
                    aggregate
                        .missing_evidence_kinds
                        .insert("test_evidence".into());
                }
                if gap.missing_verification_evidence {
                    aggregate
                        .missing_evidence_kinds
                        .insert("verification_evidence".into());
                }
            }
        }

        if aggregates.is_empty() {
            return None;
        }

        let mut denylist_targets = Vec::new();
        let mut fallback_targets = Vec::new();
        for aggregate in aggregates.into_values() {
            let missing_evidence_kinds: Vec<String> =
                aggregate.missing_evidence_kinds.into_iter().collect();
            let verification_only_gap = missing_evidence_kinds.len() == 1
                && missing_evidence_kinds
                    .iter()
                    .any(|kind| kind == "verification_evidence");
            let requires_denylist = missing_evidence_kinds
                .iter()
                .any(|kind| kind == "artifact_evidence")
                && !verification_only_gap;
            let decision = if requires_denylist {
                BossRolloutTargetDecision {
                    target_ref: aggregate.target_ref,
                    target_path: aggregate.target_path,
                    missing_evidence_kinds,
                    recommended_policy: "denylist_direct_worker_lism".into(),
                    recommended_fallback: "full_worker_dispatch".into(),
                }
            } else if verification_only_gap {
                BossRolloutTargetDecision {
                    target_ref: aggregate.target_ref,
                    target_path: aggregate.target_path,
                    missing_evidence_kinds,
                    recommended_policy: "prefer_local_reverify".into(),
                    recommended_fallback: "verification_first".into(),
                }
            } else {
                BossRolloutTargetDecision {
                    target_ref: aggregate.target_ref,
                    target_path: aggregate.target_path,
                    missing_evidence_kinds,
                    recommended_policy: "fallback_before_force_on".into(),
                    recommended_fallback: "run_verification_or_full_worker_dispatch".into(),
                }
            };
            if requires_denylist {
                denylist_targets.push(decision.clone());
            }
            fallback_targets.push(decision);
        }

        let summary = if !denylist_targets.is_empty() {
            format!(
                "artifact-scoped completion gaps detected; denylist direct worker LisM for {} target(s) and fallback {} target(s)",
                denylist_targets.len(),
                fallback_targets.len()
            )
        } else if fallback_targets.iter().any(|target| {
            target.recommended_fallback == "verification_first"
                && target
                    .missing_evidence_kinds
                    .iter()
                    .all(|kind| kind == "verification_evidence")
        }) {
            format!(
                "verification-only completion gaps detected; prefer local re-verify for {} target(s)",
                fallback_targets.len()
            )
        } else {
            format!(
                "artifact-scoped test gaps detected; require fallback/verification for {} target(s) before force-on rollout",
                fallback_targets.len()
            )
        };

        Some(BossRolloutPolicyDecision {
            denylist_targets,
            fallback_targets,
            summary,
        })
    }

    fn resolve_step_rollout_execution_policy(
        metadata: Option<&BossStepRoutedMetadata>,
    ) -> Option<StepRolloutExecutionPolicy> {
        let metadata = metadata?;
        let affected_gaps = metadata
            .completion_evidence_gaps
            .iter()
            .filter(|gap| {
                gap.missing_artifact_evidence
                    || gap.missing_verification_evidence
                    || gap.missing_test_evidence
            })
            .cloned()
            .collect::<Vec<_>>();
        if affected_gaps.is_empty() {
            return None;
        }
        let has_artifact_gap = affected_gaps
            .iter()
            .any(|gap| gap.missing_artifact_evidence);
        let has_verification_gap = affected_gaps
            .iter()
            .any(|gap| gap.missing_verification_evidence);
        let has_test_gap = affected_gaps.iter().any(|gap| gap.missing_test_evidence);
        let verification_only_gap =
            has_verification_gap && !has_artifact_gap && !has_test_gap;
        if has_artifact_gap || has_verification_gap {
            if verification_only_gap {
                Some(StepRolloutExecutionPolicy {
                    forced_worker_lism_policy: WorkerLisMPolicy::ForceOff,
                    fallback_tier: "verification_first",
                    fallback_reason: "rollout_policy_verification_gap",
                    worker_role: WorkerRole::Verify,
                    force_fresh_spawn: true,
                    affected_gaps,
                })
            } else {
                Some(StepRolloutExecutionPolicy {
                    forced_worker_lism_policy: WorkerLisMPolicy::ForceOff,
                    fallback_tier: "full_worker_dispatch",
                    fallback_reason: if has_artifact_gap {
                        "rollout_policy_exact_artifact_gap"
                    } else {
                        "rollout_policy_verification_gap"
                    },
                    worker_role: WorkerRole::Implement,
                    force_fresh_spawn: false,
                    affected_gaps,
                })
            }
        } else if metadata.fallback_tier.as_deref() == Some("verification_first") {
            Some(StepRolloutExecutionPolicy {
                forced_worker_lism_policy: WorkerLisMPolicy::ForceOff,
                fallback_tier: "full_worker_dispatch",
                fallback_reason: "rollout_policy_test_evidence_gap_escalated",
                worker_role: WorkerRole::Implement,
                force_fresh_spawn: false,
                affected_gaps,
            })
        } else {
            Some(StepRolloutExecutionPolicy {
                forced_worker_lism_policy: WorkerLisMPolicy::ForceOff,
                fallback_tier: "verification_first",
                fallback_reason: "rollout_policy_test_evidence_gap",
                worker_role: WorkerRole::Verify,
                force_fresh_spawn: true,
                affected_gaps,
            })
        }
    }

    fn build_observability_summary(
        steps: &[BossStepReport],
        tasks: Option<&TaskManager>,
        step_metrics: Option<&BossStepMetrics>,
    ) -> Option<BossObservabilitySummary> {
        let mut summary = BossObservabilitySummary::default();
        let mut has_observability = false;

        for step in steps {
            if let Some(m) = &step.routed_metadata {
                has_observability = true;
                summary.total_steps_routed += 1;
                summary.total_cache_read_tokens += m.cache_read_tokens.unwrap_or(0);
                summary.total_cache_write_tokens += m.cache_write_tokens.unwrap_or(0);
                summary.total_fallback_count += m.fallback_count.unwrap_or(0);
                if let Some(tier) = &m.fallback_tier {
                    *summary
                        .fallback_tier_counts
                        .entry(tier.clone())
                        .or_insert(0) += 1;
                }
                if let Some(reason) = &m.fallback_reason {
                    *summary
                        .fallback_reason_counts
                        .entry(reason.clone())
                        .or_insert(0) += 1;
                }
                summary.total_projection_mismatch_count += m.projection_mismatch_count.unwrap_or(0);
                summary.total_hydration_count += m.hydration_count.unwrap_or(0);
                summary.total_hydration_from_contract_count +=
                    m.hydration_from_contract_count.unwrap_or(0);
                summary.total_hydration_from_ledger_count +=
                    m.hydration_from_ledger_count.unwrap_or(0);
                summary.total_stale_ref_count += m.stale_ref_count.unwrap_or(0);
                summary.total_hydration_ref_missing += m.hydration_ref_missing.unwrap_or(0);
                summary.total_hydration_miss_unsupported_count +=
                    m.hydration_miss_unsupported_count.unwrap_or(0);
                summary.total_hydration_miss_stale_count +=
                    m.hydration_miss_stale_count.unwrap_or(0);
                summary.total_hydration_miss_no_match_count +=
                    m.hydration_miss_no_match_count.unwrap_or(0);
                summary.total_tool_dispatch_count += m.tool_dispatch_count.unwrap_or(0);
                summary.total_tool_dispatch_success_count +=
                    m.tool_dispatch_success_count.unwrap_or(0);
                summary.total_tool_dispatch_failure_count +=
                    m.tool_dispatch_failure_count.unwrap_or(0);
                summary.total_tool_dispatch_ref_write_count +=
                    m.tool_dispatch_ref_write_count.unwrap_or(0);
                for (reason, count) in &m.tool_dispatch_failure_taxonomy {
                    *summary
                        .tool_dispatch_failure_taxonomy
                        .entry(reason.clone())
                        .or_insert(0) += count;
                }
                summary.total_input_tokens += m.input_tokens.unwrap_or(0);
                summary.total_uncached_input_tokens += m.uncached_input_tokens.unwrap_or(0);
                summary.total_output_tokens += m.output_tokens.unwrap_or(0);
                summary.total_original_chars += m.original_prompt_chars.unwrap_or(0);
                summary.total_sent_chars += m.sent_prompt_chars.unwrap_or(0);
                summary.estimated_cost_micros_usd += m.estimated_cost_micros_usd.unwrap_or(0);
                if m.provider_profile_id.is_some() {
                    summary.override_hit_count += 1;
                }
                if let Some(tier) = &m.model_tier {
                    *summary.model_tier_counts.entry(tier.clone()).or_insert(0) += 1;
                }
                if Self::routed_metadata_has_usage(m) {
                    continue;
                }
            }

            let Some(task_usage) = step
                .worker_task_id
                .as_deref()
                .and_then(|task_id| tasks.and_then(|tasks| tasks.get(task_id)))
                .and_then(|task| task.usage)
            else {
                continue;
            };
            if !task_usage.is_empty() {
                has_observability = true;
                Self::add_task_usage_to_observability(&mut summary, &task_usage);
            }
        }

        if let Some(metrics) = step_metrics {
            has_observability = true;
            summary.total_original_chars += metrics.original_chars;
            summary.total_sent_chars += metrics.sent_chars;
            summary.total_cache_read_tokens += metrics.cache_read_tokens;
            summary.total_cache_write_tokens += metrics.cache_creation_tokens;
        }

        has_observability.then_some(summary)
    }

    fn routed_metadata_for_report(
        plan: &BossPlan,
        step: &BossPlanStep,
        routed_step_metadata: &std::collections::HashMap<usize, BossStepRoutedMetadata>,
    ) -> Option<BossStepRoutedMetadata> {
        routed_step_metadata.get(&step.id).cloned().or_else(|| {
            if !step.completed || routed_step_metadata.is_empty() {
                return None;
            }
            let routed = build_routed_state_frame_with_model_route(
                plan,
                BossStage::Execution,
                step.id,
                ActorRole::Worker,
            );
            let state_frame_size = serde_json::to_string(&routed.frame).ok().map(|s| s.len());
            Some(BossStepRoutedMetadata {
                toolset_id: routed.frame.toolset_id.clone(),
                skillset_id: routed.frame.skillset_id.clone(),
                model_tier: Some(model_tier_label(routed.model_route.tier).to_string()),
                provider_profile_id: routed.model_route.provider_profile_id,
                state_frame_size,
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                fallback_count: Some(0),
                fallback_tier: None,
                fallback_reason: None,
                projection_mismatch_count: Some(routed.projection_mismatch_count),
                hydration_count: Some(0),
                hydration_from_contract_count: Some(0),
                hydration_from_ledger_count: Some(0),
                stale_ref_count: Some(0),
                hydration_ref_missing: Some(0),
                hydration_miss_unsupported_count: Some(0),
                hydration_miss_stale_count: Some(0),
                hydration_miss_no_match_count: Some(0),
                tool_dispatch_count: Some(0),
                tool_dispatch_success_count: Some(0),
                tool_dispatch_failure_count: Some(0),
                tool_dispatch_ref_write_count: Some(0),
                tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                input_tokens: Some(0),
                uncached_input_tokens: Some(0),
                output_tokens: Some(0),
                original_prompt_chars: Some(0),
                sent_prompt_chars: Some(0),
                estimated_cost_micros_usd: Some(0),
                visible_tools: Vec::new(),
                allowed_actions: routed.frame.allowed_actions.clone(),
                schema_hash: None,
                permission_hash: None,
                actor_role: Some(format!("{:?}", routed.frame.role).to_ascii_lowercase()),
                cwd: None,
                config_root: None,
                workspace_capabilities: Vec::new(),
                tool_contract_mismatch_count: Some(0),
                tool_contract_mismatch: None,
                last_effective_tool_action: None,
                last_failure_kind: None,
                last_failure_recoverable: None,
                last_recommended_repair: None,
                last_failure_evidence_ref: None,
                last_failure_bounded_excerpt: None,
                last_failure_truncated: None,
                recovery_attempted: None,
                recovery_tier: None,
                recovery_outcome: None,
                terminal_blocker_kind: None,
                step_failure_classification: None,
                completion_evidence_status: None,
                completion_evidence_gaps: Vec::new(),
                worker_report: None,
                success_classification: None,
            })
        })
    }

    /// Build a `BossReportPayload` snapshot suitable for LisM sampling.
    /// `tasks` is optional so LisM direct execution can still report routed metadata,
    /// while full-context worker runs can contribute persisted task usage.
    async fn build_lism_sample_report(&self, tasks: Option<&TaskManager>) -> BossReportPayload {
        let status = self.status.read().await.clone();
        let session = self.session.read().await.clone();
        let plan = self.plan.read().await.clone();
        let routed_step_metadata = self.routed_step_metadata.read().await.clone();
        let empty_session = BossSession::from_plan_id("unknown", status.stage);
        let session = session.unwrap_or(empty_session);
        let step_continuation_context = plan
            .as_ref()
            .and_then(|plan| {
                plan.steps
                    .iter()
                    .rev()
                    .find_map(build_stage_continuation_context)
            });
        let executor_b_stage_memory = plan.as_ref().and_then(|plan| {
            let routed_step_metadata = routed_step_metadata.clone();
            plan.steps
                .iter()
                .rev()
                .find_map(|step| project_executor_b_stage_memory(step, routed_step_metadata.get(&step.id)))
        });
        let steps = plan
            .as_ref()
            .map(|plan| {
                plan.steps
                    .iter()
                    .map(|step| BossStepReport {
                        id: step.id,
                        status: step.status,
                        worker_task_id: step.worker_task_id.clone(),
                        attempt_count: step.attempt_count,
                        last_review_summary: step.last_review_summary.clone(),
                        action_required: None,
                        blocker_reason: None,
                        routed_metadata: Self::routed_metadata_for_report(
                            plan,
                            step,
                            &routed_step_metadata,
                        ),
                        stage_execution_contract: step.stage_execution_contract.clone(),
                        stage_continuation_context: build_stage_continuation_context(step),
                        executor_b_stage_memory: project_executor_b_stage_memory(
                            step,
                            routed_step_metadata.get(&step.id),
                        ),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let step_metrics = status.last_step_metrics.clone();
        let observability_summary =
            Self::build_observability_summary(&steps, tasks, step_metrics.as_ref());
        let rollout_policy_decision = Self::derive_rollout_policy_decision(&steps);
        let success_classification =
            BossReportPayload::derive_success_classification_from_steps(&steps);

        BossReportPayload {
            stage: status.stage,
            current_step: status.current_step,
            total_steps: status.total_steps,
            designer_a: session.designer_a,
            executor_b: session.executor_b,
            active_children: session.active_children,
            steps,
            history_summary: vec![],
            observability_summary,
            rollout_policy_decision,
            success_classification,
            lism_policy: self.lism_policy().await,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: step_continuation_context,
            executor_b_stage_memory,
        }
    }

    /// Fire-and-forget: record a LisM A/B sample if a sink is configured.
    /// Never blocks the main flow; failures are logged as warnings.
    async fn emit_lism_sample(
        &self,
        run_id: &str,
        lism_enabled: bool,
        outcome: BossTestRunOutcome,
        pending_approval_count: usize,
    ) {
        let Some(sink) = &self.lism_ab_sink else {
            return;
        };
        let task_manager = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| app_state.permission_context.task_manager.clone());
        let report = self.build_lism_sample_report(task_manager.as_deref()).await;
        sink.record_run(
            run_id,
            lism_enabled,
            &report,
            outcome,
            pending_approval_count,
        );
    }
    pub async fn report_progress(&self, tasks: &TaskManager) -> anyhow::Result<BossReportPayload> {
        let status = self.status.read().await.clone();
        let session = self.session.read().await.clone();
        let plan = self.plan.read().await.clone();
        let routed_step_metadata = self.routed_step_metadata.read().await.clone();
        let empty_session = BossSession::from_plan_id("unknown", status.stage);
        let session = session.unwrap_or(empty_session);
        let step_continuation_context = plan
            .as_ref()
            .and_then(|plan| {
                plan.steps
                    .iter()
                    .rev()
                    .find_map(build_stage_continuation_context)
            });
        let executor_b_stage_memory = plan.as_ref().and_then(|plan| {
            let routed_step_metadata = routed_step_metadata.clone();
            plan.steps
                .iter()
                .rev()
                .find_map(|step| project_executor_b_stage_memory(step, routed_step_metadata.get(&step.id)))
        });
        let steps = plan
            .as_ref()
            .map(|plan| {
                plan.steps
                    .iter()
                    .map(|step| BossStepReport {
                        id: step.id,
                        status: step.status,
                        worker_task_id: step.worker_task_id.clone(),
                        attempt_count: step.attempt_count,
                        last_review_summary: step.last_review_summary.clone(),
                        action_required: if step.status == BossPlanStepStatus::ReplanRequired {
                            Some("replan_current_step".into())
                        } else {
                            None
                        },
                        blocker_reason: if step.status == BossPlanStepStatus::ReplanRequired {
                            step.last_correction.as_deref().map(|value| {
                                value
                                    .strip_prefix("replan required: ")
                                    .unwrap_or(value)
                                    .to_string()
                            })
                        } else {
                            None
                        },
                        routed_metadata: Self::routed_metadata_for_report(
                            plan,
                            step,
                            &routed_step_metadata,
                        ),
                        stage_execution_contract: step.stage_execution_contract.clone(),
                        stage_continuation_context: build_stage_continuation_context(step),
                        executor_b_stage_memory: project_executor_b_stage_memory(
                            step,
                            routed_step_metadata.get(&step.id),
                        ),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let history_summary = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| history_summary_from_app_state(app_state))
            .unwrap_or_else(|| {
                tasks
                    .list()
                    .into_iter()
                    .filter(|task| task.boss_actor_id.is_some())
                    .map(|task| format!("{}:{:?}", task.id, task.status))
                    .collect::<Vec<_>>()
            });

        let step_metrics = status.last_step_metrics.clone();
        let observability_summary =
            Self::build_observability_summary(&steps, Some(tasks), step_metrics.as_ref());
        let rollout_policy_decision = Self::derive_rollout_policy_decision(&steps);
        let success_classification =
            BossReportPayload::derive_success_classification_from_steps(&steps);

        Ok(BossReportPayload {
            stage: status.stage,
            current_step: status.current_step,
            total_steps: status.total_steps,
            designer_a: session.designer_a,
            executor_b: session.executor_b,
            active_children: session.active_children,
            steps,
            history_summary,
            observability_summary,
            rollout_policy_decision,
            success_classification,
            lism_policy: self.lism_policy().await,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: step_continuation_context,
            executor_b_stage_memory,
        })
    }

    pub async fn stop(
        &self,
        tasks: &TaskManager,
        requester_session_id: &str,
        dispatcher: &NotificationDispatcher,
        deadline_ms: u64,
    ) -> anyhow::Result<BossStopOutcome> {
        let mut stages = vec![BossStopStage::CancelIssued];
        let tracked_task_ids = {
            let session = self.session.read().await;
            session
                .as_ref()
                .map(|snapshot| {
                    tasks
                        .list()
                        .into_iter()
                        .filter(|task| {
                            task.owner.session_id == requester_session_id
                                && task.boss_actor_id.is_some()
                                && (snapshot.executor_b.task_id.as_deref()
                                    == Some(task.id.as_str())
                                    || snapshot.designer_a.task_id.as_deref()
                                        == Some(task.id.as_str())
                                    || task
                                        .boss_actor_id
                                        .as_deref()
                                        .is_some_and(|actor| actor.contains("child")))
                        })
                        .map(|task| task.id)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        let mut killed = Vec::new();
        for task_id in &tracked_task_ids {
            if tasks.kill(task_id, requester_session_id, dispatcher) {
                killed.push(task_id.clone());
            }
        }

        let mut pending_after_cancel = tracked_task_ids
            .iter()
            .filter(|task_id| {
                matches!(
                    tasks.status(task_id),
                    Some(TaskStatus::Pending | TaskStatus::Running)
                )
            })
            .cloned()
            .collect::<Vec<_>>();

        if !pending_after_cancel.is_empty() {
            stages.push(BossStopStage::DeadlineExpired);
            tokio::time::sleep(tokio::time::Duration::from_millis(deadline_ms)).await;
            pending_after_cancel = tracked_task_ids
                .iter()
                .filter(|task_id| {
                    matches!(
                        tasks.status(task_id),
                        Some(TaskStatus::Pending | TaskStatus::Running)
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
        }

        let mut force_drained = false;
        for task_id in pending_after_cancel {
            force_drained = true;
            if tasks.force_kill(&task_id, dispatcher) && !killed.contains(&task_id) {
                killed.push(task_id.clone());
            }
        }
        if force_drained {
            stages.push(BossStopStage::ForceDrain);
        }

        {
            let mut session = self.session.write().await;
            if let Some(snapshot) = session.as_mut() {
                snapshot.designer_a.status = BossActorStatus::Suspended;
                snapshot.executor_b.status = BossActorStatus::Suspended;
                snapshot.active_children.clear();
            }
        }

        // Stop A and B actor runtimes via their mailboxes — await Stopped before returning.
        if let Some(registry) = self.actor_registry.read().await.as_ref() {
            let _ = registry.a_mailbox().request(DesignerACommand::Stop).await;
            let _ = registry.b_mailbox().request(ExecutorBCommand::Stop).await;
        }

        // Abort A/B LLM session tasks directly — these may not have boss_actor_id set
        // (spawned by invoke_agent_tool_with_task_id without a boss_actor_policy), so the
        // tracked_task_ids filter above may have missed them.
        self.abort_a_b_sessions(tasks, dispatcher).await;

        Ok(BossStopOutcome {
            killed_task_ids: killed,
            stages,
        })
    }

    /// Abort A's and B's LLM session tasks by their stored task_id.
    /// Uses force_kill (no session ownership check) because these tasks are owned by the
    /// coordinator's session, not the requester's session.
    async fn abort_a_b_sessions(&self, tasks: &TaskManager, dispatcher: &NotificationDispatcher) {
        let (a_task_id, b_task_id) = {
            let guard = self.session.read().await;
            let (a, b) = guard
                .as_ref()
                .map(|s| (s.designer_a.task_id.clone(), s.executor_b.task_id.clone()))
                .unwrap_or((None, None));
            (a, b)
        };
        if let Some(id) = a_task_id {
            tasks.force_kill(&id, dispatcher);
        }
        if let Some(id) = b_task_id {
            tasks.force_kill(&id, dispatcher);
        }
    }

    /// Processes a task event to update the BossPlan by structured step identity.
    pub async fn on_task_event(&self, event: &TaskEvent) -> anyhow::Result<()> {
        let tasks = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| app_state.permission_context.task_manager.clone());
        // Group fan-in: task_id starts with "group-" and orchestration_group_id is B's task id.
        // Find the step whose worker_task_id matches the group_id (B's task id).
        if event.task_id.starts_with("group-") {
            if let Some(group_id) = &event.orchestration_group_id {
                let mut plan_guard = self.plan.write().await;
                let Some(plan) = plan_guard.as_mut() else {
                    return Ok(());
                };
                let step = plan
                    .steps
                    .iter_mut()
                    .find(|s| s.worker_task_id.as_deref() == Some(group_id.as_str()));
                if let Some(step) = step {
                    let step_id = step.id;
                    match event.status {
                        TaskStatus::Completed => {
                            // Fan-in complete — enter Reviewing, not Completed.
                            // A review gate must accept before the step advances.
                            step.status = BossPlanStepStatus::Reviewing;
                            tracing::info!(
                                "BossPlan: Step {} fan-in complete, entering Reviewing",
                                step_id
                            );
                        }
                        TaskStatus::Failed | TaskStatus::Killed => {
                            step.status = BossPlanStepStatus::Failed;
                            tracing::warn!(
                                "BossPlan: Step {} fan-in failed via group {}",
                                step_id,
                                group_id
                            );
                        }
                        _ => {}
                    }
                    drop(plan_guard);
                    // No auto-advance here — A review must call on_review_event to proceed.
                    return Ok(());
                }
            }
        }

        if event.orchestration_group_id.is_some() {
            tracing::debug!(
                "BossPlan: ignoring non-group child event {} with orchestration group {:?}",
                event.task_id,
                event.orchestration_group_id
            );
            return Ok(());
        }

        let Some(step_id) = event.step_id else {
            return Ok(());
        };

        let mut plan_guard = self.plan.write().await;
        let Some(plan) = plan_guard.as_mut() else {
            return Ok(());
        };

        let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
            return Ok(());
        };

        let review_summary = match event.status {
            TaskStatus::Completed => {
                if let Some(reason) = step_artifact_verification_error(step) {
                    step.completed = false;
                    step.worker_task_id = Some(event.task_id.clone());
                    step.last_review_summary = Some(reason.clone());
                    sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                    append_artifact_verification_runtime_records(
                        step,
                        "missing_or_invalid",
                        &reason,
                    );
                    step.attempt_count += 1;
                    let repair_instruction = build_artifact_repair_instruction(step, &reason);
                    if step.attempt_count >= step.retry_budget {
                        step.status = BossPlanStepStatus::Failed;
                        update_step_continuation_context(
                            step,
                            crate::core::state_frame::ContinuityMode::Repair,
                            extract_artifact_expectations(step.objective())
                                .into_iter()
                                .next()
                                .map(|expectation| expectation.path.display().to_string()),
                            repair_instruction,
                            continuation_verified_facts(step),
                        );
                    } else {
                        step.status = BossPlanStepStatus::Rejected;
                        update_step_continuation_context(
                            step,
                            crate::core::state_frame::ContinuityMode::Repair,
                            extract_artifact_expectations(step.objective())
                                .into_iter()
                                .next()
                                .map(|expectation| expectation.path.display().to_string()),
                            repair_instruction,
                            continuation_verified_facts(step),
                        );
                    }
                    tracing::warn!(
                        "BossPlan: Step {} failed artifact verification: {}",
                        step_id,
                        reason
                    );
                    None
                } else {
                    step.worker_task_id = Some(event.task_id.clone());
                    sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                    store_step_result_diff(step, &event.result, Some(&event.summary));
                    if matches!(step.status, BossPlanStepStatus::Completed) {
                        None
                    } else {
                        step.completed = false;
                        step.status = BossPlanStepStatus::Reviewing;
                        clear_step_continuation_context(step);
                        tracing::info!(
                            "BossPlan: Step {} completed by worker, entering Reviewing",
                            step_id
                        );
                        Some(build_step_review_summary(
                            step,
                            "Worker task",
                            &[
                                ("Worker task id", event.task_id.as_str()),
                                ("Summary", event.summary.as_str()),
                                ("Result", event.result.as_str()),
                                ("Next action", event.next_action.as_str()),
                            ],
                        ))
                    }
                }
            }
            TaskStatus::Failed | TaskStatus::Killed => {
                step.completed = false;
                step.status = BossPlanStepStatus::Failed;
                step.worker_task_id = Some(event.task_id.clone());
                sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                store_step_result_diff(step, &event.result, Some(&event.summary));
                let artifact_verification_reason = step_artifact_verification_error(step);
                step.last_review_summary = artifact_verification_reason
                    .clone()
                    .map(|reason| {
                        append_artifact_verification_runtime_records(
                            step,
                            "missing_or_invalid",
                            &reason,
                        );
                        reason
                    })
                    .or_else(|| Some(event.result.clone()).filter(|text| !text.trim().is_empty()))
                    .or_else(|| Some(event.summary.clone()).filter(|text| !text.trim().is_empty()));
                tracing::warn!("BossPlan: Step {} marked as failed", step_id);
                if step.last_review_summary.is_some() {
                    let next_action = artifact_verification_reason
                        .as_deref()
                        .and_then(|reason| build_artifact_repair_instruction(step, reason))
                        .or_else(|| step.last_review_summary.clone());
                    update_step_continuation_context(
                        step,
                        crate::core::state_frame::ContinuityMode::Repair,
                        extract_artifact_expectations(step.objective())
                            .into_iter()
                            .next()
                            .map(|expectation| expectation.path.display().to_string()),
                        next_action,
                        continuation_verified_facts(step),
                    );
                }
                None
            }
            TaskStatus::Running => {
                step.status = BossPlanStepStatus::Running;
                step.worker_task_id = Some(event.task_id.clone());
                sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                None
            }
            TaskStatus::Pending => None,
        };

        let recovery_status = plan.steps.iter().find(|s| s.id == step_id).and_then(|step| {
            match step.status {
                BossPlanStepStatus::Rejected => Some(("repair_dispatched", None)),
                BossPlanStepStatus::Failed
                    if step.last_correction.is_some() && has_only_verification_evidence_gap(step) =>
                {
                    Some(("repair_dispatched", None))
                }
                BossPlanStepStatus::Failed if step.last_correction.is_some() => Some((
                    "terminal_after_repair_exhausted",
                    Some("artifact_verification_failed"),
                )),
                _ => None,
            }
        });

        if let Some(summary) = review_summary {
            drop(plan_guard);
            self.trigger_review_for_completed_step(step_id, summary)
                .await?;
            return Ok(());
        }

        let next_step = next_unfinished_step_id(plan);
        drop(plan_guard);
        if let Some((outcome, blocker)) = recovery_status {
            self.mark_routed_metadata_artifact_recovery(step_id, outcome, blocker)
                .await;
            if outcome == "repair_dispatched" {
                self.maybe_auto_advance_after_completion().await?;
            }
        }
        self.update_current_step(next_step).await;

        Ok(())
    }

    /// Called by A's review agent when it has a verdict on a step.
    ///
    /// `accepted = true` → step moves to Completed and auto-advance fires.
    /// `accepted = false` → step moves to Rejected; if under retry budget, correction is stored
    ///   and the step is reset to Pending so the next `advance_plan` re-dispatches B.
    ///   If over budget, step is marked Failed.
    pub async fn on_review_event(
        &self,
        step_id: usize,
        accepted: bool,
        review_summary: &str,
        correction: Option<&str>,
    ) -> anyhow::Result<()> {
        self.ensure_actor_registry_with_a_callbacks_auto().await;

        let has_a_callbacks = self
            .actor_registry
            .read()
            .await
            .as_ref()
            .map(|r| r.has_a_callbacks)
            .unwrap_or(false);

        let fallback_decision = if accepted {
            crate::core::boss_actor_runtime::ReviewDecision::Accept {
                summary: review_summary.to_string(),
            }
        } else if let Some(correction_text) = correction {
            if correction_text.to_uppercase().contains("REPLAN_STEP") {
                Self::parse_a_review_decision(correction_text, review_summary)
            } else {
                crate::core::boss_actor_runtime::ReviewDecision::Correct {
                    summary: review_summary.to_string(),
                    correction: Some(correction_text.to_string()),
                }
            }
        } else {
            crate::core::boss_actor_runtime::ReviewDecision::Correct {
                summary: review_summary.to_string(),
                correction: None,
            }
        };

        let a_mailbox = if has_a_callbacks {
            let guard = self.actor_registry.read().await;
            guard.as_ref().map(|registry| registry.a_mailbox().clone())
        } else {
            None
        };
        let decision = if let Some(a_mailbox) = a_mailbox {
            match a_mailbox
                .request(DesignerACommand::Review {
                    step_id,
                    accepted,
                    summary: review_summary.to_string(),
                    correction: correction.map(str::to_string),
                })
                .await
            {
                Ok(BossActorEvent::ReviewComplete { decision, .. }) => decision,
                _ => fallback_decision,
            }
        } else {
            fallback_decision
        };

        if !has_a_callbacks {
            let designer_a_state = {
                let guard = self.actor_registry.read().await;
                guard
                    .as_ref()
                    .map(|registry| registry.designer_a.state.clone())
            };
            if let Some(a_state) = designer_a_state {
                let mut a_state = a_state.write().await;
                a_state.status = BossActorStatus::Active;
                a_state.current_step = Some(step_id);
            }
            self.apply_review_verdict(step_id, &decision).await?;
        }

        Ok(())
    }

    /// Inner side-effect for A's review callback — called from DesignerARuntime handler.
    /// Mutates plan state and fires auto-advance if accepted.
    pub(crate) async fn apply_review_verdict(
        &self,
        step_id: usize,
        decision: &crate::core::boss_actor_runtime::ReviewDecision,
    ) -> anyhow::Result<()> {
        let (should_auto_advance, artifact_recovery_status) = {
            let mut plan_guard = self.plan.write().await;
            let Some(plan) = plan_guard.as_mut() else {
                return Ok(());
            };
            let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
                return Ok(());
            };
            match decision {
                crate::core::boss_actor_runtime::ReviewDecision::Accept { summary } => {
                    if let Some(reason) = step_artifact_verification_error(step) {
                        append_review_runtime_record(step, "accepted", summary, None);
                        append_artifact_verification_runtime_records(
                            step,
                            "missing_or_invalid",
                            &reason,
                        );
                        step.last_review_summary = Some(reason);
                        step.completed = false;
                        step.attempt_count += 1;
                        let verification_instruction =
                            build_verification_repair_instruction(step).or_else(|| {
                                build_artifact_repair_instruction(
                                    step,
                                    step.last_review_summary
                                        .as_deref()
                                        .unwrap_or("artifact verification failed"),
                                )
                            });
                        if step.attempt_count >= step.retry_budget {
                            step.status = BossPlanStepStatus::Failed;
                            update_step_continuation_context(
                                step,
                                crate::core::state_frame::ContinuityMode::Repair,
                                extract_artifact_expectations(step.objective())
                                    .into_iter()
                                    .next()
                                    .map(|expectation| expectation.path.display().to_string()),
                                Some("verify_artifact".into()).or(verification_instruction),
                                continuation_verified_facts(step),
                            );
                            (
                                false,
                                Some((
                                    "terminal_after_repair_exhausted",
                                    Some("artifact_verification_failed"),
                                )),
                            )
                        } else {
                            step.status = BossPlanStepStatus::Rejected;
                            update_step_continuation_context(
                                step,
                                crate::core::state_frame::ContinuityMode::Repair,
                                extract_artifact_expectations(step.objective())
                                    .into_iter()
                                    .next()
                                    .map(|expectation| expectation.path.display().to_string()),
                                Some("verify_artifact".into()).or(verification_instruction),
                                continuation_verified_facts(step),
                            );
                            (false, Some(("repair_dispatched", None)))
                        }
                    } else {
                        append_review_runtime_record(step, "accepted", summary, None);
                        append_artifact_verification_runtime_records(
                            step,
                            "verified",
                            "artifact verification passed",
                        );
                        step.last_review_summary = Some(summary.clone());
                        step.completed = true;
                        step.status = BossPlanStepStatus::Completed;
                        clear_step_continuation_context(step);
                        (true, None)
                    }
                }
                crate::core::boss_actor_runtime::ReviewDecision::Correct {
                    summary,
                    correction,
                } => {
                    append_review_runtime_record(step, "rejected", summary, correction.as_deref());
                    step.last_review_summary = Some(summary.clone());
                    step.attempt_count += 1;
                    if step.attempt_count >= step.retry_budget {
                        step.status = BossPlanStepStatus::Failed;
                    } else {
                        step.status = BossPlanStepStatus::Rejected;
                        update_step_continuation_context(
                            step,
                            crate::core::state_frame::ContinuityMode::Repair,
                            correction.clone(),
                            correction.clone(),
                            continuation_verified_facts(step),
                        );
                    }
                    (false, None)
                }
                crate::core::boss_actor_runtime::ReviewDecision::ReplanStep { summary, reason } => {
                    append_review_runtime_record(
                        step,
                        "replan_required",
                        summary,
                        Some(reason.as_str()),
                    );
                    step.last_review_summary = Some(summary.clone());
                    step.status = BossPlanStepStatus::ReplanRequired;
                    update_step_continuation_context(
                        step,
                        crate::core::state_frame::ContinuityMode::Repair,
                        None,
                        Some(format!("replan required: {reason}")),
                        continuation_verified_facts(step),
                    );
                    (false, None)
                }
            }
        };
        self.persist_plan_if_configured().await?;
        if let Some((outcome, blocker)) = artifact_recovery_status {
            self.mark_routed_metadata_artifact_recovery(step_id, outcome, blocker)
                .await;
        }
        if should_auto_advance {
            let next_step = self
                .plan
                .read()
                .await
                .as_ref()
                .and_then(|p| next_unfinished_step_id(p));
            self.update_current_step(next_step).await;
            self.maybe_auto_advance_after_completion().await?;
        } else if matches!(artifact_recovery_status, Some(("repair_dispatched", None))) {
            self.maybe_auto_advance_after_completion().await?;
        }
        Ok(())
    }

    /// Inner side-effect for A's documentation callback — called from DesignerARuntime handler.
    /// Signal is the raw user input for approval, or "finalize" for documentation loop completion.
    pub(crate) async fn apply_documentation_signal(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        signal: &str,
    ) -> anyhow::Result<()> {
        // "finalize" signal: transition to WaitingForApproval (plan already mutated by caller).
        // Approval signal: "Y"/empty → Execution; anything else → Documentation feedback.
        let stage = self.status.read().await.stage;
        if signal == "finalize" {
            self.transition_to(BossStage::WaitingForApproval).await?;
        } else if stage == BossStage::WaitingForApproval {
            if signal.trim().to_uppercase() == "Y" || signal.trim().is_empty() {
                self.transition_to(BossStage::Execution).await?;
                let _ = self.advance_plan(app_state).await;
            } else {
                self.record_documentation_feedback(signal).await?;
            }
        }
        Ok(())
    }

    /// Simplified entry point for dispatcher updates.
    pub async fn on_notification(
        &self,
        notification: &crate::interaction::notification::Notification,
    ) -> anyhow::Result<()> {
        if notification.notification_type
            != crate::interaction::notification::NotificationType::TaskUpdate
        {
            return Ok(());
        }

        let step_id = match notification.step_id {
            Some(id) => id,
            None => return Ok(()),
        };
        let tasks = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| app_state.permission_context.task_manager.clone());

        let mut plan_guard = self.plan.write().await;
        let Some(plan) = plan_guard.as_mut() else {
            return Ok(());
        };

        let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
            return Ok(());
        };

        let review_summary = match notification.status.as_deref().unwrap_or_default() {
            status if status.eq_ignore_ascii_case("completed") => {
                if let Some(reason) = step_artifact_verification_error(step) {
                    step.completed = false;
                    step.worker_task_id = notification.task_id.clone();
                    step.last_review_summary = Some(reason.clone());
                    if let Some(task_id) = notification.task_id.as_deref() {
                        sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                    }
                    append_artifact_verification_runtime_records(
                        step,
                        "missing_or_invalid",
                        &reason,
                    );
                    step.attempt_count += 1;
                    let verification_instruction = build_verification_repair_instruction(step)
                        .or_else(|| build_artifact_repair_instruction(step, &reason));
                    if step.attempt_count >= step.retry_budget {
                        step.status = BossPlanStepStatus::Failed;
                        update_step_continuation_context(
                            step,
                            crate::core::state_frame::ContinuityMode::Repair,
                            extract_artifact_expectations(step.objective())
                                .into_iter()
                                .next()
                                .map(|expectation| expectation.path.display().to_string()),
                            Some("verify_artifact".into()).or(verification_instruction),
                            continuation_verified_facts(step),
                        );
                    } else {
                        step.status = BossPlanStepStatus::Rejected;
                        update_step_continuation_context(
                            step,
                            crate::core::state_frame::ContinuityMode::Repair,
                            extract_artifact_expectations(step.objective())
                                .into_iter()
                                .next()
                                .map(|expectation| expectation.path.display().to_string()),
                            Some("verify_artifact".into()).or(verification_instruction),
                            continuation_verified_facts(step),
                        );
                    }
                    tracing::warn!(
                        "BossPlan: Step {} failed artifact verification via notification: {}",
                        step_id,
                        reason
                    );
                    None
                } else {
                    step.worker_task_id = notification.task_id.clone();
                    if let Some(task_id) = notification.task_id.as_deref() {
                        sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                    }
                    store_step_result_diff(
                        step,
                        notification.output_file.as_deref().unwrap_or_default(),
                        Some(notification.body.as_str()),
                    );
                    if matches!(step.status, BossPlanStepStatus::Completed) {
                        None
                    } else {
                        step.completed = false;
                        step.status = BossPlanStepStatus::Reviewing;
                        clear_step_continuation_context(step);
                        tracing::info!(
                            "BossPlan: Step {} completed via notification, entering Reviewing",
                            step_id
                        );
                        Some(build_step_review_summary(
                            step,
                            "Worker notification",
                            &[
                                (
                                    "Worker task id",
                                    notification.task_id.as_deref().unwrap_or(""),
                                ),
                                ("Title", notification.title.as_str()),
                                ("Body", notification.body.as_str()),
                                ("Status", notification.status.as_deref().unwrap_or_default()),
                                (
                                    "Next action",
                                    notification.next_action.as_deref().unwrap_or_default(),
                                ),
                                (
                                    "Output file",
                                    notification.output_file.as_deref().unwrap_or_default(),
                                ),
                            ],
                        ))
                    }
                }
            }
            status
                if status.eq_ignore_ascii_case("failed")
                    || status.eq_ignore_ascii_case("killed") =>
            {
                step.completed = false;
                step.status = BossPlanStepStatus::Failed;
                step.worker_task_id = notification.task_id.clone();
                if let Some(task_id) = notification.task_id.as_deref() {
                    sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                }
                store_step_result_diff(
                    step,
                    notification.output_file.as_deref().unwrap_or_default(),
                    Some(notification.body.as_str()),
                );
                let artifact_verification_reason = step_artifact_verification_error(step);
                step.last_review_summary = artifact_verification_reason
                    .clone()
                    .map(|reason| {
                        append_artifact_verification_runtime_records(
                            step,
                            "missing_or_invalid",
                            &reason,
                        );
                        reason
                    })
                    .or_else(|| {
                        notification
                            .body
                            .split("Result: ")
                            .nth(1)
                            .map(str::trim)
                            .map(str::to_string)
                            .filter(|text| !text.is_empty())
                    })
                    .or_else(|| {
                        notification
                            .next_action
                            .clone()
                            .filter(|text| !text.trim().is_empty())
                    });
                if step.last_review_summary.is_some() {
                    let next_action = artifact_verification_reason
                        .as_deref()
                        .and_then(|reason| build_artifact_repair_instruction(step, reason))
                        .or_else(|| step.last_review_summary.clone());
                    update_step_continuation_context(
                        step,
                        crate::core::state_frame::ContinuityMode::Repair,
                        extract_artifact_expectations(step.objective())
                            .into_iter()
                            .next()
                            .map(|expectation| expectation.path.display().to_string()),
                        next_action,
                        continuation_verified_facts(step),
                    );
                }
                tracing::warn!(
                    "BossPlan: Step {} marked as failed via notification",
                    step_id
                );
                None
            }
            status if status.eq_ignore_ascii_case("running") => {
                step.status = BossPlanStepStatus::Running;
                step.worker_task_id = notification.task_id.clone();
                if let Some(task_id) = notification.task_id.as_deref() {
                    sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                }
                None
            }
            _ => None,
        };

        let recovery_status =
            plan.steps
                .iter()
                .find(|s| s.id == step_id)
                .and_then(|step| match step.status {
                    BossPlanStepStatus::Rejected => Some(("repair_dispatched", None)),
                    BossPlanStepStatus::Failed if step.last_correction.is_some() => Some((
                        "terminal_after_repair_exhausted",
                        Some("artifact_verification_failed"),
                    )),
                    _ => None,
                });

        if let Some(summary) = review_summary {
            drop(plan_guard);
            self.trigger_review_for_completed_step(step_id, summary)
                .await?;
            return Ok(());
        }

        let next_step = next_unfinished_step_id(plan);
        drop(plan_guard);
        if let Some((outcome, blocker)) = recovery_status {
            self.mark_routed_metadata_artifact_recovery(step_id, outcome, blocker)
                .await;
            if outcome == "repair_dispatched" {
                self.maybe_auto_advance_after_completion().await?;
            }
        }
        self.update_current_step(next_step).await;

        Ok(())
    }

    async fn maybe_auto_advance_after_completion(&self) -> anyhow::Result<()> {
        let app_state = {
            let guard = self.auto_advance_app_state.read().await;
            guard.clone()
        };
        let Some(app_state) = app_state else {
            return Ok(());
        };
        let _ = self.advance_plan(&app_state).await?;
        Ok(())
    }

    pub async fn sync_terminal_child_task_state(
        &self,
        tasks: &TaskManager,
    ) -> anyhow::Result<bool> {
        let terminal_step = {
            let plan_guard = self.plan.read().await;
            let Some(plan) = plan_guard.as_ref() else {
                return Ok(false);
            };
            let current_step_id = self.status.read().await.current_step;
            let current_step_terminal = current_step_id.and_then(|current_step_id| {
                let step = plan.steps.iter().find(|step| step.id == current_step_id)?;
                if step.completed || step.status.is_terminal_failure() {
                    return None;
                }
                let task_id = step.worker_task_id.as_ref()?;
                let status = tasks.status(task_id)?;
                matches!(
                    status,
                    TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Killed
                )
                .then_some((step.id, task_id.clone(), status))
            });
            if current_step_terminal.is_some() {
                current_step_terminal
            } else {
                plan.steps.iter().find_map(|step| {
                    if step.completed || step.status.is_terminal_failure() {
                        return None;
                    }
                    let task_id = step.worker_task_id.as_ref()?;
                    let status = tasks.status(task_id)?;
                    if matches!(
                        status,
                        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Killed
                    ) {
                        Some((step.id, task_id.clone(), status))
                    } else {
                        None
                    }
                })
            }
        };

        let Some((step_id, task_id, status)) = terminal_step else {
            return Ok(false);
        };

        let record = tasks
            .get(&task_id)
            .ok_or_else(|| anyhow::anyhow!("unknown child task {task_id}"))?;
        let event = TaskEvent {
            owner: record.owner.clone(),
            target_task_id: Some(task_id.clone()),
            task_id: task_id.clone(),
            task_type: record.task_type,
            status,
            summary: String::new(),
            result: String::new(),
            next_action: String::new(),
            worker_role: record.worker_role,
            orchestration_group_id: record.orchestration_group_id.clone(),
            phase: record.phase,
            validation_state: record.validation_state,
            step_id: Some(step_id),
            output_file: record.output_file,
            usage: record.usage.clone(),
        };
        self.on_task_event(&event).await?;
        Ok(true)
    }

    async fn advance_once(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Option<String>> {
        let parent_session_id = app_state.active_session_id.clone();
        let next_action = {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;

            if !plan.auto_sequence {
                return Ok(None);
            }

            if plan.steps.iter().all(|step| step.completed) {
                Some(AdvanceOutcome::PlanComplete)
            } else if let Some(step) = plan
                .steps
                .iter()
                .find(|step| step.status.is_terminal_failure())
            {
                Some(AdvanceOutcome::TerminalFailure(
                    step.last_review_summary
                        .clone()
                        .unwrap_or_else(|| format!("step {} failed", step.id)),
                ))
            } else if plan
                .steps
                .iter()
                .any(|step| step.status == BossPlanStepStatus::Running)
            {
                None
            } else if let Some(step_id) = next_runnable_step(plan).map(|step| step.id) {
                let step = plan
                    .steps
                    .iter_mut()
                    .find(|step| step.id == step_id)
                    .expect("runnable step must exist");
                if step.requires_approval {
                    Some(AdvanceOutcome::ApprovalBarrier(step.id))
                } else {
                    step.status = BossPlanStepStatus::Running;
                    Some(AdvanceOutcome::Dispatch(step_id))
                }
            } else if let Some(step) = plan
                .steps
                .iter()
                .find(|step| step.status == BossPlanStepStatus::ReplanRequired)
            {
                Some(AdvanceOutcome::ReplanRequired(
                    step.id,
                    step.last_correction
                        .as_deref()
                        .map(|value| {
                            value
                                .strip_prefix("replan required: ")
                                .unwrap_or(value)
                                .to_string()
                        })
                        .unwrap_or_else(|| "current step requires replanning".to_string()),
                ))
            } else {
                Some(AdvanceOutcome::NoRunnableStep)
            }
        };

        match next_action {
            Some(AdvanceOutcome::PlanComplete) => {
                self.update_current_step(None).await;
                if self.get_stage().await != BossStage::Completed {
                    self.transition_to(BossStage::Completed).await?;
                }
                let run_id = self.current_run_id().await;
                let lism_enabled = effective_lism_enabled(
                    self.lism_policy().await,
                    app_state.permission_context.lism_enabled(),
                );
                self.emit_lism_sample_once(
                    &run_id,
                    lism_enabled,
                    BossTestRunOutcome::Completed,
                    0,
                )
                    .await;
                Ok(Some(
                    "Boss plan complete; no further steps to dispatch.".into(),
                ))
            }
            Some(AdvanceOutcome::TerminalFailure(reason)) => {
                self.update_current_step(None).await;
                if self.get_stage().await != BossStage::Documentation {
                    self.transition_to(BossStage::Documentation).await?;
                }
                let run_id = self.current_run_id().await;
                let lism_enabled = effective_lism_enabled(
                    self.lism_policy().await,
                    app_state.permission_context.lism_enabled(),
                );
                self.emit_lism_sample_once(
                    &run_id,
                    lism_enabled,
                    BossTestRunOutcome::Aborted,
                    0,
                )
                    .await;
                Ok(Some(format!(
                    "Boss plan stopped after a terminal step failure; auto-advance halted. Reason: {}",
                    reason
                )))
            }
            Some(AdvanceOutcome::ApprovalBarrier(step_id)) => {
                self.update_step_status(step_id, BossPlanStepStatus::WaitingForApproval)
                    .await?;
                self.update_current_step(Some(step_id)).await;
                Ok(Some(format!(
                    "Boss plan paused before step {} because it requires approval.",
                    step_id
                )))
            }
            Some(AdvanceOutcome::Dispatch(step_id)) => {
                self.update_current_step(Some(step_id)).await;

                let lism_enabled = effective_lism_enabled(
                    self.lism_policy().await,
                    app_state.permission_context.lism_enabled(),
                );
                let step_rollout_execution_policy = {
                    let routed_step_metadata = self.routed_step_metadata.read().await;
                    routed_step_metadata.get(&step_id).and_then(|metadata| {
                        Self::resolve_step_rollout_execution_policy(Some(metadata))
                    })
                };
                let force_full_worker_dispatch_from_policy =
                    step_rollout_execution_policy.is_some();

                if lism_enabled && !force_full_worker_dispatch_from_policy {
                    let full_worker_dispatch_fallback_enabled =
                        self.full_worker_dispatch_fallback_enabled().await;
                    let routed_preview = {
                        let plan_guard = self.plan.read().await;
                        let plan = plan_guard
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                        build_routed_state_frame_with_model_route(
                            plan,
                            BossStage::Execution,
                            step_id,
                            ActorRole::Worker,
                        )
                    };
                    if full_worker_dispatch_fallback_enabled
                        && requires_external_tool_execution(
                            &routed_preview.frame,
                            app_state.runtime_tool_registry.is_some(),
                        )
                    {
                        let state_frame_size = serde_json::to_string(&routed_preview.frame)
                            .map(|s| s.len())
                            .ok();
                        let routed_metadata = BossStepRoutedMetadata {
                            toolset_id: routed_preview.frame.toolset_id.clone(),
                            skillset_id: routed_preview.frame.skillset_id.clone(),
                            model_tier: Some(
                                model_tier_label(routed_preview.model_route.tier).to_string(),
                            ),
                            provider_profile_id: routed_preview.model_route.provider_profile_id,
                            state_frame_size,
                            cache_read_tokens: Some(0),
                            cache_write_tokens: Some(0),
                            fallback_count: Some(1),
                            fallback_tier: Some("full_worker_dispatch".into()),
                            fallback_reason: Some("external_tool_execution_required".into()),
                            projection_mismatch_count: Some(
                                routed_preview.projection_mismatch_count,
                            ),
                            hydration_count: Some(0),
                            hydration_from_contract_count: Some(0),
                            hydration_from_ledger_count: Some(0),
                            stale_ref_count: Some(0),
                            hydration_ref_missing: Some(0),
                            hydration_miss_unsupported_count: Some(0),
                            hydration_miss_stale_count: Some(0),
                            hydration_miss_no_match_count: Some(0),
                            tool_dispatch_count: Some(0),
                            tool_dispatch_success_count: Some(0),
                            tool_dispatch_failure_count: Some(0),
                            tool_dispatch_ref_write_count: Some(0),
                            tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                            input_tokens: Some(0),
                            uncached_input_tokens: Some(0),
                            output_tokens: Some(0),
                            original_prompt_chars: Some(0),
                            sent_prompt_chars: Some(0),
                            estimated_cost_micros_usd: Some(0),
                            visible_tools: Vec::new(),
                            allowed_actions: Vec::new(),
                            schema_hash: None,
                            permission_hash: None,
                            actor_role: None,
                            cwd: None,
                            config_root: None,
                            workspace_capabilities: Vec::new(),
                            tool_contract_mismatch_count: Some(0),
                            tool_contract_mismatch: None,
                            last_effective_tool_action: None,
                            last_failure_kind: None,
                            last_failure_recoverable: None,
                            last_recommended_repair: None,
                            last_failure_evidence_ref: None,
                            last_failure_bounded_excerpt: None,
                            last_failure_truncated: None,
                            recovery_attempted: None,
                            recovery_tier: None,
                            recovery_outcome: None,
                            terminal_blocker_kind: None,
                            step_failure_classification: None,
                            completion_evidence_status: None,
                            completion_evidence_gaps: Vec::new(),
                            worker_report: None,
                            success_classification: None,
                        };
                        let mut routed_step_metadata = self.routed_step_metadata.write().await;
                        routed_step_metadata.insert(step_id, routed_metadata);
                    } else if app_state
                        .permission_context
                        .inherited_active_model_snapshot
                        .is_none()
                    {
                        let state_frame_size = serde_json::to_string(&routed_preview.frame)
                            .map(|s| s.len())
                            .ok();
                        let routed_metadata = BossStepRoutedMetadata {
                            toolset_id: routed_preview.frame.toolset_id.clone(),
                            skillset_id: routed_preview.frame.skillset_id.clone(),
                            model_tier: Some(
                                model_tier_label(routed_preview.model_route.tier).to_string(),
                            ),
                            provider_profile_id: routed_preview.model_route.provider_profile_id,
                            state_frame_size,
                            cache_read_tokens: Some(0),
                            cache_write_tokens: Some(0),
                            fallback_count: Some(1),
                            fallback_tier: Some("full_worker_dispatch".into()),
                            fallback_reason: Some("missing_active_model_snapshot".into()),
                            projection_mismatch_count: Some(
                                routed_preview.projection_mismatch_count,
                            ),
                            hydration_count: Some(0),
                            hydration_from_contract_count: Some(0),
                            hydration_from_ledger_count: Some(0),
                            stale_ref_count: Some(0),
                            hydration_ref_missing: Some(0),
                            hydration_miss_unsupported_count: Some(0),
                            hydration_miss_stale_count: Some(0),
                            hydration_miss_no_match_count: Some(0),
                            tool_dispatch_count: Some(0),
                            tool_dispatch_success_count: Some(0),
                            tool_dispatch_failure_count: Some(0),
                            tool_dispatch_ref_write_count: Some(0),
                            tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                            input_tokens: Some(0),
                            uncached_input_tokens: Some(0),
                            output_tokens: Some(0),
                            original_prompt_chars: Some(0),
                            sent_prompt_chars: Some(0),
                            estimated_cost_micros_usd: Some(0),
                            visible_tools: Vec::new(),
                            allowed_actions: Vec::new(),
                            schema_hash: None,
                            permission_hash: None,
                            actor_role: None,
                            cwd: None,
                            config_root: None,
                            workspace_capabilities: Vec::new(),
                            tool_contract_mismatch_count: Some(0),
                            tool_contract_mismatch: None,
                            last_effective_tool_action: None,
                            last_failure_kind: None,
                            last_failure_recoverable: None,
                            last_recommended_repair: None,
                            last_failure_evidence_ref: None,
                            last_failure_bounded_excerpt: None,
                            last_failure_truncated: None,
                            recovery_attempted: None,
                            recovery_tier: None,
                            recovery_outcome: None,
                            terminal_blocker_kind: None,
                            step_failure_classification: None,
                            completion_evidence_status: None,
                            completion_evidence_gaps: Vec::new(),
                            worker_report: None,
                            success_classification: None,
                        };
                        let mut routed_step_metadata = self.routed_step_metadata.write().await;
                        routed_step_metadata.insert(step_id, routed_metadata);
                    } else {
                        let (outcome, routed_metadata) = {
                            let plan_guard = self.plan.read().await;
                            let plan = plan_guard
                                .as_ref()
                                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                            let inherited_snapshot = app_state
                                .permission_context
                                .inherited_active_model_snapshot
                                .as_ref()
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "LisM boss path requires an active model snapshot"
                                    )
                                })?;
                            let routed = build_routed_state_frame_with_model_route(
                                plan,
                                BossStage::Execution,
                                step_id,
                                ActorRole::Worker,
                            );
                            let state_frame_size =
                                serde_json::to_string(&routed.frame).map(|s| s.len()).ok();
                            let mut routed_metadata = BossStepRoutedMetadata {
                                toolset_id: routed.frame.toolset_id.clone(),
                                skillset_id: routed.frame.skillset_id.clone(),
                                model_tier: Some(
                                    model_tier_label(routed.model_route.tier).to_string(),
                                ),
                                provider_profile_id: routed.model_route.provider_profile_id.clone(),
                                state_frame_size,
                                cache_read_tokens: Some(0),
                                cache_write_tokens: Some(0),
                                fallback_count: Some(0),
                                fallback_tier: None,
                                fallback_reason: None,
                                projection_mismatch_count: Some(routed.projection_mismatch_count),
                                hydration_count: Some(0),
                                hydration_from_contract_count: Some(0),
                                hydration_from_ledger_count: Some(0),
                                stale_ref_count: Some(0),
                                hydration_ref_missing: Some(0),
                                hydration_miss_unsupported_count: Some(0),
                                hydration_miss_stale_count: Some(0),
                                hydration_miss_no_match_count: Some(0),
                                tool_dispatch_count: Some(0),
                                tool_dispatch_success_count: Some(0),
                                tool_dispatch_failure_count: Some(0),
                                tool_dispatch_ref_write_count: Some(0),
                                tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                                input_tokens: Some(0),
                                uncached_input_tokens: Some(0),
                                output_tokens: Some(0),
                                original_prompt_chars: Some(0),
                                sent_prompt_chars: Some(0),
                                estimated_cost_micros_usd: Some(0),
                                visible_tools: Vec::new(),
                                allowed_actions: Vec::new(),
                                schema_hash: None,
                                permission_hash: None,
                                actor_role: None,
                                cwd: None,
                                config_root: None,
                                workspace_capabilities: Vec::new(),
                                tool_contract_mismatch_count: Some(0),
                                tool_contract_mismatch: None,
                                last_effective_tool_action: None,
                                last_failure_kind: None,
                                last_failure_recoverable: None,
                                last_recommended_repair: None,
                                last_failure_evidence_ref: None,
                                last_failure_bounded_excerpt: None,
                                last_failure_truncated: None,
                                recovery_attempted: None,
                                recovery_tier: None,
                                recovery_outcome: None,
                                terminal_blocker_kind: None,
                                step_failure_classification: None,
                                completion_evidence_status: None,
                                completion_evidence_gaps: Vec::new(),
                                worker_report: None,
                                success_classification: None,
                            };
                            let cwd = app_state
                                .session
                                .as_ref()
                                .map(|s| std::path::Path::new(s.cwd.as_str()).to_path_buf())
                                .unwrap_or_else(|| std::path::PathBuf::from("."));
                            let model_registry = resolve_config_root(&cwd).ok().and_then(|root| {
                                load_model_profiles_registry_from_root(&root).ok().flatten()
                            });
                            let tool_runtime = match &app_state.runtime_tool_registry {
                                Some(registry) => {
                                    let mut permissions = app_state.permission_context.clone();
                                    permissions = permissions.with_interactive_tools(true);
                                    inject_declared_writable_artifact_paths(
                                        &permissions,
                                        &routed.frame.stage_execution_contract,
                                    );
                                    Some(StateFrameToolRuntime {
                                        registry: registry.read().await.clone(),
                                        permissions,
                                        cwd: cwd.clone(),
                                        config_root: resolve_config_root(&cwd).ok(),
                                    })
                                }
                                None => None,
                            };
                            let runtime = StepRuntimeResolutionContext {
                                inherited_snapshot,
                                model_registry: model_registry.as_ref(),
                                observability: app_state.service_observability_tracker.clone(),
                                tool_runtime,
                            };
                            let outcome = run_routed_step_with_runtime(
                                routed,
                                DecisionLoopConfig::default(),
                                runtime,
                            )
                            .await?;
                            if let Some(usage) = match &outcome {
                                StepOutcome::Completed { usage, .. } => Some(usage),
                                StepOutcome::Failed {
                                    usage: Some(usage), ..
                                } => Some(usage),
                                StepOutcome::Failed { usage: None, .. } => None,
                            } {
                                Self::apply_loop_usage_to_routed_metadata(
                                    &mut routed_metadata,
                                    usage,
                                );
                            }
                            match &outcome {
                                StepOutcome::Completed {
                                    tool_registry_snapshot: Some(snapshot),
                                    ..
                                }
                                | StepOutcome::Failed {
                                    tool_registry_snapshot: Some(snapshot),
                                    ..
                                } => {
                                    routed_metadata.visible_tools = snapshot.visible_tools.clone();
                                    routed_metadata.allowed_actions =
                                        snapshot.allowed_actions.clone();
                                    routed_metadata.schema_hash =
                                        Some(snapshot.schema_hash.clone());
                                    routed_metadata.permission_hash =
                                        Some(snapshot.permission_hash.clone());
                                    routed_metadata.actor_role = Some(snapshot.actor_role.clone());
                                    routed_metadata.cwd = Some(snapshot.cwd.display().to_string());
                                    routed_metadata.config_root = snapshot
                                        .config_root
                                        .as_ref()
                                        .map(|path| path.display().to_string());
                                    routed_metadata.workspace_capabilities =
                                        snapshot.workspace_capabilities.clone();
                                }
                                _ => {}
                            }
                            if let StepOutcome::Failed {
                                failure_classification,
                                tool_contract_mismatch,
                                ..
                            } = &outcome
                            {
                                routed_metadata.step_failure_classification =
                                    Some(*failure_classification);
                                routed_metadata.tool_contract_mismatch_count =
                                    Some(tool_contract_mismatch.iter().count());
                                routed_metadata.tool_contract_mismatch =
                                    tool_contract_mismatch.clone();
                            }
                            (outcome, routed_metadata)
                        };
                        {
                            let mut routed_step_metadata = self.routed_step_metadata.write().await;
                            routed_step_metadata.insert(step_id, routed_metadata);
                        }
                        if let Some(usage) = match &outcome {
                            StepOutcome::Completed { usage, .. } => Some(usage),
                            StepOutcome::Failed {
                                usage: Some(usage), ..
                            } => Some(usage),
                            StepOutcome::Failed { usage: None, .. } => None,
                        } {
                            if !usage.tool_execution_records.is_empty() {
                                let mut plan_guard = self.plan.write().await;
                                let plan = plan_guard
                                    .as_mut()
                                    .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                let step = plan
                                    .steps
                                    .iter_mut()
                                    .find(|step| step.id == step_id)
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("Unknown boss step {step_id}")
                                    })?;
                                for record in &usage.tool_execution_records {
                                    append_step_runtime_record(step, record.clone());
                                }
                            }
                        }

                        match outcome {
                            StepOutcome::Completed { .. } => {
                                let completion_gate_failure = {
                                    let routed_metadata = self.routed_step_metadata.read().await;
                                    let metadata = routed_metadata.get(&step_id);
                                    let plan_guard = self.plan.read().await;
                                    let plan = plan_guard
                                        .as_ref()
                                        .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                    let step = plan
                                        .steps
                                        .iter()
                                        .find(|step| step.id == step_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!("Unknown boss step {step_id}")
                                        })?;
                                    step_completion_gate_error(step, metadata)
                                };
                                {
                                    let mut plan_guard = self.plan.write().await;
                                    let plan = plan_guard
                                        .as_mut()
                                        .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                    let step = plan
                                        .steps
                                        .iter_mut()
                                        .find(|step| step.id == step_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!("Unknown boss step {step_id}")
                                        })?;
                                    if let Some((reason, failure_classification)) =
                                        completion_gate_failure
                                    {
                                        step.completed = false;
                                        apply_step_failure_classification(
                                            step,
                                            failure_classification,
                                            &reason,
                                        );
                                        let repairable_continuation_dispatched =
                                            should_continue_repairable_failure(
                                                failure_classification,
                                                step.status,
                                            );
                                        drop(plan_guard);
                                        self.update_current_step(Some(step_id)).await;
                                        if !repairable_continuation_dispatched
                                            && self.get_stage().await != BossStage::Documentation
                                        {
                                            self.transition_to(BossStage::Documentation).await?;
                                        }
                                        if let Some(path) =
                                            self.status.read().await.planning_file.clone()
                                        {
                                            self.save_plan_with_session(
                                                std::path::Path::new(&path),
                                            )
                                            .await?;
                                        }
                                        if repairable_continuation_dispatched {
                                            self.mark_routed_metadata_artifact_recovery(
                                                step_id,
                                                "repair_dispatched",
                                                None,
                                            )
                                            .await;
                                        }
                                        return Ok(Some(format!(
                                            "LisM routed boss step {} into repair continuation: {}",
                                            step_id, reason
                                        )));
                                    }
                                    step.completed = true;
                                    step.status = BossPlanStepStatus::Completed;
                                }
                                if let Some(path) = self.status.read().await.planning_file.clone() {
                                    self.save_plan_with_session(std::path::Path::new(&path))
                                        .await?;
                                }
                                let next_step = self
                                    .plan
                                    .read()
                                    .await
                                    .as_ref()
                                    .and_then(|p| next_unfinished_step_id(p));
                                self.update_current_step(next_step).await;
                                if next_step.is_none() {
                                    if self.get_stage().await != BossStage::Completed {
                                        self.transition_to(BossStage::Completed).await?;
                                    }
                                    let run_id = self.current_run_id().await;
                                    let lism_enabled = effective_lism_enabled(
                                        self.lism_policy().await,
                                        app_state.permission_context.lism_enabled(),
                                    );
                                    self.emit_lism_sample_once(
                                        &run_id,
                                        lism_enabled,
                                        BossTestRunOutcome::Completed,
                                        0,
                                    )
                                    .await;
                                }
                                return Ok(Some(format!(
                                    "LisM executed boss step {} to completion.",
                                    step_id
                                )));
                            }
                            StepOutcome::Failed {
                                reason,
                                failure_classification,
                                ..
                            } => {
                                let reason_clone = reason.clone();
                                let repairable_continuation_dispatched = {
                                    let mut dispatched = false;
                                    let mut plan_guard = self.plan.write().await;
                                    let plan = plan_guard
                                        .as_mut()
                                        .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                    let step = plan
                                        .steps
                                        .iter_mut()
                                        .find(|step| step.id == step_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!("Unknown boss step {step_id}")
                                        })?;
                                    apply_step_failure_classification(
                                        step,
                                        failure_classification,
                                        &reason_clone,
                                    );
                                    dispatched = should_continue_repairable_failure(
                                        failure_classification,
                                        step.status,
                                    );
                                    dispatched
                                };
                                self.update_current_step(Some(step_id)).await;
                                if !repairable_continuation_dispatched
                                    && self.get_stage().await != BossStage::Documentation
                                {
                                    self.transition_to(BossStage::Documentation).await?;
                                }
                                if let Some(path) = self.status.read().await.planning_file.clone() {
                                    self.save_plan_with_session(std::path::Path::new(&path))
                                        .await?;
                                }
                                if repairable_continuation_dispatched {
                                    self.mark_routed_metadata_artifact_recovery(
                                        step_id,
                                        "repair_dispatched",
                                        None,
                                    )
                                    .await;
                                } else if should_emit_terminal_aborted_sample(
                                    repairable_continuation_dispatched,
                                ) {
                                    let run_id = self.current_run_id().await;
                                    let lism_enabled = effective_lism_enabled(
                                        self.lism_policy().await,
                                        app_state.permission_context.lism_enabled(),
                                    );
                                    self.emit_lism_sample_once(
                                        &run_id,
                                        lism_enabled,
                                        BossTestRunOutcome::Aborted,
                                        0,
                                    )
                                    .await;
                                }
                                return Ok(Some(if repairable_continuation_dispatched {
                                    format!(
                                        "LisM routed boss step {} into repair continuation: {}",
                                        step_id, reason_clone
                                    )
                                } else {
                                    format!("LisM failed boss step {}: {}", step_id, reason_clone)
                                }));
                            }
                        }
                    }
                }

                if let Some(policy) = step_rollout_execution_policy.as_ref() {
                    let step_size = {
                        let plan_guard = self.plan.read().await;
                        plan_guard.as_ref().and_then(|plan| {
                            let frame = build_routed_state_frame_with_model_route(
                                plan,
                                BossStage::Execution,
                                step_id,
                                ActorRole::Worker,
                            )
                            .frame;
                            serde_json::to_string(&frame).ok().map(|s| s.len())
                        })
                    };
                    let mut routed_step_metadata = self.routed_step_metadata.write().await;
                    let metadata = routed_step_metadata.entry(step_id).or_default();
                    metadata.fallback_count = Some(metadata.fallback_count.unwrap_or(0) + 1);
                    metadata.fallback_tier = Some(policy.fallback_tier.into());
                    metadata.fallback_reason = Some(policy.fallback_reason.into());
                    if metadata.state_frame_size.is_none() {
                        metadata.state_frame_size = step_size;
                    }
                    if metadata.completion_evidence_gaps.is_empty() {
                        metadata.completion_evidence_gaps = policy.affected_gaps.clone();
                    }
                }

                let force_fresh_spawn_from_policy = step_rollout_execution_policy
                    .as_ref()
                    .map(|policy| policy.force_fresh_spawn)
                    .unwrap_or(false);

                let tasks = app_state
                    .permission_context
                    .task_manager
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;

                let running_b = if force_fresh_spawn_from_policy {
                    None
                } else {
                    let guard = self.session.read().await;
                    guard
                        .as_ref()
                        .and_then(|s| self.find_running_b_task_id(s, tasks))
                };

                let payload = if let Some(b_task_id) = running_b {
                    let continue_build = self
                        .build_step_continue_payload_internal(
                            step_id,
                            &b_task_id,
                            &parent_session_id,
                        )
                        .await?;
                    self.record_b_assignment_contract(
                        &continue_build.assignment_fingerprint,
                        &continue_build.plan_version,
                        &continue_build.step_revision,
                    )
                    .await;
                    let continue_payload = continue_build.payload;

                    self.bootstrap_actor_registry_with_app_state(app_state)
                        .await;
                    if let Some(registry) = self.actor_registry.read().await.as_ref() {
                        if let Ok(crate::core::boss_actor_runtime::BossActorEvent::StepDispatched {
                            task_id,
                            ..
                        }) = registry
                            .b_mailbox()
                            .request(ExecutorBCommand::ContinueStep {
                                step_id,
                                task_id: b_task_id.clone(),
                                payload: continue_payload.clone(),
                            })
                            .await
                        {
                            self.record_step_dispatch_task_id(step_id, &task_id).await;
                        }
                    }

                    continue_payload
                } else {
                    let b_actor_id = {
                        let guard = self.session.read().await;
                        guard
                            .as_ref()
                            .map(|s| s.executor_b.actor_id.clone())
                            .unwrap_or_else(|| "boss-unknown-b".into())
                    };
                    let spawn_build = self
                        .build_step_spawn_payload_internal(step_id, &parent_session_id, &b_actor_id)
                        .await?;
                    self.record_b_assignment_contract(
                        &spawn_build.assignment_fingerprint,
                        &spawn_build.plan_version,
                        &spawn_build.step_revision,
                    )
                    .await;
                    let spawn_payload = spawn_build.payload;

                    self.bootstrap_actor_registry_with_app_state(app_state)
                        .await;
                    if let Some(registry) = self.actor_registry.read().await.as_ref() {
                        if let Ok(crate::core::boss_actor_runtime::BossActorEvent::StepDispatched {
                            task_id,
                            ..
                        }) = registry
                            .b_mailbox()
                            .request(ExecutorBCommand::DispatchStep {
                                step_id,
                                payload: spawn_payload.clone(),
                            })
                            .await
                        {
                            self.record_step_dispatch_task_id(step_id, &task_id).await;
                        }
                    }

                    spawn_payload
                };

                Ok(Some(payload))
            }
            Some(AdvanceOutcome::ReplanRequired(step_id, reason)) => {
                self.update_current_step(Some(step_id)).await;
                Ok(Some(format!(
                    "Boss step {} requires replanning before execution can continue. Reason: {}",
                    step_id, reason
                )))
            }
            Some(AdvanceOutcome::NoRunnableStep) | None => Ok(None),
        }
    }

    /// Advances the plan by selecting the next deterministic action.
    pub async fn advance_plan(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Option<String>> {
        {
            let mut auto_advance_app_state = self.auto_advance_app_state.write().await;
            if auto_advance_app_state.is_none() {
                *auto_advance_app_state = Some(app_state.clone());
            }
        }

        let mut latest_message = None;
        loop {
            let message = self.advance_once(app_state).await?;
            let should_continue = {
                let guard = self.plan.read().await;
                guard.as_ref().is_some_and(|plan| {
                    plan.auto_sequence
                        && (plan.steps.iter().any(|step| step.completed)
                            || plan
                                .steps
                                .iter()
                                .any(|step| step.status == BossPlanStepStatus::Rejected))
                        && !plan.steps.iter().all(|step| step.completed)
                        && !plan
                            .steps
                            .iter()
                            .any(|step| step.status.is_terminal_failure())
                        && !plan
                            .steps
                            .iter()
                            .any(|step| step.status == BossPlanStepStatus::Running)
                        && next_runnable_step(plan).is_some()
                })
            };
            latest_message = message;
            if !should_continue {
                break;
            }
        }
        Ok(latest_message)
    }

    /// Returns the running ExecutorB task id if one exists in the task manager.
    /// Returns None if B has no live task (caller must spawn fresh).
    fn find_running_b_task_id(
        &self,
        session: &crate::core::boss_state::BossSession,
        tasks: &crate::task::manager::TaskManager,
    ) -> Option<String> {
        let task_id = session.executor_b.task_id.as_ref()?;
        let record = tasks.get(task_id)?;
        if matches!(
            record.status,
            crate::task::types::TaskStatus::Running | crate::task::types::TaskStatus::Pending
        ) {
            Some(task_id.clone())
        } else {
            None
        }
    }

    async fn build_executor_b_assignment_contract(
        &self,
        step_id: usize,
        parent_session_id: &str,
        include_local_continuity: bool,
    ) -> anyhow::Result<ExecutorBAssignmentContract> {
        let plan_guard = self.plan.read().await;
        let plan = plan_guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter()
            .find(|step| step.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;
        let rollout_execution_policy = {
            let routed_step_metadata = self.routed_step_metadata.read().await;
            routed_step_metadata
                .get(&step_id)
                .and_then(|metadata| Self::resolve_step_rollout_execution_policy(Some(metadata)))
        };
        let projected_stage_memory = {
            let routed_step_metadata = self.routed_step_metadata.read().await;
            project_executor_b_stage_memory(step, routed_step_metadata.get(&step_id))
        };
        let plan_version = format!("{}:steps={}", plan.plan_id, plan.steps.len());
        let step_revision = format!("step-{}-attempt-{}", step.id, step.attempt_count);
        let relevant_file_handles = extract_relevant_file_handles(step.objective(), &step_revision);
        let target_files = collect_target_files(&relevant_file_handles);
        let target_artifacts = collect_target_artifacts(step, &target_files);
        let recent_decisions = collect_recent_decisions(plan, step.id);
        let open_items = if step.status == BossPlanStepStatus::Completed {
            Vec::new()
        } else {
            step.acceptance.clone()
        };
        let blocked_items = collect_blocked_items(step);
        let recent_local_facts = if include_local_continuity {
            collect_recent_local_facts(step, 5)
        } else {
            Vec::new()
        };
        let allowed_tools = default_allowed_tools();
        let worker_role = rollout_execution_policy
            .as_ref()
            .map(|policy| policy.worker_role)
            .unwrap_or(WorkerRole::Implement);
        let verification_first_short_form = worker_role == WorkerRole::Verify
            && rollout_execution_policy
                .as_ref()
                .is_some_and(|policy| policy.fallback_tier == "verification_first");
        let lism_policy = if let Some(policy) = rollout_execution_policy.as_ref() {
            policy.forced_worker_lism_policy.as_str().to_string()
        } else {
            self.worker_lism_policy().await.as_str().to_string()
        };
        let generated_at = format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        );
        let permission_scope = PermissionScopeView {
            lism_policy: lism_policy.clone(),
            inherit_context: false,
            workspace_capability: render_workspace_capability_scope(),
            boss_actor_role: "executor_b".to_string(),
        };
        let effective_objective = if verification_first_short_form {
            build_verification_first_brief_objective(step)
        } else {
            step.objective().to_string()
        };
        let effective_acceptance = if verification_first_short_form {
            build_verification_first_acceptance(step)
        } else {
            step.acceptance.clone()
        };
        let effective_relevant_file_handles = if verification_first_short_form {
            relevant_file_handles
                .iter()
                .filter(|handle| handle.kind == "target_file" || handle.kind == "target_directory")
                .cloned()
                .collect::<Vec<_>>()
        } else {
            relevant_file_handles.clone()
        };
        let effective_target_files = if verification_first_short_form {
            target_files.iter().take(1).cloned().collect::<Vec<_>>()
        } else {
            target_files.clone()
        };
        let effective_target_artifacts = if verification_first_short_form {
            target_artifacts
                .iter()
                .take(1)
                .cloned()
                .collect::<Vec<_>>()
        } else {
            target_artifacts.clone()
        };
        let brief = BossContextBrief {
            plan_id: plan.plan_id.clone(),
            step_id: step.id,
            plan_version: plan_version.clone(),
            step_revision: step_revision.clone(),
            generated_at,
            objective: effective_objective.clone(),
            acceptance: effective_acceptance.clone(),
            last_correction: step.last_correction.clone(),
            recent_decisions: if verification_first_short_form {
                Vec::new()
            } else {
                recent_decisions.clone()
            },
            relevant_file_handles: effective_relevant_file_handles.clone(),
            target_files: effective_target_files.clone(),
            target_artifacts: effective_target_artifacts.clone(),
            allowed_tools: allowed_tools.clone(),
            permission_scope: permission_scope.clone(),
            parent_session_id: parent_session_id.to_string(),
            context_strategy: BossContextStrategy::Brief,
        };
        let required_output_hint = if verification_first_short_form {
            Some(verification_first_output_contract())
        } else {
            Some(general_worker_output_contract())
        };
        let state_frame = BossStateFrame {
            step_id: step.id,
            status: step.status,
            stage_execution_contract: build_stage_execution_contract(step, &target_artifacts),
            stage_continuation_context: build_stage_continuation_context(step),
            executor_b_stage_memory: if include_local_continuity {
                projected_stage_memory.clone()
            } else {
                projected_stage_memory.as_ref().map(|memory| ExecutorBStageMemory {
                    continuity: Some(
                        memory
                            .continuity
                            .as_ref()
                            .map(|value| match value {
                                ExecutorBStageMemoryContinuity::ReuseWithinStep => {
                                    ExecutorBStageMemoryContinuity::FreshStep
                                }
                                ExecutorBStageMemoryContinuity::FullWorkerDispatchReuse => {
                                    ExecutorBStageMemoryContinuity::FullWorkerDispatchFresh
                                }
                                ExecutorBStageMemoryContinuity::FullContextReuse => {
                                    ExecutorBStageMemoryContinuity::FullContextFresh
                                }
                                other => *other,
                            })
                            .unwrap_or(ExecutorBStageMemoryContinuity::FreshStep),
                    ),
                    ..ExecutorBStageMemory::default()
                })
            },
            open_items: if verification_first_short_form {
                effective_acceptance.clone()
            } else {
                open_items
            },
            blocked_items,
            recent_local_facts,
            allowed_actions: if verification_first_short_form {
                vec!["verify_artifact".into()]
            } else {
                vec!["implement".into()]
            },
            required_output_hint,
        };
        let assignment_fingerprint = assignment_fingerprint(&json!({
            "plan_id": plan.plan_id,
            "plan_version": plan_version,
            "plan_shape": plan.steps.iter().map(|s| json!({
                "id": s.id,
                "objective": s.objective(),
                "acceptance": s.acceptance,
                "status": format!("{:?}", s.status),
            })).collect::<Vec<_>>(),
            "step_id": step.id,
            "step_revision": step_revision,
            "objective": effective_objective,
            "acceptance": effective_acceptance,
            "last_correction": step.last_correction,
            "recent_decisions": recent_decisions,
            "relevant_file_handles": effective_relevant_file_handles,
            "target_files": effective_target_files,
            "target_artifacts": effective_target_artifacts,
            "allowed_tools": allowed_tools,
            "permission_scope": {
                "lism_policy": permission_scope.lism_policy,
                "inherit_context": permission_scope.inherit_context,
                "workspace_capability": permission_scope.workspace_capability,
                "boss_actor_role": permission_scope.boss_actor_role,
            },
            "parent_session_id": parent_session_id,
        }));

        Ok(ExecutorBAssignmentContract {
            brief,
            state_frame,
            allowed_tools,
            lism_policy,
            worker_role,
            assignment_fingerprint,
        })
    }

    async fn record_b_assignment_contract(
        &self,
        assignment_fingerprint: &str,
        plan_version: &str,
        step_revision: &str,
    ) {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.last_assignment_fingerprint =
                Some(assignment_fingerprint.to_string());
            session.executor_b.last_assignment_plan_version = Some(plan_version.to_string());
            session.executor_b.last_assignment_step_revision = Some(step_revision.to_string());
        }
    }

    async fn build_step_continue_payload_internal(
        &self,
        step_id: usize,
        b_task_id: &str,
        parent_session_id: &str,
    ) -> anyhow::Result<ContinuePayloadBuild> {
        let contract = self
            .build_executor_b_assignment_contract(step_id, parent_session_id, true)
            .await?;
        let prior_assignment = {
            let guard = self.session.read().await;
            guard.as_ref().map(|session| {
                (
                    session.executor_b.last_assignment_fingerprint.clone(),
                    session.executor_b.last_assignment_plan_version.clone(),
                    session.executor_b.last_assignment_step_revision.clone(),
                )
            })
        };
        let needs_refresh = prior_assignment
            .as_ref()
            .map(|(fingerprint, _, _)| {
                fingerprint.as_deref() != Some(contract.assignment_fingerprint.as_str())
            })
            .unwrap_or(true);
        let refresh_reason = if !needs_refresh {
            None
        } else {
            let (prior_plan_version, prior_step_revision) = prior_assignment
                .as_ref()
                .map(|(_, plan_version, step_revision)| {
                    (
                        plan_version.as_deref().unwrap_or("unknown"),
                        step_revision.as_deref().unwrap_or("unknown"),
                    )
                })
                .unwrap_or(("none", "none"));
            Some(format!(
                "stale brief detected: prior plan_version={prior_plan_version} prior step_revision={prior_step_revision}"
            ))
        };
        let verification_first_short_form = is_verification_first_assignment_contract(&contract);
        let verification_first_task_message =
            verification_first_short_form.then(|| build_verification_first_task_message(&contract));
        let message = if needs_refresh {
            format!(
                "Boss assignment refresh for step {step_id}\n\
IMPORTANT: discard any previous brief for this executor session and replace it with the refreshed brief below.\n\
refresh_reason: {}\n\n{}",
                refresh_reason
                    .clone()
                    .unwrap_or_else(|| "assignment contract changed".into()),
                verification_first_task_message
                    .clone()
                    .unwrap_or_else(|| assemble_brief_prompt(&contract.brief, &contract.state_frame)),
            )
        } else {
            verification_first_task_message.clone().unwrap_or_else(|| {
                format!(
                    "Boss step {step_id}\nplan_id: {}\nobjective: {}\nacceptance:\n{}{}",
                    contract.brief.plan_id,
                    contract.brief.objective,
                    format_acceptance_from_items(&contract.brief.acceptance),
                    render_recent_local_facts_section(&contract.state_frame.recent_local_facts),
                )
            })
        };
        let plan_id = contract.brief.plan_id.clone();
        let objective = contract.brief.objective.clone();
        let acceptance = contract.brief.acceptance.clone();
        let plan_version = contract.brief.plan_version.clone();
        let step_revision = contract.brief.step_revision.clone();
        let assignment_fingerprint = contract.assignment_fingerprint.clone();
        let continuation_payload = build_continuation_payload(&contract);
        let executor_b_stage_memory = build_stage_memory_payload(&contract);
        let payload = json!({
            "task_id": b_task_id,
            "message": message,
            "step_id": step_id,
            "boss_plan_id": plan_id,
            "step_objective": objective,
            "step_acceptance": acceptance,
            "parent_session_id": parent_session_id,
            "plan_version": plan_version,
            "step_revision": step_revision,
            "assignment_fingerprint": assignment_fingerprint,
            "stale_brief_action": if needs_refresh { "refresh" } else { "reuse" },
            "refresh_reason": refresh_reason,
            "refresh_task": if needs_refresh {
                Some(
                    verification_first_task_message
                        .clone()
                        .unwrap_or_else(|| assemble_brief_prompt(&contract.brief, &contract.state_frame))
                )
            } else {
                None
            },
            "continuation_payload": continuation_payload,
            "executor_b_stage_memory": executor_b_stage_memory,
            "recent_local_facts": if verification_first_short_form {
                Vec::<String>::new()
            } else {
                contract.state_frame.recent_local_facts.clone()
            },
            "allowed_tools": contract.allowed_tools,
            "lism_policy": contract.lism_policy,
            "task_contains_boss_context": needs_refresh,
        })
        .to_string();

        Ok(ContinuePayloadBuild {
            payload,
            assignment_fingerprint: contract.assignment_fingerprint,
            plan_version: contract.brief.plan_version,
            step_revision: contract.brief.step_revision,
        })
    }

    /// Builds a Continue payload that sends step context to a running ExecutorB task.
    pub async fn build_step_continue_payload(
        &self,
        step_id: usize,
        b_task_id: &str,
        parent_session_id: &str,
    ) -> anyhow::Result<String> {
        Ok(self
            .build_step_continue_payload_internal(step_id, b_task_id, parent_session_id)
            .await?
            .payload)
    }

    async fn build_step_spawn_payload_internal(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<SpawnPayloadBuild> {
        let contract = self
            .build_executor_b_assignment_contract(step_id, parent_session_id, false)
            .await?;
        let plan_id = contract.brief.plan_id.clone();
        let objective = contract.brief.objective.clone();
        let acceptance = contract.brief.acceptance.clone();
        let plan_version = contract.brief.plan_version.clone();
        let step_revision = contract.brief.step_revision.clone();
        let assignment_fingerprint = contract.assignment_fingerprint.clone();
        let continuation_payload = build_continuation_payload(&contract);
        let executor_b_stage_memory = build_stage_memory_payload(&contract);
        let verification_first_task_message =
            is_verification_first_assignment_contract(&contract)
                .then(|| build_verification_first_task_message(&contract));
        let payload = json!({
            "task": verification_first_task_message
                .clone()
                .unwrap_or_else(|| assemble_brief_prompt(
                    &contract.brief,
                    &contract.state_frame,
                )),
            "task_contains_boss_context": true,
            "role": contract.worker_role.as_str(),
            "inherit_context": false,
            "allowed_tools": contract.allowed_tools,
            "lism_policy": contract.lism_policy,
            "context_strategy": "brief",
            "reuse_strategy": match contract.worker_role {
                WorkerRole::Verify => "fresh",
                WorkerRole::Implement => "running_only",
                WorkerRole::Research => "running_only",
            },
            "step_id": contract.brief.step_id,
            "boss_plan_id": plan_id,
            "step_objective": objective,
            "step_acceptance": acceptance,
            "parent_session_id": parent_session_id,
            "parent_runtime_role": "coordinator",
            "orchestration_group_id": b_actor_id,
            "boss_actor_role": "executor_b",
            "boss_lineage_depth": 0,
            "plan_version": plan_version,
            "step_revision": step_revision,
            "assignment_fingerprint": assignment_fingerprint,
            "continuation_payload": continuation_payload,
            "executor_b_stage_memory": executor_b_stage_memory,
        })
        .to_string();

        Ok(SpawnPayloadBuild {
            payload,
            assignment_fingerprint: contract.assignment_fingerprint,
            plan_version: contract.brief.plan_version,
            step_revision: contract.brief.step_revision,
        })
    }

    pub async fn build_step_spawn_payload(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<String> {
        Ok(self
            .build_step_spawn_payload_internal(step_id, parent_session_id, b_actor_id)
            .await?
            .payload)
    }

    async fn invoke_agent_tool(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        payload: &str,
    ) -> anyhow::Result<()> {
        self.invoke_agent_tool_with_task_id(app_state, payload)
            .await
            .map(|_| ())
    }

    /// Like `invoke_agent_tool` but returns the spawned task id extracted from the result text.
    async fn invoke_agent_tool_with_task_id(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        payload: &str,
    ) -> anyhow::Result<String> {
        let agent_tool = crate::tool::builtin::agent::AgentTool;
        let result = agent_tool
            .invoke(
                &ToolCall::new("Agent", payload),
                &app_state.permission_context,
            )
            .await?;

        match result {
            crate::tool::definition::ToolResult::Text(text) => {
                // Result text is "agent task {task_id} respawned for ..." or "agent task {task_id} reused for ..."
                let task_id = text
                    .split_whitespace()
                    .nth(2)
                    .unwrap_or("unknown")
                    .to_string();
                Ok(task_id)
            }
            crate::tool::definition::ToolResult::Denied(reason) => {
                anyhow::bail!("boss worker dispatch denied: {reason}")
            }
            crate::tool::definition::ToolResult::PendingApproval { message, .. } => {
                anyhow::bail!("boss worker dispatch requires approval: {message}")
            }
            crate::tool::definition::ToolResult::Interrupted(reason) => {
                anyhow::bail!("boss worker dispatch interrupted: {reason}")
            }
            crate::tool::definition::ToolResult::Progress(message) => {
                anyhow::bail!(
                    "boss worker dispatch returned progress instead of spawn result: {message}"
                )
            }
            crate::tool::definition::ToolResult::ResultTooLarge(reason) => {
                anyhow::bail!("boss worker dispatch returned oversized result: {reason}")
            }
        }
    }

    /// Ensure Designer A has a real LLM session. On first call, spawns an A session via
    /// AgentTool and writes the task id back to BossSession.designer_a.session_id.
    /// Subsequent calls are no-ops if the session id is already a real task id.
    async fn ensure_a_session(&self, app_state: &Arc<crate::state::app_state::AppState>) {
        // Without a task manager there is no way to track the spawned session — skip.
        if app_state.permission_context.task_manager.is_none() {
            return;
        }
        // Check if already initialized to a real session id (not the deterministic placeholder).
        let placeholder = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| {
                    let placeholder = format!("boss-{}-a", s.plan_id);
                    s.designer_a.session_id == placeholder || s.designer_a.session_id.is_empty()
                })
                .unwrap_or(true)
        };
        if !placeholder {
            return;
        }

        let payload = match self.build_a_session_payload(app_state).await {
            Ok(p) => p,
            Err(_) => return,
        };

        if let Ok(task_id) = self
            .invoke_agent_tool_with_task_id(app_state, &payload)
            .await
        {
            let mut guard = self.session.write().await;
            if let Some(session) = guard.as_mut() {
                session.designer_a.session_id = task_id.clone();
                session.designer_a.task_id = Some(task_id);
                session.designer_a.status = crate::core::boss_state::BossActorStatus::Active;
            }
        }
    }

    /// Send a message to A's running LLM session via AgentTool Continue.
    /// Requires `ensure_a_session` to have been called first so `designer_a.session_id` is real.
    /// Fire-and-forget: enqueues the message into A's mailbox; does not wait for A's reply.
    async fn send_to_a_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.designer_a.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            return;
        }
        {
            let mut guard = self.status.write().await;
            guard.last_a_dispatch_message = Some(message.clone());
        }
        let payload = json!({ "task_id": task_id, "message": message }).to_string();
        let _ = self.invoke_agent_tool(app_state, &payload).await;
    }

    /// Send a message to A's running LLM session and wait for A's response.
    /// Returns A's response text, or an error if A is not running or times out.
    /// Polls `TaskManager::get_output` until new content appears after the message is sent.
    async fn ask_a_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) -> anyhow::Result<String> {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.designer_a.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            anyhow::bail!("A session not initialized");
        }

        let tasks = app_state
            .permission_context
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;
        let session_id = app_state
            .permission_context
            .active_session_id
            .as_deref()
            .unwrap_or("");

        // Record current output offset before sending.
        let offset_before = tasks
            .get_output(&task_id, 0)
            .map(|s| s.next_offset)
            .unwrap_or(0);

        // Record the dispatch message for observability.
        {
            let mut guard = self.status.write().await;
            guard.last_a_dispatch_message = Some(message.clone());
        }

        // send_message uses std::sync::RwLock internally — run it on the blocking pool
        // so it doesn't stall the async runtime thread.
        let tasks_clone = tasks.clone();
        let task_id_clone = task_id.clone();
        let session_id_owned = session_id.to_string();
        let message_clone = message.clone();
        let sent = tokio::task::spawn_blocking(move || {
            tasks_clone.send_message(&task_id_clone, &session_id_owned, message_clone)
        })
        .await
        .unwrap_or(false);
        if !sent {
            anyhow::bail!("A session task {task_id} is not running");
        }

        // Poll for new output with a timeout.
        let timeout_secs = 30u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("A session response timed out after 30s");
            }
            if let Some(slice) = tasks.get_output(&task_id, offset_before) {
                if !slice.content.is_empty() {
                    return Ok(slice.content);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Parse A's LLM response text into a structured review decision.
    fn parse_a_review_decision(
        response: &str,
        summary: &str,
    ) -> crate::core::boss_actor_runtime::ReviewDecision {
        let upper = response.to_uppercase();
        if upper.contains("REPLAN_STEP") {
            let reason = response
                .to_uppercase()
                .find("REASON:")
                .map(|pos| response[pos + "REASON:".len()..].trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "review requested step replanning".to_string());
            return crate::core::boss_actor_runtime::ReviewDecision::ReplanStep {
                summary: summary.to_string(),
                reason,
            };
        }
        if upper.contains("REJECT") {
            let correction = response
                .to_uppercase()
                .find("CORRECTION:")
                .map(|pos| response[pos + "CORRECTION:".len()..].trim().to_string())
                .filter(|s| !s.is_empty());
            return crate::core::boss_actor_runtime::ReviewDecision::Correct {
                summary: summary.to_string(),
                correction,
            };
        }
        crate::core::boss_actor_runtime::ReviewDecision::Accept {
            summary: summary.to_string(),
        }
    }

    /// Summarize `old_part` via A session. Returns A's response string.
    /// If A is unavailable or the ask fails, returns Err — caller must fallback.
    /// Note: this call goes through ask_a_session and may leave a trace in A's runtime history.
    /// It does NOT write to BossPlan or session_snapshot.
    async fn summarize_context_with_a(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        old_part: &str,
    ) -> anyhow::Result<String> {
        let prompt = format!(
            "Summarize the following context concisely so it can replace the original in a continuation prompt. Preserve key decisions, constraints, and outcomes:\n\n{}",
            old_part
        );
        self.ask_a_session(app_state, prompt).await
    }

    /// Summarize `old_part` via a stateless one-shot provider call.
    /// Does NOT go through any session actor — A session history is never touched.
    /// Returns Err if the active model runtime is unavailable or the call fails.
    async fn summarize_context_stateless(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        old_part: &str,
    ) -> anyhow::Result<String> {
        let runtime = app_state
            .active_model_runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("active model runtime not available"))?;
        let snapshot = runtime.snapshot().await;
        let prompt = format!(
            "Summarize the following context concisely so it can replace the original in a continuation prompt. Preserve key decisions, constraints, and outcomes:\n\n{}",
            old_part
        );
        let msg = crate::core::message::Message::user(prompt);
        let events = snapshot.client.stream_message(&msg).await;
        let text: String = events
            .into_iter()
            .filter_map(|e| {
                if let crate::service::api::streaming::StreamEvent::TextDelta(t) = e {
                    Some(t)
                } else {
                    None
                }
            })
            .collect();
        if text.is_empty() {
            anyhow::bail!("stateless summarize returned empty response");
        }
        Ok(text)
    }

    /// Mirrors `ask_a_session` but reads from `executor_b.session_id`.
    async fn ask_b_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) -> anyhow::Result<String> {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.executor_b.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            anyhow::bail!("B session not initialized");
        }

        let tasks = app_state
            .permission_context
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;
        let session_id = app_state
            .permission_context
            .active_session_id
            .as_deref()
            .unwrap_or("")
            .to_string();

        // T26.5: dispatch-time budget gate — runs before T25/T25.2 trim/summarize.
        // Reject immediately if even the static prefix exceeds budget.
        if let BudgetDecision::Reject { reason } = evaluate_message_budget(&message) {
            anyhow::bail!("prompt budget exceeded: {reason}");
        }

        // Compress outbound payload before sending — does not modify BossPlan or session_snapshot.
        // Prefer LLM summarize via stateless provider call; fall back to character-truncation trim if unavailable.
        let original_chars = message.len();
        let (message, compression_strategy) = if message.len() > B_CONTEXT_TRIM_THRESHOLD {
            let split = message.len().saturating_sub(B_CONTEXT_KEEP_CHARS);
            let old_part = &message[..split];
            let recent_tail = &message[split..];
            match self.summarize_context_stateless(app_state, old_part).await {
                Ok(summary) => (
                    assemble_summarized_payload(&summary, recent_tail),
                    CompressionStrategy::Summarized,
                ),
                Err(_) => (
                    trim_context_payload(&message, B_CONTEXT_TRIM_THRESHOLD, B_CONTEXT_KEEP_CHARS),
                    CompressionStrategy::Trimmed,
                ),
            }
        } else {
            (message, CompressionStrategy::None)
        };

        // Record the final outbound message and per-dispatch metrics for test observability.
        {
            let mut guard = self.status.write().await;
            guard.last_b_ask_message = Some(message.clone());
            guard.last_step_metrics = Some(BossStepMetrics {
                compression_strategy,
                context_mode: ContextMode::Brief,
                original_chars,
                sent_chars: message.len(),
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
                cache_prefix_instability: false,
            });
        }

        let offset_before = tasks
            .get_output(&task_id, 0)
            .map(|s| s.next_offset)
            .unwrap_or(0);

        let tasks_clone = tasks.clone();
        let task_id_clone = task_id.clone();
        let message_clone = message.clone();
        let sent = tokio::task::spawn_blocking(move || {
            tasks_clone.send_message(&task_id_clone, &session_id, message_clone)
        })
        .await
        .unwrap_or(false);
        if !sent {
            anyhow::bail!("B session task {task_id} is not running");
        }

        let timeout_secs = 30u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("B session response timed out after 30s");
            }
            if let Some(slice) = tasks.get_output(&task_id, offset_before) {
                if !slice.content.is_empty() {
                    return Ok(slice.content);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Fire-and-forget: send a message to B's running LLM session without waiting for a reply.
    async fn send_to_b_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.executor_b.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            return;
        }
        let payload = json!({ "task_id": task_id, "message": message }).to_string();
        let _ = self.invoke_agent_tool(app_state, &payload).await;
    }

    /// Build the JSON payload for spawning Designer A's LLM session.
    async fn build_a_session_payload(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<String> {
        let plan_id = self
            .plan
            .read()
            .await
            .as_ref()
            .map(|p| p.plan_id.clone())
            .unwrap_or_default();
        let parent_session_id = app_state.active_session_id.clone();
        Ok(json!({
            "task": format!(
                "Designer A review session for plan {plan_id}. Stay idle until the coordinator sends the actual review/spec content. Do not search the repository, and do not use Glob/Grep just to locate the plan id."
            ),
            "role": "research",
            "allowed_tools": ["Read"],
            "boss_plan_id": plan_id,
            "step_objective": "Review and approve boss plan steps as Designer A. Use only the text provided in-session unless a later message explicitly names a file path to inspect.",
            "parent_session_id": parent_session_id,
            "reuse_strategy": "running_only",
        })
        .to_string())
    }

    async fn update_current_step(&self, current_step: Option<usize>) {
        let mut status = self.status.write().await;
        status.current_step = current_step;
    }

    async fn update_step_status(
        &self,
        step_id: usize,
        next_status: BossPlanStepStatus,
    ) -> anyhow::Result<()> {
        let mut plan_guard = self.plan.write().await;
        let plan = plan_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter_mut()
            .find(|step| step.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;
        step.status = next_status;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AdvanceOutcome {
    Dispatch(usize),
    ApprovalBarrier(usize),
    TerminalFailure(String),
    PlanComplete,
    ReplanRequired(usize, String),
    NoRunnableStep,
}

fn model_tier_label(tier: ModelTier) -> &'static str {
    match tier {
        ModelTier::Low => "low",
        ModelTier::Medium => "medium",
        ModelTier::High => "high",
    }
}

pub(crate) fn effective_lism_enabled(policy: BossLisMPolicy, session_lism: bool) -> bool {
    match policy {
        BossLisMPolicy::Inherit => session_lism,
        BossLisMPolicy::ForceOn => true,
        BossLisMPolicy::ForceOff => false,
    }
}

fn next_unfinished_step_id(plan: &BossPlan) -> Option<usize> {
    plan.steps
        .iter()
        .find(|step| !step.completed)
        .map(|step| step.id)
}

fn next_runnable_step(plan: &BossPlan) -> Option<&BossPlanStep> {
    let verification_repair_continuation = plan.steps.iter().find(|step| {
        !step.completed
            && matches!(
                step.status,
                BossPlanStepStatus::Rejected | BossPlanStepStatus::Reviewing
            )
            && step
                .stage_continuation_context
                .as_ref()
                .and_then(|context| context.next_action.as_deref())
                .is_some_and(|action| {
                    action.eq_ignore_ascii_case("verify_artifact")
                        || action.eq_ignore_ascii_case("run_verification")
                })
    });
    verification_repair_continuation.or_else(|| {
        plan.steps.iter().find(|step| {
            !step.completed
                && matches!(
                    step.status,
                    BossPlanStepStatus::Pending | BossPlanStepStatus::Rejected
                )
        })
    })
}

fn format_acceptance(step: &BossPlanStep) -> String {
    format_acceptance_from_items(&step.acceptance)
}

fn format_acceptance_from_items(items: &[String]) -> String {
    if items.is_empty() {
        "- Complete the step objective.".into()
    } else {
        items
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn history_summary_from_app_state(
    app_state: &crate::state::app_state::AppState,
) -> Option<Vec<String>> {
    if let Some(history) = &app_state.history {
        return Some(history_summary_from_history(history));
    }

    let session_store = app_state.session_store.as_ref()?;
    let session = app_state.session.as_ref()?;
    let request = crate::history::session::SessionRestoreRequest {
        resume: Some(session.session_id.0.clone()),
        continue_session: false,
    };
    let (_, history) = session_store.load(&request)?;
    Some(history_summary_from_history(&history))
}

fn history_summary_from_history(history: &SessionHistory) -> Vec<String> {
    history
        .entries
        .iter()
        .rev()
        .take(3)
        .map(|entry| {
            let text = entry.message.text();
            let text = text.lines().next().unwrap_or("").trim();
            if text.is_empty() {
                "(empty history entry)".into()
            } else {
                text.to_string()
            }
        })
        .collect::<Vec<_>>()
}

impl Default for BossCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Saves a boss plan to a file using atomic write to prevent corruption.
pub async fn save_plan(plan: &BossPlan, path: &std::path::Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let content = serde_json::to_string_pretty(plan)?;
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, content).await?;
    tokio::fs::rename(tmp_path, path).await?;

    Ok(())
}

/// Loads a boss plan from a file (free function, no self needed).
pub async fn load_plan(path: &std::path::Path) -> anyhow::Result<BossPlan> {
    let content = tokio::fs::read_to_string(path).await?;
    let plan = serde_json::from_str(&content)?;
    Ok(plan)
}

/// Default character threshold above which outbound B context payloads are trimmed.
pub const B_CONTEXT_TRIM_THRESHOLD: usize = 80_000;
/// Default number of characters to keep (tail window) when trimming.
pub const B_CONTEXT_KEEP_CHARS: usize = 40_000;

/// Trim an outbound B context payload to at most `keep_chars` characters.
///
/// If `payload.len() <= threshold`, returns the payload unchanged.
/// Otherwise, keeps the last `keep_chars` characters and prepends a fixed notice line
/// so the provider knows earlier context was omitted.
///
/// This operates only on the runtime payload string — it never touches BossPlan,
/// session_snapshot, or any persisted state.
pub fn trim_context_payload(payload: &str, threshold: usize, keep_chars: usize) -> String {
    if payload.len() <= threshold {
        return payload.to_string();
    }
    let omitted = payload.len().saturating_sub(keep_chars);
    let tail = if keep_chars >= payload.len() {
        payload
    } else {
        &payload[payload.len() - keep_chars..]
    };
    format!("[trimmed earlier context: {omitted} chars omitted]\n{tail}")
}

/// Assemble a summarized B context payload from a pre-computed summary and the recent tail.
/// This is the output shape produced by the summarize-first path in `ask_b_session`.
/// Exposed as a pure function so tests can verify the assembly contract without a live A session.
pub fn assemble_summarized_payload(summary: &str, recent_tail: &str) -> String {
    format!("[summary: {summary}]\n{recent_tail}")
}

impl BossCoordinator {
    /// Save the current plan to disk, embedding the current session snapshot so
    /// A/B identity (session_id / task_id) survives a restart.
    /// Liveness of the stored task_id is NOT guaranteed — callers must do a
    /// live-task check before reusing it.
    async fn save_plan_with_session(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let session_snap = self.session.read().await.clone();
        let mut plan_guard = self.plan.write().await;
        if let Some(plan) = plan_guard.as_mut() {
            plan.session_snapshot = session_snap;
            save_plan(plan, path).await?;
        }
        Ok(())
    }

    pub async fn ask_b_session_pub(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) -> anyhow::Result<String> {
        self.ask_b_session(app_state, message).await
    }

    pub async fn build_b_step_payload_pub(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<String> {
        self.build_step_spawn_payload(step_id, parent_session_id, b_actor_id)
            .await
    }

    pub async fn repair_replan_step(
        &self,
        step_id: usize,
        patched_description: String,
        patched_objective: Option<String>,
        patched_acceptance: Vec<String>,
    ) -> anyhow::Result<()> {
        let plan_path = {
            self.status
                .read()
                .await
                .planning_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("No planning file configured"))?
        };

        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            let step = plan
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;

            if step.status != BossPlanStepStatus::ReplanRequired {
                anyhow::bail!(
                    "Boss step {} is not awaiting replanning (current status: {:?})",
                    step_id,
                    step.status
                );
            }

            step.description = patched_description;
            step.objective = patched_objective;
            step.acceptance = patched_acceptance;
            step.status = BossPlanStepStatus::Pending;
            step.completed = false;
            step.worker_task_id = None;
            step.review_task_id = None;
            step.attempt_count = 0;
            step.last_correction = None;
        }

        self.update_current_step(Some(step_id)).await;
        self.save_plan_with_session(std::path::Path::new(&plan_path))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
    use crate::cost::tracker::CostTracker;
    use crate::core::state_frame::{
        AgentState, CompletionEvidenceGap, CompletionEvidenceStatus, ContinuityMode,
        DeclaredArtifactContract, RepairIntent, WorkerStructuredReport,
    };
    use crate::core::boss_state::BossActorRole;
    use crate::core::state_frame_loop::LoopUsage;
    use crate::interaction::dispatcher::NotificationDispatcher;
    use crate::interaction::telegram::gateway::TelegramGateway;
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::app_state::{
        ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
    };
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use crate::task::manager::TaskManager;
    use crate::task::types::{TaskStatus, TaskType};
    use crate::tool::result::{
        ToolBatchContext, ToolExecutionOutcomeKind, ToolExecutionRecord, ToolOutcome,
        ToolOutcomeKind, ToolReportModifier,
    };
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    fn test_app_state_with_tasks(
        task_manager: Arc<TaskManager>,
        boss: Arc<BossCoordinator>,
    ) -> Arc<AppState> {
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(boss.clone());
        let permission_context = ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(task_manager)
            .with_active_session_id("test-session")
            .with_active_surface(InteractionSurface::Cli)
            .with_notification_dispatcher(dispatcher.clone())
            .with_boss_coordinator(boss);
        Arc::new(AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: dispatcher,
            audit_log: Arc::new(Mutex::new(crate::security::audit::AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "test-provider".into(),
                protocol: "MessagesApi".into(),
                compatibility_profile: "MessagesApi".into(),
                base_url_host: "localhost".into(),
                model: "test-model".into(),
                auth_status: "unset".into(),
            },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(AtomicU64::new(0)),
            cancellation_token: CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
        })
    }

    #[tokio::test]
    async fn test_boss_coordinator_initial_stage_is_documentation() {
        let coordinator = BossCoordinator::new();
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn completed_child_task_advances_boss_state_immediately() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        coordinator.advance_plan(&app_state).await.expect("advance");

        let plan = coordinator.plan.read().await;
        let step = &plan.as_ref().expect("plan").steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Completed);
        assert!(step.completed);
        assert_eq!(coordinator.get_stage().await, BossStage::Completed);
    }

    #[tokio::test]
    async fn outer_coordinator_does_not_poll_completed_child_until_timeout() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        let message = coordinator.advance_plan(&app_state).await.expect("advance");

        assert!(
            message
                .as_deref()
                .is_some_and(|value| value.contains("Boss plan complete"))
        );
        assert_ne!(coordinator.get_stage().await, BossStage::Execution);
    }

    #[tokio::test]
    async fn terminal_child_status_syncs_boss_state_before_wait_loop() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");

        {
            let mut plan = coordinator.plan.write().await;
            let step = &mut plan.as_mut().expect("plan").steps[0];
            step.status = BossPlanStepStatus::Running;
        }

        let event = crate::task::types::TaskEvent {
            owner: crate::task::types::TaskOwner {
                session_id: "test-session".into(),
                surface: InteractionSurface::Cli,
            },
            target_task_id: Some("task-0".into()),
            task_id: "task-0".into(),
            task_type: TaskType::LocalAgent,
            status: TaskStatus::Completed,
            summary: String::new(),
            result: String::new(),
            next_action: String::new(),
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            step_id: Some(0),
            output_file: record.output_file,
            usage: None,
        };
        coordinator.on_task_event(&event).await.expect("event");
        coordinator.advance_plan(&app_state).await.expect("advance");

        let plan = coordinator.plan.read().await;
        let step = &plan.as_ref().expect("plan").steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Completed);
        assert!(step.completed);
        assert_eq!(coordinator.status.read().await.current_step, None);
    }

    #[tokio::test]
    async fn full_worker_dispatch_completed_child_syncs_without_timeout_tail() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            coordinator
                .sync_terminal_child_task_state(tasks.as_ref())
                .await
                .expect("sync")
        );
    }

    #[tokio::test]
    async fn sync_terminal_child_task_state_prefers_current_step_over_scan() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(1);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![
                    BossPlanStep {
                        id: 0,
                        description: "stale step".into(),
                        objective: Some("ignore".into()),
                        acceptance: Vec::new(),
                        requires_approval: false,
                        status: BossPlanStepStatus::Running,
                        completed: false,
                        result_diff: None,
                        worker_task_id: Some("task-0".into()),
                        attempt_count: 0,
                        retry_budget: 3,
                        last_review_summary: None,
                        last_correction: None,
                        stage_execution_contract: StageExecutionContract::default(),
                        stage_continuation_context: None,
                        executor_b_stage_memory: None,
                        review_task_id: None,
                        tool_execution_records: Vec::new(),
                    },
                    BossPlanStep {
                        id: 1,
                        description: "current step".into(),
                        objective: Some("write artifact".into()),
                        acceptance: Vec::new(),
                        requires_approval: false,
                        status: BossPlanStepStatus::Running,
                        completed: false,
                        result_diff: None,
                        worker_task_id: Some("task-1".into()),
                        attempt_count: 0,
                        retry_budget: 3,
                        last_review_summary: None,
                        last_correction: None,
                        stage_execution_contract: StageExecutionContract::default(),
                        stage_continuation_context: None,
                        executor_b_stage_memory: None,
                        review_task_id: None,
                        tool_execution_records: Vec::new(),
                    },
                ],
                ..BossPlan::default()
            });
        }
        let record0 = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record0.id, "task-0");
        tasks.start("task-0");
        let record1 = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record1.id, "task-1");
        tasks.start("task-1");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tasks.complete("task-1", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            coordinator
                .sync_terminal_child_task_state(tasks.as_ref())
                .await
                .expect("sync")
        );
        let plan = coordinator.plan.read().await;
        let step0 = &plan.as_ref().expect("plan").steps[0];
        let step1 = &plan.as_ref().expect("plan").steps[1];
        assert_eq!(step0.status, BossPlanStepStatus::Running);
        assert_eq!(step1.status, BossPlanStepStatus::Completed);
        assert!(step1.completed);
    }

    #[tokio::test]
    async fn full_dispatch_completed_child_syncs_even_if_session_task_id_drifted() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut session = coordinator.session.write().await;
            *session = Some(BossSession::from_plan_id("plan", BossStage::Execution));
            if let Some(snapshot) = session.as_mut() {
                snapshot.executor_b.task_id = Some("task-0".into());
            }
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-1".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let stale = tasks.create_with_type(
            "stale worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        let current = tasks.create_with_type(
            "current worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(stale.id, "task-0");
        assert_eq!(current.id, "task-1");
        tasks.start(&stale.id);
        tasks.start(&current.id);
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete(&current.id, &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            coordinator
                .sync_terminal_child_task_state(tasks.as_ref())
                .await
                .expect("sync")
        );
        let plan = coordinator.plan.read().await;
        let step = &plan.as_ref().expect("plan").steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Completed);
        assert_eq!(step.worker_task_id.as_deref(), Some("task-1"));
    }

    #[tokio::test]
    async fn recorded_dispatch_task_id_is_not_overwritten_by_unrelated_new_task() {
        let coordinator = Arc::new(BossCoordinator::new());
        {
            let mut session = coordinator.session.write().await;
            *session = Some(BossSession::from_plan_id("plan", BossStage::Execution));
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }

        coordinator.record_step_dispatch_task_id(0, "task-real").await;

        let tasks = TaskManager::new_with_output_root(std::env::temp_dir());
        let unrelated = tasks.create_with_type(
            "unrelated",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(unrelated.id, "task-0");

        assert_eq!(coordinator.b_task_id().await.as_deref(), Some("task-real"));
        assert_eq!(
            coordinator.current_step_worker_task_id().await.as_deref(),
            None
        );
        let plan = coordinator.plan.read().await;
        assert_eq!(
            plan.as_ref().expect("plan").steps[0].worker_task_id.as_deref(),
            Some("task-real")
        );
    }

    #[tokio::test]
    async fn completed_child_current_step_does_not_return_ok_none_in_execution_tail() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "run worker".into(),
                    objective: Some("write artifact".into()),
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        let message = coordinator.advance_plan(&app_state).await.expect("advance");

        assert!(
            message.is_some(),
            "completed child tail should not resolve to Ok(None)"
        );
        assert_eq!(coordinator.get_stage().await, BossStage::Completed);
    }

    #[test]
    fn next_runnable_step_treats_verification_reviewing_step_as_runnable() {
        let step = BossPlanStep {
            id: 0,
            description: "verify target".into(),
            objective: Some("verify artifact".into()),
            acceptance: vec!["artifact verification passed".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Reviewing,
            completed: false,
            result_diff: None,
            worker_task_id: Some("task-0".into()),
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/target".into()),
                    verified_facts: vec!["verified".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/target".into()),
                verified_facts: vec!["verified".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        let plan = BossPlan {
            accepted_by_user: true,
            auto_sequence: true,
            steps: vec![step],
            ..BossPlan::default()
        };

        let runnable = next_runnable_step(&plan).map(|step| step.id);
        assert_eq!(runnable, Some(0));
    }

    #[tokio::test]
    async fn verification_continuation_step_advances_after_completed_child_sync() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        let target_path = std::env::temp_dir().join(format!(
            "boss_verification_advances_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&target_path, "verified").expect("write target");
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some(format!(
                        "任务目标：\n- 目标文件：{}\n- 验证文件存在且非空",
                        target_path.display()
                    )),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some(target_path.display().to_string()),
                            verified_facts: vec!["verified".into()],
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some(target_path.display().to_string()),
                        verified_facts: vec!["verified".into()],
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        let message = coordinator.advance_plan(&app_state).await.expect("advance");

        assert!(message.is_some());
        assert_eq!(coordinator.get_stage().await, BossStage::Completed);
    }

    #[tokio::test]
    async fn advance_plan_does_not_return_none_for_reviewing_verification_continuation() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some("verify artifact".into()),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some("/tmp/target".into()),
                            verified_facts: vec!["verified".into()],
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some("/tmp/target".into()),
                        verified_facts: vec!["verified".into()],
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        let message = coordinator.advance_plan(&app_state).await.expect("advance");

        assert!(message.is_some());
        assert_ne!(coordinator.get_stage().await, BossStage::Execution);
    }

    #[tokio::test]
    async fn verification_completed_child_clears_execution_tail_and_advances_plan() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        let target_path = std::env::temp_dir().join(format!(
            "boss_verification_tail_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&target_path, "verified").expect("write target");
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some(format!(
                        "任务目标：\n- 目标文件：{}\n- 验证文件存在且非空",
                        target_path.display()
                    )),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        let message = coordinator.advance_plan(&app_state).await.expect("advance");

        assert!(message.is_some());
        let plan = coordinator.plan.read().await;
        let step = &plan.as_ref().expect("plan").steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Completed);
        assert!(step.completed);
        assert_eq!(coordinator.status.read().await.current_step, None);
    }

    #[tokio::test]
    async fn verification_loop_exits_when_target_scoped_verification_evidence_is_sufficient() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some("verify artifact".into()),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        coordinator
            .sync_terminal_child_task_state(tasks.as_ref())
            .await
            .expect("sync");
        let message = coordinator.advance_plan(&app_state).await.expect("advance");
        assert!(message.is_some());
        assert_eq!(coordinator.get_stage().await, BossStage::Completed);
    }

    #[tokio::test]
    async fn boss_verification_continuation_advances_to_terminal_step_after_verified_child_completion() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        let target_path = std::env::temp_dir().join(format!(
            "boss_verified_child_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&target_path, "done").expect("write target");
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some(format!(
                        "任务目标：\n- 目标文件：{}\n- 验证文件存在且非空",
                        target_path.display()
                    )),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        let record = tasks.create_with_type(
            "worker",
            TaskType::LocalAgent,
            "test-session",
            InteractionSurface::Cli,
        );
        assert_eq!(record.id, "task-0");
        tasks.start("task-0");
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
            .with_boss_coordinator(coordinator.clone());
        tasks.complete("task-0", &dispatcher);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            coordinator
                .sync_terminal_child_task_state(tasks.as_ref())
                .await
                .expect("sync")
        );
        coordinator.advance_plan(&app_state).await.expect("advance");

        let plan = coordinator.plan.read().await;
        let step = &plan.as_ref().expect("plan").steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Completed);
        assert!(step.completed);
    }

    #[tokio::test]
    async fn boss_on_only_verification_tail_does_not_hit_max_iterations_after_reverify_success() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        let target_path = std::env::temp_dir().join(format!(
            "boss_reverify_success_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&target_path, "verified").expect("write target");
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some(format!(
                        "任务目标：\n- 目标文件：{}\n- 验证文件存在且非空",
                        target_path.display()
                    )),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Rejected,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 1,
                    retry_budget: 3,
                    last_review_summary: Some("verify again".into()),
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some(target_path.display().to_string()),
                            verified_facts: Vec::new(),
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some(target_path.display().to_string()),
                        verified_facts: Vec::new(),
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }

        let message = coordinator.advance_plan(&app_state).await.expect("advance");
        assert!(
            message.is_some(),
            "verification continuation should not stall in execution"
        );
    }

    #[test]
    fn verification_only_terminalization_does_not_abort_while_repair_dispatch_is_still_possible() {
        let mut step = BossPlanStep {
            id: 0,
            description: "verify target".into(),
            objective: Some("verify artifact".into()),
            acceptance: vec!["artifact verification passed".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Failed,
            completed: false,
            result_diff: None,
            worker_task_id: Some("task-0".into()),
            attempt_count: 3,
            retry_budget: 3,
            last_review_summary: Some("artifact verification failed".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/example-site".into()),
                    verified_facts: vec!["README created".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/example-site".into()),
                verified_facts: vec!["README created".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "ArtifactVerify".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Interrupted,
                summary: "artifact verification failed: /tmp/example-site".into(),
                detail: Some(
                    "artifact verification status=missing_or_invalid path=/tmp/example-site"
                        .into(),
                ),
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };

        assert!(super::has_only_verification_evidence_gap(&step));
        step.status = BossPlanStepStatus::Failed;
        assert_eq!(step.status, BossPlanStepStatus::Failed);
    }

    #[tokio::test]
    async fn u7_boss_on_only_verification_first_matches_all_on_terminalization_path() {
        let coordinator = Arc::new(BossCoordinator::new());
        let tasks = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let app_state = test_app_state_with_tasks(tasks.clone(), coordinator.clone());
        coordinator
            .attach_app_state_for_report_testing(app_state.clone())
            .await;
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some("verify artifact".into()),
                    acceptance: vec!["artifact verification passed".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Failed,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-0".into()),
                    attempt_count: 3,
                    retry_budget: 3,
                    last_review_summary: Some("artifact verification failed".into()),
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some("/tmp/example-site".into()),
                            verified_facts: vec!["README created".into()],
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some("/tmp/example-site".into()),
                        verified_facts: vec!["README created".into()],
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: vec![ToolExecutionRecord {
                        tool_name: "ArtifactVerify".into(),
                        outcome: "Text".into(),
                        kind: ToolExecutionOutcomeKind::Interrupted,
                        summary: "artifact verification failed: /tmp/example-site".into(),
                        detail: Some(
                            "artifact verification status=missing_or_invalid path=/tmp/example-site"
                                .into(),
                        ),
                        pending_approval: None,
                        report_modifier: ToolReportModifier::None,
                        observable_input: None,
                        batch_context: ToolBatchContext {
                            batch_index: 0,
                            batch_size: 1,
                            executed_in_batch: false,
                        },
                    }],
                }],
                ..BossPlan::default()
            });
        }

        let message = coordinator.advance_plan(&app_state).await.expect("advance");
        assert!(message.is_some());
        let plan = coordinator.plan.read().await;
        let step = &plan.as_ref().expect("plan").steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Failed);
        assert_eq!(
            step.stage_continuation_context
                .as_ref()
                .and_then(|context| context.next_action.as_deref()),
            Some("verify_artifact")
        );
    }

    #[tokio::test]
    async fn test_state_transition_to_waiting_for_approval() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);
    }

    #[tokio::test]
    async fn test_user_approval_y_transitions_to_execution() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        // set dummy plan to avoid ignoring boolean conversion
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                finalized: true,
                ..BossPlan::default()
            });
        }
        let confirmed = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(confirmed);
        assert_eq!(coordinator.get_stage().await, BossStage::Execution);
        assert!(
            coordinator
                .plan
                .read()
                .await
                .as_ref()
                .unwrap()
                .accepted_by_user
        );
    }

    #[tokio::test]
    async fn test_user_approval_feedback_returns_to_documentation() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan::default());
        }
        let rejected = coordinator
            .handle_user_approval("Wait, this is wrong")
            .await
            .unwrap();
        assert!(!rejected);
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[test]
    fn boss_metadata_records_last_failure_kind_and_recommended_repair() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            last_failure_outcome: Some(ToolOutcome {
                kind: ToolOutcomeKind::MissingPath,
                recoverable: true,
                recommended_next_action: Some("create_file".into()),
                evidence_ref: Some("tool_feedback:3".into()),
                bounded_excerpt: Some("No such file or directory".into()),
                truncated: false,
            }),
            ..LoopUsage::default()
        };
        usage.tool_dispatch_failure_count = 1;

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.last_effective_tool_action.as_deref(),
            Some("Read")
        );
        assert_eq!(
            routed_metadata.last_failure_kind.as_deref(),
            Some("missing_path")
        );
        assert_eq!(routed_metadata.last_failure_recoverable, Some(true));
        assert_eq!(
            routed_metadata.last_recommended_repair.as_deref(),
            Some("create_file")
        );
        assert_eq!(
            routed_metadata.last_failure_evidence_ref.as_deref(),
            Some("tool_feedback:3")
        );
    }

    #[test]
    fn boss_metadata_clears_stale_failure_after_later_success() {
        let mut routed_metadata = BossStepRoutedMetadata {
            last_effective_tool_action: Some("Edit".into()),
            last_failure_kind: Some("missing_path".into()),
            last_failure_recoverable: Some(true),
            last_recommended_repair: Some("create_file".into()),
            last_failure_evidence_ref: Some("tool_feedback:7".into()),
            last_failure_bounded_excerpt: Some("stale failure".into()),
            last_failure_truncated: Some(false),
            ..BossStepRoutedMetadata::default()
        };
        let usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            last_failure_outcome: None,
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.last_effective_tool_action.as_deref(),
            Some("Read")
        );
        assert_eq!(routed_metadata.last_failure_kind, None);
        assert_eq!(routed_metadata.last_failure_recoverable, None);
        assert_eq!(routed_metadata.last_recommended_repair, None);
        assert_eq!(routed_metadata.last_failure_evidence_ref, None);
        assert_eq!(routed_metadata.last_failure_bounded_excerpt, None);
        assert_eq!(routed_metadata.last_failure_truncated, None);
    }

    #[test]
    fn boss_metadata_records_recovery_tier_and_outcome() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            recovery_attempted: true,
            recovery_tier: Some("artifact_repair_turn".into()),
            recovery_outcome: Some("repair_turn_injected".into()),
            terminal_blocker_kind: Some("same_invalid_strategy".into()),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(routed_metadata.recovery_attempted, Some(true));
        assert_eq!(
            routed_metadata.recovery_tier.as_deref(),
            Some("artifact_repair_turn")
        );
        assert_eq!(
            routed_metadata.recovery_outcome.as_deref(),
            Some("repair_turn_injected")
        );
        assert_eq!(
            routed_metadata.terminal_blocker_kind.as_deref(),
            Some("same_invalid_strategy")
        );
    }

    #[test]
    fn unsupported_selector_is_not_reported_as_generic_no_progress() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            terminal_blocker_kind: Some("unsupported_selector".into()),
            recovery_outcome: Some("unsupported_selector".into()),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata
                .step_failure_classification
                .as_ref()
                .map(|classification| classification.as_str()),
            Some("unsupported_request")
        );
        assert!(!format!("{:?}", routed_metadata).contains("generic_failure"));
    }

    #[test]
    fn writable_artifact_recovery_is_reported_as_repairable_recovery() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            recovery_attempted: true,
            recovery_tier: Some("artifact_repair_turn".into()),
            recovery_outcome: Some("repair_turn_injected".into()),
            completion_evidence_status: Some(CompletionEvidenceStatus::MissingArtifactEvidence),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata
                .step_failure_classification
                .as_ref()
                .map(|classification| classification.as_str()),
            Some("repairable_recovery")
        );
    }

    #[test]
    fn verification_gap_repair_continuation_surfaces_in_step_state_and_report() {
        let report = BossReportPayload {
            stage: BossStage::Execution,
            current_step: Some(1),
            total_steps: Some(1),
            designer_a: BossActorHandle::new("a", "s", BossActorRole::DesignerA),
            executor_b: BossActorHandle::new("b", "s", BossActorRole::ExecutorB),
            active_children: Vec::new(),
            steps: vec![BossStepReport {
                id: 1,
                status: BossPlanStepStatus::Rejected,
                worker_task_id: None,
                attempt_count: 1,
                last_review_summary: Some("verify again".into()),
                action_required: None,
                blocker_reason: None,
                routed_metadata: Some(BossStepRoutedMetadata {
                    step_failure_classification: Some(
                        StepFailureClassification::VerificationRepairContinuation,
                    ),
                    recovery_outcome: Some("repair_turn_injected".into()),
                    completion_evidence_status: Some("missing_verification_evidence".into()),
                    ..BossStepRoutedMetadata::default()
                }),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                    repair_intent: Some(crate::core::state_frame::RepairIntent {
                        failed_target: Some("/tmp/report.md".into()),
                        verified_facts: vec!["fact: verified".into()],
                        next_action: Some("run_verification".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    failed_target: Some("/tmp/report.md".into()),
                    verified_facts: vec!["fact: verified".into()],
                    next_action: Some("run_verification".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                executor_b_stage_memory: None,
            }],
            history_summary: Vec::new(),
            observability_summary: None,
            rollout_policy_decision: None,
            success_classification: None,
            lism_policy: BossLisMPolicy::Inherit,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/report.md".into()),
                    verified_facts: vec!["fact: verified".into()],
                    next_action: Some("run_verification".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/report.md".into()),
                verified_facts: vec!["fact: verified".into()],
                next_action: Some("run_verification".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: None,
        };

        assert!(matches!(
            report.steps[0]
                .routed_metadata
                .as_ref()
                .and_then(|metadata| metadata.step_failure_classification.as_ref()),
            Some(StepFailureClassification::VerificationRepairContinuation)
        ));
        assert!(report
            .format_report()
            .contains("failure_class=verification_repair_continuation"));
        assert_eq!(
            report
                .stage_continuation_context
                .as_ref()
                .and_then(|context| context.next_action.as_deref()),
            Some("run_verification")
        );
    }

    #[test]
    fn true_external_blocker_does_not_enter_repairable_path() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            terminal_blocker_kind: Some("true_external_blocker".into()),
            recovery_outcome: Some("external_blocker".into()),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata
                .step_failure_classification
                .as_ref()
                .map(|classification| classification.as_str()),
            Some("true_external_blocker")
        );
        assert_ne!(
            routed_metadata
                .step_failure_classification
                .as_ref()
                .map(|classification| classification.as_str()),
            Some("repairable_recovery")
        );
    }

    #[test]
    fn repairable_recovery_maps_to_rejected_and_preserves_continuation_context() {
        let mut step = BossPlanStep {
            id: 1,
            description: "write artifact".into(),
            objective: Some("目标文件：/tmp/report.md".into()),
            acceptance: vec![],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract {
                declared_artifacts: vec![DeclaredArtifactContract {
                    ref_id: "artifact:report".into(),
                    path: "/tmp/report.md".into(),
                    kind: "file".into(),
                    required_evidence: vec!["artifact_evidence".into()],
                    required_actions: vec!["write_artifact".into()],
                }],
                ..StageExecutionContract::default()
            },
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        apply_step_failure_classification(
            &mut step,
            StepFailureClassification::RepairableRecovery,
            "missing artifact evidence",
        );

        assert_eq!(step.status, BossPlanStepStatus::Rejected);
        assert_eq!(
            step.stage_continuation_context
                .as_ref()
                .and_then(|context| context.next_action.as_deref()),
            Some("missing artifact evidence")
        );
        assert_eq!(
            step.stage_execution_contract.declared_artifacts[0].path,
            "/tmp/report.md"
        );
    }

    #[test]
    fn typed_stage_contract_is_preserved_on_step_dispatch_and_repair() {
        let contract = StageExecutionContract {
            declared_artifacts: vec![DeclaredArtifactContract {
                ref_id: "artifact:report".into(),
                path: "/tmp/contract-report.md".into(),
                kind: "file".into(),
                required_evidence: vec!["artifact_evidence".into()],
                required_actions: vec!["write_artifact".into()],
            }],
            verifications: vec![VerificationContract {
                target_ref: "artifact:report".into(),
                target_path: Some("/tmp/contract-report.md".into()),
                required_actions: vec!["read_back_verify".into()],
                required_evidence: vec!["verification_evidence".into()],
            }],
            ..StageExecutionContract::default()
        };
        let mut step = BossPlanStep {
            id: 1,
            description: "write artifact".into(),
            objective: Some("noise objective /boss /".into()),
            acceptance: vec![],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: contract.clone(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        apply_step_failure_classification(
            &mut step,
            StepFailureClassification::RepairableRecovery,
            "repair artifact",
        );

        assert_eq!(step.status, BossPlanStepStatus::Rejected);
        assert_eq!(step.stage_execution_contract, contract);
        assert_eq!(
            step.stage_continuation_context
                .as_ref()
                .and_then(|context| context.failed_target.as_deref()),
            Some("/tmp/contract-report.md")
        );
    }

    #[test]
    fn unsupported_request_does_not_enter_rejected_repair_path() {
        let mut step = BossPlanStep {
            id: 1,
            description: "request unsupported selector".into(),
            objective: Some("operator_action:write_artifact".into()),
            acceptance: vec![],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        apply_step_failure_classification(
            &mut step,
            StepFailureClassification::UnsupportedRequest,
            "unsupported selector",
        );

        assert_eq!(step.status, BossPlanStepStatus::Failed);
        assert!(step.stage_continuation_context.is_none());
    }

    #[tokio::test]
    async fn repairable_continuation_does_not_emit_terminal_aborted_sample_early() {
        let sink = crate::core::lism_ab_sample::new_shared_ab_sink();
        let coordinator = BossCoordinator::new().with_lism_ab_sink(sink.clone());
        let target_path = std::env::temp_dir().join(format!(
            "boss_repairable_continuation_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));

        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "write artifact".into(),
                    objective: Some(format!("任务目标：\n- 目标文件：{}\n- 生成报告", target_path.display())),
                    acceptance: vec!["target file exists and is non-empty".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Rejected,
                    completed: false,
                    result_diff: None,
                    worker_task_id: Some("task-1".into()),
                    attempt_count: 1,
                    retry_budget: 3,
                    last_review_summary: Some("artifact verification failed".into()),
                    last_correction: Some("repair artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        failed_target: Some(target_path.display().to_string()),
                        verified_facts: vec!["fact: verified".into()],
                        next_action: Some("repair artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some(target_path.display().to_string()),
                            verified_facts: vec!["fact: verified".into()],
                            next_action: Some("repair artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                    }),
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
        }

        let report = coordinator.build_lism_sample_report(None).await;
        assert_eq!(
            report.steps[0]
                .stage_continuation_context
                .as_ref()
                .and_then(|context| context.next_action.as_deref()),
            Some("repair artifact")
        );
        assert!(sink.records().is_empty());
    }

    #[test]
    fn boss_metadata_records_success_classification() {
        let mut routed_metadata = BossStepRoutedMetadata {
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Write".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: vec!["cargo test".into()],
                artifact_status: "verified".into(),
                test_status: "passed".into(),
                verification_status: "verified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["artifact:1".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..BossStepRoutedMetadata::default()
        };
        let usage = LoopUsage {
            worker_report: routed_metadata.worker_report.clone(),
            completion_evidence_status: Some(CompletionEvidenceStatus::Sufficient),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.success_classification.as_ref().map(|c| c.as_str()),
            Some("direct_success")
        );
    }

    #[test]
    fn boss_does_not_accept_typed_hydration_completion_without_verification_evidence() {
        let step = BossPlanStep {
            id: 0,
            description: "write report".into(),
            objective: None,
            acceptance: vec!["verification evidence required".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract {
                declared_artifacts: vec![DeclaredArtifactContract {
                    ref_id: "artifact:step0:0".into(),
                    path: "/tmp/report.md".into(),
                    kind: "file".into(),
                    required_actions: vec!["write".into()],
                    required_evidence: vec!["/tmp/report.md".into()],
                }],
                verifications: vec![VerificationContract {
                    target_ref: "artifact:step0:0".into(),
                    target_path: Some("/tmp/report.md".into()),
                    required_actions: vec!["verify_artifact".into()],
                    required_evidence: vec!["/tmp/report.md".into()],
                }],
                required_actions: vec!["write".into(), "verify_artifact".into()],
                required_evidence: vec!["/tmp/report.md".into()],
                ..StageExecutionContract::default()
            },
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        let metadata = BossStepRoutedMetadata {
            completion_evidence_status: Some("missing_verification_evidence".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Write".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: Vec::new(),
                artifact_status: "verified".into(),
                test_status: "not_required".into(),
                verification_status: "unverified".into(),
                stage_execution_contract: step.stage_execution_contract.clone(),
                stage_continuation_context: None,
                evidence_refs: vec!["write:/tmp/report.md".into()],
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:step0:0".into(),
                    target_path: Some("/tmp/report.md".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                }],
                remaining_risks: vec!["verification missing".into()],
                completion_evidence_status: CompletionEvidenceStatus::MissingVerificationEvidence,
            }),
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:step0:0".into(),
                target_path: Some("/tmp/report.md".into()),
                missing_artifact_evidence: false,
                missing_test_evidence: false,
                missing_verification_evidence: true,
                recommended_action: "verify_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let failure = step_completion_gate_error(&step, Some(&metadata))
            .expect("verification gate should reject direct completion");
        assert_eq!(failure.1, StepFailureClassification::VerificationRepairContinuation);
    }

    #[test]
    fn u8_placeholder_report_does_not_bypass_verification_gate() {
        let step = BossPlanStep {
            id: 0,
            description: "write report".into(),
            objective: None,
            acceptance: vec!["verification evidence required".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract {
                verifications: vec![VerificationContract {
                    target_ref: "artifact:step0:0".into(),
                    target_path: Some("/tmp/report.md".into()),
                    required_actions: vec!["verify_artifact".into()],
                    required_evidence: vec!["/tmp/report.md".into()],
                }],
                required_actions: vec!["verify_artifact".into()],
                required_evidence: vec!["/tmp/report.md".into()],
                ..StageExecutionContract::default()
            },
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        let metadata = BossStepRoutedMetadata {
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Write".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: Vec::new(),
                artifact_status: "verified".into(),
                test_status: "not_required".into(),
                verification_status: "unverified".into(),
                stage_execution_contract: step.stage_execution_contract.clone(),
                stage_continuation_context: None,
                evidence_refs: vec!["write:/tmp/report.md".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::MissingVerificationEvidence,
            }),
            completion_evidence_gaps: Vec::new(),
            ..BossStepRoutedMetadata::default()
        };

        let failure = step_completion_gate_error(&step, Some(&metadata))
            .expect("placeholder completion should not bypass verification gate");
        assert_eq!(failure.1, StepFailureClassification::RepairableRecovery);
    }

    #[test]
    fn verify_first_success_is_classified_as_fallback_success() {
        let routed_metadata = BossStepRoutedMetadata {
            fallback_tier: Some("verification_first".into()),
            recovery_outcome: Some("verification_first_success".into()),
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Verify".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: vec!["cargo test".into()],
                artifact_status: "verified".into(),
                test_status: "passed".into(),
                verification_status: "verified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["artifact:1".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..BossStepRoutedMetadata::default()
        };

        assert_eq!(
            classify_step_success(Some(&routed_metadata)).map(|c| c.as_str()),
            Some("fallback_success")
        );
    }

    #[test]
    fn full_dispatch_success_is_classified_separately() {
        let routed_metadata = BossStepRoutedMetadata {
            fallback_tier: Some("full_worker_dispatch".into()),
            recovery_outcome: Some("full_worker_dispatch_success".into()),
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Bash".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: vec!["cargo test".into()],
                artifact_status: "verified".into(),
                test_status: "passed".into(),
                verification_status: "verified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["artifact:1".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..BossStepRoutedMetadata::default()
        };

        assert_eq!(
            classify_step_success(Some(&routed_metadata)).map(|c| c.as_str()),
            Some("full_worker_dispatch_success")
        );
    }

    #[test]
    fn direct_success_is_not_promoted_when_no_fallback_or_recovery_happened() {
        let routed_metadata = BossStepRoutedMetadata {
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Read".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: vec!["cargo test".into()],
                artifact_status: "verified".into(),
                test_status: "passed".into(),
                verification_status: "verified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["artifact:1".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..BossStepRoutedMetadata::default()
        };

        assert_eq!(
            classify_step_success(Some(&routed_metadata)).map(|c| c.as_str()),
            Some("direct_success")
        );
    }

    #[test]
    fn true_external_blocker_is_not_mixed_with_recovery_success() {
        let routed_metadata = BossStepRoutedMetadata {
            terminal_blocker_kind: Some("true_external_blocker".into()),
            recovery_outcome: Some("verification_first_success".into()),
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Blocked,
                last_tool_action: Some("Verify".into()),
                files_changed: Vec::new(),
                tests_run: Vec::new(),
                artifact_status: "blocked".into(),
                test_status: "blocked".into(),
                verification_status: "blocked".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: Vec::new(),
                completion_evidence_gaps: Vec::new(),
                remaining_risks: vec!["external blocker".into()],
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..BossStepRoutedMetadata::default()
        };

        assert_eq!(
            classify_step_success(Some(&routed_metadata)).map(|c| c.as_str()),
            Some("true_external_blocker")
        );
    }

    #[test]
    fn verify_first_and_full_dispatch_do_not_override_external_blocker() {
        let routed_metadata = BossStepRoutedMetadata {
            terminal_blocker_kind: Some("true_external_blocker".into()),
            fallback_tier: Some("full_worker_dispatch".into()),
            recovery_tier: Some("verification_first".into()),
            recovery_outcome: Some("full_worker_dispatch_success".into()),
            completion_evidence_status: Some("sufficient".into()),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Blocked,
                last_tool_action: Some("Verify".into()),
                files_changed: Vec::new(),
                tests_run: Vec::new(),
                artifact_status: "blocked".into(),
                test_status: "blocked".into(),
                verification_status: "blocked".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: Vec::new(),
                completion_evidence_gaps: Vec::new(),
                remaining_risks: vec!["external blocker".into()],
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..BossStepRoutedMetadata::default()
        };

        assert_eq!(
            classify_step_success(Some(&routed_metadata)).map(|c| c.as_str()),
            Some("true_external_blocker")
        );
    }

    #[test]
    fn boss_report_surfaces_worker_structured_report() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            completion_evidence_status: Some(CompletionEvidenceStatus::Sufficient),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Bash".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: vec!["cargo_test:passed".into()],
                artifact_status: "verified".into(),
                test_status: "passed".into(),
                verification_status: "verified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["tool_output:1".into(), "artifact:step1:runtime:0".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.completion_evidence_status.as_deref(),
            Some("sufficient")
        );
        let report = routed_metadata.worker_report.expect("worker report");
        assert_eq!(report.worker_state, AgentState::Done);
        assert_eq!(report.artifact_status, "verified");
        assert!(
            report
                .evidence_refs
                .iter()
                .any(|reference| reference == "tool_output:1")
        );
    }

    #[test]
    fn boss_report_carries_success_classification() {
        let report = BossReportPayload {
            stage: BossStage::Execution,
            current_step: Some(1),
            total_steps: Some(1),
            designer_a: BossActorHandle::new("a", "s", BossActorRole::DesignerA),
            executor_b: BossActorHandle::new("b", "s", BossActorRole::ExecutorB),
            active_children: Vec::new(),
            steps: vec![BossStepReport {
                id: 1,
                status: BossPlanStepStatus::Completed,
                worker_task_id: None,
                attempt_count: 1,
                last_review_summary: None,
                action_required: None,
                blocker_reason: None,
                routed_metadata: Some(BossStepRoutedMetadata {
                    success_classification: Some(
                        crate::core::boss_state::BossSuccessClassification::RecoveredSuccess,
                    ),
                    ..BossStepRoutedMetadata::default()
                }),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                    repair_intent: Some(crate::core::state_frame::RepairIntent {
                        failed_target: Some("/tmp/report.md".into()),
                        verified_facts: vec!["fact: verified".into()],
                        next_action: Some("write_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    failed_target: Some("/tmp/report.md".into()),
                    verified_facts: vec!["fact: verified".into()],
                    next_action: Some("write_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                executor_b_stage_memory: None,
            }],
            history_summary: Vec::new(),
            observability_summary: None,
            rollout_policy_decision: None,
            success_classification: Some(
                crate::core::boss_state::BossSuccessClassification::RecoveredSuccess,
            ),
            lism_policy: Default::default(),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/report.md".into()),
                    verified_facts: vec!["fact: verified".into()],
                    next_action: Some("write_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/report.md".into()),
                verified_facts: vec!["fact: verified".into()],
                next_action: Some("write_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: None,
        };

        assert_eq!(
            report.success_classification.as_ref().map(|c| c.as_str()),
            Some("recovered_success")
        );
        assert!(report.format_report().contains("success=recovered_success"));
    }

    #[test]
    fn boss_report_identifies_exact_missing_artifact_gap_for_second_target() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            completion_evidence_status: Some(CompletionEvidenceStatus::MissingArtifactEvidence),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Executing,
                last_tool_action: Some("Write".into()),
                files_changed: vec!["/tmp/one.md".into()],
                tests_run: Vec::new(),
                artifact_status: "touched".into(),
                test_status: "not_run".into(),
                verification_status: "unverified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["change:1".into()],
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:contract:1".into(),
                    target_path: Some("/tmp/two.md".into()),
                    missing_artifact_evidence: true,
                    missing_test_evidence: false,
                    missing_verification_evidence: false,
                    recommended_action: "write_artifact".into(),
                }],
                remaining_risks: vec![
                    "completion_evidence_status=missing_artifact_evidence".into(),
                ],
                completion_evidence_status: CompletionEvidenceStatus::MissingArtifactEvidence,
            }),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(routed_metadata.completion_evidence_gaps.len(), 1);
        let gap = &routed_metadata.completion_evidence_gaps[0];
        assert_eq!(gap.target_ref, "artifact:contract:1");
        assert_eq!(gap.target_path.as_deref(), Some("/tmp/two.md"));
        assert!(gap.missing_artifact_evidence);
        assert_eq!(gap.recommended_action, "write_artifact");
    }

    #[test]
    fn boss_metadata_clears_old_completion_gaps_after_later_success() {
        let mut routed_metadata = BossStepRoutedMetadata {
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:1".into(),
                target_path: Some("/tmp/two.md".into()),
                missing_artifact_evidence: true,
                missing_test_evidence: false,
                missing_verification_evidence: false,
                recommended_action: "write_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };
        let usage = LoopUsage {
            completion_evidence_status: Some(CompletionEvidenceStatus::Sufficient),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("ArtifactVerify".into()),
                files_changed: vec!["/tmp/one.md".into(), "/tmp/two.md".into()],
                tests_run: Vec::new(),
                artifact_status: "verified".into(),
                test_status: "not_required".into(),
                verification_status: "verified".into(),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                evidence_refs: vec!["artifact:verified".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);
        assert!(routed_metadata.completion_evidence_gaps.is_empty());
    }

    #[test]
    fn boss_report_rollout_policy_denylists_exact_artifact_gap_targets() {
        let steps = vec![BossStepReport {
            id: 1,
            status: BossPlanStepStatus::Running,
            worker_task_id: None,
            attempt_count: 1,
            last_review_summary: None,
            action_required: None,
            blocker_reason: None,
            routed_metadata: Some(BossStepRoutedMetadata {
                completion_evidence_gaps: vec![
                    CompletionEvidenceGap {
                        target_ref: "artifact:contract:1".into(),
                        target_path: Some("/tmp/one.md".into()),
                        missing_artifact_evidence: false,
                        missing_test_evidence: false,
                        missing_verification_evidence: false,
                        recommended_action: "none".into(),
                    },
                    CompletionEvidenceGap {
                        target_ref: "artifact:contract:2".into(),
                        target_path: Some("/tmp/two.md".into()),
                        missing_artifact_evidence: true,
                        missing_test_evidence: false,
                        missing_verification_evidence: true,
                        recommended_action: "write_artifact".into(),
                    },
                ],
                ..BossStepRoutedMetadata::default()
            }),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
        }];

        let decision =
            BossCoordinator::derive_rollout_policy_decision(&steps).expect("policy decision");

        assert_eq!(decision.denylist_targets.len(), 1);
        assert_eq!(decision.fallback_targets.len(), 1);
        let deny = &decision.denylist_targets[0];
        assert_eq!(deny.target_ref, "artifact:contract:2");
        assert_eq!(deny.target_path.as_deref(), Some("/tmp/two.md"));
        assert_eq!(
            deny.missing_evidence_kinds,
            vec![
                "artifact_evidence".to_string(),
                "verification_evidence".to_string()
            ]
        );
        assert_eq!(deny.recommended_policy, "denylist_direct_worker_lism");
        assert_eq!(deny.recommended_fallback, "full_worker_dispatch");
    }

    #[test]
    fn boss_report_rollout_policy_clears_after_gaps_are_resolved() {
        let steps = vec![BossStepReport {
            id: 1,
            status: BossPlanStepStatus::Completed,
            worker_task_id: None,
            attempt_count: 2,
            last_review_summary: Some("artifact verified".into()),
            action_required: None,
            blocker_reason: None,
            routed_metadata: Some(BossStepRoutedMetadata {
                completion_evidence_gaps: Vec::new(),
                ..BossStepRoutedMetadata::default()
            }),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
        }];

        assert!(BossCoordinator::derive_rollout_policy_decision(&steps).is_none());
    }

    #[test]
    fn continuation_payload_prefers_declared_artifact_over_objective_text() {
        let contract = ExecutorBAssignmentContract {
            brief: BossContextBrief {
                plan_id: "plan-alpha".into(),
                step_id: 0,
                plan_version: "plan-alpha:steps=1".into(),
                step_revision: "step-0-attempt-0".into(),
                generated_at: "2026-05-04T00:00:00Z".into(),
                objective: "fix the failing continuation path".into(),
                acceptance: vec!["artifact exists".into()],
                last_correction: None,
                recent_decisions: Vec::new(),
                relevant_file_handles: Vec::new(),
                target_files: vec!["/tmp/from-target-files.md".into()],
                target_artifacts: Vec::new(),
                allowed_tools: vec!["Write".into()],
                permission_scope: PermissionScopeView {
                    lism_policy: "force_on".into(),
                    inherit_context: false,
                    workspace_capability: "write".into(),
                    boss_actor_role: "executor_b".into(),
                },
                parent_session_id: "session-alpha".into(),
                context_strategy: BossContextStrategy::Brief,
            },
            state_frame: BossStateFrame {
                step_id: 0,
                status: BossPlanStepStatus::Running,
                stage_execution_contract: StageExecutionContract {
                    declared_artifacts: vec![DeclaredArtifactContract {
                        ref_id: "artifact:contract:0".into(),
                        path: "/tmp/contract-first.md".into(),
                        kind: "file".into(),
                        required_evidence: vec!["artifact_exists".into()],
                        required_actions: vec!["write_artifact".into()],
                    }],
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                open_items: vec!["write artifact".into()],
                blocked_items: Vec::new(),
                recent_local_facts: vec!["fact: file missing".into()],
                allowed_actions: vec!["write_artifact".into()],
                required_output_hint: None,
            },
            allowed_tools: vec!["Write".into()],
            lism_policy: "force_on".into(),
            worker_role: WorkerRole::Implement,
            assignment_fingerprint: "fingerprint".into(),
        };

        let payload = build_continuation_payload(&contract);

        assert_eq!(
            payload.failed_target.as_deref(),
            Some("/tmp/contract-first.md")
        );
        assert_eq!(payload.next_action.as_deref(), Some("write_artifact"));
        assert_eq!(payload.verified_facts, vec!["fact: file missing"]);
        assert_eq!(payload.continuity_mode.as_deref(), Some("continue"));
    }

    #[tokio::test]
    async fn verification_first_payload_uses_short_target_scoped_output_contract() {
        let coordinator = BossCoordinator::new();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                plan_id: "plan-verify".into(),
                accepted_by_user: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some("write report to /tmp/verification-first.md".into()),
                    acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Rejected,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 1,
                    retry_budget: 3,
                    last_review_summary: Some("verification missing".into()),
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some("/tmp/verification-first.md".into()),
                            verified_facts: vec!["Write succeeded".into()],
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some("/tmp/verification-first.md".into()),
                        verified_facts: vec!["Write succeeded".into()],
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: Some(ExecutorBStageMemory {
                        continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                        ..ExecutorBStageMemory::default()
                    }),
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut metadata = coordinator.routed_step_metadata.write().await;
            metadata.insert(0, BossStepRoutedMetadata {
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:0".into(),
                    target_path: Some("/tmp/verification-first.md".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                }],
                fallback_tier: Some("verification_first".into()),
                fallback_reason: Some("rollout_policy_verification_gap".into()),
                ..BossStepRoutedMetadata::default()
            });
        }

        let assignment = coordinator
            .build_executor_b_assignment_contract(0, "session-alpha", true)
            .await
            .expect("build assignment");

        assert_eq!(assignment.worker_role, WorkerRole::Verify);
        let hint = assignment
            .state_frame
            .required_output_hint
            .as_deref()
            .expect("required output hint");
        assert!(hint.contains("verified_target"));
        assert!(hint.contains("minimal_evidence"));
        assert!(!hint.contains("unified diff"));
        assert!(hint.contains("Forbidden: Files changed"));
        assert!(hint.contains("further reading suggestions"));
    }

    #[tokio::test]
    async fn verification_first_verify_role_payload_forbids_coordinator_advice_prose() {
        let coordinator = BossCoordinator::new();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                plan_id: "plan-verify".into(),
                accepted_by_user: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some("write report to /tmp/verification-first.md".into()),
                    acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Rejected,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 1,
                    retry_budget: 3,
                    last_review_summary: Some("verification missing".into()),
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some("/tmp/verification-first.md".into()),
                            verified_facts: vec!["Write succeeded".into()],
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some("/tmp/verification-first.md".into()),
                        verified_facts: vec!["Write succeeded".into()],
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: Some(ExecutorBStageMemory {
                        continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                        ..ExecutorBStageMemory::default()
                    }),
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut metadata = coordinator.routed_step_metadata.write().await;
            metadata.insert(0, BossStepRoutedMetadata {
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:0".into(),
                    target_path: Some("/tmp/verification-first.md".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                }],
                fallback_tier: Some("verification_first".into()),
                fallback_reason: Some("rollout_policy_verification_gap".into()),
                ..BossStepRoutedMetadata::default()
            });
        }

        let payload = coordinator
            .build_step_spawn_payload_internal(0, "session-alpha", "boss-actor-b")
            .await
            .expect("spawn payload")
            .payload;

        assert!(payload.contains("verified_target: /tmp/verification-first.md"));
        assert!(payload.contains("acceptance:"));
        assert!(payload.contains("minimal_evidence: Write succeeded"));
        assert!(payload.contains("remaining_blocker: verify_artifact"));
        assert!(!payload.contains("return a unified diff or file edits"));
        assert!(!payload.contains("任务必须按 4 个阶段推进"));
        assert!(!payload.contains("recent_decisions:"));
        assert!(!payload.contains("Files changed"));
        assert!(!payload.contains("next_action for coordinator"));
        assert!(!payload.contains("further reading suggestions"));
    }

    #[tokio::test]
    async fn verification_first_payload_omits_explanatory_stage_memory_and_routed_metadata_prose() {
        let coordinator = BossCoordinator::new();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                plan_id: "plan-verify".into(),
                accepted_by_user: true,
                steps: vec![BossPlanStep {
                    id: 0,
                    description: "verify target".into(),
                    objective: Some("write report to /tmp/verification-first.md".into()),
                    acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Rejected,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 1,
                    retry_budget: 3,
                    last_review_summary: Some("verification missing".into()),
                    last_correction: Some("verify_artifact".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                        repair_intent: Some(crate::core::state_frame::RepairIntent {
                            failed_target: Some("/tmp/verification-first.md".into()),
                            verified_facts: vec!["Read succeeded".into()],
                            next_action: Some("verify_artifact".into()),
                            continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        }),
                        failed_target: Some("/tmp/verification-first.md".into()),
                        verified_facts: vec!["Read succeeded".into()],
                        next_action: Some("verify_artifact".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                    }),
                    executor_b_stage_memory: Some(ExecutorBStageMemory {
                        continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                        recent_reads: vec!["read /tmp/verification-first.md".into()],
                        recent_edits: vec!["write /tmp/verification-first.md".into()],
                        recent_test_refs: vec!["test ref".into()],
                        recent_verification_refs: vec!["verify ref".into()],
                        failed_targets: vec!["/tmp/verification-first.md".into()],
                        verified_targets: vec!["/tmp/verification-first.md".into()],
                    }),
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut metadata = coordinator.routed_step_metadata.write().await;
            metadata.insert(0, BossStepRoutedMetadata {
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:0".into(),
                    target_path: Some("/tmp/verification-first.md".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                }],
                fallback_tier: Some("verification_first".into()),
                fallback_reason: Some("rollout_policy_verification_gap".into()),
                ..BossStepRoutedMetadata::default()
            });
        }

        let assignment = coordinator
            .build_step_spawn_payload_internal(0, "session-alpha", "boss-actor-b")
            .await
            .expect("spawn payload");
        let payload: serde_json::Value =
            serde_json::from_str(&assignment.payload).expect("spawn payload json");
        let task = payload
            .get("task")
            .and_then(serde_json::Value::as_str)
            .expect("task prompt");

        assert!(task.contains("verified_target: /tmp/verification-first.md"));
        assert!(task.contains("minimal_evidence: Read succeeded"));
        assert!(!task.contains("recent_decisions:"));
        assert!(!task.contains("recent_local_facts:"));
        assert!(!task.contains("relevant_file_handles:"));
        assert!(!task.contains("why verification is needed"));
    }

    #[test]
    fn verification_first_completion_summary_does_not_expand_into_replan_prose() {
        let step = BossPlanStep {
            id: 0,
            description: "verify report".into(),
            objective: Some("write report to /tmp/verification-first.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/verification-first.md".into()),
                    verified_facts: vec!["Write succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/verification-first.md".into()),
                verified_facts: vec!["Write succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "Write".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Write succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
            ],
        };

        let summary = build_step_review_summary(
            &step,
            "Worker task",
            &[(
                "Next action",
                "If you approve, I can continue reading more files and expand the report with additional roadmap notes.",
            )],
        );

        assert!(summary.contains("verified_target: /tmp/verification-first.md"));
        assert!(summary.contains("verification_result: verified"));
        assert!(!summary.contains("If you approve"));
        assert!(!summary.contains("roadmap"));
        assert!(!summary.contains("Next action:"));
    }

    #[test]
    fn verification_first_verify_role_result_is_post_shaped_to_short_form() {
        let mut step = BossPlanStep {
            id: 0,
            description: "verify report".into(),
            objective: Some("write report to /tmp/verification-first.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/verification-first.md".into()),
                    verified_facts: vec!["Write succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/verification-first.md".into()),
                verified_facts: vec!["Write succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };

        store_step_result_diff(
            &mut step,
            "verification result: verified\nminimal evidence: Read succeeded\nremaining blockers / risk summary: none\nnext_action for coordinator: keep reading docs",
            None,
        );

        let shaped = step.result_diff.as_deref().expect("result diff");
        assert!(shaped.contains("verified_target: /tmp/verification-first.md"));
        assert!(shaped.contains("verification_result: verified"));
        assert!(shaped.contains("minimal_evidence: Read succeeded"));
        assert!(shaped.contains("remaining_blocker: none"));
        assert!(!shaped.contains("next_action for coordinator"));
    }

    #[test]
    fn u8_verification_first_tail_prefers_brief_verification_result() {
        let step = BossPlanStep {
            id: 0,
            description: "verify u8 report".into(),
            objective: Some("write report to /tmp/multistage-tools-memory-token-report.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/multistage-tools-memory-token-report.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/multistage-tools-memory-token-report.md".into()),
                    verified_facts: vec!["Write succeeded".into(), "Read succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/multistage-tools-memory-token-report.md".into()),
                verified_facts: vec!["Write succeeded".into(), "Read succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "Write".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Write succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Glob".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Glob succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
            ],
        };

        let summary = build_step_review_summary(
            &step,
            "Worker task",
            &[(
                "Result",
                "Long report body here. Next_action for the coordinator: continue reading docs, patch the report, and expand citations.",
            )],
        );

        assert!(summary.contains("verified_target: /tmp/multistage-tools-memory-token-report.md"));
        assert!(summary.contains("minimal_evidence: Write succeeded; Read succeeded; Glob succeeded"));
        assert!(!summary.contains("Next_action for the coordinator"));
        assert!(!summary.contains("expand citations"));
    }

    #[test]
    fn u8_verify_role_long_output_is_compressed_before_boss_summary() {
        let mut step = BossPlanStep {
            id: 0,
            description: "verify u8 report".into(),
            objective: Some("write report to /tmp/multistage-tools-memory-token-report.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/multistage-tools-memory-token-report.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/multistage-tools-memory-token-report.md".into()),
                    verified_facts: vec!["Write succeeded".into(), "Read succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/multistage-tools-memory-token-report.md".into()),
                verified_facts: vec!["Write succeeded".into(), "Read succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Write".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Write succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
            ],
        };

        store_step_result_diff(
            &mut step,
            "Files changed\nMinimal verification steps for coordinator\nnext_action for coordinator: read more docs\nremaining blockers / risk summary: doc truncation\nverification result: verified",
            Some("fallback"),
        );
        let summary = build_step_review_summary(
            &step,
            "Worker task",
            &[("Result", step.result_diff.as_deref().unwrap_or_default())],
        );

        assert!(!summary.contains("Files changed"));
        assert!(!summary.contains("Minimal verification steps"));
        assert!(!summary.contains("next_action for coordinator"));
        assert!(summary.contains("verified_target: /tmp/multistage-tools-memory-token-report.md"));
    }

    #[test]
    fn verification_first_post_shaping_discards_extra_explanatory_lines() {
        let mut step = BossPlanStep {
            id: 0,
            description: "verify target".into(),
            objective: Some("write report to /tmp/verification-first.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/verification-first.md".into()),
                    verified_facts: vec!["Read succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/verification-first.md".into()),
                verified_facts: vec!["Read succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };

        store_step_result_diff(
            &mut step,
            "verified_target: /tmp/verification-first.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nnext_action for coordinator: keep going\nRemaining blockers / risk summary: none",
            None,
        );

        assert_eq!(
            step.result_diff.as_deref(),
            Some(
                "verified_target: /tmp/verification-first.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none"
            )
        );
    }

    #[test]
    fn all_on_verification_first_result_is_reduced_to_fixed_short_form() {
        let mut step = BossPlanStep {
            id: 0,
            description: "verify u8 report".into(),
            objective: Some("write report to /tmp/multistage-tools-memory-token-report.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/multistage-tools-memory-token-report.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/multistage-tools-memory-token-report.md".into()),
                    verified_facts: vec!["Write succeeded".into(), "Read succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/multistage-tools-memory-token-report.md".into()),
                verified_facts: vec!["Write succeeded".into(), "Read succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };

        store_step_result_diff(
            &mut step,
            "Files changed: report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: source file missing\nMinimal verification steps: keep reading docs",
            None,
        );

        assert_eq!(
            step.result_diff.as_deref(),
            Some(
                "verified_target: /tmp/multistage-tools-memory-token-report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: source file missing"
            )
        );
    }

    #[test]
    fn verification_first_short_form_is_identical_for_boss_on_only_and_all_on_given_same_target_result() {
        let make_step = || BossPlanStep {
            id: 0,
            description: "verify target".into(),
            objective: Some("write report to /tmp/verification-first.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/verification-first.md".into()),
                    verified_facts: vec!["Read succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/verification-first.md".into()),
                verified_facts: vec!["Read succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };

        let mut boss_on_only = make_step();
        let mut all_on = make_step();
        let output = "verification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nnext_action for coordinator: expand docs";
        store_step_result_diff(&mut boss_on_only, output, None);
        store_step_result_diff(&mut all_on, output, None);

        assert_eq!(boss_on_only.result_diff, all_on.result_diff);
    }

    #[test]
    fn verification_first_verify_role_output_is_shorter_than_general_replan_contract() {
        let mut verification_step = BossPlanStep {
            id: 0,
            description: "verify target".into(),
            objective: Some("write report to /tmp/verification-first.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/verification-first.md".into()),
                    verified_facts: vec!["Write succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/verification-first.md".into()),
                verified_facts: vec!["Write succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };
        let general_step = BossPlanStep {
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            last_correction: None,
            ..verification_step.clone()
        };

        let brief_summary = build_step_review_summary(
            &verification_step,
            "Worker task",
            &[(
                "Result",
                "If you approve, I can continue with a longer replan, explain missing files, and suggest more reading.",
            )],
        );
        let general_summary = build_step_review_summary(
            &general_step,
            "Worker task",
            &[(
                "Result",
                "If you approve, I can continue with a longer replan, explain missing files, and suggest more reading.",
            )],
        );

        assert!(brief_summary.len() < general_summary.len());
        assert!(!brief_summary.contains("If you approve"));
        assert!(general_summary.contains("If you approve"));
    }

    #[test]
    fn verification_first_short_form_keeps_target_result_evidence_and_blocker_only() {
        let mut step = BossPlanStep {
            id: 0,
            description: "verify target".into(),
            objective: Some("write report to /tmp/verification-first.md".into()),
            acceptance: vec!["target file exists and is non-empty: /tmp/verification-first.md".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("verification missing".into()),
            last_correction: Some("verify_artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                repair_intent: Some(crate::core::state_frame::RepairIntent {
                    failed_target: Some("/tmp/verification-first.md".into()),
                    verified_facts: vec!["Read succeeded".into()],
                    next_action: Some("verify_artifact".into()),
                    continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                }),
                failed_target: Some("/tmp/verification-first.md".into()),
                verified_facts: vec!["Read succeeded".into()],
                next_action: Some("verify_artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
            }),
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                continuity: Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated),
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
        };

        store_step_result_diff(
            &mut step,
            "verification result: blocked\nminimal evidence: Read succeeded\nremaining blocker: target file still missing verification evidence\nFiles changed: none",
            None,
        );

        assert_eq!(
            step.result_diff.as_deref(),
            Some(
                "verified_target: /tmp/verification-first.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target file still missing verification evidence"
            )
        );
    }

    #[test]
    fn runtime_permissions_inherit_declared_writable_artifact_paths_for_lism_steps() {
        let permissions =
            crate::state::permission_context::ToolPermissionContext::new(
                crate::state::permission_context::PermissionMode::Default,
            );
        let contract = StageExecutionContract {
            declared_artifacts: vec![
                DeclaredArtifactContract {
                    ref_id: "artifact:readonly".into(),
                    path: "/tmp/readonly-note.md".into(),
                    kind: "file".into(),
                    required_actions: vec!["verify".into()],
                    required_evidence: vec![],
                },
                DeclaredArtifactContract {
                    ref_id: "artifact:writable".into(),
                    path: "/tmp/repair-target.md".into(),
                    kind: "file".into(),
                    required_actions: vec!["write".into(), "verify".into()],
                    required_evidence: vec![],
                },
            ],
            ..StageExecutionContract::default()
        };

        inject_declared_writable_artifact_paths(&permissions, &contract);

        assert!(!permissions.is_delegated_write_path("/tmp/readonly-note.md"));
        assert!(permissions.is_delegated_write_path("/tmp/repair-target.md"));
    }

    #[test]
    fn executor_b_stage_memory_reuses_recent_read_edit_test_and_verification_facts() {
        let step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some("创建目标文件：/tmp/report.md".into()),
            acceptance: vec!["artifact file exists and is non-empty".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Rejected,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("repair".into()),
            last_correction: Some("repair artifact".into()),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: Some(crate::core::state_frame::StageContinuationContext {
                failed_target: Some("/tmp/report.md".into()),
                verified_facts: vec!["artifact verification failed".into()],
                next_action: Some("repair artifact".into()),
                continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                repair_intent: None,
            }),
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "read src/lib.rs".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(observable_input_json(json!({"path":"src/lib.rs"}))),
                    batch_context: ToolBatchContext { batch_index: 0, batch_size: 1, executed_in_batch: false },
                },
                ToolExecutionRecord {
                    tool_name: "Edit".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "updated src/lib.rs".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(observable_input_json(json!({"path":"src/lib.rs"}))),
                    batch_context: ToolBatchContext { batch_index: 0, batch_size: 1, executed_in_batch: false },
                },
                ToolExecutionRecord {
                    tool_name: "Bash".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "cargo test passed".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(observable_input_json(json!({"command":"cargo test -p rust_agent"}))),
                    batch_context: ToolBatchContext { batch_index: 0, batch_size: 1, executed_in_batch: false },
                },
                ToolExecutionRecord {
                    tool_name: "ArtifactVerify".into(),
                    outcome: "failed".into(),
                    kind: ToolExecutionOutcomeKind::Denied,
                    summary: "artifact verification failed: target file missing".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(observable_input_json(json!({"path":"/tmp/report.md","status":"missing_or_invalid"}))),
                    batch_context: ToolBatchContext { batch_index: 0, batch_size: 1, executed_in_batch: false },
                },
            ],
        };

        let memory = project_executor_b_stage_memory(&step, None).expect("memory projected");
        assert_eq!(memory.continuity, Some(ExecutorBStageMemoryContinuity::ReuseWithinStep));
        assert_eq!(memory.recent_reads, vec!["src/lib.rs"]);
        assert_eq!(memory.recent_edits, vec!["src/lib.rs"]);
        assert_eq!(memory.recent_test_refs, vec!["cargo test -p rust_agent"]);
        assert!(
            memory
                .recent_verification_refs
                .iter()
                .any(|item| item.contains("artifact verification failed"))
        );
        assert!(memory.failed_targets.iter().any(|item| item.contains("/tmp/report.md")));
    }

    #[test]
    fn executor_b_stage_memory_marks_verification_first_as_isolated() {
        let step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some("verify artifact".into()),
            acceptance: vec![],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: Some(ExecutorBStageMemory {
                recent_reads: vec!["src/lib.rs".into()],
                ..ExecutorBStageMemory::default()
            }),
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        let metadata = BossStepRoutedMetadata {
            fallback_tier: Some("verification_first".into()),
            ..BossStepRoutedMetadata::default()
        };

        let memory =
            project_executor_b_stage_memory(&step, Some(&metadata)).expect("memory projected");
        assert_eq!(
            memory.continuity,
            Some(ExecutorBStageMemoryContinuity::VerificationFirstIsolated)
        );
        assert_eq!(memory.recent_reads, vec!["src/lib.rs"]);
    }

    #[tokio::test]
    async fn lism_sample_report_surfaces_latest_stage_continuation_context() {
        let coordinator = BossCoordinator::new();
        {
            let mut status = coordinator.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(1);
            status.total_steps = Some(2);
        }
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                plan_id: "plan-alpha".into(),
                task_description: "task".into(),
                document_spec: String::new(),
                pseudo_code: String::new(),
                draft_spec: None,
                review_feedback: None,
                revision_notes: None,
                finalized: true,
                documentation_feedback: Vec::new(),
                steps: vec![
                    BossPlanStep {
                        id: 0,
                        description: "step 0".into(),
                        objective: Some("objective 0".into()),
                        acceptance: vec!["acceptance 0".into()],
                        requires_approval: false,
                        status: BossPlanStepStatus::Completed,
                        completed: true,
                        result_diff: None,
                        worker_task_id: None,
                        attempt_count: 1,
                        retry_budget: 3,
                        last_review_summary: Some("done".into()),
                        last_correction: None,
                        stage_execution_contract: StageExecutionContract::default(),
                        stage_continuation_context: None,
                        executor_b_stage_memory: None,
                        review_task_id: None,
                        tool_execution_records: Vec::new(),
                    },
                    BossPlanStep {
                        id: 1,
                        description: "step 1".into(),
                        objective: Some("objective 1".into()),
                        acceptance: vec!["acceptance 1".into()],
                        requires_approval: false,
                        status: BossPlanStepStatus::ReplanRequired,
                        completed: false,
                        result_diff: None,
                        worker_task_id: None,
                        attempt_count: 2,
                        retry_budget: 3,
                        last_review_summary: Some("repair needed".into()),
                        last_correction: Some("/tmp/failed-report.md".into()),
                        stage_execution_contract: StageExecutionContract::default(),
                        stage_continuation_context: None,
                        executor_b_stage_memory: None,
                        review_task_id: None,
                        tool_execution_records: vec![ToolExecutionRecord {
                            tool_name: "ArtifactVerify".into(),
                            outcome: "success".into(),
                            kind: ToolExecutionOutcomeKind::Success,
                            summary: "artifact exists: /tmp/partial-report.md".into(),
                            detail: None,
                            pending_approval: None,
                            report_modifier: ToolReportModifier::None,
                            observable_input: None,
                            batch_context: ToolBatchContext {
                                batch_index: 0,
                                batch_size: 1,
                                executed_in_batch: false,
                            },
                        }],
                    },
                ],
                accepted_by_user: true,
                auto_sequence: true,
                session_snapshot: None,
            });
        }

        let report = coordinator.build_lism_sample_report(None).await;
        let step_context = report.steps[1]
            .stage_continuation_context
            .clone()
            .expect("step continuation context");
        let top_level = report
            .stage_continuation_context
            .clone()
            .expect("top-level continuation context");

        assert_eq!(
            step_context.failed_target.as_deref(),
            Some("/tmp/failed-report.md")
        );
        assert_eq!(
            step_context.verified_facts,
            vec!["artifact exists: /tmp/partial-report.md"]
        );
        assert_eq!(step_context.next_action.as_deref(), Some("/tmp/failed-report.md"));
        assert_eq!(
            step_context.continuity_mode,
            Some(ContinuityMode::Repair)
        );
        assert_eq!(
            step_context.repair_intent,
            Some(RepairIntent {
                failed_target: Some("/tmp/failed-report.md".into()),
                verified_facts: vec!["artifact exists: /tmp/partial-report.md".into()],
                next_action: Some("/tmp/failed-report.md".into()),
                continuity_mode: Some(ContinuityMode::Repair),
            })
        );
        assert_eq!(top_level, step_context);
    }

    #[test]
    fn rollout_execution_policy_forces_full_dispatch_for_exact_artifact_gap() {
        let metadata = BossStepRoutedMetadata {
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:2".into(),
                target_path: Some("/tmp/two.md".into()),
                missing_artifact_evidence: true,
                missing_test_evidence: false,
                missing_verification_evidence: false,
                recommended_action: "write_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");

        assert_eq!(policy.forced_worker_lism_policy, WorkerLisMPolicy::ForceOff);
        assert_eq!(policy.fallback_tier, "full_worker_dispatch");
        assert_eq!(policy.fallback_reason, "rollout_policy_exact_artifact_gap");
        assert_eq!(policy.affected_gaps.len(), 1);
        assert_eq!(policy.affected_gaps[0].target_ref, "artifact:contract:2");
    }

    #[test]
    fn verification_only_gap_is_not_labeled_exact_artifact_gap() {
        let metadata = BossStepRoutedMetadata {
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:b".into(),
                target_path: Some("/tmp/b.md".into()),
                missing_artifact_evidence: false,
                missing_test_evidence: false,
                missing_verification_evidence: true,
                recommended_action: "verify_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");

        assert_eq!(policy.fallback_tier, "verification_first");
        assert_eq!(policy.fallback_reason, "rollout_policy_verification_gap");
        assert_eq!(policy.worker_role, WorkerRole::Verify);
        assert!(policy.force_fresh_spawn);
    }

    #[test]
    fn rollout_execution_policy_routes_test_only_gap_to_verification_or_full_dispatch() {
        let metadata = BossStepRoutedMetadata {
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:test".into(),
                target_path: Some("/tmp/report.md".into()),
                missing_artifact_evidence: false,
                missing_test_evidence: true,
                missing_verification_evidence: false,
                recommended_action: "run_verification".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");

        assert_eq!(policy.forced_worker_lism_policy, WorkerLisMPolicy::ForceOff);
        assert_eq!(policy.fallback_tier, "verification_first");
        assert_eq!(policy.fallback_reason, "rollout_policy_test_evidence_gap");
        assert_eq!(policy.worker_role, WorkerRole::Verify);
        assert!(policy.force_fresh_spawn);
        assert_eq!(policy.affected_gaps.len(), 1);
        assert_eq!(policy.affected_gaps[0].target_ref, "artifact:contract:test");
    }

    #[test]
    fn rollout_execution_policy_clears_when_gap_is_gone() {
        let metadata = BossStepRoutedMetadata::default();
        assert!(BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata)).is_none());
    }

    #[test]
    fn rollout_execution_policy_is_step_scoped_for_multi_artifact_history() {
        let metadata = BossStepRoutedMetadata {
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:b".into(),
                target_path: Some("/tmp/b.md".into()),
                missing_artifact_evidence: false,
                missing_test_evidence: false,
                missing_verification_evidence: true,
                recommended_action: "verify_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");

        assert_eq!(policy.affected_gaps.len(), 1);
        assert_eq!(policy.affected_gaps[0].target_ref, "artifact:contract:b");
        assert_eq!(
            policy.affected_gaps[0].target_path.as_deref(),
            Some("/tmp/b.md")
        );
    }

    #[tokio::test]
    async fn verify_first_spawn_payload_uses_verify_role_and_force_off_lism() {
        let coordinator = BossCoordinator::new();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                plan_id: "plan-verify-first".into(),
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![BossPlanStep {
                    id: 1,
                    description: "verify target".into(),
                    objective: Some("Run verification on /tmp/report.md".into()),
                    acceptance: vec!["verification evidence recorded".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Pending,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut routed = coordinator.routed_step_metadata.write().await;
            routed.insert(
                1,
                BossStepRoutedMetadata {
                    completion_evidence_gaps: vec![CompletionEvidenceGap {
                        target_ref: "artifact:contract:test".into(),
                        target_path: Some("/tmp/report.md".into()),
                        missing_artifact_evidence: false,
                        missing_test_evidence: true,
                        missing_verification_evidence: false,
                        recommended_action: "run_verification".into(),
                    }],
                    ..BossStepRoutedMetadata::default()
                },
            );
        }

        let payload = coordinator
            .build_step_spawn_payload(1, "parent-session", "boss-b")
            .await
            .expect("spawn payload");
        let json: serde_json::Value = serde_json::from_str(&payload).expect("json payload");

        assert_eq!(json.get("role").and_then(|v| v.as_str()), Some("verify"));
        assert_eq!(
            json.get("lism_policy").and_then(|v| v.as_str()),
            Some("force-off")
        );
        assert_eq!(
            json.get("reuse_strategy").and_then(|v| v.as_str()),
            Some("fresh")
        );
    }

    #[test]
    fn rollout_execution_policy_escalates_test_only_gap_after_verification_first() {
        let metadata = BossStepRoutedMetadata {
            fallback_tier: Some("verification_first".into()),
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:test".into(),
                target_path: Some("/tmp/report.md".into()),
                missing_artifact_evidence: false,
                missing_test_evidence: true,
                missing_verification_evidence: false,
                recommended_action: "run_verification".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");

        assert_eq!(policy.fallback_tier, "full_worker_dispatch");
        assert_eq!(
            policy.fallback_reason,
            "rollout_policy_test_evidence_gap_escalated"
        );
        assert_eq!(policy.worker_role, WorkerRole::Implement);
        assert!(!policy.force_fresh_spawn);
    }

    #[test]
    fn verification_only_gap_does_not_recommend_full_worker_dispatch() {
        let steps = vec![BossStepReport {
            id: 1,
            status: BossPlanStepStatus::Rejected,
            worker_task_id: Some("task-0".into()),
            attempt_count: 1,
            last_review_summary: Some("verify again".into()),
            action_required: None,
            blocker_reason: None,
            routed_metadata: Some(BossStepRoutedMetadata {
                step_failure_classification: Some(
                    StepFailureClassification::VerificationRepairContinuation,
                ),
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:contract:verify".into(),
                    target_path: Some("/tmp/report.md".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                }],
                ..BossStepRoutedMetadata::default()
            }),
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
        }];

        let decision =
            BossCoordinator::derive_rollout_policy_decision(&steps).expect("policy decision");
        assert_eq!(decision.fallback_targets.len(), 1);
        assert_eq!(
            decision.fallback_targets[0].recommended_fallback,
            "verification_first"
        );
        assert!(decision.denylist_targets.is_empty());
    }

    #[test]
    fn verification_repair_continuation_prefers_local_reverify_over_full_dispatch() {
        let metadata = BossStepRoutedMetadata {
            step_failure_classification: Some(
                StepFailureClassification::VerificationRepairContinuation,
            ),
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:verify".into(),
                target_path: Some("/tmp/report.md".into()),
                missing_artifact_evidence: false,
                missing_test_evidence: false,
                missing_verification_evidence: true,
                recommended_action: "verify_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");
        assert_eq!(policy.fallback_tier, "verification_first");
        assert_eq!(policy.fallback_reason, "rollout_policy_verification_gap");
        assert_eq!(policy.worker_role, WorkerRole::Verify);
        assert!(policy.force_fresh_spawn);
    }

    #[test]
    fn artifact_plus_verification_gap_still_allows_full_dispatch() {
        let metadata = BossStepRoutedMetadata {
            step_failure_classification: Some(
                StepFailureClassification::VerificationRepairContinuation,
            ),
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:combo".into(),
                target_path: Some("/tmp/report.md".into()),
                missing_artifact_evidence: true,
                missing_test_evidence: false,
                missing_verification_evidence: true,
                recommended_action: "write_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");
        assert_eq!(policy.fallback_tier, "full_worker_dispatch");
        assert_eq!(policy.fallback_reason, "rollout_policy_exact_artifact_gap");
        assert_eq!(policy.worker_role, WorkerRole::Implement);
    }

    #[test]
    fn u7_verification_only_gap_keeps_fallback_tier_off_full_worker_dispatch() {
        let metadata = BossStepRoutedMetadata {
            step_failure_classification: Some(
                StepFailureClassification::VerificationRepairContinuation,
            ),
            completion_evidence_gaps: vec![
                CompletionEvidenceGap {
                    target_ref: "artifact:step0:0:/tmp/site".into(),
                    target_path: Some("/tmp/site".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                },
                CompletionEvidenceGap {
                    target_ref: "artifact:step0:1:/tmp/site/README.md".into(),
                    target_path: Some("/tmp/site/README.md".into()),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "verify_artifact".into(),
                },
            ],
            ..BossStepRoutedMetadata::default()
        };

        let policy = BossCoordinator::resolve_step_rollout_execution_policy(Some(&metadata))
            .expect("execution policy");
        assert_eq!(policy.fallback_tier, "verification_first");
        assert_ne!(policy.fallback_tier, "full_worker_dispatch");
    }

    #[tokio::test]
    async fn missing_artifact_after_done_escalates_to_repair_instead_of_terminal_success() {
        let coordinator = BossCoordinator::new();
        let target_path = std::env::temp_dir().join(format!(
            "boss_missing_artifact_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let objective = format!(
            "任务目标：\n- 目标文件：{}\n- 生成一份 markdown 报告",
            target_path.display()
        );
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                steps: vec![BossPlanStep {
                    id: 1,
                    description: "write report".into(),
                    objective: Some(objective),
                    acceptance: vec!["target file exists and is non-empty".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut metadata = coordinator.routed_step_metadata.write().await;
            metadata.insert(1, BossStepRoutedMetadata::default());
        }

        coordinator
            .apply_review_verdict(
                1,
                &crate::core::boss_actor_runtime::ReviewDecision::Accept {
                    summary: "worker says done".into(),
                },
            )
            .await
            .expect("apply review verdict");

        let plan = coordinator.plan.read().await;
        let step = plan
            .as_ref()
            .and_then(|plan| plan.steps.iter().find(|step| step.id == 1))
            .expect("step");
        assert_eq!(step.status, BossPlanStepStatus::Rejected);
        assert!(!step.completed);
        assert_eq!(step.attempt_count, 1);
        let correction = step.last_correction.as_deref().expect("repair correction");
        assert_eq!(correction, "verify_artifact");

        let routed_metadata = coordinator.routed_step_metadata.read().await;
        let metadata = routed_metadata.get(&1).expect("routed metadata");
        assert_eq!(metadata.recovery_attempted, Some(true));
        assert_eq!(
            metadata.recovery_tier.as_deref(),
            Some("boss_artifact_repair")
        );
        assert_eq!(
            metadata.recovery_outcome.as_deref(),
            Some("repair_dispatched")
        );
        assert_eq!(metadata.terminal_blocker_kind, None);
    }

    #[tokio::test]
    async fn test_user_approval_requires_finalized_documentation() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan::default());
        }

        let confirmed = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(!confirmed);
        assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);
        assert!(
            !coordinator
                .plan
                .read()
                .await
                .as_ref()
                .unwrap()
                .accepted_by_user
        );
    }

    #[tokio::test]
    async fn test_handle_user_approval_rejects_call_from_wrong_state() {
        let coordinator = BossCoordinator::new();
        // Still in Documentation (not WaitingForApproval) — should be a no-op and return false
        let result = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(!result);
        // Should remain unchanged
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn test_boss_plan_persistence() {
        let plan = BossPlan {
            plan_id: "plan-test".into(),
            task_description: "Fix bugs".into(),
            document_spec: "Spec v1".into(),
            pseudo_code: "Code v1".into(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let temp_dir = std::env::temp_dir().join(format!(
            "boss_test_plan_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let plan_path = temp_dir.join("planning.json");

        save_plan(&plan, &plan_path).await.unwrap();
        let loaded = load_plan(&plan_path).await.unwrap();

        assert_eq!(loaded.task_description, "Fix bugs");
        assert_eq!(loaded.document_spec, "Spec v1");
        assert!(loaded.accepted_by_user);

        std::fs::remove_file(&plan_path).unwrap();
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_default_plan_path_uses_morgo_boss_dir() {
        let root = std::path::Path::new("/home/user/project");
        let path = BossCoordinator::default_plan_path(root);
        assert_eq!(
            path,
            std::path::Path::new("/home/user/project/.morgo/boss/planning.json")
        );
    }

    #[tokio::test]
    async fn test_restore_or_init_handles_state_properly() {
        let temp_dir = std::env::temp_dir().join(format!(
            "boss_test_restore_plan_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let plan_path = temp_dir.join("planning.json");

        // 1. Init without file
        let new_coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
        assert_eq!(new_coordinator.get_stage().await, BossStage::Documentation);
        assert_eq!(
            new_coordinator
                .status
                .read()
                .await
                .planning_file
                .as_ref()
                .unwrap(),
            &plan_path.to_string_lossy().into_owned()
        );

        // 2. Save a plan that is accepted
        let plan = BossPlan {
            plan_id: "plan-restore".into(),
            task_description: "task".into(),
            accepted_by_user: true,
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![crate::core::boss_state::BossPlanStep {
                id: 0,
                description: "".into(),
                objective: None,
                acceptance: Vec::new(),
                requires_approval: false,
                status: BossPlanStepStatus::Pending,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            ..Default::default()
        };
        save_plan(&plan, &plan_path).await.unwrap();

        // 3. Restore and verify it skips straight to Execution
        let restored = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
        assert_eq!(restored.get_stage().await, BossStage::Execution);
        assert_eq!(restored.status.read().await.current_step, Some(0));

        std::fs::remove_file(&plan_path).unwrap();
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn test_has_terminal_failure_detects_failed_step() {
        let coordinator = BossCoordinator::new();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![crate::core::boss_state::BossPlanStep {
                    id: 0,
                    description: "failed".into(),
                    objective: None,
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Failed,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..Default::default()
            });
        }

        assert!(coordinator.has_terminal_failure().await);
    }

    #[test]
    fn seed_step_acceptance_adds_artifact_expectation_for_target_file() {
        let acceptance = seed_step_acceptance(
            "任务目标：\n- 目标文件：/tmp/example-report.md\n- 生成一份 markdown 报告",
        );
        assert!(
            acceptance
                .iter()
                .any(|item| item == "Task completed successfully.")
        );
        assert!(
            acceptance.iter().any(|item| {
                item == "target file exists and is non-empty: /tmp/example-report.md"
            })
        );
    }

    #[test]
    fn seed_step_acceptance_adds_readme_expectation_for_target_directory_tasks() {
        let acceptance = seed_step_acceptance(
            "任务目标：\n- 目标目录：/tmp/example-agent-site\n- 输出一个简短 README，说明如何打开与查看。",
        );
        assert!(acceptance.iter().any(|item| {
            item == "target directory exists and is non-empty: /tmp/example-agent-site"
        }));
        assert!(acceptance.iter().any(|item| {
            item == "target file exists and is non-empty: /tmp/example-agent-site/README.md"
        }));
    }

    #[test]
    fn extract_relevant_file_handles_normalizes_agent_relative_paths() {
        let repo_root = Path::new("/Users/wangmorgan/MProject/LearnCCfromCC");
        assert_eq!(
            normalize_relevant_file_hint("src/tool/definition.rs", Some(repo_root)).as_deref(),
            Some("RustAgent/Agent/src/tool/definition.rs")
        );
        assert_eq!(
            normalize_relevant_file_hint(
                "../docs/30-boss-mode-and-dual-agent-workflow.md",
                Some(repo_root)
            )
            .as_deref(),
            Some("RustAgent/docs/30-boss-mode-and-dual-agent-workflow.md")
        );
    }

    #[test]
    fn extract_relevant_file_handles_ignores_root_only_tokens() {
        let handles = extract_relevant_file_handles(
            "任务目标：\n- 工具输入：\n  - /\n  - /tmp/example/samples/\n- 目标文件：/tmp/example/report.md",
            "step-1-attempt-0",
        );
        assert!(!handles.iter().any(|handle| handle.path == "/"));
        assert!(
            handles
                .iter()
                .any(|handle| handle.path == "/tmp/example/samples/"
                    && handle.kind == "target_directory")
        );
        assert!(handles.iter().any(|handle| {
            handle.path == "/tmp/example/report.md"
                && handle.kind == "target_file"
                && handle.step_revision == "step-1-attempt-0"
        }));
    }

    #[test]
    fn extract_relevant_file_handles_filters_slash_commands_and_malformed_path_tokens() {
        let handles = extract_relevant_file_handles(
            "任务目标：\n- /boss\n- /mcp\n- 已完成，`/tmp/example/report.md\n- 目标目录：/tmp/example/output/\n- 目标文件：/tmp/example/report.md",
            "step-1-attempt-1",
        );
        assert!(!handles.iter().any(|handle| handle.path == "/boss"));
        assert!(!handles.iter().any(|handle| handle.path == "/mcp"));
        assert!(handles.iter().any(
            |handle| handle.path == "/tmp/example/output/" && handle.kind == "target_directory"
        ));
        assert!(
            handles
                .iter()
                .any(|handle| handle.path == "/tmp/example/report.md"
                    && handle.kind == "target_file")
        );
    }

    #[test]
    fn artifact_expectations_drop_pseudo_targets_before_real_targets() {
        let expectations = extract_artifact_expectations(
            "任务目标：\n- /boss\n- /\n- 目标文件：/tmp/example/report.md\n- 目标目录：/tmp/example/output/",
        );

        assert_eq!(expectations.len(), 2);
        assert!(expectations.iter().all(|item| item.path != PathBuf::from("/boss")));
        assert!(expectations.iter().all(|item| item.path != PathBuf::from("/")));
        assert!(expectations
            .iter()
            .any(|item| item.path == PathBuf::from("/tmp/example/report.md")));
        assert!(expectations
            .iter()
            .any(|item| item.path == PathBuf::from("/tmp/example/output/")));
    }

    #[test]
    fn collect_recent_decisions_keeps_latest_review_summaries() {
        let mut steps = Vec::new();
        for id in 0..5 {
            let mut step = BossPlanStep {
                id,
                description: format!("step {id}"),
                objective: Some(format!("objective {id}")),
                acceptance: vec![format!("acceptance {id}")],
                requires_approval: false,
                status: BossPlanStepStatus::Completed,
                completed: true,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: Some(format!("summary {id}")),
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            };
            if id == 4 {
                step.status = BossPlanStepStatus::Pending;
                step.completed = false;
            }
            steps.push(step);
        }
        let plan = BossPlan {
            plan_id: "plan-alpha".into(),
            task_description: "task".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps,
            accepted_by_user: true,
            auto_sequence: true,
            session_snapshot: None,
        };

        let recent = collect_recent_decisions(&plan, 4);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0], "step 1 review: summary 1");
        assert_eq!(recent[2], "step 3 review: summary 3");
    }

    #[test]
    fn collect_target_artifacts_merges_expectations_and_target_files() {
        let step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some(
                "任务目标：\n- 目标文件：/tmp/report.md\n- 目标目录：/tmp/results/\n- 产出 markdown 报告"
                    .into(),
            ),
            acceptance: Vec::new(),
            requires_approval: false,
            status: BossPlanStepStatus::Pending,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        let artifacts =
            collect_target_artifacts(&step, &["/tmp/report.md".into(), "/tmp/results/".into()]);
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/report.md")
        );
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/results/")
        );
    }

    #[test]
    fn collect_blocked_items_uses_review_summary_for_failed_steps() {
        let step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some("objective".into()),
            acceptance: Vec::new(),
            requires_approval: false,
            status: BossPlanStepStatus::Failed,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: Some("tests are still failing".into()),
            last_correction: None,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        assert_eq!(
            collect_blocked_items(&step),
            vec!["tests are still failing"]
        );
    }

    #[test]
    fn store_step_result_diff_prefers_primary_but_uses_fallback() {
        let mut step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some("objective".into()),
            acceptance: Vec::new(),
            requires_approval: false,
            status: BossPlanStepStatus::Pending,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_execution_contract: StageExecutionContract::default(),
            stage_continuation_context: None,
            executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        store_step_result_diff(&mut step, "", Some("fallback summary"));
        assert_eq!(step.result_diff.as_deref(), Some("fallback summary"));
        store_step_result_diff(&mut step, "primary result", Some("ignored"));
        assert_eq!(step.result_diff.as_deref(), Some("primary result"));
    }
}

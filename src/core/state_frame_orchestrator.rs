use crate::bootstrap::model_profiles::ModelProfileRegistry;
use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_frame::{ActorRole, StateFrame};
use crate::core::state_frame_loop::{
    DecisionLoopConfig, LoopOutcome, LoopUsage, run_decision_loop,
};
use crate::core::state_frame_model_resolver::resolve_step_model;
use crate::core::state_frame_model_router::{ModelRoute, route_model_tier};
use crate::core::state_frame_projection::{collect_projection_diagnostics, project_state_frame};
use crate::core::state_frame_router::{apply_route, route_toolset};
use crate::service::api::client::{ModelPricing, ModelProviderClient};
use crate::service::observability::ServiceObservabilityTracker;
use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;

/// Outcome of a single step execution via the StateFrame orchestrator seam.
#[derive(Debug, Clone)]
pub enum StepOutcome {
    Completed {
        usage: LoopUsage,
    },
    Failed {
        reason: String,
        usage: Option<LoopUsage>,
    },
}

#[derive(Debug, Clone)]
pub struct RoutedStateFrame {
    pub frame: StateFrame,
    pub model_route: ModelRoute,
    pub projection_mismatch_count: usize,
}

fn contains_external_effect_marker(text: &str) -> bool {
    let lowered = text.to_lowercase();
    [
        "目标目录",
        "目标文件",
        "目标路径",
        "创建",
        "生成",
        "写入",
        "修改文件",
        "运行命令",
        "运行一次",
        "执行命令",
        "create ",
        "write ",
        "modify ",
        "edit ",
        "run ",
        "execute ",
        "target directory",
        "target file",
        "output file",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

/// The current StateFrame loop can decide, summarize, and request context, but it does not
/// execute read/write/shell tool calls. Until tool dispatch is wired, direct LisM execution must
/// reject tasks whose success depends on filesystem or command side effects.
pub fn requires_external_tool_execution(frame: &StateFrame) -> bool {
    frame.role == ActorRole::Worker
        && !matches!(
            frame.required_output_schema.as_deref(),
            Some("readonly_audit_4_paragraphs_v1")
        )
        && contains_external_effect_marker(&frame.objective)
}

fn external_tool_execution_unsupported() -> StepOutcome {
    StepOutcome::Failed {
        reason: "StateFrame direct execution cannot yet perform required filesystem or command side effects; use full worker path or wire tool dispatch before enabling LisM for this step".into(),
        usage: None,
    }
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
    let mut frame = project_state_frame(plan, stage, Some(step_id), role);
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
}

#[derive(Debug, Clone)]
pub struct ResolvedRoutedStep {
    pub routed: RoutedStateFrame,
    pub resolved_snapshot: ActiveModelRuntimeSnapshot,
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
    if requires_external_tool_execution(&routed.frame) {
        return Ok(external_tool_execution_unsupported());
    }
    let outcome = run_decision_loop(client, routed.frame, config).await?;
    Ok(map_loop_outcome(outcome))
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
    if requires_external_tool_execution(&routed.frame) {
        return Ok(external_tool_execution_unsupported());
    }
    let resolved = resolve_routed_step_runtime(routed, runtime)?;
    let outcome = run_decision_loop(
        &resolved.resolved_snapshot.client,
        resolved.routed.frame,
        config,
    )
    .await?;
    Ok(map_loop_outcome_with_pricing(
        outcome,
        &resolved.resolved_snapshot.config.pricing,
    ))
}

pub fn resolve_routed_step_runtime<'a>(
    routed: RoutedStateFrame,
    runtime: StepRuntimeResolutionContext<'a>,
) -> anyhow::Result<ResolvedRoutedStep> {
    let resolved = resolve_step_model(
        &routed.model_route,
        runtime.inherited_snapshot,
        runtime.model_registry,
        runtime.observability,
    )?;
    Ok(ResolvedRoutedStep {
        routed,
        resolved_snapshot: resolved.snapshot,
    })
}

pub async fn run_routed_step_with_runtime<'a>(
    routed: RoutedStateFrame,
    config: DecisionLoopConfig,
    runtime: StepRuntimeResolutionContext<'a>,
) -> anyhow::Result<StepOutcome> {
    if requires_external_tool_execution(&routed.frame) {
        return Ok(external_tool_execution_unsupported());
    }
    let resolved = resolve_routed_step_runtime(routed, runtime)?;
    let outcome = run_decision_loop(
        &resolved.resolved_snapshot.client,
        resolved.routed.frame,
        config,
    )
    .await?;
    Ok(map_loop_outcome_with_pricing(
        outcome,
        &resolved.resolved_snapshot.config.pricing,
    ))
}

fn map_loop_outcome(outcome: LoopOutcome) -> StepOutcome {
    match outcome {
        LoopOutcome::Done { usage, .. } => StepOutcome::Completed { usage },
        LoopOutcome::Rejected { reason, usage } => StepOutcome::Failed {
            reason,
            usage: Some(usage),
        },
        LoopOutcome::MaxIterationsReached { last_state, usage } => StepOutcome::Failed {
            reason: format!("max iterations reached; last state: {last_state:?}"),
            usage: Some(usage),
        },
        LoopOutcome::NoProgress {
            last_state,
            reason,
            usage,
        } => StepOutcome::Failed {
            reason: format!("{reason}; last state: {last_state:?}"),
            usage: Some(usage),
        },
        LoopOutcome::RepairExhausted {
            reason,
            raw_json,
            usage,
        } => StepOutcome::Failed {
            reason: format!("repair exhausted: {reason}; raw: {raw_json}"),
            usage: Some(usage),
        },
    }
}

fn map_loop_outcome_with_pricing(outcome: LoopOutcome, pricing: &ModelPricing) -> StepOutcome {
    match map_loop_outcome(outcome) {
        StepOutcome::Completed { mut usage } => {
            usage.estimated_cost_micros_usd = estimate_loop_usage_cost_micros(&usage, pricing);
            StepOutcome::Completed { usage }
        }
        StepOutcome::Failed {
            reason,
            usage: Some(mut usage),
        } => {
            usage.estimated_cost_micros_usd = estimate_loop_usage_cost_micros(&usage, pricing);
            StepOutcome::Failed {
                reason,
                usage: Some(usage),
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

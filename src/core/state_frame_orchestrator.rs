use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_frame::{ActorRole, StateFrame};
use crate::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
use crate::core::state_frame_model_router::{ModelRoute, route_model_tier};
use crate::core::state_frame_projection::project_state_frame;
use crate::core::state_frame_router::{apply_route, route_toolset};
use crate::service::api::client::ModelProviderClient;

/// Outcome of a single step execution via the StateFrame orchestrator seam.
#[derive(Debug, Clone)]
pub enum StepOutcome {
    Completed,
    Failed { reason: String },
}

#[derive(Debug, Clone)]
pub struct RoutedStateFrame {
    pub frame: StateFrame,
    pub model_route: ModelRoute,
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
    let route = route_toolset(&frame);
    apply_route(&mut frame, route);
    let model_route = route_model_tier(frame.budget.effort, frame.role, frame.state);
    RoutedStateFrame { frame, model_route }
}

/// Run a single plan step through the StateFrame decision loop.
///
/// Pure orchestrator seam — no AppState, no session actors, no BossCoordinator mutation.
/// Callers are responsible for persisting the outcome back to the plan.
pub async fn run_step_with_state_frame(
    client: &ModelProviderClient,
    plan: &BossPlan,
    stage: BossStage,
    step_id: usize,
    role: ActorRole,
    config: DecisionLoopConfig,
) -> anyhow::Result<StepOutcome> {
    let routed = build_routed_state_frame_with_model_route(plan, stage, step_id, role);
    let outcome = run_decision_loop(client, routed.frame, config).await?;
    Ok(map_loop_outcome(outcome))
}

fn map_loop_outcome(outcome: LoopOutcome) -> StepOutcome {
    match outcome {
        LoopOutcome::Done { .. } => StepOutcome::Completed,
        LoopOutcome::Rejected { reason } => StepOutcome::Failed { reason },
        LoopOutcome::MaxIterationsReached { last_state } => StepOutcome::Failed {
            reason: format!("max iterations reached; last state: {last_state:?}"),
        },
        LoopOutcome::RepairExhausted { reason, raw_json } => StepOutcome::Failed {
            reason: format!("repair exhausted: {reason}; raw: {raw_json}"),
        },
    }
}

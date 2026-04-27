use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_frame::ActorRole;
use crate::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
use crate::core::state_frame_projection::project_state_frame;
use crate::service::api::client::ModelProviderClient;

/// Outcome of a single step execution via the StateFrame orchestrator seam.
#[derive(Debug, Clone)]
pub enum StepOutcome {
    Completed,
    Failed { reason: String },
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
    let frame = project_state_frame(plan, stage, Some(step_id), role);
    let outcome = run_decision_loop(client, frame, config).await?;
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

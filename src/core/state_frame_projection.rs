use crate::core::boss_state::{BossPlan, BossPlanStepStatus, BossStage};
use crate::core::state_frame::{ActorRole, AgentState, StateFrame, StateBudget};

/// Map a `BossStage` to the corresponding `AgentState` for prompt projection.
fn stage_to_agent_state(stage: BossStage) -> AgentState {
    match stage {
        BossStage::Documentation => AgentState::Planning,
        BossStage::WaitingForApproval => AgentState::Blocked,
        BossStage::Execution => AgentState::Executing,
        BossStage::Completed => AgentState::Done,
    }
}

/// Static allowed_actions per AgentState.
/// Reviewing / Correcting / Verifying are not yet reachable from BossStage — omitted.
fn allowed_actions_for_state(state: AgentState) -> Vec<String> {
    match state {
        AgentState::Planning => vec!["read_file".into(), "write_spec".into()],
        AgentState::Executing => vec!["read_file".into(), "edit_file".into(), "run_test".into()],
        AgentState::Blocked | AgentState::Done => vec![],
        // Reviewing / Correcting / Verifying not yet reachable from BossStage projection.
        _ => vec![],
    }
}

/// Project a `StateFrame` from a `BossPlan`, the current `BossStage`, an optional step id,
/// and the target actor role.
///
/// Pure function — no side effects, no LLM calls, no state mutation.
pub fn project_state_frame(
    plan: &BossPlan,
    stage: BossStage,
    step_id: Option<usize>,
    role: ActorRole,
) -> StateFrame {
    let state = stage_to_agent_state(stage);

    // objective: current step objective if available, else plan task description.
    let objective = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .map(|s| s.objective().to_string())
        .unwrap_or_else(|| plan.task_description.clone());

    // open_items: acceptance criteria of the current step (only if not yet completed).
    let open_items = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .filter(|s| !s.completed)
        .map(|s| s.acceptance.clone())
        .unwrap_or_default();

    // blocked_items: explicit blocker when waiting for approval.
    let blocked_items = if stage == BossStage::WaitingForApproval {
        vec!["waiting for user approval".into()]
    } else {
        vec![]
    };

    // accepted_summary: descriptions of completed steps only (not the current step).
    let current_id = step_id.unwrap_or(usize::MAX);
    let accepted_summary: Vec<String> = plan
        .steps
        .iter()
        .filter(|s| s.id != current_id && s.status == BossPlanStepStatus::Completed)
        .map(|s| s.description.clone())
        .collect();

    // recent_evidence: last review summary and/or correction from the current step.
    let recent_evidence: Vec<String> = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .map(|s| {
            let mut ev = Vec::new();
            if let Some(r) = &s.last_review_summary {
                ev.push(format!("review: {r}"));
            }
            if let Some(c) = &s.last_correction {
                ev.push(format!("correction: {c}"));
            }
            ev
        })
        .unwrap_or_default();

    let allowed_actions = allowed_actions_for_state(state);

    StateFrame {
        role,
        state,
        objective,
        open_items,
        blocked_items,
        accepted_summary,
        recent_evidence,
        allowed_actions,
        toolset_id: None,
        skillset_id: None,
        required_output_schema: Some("state_decision_v1".into()),
        budget: StateBudget::default(),
    }
}

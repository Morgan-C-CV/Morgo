use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_frame::{ActorRole, AgentState, StateFrame, StateBudget};
use crate::core::state_frame_archive::{
    archive_to_summary, build_accepted_archive, retain_blocked_items, retain_open_items,
};

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
        _ => vec![],
    }
}

/// Project a `StateFrame` from a `BossPlan`, the current `BossStage`, an optional step id,
/// and the target actor role.
///
/// Pure function — no side effects, no LLM calls, no state mutation.
/// Uses `state_frame_archive` for accepted_summary / open_items / blocked_items.
pub fn project_state_frame(
    plan: &BossPlan,
    stage: BossStage,
    step_id: Option<usize>,
    role: ActorRole,
) -> StateFrame {
    let state = stage_to_agent_state(stage);

    // Build archive of completed steps (excluding current step).
    let archive = build_accepted_archive(plan, step_id);

    // objective: current step objective if available, else plan task description.
    let objective = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .map(|s| s.objective().to_string())
        .unwrap_or_else(|| plan.task_description.clone());

    // open_items: unsatisfied acceptance criteria of the current step.
    let open_items = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .filter(|s| !s.completed)
        .map(|s| retain_open_items(&s.acceptance, &archive))
        .unwrap_or_default();

    // blocked_items: stage-driven via archive.
    let blocked_items = retain_blocked_items(stage, &archive);

    // accepted_summary: rendered from archive.
    let accepted_summary = archive_to_summary(&archive);

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


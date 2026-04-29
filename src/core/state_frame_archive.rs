use crate::core::boss_state::{BossPlan, BossPlanStepStatus, BossStage};

/// A single archived accepted step — immutable record of completed work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedItem {
    pub step_id: usize,
    pub description: String,
    /// Acceptance criteria that were satisfied when this step completed.
    pub acceptance_criteria: Vec<String>,
}

/// Build the accepted-item archive from a plan, excluding the current step.
///
/// Only `Completed` steps are included. The current step (if any) is excluded
/// so it does not appear in both `accepted_summary` and `open_items`.
pub fn build_accepted_archive(
    plan: &BossPlan,
    current_step_id: Option<usize>,
) -> Vec<AcceptedItem> {
    let exclude = current_step_id.unwrap_or(usize::MAX);
    plan.steps
        .iter()
        .filter(|s| s.id != exclude && s.status == BossPlanStepStatus::Completed)
        .map(|s| AcceptedItem {
            step_id: s.id,
            description: s.description.clone(),
            acceptance_criteria: s.acceptance.clone(),
        })
        .collect()
}

/// Retain only the open (unsatisfied) acceptance criteria for the current step.
///
/// A criterion is considered satisfied if it appears verbatim in any archived item's
/// `acceptance_criteria`. This prevents already-met criteria from re-appearing as open items.
pub fn retain_open_items(step_acceptance: &[String], archive: &[AcceptedItem]) -> Vec<String> {
    let satisfied: std::collections::HashSet<&str> = archive
        .iter()
        .flat_map(|a| a.acceptance_criteria.iter().map(|s| s.as_str()))
        .collect();

    step_acceptance
        .iter()
        .filter(|c| !satisfied.contains(c.as_str()))
        .cloned()
        .collect()
}

/// Return blocked items based on the current stage and archive.
///
/// `WaitingForApproval` always produces a single blocker entry.
/// All other stages return an empty list.
pub fn retain_blocked_items(stage: BossStage, _archive: &[AcceptedItem]) -> Vec<String> {
    if stage == BossStage::WaitingForApproval {
        vec!["waiting for user approval".into()]
    } else {
        vec![]
    }
}

/// Render the accepted archive as a `Vec<String>` for `StateFrame.accepted_summary`.
pub fn archive_to_summary(archive: &[AcceptedItem]) -> Vec<String> {
    archive.iter().map(|a| a.description.clone()).collect()
}

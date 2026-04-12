use crate::plan::types::{PlanState, PlanStep, PlanStepStatus};
use crate::state::app_state::AppState;

pub fn describe_plan_context(app_state: &AppState) -> String {
    let Some(manager) = app_state.permission_context.plan_manager.as_ref() else {
        return String::new();
    };
    let Some(state) = manager.state() else {
        return String::new();
    };
    if !matches!(state.status, crate::plan::types::PlanStatus::Approved) {
        return String::new();
    }

    let reconciled = app_state
        .permission_context
        .task_list_manager
        .as_ref()
        .and_then(|tasks| tasks.reconcile_plan_state(&state))
        .unwrap_or(state);
    render_approved_plan(&reconciled, app_state)
}

fn render_approved_plan(state: &PlanState, app_state: &AppState) -> String {
    let mut lines = vec![format!("Approved plan status: {}", state.status.as_str())];

    if let Some(execution) = &state.execution {
        lines.push(format!(
            "Execution summary: {}/{} completed ({}%)",
            execution.completed_steps, execution.total_steps, execution.progress_percent
        ));
        if let Some(active_step_id) = execution.active_step_id.as_ref() {
            lines.push(format!("Active step: {active_step_id}"));
        }
    }

    if let Some(draft) = &state.draft {
        if !draft.summary.trim().is_empty() {
            lines.push(format!("Plan summary: {}", draft.summary.trim()));
        }
        if !draft.steps.is_empty() {
            let linked = app_state
                .permission_context
                .task_list_manager
                .as_ref()
                .map(|tasks| tasks.tasks_grouped_by_plan_step())
                .unwrap_or_default();
            let blocked = linked
                .values()
                .flatten()
                .filter(|task| !task.blocked_by.is_empty())
                .count();
            let in_progress = draft
                .steps
                .iter()
                .filter(|step| step.status == PlanStepStatus::InProgress)
                .count();
            let completed = draft
                .steps
                .iter()
                .filter(|step| step.status == PlanStepStatus::Completed)
                .count();
            lines.push(format!(
                "Linked task summary: linked_steps={}, blocked_tasks={}, in_progress_steps={}, completed_steps={}",
                linked.len(), blocked, in_progress, completed
            ));
            if let Some(next_step) = draft
                .steps
                .iter()
                .find(|step| step.status == PlanStepStatus::InProgress)
                .or_else(|| draft.steps.iter().find(|step| step.status == PlanStepStatus::Pending))
            {
                lines.push(format!("Next actionable step: {}", next_step.title));
            }
            lines.push("Plan steps:".to_string());
            for step in &draft.steps {
                let linkage = linked
                    .get(step.id.as_str())
                    .map(|tasks| format!(" linked_tasks={}", tasks.len()))
                    .unwrap_or_else(|| " linked_tasks=0".to_string());
                lines.push(format!("- {} [{}]{}", render_step(step), step.status.as_str(), linkage));
            }
            let mismatches = draft
                .steps
                .iter()
                .filter(|step| !linked.contains_key(step.id.as_str()))
                .count();
            if mismatches > 0 {
                lines.push(format!("Warnings: {} approved step(s) are not yet linked to durable tasks.", mismatches));
            }
        }
        if let Some(notes) = draft.notes.as_ref().map(|value| value.trim()).filter(|value| !value.is_empty()) {
            lines.push(format!("Plan notes: {notes}"));
        }
    }

    if let Some(summary) = state
        .approval_summary
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Approval summary: {summary}"));
    }

    lines.push("Approved plan steps are linked to durable Task List entries; continue maintaining that task list during execution.".to_string());
    lines.join("\n")
}

fn render_step(step: &PlanStep) -> String {
    match step.details.as_ref().map(|value| value.trim()).filter(|value| !value.is_empty()) {
        Some(details) => format!("{} — {}", step.title, details),
        None => step.title.clone(),
    }
}

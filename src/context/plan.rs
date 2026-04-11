use crate::plan::types::{PlanState, PlanStep};
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

    render_approved_plan(&state)
}

fn render_approved_plan(state: &PlanState) -> String {
    let mut lines = vec![format!("Approved plan status: {}", state.status.as_str())];

    if let Some(draft) = &state.draft {
        if !draft.summary.trim().is_empty() {
            lines.push(format!("Plan summary: {}", draft.summary.trim()));
        }
        if !draft.steps.is_empty() {
            lines.push("Plan steps:".to_string());
            for step in &draft.steps {
                lines.push(format!("- {} [{}]", render_step(step), step.status.as_str()));
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

    lines.push("During execution, create and maintain Task List entries that track this approved plan.".to_string());
    lines.join("\n")
}

fn render_step(step: &PlanStep) -> String {
    match step.details.as_ref().map(|value| value.trim()).filter(|value| !value.is_empty()) {
        Some(details) => format!("{} — {}", step.title, details),
        None => step.title.clone(),
    }
}

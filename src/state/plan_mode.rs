use crate::plan::types::PlanState;
use crate::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
use crate::tool::definition::ToolResult;

pub fn render_plan_status(permissions: &ToolPermissionContext) -> String {
    let mode_line = if matches!(permissions.mode(), PermissionMode::Plan) {
        "Plan mode is on. Use /plan exit [summary] when ready to leave.".to_string()
    } else {
        "Plan mode is off. Use /plan enter [reason] to start planning.".to_string()
    };

    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        return mode_line;
    };
    let Some(state) = plan_manager.state() else {
        return format!("{mode_line}\nNo plan object exists for this session yet.");
    };

    format!("{mode_line}\n{}", summarize_plan_state(&state))
}

pub fn render_plan_show(permissions: &ToolPermissionContext) -> String {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        return "No plan manager is available in this session.".into();
    };
    let Some(state) = plan_manager.state() else {
        return "No plan object exists for this session yet.".into();
    };

    let mut lines = vec![format!("Plan status: {}", state.status.as_str())];
    if let Some(draft) = state.draft.as_ref() {
        if !draft.summary.trim().is_empty() {
            lines.push(format!("Summary: {}", draft.summary.trim()));
        }
        if let Some(notes) = draft.notes.as_ref().map(|value| value.trim()).filter(|value| !value.is_empty()) {
            lines.push(format!("Notes: {notes}"));
        }
        if draft.steps.is_empty() {
            lines.push("Steps: none".into());
        } else {
            lines.push("Steps:".into());
            for step in &draft.steps {
                let details = step
                    .details
                    .as_ref()
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty())
                    .map(|value| format!(" — {value}"))
                    .unwrap_or_default();
                lines.push(format!("- {} [{}]{}", step.title, step.status.as_str(), details));
            }
        }
    } else {
        lines.push("Draft: none".into());
    }

    if let Some(summary) = state
        .approval_summary
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Approval summary: {summary}"));
    }
    if let Some(approved_at) = state.approved_at.as_ref() {
        lines.push(format!("Approved at: {approved_at}"));
    }

    lines.join("\n")
}

pub fn request_enter_plan_mode(
    permissions: &ToolPermissionContext,
    reason: &str,
) -> ToolResult {
    if matches!(permissions.mode(), PermissionMode::Plan) {
        return ToolResult::Text("Already in plan mode.".into());
    }

    let message = if reason.trim().is_empty() {
        "approve entering plan mode".to_string()
    } else {
        format!("approve entering plan mode: {}", reason.trim())
    };
    permissions.set_pending_approval(Some(PendingApproval {
        tool_name: "EnterPlanMode".into(),
        tool_input: reason.trim().to_string(),
        message: message.clone(),
    }));

    ToolResult::PendingApproval {
        tool_name: "EnterPlanMode".into(),
        message,
    }
}

pub fn request_exit_plan_mode(
    permissions: &ToolPermissionContext,
    summary: &str,
) -> ToolResult {
    if !matches!(permissions.mode(), PermissionMode::Plan) {
        return ToolResult::Denied("Plan mode is not active.".into());
    }

    if let Some(plan_manager) = permissions.plan_manager.as_ref() {
        let Some(state) = plan_manager.state() else {
            return ToolResult::Denied("No plan draft exists to approve.".into());
        };
        let is_empty = state
            .draft
            .as_ref()
            .map(|draft| draft.summary.trim().is_empty() && draft.steps.is_empty())
            .unwrap_or(true);
        if is_empty {
            return ToolResult::Denied("Cannot approve an empty plan draft.".into());
        }
    }

    let message = if summary.trim().is_empty() {
        "approve exiting plan mode".to_string()
    } else {
        format!("approve exiting plan mode: {}", summary.trim())
    };
    permissions.set_pending_approval(Some(PendingApproval {
        tool_name: "ExitPlanMode".into(),
        tool_input: summary.trim().to_string(),
        message: message.clone(),
    }));

    ToolResult::PendingApproval {
        tool_name: "ExitPlanMode".into(),
        message,
    }
}

pub fn apply_enter_plan_mode(permissions: &ToolPermissionContext, reason: &str) -> String {
    permissions.set_mode(PermissionMode::Plan);
    if let Some(plan_manager) = permissions.plan_manager.as_ref() {
        plan_manager.ensure_draft(non_empty(reason));
    }
    if reason.trim().is_empty() {
        "entered plan mode".into()
    } else {
        format!("entered plan mode: {}", reason.trim())
    }
}

pub fn apply_exit_plan_mode(
    permissions: &ToolPermissionContext,
    summary: &str,
) -> anyhow::Result<String> {
    if let Some(plan_manager) = permissions.plan_manager.as_ref() {
        plan_manager.approve(non_empty(summary))?;
    }
    permissions.set_mode(PermissionMode::Default);
    Ok(if summary.trim().is_empty() {
        "plan approved; exited plan mode".into()
    } else {
        format!("plan approved; exited plan mode: {}", summary.trim())
    })
}

fn summarize_plan_state(state: &PlanState) -> String {
    let summary = state
        .draft
        .as_ref()
        .map(|draft| draft.summary.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("(no summary yet)");
    let steps = state
        .draft
        .as_ref()
        .map(|draft| draft.steps.len())
        .unwrap_or(0);
    format!(
        "Plan object: status={}, summary={}, steps={}",
        state.status.as_str(),
        summary,
        steps
    )
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

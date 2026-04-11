use crate::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
use crate::tool::definition::ToolResult;

pub fn render_plan_status(permissions: &ToolPermissionContext) -> String {
    if matches!(permissions.mode(), PermissionMode::Plan) {
        "Plan mode is on. Use /plan exit [summary] when ready to leave.".into()
    } else {
        "Plan mode is off. Use /plan enter [reason] to start planning.".into()
    }
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
    if reason.trim().is_empty() {
        "entered plan mode".into()
    } else {
        format!("entered plan mode: {}", reason.trim())
    }
}

pub fn apply_exit_plan_mode(permissions: &ToolPermissionContext, summary: &str) -> String {
    permissions.set_mode(PermissionMode::Default);
    if summary.trim().is_empty() {
        "plan approved; exited plan mode".into()
    } else {
        format!("plan approved; exited plan mode: {}", summary.trim())
    }
}

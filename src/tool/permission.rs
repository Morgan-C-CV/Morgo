use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::definition::{PermissionDecision, ToolCall, ToolMetadata};

pub fn is_tool_allowed(metadata: &ToolMetadata, permissions: &ToolPermissionContext) -> bool {
    matches!(
        evaluate_tool_permission(
            metadata,
            &ToolCall {
                name: metadata.name.into(),
                input: String::new(),
            },
            permissions
        ),
        PermissionDecision::Allow | PermissionDecision::Ask(_)
    )
}

pub fn evaluate_tool_permission(
    metadata: &ToolMetadata,
    call: &ToolCall,
    permissions: &ToolPermissionContext,
) -> PermissionDecision {
    if permissions
        .always_deny_rules
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Deny(format!("tool {} denied by explicit rule", metadata.name));
    }

    if metadata.destructive && matches!(permissions.mode, PermissionMode::Plan) {
        return PermissionDecision::Deny(format!(
            "tool {} not allowed in plan mode",
            metadata.name
        ));
    }

    if permissions
        .always_allow_rules
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Allow;
    }

    if metadata.requires_auth && permissions.always_allow_rules.is_empty() {
        return PermissionDecision::Ask(format!(
            "tool {} requires explicit approval",
            metadata.name
        ));
    }

    PermissionDecision::Allow
}

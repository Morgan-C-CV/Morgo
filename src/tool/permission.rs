use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::definition::{PermissionDecision, ToolCall, ToolMetadata};

pub fn is_tool_allowed(metadata: &ToolMetadata, permissions: &ToolPermissionContext) -> bool {
    matches!(
        evaluate_tool_permission(
            metadata,
            &ToolCall::new(metadata.name, String::new()),
            permissions
        ),
        PermissionDecision::Allow | PermissionDecision::Ask { .. }
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
        return PermissionDecision::Deny {
            message: format!("tool {} denied by explicit rule", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Rule,
        };
    }

    if metadata.destructive && matches!(permissions.mode, PermissionMode::Plan) {
        return PermissionDecision::Deny {
            message: format!("tool {} not allowed in plan mode", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Mode,
        };
    }

    if permissions
        .always_ask_rules
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Ask {
            message: format!("tool {} requires explicit approval by ask rule", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Rule,
        };
    }

    if permissions
        .always_allow_rules
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Allow;
    }

    if metadata.requires_auth && permissions.always_allow_rules.is_empty() {
        return PermissionDecision::Ask {
            message: format!("tool {} requires explicit approval", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Rule,
        };
    }

    PermissionDecision::Allow
}

use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::definition::{
    PermissionApprovalMetadata, PermissionDecision, ToolCall, ToolMetadata,
};

fn explicit_ask_rule_detail(metadata: &ToolMetadata) -> String {
    format!(
        "Reason: explicit approval is required by ask rule for {}.\nChoose approve to run it, or deny to keep it from executing.",
        metadata.name
    )
}

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
        .always_deny_rules()
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Deny {
            message: format!("tool {} denied by explicit rule", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Rule,
        };
    }

    if metadata.destructive && matches!(permissions.mode(), PermissionMode::Plan) {
        return PermissionDecision::Deny {
            message: format!("tool {} not allowed in plan mode", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Mode,
        };
    }

    if permissions
        .always_ask_rules()
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Ask {
            message: format!(
                "tool {} requires explicit approval by ask rule",
                metadata.name
            ),
            reason: crate::tool::definition::PermissionDecisionReason::Rule,
            metadata: Some(PermissionApprovalMetadata {
                code: Some("explicit_ask_rule".into()),
                summary: Some(format!("{} approval required", metadata.name)),
                detail: Some(explicit_ask_rule_detail(metadata)),
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["explicit_ask_rule".into()],
            }),
        };
    }

    if permissions
        .always_allow_rules()
        .iter()
        .any(|rule| rule == metadata.name || rule == call.name.as_str())
    {
        return PermissionDecision::Allow;
    }

    if metadata.should_defer && !metadata.always_load && !permissions.include_deferred_tools {
        return PermissionDecision::Deny {
            message: format!("tool {} is deferred until explicitly loaded", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Tool,
        };
    }

    if metadata.requires_user_interaction && !permissions.include_interactive_tools {
        return PermissionDecision::Deny {
            message: format!("tool {} requires an interactive surface", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Tool,
        };
    }

    if metadata.requires_auth && delegated_write_call_is_allowed(metadata, call, permissions) {
        return PermissionDecision::Allow;
    }

    if metadata.requires_auth && permissions.always_allow_rules().is_empty() {
        return PermissionDecision::Ask {
            message: format!("tool {} requires explicit approval", metadata.name),
            reason: crate::tool::definition::PermissionDecisionReason::Rule,
            metadata: None,
        };
    }

    PermissionDecision::Allow
}

fn delegated_write_call_is_allowed(
    metadata: &ToolMetadata,
    call: &ToolCall,
    permissions: &ToolPermissionContext,
) -> bool {
    if metadata.name != "Write" && metadata.name != "Edit" {
        return false;
    }
    let Some(input) = call.json_input() else {
        return false;
    };
    let Some(file_path) = input.get("file_path").and_then(|value| value.as_str()) else {
        return false;
    };
    if file_path.trim().is_empty() || !permissions.is_delegated_write_path(file_path.trim()) {
        return false;
    }
    let Some(policy) = permissions.filesystem_policy() else {
        return true;
    };
    policy
        .check_existing_or_create_path_for_write(std::path::Path::new(file_path.trim()))
        .is_allowed()
}

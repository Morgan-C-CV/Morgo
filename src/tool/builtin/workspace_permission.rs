use std::path::Path;

use crate::security::workspace_capability::{
    WorkspacePermissionCheck, WorkspacePermissionConfig, WorkspacePermissionLevel,
};
use crate::tool::definition::{
    PermissionApprovalMetadata, PermissionDecision, PermissionDecisionReason, ToolCall,
};

use crate::state::permission_context::ToolPermissionContext;

pub fn session_allow_rule_matches(
    tool_name: &str,
    call: &ToolCall,
    permissions: &ToolPermissionContext,
) -> bool {
    permissions
        .always_allow_rules()
        .iter()
        .any(|rule| rule == tool_name || rule == call.name.as_str())
}

pub fn decision_for_path(
    tool_name: &str,
    config: &WorkspacePermissionConfig,
    target: &Path,
    required: WorkspacePermissionLevel,
) -> PermissionDecision {
    match config.check_path(target, required) {
        WorkspacePermissionCheck::Allowed { .. } => PermissionDecision::Allow,
        WorkspacePermissionCheck::RequiresApproval {
            target_path,
            required,
            current,
            matched_path,
            reason,
        } => workspace_ask(
            tool_name,
            target_path.display().to_string(),
            required,
            current,
            matched_path.map(|path| path.display().to_string()),
            reason,
        ),
    }
}

pub fn workspace_ask(
    tool_name: &str,
    target: String,
    required: WorkspacePermissionLevel,
    current: Option<WorkspacePermissionLevel>,
    matched_path: Option<String>,
    reason: String,
) -> PermissionDecision {
    let current_label = current
        .map(|permission| permission.as_str().to_string())
        .unwrap_or_else(|| "untrusted".into());
    let scope_label = matched_path.unwrap_or_else(|| "no trusted workspace".into());
    let message = format!(
        "{tool_name} requires workspace {required} permission for {target}; current={current_label}; scope={scope_label}"
    );
    PermissionDecision::Ask {
        message: message.clone(),
        reason: PermissionDecisionReason::Safety,
        metadata: Some(PermissionApprovalMetadata {
            code: Some("workspace_permission".into()),
            summary: Some(format!("{tool_name} pending workspace approval")),
            detail: Some(format!(
                "Target: {target}\nRequired: {required}\nCurrent: {current_label}\nScope: {scope_label}\nReason: {reason}\nAction: choose an approval option below"
            )),
            approval_kind: Some("workspace_permission".into()),
            escalation_reasons: vec![
                reason,
                format!("workspace.required={}", required.as_str()),
                format!("workspace.current={current_label}"),
            ],
        }),
    }
}

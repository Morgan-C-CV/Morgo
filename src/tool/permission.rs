use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::ToolMetadata;

pub fn is_tool_allowed(metadata: &ToolMetadata, permissions: &ToolPermissionContext) -> bool {
    if permissions
        .always_deny_rules
        .iter()
        .any(|rule| rule == metadata.name)
    {
        return false;
    }

    if permissions
        .always_allow_rules
        .iter()
        .any(|rule| rule == metadata.name)
    {
        return true;
    }

    !(metadata.destructive
        && matches!(
            permissions.mode,
            crate::state::permission_context::PermissionMode::Plan
        ))
}

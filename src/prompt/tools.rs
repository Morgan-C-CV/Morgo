use crate::state::permission_context::ToolPermissionContext;
use crate::tool::registry::ToolRegistry;

pub fn build_tools_prompt(registry: &ToolRegistry, permissions: &ToolPermissionContext) -> String {
    registry
        .visible_tools(permissions)
        .into_iter()
        .map(|tool| format!("{} - {}", tool.name, tool.description))
        .collect::<Vec<_>>()
        .join("\n")
}

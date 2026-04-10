use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::web_fetch::WebFetchTool;
use rust_agent::tool::definition::Tool;
use rust_agent::tool::permission::is_tool_allowed;

#[test]
fn deny_rules_override_tool_visibility() {
    let mut context = ToolPermissionContext::new(PermissionMode::Default);
    context.always_deny_rules.push("WebFetch".into());
    let metadata = WebFetchTool.metadata();

    assert!(!is_tool_allowed(&metadata, &context));
}

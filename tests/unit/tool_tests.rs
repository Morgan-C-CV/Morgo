use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::web_fetch::WebFetchTool;
use rust_agent::tool::definition::{Tool, ToolCall};
use rust_agent::tool::permission::{evaluate_tool_permission, is_tool_allowed};

#[test]
fn deny_rules_override_tool_visibility() {
    let mut context = ToolPermissionContext::new(PermissionMode::Default);
    context.always_deny_rules.push("WebFetch".into());
    let metadata = WebFetchTool.metadata();

    assert!(!is_tool_allowed(&metadata, &context));
}

#[test]
fn destructive_tools_are_denied_in_plan_mode() {
    let context = ToolPermissionContext::new(PermissionMode::Plan);
    let mut metadata = WebFetchTool.metadata();
    metadata.destructive = true;
    let call = ToolCall {
        name: "WebFetch".into(),
        input: "https://example.com".into(),
    };

    assert!(matches!(
        evaluate_tool_permission(&metadata, &call, &context),
        rust_agent::tool::definition::PermissionDecision::Deny { .. }
    ));
}

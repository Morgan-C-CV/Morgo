use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::ask_user::AskUserQuestionTool;
use rust_agent::tool::builtin::web_fetch::WebFetchTool;
use rust_agent::tool::definition::{Tool, ToolCall};
use rust_agent::tool::permission::{evaluate_tool_permission, is_tool_allowed};

#[test]
fn deny_rules_override_tool_visibility() {
    let context = ToolPermissionContext::new(PermissionMode::Default);
    context.add_always_deny_rule("WebFetch");
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

#[test]
fn ask_rules_force_ask_decision_before_allow() {
    let context = ToolPermissionContext::new(PermissionMode::Default);
    context.add_always_allow_rule("WebFetch");
    context.add_always_ask_rule("WebFetch");
    let metadata = WebFetchTool.metadata();
    let call = ToolCall {
        name: "WebFetch".into(),
        input: "https://example.com".into(),
    };

    assert!(matches!(
        evaluate_tool_permission(&metadata, &call, &context),
        rust_agent::tool::definition::PermissionDecision::Ask { .. }
    ));
}

#[test]
fn deferred_tools_are_hidden_until_explicitly_included() {
    let context = ToolPermissionContext::new(PermissionMode::Default);
    assert!(!is_tool_allowed(&WebFetchTool.metadata(), &context));

    let with_deferred = ToolPermissionContext::new(PermissionMode::Default).with_deferred_tools(true);
    assert!(is_tool_allowed(&WebFetchTool.metadata(), &with_deferred));
}

#[test]
fn interactive_tools_can_be_disabled_for_non_interactive_runtime() {
    let context = ToolPermissionContext::new(PermissionMode::Default).with_interactive_tools(false);
    assert!(!is_tool_allowed(&AskUserQuestionTool.metadata(), &context));
}

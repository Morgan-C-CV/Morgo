use rust_agent::hook::executor::run_hook;
use rust_agent::hook::output::HookPermissionResult;
use rust_agent::hook::permission_resolution::{
    resolve_hook_permission_decision, updated_input_from_hook,
};
use rust_agent::hook::registry::{
    HookEvent, HookEventMatcher, HookRegistry, HookRule, HookRuleLayer,
};
use rust_agent::tool::definition::{PermissionDecision, PermissionDecisionReason};

#[test]
fn hook_allow_cannot_override_base_deny() {
    let resolved = resolve_hook_permission_decision(
        &HookPermissionResult::Allow {
            updated_input: Some("patched".into()),
            reason: Some("hook says allow".into()),
        },
        PermissionDecision::Deny {
            message: "base deny".into(),
            reason: PermissionDecisionReason::Rule,
        },
    );

    assert!(matches!(resolved, PermissionDecision::Deny { .. }));
}

#[test]
fn hook_ask_upgrades_allow_to_ask() {
    let resolved = resolve_hook_permission_decision(
        &HookPermissionResult::Ask {
            updated_input: None,
            reason: Some("confirm first".into()),
        },
        PermissionDecision::Allow,
    );

    assert!(matches!(resolved, PermissionDecision::Ask { .. }));
}

#[test]
fn hook_updated_input_is_extracted_from_permission_result() {
    let updated = updated_input_from_hook(&HookPermissionResult::Deny {
        updated_input: Some("rewritten-input".into()),
        reason: Some("blocked".into()),
    });

    assert_eq!(updated.as_deref(), Some("rewritten-input"));
}

#[test]
fn additional_context_does_not_enter_hook_result_messages() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::UserPromptSubmit,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: Some("runtime-only hint".into()),
    });

    let result = run_hook(&registry, HookEvent::UserPromptSubmit);

    assert!(
        result.messages.is_empty(),
        "additional_context must not produce any messages — it is runtime-only"
    );
    assert_eq!(
        result.payload.additional_context.as_slice(),
        &["runtime-only hint"],
        "additional_context must still be visible in payload"
    );
}

#[test]
fn append_message_still_enters_messages_when_additional_context_is_absent() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::Stop,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: Some("hook says stop".into()),
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });

    let result = run_hook(&registry, HookEvent::Stop);

    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].content, "hook says stop");
    assert!(result.payload.additional_context.is_empty());
}

#[test]
fn additional_context_and_append_message_are_independent() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::UserPromptSubmit,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: Some("visible to model".into()),
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: Some("runtime-only".into()),
    });

    let result = run_hook(&registry, HookEvent::UserPromptSubmit);

    assert_eq!(
        result.messages.len(),
        1,
        "only append_message enters messages"
    );
    assert_eq!(result.messages[0].content, "visible to model");
    assert_eq!(
        result.payload.additional_context.as_slice(),
        &["runtime-only"],
        "additional_context stays in payload only"
    );
}

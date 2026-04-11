use rust_agent::core::message::Message;
use rust_agent::hook::executor::{HookDecision, run_hook};
use rust_agent::hook::registry::{HookEvent, HookEventMatcher, HookRegistry, HookRule};

#[test]
fn hook_registry_records_lifecycle_events() {
    let registry = HookRegistry::default();
    assert_eq!(
        run_hook(&registry, HookEvent::SessionStart).decision,
        HookDecision::Allow
    );
    assert_eq!(
        run_hook(&registry, HookEvent::Setup).decision,
        HookDecision::Allow
    );
    assert_eq!(
        run_hook(&registry, HookEvent::Stop).decision,
        HookDecision::Allow
    );

    let events = registry.recorded_events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], HookEvent::SessionStart);
    assert_eq!(events[1], HookEvent::Setup);
    assert_eq!(events[2], HookEvent::Stop);
}

#[test]
fn pre_tool_hook_can_deny_specific_tool() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PreToolUse,
        deny_match: Some("Agent".into()),
        append_message: None,
        prevent_continuation: false,
    });

    let result = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Agent".into(),
        },
    );

    assert_eq!(
        result.decision,
        HookDecision::Deny("tool Agent denied by hook policy".into())
    );
}

#[test]
fn unrelated_tool_is_allowed() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PreToolUse,
        deny_match: Some("Agent".into()),
        append_message: None,
        prevent_continuation: false,
    });

    let decision = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Read".into(),
        },
    );

    assert_eq!(decision.decision, HookDecision::Allow);
}

#[test]
fn hook_rule_can_append_message_and_prevent_continuation() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::Stop,
        deny_match: None,
        append_message: Some("stop hook says wait".into()),
        prevent_continuation: true,
    });

    let result = run_hook(&registry, HookEvent::Stop);

    assert_eq!(result.decision, HookDecision::Allow);
    assert!(result.prevent_continuation);
    assert_eq!(
        result.messages,
        vec![Message::assistant("stop hook says wait")]
    );
}

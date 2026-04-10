use rust_agent::hook::executor::{HookDecision, run_hook};
use rust_agent::hook::registry::{HookEvent, HookEventMatcher, HookRegistry, HookRule};

#[test]
fn hook_registry_records_lifecycle_events() {
    let registry = HookRegistry::default();
    assert_eq!(
        run_hook(&registry, HookEvent::SessionStart),
        HookDecision::Allow
    );
    assert_eq!(run_hook(&registry, HookEvent::Setup), HookDecision::Allow);
    assert_eq!(run_hook(&registry, HookEvent::Stop), HookDecision::Allow);

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
    });

    let decision = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Agent".into(),
        },
    );

    assert_eq!(
        decision,
        HookDecision::Deny("tool Agent denied by hook policy".into())
    );
}

#[test]
fn unrelated_tool_is_allowed() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PreToolUse,
        deny_match: Some("Agent".into()),
    });

    let decision = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Read".into(),
        },
    );

    assert_eq!(decision, HookDecision::Allow);
}

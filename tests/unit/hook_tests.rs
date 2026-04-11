use rust_agent::bootstrap::InteractionSurface;
use rust_agent::core::message::Message;
use rust_agent::hook::executor::{HookDecision, run_hook};
use rust_agent::hook::registry::{HookEvent, HookEventMatcher, HookRegistry, HookRule};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::{Notification, NotificationType};
use rust_agent::interaction::telegram::gateway::TelegramGateway;

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
        permission_decision: None,
        updated_input: None,
        additional_context: None,
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
        permission_decision: None,
        updated_input: None,
        additional_context: None,
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
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });

    let result = run_hook(&registry, HookEvent::Stop);

    assert_eq!(result.decision, HookDecision::Allow);
    assert!(result.prevent_continuation);
    assert_eq!(
        result.messages,
        vec![Message::assistant("stop hook says wait")]
    );
}

#[test]
fn notification_hook_can_match_typed_payload() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::Notification,
        deny_match: Some("task-9".into()),
        append_message: None,
        prevent_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
        .with_hook_registry(registry.clone());
    let notification = Notification {
        session_id: "session-1".into(),
        title: "Task completed".into(),
        body: "demo body".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-9".into()),
        status: Some("Completed".into()),
        output_file: Some("/tmp/task-9.log".into()),
        wake_up: true,
        target: None,
    };

    dispatcher.dispatch(InteractionSurface::Cli, notification);

    let events = registry.recorded_events();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        HookEvent::Notification {
            title: "Task completed".into(),
            body: "demo body".into(),
            notification_type: "task_update".into(),
            task_id: Some("task-9".into()),
            status: Some("Completed".into()),
            output_file: Some("/tmp/task-9.log".into()),
        }
    );
    assert!(dispatcher.delivered().is_empty());
}

#[test]
fn hook_rule_can_provide_typed_payload() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PreToolUse,
        deny_match: None,
        append_message: None,
        prevent_continuation: false,
        permission_decision: Some("deny".into()),
        updated_input: Some("patched-input".into()),
        additional_context: Some("extra context".into()),
    });

    let result = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Read".into(),
        },
    );

    assert!(matches!(
        result.payload.permission_result,
        rust_agent::hook::output::HookPermissionResult::Deny { .. }
    ));
    assert_eq!(result.payload.additional_context.as_deref(), Some("extra context"));
}

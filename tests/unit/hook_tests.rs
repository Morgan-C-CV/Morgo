use rust_agent::bootstrap::InteractionSurface;
use rust_agent::core::message::Message;
use rust_agent::hook::executor::{HookDecision, run_hook};
use rust_agent::hook::registry::{
    HookEvent, HookEventMatcher, HookRegistry, HookRule, HookRuleLayer,
};
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
        run_hook(
            &registry,
            HookEvent::PermissionRequest {
                tool_name: "Read".into(),
            },
        )
        .decision,
        HookDecision::Allow
    );
    assert_eq!(
        run_hook(&registry, HookEvent::Stop).decision,
        HookDecision::Allow
    );

    let events = registry.recorded_events();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0], HookEvent::SessionStart);
    assert_eq!(events[1], HookEvent::Setup);
    assert_eq!(
        events[2],
        HookEvent::PermissionRequest {
            tool_name: "Read".into(),
        }
    );
    assert_eq!(events[3], HookEvent::Stop);
}

#[test]
fn pre_tool_hook_can_deny_specific_tool() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PreToolUse,
        layer: HookRuleLayer::Defaults,
        deny_match: Some("Agent".into()),
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
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
        layer: HookRuleLayer::Defaults,
        deny_match: Some("Agent".into()),
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
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
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: Some("stop hook says wait".into()),
        prevent_continuation: true,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });

    let result = run_hook(&registry, HookEvent::Stop);

    assert_eq!(result.decision, HookDecision::Allow);
    assert!(result.prevent_continuation);
    assert!(!result.block_continuation);
    assert_eq!(
        result.messages,
        vec![Message::assistant("stop hook says wait")]
    );
}

#[test]
fn hook_rule_can_block_continuation_without_preventing() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::Stop,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: Some("stop hook needs revision".into()),
        prevent_continuation: false,
        block_continuation: true,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });

    let result = run_hook(&registry, HookEvent::Stop);

    assert_eq!(result.decision, HookDecision::Allow);
    assert!(!result.prevent_continuation);
    assert!(result.block_continuation);
    assert_eq!(
        result.messages,
        vec![Message::assistant("stop hook needs revision")]
    );
}

#[test]
fn notification_hook_can_match_typed_payload() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::Notification,
        layer: HookRuleLayer::Defaults,
        deny_match: Some("task-9".into()),
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
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
        task_type: Some("local_agent".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-9".into()),
        worker_role: Some("research".into()),
        orchestration_group_id: None,
        phase: Some("research".into()),
        validation_state: Some("not_needed".into()),
        output_file: Some("/tmp/task-9.log".into()),
        usage: None,
        tool_name: None,
        approval_code: None,
        approval_summary: None,
        approval_detail: None,
        approval_kind: None,
        approval_escalation_reasons: Vec::new(),
        notice_kind: None,
        notice_code: None,
        runtime_kind: None,
        service_failure_code: None,
        provider_kind: None,
        status_code: None,
        retryable: None,
        surface_visible: None,
        dedupe_key: None,
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
            task_type: Some("local_agent".into()),
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
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
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
    assert_eq!(
        result.payload.additional_context.as_slice(),
        &["extra context"]
    );
}

#[test]
fn higher_layer_permission_directive_overrides_lower_layer() {
    let registry = HookRegistry::default()
        .register_rule(HookRule {
            event: HookEventMatcher::PreToolUse,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: Some("ask".into()),
            updated_input: Some("default-input".into()),
            additional_context: None,
        })
        .register_rule(HookRule {
            event: HookEventMatcher::PreToolUse,
            layer: HookRuleLayer::Runtime,
            deny_match: None,
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: Some("deny".into()),
            updated_input: Some("runtime-input".into()),
            additional_context: None,
        });

    let result = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Read".into(),
        },
    );

    assert!(matches!(
        result.payload.permission_result,
        rust_agent::hook::output::HookPermissionResult::Deny {
            updated_input: Some(ref input),
            ..
        } if input == "runtime-input"
    ));
}

#[test]
fn multiple_additional_context_rules_accumulate_not_overwrite() {
    let registry = HookRegistry::default()
        .register_rule(HookRule {
            event: HookEventMatcher::UserPromptSubmit,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: Some("context-from-defaults".into()),
        })
        .register_rule(HookRule {
            event: HookEventMatcher::UserPromptSubmit,
            layer: HookRuleLayer::File,
            deny_match: None,
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: Some("context-from-file".into()),
        });

    let result = run_hook(&registry, HookEvent::UserPromptSubmit);

    assert_eq!(result.payload.additional_context.len(), 2);
    assert!(
        result
            .payload
            .additional_context
            .contains(&"context-from-defaults".to_string())
    );
    assert!(
        result
            .payload
            .additional_context
            .contains(&"context-from-file".to_string())
    );
}

#[test]
fn additional_context_deduplicates_after_normalize() {
    let registry = HookRegistry::default()
        .register_rule(HookRule {
            event: HookEventMatcher::UserPromptSubmit,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: Some("  duplicate context  ".into()),
        })
        .register_rule(HookRule {
            event: HookEventMatcher::UserPromptSubmit,
            layer: HookRuleLayer::File,
            deny_match: None,
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: Some("duplicate context".into()),
        });

    let result = run_hook(&registry, HookEvent::UserPromptSubmit);

    assert_eq!(
        result.payload.additional_context.len(),
        1,
        "trimmed duplicates must appear exactly once"
    );
    assert_eq!(result.payload.additional_context[0], "duplicate context");
}

#[test]
fn hook_deny_emits_audit_log_entry() {
    use rust_agent::hook::executor::run_hook_with_audit;
    use rust_agent::security::audit::{AuditEvent, AuditLog};

    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PreToolUse,
        layer: HookRuleLayer::Defaults,
        deny_match: Some("Bash".into()),
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });

    let audit_log = std::sync::Mutex::new(AuditLog::default());
    let result = run_hook_with_audit(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Bash".into(),
        },
        Some(&audit_log),
    );

    assert!(matches!(result.decision, HookDecision::Deny(_)));
    let log = audit_log.lock().unwrap();
    assert_eq!(log.events().len(), 1);
    assert!(matches!(log.events()[0], AuditEvent::HookDenied { .. }));
}

#[test]
fn hook_allow_with_updated_input_emits_updated_input_audit_entry() {
    use rust_agent::hook::executor::run_hook_with_audit;
    use rust_agent::security::audit::{AuditEvent, AuditLog};

    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::UserPromptSubmit,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: Some("rewritten input".into()),
        additional_context: None,
    });

    let audit_log = std::sync::Mutex::new(AuditLog::default());
    run_hook_with_audit(&registry, HookEvent::UserPromptSubmit, Some(&audit_log));

    let log = audit_log.lock().unwrap();
    assert_eq!(log.events().len(), 1);
    assert!(matches!(
        log.events()[0],
        AuditEvent::HookUpdatedInput { .. }
    ));
}

#[test]
fn hook_allow_without_mutation_emits_allowed_audit_entry() {
    use rust_agent::hook::executor::run_hook_with_audit;
    use rust_agent::security::audit::{AuditEvent, AuditLog};

    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::UserPromptSubmit,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: Some("hello".into()),
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });

    let audit_log = std::sync::Mutex::new(AuditLog::default());
    run_hook_with_audit(&registry, HookEvent::UserPromptSubmit, Some(&audit_log));

    let log = audit_log.lock().unwrap();
    assert_eq!(log.events().len(), 1);
    assert!(matches!(log.events()[0], AuditEvent::HookAllowed { .. }));
}

#[test]
fn ask_permission_result_resolves_to_ask_decision_not_passthrough() {
    use rust_agent::hook::output::HookPermissionResult;
    use rust_agent::hook::permission_resolution::resolve_hook_permission_decision;
    use rust_agent::tool::definition::{PermissionDecision, PermissionDecisionReason};

    let ask_result = HookPermissionResult::Ask {
        updated_input: None,
        reason: Some("hook requires approval".into()),
    };
    let base = PermissionDecision::Allow;
    let resolved = resolve_hook_permission_decision(&ask_result, base);

    assert!(
        matches!(
            resolved,
            PermissionDecision::Ask {
                reason: PermissionDecisionReason::Hook,
                ..
            }
        ),
        "Ask permission result must resolve to Ask decision, not Passthrough"
    );
}

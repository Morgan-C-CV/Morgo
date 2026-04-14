use rust_agent::bootstrap::InteractionSurface;
use rust_agent::hook::registry::{
    HookEvent, HookEventMatcher, HookRegistry, HookRule, HookRuleLayer,
};
use rust_agent::interaction::cli::renderer::{
    build_tui_screen, render_document_output, render_document_tui_output, render_turn_document,
    render_turn_output,
};
use rust_agent::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::{Notification, NotificationTarget, NotificationType};
use rust_agent::interaction::remote::{
    REMOTE_CHANNEL_MATRIX, RemoteChannelEventKind, RemoteChannelRule, RemoteDeliveryMode,
    RemoteEventEnvelope, RemoteEventPayload, drain_remote_notifications,
    remote_channel_kind_for_notification, remote_channel_kind_for_surface_item,
    remote_delivery_mode_for_kind, remote_delivery_mode_for_notification,
    remote_delivery_mode_for_surface_item,
};
use rust_agent::interaction::telegram::binding::{
    SessionBinding, TelegramDeliveryTarget, TelegramOutgoingMessage,
};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::interaction::view::{
    SurfaceItem, WebItem, build_surface_view, build_telegram_view, build_web_view,
    surface_item_from_cli_event,
};
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus, TaskUsageSummary};

#[test]
fn dispatcher_records_cli_notifications() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let notification = Notification {
        session_id: "session-1".into(),
        title: "Task completed".into(),
        body: "demo body".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-1".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-1".into()),
        worker_role: Some("research".into()),
        orchestration_group_id: None,
        phase: Some("research".into()),
        validation_state: Some("not_needed".into()),
        output_file: Some("/tmp/task-1.log".into()),
        usage: None,
        tool_name: None,
        notice_kind: None,
        dedupe_key: None,
        wake_up: true,
        target: None,
    };

    dispatcher.dispatch(InteractionSurface::Cli, notification.clone());

    assert_eq!(dispatcher.delivered(), vec![notification]);
}

#[test]
fn dispatcher_records_notification_hook_payloads_for_all_notification_types() {
    let registry = HookRegistry::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
        .with_hook_registry(registry.clone());

    dispatcher.dispatch(
        InteractionSurface::Cli,
        Notification::task_update(
            "session-1",
            "Task completed",
            "task body",
            "task-7",
            "Completed",
            "inspect task output for task-7",
            None,
            None,
            None,
            None,
            "/tmp/task-7.log",
            None,
        ),
    );
    dispatcher.dispatch(
        InteractionSurface::Cli,
        Notification::approval_required("session-1", "Bash", "requires explicit approval"),
    );
    dispatcher.dispatch(
        InteractionSurface::Cli,
        Notification::runtime_notice("session-1", "tool", "runtime warning"),
    );

    let events = registry.recorded_events();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events[0],
        HookEvent::Notification {
            title: "Task completed".into(),
            body: "task body".into(),
            notification_type: "task_update".into(),
            task_id: Some("task-7".into()),
            status: Some("Completed".into()),
            output_file: Some("/tmp/task-7.log".into()),
        }
    );
    assert_eq!(
        events[1],
        HookEvent::Notification {
            title: "Approval required: Bash".into(),
            body: "requires explicit approval".into(),
            notification_type: "approval_required".into(),
            task_id: None,
            status: None,
            output_file: None,
        }
    );
    assert_eq!(
        events[2],
        HookEvent::Notification {
            title: "Runtime notice: tool".into(),
            body: "runtime warning".into(),
            notification_type: "runtime_notice".into(),
            task_id: None,
            status: None,
            output_file: None,
        }
    );
}

#[test]
fn dispatcher_can_deny_approval_notification_via_hook_rule() {
    let registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::Notification,
        layer: HookRuleLayer::Defaults,
        deny_match: Some("approval_required".into()),
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
        .with_hook_registry(registry.clone());

    dispatcher.dispatch(
        InteractionSurface::Cli,
        Notification::approval_required("session-1", "Bash", "requires explicit approval"),
    );

    let events = registry.recorded_events();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        HookEvent::Notification {
            title: "Approval required: Bash".into(),
            body: "requires explicit approval".into(),
            notification_type: "approval_required".into(),
            task_id: None,
            status: None,
            output_file: None,
        }
    );
    assert!(dispatcher.delivered().is_empty());
}

#[test]
fn cli_renderer_marks_task_event_lines() {
    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: "assistant reply".into(),
        events: vec![CliDisplayEvent::TaskEvent(TaskEvent {
            owner: TaskOwner {
                session_id: "session-1".into(),
                surface: InteractionSurface::Cli,
            },
            target_task_id: Some("task-1".into()),
            task_id: "task-1".into(),
            status: TaskStatus::Completed,
            summary: "demo task".into(),
            result: "Task completed".into(),
            next_action: "inspect task output for task-1".into(),
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file: "/tmp/task-1.log".into(),
            usage: None,
        })],
    });

    assert!(rendered.contains("assistant reply"));
    assert!(rendered.contains("== Task update =="));
    assert!(rendered.contains("[task] id: task-1"));
    assert!(rendered.contains("[task] summary: demo task"));
    assert!(rendered.contains("[task] status: Completed"));
    assert!(rendered.contains("[task] result: Task completed"));
    assert!(rendered.contains("[task] worker_role: none"));
    assert!(rendered.contains("[task] output: /tmp/task-1.log"));
    assert!(rendered.contains("[task] next_action: inspect task output for task-1"));
}

#[test]
fn cli_renderer_surfaces_implement_verify_and_risk_contract_lines() {
    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: "final synthesis pending verification".into(),
        events: vec![
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "session-1".into(),
                    surface: InteractionSurface::Cli,
                },
                target_task_id: Some("task-2".into()),
                task_id: "task-2".into(),
                status: TaskStatus::Completed,
                summary: "implement worker finished patch".into(),
                result: "Task completed".into(),
                next_action: "dispatch verify worker for task-2".into(),
                worker_role: Some(rust_agent::state::app_state::WorkerRole::Implement),
                orchestration_group_id: None,
                phase: Some(rust_agent::task::types::WorkerPhase::Implement),
                validation_state: Some(rust_agent::task::types::ValidationState::PendingVerification),
                output_file: "/tmp/task-2.log".into(),
                usage: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "Validation pending; final answer must call out unverified risk until verify completes.".into(),
            }),
        ],
    });

    assert!(rendered.contains("== Task update =="));
    assert!(rendered.contains("[panel:task]"));
    assert!(rendered.contains("[task] worker_role: implement"));
    assert!(rendered.contains("[task] next_action: dispatch verify worker for task-2"));
    assert!(rendered.contains("== Notice: validation =="));
    assert!(rendered.contains("[panel:notice]"));
    assert!(rendered.contains(
        "Validation pending; final answer must call out unverified risk until verify completes."
    ));
}

#[test]
fn cli_renderer_renders_approval_and_tool_result_panels() {
    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: "assistant reply".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "requires explicit approval".into(),
                summary: Some("Bash pending approval".into()),
                detail: Some("requires explicit approval".into()),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one\nline two".into(),
                summary: Some("Read succeeded".into()),
                detail: Some("line one\nline two".into()),
            }),
        ],
    });

    assert!(rendered.contains("assistant reply"));
    assert!(rendered.contains("== Approval required =="));
    assert!(rendered.contains("[panel:approval]"));
    assert!(rendered.contains("Tool: Bash"));
    assert!(rendered.contains("requires explicit approval"));
    assert!(rendered.contains("== Tool result =="));
    assert!(rendered.contains("[panel:tool]"));
    assert!(rendered.contains("Tool: Read"));
    assert!(rendered.contains("line one"));
    assert!(rendered.contains("line two"));
}

#[test]
fn surface_and_remote_views_preserve_structured_tool_fields() {
    let turn = CliTurnOutput {
        primary_text: "Status".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "requires explicit approval".into(),
                summary: Some("Bash pending approval".into()),
                detail: Some("requires explicit approval".into()),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one".into(),
                summary: Some("Read succeeded".into()),
                detail: Some("line one".into()),
            }),
        ],
    };

    let view = build_surface_view(&turn);
    assert!(matches!(
        &view.items[0],
        SurfaceItem::ApprovalRequired {
            tool_name,
            message,
            summary,
            detail,
        } if tool_name == "Bash"
            && message == "requires explicit approval"
            && summary.as_deref() == Some("Bash pending approval")
            && detail.as_deref() == Some("requires explicit approval")
    ));
    assert!(matches!(
        &view.items[1],
        SurfaceItem::ToolResult {
            tool_name,
            content,
            summary,
            detail,
        } if tool_name == "Read"
            && content == "line one"
            && summary.as_deref() == Some("Read succeeded")
            && detail.as_deref() == Some("line one")
    ));

    let remote_events = view
        .items
        .into_iter()
        .map(RemoteEventEnvelope::from)
        .collect::<Vec<_>>();
    assert!(matches!(
        &remote_events[0].payload,
        RemoteEventPayload::ApprovalRequired {
            tool_name,
            message,
            summary,
            detail,
        } if tool_name == "Bash"
            && message == "requires explicit approval"
            && summary.as_deref() == Some("Bash pending approval")
            && detail.as_deref() == Some("requires explicit approval")
    ));
    assert!(matches!(
        &remote_events[1].payload,
        RemoteEventPayload::ToolResult {
            tool_name,
            content,
            summary,
            detail,
        } if tool_name == "Read"
            && content == "line one"
            && summary.as_deref() == Some("Read succeeded")
            && detail.as_deref() == Some("line one")
    ));
}

#[test]
fn cli_renderer_keeps_primary_text_before_mixed_panels_in_order() {
    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: "Status\n\nPlugins:\n- discovered_plugins: 1".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "verify before shipping".into(),
            }),
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "session-1".into(),
                    surface: InteractionSurface::Cli,
                },
                target_task_id: Some("task-3".into()),
                task_id: "task-3".into(),
                status: TaskStatus::Running,
                summary: "verify plugin snapshot".into(),
                result: "Task running".into(),
                next_action: "wait for verify worker".into(),
                worker_role: Some(rust_agent::state::app_state::WorkerRole::Verify),
                orchestration_group_id: Some("group-1".into()),
                phase: Some(rust_agent::task::types::WorkerPhase::Verify),
                validation_state: Some(
                    rust_agent::task::types::ValidationState::PendingVerification,
                ),
                output_file: "/tmp/task-3.log".into(),
                usage: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "plugin manifest updated".into(),
                summary: None,
                detail: None,
            }),
        ],
    });

    let primary_idx = rendered.find("Status").expect("primary text present");
    let notice_idx = rendered
        .find("== Notice: validation ==")
        .expect("notice panel present");
    let task_idx = rendered
        .find("== Task update ==")
        .expect("task panel present");
    let tool_idx = rendered
        .find("== Tool result ==")
        .expect("tool panel present");

    assert!(primary_idx < notice_idx);
    assert!(notice_idx < task_idx);
    assert!(task_idx < tool_idx);
    assert!(rendered.contains("[panel:notice]"));
    assert!(rendered.contains("[panel:task]"));
    assert!(rendered.contains("[panel:tool]"));
}

#[test]
fn cli_renderer_supports_help_style_primary_text_with_mixed_panels() {
    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: "Available commands:\nBuilt-in (1):\n- /help — Show the available commands"
            .into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "approval needed for follow-up".into(),
                summary: None,
                detail: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "background work still running".into(),
            }),
        ],
    });

    let help_idx = rendered
        .find("Available commands:")
        .expect("help text present");
    let approval_idx = rendered
        .find("== Approval required ==")
        .expect("approval panel present");
    let notice_idx = rendered
        .find("== Notice: runtime ==")
        .expect("notice panel present");

    assert!(help_idx < approval_idx);
    assert!(approval_idx < notice_idx);
}

#[test]
fn cli_renderer_shared_document_path_preserves_text_output() {
    let turn = CliTurnOutput {
        primary_text: "Status\n\nPlugins:\n- discovered_plugins: 1".into(),
        events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
            kind: "validation".into(),
            message: "verify before shipping".into(),
        })],
    };

    let rendered = render_turn_output(&turn);
    let document = render_turn_document(&turn);
    let rendered_via_document = render_document_output(&document);

    assert_eq!(rendered, rendered_via_document);
    assert!(rendered_via_document.contains("Status"));
    assert!(rendered_via_document.contains("== Notice: validation =="));
}

#[test]
fn cli_renderer_tui_output_keeps_main_panels_and_footer_in_order() {
    let turn = CliTurnOutput {
        primary_text: "Available commands:\nBuilt-in (1):\n- /help — Show the available commands"
            .into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "approval needed for follow-up".into(),
                summary: None,
                detail: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one\nline two".into(),
                summary: None,
                detail: None,
            }),
        ],
    };

    let rendered = render_document_tui_output(&render_turn_document(&turn));

    let main_idx = rendered.find("[Main]").expect("main section present");
    let approval_idx = rendered
        .find("[Approval required]")
        .expect("approval section present");
    let tool_idx = rendered
        .find("[Tool result]")
        .expect("tool section present");
    let prompt_idx = rendered.find("[Prompt]").expect("prompt section present");
    let footer_idx = rendered.find("[Footer]").expect("footer section present");

    assert!(main_idx < approval_idx);
    assert!(approval_idx < tool_idx);
    assert!(tool_idx < prompt_idx);
    assert!(prompt_idx < footer_idx);
    assert!(rendered.contains("╔════════════════ CLI TUI ════════════════"));
    assert!(rendered.contains("Available commands:"));
    assert!(rendered.contains("approval needed for follow-up"));
    assert!(rendered.contains("line one"));
    assert!(rendered.contains("line two"));
    assert!(rendered.contains("  > enter a request and press return"));
    assert!(rendered.contains("Controls: /exit, exit, or quit leaves the TUI."));
}

#[test]
fn cli_renderer_builds_tui_screen_with_fixed_layout_sections() {
    let turn = CliTurnOutput {
        primary_text: "Status\n\nPlugins:\n- discovered_plugins: 1".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "verify before shipping".into(),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "plugin manifest updated".into(),
                summary: None,
                detail: None,
            }),
        ],
    };

    let screen = build_tui_screen(&render_turn_document(&turn));

    assert_eq!(screen.main.first().map(String::as_str), Some("Status"));
    assert_eq!(screen.panels.len(), 2);
    assert_eq!(screen.panels[0].title, "Notice: validation");
    assert_eq!(screen.panels[1].title, "Tool result");
    assert_eq!(screen.prompt.first().map(String::as_str), Some("Prompt"));
    assert!(
        screen
            .footer
            .iter()
            .any(|line| line.contains("Controls: /exit, exit, or quit leaves the TUI."))
    );
}

#[test]
fn cli_renderer_tui_screen_uses_welcome_empty_state_when_document_is_empty() {
    let screen = build_tui_screen(&render_turn_document(&CliTurnOutput {
        primary_text: String::new(),
        events: vec![],
    }));

    assert_eq!(
        screen.main.first().map(String::as_str),
        Some("Welcome to RustAgent TUI.")
    );
    assert!(screen.main.iter().any(|line| line.contains("Try /help")));
    assert_eq!(screen.prompt.first().map(String::as_str), Some("Prompt"));
}

#[test]
fn remote_channel_matrix_matches_final_contract() {
    assert_eq!(
        REMOTE_CHANNEL_MATRIX,
        &[
            RemoteChannelRule {
                kind: RemoteChannelEventKind::TaskUpdate,
                mode: RemoteDeliveryMode::DualChannel,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::ApprovalRequired,
                mode: RemoteDeliveryMode::DualChannel,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::RuntimeNotice,
                mode: RemoteDeliveryMode::DualChannel,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::ToolCallStarted,
                mode: RemoteDeliveryMode::ResponseOnly,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::ToolResult,
                mode: RemoteDeliveryMode::ResponseOnly,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::AssistantDelta,
                mode: RemoteDeliveryMode::ResponseOnly,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::Transition,
                mode: RemoteDeliveryMode::ResponseOnly,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::Terminal,
                mode: RemoteDeliveryMode::ResponseOnly,
            },
            RemoteChannelRule {
                kind: RemoteChannelEventKind::SessionMilestone,
                mode: RemoteDeliveryMode::ResponseOnly,
            },
        ]
    );
}

#[test]
fn remote_delivery_mode_classifies_notification_types() {
    assert_eq!(
        remote_channel_kind_for_notification(&NotificationType::TaskUpdate),
        RemoteChannelEventKind::TaskUpdate
    );
    assert_eq!(
        remote_delivery_mode_for_notification(&NotificationType::TaskUpdate),
        RemoteDeliveryMode::DualChannel
    );
    assert_eq!(
        remote_channel_kind_for_notification(&NotificationType::ApprovalRequired),
        RemoteChannelEventKind::ApprovalRequired
    );
    assert_eq!(
        remote_delivery_mode_for_notification(&NotificationType::ApprovalRequired),
        RemoteDeliveryMode::DualChannel
    );
    assert_eq!(
        remote_channel_kind_for_notification(&NotificationType::RuntimeNotice),
        RemoteChannelEventKind::RuntimeNotice
    );
    assert_eq!(
        remote_delivery_mode_for_notification(&NotificationType::RuntimeNotice),
        RemoteDeliveryMode::DualChannel
    );
}

#[test]
fn remote_delivery_mode_lookup_uses_matrix() {
    assert_eq!(
        remote_delivery_mode_for_kind(RemoteChannelEventKind::TaskUpdate),
        RemoteDeliveryMode::DualChannel
    );
    assert_eq!(
        remote_delivery_mode_for_kind(RemoteChannelEventKind::ApprovalRequired),
        RemoteDeliveryMode::DualChannel
    );
    assert_eq!(
        remote_delivery_mode_for_kind(RemoteChannelEventKind::RuntimeNotice),
        RemoteDeliveryMode::DualChannel
    );
    assert_eq!(
        remote_delivery_mode_for_kind(RemoteChannelEventKind::ToolResult),
        RemoteDeliveryMode::ResponseOnly
    );
}

#[test]
fn remote_delivery_mode_classifies_dual_channel_and_response_only_surface_items() {
    let task_item = SurfaceItem::TaskUpdate(rust_agent::interaction::view::TaskView {
        task_id: "task-1".into(),
        status: "completed",
        summary: "demo task".into(),
        result: "Task completed".into(),
        next_action: "inspect task output for task-1".into(),
        worker_role: None,
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        output_file: "/tmp/task-1.log".into(),
        usage: Some(TaskUsageSummary {
            requests: 1,
            input_tokens: 10,
            output_tokens: 5,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            estimated_cost_micros_usd: 42,
        }),
    });
    assert_eq!(
        remote_channel_kind_for_surface_item(&task_item),
        RemoteChannelEventKind::TaskUpdate
    );
    assert_eq!(
        remote_delivery_mode_for_surface_item(&task_item),
        RemoteDeliveryMode::DualChannel
    );

    let approval_item = SurfaceItem::ApprovalRequired {
        tool_name: "Bash".into(),
        message: "requires explicit approval".into(),
        summary: None,
        detail: None,
    };
    assert_eq!(
        remote_channel_kind_for_surface_item(&approval_item),
        RemoteChannelEventKind::ApprovalRequired
    );
    assert_eq!(
        remote_delivery_mode_for_surface_item(&approval_item),
        RemoteDeliveryMode::DualChannel
    );

    let notice_item = SurfaceItem::RuntimeNotice {
        kind: "validation".into(),
        message: "pending verify".into(),
    };
    assert_eq!(
        remote_channel_kind_for_surface_item(&notice_item),
        RemoteChannelEventKind::RuntimeNotice
    );
    assert_eq!(
        remote_delivery_mode_for_surface_item(&notice_item),
        RemoteDeliveryMode::DualChannel
    );

    let delta_item = SurfaceItem::AssistantDelta {
        text: "partial reply".into(),
    };
    assert_eq!(
        remote_channel_kind_for_surface_item(&delta_item),
        RemoteChannelEventKind::AssistantDelta
    );
    assert_eq!(
        remote_delivery_mode_for_surface_item(&delta_item),
        RemoteDeliveryMode::ResponseOnly
    );
}

#[test]
fn surface_view_classifies_cli_events_for_cli_and_remote_reuse() {
    let turn = CliTurnOutput {
        primary_text: "Status".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "pending verify".into(),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one".into(),
                summary: None,
                detail: None,
            }),
        ],
    };

    let view = build_surface_view(&turn);

    assert_eq!(view.primary_text, "Status");
    assert_eq!(view.items.len(), 2);
    assert!(matches!(
        &view.items[0],
        rust_agent::interaction::view::SurfaceItem::RuntimeNotice { kind, message }
            if kind == "validation" && message == "pending verify"
    ));
    assert!(matches!(
        &view.items[1],
        rust_agent::interaction::view::SurfaceItem::ToolResult { tool_name, content, .. }
            if tool_name == "Read" && content == "line one"
    ));
}

#[test]
fn remote_event_envelope_preserves_structured_task_payload() {
    let event = CliDisplayEvent::TaskEvent(TaskEvent {
        owner: TaskOwner {
            session_id: "session-1".into(),
            surface: InteractionSurface::Remote,
        },
        target_task_id: Some("task-1".into()),
        task_id: "task-1".into(),
        status: TaskStatus::Completed,
        summary: "demo task".into(),
        result: "Task completed".into(),
        next_action: "inspect task output for task-1".into(),
        worker_role: Some(rust_agent::state::app_state::WorkerRole::Verify),
        orchestration_group_id: Some("group-1".into()),
        phase: Some(rust_agent::task::types::WorkerPhase::Verify),
        validation_state: Some(rust_agent::task::types::ValidationState::Verified),
        output_file: "/tmp/task-1.log".into(),
        usage: Some(TaskUsageSummary {
            requests: 2,
            input_tokens: 20,
            output_tokens: 8,
            cache_creation_input_tokens: 3,
            cache_read_input_tokens: 4,
            estimated_cost_micros_usd: 88,
        }),
    });
    let envelope = RemoteEventEnvelope::from(surface_item_from_cli_event(&event));

    assert_eq!(envelope.event_type, "task_update");
    assert!(matches!(
        envelope.payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-1"
                && task.status == "completed"
                && task.worker_role == Some("verify")
                && task.phase == Some("verify")
                && task.validation_state == Some("verified")
                && task.usage == Some(TaskUsageSummary {
                    requests: 2,
                    input_tokens: 20,
                    output_tokens: 8,
                    cache_creation_input_tokens: 3,
                    cache_read_input_tokens: 4,
                    estimated_cost_micros_usd: 88,
                })
    ));
}

#[test]
fn telegram_view_keeps_only_telegram_relevant_semantic_items() {
    let turn = CliTurnOutput {
        primary_text: "Status".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "pending verify".into(),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one".into(),
                summary: None,
                detail: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "requires explicit approval".into(),
                summary: None,
                detail: None,
            }),
        ],
    };

    let telegram_view = build_telegram_view(&build_surface_view(&turn));

    assert_eq!(telegram_view.primary_text, "Status");
    assert_eq!(telegram_view.items.len(), 2);
    assert!(matches!(
        &telegram_view.items[0],
        rust_agent::interaction::view::TelegramItem::RuntimeNotice { kind, message }
            if kind == "validation" && message == "pending verify"
    ));
    assert!(matches!(
        &telegram_view.items[1],
        rust_agent::interaction::view::TelegramItem::ApprovalRequired { tool_name, message }
            if tool_name == "Bash" && message == "requires explicit approval"
    ));
}

#[test]
fn telegram_gateway_builds_semantic_outgoing_messages_without_cli_renderer_types() {
    let gateway = TelegramGateway {
        allowed_bindings: vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: Some("thread-9".into()),
            }),
        }],
    };
    let view = build_surface_view(&CliTurnOutput {
        primary_text: "Primary reply".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "background work still running".into(),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "secret lines".into(),
                summary: None,
                detail: None,
            }),
        ],
    });

    let messages = gateway.build_outgoing_messages("telegram-session-1", &view);

    assert_eq!(
        messages,
        vec![
            TelegramOutgoingMessage {
                target: TelegramDeliveryTarget {
                    chat_id: "chat-1".into(),
                    thread_id: Some("thread-9".into()),
                },
                text: "Primary reply".into(),
            },
            TelegramOutgoingMessage {
                target: TelegramDeliveryTarget {
                    chat_id: "chat-1".into(),
                    thread_id: Some("thread-9".into()),
                },
                text: "Notice: runtime\nbackground work still running".into(),
            }
        ]
    );
}

#[test]
fn web_view_is_derived_from_surface_view_with_frontend_friendly_kinds() {
    let turn = CliTurnOutput {
        primary_text: "Primary reply".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "background work still running".into(),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Transition {
                text: "next_turn".into(),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one".into(),
                summary: None,
                detail: None,
            }),
        ],
    };

    let web_view = build_web_view(&build_surface_view(&turn));

    assert_eq!(web_view.primary_text, "Primary reply");
    assert_eq!(web_view.items.len(), 3);
    assert!(matches!(
        &web_view.items[0],
        WebItem::RuntimeNotice { notice_kind, message }
            if notice_kind == "runtime" && message == "background work still running"
    ));
    assert!(matches!(
        &web_view.items[1],
        WebItem::Transition { transition_kind, text }
            if transition_kind == "next_turn" && text == "next_turn"
    ));
    assert!(matches!(
        &web_view.items[2],
        WebItem::ToolResult { tool_name, content, .. }
            if tool_name == "Read" && content == "line one"
    ));
}

#[test]
fn same_surface_view_feeds_remote_telegram_and_web_without_cli_renderer_types() {
    let view = build_surface_view(&CliTurnOutput {
        primary_text: "Shared reply".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "requires explicit approval".into(),
                summary: None,
                detail: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "background work still running".into(),
            }),
        ],
    });

    let remote_events = view
        .items
        .clone()
        .into_iter()
        .map(RemoteEventEnvelope::from)
        .collect::<Vec<_>>();
    let telegram_view = build_telegram_view(&view);
    let web_view = build_web_view(&view);

    assert_eq!(remote_events.len(), 2);
    assert_eq!(telegram_view.items.len(), 2);
    assert_eq!(web_view.items.len(), 2);
    assert!(matches!(
        remote_events[0].payload,
        RemoteEventPayload::ApprovalRequired { .. }
    ));
    assert!(matches!(
        &telegram_view.items[0],
        rust_agent::interaction::view::TelegramItem::ApprovalRequired { .. }
    ));
    assert!(matches!(
        &web_view.items[0],
        WebItem::ApprovalRequired { .. }
    ));
}

#[test]
fn dispatcher_drains_remote_session_and_actor_notifications() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice("remote-session", "tool", "session scoped"),
    );
    let mut actor_notification =
        Notification::approval_required("remote-session", "Bash", "requires explicit approval");
    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    dispatcher.dispatch(InteractionSurface::Remote, actor_notification);

    let actor_drained = dispatcher.drain_remote_notifications("remote-session", Some("actor-1"));
    assert_eq!(actor_drained.len(), 2);
    assert!(
        actor_drained
            .iter()
            .any(|notification| notification.notification_type == NotificationType::RuntimeNotice)
    );
    assert!(
        actor_drained.iter().any(
            |notification| notification.notification_type == NotificationType::ApprovalRequired
        )
    );

    dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice("remote-session", "tool", "session only"),
    );
    let session_only = dispatcher.drain_remote_notifications("remote-session", None);
    assert_eq!(session_only.len(), 1);
    assert_eq!(
        session_only[0].notification_type,
        NotificationType::RuntimeNotice
    );

    assert!(
        dispatcher
            .drain_remote_notifications("remote-session", Some("actor-1"))
            .is_empty()
    );
}

#[test]
fn remote_drain_dedupes_session_and_actor_notifications() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let mut session_notification =
        Notification::runtime_notice("remote-session", "tool", "same message");
    session_notification.dedupe_key = Some("notice-1".into());
    dispatcher.dispatch(InteractionSurface::Remote, session_notification);

    let mut actor_notification =
        Notification::runtime_notice("remote-session", "tool", "same message");
    actor_notification.dedupe_key = Some("notice-1".into());
    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    dispatcher.dispatch(InteractionSurface::Remote, actor_notification);

    let drained = dispatcher.drain_remote_notifications("remote-session", Some("actor-1"));
    assert_eq!(drained.len(), 1);
}

#[test]
fn remote_task_update_notifications_use_dedupe_key() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let mut session_notification = Notification::task_update(
        "remote-session",
        "Task completed",
        "remote task (task-1)",
        "task-1",
        "completed",
        "inspect task output for task-1",
        None,
        None,
        None,
        None,
        "/tmp/task-1.log",
        None,
    );
    session_notification.dedupe_key = Some("task_update:remote-session:task-1:completed".into());
    dispatcher.dispatch(InteractionSurface::Remote, session_notification);

    let mut actor_notification = Notification::task_update(
        "remote-session",
        "Task completed",
        "remote task (task-1)",
        "task-1",
        "completed",
        "inspect task output for task-1",
        None,
        None,
        None,
        None,
        "/tmp/task-1.log",
        None,
    );
    actor_notification.dedupe_key = Some("task_update:remote-session:task-1:completed".into());
    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    dispatcher.dispatch(InteractionSurface::Remote, actor_notification);

    let drained = dispatcher.drain_remote_notifications("remote-session", Some("actor-1"));
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].notification_type, NotificationType::TaskUpdate);
}

#[test]
fn drain_remote_notifications_maps_structured_payloads() {
    let app_state = rust_agent::state::app_state::AppState {
        surface: InteractionSurface::Remote,
        session_mode: rust_agent::bootstrap::SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::RemoteControl,
        session_source: rust_agent::bootstrap::SessionSource::RemoteControl,
        runtime_role: rust_agent::state::app_state::RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: rust_agent::state::permission_context::ToolPermissionContext::new(
            rust_agent::state::permission_context::PermissionMode::Default,
        ),
        command_registry: None,
        runtime_tool_registry: None,
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: Vec::new(),
        active_session_id: "remote-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let mut notification =
        Notification::approval_required("remote-session", "Bash", "requires explicit approval");
    notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, notification);

    let drained = drain_remote_notifications(&app_state, "remote-session", Some("actor-1"));
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].event_type, "approval_required");
    assert!(matches!(
        &drained[0].payload,
        RemoteEventPayload::ApprovalRequired { tool_name, message, .. }
            if tool_name == "Bash" && message == "requires explicit approval"
    ));
}

#[test]
fn dispatcher_requires_delivery_ready_binding_for_telegram() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway {
        allowed_bindings: vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: None,
            }),
        }],
    });
    let notification = Notification {
        session_id: "telegram-session-1".into(),
        title: "Task completed".into(),
        body: "demo body".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-1".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-1".into()),
        worker_role: Some("verify".into()),
        orchestration_group_id: None,
        phase: Some("verify".into()),
        validation_state: Some("verified".into()),
        output_file: Some("/tmp/task-1.log".into()),
        usage: None,
        tool_name: None,
        notice_kind: None,
        dedupe_key: None,
        wake_up: true,
        target: Some(NotificationTarget::Session {
            session_id: "telegram-session-1".into(),
        }),
    };

    dispatcher.dispatch(InteractionSurface::Telegram, notification);

    assert_eq!(
        dispatcher.delivered(),
        vec![Notification {
            session_id: "telegram-session-1".into(),
            title: "Task completed".into(),
            body: "demo body".into(),
            notification_type: NotificationType::TaskUpdate,
            task_id: Some("task-1".into()),
            status: Some("Completed".into()),
            next_action: Some("inspect task output for task-1".into()),
            worker_role: Some("verify".into()),
            orchestration_group_id: None,
            phase: Some("verify".into()),
            validation_state: Some("verified".into()),
            output_file: Some("/tmp/task-1.log".into()),
            usage: None,
            tool_name: None,
            notice_kind: None,
            dedupe_key: None,
            wake_up: true,
            target: Some(NotificationTarget::Telegram(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: None,
            })),
        }]
    );
}

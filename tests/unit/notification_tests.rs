use rust_agent::bootstrap::InteractionSurface;
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
    remote_channel_kind_for_cli_event, remote_channel_kind_for_notification,
    remote_delivery_mode_for_cli_event, remote_delivery_mode_for_kind,
    remote_delivery_mode_for_notification,
};
use rust_agent::interaction::view::{build_surface_view, surface_item_from_cli_event};
use rust_agent::interaction::telegram::binding::{SessionBinding, TelegramDeliveryTarget};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus};

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
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one\nline two".into(),
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
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "plugin manifest updated".into(),
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
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Read".into(),
                content: "line one\nline two".into(),
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
fn remote_delivery_mode_classifies_dual_channel_and_response_only_events() {
    let task_event = CliDisplayEvent::TaskEvent(TaskEvent {
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
        worker_role: None,
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        output_file: "/tmp/task-1.log".into(),
    });
    assert_eq!(
        remote_channel_kind_for_cli_event(&task_event),
        RemoteChannelEventKind::TaskUpdate
    );
    assert_eq!(
        remote_delivery_mode_for_cli_event(&task_event),
        RemoteDeliveryMode::DualChannel
    );

    let approval_event = CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
        tool_name: "Bash".into(),
        message: "requires explicit approval".into(),
    });
    assert_eq!(
        remote_channel_kind_for_cli_event(&approval_event),
        RemoteChannelEventKind::ApprovalRequired
    );
    assert_eq!(
        remote_delivery_mode_for_cli_event(&approval_event),
        RemoteDeliveryMode::DualChannel
    );

    let notice_event = CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
        kind: "validation".into(),
        message: "pending verify".into(),
    });
    assert_eq!(
        remote_channel_kind_for_cli_event(&notice_event),
        RemoteChannelEventKind::RuntimeNotice
    );
    assert_eq!(
        remote_delivery_mode_for_cli_event(&notice_event),
        RemoteDeliveryMode::DualChannel
    );

    let delta_event = CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
        text: "partial reply".into(),
    });
    assert_eq!(
        remote_channel_kind_for_cli_event(&delta_event),
        RemoteChannelEventKind::AssistantDelta
    );
    assert_eq!(
        remote_delivery_mode_for_cli_event(&delta_event),
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
        rust_agent::interaction::view::SurfaceItem::ToolResult { tool_name, content }
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
        RemoteEventPayload::ApprovalRequired { tool_name, message }
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

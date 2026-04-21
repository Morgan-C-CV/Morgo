use std::sync::Arc;

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{
    InMemorySessionStore, SessionHistory, SessionRestoreRequest, SessionSnapshot, SessionStore,
};
use rust_agent::hook::registry::{
    HookEvent, HookEventMatcher, HookRegistry, HookRule, HookRuleLayer,
};
use rust_agent::interaction::cli::renderer::{
    build_tui_screen, render_document_output, render_document_tui_output, render_turn_document,
    render_turn_output,
};
use rust_agent::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::notification::{Notification, NotificationTarget, NotificationType};
use rust_agent::interaction::remote::{
    REMOTE_CHANNEL_MATRIX, RemoteChannelEventKind, RemoteChannelRule, RemoteDeliveryMode,
    RemoteEventEnvelope, RemoteEventPayload, drain_remote_notifications,
    remote_channel_kind_for_notification, remote_channel_kind_for_surface_item,
    remote_delivery_mode_for_kind, remote_delivery_mode_for_notification,
    remote_delivery_mode_for_surface_item,
};
use rust_agent::interaction::telegram::adapter::{
    TelegramInboundEnvelope, intake_transport_envelope,
};
use rust_agent::interaction::telegram::binding::{
    SessionBinding, TelegramBindingAuthorization, TelegramDeliveryTarget,
    TelegramInboundBindingAuthorization, TelegramOutgoingMessage,
};
use rust_agent::interaction::telegram::gateway::{
    TelegramGateway, TelegramInboundIntake, TelegramInboundRequest,
};
use rust_agent::interaction::telegram::runtime::{
    TelegramRuntimeResponse, handle_telegram_envelope,
};
use rust_agent::interaction::view::{
    SurfaceItem, WebItem, build_surface_view, build_telegram_view, build_web_view,
    surface_item_from_cli_event,
};
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::audit::AuditLog;
use rust_agent::security::authorizer::{
    AuthDecision, AuthDenyCategory, DefaultSurfaceAuthorizer, SurfaceAdmissionPolicy,
    SurfaceAuthorizer,
};
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus, TaskUsageSummary};
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

struct TelegramPromptCommand;

#[async_trait]
impl Command for TelegramPromptCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "telegram-prompt".into(),
            description: "Enter query engine from telegram tests".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Prompt,
            availability: CommandAvailability::RemoteSafe,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &rust_agent::interaction::envelope::NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Prompt("telegram runtime prompt".into()))
    }
}

fn telegram_test_app_state(
    command_registry: Arc<CommandRegistry>,
    gateway: TelegramGateway,
    session_store: Arc<InMemorySessionStore>,
    active_session_id: &str,
) -> AppState {
    AppState {
        surface: InteractionSurface::Telegram,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Bot,
        session_source: SessionSource::Telegram,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default()))
            .with_plan_manager(Arc::new(PlanManager::default())),
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(gateway),
        audit_log: Arc::new(std::sync::Mutex::new(AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: active_session_id.into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    }
}

#[test]
fn dispatcher_records_cli_notifications() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let notification = Notification {
        session_id: "session-1".into(),
        title: "Task completed".into(),
        body: "demo body".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-1".into()),
        task_type: Some("local_agent".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-1".into()),
        worker_role: Some("research".into()),
        orchestration_group_id: None,
        phase: Some("research".into()),
        validation_state: Some("not_needed".into()),
        output_file: Some("/tmp/task-1.log".into()),
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
            Some("local_agent"),
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
        Notification::approval_required(
            "session-1",
            "Bash",
            "requires explicit approval",
            Some("bash_warning".into()),
            Some("Bash pending approval".into()),
            Some("requires explicit approval".into()),
            Some("tool_permission".into()),
            vec!["privileged_system".into()],
        ),
    );
    dispatcher.dispatch(
        InteractionSurface::Cli,
        Notification::runtime_notice(
            "session-1",
            "tool",
            "runtime warning",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
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
            task_type: Some("local_agent".into()),
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
            task_type: None,
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
            task_type: None,
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
        Notification::approval_required(
            "session-1",
            "Bash",
            "requires explicit approval",
            Some("bash_warning".into()),
            Some("Bash pending approval".into()),
            Some("requires explicit approval".into()),
            Some("tool_permission".into()),
            vec!["privileged_system".into()],
        ),
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
            task_type: None,
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
            task_type: rust_agent::task::types::TaskType::LocalAgent,
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
                task_type: rust_agent::task::types::TaskType::LocalAgent,
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
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
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
                code: Some("bash_warning".into()),
                summary: Some("Bash pending approval".into()),
                detail: Some("requires explicit approval".into()),
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["privileged_system".into()],
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
                code: Some("bash_warning".into()),
                summary: Some("Bash pending approval".into()),
                detail: Some("requires explicit approval".into()),
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["privileged_system".into()],
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
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        } if tool_name == "Bash"
            && message == "requires explicit approval"
            && code.as_deref() == Some("bash_warning")
            && summary.as_deref() == Some("Bash pending approval")
            && detail.as_deref() == Some("requires explicit approval")
            && approval_kind.as_deref() == Some("tool_permission")
            && escalation_reasons.as_slice() == ["privileged_system"]
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
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        } if tool_name == "Bash"
            && message == "requires explicit approval"
            && code.as_deref() == Some("bash_warning")
            && summary.as_deref() == Some("Bash pending approval")
            && detail.as_deref() == Some("requires explicit approval")
            && approval_kind.as_deref() == Some("tool_permission")
            && escalation_reasons.as_slice() == ["privileged_system"]
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
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
            }),
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "session-1".into(),
                    surface: InteractionSurface::Cli,
                },
                target_task_id: Some("task-3".into()),
                task_id: "task-3".into(),
                task_type: rust_agent::task::types::TaskType::LocalAgent,
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
                code: Some("bash_warning".into()),
                summary: None,
                detail: None,
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["privileged_system".into()],
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "background work still running".into(),
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
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
            code: None,
            runtime_kind: None,
            service_failure_code: None,
            provider_kind: None,
            status_code: None,
            retryable: None,
            surface_visible: None,
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
                code: Some("bash_warning".into()),
                summary: None,
                detail: None,
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["privileged_system".into()],
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
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
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
        task_type: "local_agent",
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
        code: Some("bash_warning".into()),
        summary: None,
        detail: None,
        approval_kind: Some("tool_permission".into()),
        escalation_reasons: vec!["privileged_system".into()],
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
        code: Some("api_provider_http_5xx".into()),
        runtime_kind: Some("ModelError".into()),
        service_failure_code: Some("api_provider_http_5xx".into()),
        provider_kind: Some("anthropic".into()),
        status_code: Some(503),
        retryable: Some(true),
        surface_visible: Some(true),
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
                code: Some("api_provider_http_5xx".into()),
                runtime_kind: Some("ModelError".into()),
                service_failure_code: Some("api_provider_http_5xx".into()),
                provider_kind: Some("anthropic".into()),
                status_code: Some(503),
                retryable: Some(true),
                surface_visible: Some(true),
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
        rust_agent::interaction::view::SurfaceItem::RuntimeNotice {
            kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        }
            if kind == "validation"
                && message == "pending verify"
                && code.as_deref() == Some("api_provider_http_5xx")
                && runtime_kind.as_deref() == Some("ModelError")
                && service_failure_code.as_deref() == Some("api_provider_http_5xx")
                && provider_kind.as_deref() == Some("anthropic")
                && status_code == &Some(503)
                && retryable == &Some(true)
                && surface_visible == &Some(true)
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
        task_type: rust_agent::task::types::TaskType::LocalAgent,
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
                && task.task_type == "local_agent"
                && task.status == "completed"
                && task.summary == "demo task"
                && task.result == "Task completed"
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
    let view = build_surface_view(&CliTurnOutput {
        primary_text: String::new(),
        events: vec![event],
    });
    assert!(matches!(
        &view.items[0],
        SurfaceItem::TaskUpdate(task)
            if task.task_type == "local_agent"
                && task.summary == "demo task"
                && task.result == "Task completed"
    ));
}

#[test]
fn remote_notification_envelope_preserves_task_type_and_uses_generic_fallback() {
    let typed = rust_agent::interaction::remote::RemoteNotificationEnvelope::from(
        Notification::task_update(
            "remote-session",
            "Command completed",
            "bash: ls (task-2) — command completed",
            "task-2",
            Some("local_bash"),
            "completed",
            "inspect command output for task-2",
            None,
            None,
            None,
            None,
            "/tmp/task-2.log",
            None,
        ),
    );
    assert!(matches!(
        typed.payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-2"
                && task.task_type == "local_bash"
                && task.next_action == "inspect command output for task-2"
    ));

    let fallback =
        rust_agent::interaction::remote::RemoteNotificationEnvelope::from(Notification {
            session_id: "remote-session".into(),
            title: "Task completed".into(),
            body: "fallback body".into(),
            notification_type: NotificationType::TaskUpdate,
            task_id: Some("task-fallback".into()),
            task_type: None,
            status: Some("completed".into()),
            next_action: Some("inspect task notification".into()),
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file: Some("/tmp/task-fallback.log".into()),
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
        });
    assert!(matches!(
        fallback.payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-fallback" && task.task_type == "generic"
    ));
}

#[test]
fn telegram_view_keeps_only_telegram_relevant_semantic_items() {
    let turn = CliTurnOutput {
        primary_text: "Status".into(),
        events: vec![
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "session-1".into(),
                    surface: InteractionSurface::Telegram,
                },
                target_task_id: Some("task-tele-1".into()),
                task_id: "task-tele-1".into(),
                task_type: rust_agent::task::types::TaskType::LocalAgent,
                status: TaskStatus::Completed,
                summary: "telegram task".into(),
                result: "Task completed".into(),
                next_action: "inspect task output for task-tele-1".into(),
                worker_role: None,
                orchestration_group_id: None,
                phase: None,
                validation_state: None,
                output_file: "/tmp/task-tele-1.log".into(),
                usage: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "pending verify".into(),
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
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
                code: Some("bash_warning".into()),
                summary: None,
                detail: None,
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["privileged_system".into()],
            }),
        ],
    };

    let telegram_view = build_telegram_view(&build_surface_view(&turn));

    assert_eq!(telegram_view.primary_text, "Status");
    assert_eq!(telegram_view.items.len(), 3);
    assert!(matches!(
        &telegram_view.items[0],
        rust_agent::interaction::view::TelegramItem::TaskUpdate(task)
            if task.task_type == "local_agent" && task.task_id == "task-tele-1"
    ));
    assert!(matches!(
        &telegram_view.items[1],
        rust_agent::interaction::view::TelegramItem::RuntimeNotice { kind, message }
            if kind == "validation" && message == "pending verify"
    ));
    assert!(matches!(
        &telegram_view.items[2],
        rust_agent::interaction::view::TelegramItem::ApprovalRequired { tool_name, message }
            if tool_name == "Bash" && message == "requires explicit approval"
    ));
}

#[test]
fn telegram_view_trims_shared_runtime_lifecycle_notices_but_keeps_surface_relevant_ones() {
    let telegram_view = build_telegram_view(&build_surface_view(&CliTurnOutput {
        primary_text: "Status".into(),
        events: vec![
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "NormalTerminal: completed".into(),
                code: None,
                runtime_kind: Some("NormalTerminal".into()),
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "validation".into(),
                message: "pending verify".into(),
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
            }),
        ],
    }));

    assert_eq!(telegram_view.primary_text, "Status");
    assert_eq!(telegram_view.items.len(), 1);
    assert!(matches!(
        &telegram_view.items[0],
        rust_agent::interaction::view::TelegramItem::RuntimeNotice { kind, message }
            if kind == "validation" && message == "pending verify"
    ));
}

#[test]
fn telegram_gateway_authorization_distinguishes_binding_and_delivery_readiness() {
    let gateway = TelegramGateway {
        allowed_bindings: vec![
            SessionBinding {
                actor_id: "actor-1".into(),
                session_id: "telegram-session-1".into(),
                telegram_user_id: Some("user-1".into()),
                bot_id: Some("bot-1".into()),
                delivery_target: Some(TelegramDeliveryTarget {
                    chat_id: "chat-1".into(),
                    thread_id: Some("thread-9".into()),
                }),
            },
            SessionBinding {
                actor_id: "actor-2".into(),
                session_id: "telegram-session-2".into(),
                telegram_user_id: Some("user-2".into()),
                bot_id: Some("bot-1".into()),
                delivery_target: None,
            },
        ],
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    assert_eq!(
        gateway.authorize_binding("actor-1", "telegram-session-1"),
        TelegramBindingAuthorization::DeliveryReady(TelegramDeliveryTarget {
            chat_id: "chat-1".into(),
            thread_id: Some("thread-9".into()),
        })
    );
    assert_eq!(
        gateway.authorize_binding("actor-2", "telegram-session-2"),
        TelegramBindingAuthorization::AuthorizedNoDeliveryTarget
    );
    assert_eq!(
        gateway.authorize_binding("actor-3", "telegram-session-3"),
        TelegramBindingAuthorization::Unauthorized
    );
}

#[test]
fn telegram_gateway_rejects_explicit_target_without_matching_binding() {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };
    let notification = Notification {
        session_id: "telegram-session-1".into(),
        title: "Approval required: Bash".into(),
        body: "requires explicit approval".into(),
        notification_type: NotificationType::ApprovalRequired,
        task_id: None,
        task_type: None,
        status: None,
        next_action: None,
        worker_role: None,
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        output_file: None,
        usage: None,
        tool_name: Some("Bash".into()),
        approval_code: Some("bash_warning".into()),
        approval_summary: Some("Bash pending approval".into()),
        approval_detail: Some("requires explicit approval".into()),
        approval_kind: Some("tool_permission".into()),
        approval_escalation_reasons: vec!["privileged_system".into()],
        notice_kind: None,
        notice_code: None,
        runtime_kind: None,
        service_failure_code: None,
        provider_kind: None,
        status_code: None,
        retryable: None,
        surface_visible: None,
        dedupe_key: Some("approval_required:Bash:tool_permission:bash_warning".into()),
        wake_up: true,
        target: Some(NotificationTarget::Telegram(TelegramDeliveryTarget {
            chat_id: "chat-other".into(),
            thread_id: None,
        })),
    };

    assert!(!gateway.can_deliver(&notification));
    assert_eq!(gateway.prepare_delivery(&notification), None);
}

#[test]
fn telegram_gateway_authorizes_telegram_principal_separately_from_delivery_readiness() {
    let gateway = TelegramGateway {
        allowed_bindings: vec![
            SessionBinding {
                actor_id: "actor-1".into(),
                session_id: "telegram-session-1".into(),
                telegram_user_id: Some("user-1".into()),
                bot_id: Some("bot-1".into()),
                delivery_target: Some(TelegramDeliveryTarget {
                    chat_id: "chat-1".into(),
                    thread_id: None,
                }),
            },
            SessionBinding {
                actor_id: "actor-2".into(),
                session_id: "telegram-session-2".into(),
                telegram_user_id: Some("user-2".into()),
                bot_id: Some("bot-1".into()),
                delivery_target: None,
            },
        ],
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    assert_eq!(
        gateway.authorize_principal("user-1", "bot-1", "telegram-session-1"),
        TelegramBindingAuthorization::DeliveryReady(TelegramDeliveryTarget {
            chat_id: "chat-1".into(),
            thread_id: None,
        })
    );
    assert_eq!(
        gateway.authorize_principal("user-2", "bot-1", "telegram-session-2"),
        TelegramBindingAuthorization::AuthorizedNoDeliveryTarget
    );
    assert_eq!(
        gateway.authorize_principal("user-9", "bot-1", "telegram-session-1"),
        TelegramBindingAuthorization::Unauthorized
    );
}

#[test]
fn telegram_gateway_resolves_remote_actor_delivery_target_only_for_bound_actor() {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };
    let mut actor_notification = Notification::approval_required(
        "telegram-session-1",
        "Bash",
        "needs approval",
        Some("bash_warning".into()),
        Some("Bash pending approval".into()),
        Some("needs approval".into()),
        Some("tool_permission".into()),
        vec!["privileged_system".into()],
    );
    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "telegram-session-1".into(),
        actor_id: "actor-1".into(),
    });

    let prepared = gateway
        .prepare_delivery(&actor_notification)
        .expect("bound actor should resolve to telegram target");
    assert_eq!(
        prepared.target,
        Some(NotificationTarget::Telegram(TelegramDeliveryTarget {
            chat_id: "chat-1".into(),
            thread_id: Some("thread-9".into()),
        }))
    );

    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "telegram-session-1".into(),
        actor_id: "other-actor".into(),
    });
    assert_eq!(gateway.prepare_delivery(&actor_notification), None);
}

#[test]
fn telegram_gateway_inbound_binding_requires_matching_principal_actor_session_and_bot() {
    let gateway = TelegramGateway {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    assert!(matches!(
        gateway.authorize_inbound_binding("user-1", "bot-1", "actor-1", "telegram-session-1"),
        TelegramInboundBindingAuthorization::Authorized(binding)
            if binding.actor_id == "actor-1" && binding.session_id == "telegram-session-1"
    ));
    assert_eq!(
        gateway.authorize_inbound_binding("user-9", "bot-1", "actor-1", "telegram-session-1"),
        TelegramInboundBindingAuthorization::PrincipalMismatch
    );
    assert_eq!(
        gateway.authorize_inbound_binding("user-1", "bot-9", "actor-1", "telegram-session-1"),
        TelegramInboundBindingAuthorization::BotMismatch
    );
    assert_eq!(
        gateway.authorize_inbound_binding("user-1", "bot-1", "actor-9", "telegram-session-1"),
        TelegramInboundBindingAuthorization::ActorMismatch
    );
    assert_eq!(
        gateway.authorize_inbound_binding("user-1", "bot-1", "actor-1", "missing-session"),
        TelegramInboundBindingAuthorization::SessionNotBound
    );
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };
    let view = build_surface_view(&CliTurnOutput {
        primary_text: "Primary reply".into(),
        events: vec![
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "telegram-session-1".into(),
                    surface: InteractionSurface::Telegram,
                },
                target_task_id: Some("task-tele-2".into()),
                task_id: "task-tele-2".into(),
                task_type: rust_agent::task::types::TaskType::LocalBash,
                status: TaskStatus::Completed,
                summary: "bash: pwd".into(),
                result: "Command completed".into(),
                next_action: "inspect command output for task-tele-2".into(),
                worker_role: None,
                orchestration_group_id: None,
                phase: None,
                validation_state: None,
                output_file: "/tmp/task-tele-2.log".into(),
                usage: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "delivery".into(),
                message: "background work still running".into(),
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
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
                text: "Task: bash: pwd\nType: local_bash\nStatus: completed\nResult: Command completed\nNext: inspect command output for task-tele-2\nOutput: /tmp/task-tele-2.log".into(),
            },
            TelegramOutgoingMessage {
                target: TelegramDeliveryTarget {
                    chat_id: "chat-1".into(),
                    thread_id: Some("thread-9".into()),
                },
                text: "Notice: delivery\nbackground work still running".into(),
            }
        ]
    );
}

#[test]
fn normalized_input_from_telegram_raw_marks_telegram_surface_and_actor() {
    let input = rust_agent::interaction::envelope::NormalizedInput::from_telegram_raw(
        "telegram-session-1",
        "actor-1",
        true,
        "/help please",
    );

    assert_eq!(input.surface, InteractionSurface::Telegram);
    assert_eq!(input.session_id, "telegram-session-1");
    assert_eq!(input.actor.actor_id, "actor-1");
    assert!(input.actor.is_authenticated);
    assert!(input.metadata.from_trusted_surface);
    assert_eq!(input.command_name.as_deref(), Some("help"));
    assert_eq!(input.command_args, "please");
}

#[test]
fn telegram_inbound_intake_authorizes_before_normalizing_input() {
    let gateway = TelegramGateway {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    let intake = gateway.intake_inbound(TelegramInboundRequest {
        telegram_user_id: "user-1".into(),
        bot_id: "bot-1".into(),
        actor_id: "actor-1".into(),
        session_id: "telegram-session-1".into(),
        raw: "/help please".into(),
    });

    assert!(matches!(
        intake,
        TelegramInboundIntake::Authorized { binding, input }
            if binding.actor_id == "actor-1"
                && input.surface == InteractionSurface::Telegram
                && input.session_id == "telegram-session-1"
                && input.actor.actor_id == "actor-1"
                && input.command_name.as_deref() == Some("help")
                && input.command_args == "please"
    ));
}

#[test]
fn telegram_inbound_intake_preserves_explicit_rejection_paths_without_normalizing() {
    let gateway = TelegramGateway {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    assert_eq!(
        gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-9".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help please".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::PrincipalMismatch)
    );
    assert_eq!(
        gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-9".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help please".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::BotMismatch)
    );
    assert_eq!(
        gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-9".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help please".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::ActorMismatch)
    );
    assert_eq!(
        gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "missing-session".into(),
            raw: "/help please".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::SessionNotBound)
    );
}

#[test]
fn telegram_inbound_intake_rejects_not_allowlisted_rate_limited_and_abuse_blocked() {
    let allowlisted_gateway = TelegramGateway::default()
        .with_bindings(vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: None,
            }),
        }])
        .with_admission_policy(SurfaceAdmissionPolicy {
            allowlisted_actors: ["actor-9".to_string()].into_iter().collect(),
            max_requests_per_window: None,
            window_seconds: 60,
            abuse_denial_threshold: None,
        });
    assert_eq!(
        allowlisted_gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help please".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::NotAllowlisted)
    );

    let rate_limited_gateway = TelegramGateway::default()
        .with_bindings(vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: None,
            }),
        }])
        .with_admission_policy(SurfaceAdmissionPolicy {
            allowlisted_actors: std::collections::HashSet::new(),
            max_requests_per_window: Some(1),
            window_seconds: 60,
            abuse_denial_threshold: None,
        });
    assert!(matches!(
        rate_limited_gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help first".into(),
        }),
        TelegramInboundIntake::Authorized { .. }
    ));
    assert_eq!(
        rate_limited_gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help second".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::RateLimited)
    );

    let abuse_blocked_gateway = TelegramGateway::default()
        .with_bindings(vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: None,
            }),
        }])
        .with_admission_policy(SurfaceAdmissionPolicy {
            allowlisted_actors: ["actor-9".to_string()].into_iter().collect(),
            max_requests_per_window: None,
            window_seconds: 60,
            abuse_denial_threshold: Some(1),
        });
    assert_eq!(
        abuse_blocked_gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help first".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::NotAllowlisted)
    );
    assert_eq!(
        abuse_blocked_gateway.intake_inbound(TelegramInboundRequest {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw: "/help second".into(),
        }),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::AbuseBlocked)
    );
}

#[test]
fn authorizer_uses_audit_aligned_deny_reason_codes() {
    let authorizer =
        DefaultSurfaceAuthorizer::default().with_remote_policy(SurfaceAdmissionPolicy {
            allowlisted_actors: ["approved-actor".to_string()].into_iter().collect(),
            max_requests_per_window: Some(1),
            window_seconds: 60,
            abuse_denial_threshold: Some(2),
        });

    let unauthenticated =
        NormalizedInput::from_remote_raw("session-1", "actor-1", false, true, "hello");
    assert_eq!(
        authorizer.authorize(&unauthenticated),
        AuthDecision::Deny {
            category: AuthDenyCategory::Unauthenticated,
            reason: "unauthenticated: unauthenticated actor for Remote surface".into(),
        }
    );

    let not_allowlisted =
        NormalizedInput::from_remote_raw("session-1", "actor-1", true, true, "hello");
    assert_eq!(
        authorizer.authorize(&not_allowlisted),
        AuthDecision::Deny {
            category: AuthDenyCategory::NotAllowlisted,
            reason: "not_allowlisted: actor actor-1 is not allowlisted for Remote surface".into(),
        }
    );
    assert_eq!(
        authorizer.authorize(&not_allowlisted),
        AuthDecision::Deny {
            category: AuthDenyCategory::AbuseBlocked,
            reason: "abuse_blocked: actor actor-1 is temporarily blocked on Remote surface".into(),
        }
    );

    let rate_limited_authorizer =
        DefaultSurfaceAuthorizer::default().with_remote_policy(SurfaceAdmissionPolicy {
            allowlisted_actors: std::collections::HashSet::new(),
            max_requests_per_window: Some(1),
            window_seconds: 60,
            abuse_denial_threshold: None,
        });
    let rate_limited =
        NormalizedInput::from_remote_raw("session-2", "actor-2", true, true, "hello");
    assert_eq!(
        rate_limited_authorizer.authorize(&rate_limited),
        AuthDecision::Allow
    );
    assert_eq!(
        rate_limited_authorizer.authorize(&rate_limited),
        AuthDecision::Deny {
            category: AuthDenyCategory::RateLimited,
            reason: "rate_limited: actor actor-2 exceeded request rate for Remote surface".into(),
        }
    );

    let command_blocked =
        NormalizedInput::from_remote_raw("session-3", "actor-3", true, true, "/permissions");
    let command_authorizer = DefaultSurfaceAuthorizer::default();
    assert_eq!(
        command_authorizer.authorize(&command_blocked),
        AuthDecision::Deny {
            category: AuthDenyCategory::SurfaceCommandBlocked,
            reason: "surface_command_blocked: command is blocked on remote surface".into(),
        }
    );
}

#[test]
fn telegram_transport_adapter_maps_webhook_fields_without_adding_policy_logic() {
    let gateway = TelegramGateway {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    let intake = intake_transport_envelope(
        &gateway,
        TelegramInboundEnvelope {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            raw_text: "/help please".into(),
        },
    );

    assert!(matches!(
        intake,
        TelegramInboundIntake::Authorized { input, .. }
            if input.surface == InteractionSurface::Telegram
                && input.session_id == "telegram-session-1"
                && input.actor.actor_id == "actor-1"
                && input.command_name.as_deref() == Some("help")
                && input.command_args == "please"
    ));
}

#[test]
fn telegram_transport_adapter_preserves_rejection_reason() {
    let gateway = TelegramGateway {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };

    assert_eq!(
        intake_transport_envelope(
            &gateway,
            TelegramInboundEnvelope {
                telegram_user_id: "user-1".into(),
                bot_id: "bot-9".into(),
                actor_id: "actor-1".into(),
                session_id: "telegram-session-1".into(),
                raw_text: "/help please".into(),
            }
        ),
        TelegramInboundIntake::Rejected(TelegramInboundBindingAuthorization::BotMismatch)
    );
}

#[tokio::test]
async fn telegram_runtime_entry_routes_authorized_input_into_shared_runtime_and_messages() {
    let command_registry =
        Arc::new(CommandRegistry::new().register(Arc::new(TelegramPromptCommand)));
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("telegram-runtime-session".into()),
            surface: InteractionSurface::Telegram,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/telegram-runtime".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    let gateway = TelegramGateway {
        allowed_bindings: vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-runtime-session".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: Some("thread-1".into()),
            }),
        }],
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };
    let app_state = telegram_test_app_state(
        command_registry.clone(),
        gateway.clone(),
        session_store.clone(),
        "bootstrap-telegram-session",
    );
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("telegram runtime reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_telegram_envelope(
        &router,
        &engine,
        &app_state,
        &gateway,
        TelegramInboundEnvelope {
            telegram_user_id: "user-1".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-runtime-session".into(),
            raw_text: "/telegram-prompt run".into(),
        },
    )
    .await
    .expect("telegram runtime should succeed");

    assert!(matches!(
        &response,
        TelegramRuntimeResponse::Authorized { primary_text, .. }
            if primary_text.contains("telegram runtime reply")
    ));
    let TelegramRuntimeResponse::Authorized { messages, .. } = response else {
        panic!("expected authorized telegram runtime response");
    };
    assert_eq!(
        messages,
        vec![TelegramOutgoingMessage {
            target: TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: Some("thread-1".into()),
            },
            text: "telegram runtime reply".into(),
        }]
    );

    let (_, default_history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("bootstrap-telegram-session".into()),
            continue_session: false,
        })
        .unwrap_or((
            SessionSnapshot {
                session_id: rust_agent::history::session::SessionId(
                    "bootstrap-telegram-session".into(),
                ),
                surface: InteractionSurface::Telegram,
                session_mode: SessionMode::Interactive,
                cwd: String::new(),
                last_turn_at: None,
                prompt_seed: None,
            },
            SessionHistory::default(),
        ));
    assert!(default_history.entries.is_empty());

    let (_, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("telegram-runtime-session".into()),
            continue_session: false,
        })
        .expect("expected telegram runtime history");
    assert_eq!(history.entries.len(), 2);
    assert_eq!(
        history.entries[0].message,
        rust_agent::core::message::Message::user("telegram runtime prompt")
    );
    assert_eq!(
        history.entries[1].message,
        rust_agent::core::message::Message::assistant("telegram runtime reply")
    );
}

#[tokio::test]
async fn telegram_runtime_entry_preserves_rejection_without_running_shared_runtime() {
    let command_registry =
        Arc::new(CommandRegistry::new().register(Arc::new(TelegramPromptCommand)));
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let session_store = Arc::new(InMemorySessionStore::default());
    let gateway = TelegramGateway {
        allowed_bindings: vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-runtime-session".into(),
            telegram_user_id: Some("user-1".into()),
            bot_id: Some("bot-1".into()),
            delivery_target: Some(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: Some("thread-1".into()),
            }),
        }],
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    };
    let app_state = telegram_test_app_state(
        command_registry.clone(),
        gateway.clone(),
        session_store.clone(),
        "bootstrap-telegram-session",
    );
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("should not run".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_telegram_envelope(
        &router,
        &engine,
        &app_state,
        &gateway,
        TelegramInboundEnvelope {
            telegram_user_id: "user-9".into(),
            bot_id: "bot-1".into(),
            actor_id: "actor-1".into(),
            session_id: "telegram-runtime-session".into(),
            raw_text: "/telegram-prompt run".into(),
        },
    )
    .await
    .expect("telegram runtime should return rejection");

    assert_eq!(
        response,
        TelegramRuntimeResponse::Rejected(TelegramInboundBindingAuthorization::PrincipalMismatch)
    );
    assert!(
        session_store
            .load(&SessionRestoreRequest {
                resume: Some("telegram-runtime-session".into()),
                continue_session: false,
            })
            .is_none()
    );
}

#[test]
fn web_view_is_derived_from_surface_view_with_frontend_friendly_kinds() {
    let turn = CliTurnOutput {
        primary_text: "Primary reply".into(),
        events: vec![
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "session-web".into(),
                    surface: InteractionSurface::Cli,
                },
                target_task_id: Some("task-web-1".into()),
                task_id: "task-web-1".into(),
                task_type: rust_agent::task::types::TaskType::LocalBash,
                status: TaskStatus::Completed,
                summary: "bash: ls".into(),
                result: "Command completed".into(),
                next_action: "inspect command output for task-web-1".into(),
                worker_role: None,
                orchestration_group_id: None,
                phase: None,
                validation_state: None,
                output_file: "/tmp/task-web-1.log".into(),
                usage: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "runtime".into(),
                message: "background work still running".into(),
                code: Some("api_stream_interrupted".into()),
                runtime_kind: Some("RetryScheduled".into()),
                service_failure_code: Some("api_stream_interrupted".into()),
                provider_kind: Some("anthropic".into()),
                status_code: Some(503),
                retryable: Some(true),
                surface_visible: Some(true),
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Transition {
                kind: "next_turn".into(),
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
    assert_eq!(web_view.items.len(), 4);
    assert!(matches!(
        &web_view.items[0],
        WebItem::TaskUpdate(task)
            if task.task_type == "local_bash"
                && task.task_id == "task-web-1"
                && task.next_action == "inspect command output for task-web-1"
    ));
    assert!(matches!(
        &web_view.items[1],
        WebItem::RuntimeNotice {
            notice_kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        }
            if notice_kind == "runtime"
                && message == "background work still running"
                && code.as_deref() == Some("api_stream_interrupted")
                && runtime_kind.as_deref() == Some("RetryScheduled")
                && service_failure_code.as_deref() == Some("api_stream_interrupted")
                && provider_kind.as_deref() == Some("anthropic")
                && status_code == &Some(503)
                && retryable == &Some(true)
                && surface_visible == &Some(true)
    ));
    assert!(matches!(
        &web_view.items[2],
        WebItem::Transition { transition_kind, text }
            if transition_kind == "next_turn" && text == "next_turn"
    ));
    assert!(matches!(
        &web_view.items[3],
        WebItem::ToolResult { tool_name, content, .. }
            if tool_name == "Read" && content == "line one"
    ));
}

#[test]
fn same_surface_view_feeds_remote_telegram_and_web_without_cli_renderer_types() {
    let view = build_surface_view(&CliTurnOutput {
        primary_text: "Shared reply".into(),
        events: vec![
            CliDisplayEvent::TaskEvent(TaskEvent {
                owner: TaskOwner {
                    session_id: "shared-session".into(),
                    surface: InteractionSurface::Cli,
                },
                target_task_id: Some("task-shared-1".into()),
                task_id: "task-shared-1".into(),
                task_type: rust_agent::task::types::TaskType::LocalAgent,
                status: TaskStatus::Completed,
                summary: "shared task".into(),
                result: "Task completed".into(),
                next_action: "inspect task output for task-shared-1".into(),
                worker_role: None,
                orchestration_group_id: None,
                phase: None,
                validation_state: None,
                output_file: "/tmp/task-shared-1.log".into(),
                usage: None,
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "requires explicit approval".into(),
                code: Some("bash_warning".into()),
                summary: None,
                detail: None,
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["privileged_system".into()],
            }),
            CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "delivery".into(),
                message: "background work still running".into(),
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
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

    assert_eq!(remote_events.len(), 3);
    assert_eq!(telegram_view.items.len(), 3);
    assert_eq!(web_view.items.len(), 3);
    assert!(matches!(
        &remote_events[0].payload,
        RemoteEventPayload::TaskUpdate(task) if task.task_type == "local_agent"
    ));
    assert!(matches!(
        &remote_events[1].payload,
        RemoteEventPayload::ApprovalRequired {
            code,
            approval_kind,
            escalation_reasons,
            ..
        } if code.as_deref() == Some("bash_warning")
            && approval_kind.as_deref() == Some("tool_permission")
            && escalation_reasons == &vec!["privileged_system".to_string()]
    ));
    assert!(matches!(
        &telegram_view.items[0],
        rust_agent::interaction::view::TelegramItem::TaskUpdate(task)
            if task.task_type == "local_agent"
    ));
    assert!(matches!(
        &telegram_view.items[1],
        rust_agent::interaction::view::TelegramItem::ApprovalRequired { .. }
    ));
    assert!(matches!(
        &web_view.items[0],
        WebItem::TaskUpdate(task) if task.task_type == "local_agent"
    ));
    assert!(matches!(
        &web_view.items[1],
        WebItem::ApprovalRequired { .. }
    ));
}

#[test]
fn dispatcher_drains_remote_session_and_actor_notifications() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice(
            "remote-session",
            "tool",
            "session scoped",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    );
    let mut actor_notification = Notification::approval_required(
        "remote-session",
        "Bash",
        "requires explicit approval",
        Some("bash_warning".into()),
        Some("Bash pending approval".into()),
        Some("requires explicit approval".into()),
        Some("tool_permission".into()),
        vec!["privileged_system".into()],
    );
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
        Notification::runtime_notice(
            "remote-session",
            "tool",
            "session only",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
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
    let session_notification = Notification::runtime_notice(
        "remote-session",
        "tool",
        "same message",
        Some("api_stream_interrupted".into()),
        Some("RetryScheduled".into()),
        Some("api_stream_interrupted".into()),
        Some("anthropic".into()),
        Some(503),
        Some(true),
        Some(true),
    );
    let session_dedupe = session_notification.dedupe_key.clone();
    dispatcher.dispatch(InteractionSurface::Remote, session_notification);

    let mut actor_notification = Notification::runtime_notice(
        "remote-session",
        "tool",
        "different display message",
        Some("api_stream_interrupted".into()),
        Some("RetryScheduled".into()),
        Some("api_stream_interrupted".into()),
        Some("anthropic".into()),
        Some(503),
        Some(true),
        Some(true),
    );
    assert_eq!(actor_notification.dedupe_key, session_dedupe);
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
        Some("local_agent"),
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
        Some("local_agent"),
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
fn approval_and_runtime_notice_dedupe_keys_ignore_display_message_text() {
    let approval_a = Notification::approval_required(
        "remote-session",
        "Bash",
        "requires explicit approval",
        Some("bash_warning".into()),
        Some("Bash pending approval".into()),
        Some("requires explicit approval".into()),
        Some("tool_permission".into()),
        vec!["privileged_system".into()],
    );
    let approval_b = Notification::approval_required(
        "remote-session",
        "Bash",
        "wording changed but same approval",
        Some("bash_warning".into()),
        Some("Bash pending approval".into()),
        Some("wording changed but same approval".into()),
        Some("tool_permission".into()),
        vec!["privileged_system".into()],
    );
    assert_eq!(approval_a.dedupe_key, approval_b.dedupe_key);

    let runtime_a = Notification::runtime_notice(
        "remote-session",
        "tool",
        "background update",
        Some("api_stream_interrupted".into()),
        Some("RetryScheduled".into()),
        Some("api_stream_interrupted".into()),
        Some("anthropic".into()),
        Some(503),
        Some(true),
        Some(true),
    );
    let runtime_b = Notification::runtime_notice(
        "remote-session",
        "tool",
        "different wording for same runtime state",
        Some("api_stream_interrupted".into()),
        Some("RetryScheduled".into()),
        Some("api_stream_interrupted".into()),
        Some("anthropic".into()),
        Some(503),
        Some(true),
        Some(true),
    );
    assert_eq!(runtime_a.dedupe_key, runtime_b.dedupe_key);
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
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: std::sync::Arc::new(std::sync::Mutex::new(AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "remote-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    let mut approval = Notification::approval_required(
        "remote-session",
        "Bash",
        "requires explicit approval",
        Some("bash_warning".into()),
        Some("Bash pending approval".into()),
        Some("requires explicit approval".into()),
        Some("tool_permission".into()),
        vec!["privileged_system".into()],
    );
    approval.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, approval);

    let mut runtime_notice = Notification::runtime_notice(
        "remote-session",
        "runtime",
        "provider retry scheduled",
        Some("api_provider_http_5xx".into()),
        Some("RetryScheduled".into()),
        Some("api_provider_http_5xx".into()),
        Some("anthropic".into()),
        Some(503),
        Some(true),
        Some(true),
    );
    runtime_notice.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, runtime_notice);

    let drained = drain_remote_notifications(&app_state, "remote-session", Some("actor-1"));
    assert_eq!(drained.len(), 2);
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::ApprovalRequired {
            tool_name,
            message,
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        }
            if tool_name == "Bash"
                && message == "requires explicit approval"
                && code.as_deref() == Some("bash_warning")
                && summary.as_deref() == Some("Bash pending approval")
                && detail.as_deref() == Some("requires explicit approval")
                && approval_kind.as_deref() == Some("tool_permission")
                && escalation_reasons == &vec!["privileged_system".to_string()]
    )));
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::RuntimeNotice {
            kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        }
            if kind == "runtime"
                && message == "provider retry scheduled"
                && code.as_deref() == Some("api_provider_http_5xx")
                && runtime_kind.as_deref() == Some("RetryScheduled")
                && service_failure_code.as_deref() == Some("api_provider_http_5xx")
                && provider_kind.as_deref() == Some("anthropic")
                && status_code == &Some(503)
                && retryable == &Some(true)
                && surface_visible == &Some(true)
    )));
}

#[test]
fn drain_remote_task_update_notifications_preserve_task_type() {
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
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: std::sync::Arc::new(std::sync::Mutex::new(AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "remote-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    let mut notification = Notification::task_update(
        "remote-session",
        "Command completed",
        "bash: ls (task-9) — command completed",
        "task-9",
        Some("local_bash"),
        "completed",
        "inspect command output for task-9",
        None,
        None,
        None,
        None,
        "/tmp/task-9.log",
        None,
    );
    notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-session".into(),
        actor_id: "actor-1".into(),
    });
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, notification);

    let drained = drain_remote_notifications(&app_state, "remote-session", Some("actor-1"));
    assert_eq!(drained.len(), 1);
    assert!(matches!(
        &drained[0].payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-9"
                && task.task_type == "local_bash"
                && task.next_action == "inspect command output for task-9"
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    });
    let prepared = Notification {
        session_id: "telegram-session-1".into(),
        title: "Task completed — validation: verified".into(),
        body: "demo body — completed".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-1".into()),
        task_type: Some("local_agent".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-1".into()),
        worker_role: Some("verify".into()),
        orchestration_group_id: None,
        phase: Some("verify".into()),
        validation_state: Some("verified".into()),
        output_file: Some("/tmp/task-1.log".into()),
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
        target: Some(NotificationTarget::Telegram(TelegramDeliveryTarget {
            chat_id: "chat-1".into(),
            thread_id: None,
        })),
    };
    let notification = Notification {
        target: Some(NotificationTarget::Session {
            session_id: "telegram-session-1".into(),
        }),
        ..prepared.clone()
    };

    dispatcher.dispatch(InteractionSurface::Telegram, notification);

    assert_eq!(dispatcher.delivered(), vec![prepared.clone()]);
    assert_eq!(
        dispatcher.drain_telegram_notifications("telegram-session-1"),
        vec![prepared]
    );
}

#[test]
fn telegram_dispatch_only_enqueues_wake_up_notifications() {
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
        surface_authorizer: DefaultSurfaceAuthorizer::default(),
    });

    dispatcher.dispatch(
        InteractionSurface::Telegram,
        Notification::approval_required(
            "telegram-session-1",
            "Bash",
            "needs approval",
            Some("bash_warning".into()),
            Some("Bash pending approval".into()),
            Some("needs approval".into()),
            Some("tool_permission".into()),
            vec!["privileged_system".into()],
        ),
    );
    dispatcher.dispatch(
        InteractionSurface::Telegram,
        Notification::runtime_notice(
            "telegram-session-1",
            "tool",
            "background only",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    );

    let drained = dispatcher.drain_telegram_notifications("telegram-session-1");
    assert_eq!(drained.len(), 1);
    assert_eq!(
        drained[0].notification_type,
        NotificationType::ApprovalRequired
    );
    assert!(
        dispatcher
            .delivered()
            .iter()
            .any(|notification| notification.notification_type == NotificationType::RuntimeNotice)
    );
}

use rust_agent::bootstrap::InteractionSurface;
use rust_agent::interaction::cli::renderer::render_turn_output;
use rust_agent::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::{Notification, NotificationTarget, NotificationType};
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
    assert!(rendered.contains("[task] worker_role: implement"));
    assert!(rendered.contains("[task] next_action: dispatch verify worker for task-2"));
    assert!(rendered.contains("[notice:validation] Validation pending; final answer must call out unverified risk until verify completes."));
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
    assert!(rendered.contains("Tool: Bash"));
    assert!(rendered.contains("requires explicit approval"));
    assert!(rendered.contains("== Tool result =="));
    assert!(rendered.contains("Tool: Read"));
    assert!(rendered.contains("line one"));
    assert!(rendered.contains("line two"));
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
            wake_up: true,
            target: Some(NotificationTarget::Telegram(TelegramDeliveryTarget {
                chat_id: "chat-1".into(),
                thread_id: None,
            })),
        }]
    );
}

use rust_agent::bootstrap::InteractionSurface;
use rust_agent::interaction::cli::renderer::render_output;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::{Notification, NotificationType};
use rust_agent::interaction::telegram::binding::SessionBinding;
use rust_agent::interaction::telegram::gateway::TelegramGateway;

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
        output_file: Some("/tmp/task-1.log".into()),
        wake_up: true,
        target: None,
    };

    dispatcher.dispatch(InteractionSurface::Cli, notification.clone());

    assert_eq!(dispatcher.delivered(), vec![notification]);
}

#[test]
fn cli_renderer_marks_task_notification_lines() {
    let rendered = render_output(
        "<task-notification>\n<task-id>task-1</task-id>\n<status>Completed</status>\n</task-notification>",
    );

    assert!(rendered.contains("[task] <task-notification>"));
    assert!(rendered.contains("[task] <task-id>task-1</task-id>"));
    assert!(rendered.contains("[task] <status>Completed</status>"));
}

#[test]
fn dispatcher_requires_delivery_ready_binding_for_telegram() {
    let dispatcher = NotificationDispatcher::new(TelegramGateway {
        allowed_bindings: vec![SessionBinding {
            actor_id: "actor-1".into(),
            session_id: "telegram-session-1".into(),
            delivery_target: Some("chat-1".into()),
        }],
    });
    let notification = Notification {
        session_id: "telegram-session-1".into(),
        title: "Task completed".into(),
        body: "demo body".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-1".into()),
        status: Some("Completed".into()),
        output_file: Some("/tmp/task-1.log".into()),
        wake_up: true,
        target: Some("chat-1".into()),
    };

    dispatcher.dispatch(InteractionSurface::Telegram, notification.clone());

    assert_eq!(dispatcher.delivered(), vec![notification]);
}

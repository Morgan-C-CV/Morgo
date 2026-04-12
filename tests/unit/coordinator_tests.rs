use rust_agent::bootstrap::InteractionSurface;
use rust_agent::coordinator::mode::{is_coordinator_mode, match_session_mode, set_coordinator_mode};
use rust_agent::coordinator::worker::{filter_tools_for_worker, notification_to_task_notification, TaskNotification};
use rust_agent::interaction::notification::{Notification, NotificationType};
use rust_agent::state::app_state::WorkerRole;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::ask_user::AskUserQuestionTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::web_search::WebSearchTool;
use rust_agent::tool::definition::Tool;

#[test]
fn coordinator_mode_matches_resumed_session() {
    set_coordinator_mode(false);
    let message = match_session_mode(Some("coordinator"));
    assert!(is_coordinator_mode());
    assert_eq!(message.as_deref(), Some("Entered coordinator mode to match resumed session."));
}

#[test]
fn worker_notification_formats_as_task_notification_xml() {
    let event = TaskEvent {
        owner: TaskOwner {
            session_id: "session-1".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some("task-7".into()),
        task_id: "task-7".into(),
        status: TaskStatus::Completed,
        summary: "Worker finished research".into(),
        result: "Task completed".into(),
        next_action: "inspect task output for task-7".into(),
        worker_role: None,
        output_file: "/tmp/task-7.log".into(),
    };

    let notification = TaskNotification::from_task_event(&event);
    let formatted = notification.format_as_user_message();
    assert!(formatted.contains("<task-notification>"));
    assert!(formatted.contains("<task-id>task-7</task-id>"));
    assert!(formatted.contains("<summary>Worker finished research</summary>"));
}

#[test]
fn notification_conversion_preserves_worker_role_and_next_action() {
    let notification = Notification {
        session_id: "session-1".into(),
        title: "Task completed".into(),
        body: "Worker finished verify".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-8".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-8".into()),
        worker_role: Some("verify".into()),
        output_file: Some("/tmp/task-8.log".into()),
        wake_up: true,
        target: None,
    };

    let converted = notification_to_task_notification(&notification).expect("should convert");
    assert_eq!(converted.task_id, "task-8");
    assert_eq!(converted.next_action, "inspect task output for task-8");
    assert_eq!(converted.worker_role, Some(WorkerRole::Verify));
}

#[test]
fn coordinator_worker_filter_excludes_interactive_and_deferred_tools() {
    let all_tools = vec![
        AgentTool.metadata(),
        AskUserQuestionTool.metadata(),
        FileReadTool.metadata(),
        WebSearchTool.metadata(),
    ];

    let filtered = filter_tools_for_worker(&all_tools);
    let names = filtered.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(!names.contains(&"Agent"));
    assert!(!names.contains(&"AskUserQuestion"));
    assert!(!names.contains(&"WebSearch"));
}

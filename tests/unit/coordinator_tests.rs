use rust_agent::bootstrap::InteractionSurface;
use rust_agent::coordinator::mode::{is_coordinator_mode, match_session_mode, set_coordinator_mode};
use rust_agent::coordinator::worker::{filter_tools_for_worker, TaskNotification};
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
        output_file: "/tmp/task-7.log".into(),
    };

    let notification = TaskNotification::from_task_event(&event);
    let formatted = notification.format_as_user_message();
    assert!(formatted.contains("<task-notification>"));
    assert!(formatted.contains("<task-id>task-7</task-id>"));
    assert!(formatted.contains("<summary>Worker finished research</summary>"));
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

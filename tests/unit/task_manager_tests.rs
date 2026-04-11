use std::sync::Arc;

use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::TaskStatus;
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};

#[test]
fn terminal_task_states_mark_delivery_notified() {
    let manager = TaskManager::default();
    let task = manager.create("demo task");
    manager.start(&task.id);
    manager.append_output(&task.id, "hello output");
    manager.complete(&task.id, "session-1");

    let stored = manager.get(&task.id).expect("task should exist");
    assert_eq!(stored.status, TaskStatus::Completed);
    let output = manager
        .get_output(&task.id, 0)
        .expect("task output should be readable");
    assert_eq!(output.content, "hello output");
    assert_eq!(output.next_offset, "hello output".len());
    assert!(stored.output_file.ends_with("task-0.log"));
    assert_eq!(stored.output_offset, "hello output".len());
    assert!(stored.delivery.notified);
    let notification = stored
        .delivery
        .notification
        .expect("notification should exist");
    assert_eq!(notification.session_id, "session-1");
    assert_eq!(notification.title, "Task completed");
}

#[test]
fn task_manager_tracks_failed_and_killed_states() {
    let manager = TaskManager::default();
    let failed = manager.create("failing task");
    manager.fail(&failed.id, "session-2");
    assert_eq!(manager.get(&failed.id).unwrap().status, TaskStatus::Failed);

    let killed = manager.create("killed task");
    manager.kill(&killed.id, "session-3");
    assert_eq!(manager.get(&killed.id).unwrap().status, TaskStatus::Killed);
}

#[tokio::test]
async fn agent_tool_creates_running_task_in_shared_manager() {
    let manager = Arc::new(TaskManager::default());
    let permissions =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());

    let result = AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: "inspect repository".into(),
            },
            &permissions,
        )
        .await
        .expect("agent tool should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("agent task task-0 created and running"));

    let created = manager.get("task-0").expect("task should be created");
    assert_eq!(created.status, TaskStatus::Running);
    let output = manager
        .get_output("task-0", 0)
        .expect("task output should exist");
    assert!(
        output
            .content
            .contains("pending subagent input: inspect repository")
    );
}

#[test]
fn task_output_reads_support_offsets() {
    let manager = TaskManager::default();
    let task = manager.create("offset task");
    manager.append_output(&task.id, "hello");
    manager.append_output(&task.id, " world");

    let first = manager
        .get_output(&task.id, 0)
        .expect("full output should be readable");
    assert_eq!(first.content, "hello world");

    let second = manager
        .get_output(&task.id, 5)
        .expect("delta output should be readable");
    assert_eq!(second.content, " world");
    assert_eq!(second.next_offset, "hello world".len());
}

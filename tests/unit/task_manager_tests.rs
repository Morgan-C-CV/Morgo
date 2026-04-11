use std::sync::Arc;

use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskNotification, TaskStatus};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};

#[test]
fn terminal_task_states_mark_delivery_notified() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("demo task");
    manager.start(&task.id);
    manager.append_output(&task.id, "hello output");
    manager.complete(&task.id, "session-1", &dispatcher);

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
    assert_eq!(notification.task_id.as_deref(), Some("task-0"));
    assert_eq!(notification.status.as_deref(), Some("Completed"));
    assert_eq!(dispatcher.delivered().len(), 1);
}

#[test]
fn task_manager_tracks_failed_and_killed_states() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let failed = manager.create("failing task");
    manager.fail(&failed.id, "session-2", &dispatcher);
    assert_eq!(manager.get(&failed.id).unwrap().status, TaskStatus::Failed);

    let killed = manager.create("killed task");
    manager.kill(&killed.id, "session-3", &dispatcher);
    assert_eq!(manager.get(&killed.id).unwrap().status, TaskStatus::Killed);
}

#[tokio::test]
async fn agent_tool_launches_subagent_and_completes_task() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools =
        rust_agent::tool::registry::ToolRegistry::new().register(Arc::new(AgentTool));
    let inherited_hooks = rust_agent::hook::registry::HookRegistry::default().register_rule(
        rust_agent::hook::registry::HookRule {
            event: rust_agent::hook::registry::HookEventMatcher::SubagentStop,
            deny_match: None,
            append_message: Some("shared hook message".into()),
            prevent_continuation: false,
        },
    );
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-7")
        .with_inherited_tool_registry(inherited_tools)
        .with_inherited_hook_registry(inherited_hooks)
        .with_subagent_scripted_turns(vec![vec![
            rust_agent::service::api::streaming::StreamEvent::MessageStart,
            rust_agent::service::api::streaming::StreamEvent::TextDelta("subagent answer".into()),
            rust_agent::service::api::streaming::StreamEvent::MessageStop {
                stop_reason: rust_agent::service::api::streaming::StopReason::EndTurn,
            },
        ]]);

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
    assert!(text.contains("agent task task-0 launched"));

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let created = manager.get("task-0").expect("task should be created");
            if created.status == TaskStatus::Completed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subagent task should complete");

    let created = manager.get("task-0").expect("task should be created");
    assert_eq!(created.status, TaskStatus::Completed);
    let output = manager
        .get_output("task-0", 0)
        .expect("task output should exist");
    assert!(output.content.contains("subagent answer"));
    assert!(output.content.contains("shared hook message"));
    assert!(created.delivery.notified);
    assert_eq!(
        created
            .delivery
            .notification
            .as_ref()
            .expect("notification should exist")
            .session_id,
        "session-7"
    );
}

#[test]
fn task_manager_queues_internal_task_notifications() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("demo task");
    manager.complete(&task.id, "session-1", &dispatcher);

    let notifications = manager.drain_notifications("session-1");
    assert_eq!(notifications.len(), 1);
    assert_eq!(
        notifications[0],
        TaskNotification {
            session_id: "session-1".into(),
            task_id: task.id.clone(),
            status: TaskStatus::Completed,
            summary: format!("demo task ({})", task.id),
            output_file: task.output_file.clone(),
        }
    );
    assert!(manager.drain_notifications("session-1").is_empty());
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

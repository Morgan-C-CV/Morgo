use std::sync::Arc;

use rust_agent::bootstrap::InteractionSurface;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::send_message::SendMessageTool;
use rust_agent::tool::builtin::task_create::TaskCreateTool;
use rust_agent::tool::builtin::task_get::TaskGetTool;
use rust_agent::tool::builtin::task_list::TaskListTool;
use rust_agent::tool::builtin::task_output::TaskOutputTool;
use rust_agent::tool::builtin::task_stop::TaskStopTool;
use rust_agent::tool::builtin::task_update::TaskUpdateTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};

#[test]
fn terminal_task_states_mark_delivery_notified() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("demo task", "session-1", InteractionSurface::Cli);
    manager.start(&task.id);
    manager.append_output(&task.id, "hello output");
    manager.complete(&task.id, &dispatcher);

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
    let failed = manager.create("failing task", "session-2", InteractionSurface::Cli);
    manager.fail(&failed.id, &dispatcher);
    assert_eq!(manager.get(&failed.id).unwrap().status, TaskStatus::Failed);

    let killed = manager.create("killed task", "session-3", InteractionSurface::Cli);
    assert!(manager.kill(&killed.id, "session-3", &dispatcher));
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
    let task = manager.create("demo task", "session-1", InteractionSurface::Cli);
    manager.complete(&task.id, &dispatcher);

    let notifications = manager.drain_events("session-1");
    assert_eq!(notifications.len(), 1);
    assert_eq!(
        notifications[0],
        TaskEvent {
            owner: TaskOwner {
                session_id: "session-1".into(),
                surface: InteractionSurface::Cli,
            },
            target_task_id: Some(task.id.clone()),
            task_id: task.id.clone(),
            status: TaskStatus::Completed,
            summary: format!("demo task ({})", task.id),
            output_file: task.output_file.clone(),
        }
    );
    assert!(manager.drain_events("session-1").is_empty());
}

#[tokio::test]
async fn non_owner_cannot_kill_running_task() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("running task", "session-owner", InteractionSurface::Cli);

    manager.launch(&task.id, "inspect", async move {
        tokio::task::yield_now().await;
    });

    assert_eq!(
        manager.running_owner(&task.id),
        Some(TaskOwner {
            session_id: "session-owner".into(),
            surface: InteractionSurface::Cli,
        })
    );
    assert!(!manager.kill(&task.id, "session-other", &dispatcher));
}

#[tokio::test]
async fn owner_can_send_mailbox_message_but_non_owner_cannot() {
    let manager = TaskManager::default();
    let task = manager.create("running task", "session-owner", InteractionSurface::Cli);

    manager.launch(&task.id, "continue me", std::future::pending::<()>());

    assert!(manager.send_message(&task.id, "session-owner", "continue me"));
    assert_eq!(
        manager.wait_for_mailbox_message(&task.id).await,
        Some("continue me".into())
    );
    assert!(!manager.send_message(&task.id, "session-other", "nope"));
}

#[tokio::test]
async fn non_owner_cannot_continue_existing_task() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools =
        rust_agent::tool::registry::ToolRegistry::new().register(Arc::new(AgentTool));
    let owner_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-owner")
        .with_inherited_tool_registry(inherited_tools.clone())
        .with_subagent_scripted_turns(vec![vec![
            rust_agent::service::api::streaming::StreamEvent::MessageStart,
            rust_agent::service::api::streaming::StreamEvent::TextDelta("owned answer".into()),
            rust_agent::service::api::streaming::StreamEvent::MessageStop {
                stop_reason: rust_agent::service::api::streaming::StopReason::EndTurn,
            },
        ]]);

    AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: "inspect repository".into(),
            },
            &owner_permissions,
        )
        .await
        .expect("initial launch should succeed");

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
    .expect("initial task should complete");

    let other_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-other")
        .with_inherited_tool_registry(inherited_tools);

    let error = AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: "continue: task-0: follow-up".into(),
            },
            &other_permissions,
        )
        .await
        .expect_err("non-owner continuation should fail");

    assert!(
        error
            .to_string()
            .contains("not running or not owned by this session")
    );
}

#[test]
fn addressed_event_drain_filters_by_target_task() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task_a = manager.create("task a", "session-1", InteractionSurface::Cli);
    let task_b = manager.create("task b", "session-1", InteractionSurface::Cli);
    manager.complete(&task_a.id, &dispatcher);
    manager.complete(&task_b.id, &dispatcher);

    let drained = manager.drain_events_for_target("session-1", Some(&task_a.id));
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].task_id, task_a.id);
    assert_eq!(
        drained[0].target_task_id.as_deref(),
        Some(task_a.id.as_str())
    );
}

#[tokio::test]
async fn agent_tool_allows_owner_to_message_running_task() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools =
        rust_agent::tool::registry::ToolRegistry::new().register(Arc::new(AgentTool));
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-9")
        .with_inherited_tool_registry(inherited_tools)
        .with_subagent_scripted_turns(vec![
            vec![
                rust_agent::service::api::streaming::StreamEvent::MessageStart,
                rust_agent::service::api::streaming::StreamEvent::TextDelta(
                    "initial answer".into(),
                ),
                rust_agent::service::api::streaming::StreamEvent::MessageStop {
                    stop_reason: rust_agent::service::api::streaming::StopReason::EndTurn,
                },
            ],
            vec![
                rust_agent::service::api::streaming::StreamEvent::MessageStart,
                rust_agent::service::api::streaming::StreamEvent::TextDelta(
                    "continued answer".into(),
                ),
                rust_agent::service::api::streaming::StreamEvent::MessageStop {
                    stop_reason: rust_agent::service::api::streaming::StopReason::EndTurn,
                },
            ],
        ]);

    AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: "inspect repository".into(),
            },
            &permissions,
        )
        .await
        .expect("initial agent launch should succeed");

    let result = AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: "continue: task-0: follow-up".into(),
            },
            &permissions,
        )
        .await
        .expect("continue should succeed");

    assert_eq!(
        result,
        ToolResult::Text("agent task task-0 accepted message follow-up".into())
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let output = manager
                .get_output("task-0", 0)
                .expect("task output should exist");
            if output.content.contains("continued answer") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("continued worker should produce follow-up output");
}

#[tokio::test]
async fn task_stop_tool_allows_owner_and_rejects_non_owner() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("stoppable task", "session-owner", InteractionSurface::Cli);

    manager.launch(&task.id, "work", std::future::pending::<()>());

    let non_owner_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-other");
    let non_owner_error = TaskStopTool
        .invoke(
            &ToolCall {
                name: "TaskStop".into(),
                input: task.id.clone(),
            },
            &non_owner_permissions,
        )
        .await
        .expect_err("non-owner stop should fail");
    assert!(
        non_owner_error
            .to_string()
            .contains("not running or not owned by this session")
    );

    let owner_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-owner");
    let result = TaskStopTool
        .invoke(
            &ToolCall {
                name: "TaskStop".into(),
                input: task.id.clone(),
            },
            &owner_permissions,
        )
        .await
        .expect("owner stop should succeed");

    assert_eq!(
        result,
        ToolResult::Text(format!("task {} stopped", task.id))
    );
    assert_eq!(manager.get(&task.id).unwrap().status, TaskStatus::Killed);
    assert_eq!(dispatcher.delivered().len(), 0);
}

#[tokio::test]
async fn task_list_tools_follow_planning_model_and_runtime_output_still_works() {
    let manager = Arc::new(TaskManager::default());
    let task_list = Arc::new(rust_agent::task::list_manager::TaskListManager::default());
    let runtime_task = manager.create(
        "owned runtime task",
        "session-owner",
        InteractionSurface::Cli,
    );
    manager.append_output(&runtime_task.id, "owned output");

    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_task_list_manager(task_list.clone())
        .with_active_session_id("session-owner");

    let created = TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "plan task:write tests:Writing tests".into(),
            },
            &permissions,
        )
        .await
        .expect("task create should succeed");
    let ToolResult::Text(created_text) = created else {
        panic!("expected text result");
    };
    assert!(created_text.contains("subject: plan task"));

    let updated = TaskUpdateTool
        .invoke(
            &ToolCall {
                name: "TaskUpdate".into(),
                input: "task-0:renamed task:refined description:Refining:in_progress:session-owner"
                    .into(),
            },
            &permissions,
        )
        .await
        .expect("task update should succeed");
    let ToolResult::Text(updated_text) = updated else {
        panic!("expected text result");
    };
    assert!(updated_text.contains("subject: renamed task"));
    assert!(updated_text.contains("status: InProgress"));

    let list = TaskListTool
        .invoke(
            &ToolCall {
                name: "TaskList".into(),
                input: "ignored".into(),
            },
            &permissions,
        )
        .await
        .expect("task list should succeed");
    let ToolResult::Text(list_text) = list else {
        panic!("expected text result");
    };
    assert!(list_text.contains("subject: renamed task"));
    assert!(!list_text.contains("output_file:"));

    let get = TaskGetTool
        .invoke(
            &ToolCall {
                name: "TaskGet".into(),
                input: "task-0".into(),
            },
            &permissions,
        )
        .await
        .expect("task get should succeed");
    let ToolResult::Text(get_text) = get else {
        panic!("expected text result");
    };
    assert!(get_text.contains("active_form: Refining"));
    assert!(get_text.contains("owner: session-owner"));

    let output = TaskOutputTool
        .invoke(
            &ToolCall {
                name: "TaskOutput".into(),
                input: format!("{}:0", runtime_task.id),
            },
            &permissions,
        )
        .await
        .expect("runtime task output should still succeed");
    let ToolResult::Text(output_text) = output else {
        panic!("expected text result");
    };
    assert!(output_text.contains("content:\nowned output"));
}

#[tokio::test]
async fn send_message_tool_allows_owner_and_rejects_non_owner() {
    let manager = Arc::new(TaskManager::default());
    let task = manager.create("running task", "session-owner", InteractionSurface::Cli);
    manager.launch(&task.id, "work", std::future::pending::<()>());

    let owner_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-owner");
    let result = SendMessageTool
        .invoke(
            &ToolCall {
                name: "SendMessage".into(),
                input: format!("{}:follow-up", task.id),
            },
            &owner_permissions,
        )
        .await
        .expect("owner send should succeed");
    assert_eq!(
        result,
        ToolResult::Text(format!("task {} accepted message follow-up", task.id))
    );
    assert_eq!(
        manager.wait_for_mailbox_message(&task.id).await,
        Some("follow-up".into())
    );

    let other_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-other");
    let error = SendMessageTool
        .invoke(
            &ToolCall {
                name: "SendMessage".into(),
                input: format!("{}:nope", task.id),
            },
            &other_permissions,
        )
        .await
        .expect_err("non-owner send should fail");
    assert!(
        error
            .to_string()
            .contains("not running or not owned by this session")
    );
}

#[test]
fn task_output_reads_support_offsets() {
    let manager = TaskManager::default();
    let task = manager.create("offset task", "session-4", InteractionSurface::Cli);
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

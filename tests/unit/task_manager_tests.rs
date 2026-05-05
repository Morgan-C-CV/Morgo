use std::sync::Arc;

use async_trait::async_trait;
use rust_agent::bootstrap::InteractionSurface;
use rust_agent::history::session::{InMemorySessionStore, SessionId, SessionStore};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::NotificationTarget;
use rust_agent::interaction::telegram::binding::{SessionBinding, TelegramDeliveryTarget};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::WorkerRole;
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::send_message::SendMessageTool;
use rust_agent::tool::builtin::task_create::TaskCreateTool;
use rust_agent::tool::builtin::task_get::TaskGetTool;
use rust_agent::tool::builtin::task_list::TaskListTool;
use rust_agent::tool::builtin::task_output::TaskOutputTool;
use rust_agent::tool::builtin::task_stop::TaskStopTool;
use rust_agent::tool::builtin::task_update::TaskUpdateTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};
use rust_agent::tool::registry::ToolRegistry;
use std::sync::atomic::{AtomicU64, Ordering};

struct SafeTool {
    name: &'static str,
    aliases: &'static [&'static str],
}

#[async_trait]
impl Tool for SafeTool {
    fn metadata(&self) -> rust_agent::tool::definition::ToolMetadata {
        rust_agent::tool::definition::ToolMetadata {
            name: self.name,
            description: "safe test tool".into(),
            aliases: self.aliases,
            search_hint: None,
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: true,
        }
    }

    async fn invoke(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Text(format!("{} ok", self.name)))
    }
}

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
    assert_eq!(notification.body, "demo task (task-0) — completed");
    assert_eq!(
        dispatcher.delivered()[0].body,
        "demo task (task-0) — completed"
    );
    assert_eq!(notification.task_id.as_deref(), Some("task-0"));
    assert_eq!(notification.status.as_deref(), Some("completed"));
    assert_eq!(
        notification.next_action.as_deref(),
        Some("inspect task output for task-0")
    );
    assert_eq!(notification.worker_role, None);
    assert_eq!(notification.usage, None);
    assert_eq!(dispatcher.delivered().len(), 1);
}

#[tokio::test]
async fn task_manager_tracks_failed_and_killed_states() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let failed = manager.create("failing task", "session-2", InteractionSurface::Cli);
    manager.fail(&failed.id, &dispatcher);
    assert_eq!(manager.get(&failed.id).unwrap().status, TaskStatus::Failed);
    assert_eq!(manager.is_terminal(&failed.id), Some(true));
    let failed_notification = manager
        .get(&failed.id)
        .and_then(|task| task.delivery.notification)
        .expect("failed notification should exist");
    assert_eq!(failed_notification.title, "Task failed");
    assert_eq!(failed_notification.body, "failing task (task-0) — failed");

    let killed = manager.create("killed task", "session-3", InteractionSurface::Cli);
    manager.launch(&killed.id, "work", std::future::pending::<()>());
    assert!(manager.kill(&killed.id, "session-3", &dispatcher));
    let killed_task = manager.get(&killed.id).unwrap();
    assert_eq!(killed_task.status, TaskStatus::Killed);
    assert_eq!(manager.is_terminal(&killed.id), Some(true));
    let killed_notification = killed_task
        .delivery
        .notification
        .expect("killed notification should exist");
    assert_eq!(killed_notification.title, "Task killed");
    assert_eq!(killed_notification.body, "killed task (task-1) — killed");
}

#[test]
fn task_manager_updates_activity_tracker_on_runtime_progress() {
    let manager = TaskManager::default();
    let tracker = Arc::new(AtomicU64::new(1));
    manager.set_activity_tracker(tracker.clone());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("tracked task", "session-activity", InteractionSurface::Cli);

    manager.start(&task.id);
    let after_start = tracker.load(Ordering::Acquire);
    assert!(after_start >= 1);

    tracker.store(1, Ordering::Release);
    manager.append_output(&task.id, "background progress");
    assert!(tracker.load(Ordering::Acquire) > 1);

    tracker.store(1, Ordering::Release);
    manager.complete(&task.id, &dispatcher);
    assert!(tracker.load(Ordering::Acquire) > 1);
}

#[test]
fn telegram_task_notifications_resolve_session_target_to_delivery_target() {
    let manager = TaskManager::default();
    let task = manager.create_with_type(
        "telegram task",
        rust_agent::task::types::TaskType::LocalAgent,
        "telegram-session",
        InteractionSurface::Telegram,
    );
    manager.set_worker_role(&task.id, WorkerRole::Verify);
    let gateway = TelegramGateway::default().with_bindings(vec![SessionBinding {
        actor_id: "actor-1".into(),
        session_id: "telegram-session".into(),
        telegram_user_id: Some("user-1".into()),
        bot_id: Some("bot-1".into()),
        delivery_target: Some(TelegramDeliveryTarget {
            chat_id: "chat-1".into(),
            thread_id: Some("thread-9".into()),
        }),
    }]);
    let dispatcher = NotificationDispatcher::new(gateway);

    manager.complete(&task.id, &dispatcher);

    let delivered = dispatcher.delivered();
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0].target,
        Some(NotificationTarget::Telegram(TelegramDeliveryTarget {
            chat_id: "chat-1".into(),
            thread_id: Some("thread-9".into()),
        }))
    );
    assert_eq!(delivered[0].worker_role.as_deref(), Some("verify"));
    assert_eq!(
        delivered[0].next_action.as_deref(),
        Some("synthesize validated result for task-0")
    );
    assert_eq!(delivered[0].usage, None);
}

#[tokio::test]
async fn agent_tool_launches_subagent_and_completes_task() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools =
        rust_agent::tool::registry::ToolRegistry::new().register(Arc::new(AgentTool));
    let inherited_hooks = rust_agent::hook::registry::HookRegistry::default().register_rule(
        rust_agent::hook::registry::HookRule {
            event: rust_agent::hook::registry::HookEventMatcher::SubagentStop,
            layer: rust_agent::hook::registry::HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("shared hook message".into()),
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
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
                input: serde_json::json!({
                    "task": "inspect repository",
                    "role": "research"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("agent tool should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("agent task task-0 respawned for research worker: inspect repository"));

    tokio::time::timeout(std::time::Duration::from_secs(4), async {
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
    assert_eq!(created.worker_role, Some(WorkerRole::Research));
    let output = manager
        .get_output("task-0", 0)
        .expect("task output should exist");
    assert!(output.content.contains("subagent answer"));
    assert!(output.content.contains("shared hook message"));
    assert!(created.delivery.notified);
    let notification = created
        .delivery
        .notification
        .as_ref()
        .expect("notification should exist");
    assert_eq!(notification.session_id, "session-7");
    assert!(notification.body.contains("inspect repository"));
    assert!(notification.body.contains("worker completed"));
    assert!(notification.title.starts_with("Agent task completed"));
    assert_eq!(notification.worker_role.as_deref(), Some("research"));
    assert_eq!(
        notification.next_action.as_deref(),
        Some("synthesize findings or request follow-up research for task-0")
    );
    assert!(
        notification.usage.is_none() || notification.title.contains("requests="),
        "usage-bearing notifications should surface compact usage in the title"
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
            task_type: rust_agent::task::types::TaskType::Generic,
            status: TaskStatus::Completed,
            summary: format!("demo task ({}) — completed", task.id),
            result: "Task completed".into(),
            next_action: format!("inspect task output for {}", task.id),
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            step_id: None,
            output_file: task.output_file.clone(),
            usage: None,
        }
    );
    assert!(manager.drain_events("session-1").is_empty());
}

#[test]
fn task_type_changes_terminal_summary_result_and_next_action_semantics() {
    let manager = TaskManager::default();
    let usage = Some(rust_agent::task::types::TaskUsageSummary {
        requests: 1,
        input_tokens: 10,
        uncached_input_tokens: 10,
        output_tokens: 5,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        original_prompt_chars: 0,
        sent_prompt_chars: 0,
        cache_hit_requests: 0,
        estimated_cost_micros_usd: 42,
    });

    let generic_summary = rust_agent::task::types::format_task_summary(
        "demo task",
        "task-1",
        rust_agent::task::types::TaskType::Generic,
        &TaskStatus::Completed,
        usage.as_ref(),
    );
    let bash_summary = rust_agent::task::types::format_task_summary(
        "bash: ls",
        "task-2",
        rust_agent::task::types::TaskType::LocalBash,
        &TaskStatus::Completed,
        usage.as_ref(),
    );
    let agent_summary = rust_agent::task::types::format_task_summary(
        "Spawned research worker",
        "task-3",
        rust_agent::task::types::TaskType::LocalAgent,
        &TaskStatus::Completed,
        usage.as_ref(),
    );

    assert!(generic_summary.contains("demo task (task-1) — completed"));
    assert!(bash_summary.contains("bash: ls (task-2) — command completed"));
    assert!(agent_summary.contains("Spawned research worker (task-3) — worker completed"));

    let generic_result = rust_agent::task::types::format_task_result(
        rust_agent::task::types::TaskType::Generic,
        &TaskStatus::Completed,
        None,
        usage.as_ref(),
    );
    let bash_result = rust_agent::task::types::format_task_result(
        rust_agent::task::types::TaskType::LocalBash,
        &TaskStatus::Completed,
        None,
        usage.as_ref(),
    );
    let agent_result = rust_agent::task::types::format_task_result(
        rust_agent::task::types::TaskType::LocalAgent,
        &TaskStatus::Completed,
        None,
        usage.as_ref(),
    );

    assert!(generic_result.starts_with("Task completed — usage:"));
    assert!(bash_result.starts_with("Command completed — usage:"));
    assert!(agent_result.starts_with("Agent task completed — usage:"));

    let generic_task = manager.create("generic task", "session-1", InteractionSurface::Cli);
    let bash_task = manager.create_with_type(
        "bash: ls",
        rust_agent::task::types::TaskType::LocalBash,
        "session-1",
        InteractionSurface::Cli,
    );
    let agent_task = manager.create_with_type(
        "research worker",
        rust_agent::task::types::TaskType::LocalAgent,
        "session-1",
        InteractionSurface::Cli,
    );
    manager.set_worker_role(&agent_task.id, WorkerRole::Research);
    manager.complete(
        &agent_task.id,
        &NotificationDispatcher::new(TelegramGateway::default()),
    );
    let completed_agent_task = manager
        .get(&agent_task.id)
        .expect("agent task should exist");

    let generic_hint = manager.task_hint(&generic_task);
    let bash_hint = manager.task_hint(&bash_task);
    let agent_hint = manager.task_hint(&completed_agent_task);

    assert_eq!(
        generic_hint,
        format!("inspect task output for {}", generic_task.id)
    );
    assert_eq!(
        bash_hint,
        format!("inspect command output for {}", bash_task.id)
    );
    assert_eq!(
        agent_hint,
        format!(
            "synthesize findings or request follow-up research for {}",
            agent_task.id
        )
    );
}

#[test]
fn grouped_task_type_semantics_drive_group_hint_and_fan_in() {
    let manager = TaskManager::default();

    let bash_a = manager.create_with_type(
        "bash: one",
        rust_agent::task::types::TaskType::LocalBash,
        "session-1",
        InteractionSurface::Cli,
    );
    let bash_b = manager.create_with_type(
        "bash: two",
        rust_agent::task::types::TaskType::LocalBash,
        "session-1",
        InteractionSurface::Cli,
    );
    manager.set_orchestration_group_id(&bash_a.id, Some("group-bash".into()));
    manager.set_orchestration_group_id(&bash_b.id, Some("group-bash".into()));
    manager.start(&bash_a.id);
    manager.start(&bash_b.id);
    manager.set_phase(
        &bash_a.id,
        Some(rust_agent::task::types::WorkerPhase::Research),
    );
    manager.fail(
        &bash_a.id,
        &NotificationDispatcher::new(TelegramGateway::default()),
    );
    manager.complete(
        &bash_b.id,
        &NotificationDispatcher::new(TelegramGateway::default()),
    );

    let bash_group = manager
        .group_summary("group-bash")
        .expect("bash group exists");
    assert_eq!(
        bash_group.hint,
        "group group-bash is ready for command-result review"
    );
    assert!(manager.group_ready_for_fan_in("group-bash"));

    let agent = manager.create_with_type(
        "implement patch",
        rust_agent::task::types::TaskType::LocalAgent,
        "session-1",
        InteractionSurface::Cli,
    );
    manager.set_orchestration_group_id(&agent.id, Some("group-agent".into()));
    manager.set_worker_role(&agent.id, WorkerRole::Implement);
    manager.set_validation_state(
        &agent.id,
        Some(rust_agent::task::types::ValidationState::PendingVerification),
    );
    manager.complete(
        &agent.id,
        &NotificationDispatcher::new(TelegramGateway::default()),
    );

    let agent_group = manager
        .group_summary("group-agent")
        .expect("agent group exists");
    assert_eq!(
        agent_group.hint,
        "group group-agent is waiting for verification"
    );
    assert!(!manager.group_ready_for_fan_in("group-agent"));
}

#[tokio::test]
async fn non_owner_cannot_kill_running_task() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("running task", "session-owner", InteractionSurface::Cli);

    manager.launch(&task.id, "inspect", std::future::pending::<()>());

    assert_eq!(
        manager.running_owner(&task.id),
        Some(TaskOwner {
            session_id: "session-owner".into(),
            surface: InteractionSurface::Cli,
        })
    );
    assert!(!manager.kill(&task.id, "session-other", &dispatcher));
    assert_eq!(manager.get(&task.id).unwrap().status, TaskStatus::Running);
    assert_eq!(manager.is_terminal(&task.id), Some(false));
    assert!(
        manager
            .get(&task.id)
            .unwrap()
            .delivery
            .notification
            .is_none()
    );
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

    tokio::time::timeout(std::time::Duration::from_secs(4), async {
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
async fn agent_tool_reuses_running_research_worker_and_respawns_terminal_worker() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools =
        rust_agent::tool::registry::ToolRegistry::new().register(Arc::new(AgentTool));
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-10")
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
                    "replacement answer".into(),
                ),
                rust_agent::service::api::streaming::StreamEvent::MessageStop {
                    stop_reason: rust_agent::service::api::streaming::StopReason::EndTurn,
                },
            ],
        ]);

    let first = AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: serde_json::json!({
                    "task": "inspect repository",
                    "role": "research"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("initial spawn should succeed");
    assert_eq!(
        first,
        ToolResult::Text(
            "agent task task-0 respawned for research worker: inspect repository".into()
        )
    );

    let reused = AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: serde_json::json!({
                    "task": "inspect repository",
                    "role": "research"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("running worker reuse should succeed");
    assert_eq!(
        reused,
        ToolResult::Text("agent task task-0 reused for research worker: inspect repository".into())
    );

    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;

    let respawned = AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: serde_json::json!({
                    "task": "inspect repository",
                    "role": "research"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("terminal worker should respawn");
    assert_eq!(
        respawned,
        ToolResult::Text(
            "agent task task-1 respawned for research worker: inspect repository".into()
        )
    );

    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        loop {
            let replacement = manager
                .get("task-1")
                .expect("replacement task should exist");
            if replacement.status == TaskStatus::Completed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacement task should complete");
}

#[tokio::test]
async fn agent_tool_records_runtime_tool_execution_records_for_spawned_worker() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools = rust_agent::tool::registry::ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(FileReadTool));
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-tool-records")
        .with_inherited_tool_registry(inherited_tools)
        .with_subagent_scripted_turns(vec![
            vec![
                rust_agent::service::api::streaming::StreamEvent::MessageStart,
                rust_agent::service::api::streaming::StreamEvent::ToolUse {
                    tool_name: "Read".into(),
                    input: r#"{"path":"src/task/manager.rs"}"#.into(),
                },
                rust_agent::service::api::streaming::StreamEvent::MessageStop {
                    stop_reason: rust_agent::service::api::streaming::StopReason::ToolUse,
                },
            ],
            vec![
                rust_agent::service::api::streaming::StreamEvent::MessageStart,
                rust_agent::service::api::streaming::StreamEvent::TextDelta("done".into()),
                rust_agent::service::api::streaming::StreamEvent::MessageStop {
                    stop_reason: rust_agent::service::api::streaming::StopReason::EndTurn,
                },
            ],
        ]);

    AgentTool
        .invoke(
            &ToolCall {
                name: "Agent".into(),
                input: serde_json::json!({
                    "task": "inspect src/task/manager.rs",
                    "role": "research"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("spawn should succeed");

    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        loop {
            let task = manager.get("task-0").expect("task should exist");
            if task.status == TaskStatus::Completed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker should finish");

    let records = manager.tool_execution_records("task-0");
    assert!(records.iter().any(|record| {
        record.tool_name == "Read"
            && record
                .observable_input
                .as_ref()
                .map(|input| input.value.contains("src/task/manager.rs"))
                .unwrap_or(false)
    }));
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

    tokio::time::timeout(std::time::Duration::from_secs(4), async {
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
async fn agent_tool_respects_allowed_tools_and_max_turns() {
    let manager = Arc::new(TaskManager::default());
    let inherited_tools = ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(SafeTool {
            name: "Read",
            aliases: &["FileRead"],
        }))
        .register(Arc::new(SafeTool {
            name: "Write",
            aliases: &[],
        }));
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-12")
        .with_inherited_tool_registry(inherited_tools)
        .with_subagent_scripted_turns(vec![
            vec![
                rust_agent::service::api::streaming::StreamEvent::MessageStart,
                rust_agent::service::api::streaming::StreamEvent::TextDelta(
                    "first bounded answer".into(),
                ),
                rust_agent::service::api::streaming::StreamEvent::MessageStop {
                    stop_reason: rust_agent::service::api::streaming::StopReason::MaxTokens,
                },
            ],
            vec![
                rust_agent::service::api::streaming::StreamEvent::MessageStart,
                rust_agent::service::api::streaming::StreamEvent::TextDelta(
                    "second bounded answer".into(),
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
                input: serde_json::json!({
                    "task": "bounded inspection",
                    "role": "verify",
                    "max_turns": 1,
                    "allowed_tools": ["Read"]
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("bounded agent launch should succeed");

    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        loop {
            let created = manager.get("task-0").expect("task should be created");
            if matches!(created.status, TaskStatus::Failed) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded worker should finish");

    let created = manager.get("task-0").expect("task should be created");
    assert_eq!(created.worker_role, Some(WorkerRole::Verify));
    assert_eq!(created.status, TaskStatus::Failed);

    let output = manager
        .get_output("task-0", 0)
        .expect("task output should exist");
    assert!(
        output.content.contains("verified_target:"),
        "unexpected output: {}",
        output.content
    );
    assert!(
        output.content.contains("verification_result: verified"),
        "unexpected output: {}",
        output.content
    );
    assert!(
        output.content.contains("minimal_evidence:"),
        "unexpected output: {}",
        output.content
    );
    assert!(
        output.content.contains("remaining_blocker: none"),
        "unexpected output: {}",
        output.content
    );
    assert!(
        !output.content.contains("second bounded answer"),
        "unexpected output: {}",
        output.content
    );

    let notification = created
        .delivery
        .notification
        .as_ref()
        .expect("notification should exist");
    assert_eq!(notification.worker_role.as_deref(), Some("verify"));

    let worker_tools = permissions
        .inherited_tool_registry
        .as_ref()
        .expect("inherited tool registry should exist")
        .assemble_worker_registry(Some(&["Read".to_string()]))
        .visible_tools(&permissions)
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert_eq!(worker_tools, vec!["Read"]);
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
    let stored = manager.get(&task.id).unwrap();
    assert_eq!(stored.status, TaskStatus::Killed);
    assert!(stored.delivery.notification.is_some());
    assert_eq!(
        stored.delivery.notification.as_ref().unwrap().title,
        "Task killed"
    );
    assert_eq!(dispatcher.delivered().len(), 0);

    let second_stop_error = TaskStopTool
        .invoke(
            &ToolCall {
                name: "TaskStop".into(),
                input: task.id.clone(),
            },
            &owner_permissions,
        )
        .await
        .expect_err("second stop should fail once task is terminal");
    assert!(
        second_stop_error
            .to_string()
            .contains("not running or not owned by this session")
    );
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

    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "plan task:write tests:Writing tests".into(),
            },
            &permissions,
        )
        .await
        .expect("task create should succeed");
    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "blocked task:wait for tests:Waiting".into(),
            },
            &permissions,
        )
        .await
        .expect("second task create should succeed");

    let updated = TaskUpdateTool
        .invoke(
            &ToolCall {
                name: "TaskUpdate".into(),
                input: "task-0:renamed task:refined description:Refining:in_progress:session-owner:task-1:-"
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

    let get = TaskGetTool
        .invoke(
            &ToolCall {
                name: "TaskGet".into(),
                input: "task-1".into(),
            },
            &permissions,
        )
        .await
        .expect("task get should succeed");
    let ToolResult::Text(get_text) = get else {
        panic!("expected text result");
    };
    assert!(get_text.contains("blocked_by: task-0"));
    assert!(get_text.contains("blocks: "));

    let list_before_completion = TaskListTool
        .invoke(
            &ToolCall {
                name: "TaskList".into(),
                input: "ignored".into(),
            },
            &permissions,
        )
        .await
        .expect("task list should succeed before completion");
    let ToolResult::Text(list_before_completion_text) = list_before_completion else {
        panic!("expected text result");
    };
    assert!(list_before_completion_text.contains("subject: renamed task"));
    assert!(list_before_completion_text.contains("blocked_by: task-0"));
    assert!(!list_before_completion_text.contains("output_file:"));

    TaskUpdateTool
        .invoke(
            &ToolCall {
                name: "TaskUpdate".into(),
                input: "task-0:-:-:-:completed:-:-:-".into(),
            },
            &permissions,
        )
        .await
        .expect("task completion update should succeed");

    let list_after_completion = TaskListTool
        .invoke(
            &ToolCall {
                name: "TaskList".into(),
                input: "ignored".into(),
            },
            &permissions,
        )
        .await
        .expect("task list should succeed after completion");
    let ToolResult::Text(list_after_completion_text) = list_after_completion else {
        panic!("expected text result");
    };
    assert!(list_after_completion_text.contains("subject: renamed task"));
    assert!(list_after_completion_text.contains("id: task-1\nsubject: blocked task"));
    assert!(list_after_completion_text.contains("blocked_by: \nblocks: "));

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
async fn task_update_adds_reciprocal_dependencies_without_duplicates() {
    let task_list = Arc::new(rust_agent::task::list_manager::TaskListManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_list_manager(task_list)
        .with_active_session_id("session-owner");

    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "task a:alpha:-".into(),
            },
            &permissions,
        )
        .await
        .expect("create task a");
    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "task b:beta:-".into(),
            },
            &permissions,
        )
        .await
        .expect("create task b");
    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "task c:gamma:-".into(),
            },
            &permissions,
        )
        .await
        .expect("create task c");

    TaskUpdateTool
        .invoke(
            &ToolCall {
                name: "TaskUpdate".into(),
                input: "task-0:-:-:-:-:-:task-1:task-2".into(),
            },
            &permissions,
        )
        .await
        .expect("add dependency edges");
    TaskUpdateTool
        .invoke(
            &ToolCall {
                name: "TaskUpdate".into(),
                input: "task-0:-:-:-:-:-:task-1:task-2".into(),
            },
            &permissions,
        )
        .await
        .expect("duplicate dependency edges should be ignored");

    let task_a = TaskGetTool
        .invoke(
            &ToolCall {
                name: "TaskGet".into(),
                input: "task-0".into(),
            },
            &permissions,
        )
        .await
        .expect("get task a");
    let ToolResult::Text(task_a_text) = task_a else {
        panic!("expected text result");
    };
    assert!(task_a_text.contains("blocks: task-1"));
    assert!(task_a_text.contains("blocked_by: task-2"));

    let task_b = TaskGetTool
        .invoke(
            &ToolCall {
                name: "TaskGet".into(),
                input: "task-1".into(),
            },
            &permissions,
        )
        .await
        .expect("get task b");
    let ToolResult::Text(task_b_text) = task_b else {
        panic!("expected text result");
    };
    assert!(task_b_text.contains("blocked_by: task-0"));
    assert!(task_b_text.matches("task-0").count() >= 1);

    let task_c = TaskGetTool
        .invoke(
            &ToolCall {
                name: "TaskGet".into(),
                input: "task-2".into(),
            },
            &permissions,
        )
        .await
        .expect("get task c");
    let ToolResult::Text(task_c_text) = task_c else {
        panic!("expected text result");
    };
    assert!(task_c_text.contains("blocks: task-0"));
    assert!(task_c_text.contains("blocked_by: "));
}

#[tokio::test]
async fn task_list_persistence_round_trips_snapshot_and_next_id() {
    let session_store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("session-owner".into());
    let task_list = Arc::new(
        rust_agent::task::list_manager::TaskListManager::default()
            .with_persistence(session_store.clone(), session_id.clone()),
    );
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_list_manager(task_list.clone())
        .with_active_session_id("session-owner");

    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "task a:alpha:Doing alpha".into(),
            },
            &permissions,
        )
        .await
        .expect("create task a");
    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "task b:beta:Doing beta".into(),
            },
            &permissions,
        )
        .await
        .expect("create task b");
    TaskUpdateTool
        .invoke(
            &ToolCall {
                name: "TaskUpdate".into(),
                input: "task-0:-:-:-:completed:-:task-1:-".into(),
            },
            &permissions,
        )
        .await
        .expect("persist dependency and status update");

    let persisted = session_store
        .load_task_list(&session_id)
        .expect("task list snapshot should persist");
    assert_eq!(persisted.next_id, 2);
    assert_eq!(persisted.tasks.len(), 2);

    let restored = rust_agent::task::list_manager::TaskListManager::from_snapshot(persisted)
        .with_persistence(session_store.clone(), session_id.clone());
    let restored_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_list_manager(Arc::new(restored))
        .with_active_session_id("session-owner");

    let restored_task = TaskGetTool
        .invoke(
            &ToolCall {
                name: "TaskGet".into(),
                input: "task-1".into(),
            },
            &restored_permissions,
        )
        .await
        .expect("restored task should be readable");
    let ToolResult::Text(restored_task_text) = restored_task else {
        panic!("expected text result");
    };
    assert!(restored_task_text.contains("blocked_by: task-0"));

    TaskCreateTool
        .invoke(
            &ToolCall {
                name: "TaskCreate".into(),
                input: "task c:gamma:Doing gamma".into(),
            },
            &restored_permissions,
        )
        .await
        .expect("create task c after restore");

    let restored_snapshot = session_store
        .load_task_list(&session_id)
        .expect("updated task list snapshot should persist");
    assert_eq!(restored_snapshot.next_id, 3);
    assert!(
        restored_snapshot
            .tasks
            .iter()
            .any(|task| task.id == "task-2")
    );
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_root = std::env::temp_dir().join(format!("rust-agent-test-{now}"));
    let manager = TaskManager::new_with_output_root(&temp_root);
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

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_root);
}

#[test]
fn events_ring_buffer_drops_oldest_when_full() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    // Create and complete 257 tasks — one more than MAX_QUEUED_EVENTS (256).
    let mut first_task_id = String::new();
    for i in 0..257 {
        let task = manager.create(format!("task {i}"), "session-ring", InteractionSurface::Cli);
        if i == 0 {
            first_task_id = task.id.clone();
        }
        manager.complete(&task.id, &dispatcher);
    }

    let all_events = manager.drain_events_for_target("session-ring", None);
    // Must not exceed the cap.
    assert!(
        all_events.len() <= 256,
        "expected at most 256 events, got {}",
        all_events.len()
    );
    // The very first task's event should have been evicted.
    assert!(
        !all_events.iter().any(|e| e.task_id == first_task_id),
        "oldest event should have been dropped"
    );
}

#[test]
fn events_drain_clears_matched_events_regression() {
    let manager = TaskManager::default();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task_a = manager.create("task a", "session-drain", InteractionSurface::Cli);
    let task_b = manager.create("task b", "session-drain", InteractionSurface::Cli);
    manager.complete(&task_a.id, &dispatcher);
    manager.complete(&task_b.id, &dispatcher);

    let drained = manager.drain_events_for_target("session-drain", None);
    assert_eq!(drained.len(), 2);
    // Second drain should be empty — events were consumed.
    let second_drain = manager.drain_events_for_target("session-drain", None);
    assert_eq!(second_drain.len(), 0);
}

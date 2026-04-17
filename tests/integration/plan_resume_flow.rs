use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{InteractionSurface, SessionMode};
use rust_agent::history::session::{
    FileBackedSessionStore, SessionHistory, SessionId, SessionSnapshot, SessionStore,
};
use rust_agent::plan::manager::PlanManager;
use rust_agent::state::app_state::WorkerRole;
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::list_manager::TaskListManager;
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{ValidationState, WorkerPhase};

fn unique_temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn approved_plan_runtime_overlay_updates_with_live_orchestration_state() {
    let plan_manager = Arc::new(PlanManager::default());
    plan_manager.ensure_draft(None);
    plan_manager.set_summary("Execute live runtime plan");
    let first = plan_manager
        .add_step("Inspect current state", Some("verify live runtime overlay"))
        .expect("add first step");
    let second = plan_manager
        .add_step(
            "Execute task linkage",
            Some("materialize runtime orchestration"),
        )
        .expect("add second step");

    let task_list_manager = Arc::new(TaskListManager::default());
    task_list_manager.create(
        "Inspect current state",
        "verify live runtime overlay",
        None,
        None,
        Some(first.id.clone()),
    );
    task_list_manager.create(
        "Execute task linkage",
        "materialize runtime orchestration",
        None,
        None,
        Some(second.id.clone()),
    );

    let runtime_tasks = Arc::new(TaskManager::default());
    let runtime_task = runtime_tasks.create(
        "execute linked runtime work",
        "plan-live-session",
        InteractionSurface::Cli,
    );
    runtime_tasks.set_orchestration_group_id(&runtime_task.id, Some(second.id.clone()));
    runtime_tasks.set_worker_role(&runtime_task.id, WorkerRole::Implement);
    runtime_tasks.set_phase(&runtime_task.id, Some(WorkerPhase::Implement));
    runtime_tasks
        .set_validation_state(&runtime_task.id, Some(ValidationState::PendingVerification));
    runtime_tasks.start(&runtime_task.id);

    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(runtime_tasks.clone())
        .with_task_list_manager(task_list_manager)
        .with_plan_manager(plan_manager.clone());
    rust_agent::state::plan_mode::apply_exit_plan_mode(&permissions, "ready to execute")
        .expect("approve plan");

    let running = rust_agent::state::plan_mode::render_plan_show(&permissions);
    assert!(running.contains("Runtime orchestration: groups=1, waiting_for_verification=0, ready_for_synthesis=0, still_in_progress=1"));
    assert!(running.contains(&format!(
        "runtime group: {} — group {} still in progress",
        second.id, second.id
    )));
    assert!(running.contains("runtime task: task-0 [Running] role=implement phase=implement validation_state=pending_verification"));

    runtime_tasks.set_phase(&runtime_task.id, Some(WorkerPhase::Verify));
    runtime_tasks.set_validation_state(&runtime_task.id, Some(ValidationState::Verified));
    runtime_tasks.complete(
        &runtime_task.id,
        &rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
    );

    let completed = rust_agent::state::plan_mode::render_plan_show(&permissions);
    assert!(completed.contains("Runtime orchestration: groups=1, waiting_for_verification=0, ready_for_synthesis=0, still_in_progress=0"));
    assert!(completed.contains(&format!(
        "runtime group: {} — group {} is ready for inspection",
        second.id, second.id
    )));
    assert!(completed.contains(
        "runtime task: task-0 [Completed] role=implement phase=verify validation_state=verified"
    ));

    let history = rust_agent::state::plan_mode::render_plan_history(&permissions);
    assert!(history.contains("Current runtime overlay:"));
    assert!(history.contains("ready_for_synthesis_groups=0"));
}

#[test]
fn approved_plan_reorder_and_task_binding_survive_restore() {
    let root = unique_temp_path("rust-agent-plan-resume");
    let store = Arc::new(FileBackedSessionStore::new(root.clone()));
    let session_id = SessionId("plan-resume-session".into());
    store.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/plan-resume".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );

    let task_list_manager =
        Arc::new(TaskListManager::default().with_persistence(store.clone(), session_id.clone()));
    let plan_manager =
        Arc::new(PlanManager::default().with_persistence(store.clone(), session_id.clone()));
    let permissions = ToolPermissionContext::new(PermissionMode::Plan)
        .with_task_list_manager(task_list_manager.clone())
        .with_plan_manager(plan_manager.clone());

    plan_manager.ensure_draft(None);
    plan_manager.set_summary("Execute durable plan");
    let first = plan_manager
        .add_step("Inspect current state", Some("verify persisted ordering"))
        .expect("add first step");
    let second = plan_manager
        .add_step(
            "Execute task linkage",
            Some("materialize linked task list items"),
        )
        .expect("add second step");
    rust_agent::state::plan_mode::reorder_plan_steps(
        &permissions,
        &[second.id.clone(), first.id.clone()],
    )
    .expect("reorder approved plan steps");
    rust_agent::state::plan_mode::apply_exit_plan_mode(&permissions, "ready to resume")
        .expect("approve plan and sync tasks");

    let synced_tasks = task_list_manager.list();
    assert_eq!(synced_tasks.len(), 2);
    assert_eq!(
        synced_tasks[0].plan_step_id.as_deref(),
        Some(second.id.as_str())
    );
    assert_eq!(
        synced_tasks[1].plan_step_id.as_deref(),
        Some(first.id.as_str())
    );
    assert_eq!(synced_tasks[0].subject, "Execute task linkage");
    assert_eq!(synced_tasks[1].subject, "Inspect current state");
    assert_eq!(synced_tasks[0].status, rust_agent::task::list_types::TaskListStatus::InProgress);
    assert_eq!(synced_tasks[1].status, rust_agent::task::list_types::TaskListStatus::Pending);
    assert!(synced_tasks[0].blocked_by.is_empty());
    assert_eq!(synced_tasks[0].blocks, vec![synced_tasks[1].id.clone()]);
    assert_eq!(synced_tasks[1].blocked_by, vec![synced_tasks[0].id.clone()]);

    let restored_plan = store
        .load_plan_state(&session_id)
        .expect("plan state should persist");
    let restored_steps = &restored_plan
        .draft
        .as_ref()
        .expect("draft should exist")
        .steps;
    assert_eq!(restored_steps[0].id, second.id);
    assert_eq!(restored_steps[1].id, first.id);

    let restored_tasks = store
        .load_task_list(&session_id)
        .expect("task list should persist");
    assert_eq!(restored_tasks.tasks.len(), 2);
    assert_eq!(
        restored_tasks.tasks[0].plan_step_id.as_deref(),
        Some(second.id.as_str())
    );
    assert_eq!(
        restored_tasks.tasks[1].plan_step_id.as_deref(),
        Some(first.id.as_str())
    );

    let rehydrated_task_list = TaskListManager::from_snapshot(restored_tasks.clone());
    let rehydrated_plan = PlanManager::from_state(restored_plan.clone());
    let restored_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_list_manager(Arc::new(rehydrated_task_list))
        .with_plan_manager(Arc::new(rehydrated_plan));
    let rendered = rust_agent::state::plan_mode::render_plan_show(&restored_permissions);
    assert!(rendered.contains("Plan status: approved"));
    assert!(rendered.contains(
        "Step summary: total=2, completed=0, in_progress=1, pending=1, linked=2, unlinked=0"
    ));
    assert!(rendered.contains("Active step: step-2"));
    assert!(rendered.contains(&second.id));
    assert!(rendered.contains(&first.id));
    assert!(rendered.contains("linked task: task-0 [in_progress]"));
    assert!(rendered.contains("linked task: task-1 [pending]"));

    std::fs::remove_dir_all(root).expect("cleanup durable plan resume store");
}

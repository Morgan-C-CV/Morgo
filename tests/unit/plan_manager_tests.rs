use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{InteractionSurface, SessionMode};
use rust_agent::history::session::{
    FileBackedSessionStore, InMemorySessionStore, SessionHistory, SessionId, SessionSnapshot,
    SessionStore,
};
use rust_agent::plan::manager::PlanManager;
use rust_agent::plan::types::{PlanState, PlanStatus, PlanStepStatus};
use rust_agent::task::list_manager::{TaskListManager, TaskListUpdate};
use rust_agent::task::list_types::TaskListStatus;

fn unique_temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn plan_manager_supports_step_crud_and_history() {
    let manager = PlanManager::default();
    manager.ensure_draft(Some("draft work"));
    manager.set_summary("Ship planning v2");

    let first = manager
        .add_step("Inspect types", Some("start with plan/types.rs"))
        .expect("add first step");
    let second = manager
        .add_step("Wire commands", Some("extend /plan command"))
        .expect("add second step");

    manager
        .update_step(
            &first.id,
            Some("Inspect plan types"),
            Some(Some("cover state + history")),
            Some(PlanStepStatus::InProgress),
        )
        .expect("update step");
    manager
        .mark_step_status(&first.id, PlanStepStatus::Completed)
        .expect("complete first step");
    manager
        .reorder_steps(&[second.id.clone(), first.id.clone()])
        .expect("reorder steps");
    manager.remove_step(&second.id).expect("remove second step");

    let state = manager.state().expect("plan state should exist");
    let draft = state.draft.expect("draft should exist");
    assert_eq!(draft.summary, "Ship planning v2");
    assert_eq!(draft.steps.len(), 1);
    assert_eq!(draft.steps[0].id, first.id);
    assert_eq!(draft.steps[0].status, PlanStepStatus::Completed);
    assert!(matches!(state.status, PlanStatus::Completed));
    assert!(state.history.len() >= 6);
}

#[test]
fn approve_preserves_execution_contract_and_progress() {
    let manager = PlanManager::default();
    manager.ensure_draft(None);
    manager.set_summary("Implement planning");
    let first = manager.add_step("Types", None).expect("add step");
    let second = manager.add_step("Commands", None).expect("add step");
    manager
        .mark_step_status(&first.id, PlanStepStatus::Completed)
        .expect("complete first step");
    manager
        .mark_step_status(&second.id, PlanStepStatus::InProgress)
        .expect("mark second in progress");

    let approved = manager
        .approve(Some("ready to execute"))
        .expect("approve plan");
    assert_eq!(approved.status, PlanStatus::Approved);
    let execution = approved.execution.expect("execution should be retained");
    assert_eq!(execution.completed_steps, 1);
    assert_eq!(execution.total_steps, 2);
    assert_eq!(execution.progress_percent, 50);
    assert_eq!(
        execution.active_step_id.as_deref(),
        Some(second.id.as_str())
    );
    assert_eq!(
        approved.approval_summary.as_deref(),
        Some("ready to execute")
    );
}

#[test]
fn persisted_plan_state_round_trips_with_history() {
    let store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("plan-session".into());
    store.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/plan".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );

    let manager = PlanManager::default().with_persistence(store.clone(), session_id.clone());
    manager.ensure_draft(Some("persist me"));
    manager.set_summary("Persistent plan");
    let step = manager.add_step("Write tests", None).expect("add step");
    manager
        .mark_step_status(&step.id, PlanStepStatus::Completed)
        .expect("complete step");
    manager
        .approve(Some("saved"))
        .expect("approve persisted plan");

    let restored = store
        .load_plan_state(&session_id)
        .expect("plan state should persist");
    assert_eq!(
        restored.draft.as_ref().expect("draft").summary,
        "Persistent plan"
    );
    assert!(!restored.history.is_empty());
}

#[test]
fn task_list_reconciliation_updates_plan_execution_view() {
    let manager = PlanManager::default();
    manager.ensure_draft(None);
    manager.set_summary("Reconcile linked tasks");
    let first = manager.add_step("Inspect", None).expect("add first step");
    let second = manager.add_step("Patch", None).expect("add second step");
    let approved = manager.approve(Some("execute")).expect("approve plan");

    let task_list = TaskListManager::default();
    let first_task = task_list.create(
        "Inspect",
        "inspect repo",
        None,
        None,
        Some(first.id.clone()),
    );
    let second_task = task_list.create("Patch", "patch repo", None, None, Some(second.id.clone()));
    task_list
        .update(
            &first_task.id,
            TaskListUpdate {
                status: Some(TaskListStatus::Completed),
                ..Default::default()
            },
        )
        .expect("complete first linked task");
    task_list
        .update(
            &second_task.id,
            TaskListUpdate {
                status: Some(TaskListStatus::InProgress),
                ..Default::default()
            },
        )
        .expect("start second linked task");

    let reconciled = task_list
        .reconcile_plan_state(&approved)
        .expect("reconciled plan should change");
    let draft = reconciled.draft.expect("draft should exist");
    assert_eq!(draft.steps[0].status, PlanStepStatus::Completed);
    assert_eq!(draft.steps[1].status, PlanStepStatus::InProgress);
    let execution = reconciled.execution.expect("execution should exist");
    assert_eq!(execution.completed_steps, 1);
    assert_eq!(execution.total_steps, 2);
    assert_eq!(execution.progress_percent, 50);
    assert_eq!(
        execution.active_step_id.as_deref(),
        Some(second.id.as_str())
    );
}

#[test]
fn file_backed_plan_state_survives_new_store_instance() {
    let root = unique_temp_path("rust-agent-plan-store");
    let store_a = Arc::new(FileBackedSessionStore::new(root.clone()));
    let session_id = SessionId("durable-plan".into());
    store_a.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/durable-plan".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );

    let manager = PlanManager::default().with_persistence(store_a.clone(), session_id.clone());
    manager.ensure_draft(None);
    manager.set_summary("Durable plan");
    let step = manager
        .add_step("Resume me", Some("persist across instances"))
        .expect("add step");
    manager
        .mark_step_status(&step.id, PlanStepStatus::Completed)
        .expect("complete durable step");

    let store_b = FileBackedSessionStore::new(root.clone());
    let restored: PlanState = store_b
        .load_plan_state(&session_id)
        .expect("durable plan state should load");
    assert_eq!(
        restored.draft.as_ref().expect("draft").summary,
        "Durable plan"
    );
    assert_eq!(restored.draft.as_ref().expect("draft").steps.len(), 1);
    assert!(!restored.history.is_empty());

    std::fs::remove_dir_all(root).expect("cleanup durable plan store");
}

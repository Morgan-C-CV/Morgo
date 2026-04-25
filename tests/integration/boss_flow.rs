use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::boss::{BossCoordinator, save_plan};
use rust_agent::core::boss_state::{
    BossActorRole, BossActorStatus, BossActorHandle, BossPlan, BossPlanStep, BossPlanStepStatus,
    BossStage,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole, WorkerRole,
};
use rust_agent::state::permission_context::{
    BossActorPolicy, PermissionMode, ToolPermissionContext,
};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus, TaskType};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::definition::{Tool, ToolCall};
use rust_agent::tool::registry::{ToolAssemblyContext, ToolRegistry};
use tokio::sync::RwLock;

fn boss_step(id: usize, description: &str) -> BossPlanStep {
    BossPlanStep {
        id,
        description: description.into(),
        objective: Some(format!("objective {id}")),
        acceptance: vec![format!("acceptance {id}")],
        requires_approval: false,
        status: BossPlanStepStatus::Pending,
        completed: false,
        result_diff: None,
        worker_task_id: None,
    }
}

fn boss_plan(steps: Vec<BossPlanStep>) -> BossPlan {
    BossPlan {
        plan_id: "plan-alpha".into(),
        task_description: "Multi-step task".into(),
        steps,
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

fn app_state(active_session_id: &str) -> Arc<AppState> {
    app_state_with_tasks(active_session_id, Arc::new(TaskManager::default()))
}

fn app_state_with_tasks(active_session_id: &str, task_manager: Arc<TaskManager>) -> Arc<AppState> {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager)
        .with_active_session_id(active_session_id)
        .with_active_surface(InteractionSurface::Cli);
    Arc::new(AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: active_session_id.into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
    })
}

fn task_event(task_id: &str, step_id: usize, status: TaskStatus) -> TaskEvent {
    TaskEvent {
        task_id: task_id.into(),
        task_type: TaskType::LocalAgent,
        status,
        step_id: Some(step_id),
        owner: TaskOwner {
            session_id: "test-session".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some(task_id.into()),
        summary: format!("{task_id} summary"),
        result: format!("{task_id} result"),
        next_action: "None".into(),
        worker_role: Some(WorkerRole::Implement),
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    }
}

async fn coordinator_with_plan(
    plan: BossPlan,
    file_name: &str,
) -> (Arc<BossCoordinator>, std::path::PathBuf) {
    let plan_path = std::env::temp_dir().join(file_name);
    save_plan(&plan, &plan_path).await.unwrap();
    let coordinator = Arc::new(BossCoordinator::restore_or_init(&plan_path).await.unwrap());
    (coordinator, plan_path)
}

#[tokio::test]
async fn boss_auto_advances_to_next_step_after_completion() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                worker_task_id: Some("worker-task-0".into()),
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
        ]),
        "test_boss_flow_auto_advance.json",
    )
    .await;

    assert_eq!(coordinator.get_stage().await, BossStage::Execution);
    let payload = coordinator
        .advance_plan(&app_state("parent-session-1"))
        .await
        .unwrap()
        .expect("next step should dispatch");

    assert!(payload.contains("\"boss_plan_id\":\"plan-alpha\""));
    assert!(payload.contains("\"step_id\":1"));
    assert!(payload.contains("\"step_objective\":\"objective 1\""));
    assert!(payload.contains("\"step_acceptance\":[\"acceptance 1\"]"));
    assert!(payload.contains("\"parent_session_id\":\"parent-session-1\""));

    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[1];
    assert_eq!(step.status, BossPlanStepStatus::Running);
    assert_eq!(coordinator.status.read().await.current_step, Some(1));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_stops_before_approval_barrier() {
    let mut approval_step = boss_step(1, "Approval-gated step");
    approval_step.requires_approval = true;
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(0, "Step 1")
            },
            approval_step,
        ]),
        "test_boss_flow_approval_stop.json",
    )
    .await;

    let outcome = coordinator
        .advance_plan(&app_state("parent-session-2"))
        .await
        .unwrap()
        .expect("approval barrier should be reported");

    assert!(outcome.contains("paused before step 1"));
    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[1];
    assert_eq!(step.status, BossPlanStepStatus::WaitingForApproval);
    assert!(step.worker_task_id.is_none());

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_stops_after_step_failure() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step 1"), boss_step(1, "Step 2")]),
        "test_boss_flow_failure_stop.json",
    )
    .await;

    coordinator
        .on_task_event(&task_event("worker-task-failed", 0, TaskStatus::Failed))
        .await
        .unwrap();
    let outcome = coordinator
        .advance_plan(&app_state("parent-session-3"))
        .await
        .unwrap()
        .expect("failure should be reported");

    assert!(outcome.contains("terminal step failure"));
    let plan = coordinator.plan.read().await;
    assert_eq!(
        plan.as_ref().unwrap().steps[0].status,
        BossPlanStepStatus::Failed
    );
    assert_eq!(
        plan.as_ref().unwrap().steps[1].status,
        BossPlanStepStatus::Pending
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_advance_plan_actually_spawns_worker() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-dispatch", task_manager.clone());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
        ]),
        "test_boss_flow_real_dispatch.json",
    )
    .await;

    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("worker dispatch payload should be returned");

    assert!(payload.contains("\"step_id\":1"));
    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_type, TaskType::LocalAgent);
    assert_eq!(tasks[0].worker_role, Some(WorkerRole::Implement));
    assert_eq!(tasks[0].step_id, Some(1));
    assert_eq!(tasks[0].owner.session_id, "parent-session-dispatch");
    assert!(matches!(
        tasks[0].status,
        TaskStatus::Running | TaskStatus::Completed
    ));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn concurrent_worker_updates_do_not_cross_step_boundaries() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step 1"), boss_step(1, "Step 2")]),
        "test_boss_flow_concurrent_isolation.json",
    )
    .await;

    let left = coordinator.clone();
    let right = coordinator.clone();
    let left_event = task_event("worker-task-left", 0, TaskStatus::Completed);
    let right_event = task_event("worker-task-right", 1, TaskStatus::Completed);

    let (left_result, right_result) = tokio::join!(
        async move { left.on_task_event(&left_event).await },
        async move { right.on_task_event(&right_event).await }
    );
    left_result.unwrap();
    right_result.unwrap();

    let plan = coordinator.plan.read().await;
    let steps = &plan.as_ref().unwrap().steps;
    assert!(steps[0].completed);
    assert!(steps[1].completed);
    assert_eq!(steps[0].worker_task_id.as_deref(), Some("worker-task-left"));
    assert_eq!(
        steps[1].worker_task_id.as_deref(),
        Some("worker-task-right")
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_step_complete_auto_dispatches_next() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-auto-chain", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                status: BossPlanStepStatus::Running,
                worker_task_id: Some("worker-task-step0".into()),
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
        ]),
        "test_boss_flow_auto_chain.json",
    )
    .await;

    // Seed the auto-advance app_state by calling advance_plan once.
    // With step 0 Running, advance_plan returns None (already running) but stores app_state.
    let _ = coordinator.advance_plan(&app_state).await.unwrap();

    // Fire the completion event for step 0 — should auto-trigger advance_plan for step 1.
    coordinator
        .on_task_event(&task_event("worker-task-step0", 0, TaskStatus::Completed))
        .await
        .unwrap();

    let plan = coordinator.plan.read().await;
    let steps = &plan.as_ref().unwrap().steps;
    assert_eq!(steps[0].status, BossPlanStepStatus::Completed);
    assert!(steps[0].completed);
    assert_eq!(steps[1].status, BossPlanStepStatus::Running);
    drop(plan);

    let tasks = task_manager.list();
    assert_eq!(
        tasks.len(),
        1,
        "one worker should have been spawned for step 1"
    );
    assert_eq!(tasks[0].step_id, Some(1));
    assert_eq!(tasks[0].owner.session_id, "parent-session-auto-chain");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_starts_two_global_agents_and_restores_handles() {
    let plan = BossPlan {
        plan_id: "restore-test".into(),
        task_description: "restore test".into(),
        steps: vec![boss_step(0, "step 0")],
        accepted_by_user: true,
        auto_sequence: false,
        ..Default::default()
    };

    let dir = std::env::temp_dir().join("boss_restore_handles_test");
    std::fs::create_dir_all(&dir).unwrap();
    let plan_path = dir.join("planning.json");
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path)
        .await
        .expect("restore should succeed");

    let session_guard = coordinator.session.read().await;
    let session = session_guard
        .as_ref()
        .expect("session should be populated after restore");

    assert_eq!(session.plan_id, "restore-test");
    assert_eq!(session.designer_a.actor_id, "boss-restore-test-a");
    assert_eq!(session.executor_b.actor_id, "boss-restore-test-b");
    assert_eq!(session.designer_a.role, BossActorRole::DesignerA);
    assert_eq!(session.executor_b.role, BossActorRole::ExecutorB);
    assert_eq!(session.designer_a.status, BossActorStatus::Pending);
    assert_eq!(session.executor_b.status, BossActorStatus::Pending);
    assert!(session.active_children.is_empty());

    let _ = std::fs::remove_file(&plan_path);
    let _ = std::fs::remove_dir(dir);
}

#[tokio::test]
async fn boss_actor_registry_tracks_a_b_and_children() {
    let coordinator = BossCoordinator::new();

    let empty = coordinator.actor_registry_snapshot().await;
    assert!(empty.is_empty(), "no session means empty registry");

    coordinator
        .ensure_actor_session("plan-beta", BossStage::Execution)
        .await;

    let snapshot = coordinator.actor_registry_snapshot().await;
    assert_eq!(snapshot.len(), 2, "A and B should be present");
    assert!(snapshot.iter().any(|h| h.role == BossActorRole::DesignerA));
    assert!(snapshot.iter().any(|h| h.role == BossActorRole::ExecutorB));

    // Idempotent: same plan_id must not duplicate handles.
    coordinator
        .ensure_actor_session("plan-beta", BossStage::Execution)
        .await;
    let snapshot2 = coordinator.actor_registry_snapshot().await;
    assert_eq!(snapshot2.len(), 2);

    coordinator
        .update_actor_status("boss-plan-beta-a", BossActorStatus::Active)
        .await;
    let snapshot3 = coordinator.actor_registry_snapshot().await;
    let a = snapshot3
        .iter()
        .find(|h| h.role == BossActorRole::DesignerA)
        .unwrap();
    assert_eq!(a.status, BossActorStatus::Active);
    let b = snapshot3
        .iter()
        .find(|h| h.role == BossActorRole::ExecutorB)
        .unwrap();
    assert_eq!(b.status, BossActorStatus::Pending);

    // Inject one of each child role and verify the registry distinguishes them.
    {
        use rust_agent::core::boss_state::BossActorHandle;
        let mut guard = coordinator.session.write().await;
        let session = guard.as_mut().unwrap();
        session.active_children.push(BossActorHandle::new(
            "child-review-1",
            "child-review-1",
            BossActorRole::ReviewChild,
        ));
        session.active_children.push(BossActorHandle::new(
            "child-impl-1",
            "child-impl-1",
            BossActorRole::ImplementChild,
        ));
        session.active_children.push(BossActorHandle::new(
            "child-verify-1",
            "child-verify-1",
            BossActorRole::VerifyChild,
        ));
    }

    let snapshot4 = coordinator.actor_registry_snapshot().await;
    assert_eq!(snapshot4.len(), 5, "A + B + 3 children");
    assert!(snapshot4.iter().any(|h| h.role == BossActorRole::ReviewChild));
    assert!(snapshot4.iter().any(|h| h.role == BossActorRole::ImplementChild));
    assert!(snapshot4.iter().any(|h| h.role == BossActorRole::VerifyChild));

    // All three child roles must report is_child() == true.
    let children: Vec<_> = snapshot4.iter().filter(|h| h.role.is_child()).collect();
    assert_eq!(children.len(), 3);
    assert!(children.iter().all(|h| h.role.is_child()));

    // A and B must NOT be classified as children.
    assert!(!BossActorRole::DesignerA.is_child());
    assert!(!BossActorRole::ExecutorB.is_child());
}

// --- T16.6.B: Boss-aware spawn policy ---

#[test]
fn boss_b_executor_b_context_is_boss_executor_b() {
    let ctx = ToolAssemblyContext::executor_b(InteractionSurface::Cli, SessionMode::Headless);
    assert!(ctx.is_boss_executor_b(), "executor_b context must report is_boss_executor_b");
}

#[test]
fn boss_worker_context_is_not_boss_executor_b() {
    let ctx = ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless);
    assert!(!ctx.is_boss_executor_b(), "plain worker must not report is_boss_executor_b");
}

#[test]
fn boss_spawn_policy_denies_out_of_phase_child_spawn() {
    // A policy with phase != Execution must not allow spawning.
    let policy = BossActorPolicy {
        actor_role: BossActorRole::ExecutorB,
        lineage_depth: 0,
        phase: BossStage::Documentation,
    };
    assert!(
        !policy.may_spawn(),
        "ExecutorB outside Execution phase must not be allowed to spawn"
    );
}

#[tokio::test]
async fn boss_child_cannot_spawn_grandchild_agent() {
    // Build a ToolPermissionContext that looks like a ReviewChild.
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy::child(
            BossActorRole::ReviewChild,
            1,
            BossStage::Execution,
        ));

    let call = ToolCall::new(
        "Agent",
        serde_json::json!({
            "prompt": "do something",
            "session_id": "child-session"
        })
        .to_string(),
    );

    let err = AgentTool
        .invoke(&call, &permissions)
        .await
        .expect_err("child actor must not be allowed to spawn a grandchild");

    assert!(
        err.to_string().contains("boss spawn policy"),
        "error must mention boss spawn policy, got: {err}"
    );
    assert!(
        err.to_string().contains("review_child"),
        "error must name the role, got: {err}"
    );
}

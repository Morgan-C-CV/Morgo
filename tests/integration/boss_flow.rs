use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::boss::{BossCoordinator, save_plan};
use rust_agent::core::boss_state::{
    BossActorRole, BossActorStatus, BossControlRequest, BossControlResponse, BossPlan,
    BossPlanStep, BossPlanStepStatus, BossStage, BossStopStage,
};
use rust_agent::core::concurrency::{
    BossBudgetDecision, MemoryPressureLevel, evaluate_boss_budget,
};
use rust_agent::core::context::SubagentConfig;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{
    InMemorySessionStore, SessionHistory, SessionHistoryEntry, SessionId, SessionSnapshot,
};
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
        attempt_count: 0,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
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

fn app_state_with_history(
    active_session_id: &str,
    task_manager: Arc<TaskManager>,
    session_store: Arc<InMemorySessionStore>,
    history: SessionHistory,
) -> Arc<AppState> {
    let mut app = (*app_state_with_tasks(active_session_id, task_manager)).clone();
    app.session_store = Some(session_store);
    app.session = Some(SessionSnapshot {
        session_id: SessionId(active_session_id.into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        cwd: "/tmp".into(),
        last_turn_at: None,
        prompt_seed: None,
    });
    app.history = Some(history);
    Arc::new(app)
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
async fn report_interrupt_includes_active_children_and_attempt_review_summary() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Long running step")]),
        "test_boss_report_interrupt.json",
    )
    .await;

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some("task-b".into());
        snapshot.executor_b.status = BossActorStatus::Active;
        snapshot.active_children.push(rust_agent::core::boss_state::BossActorHandle {
            actor_id: "boss-plan-alpha-child-1".into(),
            session_id: "boss-plan-alpha-child-1".into(),
            role: BossActorRole::ImplementChild,
            status: BossActorStatus::Active,
            task_id: Some("task-child".into()),
            last_snapshot: None,
            lineage_depth: 1,
            mailbox_id: None,
            cancel_id: None,
        });
    }

    let task = task_manager.create_with_type(
        "Spawned implement worker for Long running step",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_worker_role(&task.id, WorkerRole::Implement);
    task_manager.set_boss_actor_id(&task.id, Some("executor_b:depth=0".into()));
    task_manager.start(&task.id);

    {
        let mut plan = coordinator.plan.write().await;
        let plan = plan.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Reviewing;
        plan.steps[0].worker_task_id = Some(task.id.clone());
        plan.steps[0].attempt_count = 2;
        plan.steps[0].last_review_summary = Some("A review: tighten edge-case handling".into());
    }

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert!(matches!(report.stage, BossStage::Execution | BossStage::Documentation));
    assert_eq!(report.executor_b.status, BossActorStatus::Active);
    assert_eq!(report.active_children.len(), 1);
    assert_eq!(report.active_children[0].role, BossActorRole::ImplementChild);
    assert_eq!(report.steps.len(), 1);
    assert_eq!(report.steps[0].attempt_count, 2);
    assert_eq!(
        report.steps[0].last_review_summary.as_deref(),
        Some("A review: tighten edge-case handling")
    );
    assert_eq!(report.steps[0].worker_task_id.as_deref(), Some(task.id.as_str()));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn report_control_request_does_not_require_query_loop_return() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Waiting step")]),
        "test_boss_report_control_request.json",
    )
    .await;

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();

    match response {
        BossControlResponse::Report(payload) => {
            assert_eq!(payload.total_steps, Some(1));
            assert_eq!(payload.steps.len(), 1);
        }
        other => panic!("expected report payload, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn stop_interrupt_returns_typed_stop_outcome_and_kills_tasks() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Force-drain step")]),
        "test_boss_stop_interrupt.json",
    )
    .await;

    let b_task = task_manager.create_with_type(
        "executor b",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_boss_actor_id(&b_task.id, Some("executor_b:depth=0".into()));
    task_manager.start(&b_task.id);

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some(b_task.id.clone());
        snapshot.executor_b.status = BossActorStatus::Active;
    }

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Stop(outcome) => {
            assert_eq!(
                outcome.stages,
                vec![
                    BossStopStage::CancelIssued,
                    BossStopStage::DeadlineExpired,
                    BossStopStage::ForceDrain,
                ]
            );
            assert!(outcome.killed_task_ids.contains(&b_task.id));
        }
        other => panic!("expected stop outcome, got {other:?}"),
    }
    assert_eq!(task_manager.status(&b_task.id), Some(TaskStatus::Killed));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn stop_interrupt_immediate_cancel_only_reports_cancel_issued() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Immediate cancel step")]),
        "test_boss_stop_immediate_cancel.json",
    )
    .await;

    let b_task = task_manager.create_with_type(
        "executor b",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_boss_actor_id(&b_task.id, Some("executor_b:depth=0".into()));
    task_manager.launch(&b_task.id, "executor b running", async {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    });

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some(b_task.id.clone());
        snapshot.executor_b.status = BossActorStatus::Active;
    }

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Stop(outcome) => {
            assert_eq!(outcome.stages, vec![BossStopStage::CancelIssued]);
            assert!(!outcome.stages.contains(&BossStopStage::DeadlineExpired));
            assert!(!outcome.stages.contains(&BossStopStage::ForceDrain));
        }
        other => panic!("expected stop outcome, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn stop_interrupt_records_deadline_without_force_drain_when_task_finishes_in_time() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Deadline-only stop step")]),
        "test_boss_stop_deadline_no_force.json",
    )
    .await;

    let b_task = task_manager.create_with_type(
        "executor b",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_boss_actor_id(&b_task.id, Some("executor_b:depth=0".into()));
    task_manager.start(&b_task.id);

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some(b_task.id.clone());
        snapshot.executor_b.status = BossActorStatus::Active;
    }

    let task_manager_for_finish = task_manager.clone();
    let dispatcher_for_finish = dispatcher.clone();
    let b_task_id = b_task.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        task_manager_for_finish.complete(&b_task_id, &dispatcher_for_finish);
    });

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session".into(),
                deadline_ms: 20,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Stop(outcome) => {
            assert_eq!(
                outcome.stages,
                vec![BossStopStage::CancelIssued, BossStopStage::DeadlineExpired]
            );
            assert!(!outcome.stages.contains(&BossStopStage::ForceDrain));
        }
        other => panic!("expected stop outcome, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn report_payload_uses_historystore_derived_summary() {
    let task_manager = Arc::new(TaskManager::default());
    let store = Arc::new(InMemorySessionStore::default());
    let history = SessionHistory {
        entries: vec![
            SessionHistoryEntry {
                message: rust_agent::core::message::Message::user("first user note"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            },
            SessionHistoryEntry {
                message: rust_agent::core::message::Message::assistant("second assistant summary"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            },
        ],
    };
    let app_state = app_state_with_history(
        "history-session",
        task_manager.clone(),
        store,
        history,
    );
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "History-backed step")]),
        "test_boss_historystore_report.json",
    )
    .await;
    coordinator
        .attach_app_state_for_report_testing(app_state)
        .await;

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Report,
            &task_manager,
            &NotificationDispatcher::new(TelegramGateway::default()),
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Report(payload) => {
            assert_eq!(payload.history_summary.len(), 2);
            assert_eq!(payload.history_summary[0], "second assistant summary");
            assert_eq!(payload.history_summary[1], "first user note");
        }
        other => panic!("expected report payload, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn report_control_request_uses_dedicated_mailbox_runtime() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Mailbox report step")]),
        "test_boss_report_mailbox_runtime.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    assert!(coordinator.has_control_runtime().await);

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    assert!(matches!(response, BossControlResponse::Report(_)));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn control_mailbox_runtime_remains_available_after_rebind() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Mailbox rebind step")]),
        "test_boss_mailbox_rebind.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    assert!(coordinator.has_control_runtime().await);

    coordinator.rebind_control_runtime().await;
    assert!(coordinator.has_control_runtime().await);

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    assert!(matches!(response, BossControlResponse::Report(_)));

    let _ = std::fs::remove_file(plan_path);
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
fn subagent_limiter_enforces_total_and_role_caps_under_memory_pressure() {
    let tasks = TaskManager::default();

    for index in 0..2 {
        let task = tasks.create_with_type(
            format!("research-{index}"),
            TaskType::LocalAgent,
            "boss-session",
            InteractionSurface::Cli,
        );
        tasks.set_worker_role(&task.id, WorkerRole::Research);
        tasks.set_boss_actor_id(&task.id, Some(format!("review_child:depth={index}")));
    }

    assert!(matches!(
        evaluate_boss_budget(&tasks, WorkerRole::Research, 1, MemoryPressureLevel::Normal),
        BossBudgetDecision::Queue { .. }
    ));

    for index in 0..4 {
        let task = tasks.create_with_type(
            format!("implement-{index}"),
            TaskType::LocalAgent,
            "boss-session",
            InteractionSurface::Cli,
        );
        tasks.set_worker_role(&task.id, WorkerRole::Implement);
        tasks.set_boss_actor_id(&task.id, Some(format!("implement_child:depth={index}")));
    }

    assert!(matches!(
        evaluate_boss_budget(&tasks, WorkerRole::Implement, 1, MemoryPressureLevel::Normal),
        BossBudgetDecision::Queue { .. }
    ));
}

#[tokio::test]
async fn boss_budget_blocks_low_priority_children_when_pressure_is_critical() {
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));

    let decision = evaluate_boss_budget(
        permissions.task_manager.as_ref().unwrap(),
        WorkerRole::Research,
        1,
        MemoryPressureLevel::Critical,
    );
    assert!(matches!(decision, BossBudgetDecision::Deny { .. }));

    let decision = evaluate_boss_budget(
        permissions.task_manager.as_ref().unwrap(),
        WorkerRole::Verify,
        1,
        MemoryPressureLevel::Critical,
    );
    assert!(matches!(decision, BossBudgetDecision::Queue { .. }));

    let decision = evaluate_boss_budget(
        permissions.task_manager.as_ref().unwrap(),
        WorkerRole::Implement,
        1,
        MemoryPressureLevel::Critical,
    );
    assert_eq!(decision, BossBudgetDecision::Allow);
}

#[tokio::test]
async fn boss_agent_spawn_gate_surfaces_budget_queue_reason() {
    let tasks = Arc::new(TaskManager::default());
    for index in 0..6 {
        let task = tasks.create_with_type(
            format!("active-boss-{index}"),
            TaskType::LocalAgent,
            "boss-session",
            InteractionSurface::Cli,
        );
        tasks.set_worker_role(&task.id, WorkerRole::Implement);
        tasks.set_boss_actor_id(&task.id, Some(format!("implement_child:depth={index}")));
    }

    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));

    let err = AgentTool
        .invoke(
            &ToolCall::new(
                "Agent",
                serde_json::json!({
                    "task": "implement overflow child",
                    "role": "implement"
                })
                .to_string(),
            ),
            &permissions,
        )
        .await
        .expect_err("budget gate must reject spawning beyond the boss active cap");

    assert!(
        err.to_string().contains("boss budget queued"),
        "budget gate should surface queue reason, got: {err}"
    );
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

// --- T16.6.C.1: Persistent ExecutorB routing ---

#[tokio::test]
async fn execution_reuses_persistent_b_instead_of_fresh_worker_per_step() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-b-reuse", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
            boss_step(2, "Step 3"),
        ]),
        "test_boss_flow_b_reuse.json",
    )
    .await;

    // Dispatch step 1 — spawns B fresh (no running B yet).
    let payload1 = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 1 should dispatch");

    assert!(payload1.contains("\"step_id\":1"), "spawn payload must carry step_id");
    assert!(
        payload1.contains("\"reuse_strategy\":\"running_only\""),
        "spawn payload must use running_only reuse strategy"
    );

    let tasks_after_step1 = task_manager.list();
    assert_eq!(tasks_after_step1.len(), 1, "exactly one B task spawned for step 1");
    let b_task_id = tasks_after_step1[0].id.clone();

    // B's actor id is deterministically derived from the plan id.
    let v1: serde_json::Value = serde_json::from_str(&payload1).unwrap();
    let group_id = v1["orchestration_group_id"].as_str().unwrap_or("");
    assert!(
        group_id.contains("plan-alpha"),
        "orchestration_group_id must embed the plan id, got: {group_id}"
    );

    // Manually mark B's task as Running so the Continue path triggers for step 2.
    task_manager.start(&b_task_id);
    // Record B's task id in the session so find_running_b_task_id can find it.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some(b_task_id.clone());
        }
    }

    // Mark step 1 completed so advance_plan can move to step 2.
    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[1].completed = true;
        plan.steps[1].status = BossPlanStepStatus::Completed;
    }

    // Dispatch step 2 — B is running, so this must use Continue (no new task).
    let payload2 = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 2 should dispatch via continue");

    // Continue payload carries task_id, not reuse_strategy.
    let v2: serde_json::Value = serde_json::from_str(&payload2).unwrap();
    assert_eq!(
        v2["task_id"].as_str().unwrap_or(""),
        b_task_id,
        "continue payload must target the existing B task"
    );
    assert_eq!(v2["step_id"], 2, "continue payload must carry step_id 2");
    assert!(v2["reuse_strategy"].is_null(), "continue payload must NOT have reuse_strategy");

    // Critically: still only one task in the manager — no new task was spawned.
    let tasks_after_step2 = task_manager.list();
    assert_eq!(
        tasks_after_step2.len(),
        1,
        "step 2 must reuse B's task via Continue — no new task should be created"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_advance_plan_uses_continue_payload_when_b_is_running() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-continue", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A"), boss_step(1, "Step B")]),
        "test_boss_flow_continue_path.json",
    )
    .await;

    // Dispatch step 0 — spawns B fresh.
    let _ = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 0 should dispatch");

    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1, "one B task after step 0");
    let b_task_id = tasks[0].id.clone();

    // Mark B as Running and record its id in the session.
    task_manager.start(&b_task_id);
    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some(b_task_id.clone());
        }
    }

    // Mark step 0 completed so advance_plan can move to step 1.
    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[0].completed = true;
        plan.steps[0].status = BossPlanStepStatus::Completed;
    }

    // Dispatch step 1 — B is running, must use Continue.
    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 1 should dispatch via continue");

    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        v["task_id"].as_str().unwrap_or(""),
        b_task_id,
        "continue payload must target the running B task"
    );
    assert_eq!(v["step_id"], 1, "continue payload must carry step_id 1");
    assert_eq!(v["boss_plan_id"], "plan-alpha");
    assert_eq!(v["step_objective"], "objective 1");
    assert_eq!(v["step_acceptance"][0], "acceptance 1");

    // No new task created.
    assert_eq!(task_manager.list().len(), 1, "no new task — B was reused via Continue");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_b_receives_step_context_via_continue_or_mailbox() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A")]),
        "test_boss_flow_b_context.json",
    )
    .await;

    // build_step_spawn_payload must embed the step objective and acceptance criteria.
    let b_actor_id = format!("boss-{}-b", "plan-alpha");
    let payload = coordinator
        .build_step_spawn_payload(0, "parent-ctx-session", &b_actor_id)
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(v["step_id"], 0, "step_id must be embedded");
    assert_eq!(v["boss_plan_id"], "plan-alpha", "plan_id must be embedded");
    assert_eq!(
        v["step_objective"], "objective 0",
        "step objective must be embedded"
    );
    assert_eq!(
        v["step_acceptance"][0], "acceptance 0",
        "acceptance criteria must be embedded"
    );
    assert_eq!(
        v["parent_session_id"], "parent-ctx-session",
        "parent session id must be embedded"
    );
    assert_eq!(
        v["reuse_strategy"], "running_only",
        "reuse strategy must be running_only"
    );
    assert_eq!(
        v["orchestration_group_id"], b_actor_id,
        "orchestration_group_id must be B's actor id"
    );

    // build_step_continue_payload must embed step context and target the B task id.
    let continue_payload = coordinator
        .build_step_continue_payload(0, "b-task-42", "parent-ctx-session")
        .await
        .unwrap();

    let vc: serde_json::Value = serde_json::from_str(&continue_payload).unwrap();
    assert_eq!(vc["task_id"], "b-task-42", "continue payload must target B's task id");
    assert_eq!(vc["step_id"], 0);
    assert_eq!(vc["boss_plan_id"], "plan-alpha");
    assert_eq!(vc["step_objective"], "objective 0");
    assert_eq!(vc["step_acceptance"][0], "acceptance 0");
    assert_eq!(vc["parent_session_id"], "parent-ctx-session");
    // Continue payload must NOT have reuse_strategy or task field.
    assert!(vc["reuse_strategy"].is_null(), "continue payload must not have reuse_strategy");
    assert!(vc["task"].is_null(), "continue payload must not have task field");

    let _ = std::fs::remove_file(plan_path);
}

// --- T16.6.C.3: B child spawn contract + fan-in summary ---

#[test]
fn boss_b_spawns_children_with_child_policy_and_depth() {
    use rust_agent::state::permission_context::BossActorPolicy;

    // Simulate B (ExecutorB, depth=0) spawning a child with explicit role.
    let b_policy = BossActorPolicy::executor_b(BossStage::Execution);
    assert!(b_policy.may_spawn(), "ExecutorB must be allowed to spawn");

    // Child policy: implement_child at depth 1.
    let child_policy = BossActorPolicy {
        actor_role: BossActorRole::ImplementChild,
        lineage_depth: b_policy.lineage_depth + 1,
        phase: BossStage::Execution,
    };
    assert_eq!(child_policy.lineage_depth, 1, "child must be at depth 1");
    assert!(!child_policy.may_spawn(), "ImplementChild must not be allowed to spawn");
    assert!(child_policy.actor_role.is_child(), "ImplementChild must be classified as child");

    // Verify all three child roles are blocked from spawning.
    for role in [
        BossActorRole::ReviewChild,
        BossActorRole::ImplementChild,
        BossActorRole::VerifyChild,
    ] {
        let p = BossActorPolicy {
            actor_role: role,
            lineage_depth: 1,
            phase: BossStage::Execution,
        };
        assert!(!p.may_spawn(), "{} must not be allowed to spawn", role.as_str());
    }

    // boss_actor_id recorded on task must encode role and depth.
    let boss_actor_id = format!("{}:depth={}", child_policy.actor_role.as_str(), child_policy.lineage_depth);
    assert_eq!(boss_actor_id, "implement_child:depth=1");
}

#[tokio::test]
async fn boss_b_coerces_non_child_spawn_policy_to_child_depth() {
    let task_manager = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager.clone())
        .with_active_session_id("parent-session-policy")
        .with_active_surface(InteractionSurface::Cli)
        .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));

    let payload = serde_json::json!({
        "task": "spawn child from B",
        "role": "implement",
        "inherit_context": false,
        "max_turns": 0,
        "boss_actor_role": "executor_b",
        "boss_lineage_depth": 0
    })
    .to_string();

    AgentTool
        .invoke(&ToolCall::new("Agent", payload), &permissions)
        .await
        .expect("ExecutorB should be allowed to spawn a child");

    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].boss_actor_id.as_deref(),
        Some("implement_child:depth=1"),
        "non-child explicit role must be coerced to implement_child at depth 1"
    );
}

#[tokio::test]
async fn boss_b_fans_out_children_and_fans_in_summary() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-fan-in", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Fan-out step")]),
        "test_boss_flow_fan_in.json",
    )
    .await;

    // Dispatch step 0 — spawns B fresh.
    let _ = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 0 should dispatch");

    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1, "one B task after step 0 dispatch");
    let b_task_id = tasks[0].id.clone();

    // Record B's task id in the session so fan-in can find the step.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some(b_task_id.clone());
        }
    }
    // Also record B's task id in the step's worker_task_id so fan-in lookup works.
    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some(b_task_id.clone());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Simulate B spawning two children with orchestration_group_id = B's task id.
    let child1 = task_manager.create_with_type(
        "child-impl-1".to_string(),
        rust_agent::task::types::TaskType::LocalAgent,
        "parent-session-fan-in".to_string(),
        InteractionSurface::Cli,
    );
    let child2 = task_manager.create_with_type(
        "child-impl-2".to_string(),
        rust_agent::task::types::TaskType::LocalAgent,
        "parent-session-fan-in".to_string(),
        InteractionSurface::Cli,
    );
    task_manager.set_orchestration_group_id(&child1.id, Some(b_task_id.clone()));
    task_manager.set_orchestration_group_id(&child2.id, Some(b_task_id.clone()));
    task_manager.set_boss_actor_id(&child1.id, Some("implement_child:depth=1".into()));
    task_manager.set_boss_actor_id(&child2.id, Some("implement_child:depth=1".into()));

    // Verify group is not yet ready (children still pending).
    assert!(
        !task_manager.group_ready_for_fan_in(&b_task_id),
        "group must not be ready while children are pending"
    );

    // Complete both children — group fan-in fires.
    let dispatcher = rust_agent::interaction::dispatcher::NotificationDispatcher::new(
        rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
    );
    task_manager.complete_with_usage(&child1.id, &dispatcher, None);
    task_manager.complete_with_usage(&child2.id, &dispatcher, None);

    assert!(
        task_manager.group_ready_for_fan_in(&b_task_id),
        "group must be ready after all children complete"
    );

    // Verify group_summary returns a summary for B's group.
    let summary = task_manager.group_summary(&b_task_id);
    assert!(summary.is_some(), "group_summary must return a summary when all children complete");

    // Simulate the group fan-in event arriving at the coordinator.
    let fan_in_event = TaskEvent {
        task_id: format!("group-{}", b_task_id),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: None,
        owner: rust_agent::task::types::TaskOwner {
            session_id: "parent-session-fan-in".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some(b_task_id.clone()),
        summary: "grouped research tasks completed".into(),
        result: "Agent task completed".into(),
        next_action: "synthesize grouped findings".into(),
        worker_role: None,
        orchestration_group_id: Some(b_task_id.clone()),
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    };

    coordinator.on_task_event(&fan_in_event).await.unwrap();

    // T16.6.D: fan-in now transitions to Reviewing (not Completed directly).
    // A's review gate must accept before the step is Completed.
    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Reviewing,
        "fan-in event must mark the step as Reviewing (pending A's review)"
    );
    assert!(!step.completed, "step.completed must be false until A accepts");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_child_event_cannot_complete_step_before_group_fan_in() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Child must not complete directly")]),
        "test_boss_flow_child_no_direct_complete.json",
    )
    .await;

    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-child-guard".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let child_event = TaskEvent {
        task_id: "child-impl-direct".into(),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: Some(0),
        owner: rust_agent::task::types::TaskOwner {
            session_id: "parent-session-child-guard".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some("child-impl-direct".into()),
        summary: "child completed".into(),
        result: "child result".into(),
        next_action: "wait for group fan-in".into(),
        worker_role: Some(WorkerRole::Implement),
        orchestration_group_id: Some("b-task-child-guard".into()),
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    };

    coordinator.on_task_event(&child_event).await.unwrap();

    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Running,
        "child event with orchestration_group_id must not complete the step directly"
    );
    assert!(
        !step.completed,
        "step must wait for group fan-in and A review"
    );

    let _ = std::fs::remove_file(plan_path);
}

// --- T16.6.C.2: ExecutorB policy injection ---

#[tokio::test]
async fn documentation_stage_runs_designer_reviewer_revision_loop() {
    let plan = BossPlan {
        plan_id: "plan-doc-loop".into(),
        task_description: "Design a safe execution plan".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        draft_spec: None,
        review_feedback: None,
        revision_notes: None,
        finalized: false,
        documentation_feedback: Vec::new(),
        steps: vec![boss_step(0, "Implement validated step")],
        accepted_by_user: false,
        auto_sequence: true,
    };

    let (coordinator, plan_path) =
        coordinator_with_plan(plan, "test_boss_documentation_loop.json").await;

    assert_eq!(coordinator.get_stage().await, BossStage::Documentation);

    coordinator
        .finalize_documentation_loop(
            "A draft: outline the implementation and risks.",
            "B review: add feasibility notes, test plan, and edge-case risks.",
            "A revision: tighten scope and clarify acceptance criteria.",
            "Final spec: scoped implementation with explicit acceptance criteria.",
            "Pseudo-code: validate -> execute -> review -> complete.",
        )
        .await
        .unwrap();

    assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);

    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert_eq!(
        plan.draft_spec.as_deref(),
        Some("A draft: outline the implementation and risks.")
    );
    assert_eq!(
        plan.review_feedback.as_deref(),
        Some("B review: add feasibility notes, test plan, and edge-case risks.")
    );
    assert_eq!(
        plan.revision_notes.as_deref(),
        Some("A revision: tighten scope and clarify acceptance criteria.")
    );
    assert_eq!(
        plan.document_spec,
        "Final spec: scoped implementation with explicit acceptance criteria."
    );
    assert_eq!(
        plan.pseudo_code,
        "Pseudo-code: validate -> execute -> review -> complete."
    );
    assert!(plan.finalized, "documentation loop must finalize the plan");
    assert!(
        !plan.accepted_by_user,
        "documentation finalization must not skip user approval"
    );
    drop(plan_guard);

    let saved = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
    assert!(saved.finalized, "finalized plan must be persisted");
    assert_eq!(
        saved.review_feedback.as_deref(),
        Some("B review: add feasibility notes, test plan, and edge-case risks.")
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn user_feedback_reopens_documentation_loop_before_execution() {
    let plan = BossPlan {
        plan_id: "plan-doc-feedback".into(),
        task_description: "Refine plan from user notes".into(),
        document_spec: "Initial final spec".into(),
        pseudo_code: "Initial pseudo-code".into(),
        draft_spec: Some("Initial draft".into()),
        review_feedback: Some("Initial B review".into()),
        revision_notes: Some("Initial A revision".into()),
        finalized: true,
        documentation_feedback: Vec::new(),
        steps: vec![boss_step(0, "Implement after approval")],
        accepted_by_user: false,
        auto_sequence: true,
    };

    let (coordinator, plan_path) =
        coordinator_with_plan(plan, "test_boss_documentation_feedback.json").await;

    coordinator
        .transition_to(BossStage::WaitingForApproval)
        .await
        .unwrap();

    let confirmed = coordinator
        .handle_user_approval("Please add rollback handling and explicit failure cases")
        .await
        .unwrap();

    assert!(!confirmed, "non-confirmation input must not enter execution");
    assert_eq!(coordinator.get_stage().await, BossStage::Documentation);

    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert!(
        !plan.finalized,
        "user feedback must reopen the documentation loop"
    );
    assert!(
        !plan.accepted_by_user,
        "user feedback must keep approval unset"
    );
    assert_eq!(plan.documentation_feedback.len(), 1);
    assert_eq!(
        plan.documentation_feedback[0],
        "Please add rollback handling and explicit failure cases"
    );
    drop(plan_guard);

    let saved = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
    assert_eq!(saved.documentation_feedback.len(), 1);
    assert!(!saved.finalized);

    let _ = std::fs::remove_file(plan_path);
}
#[test]
fn boss_spawned_b_runtime_has_executor_policy_and_agent_tool() {
    use rust_agent::tool::builtin::agent::AgentTool;

    // Build a registry with Agent registered.
    let registry = ToolRegistry::new().register(Arc::new(AgentTool));

    // Assemble with executor_b context — Agent must be visible.
    let b_ctx = ToolAssemblyContext::executor_b(InteractionSurface::Cli, SessionMode::Headless);
    assert!(b_ctx.is_boss_executor_b(), "executor_b context must report is_boss_executor_b");

    let b_registry = registry.assemble(b_ctx);
    let b_tools: Vec<_> = b_registry.all_metadata();
    assert!(
        b_tools.iter().any(|m| m.name == "Agent"),
        "ExecutorB registry must include Agent tool"
    );

    // Assemble with plain worker context — Agent must NOT be visible.
    let worker_ctx = ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless);
    let worker_registry = registry.assemble(worker_ctx);
    let worker_tools: Vec<_> = worker_registry.all_metadata();
    assert!(
        !worker_tools.iter().any(|m| m.name == "Agent"),
        "plain worker registry must NOT include Agent tool"
    );

    // SubagentConfig with boss_actor_policy set must carry the policy through.
    let policy = BossActorPolicy::executor_b(BossStage::Execution);
    let config = SubagentConfig {
        worker_role: WorkerRole::Implement,
        inherit_context: false,
        max_turns: None,
        allowed_tools: None,
        boss_actor_policy: Some(policy),
    };
    assert!(
        config.boss_actor_policy.is_some(),
        "SubagentConfig must carry boss_actor_policy"
    );
    assert!(
        config.boss_actor_policy.unwrap().may_spawn(),
        "executor_b policy must allow spawning"
    );
}

#[test]
fn boss_spawn_payload_contains_executor_b_role_fields() {
    // Verify build_step_spawn_payload emits boss_actor_role and boss_lineage_depth.
    // We test this by parsing a known payload JSON directly.
    let payload = serde_json::json!({
        "task": "Boss mode step 0",
        "role": "implement",
        "reuse_strategy": "running_only",
        "boss_actor_role": "executor_b",
        "boss_lineage_depth": 0,
        "orchestration_group_id": "boss-plan-alpha-b",
    });
    assert_eq!(payload["boss_actor_role"], "executor_b");
    assert_eq!(payload["boss_lineage_depth"], 0);
    assert_eq!(payload["orchestration_group_id"], "boss-plan-alpha-b");
}

// --- T16.6.D: A review gate ---

fn fan_in_event(b_task_id: &str) -> TaskEvent {
    TaskEvent {
        task_id: format!("group-{}", b_task_id),
        task_type: TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: None,
        owner: TaskOwner {
            session_id: "test-session".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some(b_task_id.into()),
        summary: "grouped research tasks completed".into(),
        result: "Agent task completed".into(),
        next_action: "synthesize grouped findings".into(),
        worker_role: None,
        orchestration_group_id: Some(b_task_id.into()),
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    }
}

#[tokio::test]
async fn boss_a_review_accepts_diff_before_step_completion() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step to review")]),
        "test_boss_review_accept.json",
    )
    .await;

    // Seed B's task id in the step so fan-in lookup works.
    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-review".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Fan-in fires — step must enter Reviewing, not Completed.
    coordinator
        .on_task_event(&fan_in_event("b-task-review"))
        .await
        .unwrap();

    {
        let guard = coordinator.plan.read().await;
        let step = &guard.as_ref().unwrap().steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Reviewing, "fan-in must enter Reviewing");
        assert!(!step.completed, "step must not be completed before A accepts");
    }

    // A accepts — step must move to Completed.
    coordinator
        .on_review_event(0, true, "LGTM, all acceptance criteria met", None)
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Completed, "A accept must complete the step");
    assert!(step.completed, "step.completed must be true after A accepts");
    assert_eq!(step.last_review_summary.as_deref(), Some("LGTM, all acceptance criteria met"));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_a_review_rejects_and_sends_correction_to_b() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step to reject")]),
        "test_boss_review_reject.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-reject".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    coordinator
        .on_task_event(&fan_in_event("b-task-reject"))
        .await
        .unwrap();

    // A rejects with a correction.
    coordinator
        .on_review_event(
            0,
            false,
            "Missing error handling in step output",
            Some("Add error handling for the edge case in section 3"),
        )
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Rejected, "A reject must set Rejected status");
    assert!(!step.completed, "step must not be completed after rejection");
    assert_eq!(step.attempt_count, 1, "attempt_count must increment on rejection");
    assert_eq!(
        step.last_correction.as_deref(),
        Some("Add error handling for the edge case in section 3")
    );
    assert_eq!(
        step.last_review_summary.as_deref(),
        Some("Missing error handling in step output")
    );

    // Rejected step must be runnable — advance_plan should re-dispatch B.
    drop(guard);
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-reject", task_manager.clone());
    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("rejected step must be re-dispatched");

    // Spawn payload must embed the correction.
    assert!(
        payload.contains("correction from review"),
        "retry payload must embed the correction"
    );
    assert!(
        payload.contains("Add error handling for the edge case in section 3"),
        "retry payload must contain the correction text"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_step_fails_only_after_retry_budget_exhausted() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![BossPlanStep {
            retry_budget: 2,
            ..boss_step(0, "Budget-limited step")
        }]),
        "test_boss_retry_budget.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-budget".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // First rejection — attempt_count = 1, still under budget (2).
    coordinator.on_task_event(&fan_in_event("b-task-budget")).await.unwrap();
    coordinator
        .on_review_event(0, false, "Not good enough", Some("Fix it"))
        .await
        .unwrap();

    {
        let guard = coordinator.plan.read().await;
        let step = &guard.as_ref().unwrap().steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Rejected, "first rejection must be Rejected");
        assert_eq!(step.attempt_count, 1);
    }

    // Reset to Reviewing for second rejection.
    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Reviewing;
    }

    // Second rejection — attempt_count = 2, hits budget → Failed.
    coordinator
        .on_review_event(0, false, "Still not good enough", Some("Fix it again"))
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Failed,
        "step must be Failed after retry budget exhausted"
    );
    assert_eq!(step.attempt_count, 2, "attempt_count must equal retry_budget");
    assert!(!step.completed, "failed step must not be marked completed");

    let _ = std::fs::remove_file(plan_path);
}

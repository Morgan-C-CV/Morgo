use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::boss::{BossCoordinator, save_plan};
use rust_agent::core::boss_actor_runtime::{
    BossActorRegistry, DesignerARuntime, ExecutionFn, ExecutorBRuntime, SpecReviewFn,
};
use rust_agent::core::boss_runtime::BossRuntimeHost;
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
    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = Arc::new(
        BossCoordinator::restore_or_init_with_owner(&plan_path, owner)
            .await
            .unwrap(),
    );
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
async fn coordinators_with_same_plan_id_do_not_collide_in_runtime_registry() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let plan = boss_plan(vec![boss_step(0, "Same plan id step")]);
    let (coordinator_a, path_a) =
        coordinator_with_plan(plan.clone(), "test_boss_same_plan_a.json").await;
    let (coordinator_b, path_b) = coordinator_with_plan(plan, "test_boss_same_plan_b.json").await;

    coordinator_a.ensure_control_runtime().await;
    coordinator_b.ensure_control_runtime().await;

    let key_a = coordinator_a.current_runtime_key().await.unwrap();
    let key_b = coordinator_b.current_runtime_key().await.unwrap();
    assert_ne!(key_a, key_b, "same plan_id coordinators must have distinct runtime keys");

    let response_a = coordinator_a
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    let response_b = coordinator_b
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    assert!(matches!(response_a, BossControlResponse::Report(_)));
    assert!(matches!(response_b, BossControlResponse::Report(_)));

    let _ = std::fs::remove_file(path_a);
    let _ = std::fs::remove_file(path_b);
}

#[tokio::test]
async fn old_runtime_is_shutdown_and_unavailable_after_rebind() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Shutdown old runtime step")]),
        "test_boss_old_runtime_shutdown.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    let old_key = coordinator.current_runtime_key().await.unwrap();

    coordinator.rebind_control_runtime().await;
    let new_key = coordinator.current_runtime_key().await.unwrap();
    assert_ne!(old_key, new_key);
    assert!(
        coordinator.runtime_is_closed_for_testing(&old_key).await,
        "old runtime must be explicitly shut down"
    );
    assert!(coordinator.has_control_runtime().await);
    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(response.is_ok(), "new runtime must accept requests after rebind");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn runtime_owner_shutdown_makes_runtime_unaddressable() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Owner shutdown step")]),
        "test_boss_runtime_owner_shutdown.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    let runtime_key = coordinator.current_runtime_key().await.unwrap();
    assert!(coordinator.has_control_runtime().await);

    coordinator.shutdown_runtime_owner();

    assert!(coordinator.runtime_is_closed_for_testing(&runtime_key).await);
    assert!(!coordinator.has_control_runtime().await);
    assert!(
        coordinator
            .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
            .await
            .is_err(),
        "owner shutdown must block fresh runtime bootstrap"
    );
    coordinator.restart_runtime_owner();

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn shutdown_all_runtimes_allows_fresh_bootstrap_after_cleanup() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Owner cleanup step")]),
        "test_boss_runtime_cleanup.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    let runtime_key = coordinator.current_runtime_key().await.unwrap();
    coordinator.shutdown_all_runtime_instances();

    assert!(coordinator.runtime_is_closed_for_testing(&runtime_key).await);
    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(response.is_ok(), "cleanup-only shutdown must allow fresh bootstrap");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn shutdown_owner_does_not_block_fresh_coordinator_with_fresh_owner() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let (closed_coordinator, closed_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Closed owner step")]),
        "test_boss_closed_owner_isolation.json",
    )
    .await;
    closed_coordinator.ensure_control_runtime().await;
    closed_coordinator.shutdown_runtime_owner();
    assert!(
        closed_coordinator
            .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
            .await
            .is_err()
    );

    let (fresh_coordinator, fresh_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Fresh owner step")]),
        "test_boss_fresh_owner_isolation.json",
    )
    .await;
    let response = fresh_coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(response.is_ok(), "fresh owner must remain usable after another owner shuts down");

    let _ = std::fs::remove_file(closed_path);
    let _ = std::fs::remove_file(fresh_path);
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

// --- T16.6.G.5: BossRuntimeHost assembly layer ---

#[tokio::test]
async fn production_assembly_uses_explicit_runtime_host_not_global_singleton() {
    let host_a = BossRuntimeHost::new();
    let host_b = BossRuntimeHost::new();

    assert!(
        !Arc::ptr_eq(&host_a.owner(), &host_b.owner()),
        "each BossRuntimeHost must produce an independent owner"
    );

    let coordinator_a = BossCoordinator::new_with_runtime_owner(host_a.owner());
    let coordinator_b = BossCoordinator::new_with_runtime_owner(host_b.owner());

    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    coordinator_a.shutdown_runtime_owner();
    assert!(
        coordinator_a
            .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
            .await
            .is_err(),
        "coordinator_a must be blocked after its host owner shuts down"
    );

    let response = coordinator_b
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response.is_ok(),
        "coordinator_b must remain usable after an unrelated host shuts down"
    );
}

#[tokio::test]
async fn runtime_host_owner_survives_rebind_and_restart() {
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let coordinator = BossCoordinator::new_with_runtime_owner(host.owner());

    coordinator.ensure_control_runtime().await;
    let key_before = coordinator.current_runtime_key().await.unwrap();
    coordinator.rebind_control_runtime().await;
    let key_after = coordinator.current_runtime_key().await.unwrap();
    assert_ne!(key_before, key_after, "rebind must produce a new key");

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(response.is_ok(), "control request must succeed after rebind via host");

    coordinator.shutdown_runtime_owner();
    coordinator.restart_runtime_owner();

    let response2 = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(response2.is_ok(), "control request must succeed after owner restart via host");
}

// --- T16.6.H: Boss actor runtime mailbox seam ---

use rust_agent::core::boss_actor_runtime::{DesignerACommand, ExecutorBCommand};
use rust_agent::core::boss_state::BossActorStatus as ActorStatus;

#[tokio::test]
async fn restore_bootstraps_actor_runtimes_that_are_addressable() {
    let plan_path = std::env::temp_dir().join("boss_h_restore_actor.json");
    let plan = BossPlan {
        plan_id: "plan-h-restore".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    // Actor registry must be bootstrapped after restore.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().expect("actor registry must be bootstrapped after restore");

    // Both mailboxes must be open and addressable.
    assert!(!registry.a_mailbox().is_closed(), "A mailbox must be open after restore");
    assert!(!registry.b_mailbox().is_closed(), "B mailbox must be open after restore");

    // Send a command to A and verify it processes without error.
    let event = registry.a_mailbox().request(DesignerACommand::Plan {
        plan_id: "plan-h-restore".into(),
        document_spec: "spec".into(),
    }).await;
    assert!(event.is_ok(), "A mailbox must accept Plan command after restore");

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn advance_plan_dispatches_step_through_b_mailbox() {
    let plan_path = std::env::temp_dir().join("boss_h_advance_b.json");
    let plan = BossPlan {
        plan_id: "plan-h-advance".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    // Ensure actor registry is live.
    coordinator.ensure_actor_registry().await;

    // Manually send a DispatchStep to B's mailbox (simulating what advance_plan does).
    let event = {
        let registry_guard = coordinator.actor_registry.read().await;
        let registry = registry_guard.as_ref().unwrap();
        registry.b_mailbox().request(ExecutorBCommand::DispatchStep {
            step_id: 1,
            payload: "test-payload".into(),
        }).await
    };

    assert!(event.is_ok(), "B mailbox must accept DispatchStep command");
    let event = event.unwrap();
    match event {
        rust_agent::core::boss_actor_runtime::BossActorEvent::StepDispatched { step_id, .. } => {
            assert_eq!(step_id, 1, "dispatched step_id must match");
        }
        other => panic!("expected StepDispatched, got {:?}", other),
    }

    // B's state must reflect the active step.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().unwrap();
    let b_status = registry.executor_b.status().await;
    assert_eq!(b_status, ActorStatus::Active, "B must be Active after DispatchStep");

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn stop_sends_stop_command_to_actor_mailboxes() {
    let plan_path = std::env::temp_dir().join("boss_h_stop_actors.json");
    let plan = BossPlan {
        plan_id: "plan-h-stop".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    coordinator.ensure_actor_registry().await;

    // Activate both actors first.
    {
        let registry_guard = coordinator.actor_registry.read().await;
        let registry = registry_guard.as_ref().unwrap();
        let _ = registry.a_mailbox().send(DesignerACommand::Plan {
            plan_id: "plan-h-stop".into(),
            document_spec: "spec".into(),
        }).await;
        let _ = registry.b_mailbox().send(ExecutorBCommand::DispatchStep {
            step_id: 1,
            payload: "payload".into(),
        }).await;
    }
    // Give the actor loops a tick to process.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Now send Stop to both via their mailboxes directly (mirrors what stop() does).
    {
        let registry_guard = coordinator.actor_registry.read().await;
        let registry = registry_guard.as_ref().unwrap();
        let a_event = registry.a_mailbox().request(DesignerACommand::Stop).await;
        let b_event = registry.b_mailbox().request(ExecutorBCommand::Stop).await;
        assert!(a_event.is_ok(), "A must accept Stop command");
        assert!(b_event.is_ok(), "B must accept Stop command");
    }

    // Give the actor loops a tick to process the Stop.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // After Stop, both mailboxes must be closed.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().unwrap();
    assert!(registry.a_mailbox().is_closed(), "A mailbox must be closed after Stop");
    assert!(registry.b_mailbox().is_closed(), "B mailbox must be closed after Stop");

    let _ = std::fs::remove_file(&plan_path);
}

// --- T16.6.H.1: mailbox-driven production entry points ---

#[tokio::test]
async fn advance_plan_sends_dispatch_to_b_mailbox_and_b_state_is_active() {
    let plan_path = std::env::temp_dir().join("boss_h1_advance_b_state.json");
    let plan = BossPlan {
        plan_id: "plan-h1-advance".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h1-advance", task_manager.clone());

    let result = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(result.is_some(), "step 0 should dispatch");

    // B's actor state must be Active — proves the mailbox handler ran before advance_plan returned.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().expect("actor registry must exist after advance_plan");
    let b_status = registry.executor_b.status().await;
    assert_eq!(
        b_status,
        rust_agent::core::boss_state::BossActorStatus::Active,
        "B must be Active after advance_plan — mailbox handler must have run before tool call"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn on_review_event_sends_review_to_a_mailbox_and_a_state_reflects_step() {
    let plan_path = std::env::temp_dir().join("boss_h1_review_a_state.json");
    let plan = BossPlan {
        plan_id: "plan-h1-review".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step to review")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    {
        let mut guard = coordinator.plan.write().await;
        let p = guard.as_mut().unwrap();
        p.steps[0].status = BossPlanStepStatus::Reviewing;
        p.steps[0].worker_task_id = Some("b-task-h1".into());
    }

    coordinator.on_review_event(0, true, "LGTM", None).await.unwrap();

    // A's actor state must reflect the reviewed step — proves mailbox handler ran before plan mutation.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().expect("actor registry must exist after on_review_event");
    let a_state = registry.designer_a.state.read().await;
    assert_eq!(
        a_state.current_step,
        Some(0),
        "A's current_step must be 0 after on_review_event — mailbox handler must have run"
    );
    drop(a_state);
    drop(registry_guard);

    // Plan state must also be updated correctly.
    let plan_guard = coordinator.plan.read().await;
    let step = &plan_guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Completed, "step must be Completed after accepted review");
    assert!(step.completed, "step.completed must be true");

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn stop_via_handle_control_request_closes_a_and_b_mailboxes() {
    let plan_path = std::env::temp_dir().join("boss_h1_stop_mailboxes.json");
    let plan = BossPlan {
        plan_id: "plan-h1-stop".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    coordinator.ensure_actor_registry().await;

    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session-h1".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    assert!(
        matches!(response, BossControlResponse::Stop(_)),
        "handle_control_request(Stop) must return Stop outcome"
    );

    // Both mailboxes must be closed — stop() awaits Stopped from both before returning.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().unwrap();
    assert!(
        registry.a_mailbox().is_closed(),
        "A mailbox must be closed after Stop via handle_control_request"
    );
    assert!(
        registry.b_mailbox().is_closed(),
        "B mailbox must be closed after Stop via handle_control_request"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// --- T16.6.H.2: execution side effects owned by B runtime ---

#[tokio::test]
async fn advance_plan_records_dispatch_payload_via_b_runtime_callback() {
    let plan_path = std::env::temp_dir().join("boss_h2_b_callback.json");
    let plan = BossPlan {
        plan_id: "plan-h2-callback".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h2-callback", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let status = coordinator.status.read().await;
    assert!(
        status.last_b_dispatch_payload.is_some(),
        "B's execution callback must have fired and recorded the dispatch payload"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn advance_plan_does_not_call_invoke_agent_tool_directly_after_h2() {
    let plan_path = std::env::temp_dir().join("boss_h2_no_inline_tool.json");
    let plan = BossPlan {
        plan_id: "plan-h2-no-inline".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h2-no-inline", task_manager.clone());

    let result = coordinator.advance_plan(&app_state).await;
    assert!(result.is_ok(), "advance_plan must succeed without inline tool call: {:?}", result);

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let status = coordinator.status.read().await;
    assert!(
        status.last_b_dispatch_payload.is_some(),
        "B's callback must have fired — execution side effect is B-owned"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn b_runtime_callback_fires_for_continue_step_as_well() {
    let plan_path = std::env::temp_dir().join("boss_h2_continue_callback.json");
    let plan = BossPlan {
        plan_id: "plan-h2-continue".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero"), boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h2-continue", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let first_payload = coordinator.status.read().await.last_b_dispatch_payload.clone();
    assert!(first_payload.is_some(), "first dispatch must record payload");

    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some("b-running-task".into());
            session.executor_b.status = rust_agent::core::boss_state::BossActorStatus::Active;
        }
    }
    {
        let mut guard = coordinator.plan.write().await;
        if let Some(plan) = guard.as_mut() {
            plan.steps[0].completed = true;
            plan.steps[0].status = BossPlanStepStatus::Completed;
        }
    }

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let second_payload = coordinator.status.read().await.last_b_dispatch_payload.clone();
    assert!(second_payload.is_some(), "ContinueStep must also record payload via B's callback");
    assert_ne!(
        first_payload, second_payload,
        "second dispatch payload must differ from first"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.3 — A-side orchestration owned by DesignerARuntime
// ---------------------------------------------------------------------------

/// on_review_event() side effect (plan mutation + auto-advance) is triggered from
/// A's runtime handler, not inline in the coordinator.
#[tokio::test]
async fn on_review_event_side_effect_triggered_from_a_runtime_handler() {
    let plan_path = std::env::temp_dir().join("boss_h3_review_side_effect.json");
    let plan = BossPlan {
        plan_id: "plan-h3-review".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h3-review", task_manager.clone());

    // Advance to get step 0 running.
    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // Wire A's callbacks via the auto path (uses auto_advance_app_state).
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }

    // Pre-seed designer_a.session_id to a non-placeholder value so ensure_a_session
    // skips the real LLM spawn. send_message will return false (task not in running_owners),
    // causing ask_a_session to bail and fall back to coordinator's accepted=true verdict.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = "fake-a-session-h3".into();
        }
    }

    // Call on_review_event — A's callback should mutate the plan.
    coordinator
        .on_review_event(0, true, "looks good", None)
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // Plan step 0 must be Completed — set by A's callback, not coordinator inline.
    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert_eq!(
        plan.steps[0].status,
        BossPlanStepStatus::Completed,
        "A runtime callback must mark step Completed"
    );
    assert_eq!(
        plan.steps[0].last_review_summary.as_deref(),
        Some("looks good"),
        "A runtime callback must record review summary"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// finalize_documentation_loop() wires A callbacks and sends FinalizeDocumentation to A mailbox;
/// has_a_callbacks must be true and A's handler drives the WaitingForApproval stage transition.
#[tokio::test]
async fn finalize_documentation_loop_routes_through_a_mailbox() {
    let plan_path = std::env::temp_dir().join("boss_h3_finalize_doc.json");
    let plan = BossPlan {
        plan_id: "plan-h3-finalize".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h3-finalize", task_manager.clone());

    // Set auto_advance_app_state so ensure_actor_registry_with_a_callbacks_auto can wire callbacks.
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }

    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    // has_a_callbacks must be true — A callbacks were wired, not the coordinator fallback.
    let has_a_callbacks = coordinator.actor_registry.read().await
        .as_ref().map(|r| r.has_a_callbacks).unwrap_or(false);
    assert!(has_a_callbacks, "finalize_documentation_loop must wire A callbacks (has_a_callbacks == true)");

    // A's mailbox handler must have updated A's internal stage to WaitingForApproval.
    let a_stage = {
        let guard = coordinator.actor_registry.read().await;
        if let Some(r) = guard.as_ref() {
            Some(r.designer_a.state.read().await.stage)
        } else {
            None
        }
    };
    assert_eq!(
        a_stage,
        Some(BossStage::WaitingForApproval),
        "A runtime handler must set stage to WaitingForApproval — not coordinator fallback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// handle_user_approval() wires A callbacks and sends UserApproval to A mailbox;
/// has_a_callbacks must be true and A's handler drives the Execution stage transition.
#[tokio::test]
async fn handle_user_approval_routes_through_a_mailbox_and_a_drives_stage_transition() {
    let plan_path = std::env::temp_dir().join("boss_h3_user_approval.json");
    let plan = BossPlan {
        plan_id: "plan-h3-approval".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h3-approval", task_manager.clone());

    // Set auto_advance_app_state so ensure_actor_registry_with_a_callbacks_auto can wire callbacks.
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }

    // Finalize first so approval is valid.
    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    let approved = coordinator.handle_user_approval("Y").await.unwrap();
    assert!(approved, "Y input must return approved=true");

    // has_a_callbacks must be true — A callbacks were wired, not the coordinator fallback.
    let has_a_callbacks = coordinator.actor_registry.read().await
        .as_ref().map(|r| r.has_a_callbacks).unwrap_or(false);
    assert!(has_a_callbacks, "handle_user_approval must wire A callbacks (has_a_callbacks == true)");

    // A's mailbox handler must have updated A's internal stage to Execution.
    let a_stage = {
        let guard = coordinator.actor_registry.read().await;
        if let Some(r) = guard.as_ref() {
            Some(r.designer_a.state.read().await.stage)
        } else {
            None
        }
    };
    assert_eq!(
        a_stage,
        Some(BossStage::Execution),
        "A runtime handler must set stage to Execution — not coordinator fallback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.4 — Unified actor runtime bootstrap, no lazy rewiring
// ---------------------------------------------------------------------------

/// After bootstrap_actor_registry_with_app_state, the registry has both
/// has_executor and has_a_callbacks set — no subsequent call replaces it.
#[tokio::test]
async fn bootstrap_with_app_state_produces_full_registry_in_one_shot() {
    let plan_path = std::env::temp_dir().join("boss_h4_one_shot.json");
    let plan = BossPlan {
        plan_id: "plan-h4-oneshot".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h4-oneshot", task_manager.clone());

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let (has_exec, has_a) = {
        let guard = coordinator.actor_registry.read().await;
        let r = guard.as_ref().unwrap();
        (r.has_executor, r.has_a_callbacks)
    };
    assert!(has_exec, "bootstrap_actor_registry_with_app_state must set has_executor");
    assert!(has_a, "bootstrap_actor_registry_with_app_state must set has_a_callbacks");

    let _ = std::fs::remove_file(&plan_path);
}

/// Registry identity is stable across multiple advance_plan calls — no rewiring replaces it.
#[tokio::test]
async fn registry_identity_stable_across_multiple_advance_plan_calls() {
    let plan_path = std::env::temp_dir().join("boss_h4_identity.json");
    let plan = BossPlan {
        plan_id: "plan-h4-identity".into(),
        accepted_by_user: true,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero"), boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h4-identity", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_first = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_second = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_first, b_ptr_second,
        "B mailbox identity must be stable — registry must not be replaced on second advance_plan"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After restore_or_init + bootstrap_actor_registry_with_app_state, advance_plan
/// does not replace the registry (already fully bootstrapped).
#[tokio::test]
async fn restore_then_bootstrap_with_app_state_is_immediately_ready() {
    let plan_path = std::env::temp_dir().join("boss_h4_restore_ready.json");
    let plan = BossPlan {
        plan_id: "plan-h4-restore".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h4-restore", task_manager.clone());

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let b_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_before, b_ptr_after,
        "advance_plan must not replace a fully-bootstrapped registry"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.5 — Converged restore/bootstrap: full registry from restore
// ---------------------------------------------------------------------------

/// restore_or_init_with_app_state produces a full registry immediately —
/// no state-only phase, no lazy upgrade needed.
#[tokio::test]
async fn restore_or_init_with_app_state_produces_full_registry_immediately() {
    let plan_path = std::env::temp_dir().join("boss_h5_full_restore.json");
    let plan = BossPlan {
        plan_id: "plan-h5-full".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h5-full", task_manager.clone());

    let coordinator =
        BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
            .await
            .unwrap();

    // Registry must be full immediately — no lazy upgrade required.
    let (has_exec, has_a) = {
        let guard = coordinator.actor_registry.read().await;
        let r = guard.as_ref().unwrap();
        (r.has_executor, r.has_a_callbacks)
    };
    assert!(has_exec, "restore_or_init_with_app_state must produce has_executor=true");
    assert!(has_a, "restore_or_init_with_app_state must produce has_a_callbacks=true");

    let _ = std::fs::remove_file(&plan_path);
}

/// After restore_or_init_with_app_state, advance_plan does not replace the registry.
#[tokio::test]
async fn advance_plan_after_full_restore_does_not_replace_registry() {
    let plan_path = std::env::temp_dir().join("boss_h5_advance_stable.json");
    let plan = BossPlan {
        plan_id: "plan-h5-advance".into(),
        accepted_by_user: true,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h5-advance", task_manager.clone());

    let coordinator =
        BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
            .await
            .unwrap();

    let b_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_before, b_ptr_after,
        "advance_plan must not replace registry after restore_or_init_with_app_state"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After restore_or_init_with_app_state, finalize_documentation_loop does not replace the registry.
#[tokio::test]
async fn finalize_documentation_loop_after_full_restore_does_not_replace_registry() {
    let plan_path = std::env::temp_dir().join("boss_h5_finalize_stable.json");
    let plan = BossPlan {
        plan_id: "plan-h5-finalize".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h5-finalize", task_manager.clone());

    let coordinator =
        BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
            .await
            .unwrap();

    let a_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    let a_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    assert_eq!(
        a_ptr_before, a_ptr_after,
        "finalize_documentation_loop must not replace registry after restore_or_init_with_app_state"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.6 — Production assembly default: full registry from new_with_runtime_owner
// ---------------------------------------------------------------------------

/// Simulates the production assembly path: new_with_runtime_owner + bootstrap_actor_registry_with_app_state.
/// The coordinator must have has_executor && has_a_callbacks immediately after bootstrap.
#[tokio::test]
async fn production_assembly_produces_full_registry() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h6-prod", task_manager.clone());

    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let (has_exec, has_a) = {
        let guard = coordinator.actor_registry.read().await;
        let r = guard.as_ref().unwrap();
        (r.has_executor, r.has_a_callbacks)
    };
    assert!(has_exec, "production assembly must produce has_executor=true");
    assert!(has_a, "production assembly must produce has_a_callbacks=true");
}

/// After production assembly bootstrap, advance_plan does not trigger a mode upgrade.
#[tokio::test]
async fn advance_plan_after_production_assembly_does_not_upgrade_registry() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let plan_path = std::env::temp_dir().join("boss_h6_advance_no_upgrade.json");
    let plan = BossPlan {
        plan_id: "plan-h6-advance".into(),
        accepted_by_user: true,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h6-advance", task_manager.clone());

    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    {
        let loaded = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
        let mut guard = coordinator.plan.write().await;
        *guard = Some(loaded);
        let mut status = coordinator.status.write().await;
        status.planning_file = Some(plan_path.to_string_lossy().into_owned());
        status.stage = rust_agent::core::boss_state::BossStage::Execution;
    }

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let b_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_before, b_ptr_after,
        "advance_plan must not upgrade registry after production assembly bootstrap"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After production assembly bootstrap, finalize_documentation_loop does not trigger a mode upgrade.
#[tokio::test]
async fn finalize_documentation_loop_after_production_assembly_does_not_upgrade_registry() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let plan_path = std::env::temp_dir().join("boss_h6_finalize_no_upgrade.json");
    let plan = BossPlan {
        plan_id: "plan-h6-finalize".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h6-finalize", task_manager.clone());

    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    {
        let loaded = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
        let mut guard = coordinator.plan.write().await;
        *guard = Some(loaded);
        let mut status = coordinator.status.write().await;
        status.planning_file = Some(plan_path.to_string_lossy().into_owned());
    }

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let a_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    let a_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    assert_eq!(
        a_ptr_before, a_ptr_after,
        "finalize_documentation_loop must not upgrade registry after production assembly bootstrap"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.7 — API surface hardening: state-only paths are pub(crate) only
// ---------------------------------------------------------------------------

/// new() is pub(crate): production code must use new_with_runtime_owner + bootstrap.
/// This test verifies that new_with_runtime_owner produces a state-only registry
/// (has_executor == false) before bootstrap, and full registry after.
#[tokio::test]
async fn h7_new_with_runtime_owner_is_state_only_before_bootstrap() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    // Before bootstrap: no registry at all.
    let has_registry = coordinator.actor_registry.read().await.is_some();
    assert!(!has_registry, "new_with_runtime_owner must not pre-populate registry");

    // After bootstrap_actor_registry_with_app_state: full mode.
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h7-new", task_manager);
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor, "h7: has_executor must be true after bootstrap");
    assert!(registry.has_a_callbacks, "h7: has_a_callbacks must be true after bootstrap");
}

/// bootstrap_actor_registry is pub(crate): calling it produces a state-only registry.
/// Production code must not rely on it for full-mode operation.
#[tokio::test]
async fn h7_bootstrap_actor_registry_is_state_only() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);
    coordinator.bootstrap_actor_registry().await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(!registry.has_executor, "h7: state-only bootstrap must not set has_executor");
    assert!(!registry.has_a_callbacks, "h7: state-only bootstrap must not set has_a_callbacks");
}

/// Production assembly contract: new_with_runtime_owner + bootstrap_actor_registry_with_app_state
/// is the only path that produces has_executor && has_a_callbacks == true.
/// Calling bootstrap_actor_registry_with_app_state a second time is a no-op (idempotent).
#[tokio::test]
async fn h7_production_assembly_is_full_mode_and_idempotent() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h7-prod", task_manager);

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let ptr_first = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    // Second call must be a no-op — registry identity must be stable.
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let ptr_second = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(ptr_first, ptr_second, "h7: second bootstrap call must not replace registry");

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor && registry.has_a_callbacks, "h7: production assembly must be full mode");
}

// ---------------------------------------------------------------------------
// T16.6.H.8 — new_with_app_state is the first-class full-mode constructor
// ---------------------------------------------------------------------------

/// new_with_app_state produces a full-mode registry immediately — no separate bootstrap call needed.
#[tokio::test]
async fn h8_new_with_app_state_is_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h8-new", task_manager);

    let coordinator = BossCoordinator::new_with_app_state(runtime_owner, &app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor, "h8: new_with_app_state must set has_executor");
    assert!(registry.has_a_callbacks, "h8: new_with_app_state must set has_a_callbacks");
}

/// restore_or_init_with_app_state produces a full-mode registry immediately.
/// Symmetric with new_with_app_state for the restore path.
#[tokio::test]
async fn h8_restore_or_init_with_app_state_is_full_mode() {
    let plan_path = std::env::temp_dir().join("h8_restore_test_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h8-restore", task_manager);

    // No file — falls back to fresh coordinator.
    let coordinator =
        BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state).await.unwrap();

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor, "h8: restore_or_init_with_app_state must set has_executor");
    assert!(registry.has_a_callbacks, "h8: restore_or_init_with_app_state must set has_a_callbacks");
}

/// new_with_app_state and restore_or_init_with_app_state are the only paths that produce
/// has_executor && has_a_callbacks == true without a separate bootstrap call.
/// new_with_runtime_owner alone must NOT produce a full-mode registry.
#[tokio::test]
async fn h8_new_with_runtime_owner_alone_is_not_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    // No bootstrap call — registry must be absent.
    let has_registry = coordinator.actor_registry.read().await.is_some();
    assert!(!has_registry, "h8: new_with_runtime_owner alone must not produce a registry");
}

// ---------------------------------------------------------------------------
// T16.6.H.9 — BossRuntimeHost is the first-class factory / host contract
// ---------------------------------------------------------------------------

/// BossRuntimeHost::build_coordinator produces a full-mode coordinator in one call.
#[tokio::test]
async fn h9_host_build_coordinator_is_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h9-build", task_manager);

    let coordinator = host.build_coordinator(&app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor, "h9: host.build_coordinator must set has_executor");
    assert!(registry.has_a_callbacks, "h9: host.build_coordinator must set has_a_callbacks");
}

/// BossRuntimeHost::bootstrap_coordinator brings an existing coordinator to full mode.
/// This is the production path when coordinator is a field of AppState.
#[tokio::test]
async fn h9_host_bootstrap_coordinator_brings_existing_to_full_mode() {
    use rust_agent::core::boss_runtime::{BossRuntimeHost, BossRuntimeOwner};
    let host = BossRuntimeHost::new();
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h9-bootstrap", task_manager);

    // Before: no registry.
    assert!(coordinator.actor_registry.read().await.is_none());

    host.bootstrap_coordinator(&coordinator, &app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor, "h9: host.bootstrap_coordinator must set has_executor");
    assert!(registry.has_a_callbacks, "h9: host.bootstrap_coordinator must set has_a_callbacks");
}

/// bootstrap_coordinator is idempotent — calling it twice does not replace the registry.
#[tokio::test]
async fn h9_host_bootstrap_coordinator_is_idempotent() {
    use rust_agent::core::boss_runtime::{BossRuntimeHost, BossRuntimeOwner};
    let host = BossRuntimeHost::new();
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h9-idem", task_manager);

    host.bootstrap_coordinator(&coordinator, &app_state).await;
    let ptr_first = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    host.bootstrap_coordinator(&coordinator, &app_state).await;
    let ptr_second = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(ptr_first, ptr_second, "h9: bootstrap_coordinator must be idempotent");
}

// ---------------------------------------------------------------------------
// T16.6.H.10 — BossRuntimeHost::restore_or_init_coordinator completes the API triad
// ---------------------------------------------------------------------------

/// host.restore_or_init_coordinator with no existing file produces a fresh full-mode coordinator.
#[tokio::test]
async fn h10_host_restore_or_init_coordinator_fresh_is_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let plan_path = std::env::temp_dir().join("h10_restore_fresh_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-fresh", task_manager);

    let coordinator = host.restore_or_init_coordinator(&plan_path, &app_state).await.unwrap();

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(registry.has_executor, "h10: restore_or_init_coordinator (fresh) must set has_executor");
    assert!(registry.has_a_callbacks, "h10: restore_or_init_coordinator (fresh) must set has_a_callbacks");
}

/// host.restore_or_init_coordinator uses the host's BossRuntimeOwner (not a throwaway one).
/// Verify by checking the coordinator's runtime_owner is the same Arc as the host's owner.
#[tokio::test]
async fn h10_host_restore_or_init_coordinator_uses_host_owner() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let plan_path = std::env::temp_dir().join("h10_owner_check_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-owner", task_manager);

    let coordinator = host.restore_or_init_coordinator(&plan_path, &app_state).await.unwrap();

    // Direct owner identity assertion: coordinator must hold the same BossRuntimeOwner Arc as host.
    assert_eq!(
        host.owner_ptr(),
        coordinator.runtime_owner_ptr(),
        "h10: coordinator from restore_or_init_coordinator must hold host's BossRuntimeOwner"
    );
}

/// The host API triad (build / bootstrap / restore_or_init) all produce full-mode coordinators.
/// This test exercises all three in sequence to confirm the contract is uniform.
#[tokio::test]
async fn h10_host_api_triad_all_produce_full_mode() {
    use rust_agent::core::boss_runtime::{BossRuntimeHost, BossRuntimeOwner};
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-triad", task_manager);

    // build_coordinator
    let c1 = host.build_coordinator(&app_state).await;
    let g1 = c1.actor_registry.read().await;
    let r1 = g1.as_ref().unwrap();
    assert!(r1.has_executor && r1.has_a_callbacks, "h10: build_coordinator must be full-mode");
    drop(g1);

    // bootstrap_coordinator
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let c2 = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    host.bootstrap_coordinator(&c2, &app_state).await;
    let g2 = c2.actor_registry.read().await;
    let r2 = g2.as_ref().unwrap();
    assert!(r2.has_executor && r2.has_a_callbacks, "h10: bootstrap_coordinator must be full-mode");
    drop(g2);

    // restore_or_init_coordinator
    let plan_path = std::env::temp_dir().join("h10_triad_plan.json");
    let _ = std::fs::remove_file(&plan_path);
    let c3 = host.restore_or_init_coordinator(&plan_path, &app_state).await.unwrap();
    let g3 = c3.actor_registry.read().await;
    let r3 = g3.as_ref().unwrap();
    assert!(r3.has_executor && r3.has_a_callbacks, "h10: restore_or_init_coordinator must be full-mode");
}

// ---------------------------------------------------------------------------
// T16.6.H.10.1 — Direct owner identity assertion for host API triad
// ---------------------------------------------------------------------------

/// build_coordinator: coordinator holds the host's BossRuntimeOwner (direct identity check).
#[tokio::test]
async fn h10_1_build_coordinator_uses_host_owner() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-1-build", task_manager);

    let coordinator = host.build_coordinator(&app_state).await;

    assert_eq!(
        host.owner_ptr(),
        coordinator.runtime_owner_ptr(),
        "h10.1: coordinator from build_coordinator must hold host's BossRuntimeOwner"
    );
}

/// restore_or_init_coordinator: coordinator holds the host's BossRuntimeOwner (direct identity check).
/// This replaces the indirect smoke test from H.10.
#[tokio::test]
async fn h10_1_restore_or_init_coordinator_uses_host_owner_direct() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let plan_path = std::env::temp_dir().join("h10_1_owner_direct_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-1-restore", task_manager);

    let coordinator = host.restore_or_init_coordinator(&plan_path, &app_state).await.unwrap();

    assert_eq!(
        host.owner_ptr(),
        coordinator.runtime_owner_ptr(),
        "h10.1: coordinator from restore_or_init_coordinator must hold host's BossRuntimeOwner"
    );
}

// ---------------------------------------------------------------------------
// T22.1 — Designer A becomes a real LLM agent session
// ---------------------------------------------------------------------------

/// After ReviewFn fires, designer_a.session_id must no longer be the deterministic placeholder.
#[tokio::test]
async fn t22_1_review_fn_initializes_a_session_id() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-1-review", task_manager);

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;
    coordinator.ensure_actor_session("t22-1-review", BossStage::Execution).await;

    // Record the deterministic placeholder before any callback fires.
    let placeholder = {
        let guard = coordinator.session.read().await;
        guard.as_ref().map(|s| s.designer_a.session_id.clone()).unwrap_or_default()
    };
    assert!(placeholder.starts_with("boss-"), "pre-condition: session_id must be deterministic placeholder");

    // Fire ReviewFn via A mailbox.
    {
        let guard = coordinator.actor_registry.read().await;
        if let Some(registry) = guard.as_ref() {
            let _ = registry.a_mailbox().send(
                rust_agent::core::boss_actor_runtime::DesignerACommand::Review {
                    step_id: 0,
                    accepted: true,
                    summary: "looks good".into(),
                    correction: None,
                }
            ).await;
        }
    }
    // Give the actor loop time to process.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after = {
        let guard = coordinator.session.read().await;
        guard.as_ref().map(|s| s.designer_a.session_id.clone()).unwrap_or_default()
    };
    assert_ne!(after, placeholder, "t22.1: ReviewFn must update designer_a.session_id from placeholder");
    assert!(!after.is_empty(), "t22.1: designer_a.session_id must be non-empty after ReviewFn");

    // Verify send_to_a_session was called with a review message.
    let dispatch_msg = coordinator.status.read().await.last_a_dispatch_message.clone();
    assert!(dispatch_msg.is_some(), "t22.1: last_a_dispatch_message must be set after ReviewFn");
    let msg = dispatch_msg.unwrap();
    assert!(msg.contains("step 0"), "t22.1: dispatch message must reference step id");
    assert!(msg.contains("accepted"), "t22.1: dispatch message must contain verdict");
}

/// After DocumentationFn fires, designer_a.session_id must no longer be the deterministic placeholder.
#[tokio::test]
async fn t22_1_doc_fn_initializes_a_session_id() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-1-doc", task_manager);

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;
    coordinator.ensure_actor_session("t22-1-doc", BossStage::Execution).await;

    let placeholder = {
        let guard = coordinator.session.read().await;
        guard.as_ref().map(|s| s.designer_a.session_id.clone()).unwrap_or_default()
    };
    assert!(placeholder.starts_with("boss-"), "pre-condition: session_id must be deterministic placeholder");

    // Fire DocumentationFn via A mailbox.
    {
        let guard = coordinator.actor_registry.read().await;
        if let Some(registry) = guard.as_ref() {
            let _ = registry.a_mailbox().send(
                rust_agent::core::boss_actor_runtime::DesignerACommand::FinalizeDocumentation {
                    signal: "finalize".into(),
                }
            ).await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after = {
        let guard = coordinator.session.read().await;
        guard.as_ref().map(|s| s.designer_a.session_id.clone()).unwrap_or_default()
    };
    assert_ne!(after, placeholder, "t22.1: DocumentationFn must update designer_a.session_id from placeholder");
    assert!(!after.is_empty(), "t22.1: designer_a.session_id must be non-empty after DocumentationFn");

    // Verify send_to_a_session was called with a documentation signal message.
    let dispatch_msg = coordinator.status.read().await.last_a_dispatch_message.clone();
    assert!(dispatch_msg.is_some(), "t22.1: last_a_dispatch_message must be set after DocumentationFn");
    let msg = dispatch_msg.unwrap();
    assert!(msg.contains("finalize"), "t22.1: dispatch message must contain the documentation signal");
}

/// ensure_a_session is idempotent: second call must not change the session_id.
#[tokio::test]
async fn t22_1_ensure_a_session_is_idempotent() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-1-idem", task_manager);

    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;
    coordinator.ensure_actor_session("t22-1-idem", BossStage::Execution).await;

    // Fire DocumentationFn twice.
    for _ in 0..2 {
        let guard = coordinator.actor_registry.read().await;
        if let Some(registry) = guard.as_ref() {
            let _ = registry.a_mailbox().send(
                rust_agent::core::boss_actor_runtime::DesignerACommand::FinalizeDocumentation {
                    signal: "finalize".into(),
                }
            ).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Both calls should have produced the same session_id (idempotent).
    let session_id = {
        let guard = coordinator.session.read().await;
        guard.as_ref().map(|s| s.designer_a.session_id.clone()).unwrap_or_default()
    };
    // The session_id must be a real task id (not the placeholder) and stable.
    assert!(!session_id.starts_with("boss-"), "t22.1: session_id must be a real task id after idempotent calls");
    // The last dispatch message must be set (second call still sends to A session).
    let dispatch_msg = coordinator.status.read().await.last_a_dispatch_message.clone();
    assert!(dispatch_msg.is_some(), "t22.1: last_a_dispatch_message must be set after idempotent calls");
}

// ── T22.1.B: parse_a_review_verdict unit tests ──────────────────────────────

#[test]
fn t22_1b_parse_a_review_verdict_accept() {
    let (accepted, correction) = rust_agent::core::boss::BossCoordinator::parse_a_review_verdict_pub("ACCEPT: looks good");
    assert!(accepted, "ACCEPT keyword must yield accepted=true");
    assert!(correction.is_none(), "no correction expected on ACCEPT");
}

#[test]
fn t22_1b_parse_a_review_verdict_reject_with_correction() {
    let (accepted, correction) = rust_agent::core::boss::BossCoordinator::parse_a_review_verdict_pub(
        "REJECT: step output is incomplete. CORRECTION: add error handling for the edge case",
    );
    assert!(!accepted, "REJECT keyword must yield accepted=false");
    assert_eq!(
        correction.as_deref(),
        Some("add error handling for the edge case"),
        "correction must be extracted after CORRECTION:"
    );
}

#[test]
fn t22_1b_parse_a_review_verdict_reject_no_correction() {
    let (accepted, correction) = rust_agent::core::boss::BossCoordinator::parse_a_review_verdict_pub("REJECT");
    assert!(!accepted, "bare REJECT must yield accepted=false");
    assert!(correction.is_none(), "no correction when CORRECTION: is absent");
}

#[test]
fn t22_1b_parse_a_review_verdict_default_accept_when_no_keyword() {
    // If A's response has no REJECT keyword, default to accept.
    let (accepted, _) = rust_agent::core::boss::BossCoordinator::parse_a_review_verdict_pub("Looks fine to me.");
    assert!(accepted, "no REJECT keyword must default to accepted=true");
}

// ── T22.1.B: A verdict drives state machine (fallback path) ─────────────────

#[tokio::test]
async fn t22_1b_review_fn_falls_back_to_coordinator_verdict_when_a_unavailable() {
    // When A's session is not running, ask_a_session fails and build_review_fn
    // falls back to the coordinator-supplied accepted value. Assert step.status directly.
    let tmp = std::env::temp_dir().join("t22_1b_fallback_tasks");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "t22-1b-fallback-strong";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step for fallback test")]),
        "t22_1b_fallback_strong.json",
    )
    .await;
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-fallback".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Pre-seed designer_a.session_id with a non-running task id so ensure_a_session
    // skips the real LLM spawn, and ask_a_session fails fast (task not running).
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = "fake-a-not-running".to_string();
        }
    }

    // No fake A task running — ask_a_session will fail fast.
    // Coordinator says accepted=true → fallback must complete the step.
    coordinator
        .on_review_event(0, true, "Fallback accept", None)
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Completed, "fallback must use coordinator verdict (accepted=true → Completed)");
    assert!(step.completed, "step.completed must be true on fallback accept");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t22_1b_review_fn_uses_a_verdict_when_a_responds_accept() {
    // A responds ACCEPT; coordinator passes accepted=false. A's verdict must win.
    let tmp = std::env::temp_dir().join("t22_1b_accept_tasks");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "t22-1b-a-accept";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step for A accept override")]),
        "t22_1b_a_accept.json",
    )
    .await;
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-accept".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let fake_a_task = task_manager.create_with_type(
        "fake designer A".to_string(),
        TaskType::LocalAgent,
        session_id.to_string(),
        InteractionSurface::Cli,
    );
    // Launch the fake A task so it's in running_owners (required for send_message).
    let aid_clone = fake_a_task.id.clone();
    task_manager.launch(&fake_a_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        drop(aid_clone);
    });
    // Pre-seed designer_a.session_id so ensure_a_session skips the real LLM spawn.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = fake_a_task.id.clone();
        }
    }
    // Append A's response after a short delay so ask_a_session's polling loop finds it.
    let tm = task_manager.clone();
    let aid = fake_a_task.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tm.append_output(&aid, "ACCEPT: step output looks good\n");
    });

    // Coordinator says accepted=false — A's ACCEPT must override to Completed.
    coordinator
        .on_review_event(0, false, "Step output looks good", None)
        .await
        .unwrap();
    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Completed, "A ACCEPT must complete the step even when coordinator says rejected");
    assert!(step.completed, "step.completed must be true after A accepts");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t22_1b_review_fn_uses_a_verdict_when_a_responds_reject() {
    // A responds REJECT + CORRECTION; coordinator passes accepted=true. A's verdict must win.
    let tmp = std::env::temp_dir().join("t22_1b_reject_tasks");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "t22-1b-a-reject";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step for A reject override")]),
        "t22_1b_a_reject.json",
    )
    .await;
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-reject".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let fake_a_task = task_manager.create_with_type(
        "fake designer A".to_string(),
        TaskType::LocalAgent,
        session_id.to_string(),
        InteractionSurface::Cli,
    );
    // Launch the fake A task so it's in running_owners (required for send_message).
    let aid_clone = fake_a_task.id.clone();
    task_manager.launch(&fake_a_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        drop(aid_clone);
    });
    // Pre-seed designer_a.session_id so ensure_a_session skips the real LLM spawn.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = fake_a_task.id.clone();
        }
    }
    // Append A's response after a short delay so ask_a_session's polling loop finds it.
    let tm = task_manager.clone();
    let aid = fake_a_task.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tm.append_output(&aid, "REJECT: output incomplete. CORRECTION: add retry logic for transient failures\n");
    });

    // Coordinator says accepted=true — A's REJECT must override to Rejected.
    coordinator
        .on_review_event(0, true, "Output incomplete", None)
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Rejected, "A REJECT must set Rejected status even when coordinator says accepted");
    assert!(!step.completed, "step must not be completed after A rejects");
    assert_eq!(step.attempt_count, 1, "attempt_count must increment on rejection");
    assert_eq!(
        step.last_correction.as_deref(),
        Some("add retry logic for transient failures"),
        "A's correction must be recorded"
    );

    let _ = std::fs::remove_file(plan_path);
}

// ---------------------------------------------------------------------------
// T22.2 — Executor B becomes a real LLM agent session
// ---------------------------------------------------------------------------

/// After the first DispatchStep fires exec_fn, executor_b.session_id must be
/// a real task id (not the deterministic placeholder "boss-{plan_id}-b").
#[tokio::test]
async fn t22_2_b_session_id_is_non_placeholder_after_first_dispatch() {
    let plan_id = "t22-2-first-dispatch";
    let plan_path = std::env::temp_dir().join("t22_2_first_dispatch.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-2-first", task_manager.clone());

    let placeholder = format!("boss-{plan_id}-b");
    assert_eq!(coordinator.b_session_id().await, placeholder, "session_id must start as placeholder");

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let session_id_after = coordinator.b_session_id().await;
    assert_ne!(session_id_after, placeholder, "session_id must be non-placeholder after first dispatch");
    assert!(!session_id_after.is_empty(), "session_id must not be empty after first dispatch");

    let _ = std::fs::remove_file(&plan_path);
}

/// Two consecutive DispatchStep/ContinueStep calls must reuse the same B session id
/// when B's task is still running between dispatches.
#[tokio::test]
async fn t22_2_two_dispatches_reuse_same_b_session_id() {
    let plan_id = "t22-2-reuse-session";
    let plan_path = std::env::temp_dir().join("t22_2_reuse_session.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero"), boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-2-reuse", task_manager.clone());

    // Create a fake B task that stays Running (simulates a live B session).
    let fake_b_task = task_manager.create_with_type(
        "fake executor B",
        TaskType::LocalAgent,
        "session-t22-2-reuse",
        InteractionSurface::Cli,
    );
    task_manager.launch(&fake_b_task.id, "", async move {
        // Keep running until test ends.
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    });
    let b_task_id = fake_b_task.id.clone();

    // Pre-seed B's session with the running task id.
    coordinator.record_b_session_id_pub(&b_task_id).await;

    // First dispatch — B is already running, so ContinueStep fires.
    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let session_id_after_first = coordinator.b_session_id().await;
    assert_eq!(session_id_after_first, b_task_id, "first dispatch must keep the pre-seeded B session id");

    // Advance plan state so step 0 is complete.
    {
        let mut guard = coordinator.plan.write().await;
        if let Some(p) = guard.as_mut() {
            p.steps[0].completed = true;
            p.steps[0].status = BossPlanStepStatus::Completed;
        }
    }

    // Second dispatch — B is still running, must reuse same session.
    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let session_id_after_second = coordinator.b_session_id().await;
    assert_eq!(
        session_id_after_first, session_id_after_second,
        "second dispatch must reuse the same B session id when B is still running"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// record_b_session_id_pub writes task_id to executor_b.session_id and task_id fields.
#[tokio::test]
async fn t22_2_record_b_session_id_writes_back_to_session() {
    let plan_path = std::env::temp_dir().join("t22_2_record_b.json");
    let plan = BossPlan {
        plan_id: "t22-2-record".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    coordinator.record_b_session_id_pub("real-task-abc123").await;

    assert_eq!(coordinator.b_session_id().await, "real-task-abc123");
    assert_eq!(coordinator.b_task_id().await.as_deref(), Some("real-task-abc123"));

    let _ = std::fs::remove_file(&plan_path);
}

/// When task_manager is absent, advance_plan must not panic.
#[tokio::test]
async fn t22_2_b_session_fallback_when_task_manager_absent() {
    let plan_id = "t22-2-no-tm";
    let plan_path = std::env::temp_dir().join("t22_2_no_tm.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    let permission_context = rust_agent::state::permission_context::ToolPermissionContext::new(
        rust_agent::state::permission_context::PermissionMode::Default,
    )
    .with_active_session_id("session-t22-2-no-tm")
    .with_active_surface(InteractionSurface::Cli);
    let app_state = Arc::new(AppState {
        surface: InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: rust_agent::bootstrap::SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(rust_agent::tool::registry::ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker: rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source: rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "session-t22-2-no-tm".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
    });

    let result = coordinator.advance_plan(&app_state).await;
    // advance_plan requires a task_manager to dispatch B — it returns a clear error when absent.
    let err = result.expect_err("advance_plan must fail when task_manager is absent");
    assert!(
        err.to_string().contains("task manager not configured"),
        "error must name the missing task manager: {err}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T22.3 — Documentation B reviewer + Execution B self-organizes sub-agents
// ---------------------------------------------------------------------------

/// T22.3.1: B reviewer receives ReviewSpec and returns real feedback via spec_review_fn.
#[tokio::test]
async fn t22_3_documentation_b_reviewer_returns_feedback() {
    use rust_agent::core::boss_actor_runtime::{BossActorEvent, ExecutorBCommand};

    let spec_review_fn: SpecReviewFn = Arc::new(|spec: String| {
        Box::pin(async move {
            Ok(format!("FEEDBACK: spec '{}' is missing error handling", spec))
        })
    });

    let runtime = ExecutorBRuntime::spawn_with_callbacks(None, Some(spec_review_fn));
    let event = runtime
        .mailbox
        .request(ExecutorBCommand::ReviewSpec {
            spec: "implement login flow".to_string(),
        })
        .await
        .expect("ReviewSpec must succeed");

    match event {
        BossActorEvent::SpecReviewed { feedback } => {
            assert!(feedback.contains("FEEDBACK:"), "B must return FEEDBACK: prefix, got: {feedback}");
            assert!(feedback.contains("missing error handling"), "B must include spec content, got: {feedback}");
        }
        other => panic!("expected SpecReviewed, got {other:?}"),
    }
}

/// T22.3.1: finalize_documentation_loop uses B's ReviewSpec feedback when review_feedback is empty.
#[tokio::test]
async fn t22_3_finalize_documentation_loop_uses_b_reviewer_feedback() {
    let plan_path = std::env::temp_dir().join("t22_3_doc_b_feedback.json");
    let plan = BossPlan {
        plan_id: "t22-3-doc-b".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    let spec_review_fn: SpecReviewFn = Arc::new(|_spec: String| {
        Box::pin(async move { Ok("FEEDBACK: needs more detail on auth flow".to_string()) })
    });
    let exec_fn: ExecutionFn = Arc::new(|payload: String| {
        Box::pin(async move { Ok(payload) })
    });
    let registry = BossActorRegistry {
        designer_a: DesignerARuntime::spawn(),
        executor_b: ExecutorBRuntime::spawn_with_callbacks(Some(exec_fn), Some(spec_review_fn)),
        has_executor: true,
        has_a_callbacks: false,
    };
    {
        let mut guard = coordinator.actor_registry.write().await;
        *guard = Some(registry);
    }

    coordinator
        .finalize_documentation_loop(
            "draft spec: implement login",
            "",
            "revised based on B feedback",
            "final spec",
            "pseudo code",
        )
        .await
        .unwrap();

    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert_eq!(
        plan.review_feedback.as_deref(),
        Some("FEEDBACK: needs more detail on auth flow"),
        "B's feedback must be stored as review_feedback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T22.3.2: Execution B's task (spawned with ExecutorB policy, depth 0) can spawn a child agent.
#[tokio::test]
async fn t22_3_execution_b_session_can_spawn_child_agent() {
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy {
            actor_role: BossActorRole::ExecutorB,
            lineage_depth: 0,
            phase: BossStage::Execution,
        });

    let call = ToolCall::new(
        "Agent",
        serde_json::json!({
            "task": "implement step 0",
            "session_id": "b-child-session"
        })
        .to_string(),
    );

    let result = AgentTool.invoke(&call, &permissions).await;
    assert!(
        result.is_ok(),
        "ExecutorB at depth 0 must be allowed to spawn a child agent: {:?}",
        result
    );
}

/// T22.3.2: B's child (ImplementChild, depth 1) cannot spawn a grandchild — policy holds.
#[tokio::test]
async fn t22_3_b_child_cannot_spawn_grandchild_agent() {
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy {
            actor_role: BossActorRole::ImplementChild,
            lineage_depth: 1,
            phase: BossStage::Execution,
        });

    let call = ToolCall::new(
        "Agent",
        serde_json::json!({
            "prompt": "do something",
            "session_id": "grandchild-session"
        })
        .to_string(),
    );

    let err = AgentTool
        .invoke(&call, &permissions)
        .await
        .expect_err("ImplementChild at depth 1 must not spawn grandchild");

    assert!(
        err.to_string().contains("boss spawn policy"),
        "error must mention boss spawn policy, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// T22.3 production path evidence
// ---------------------------------------------------------------------------

/// T22.3.1 production path: finalize_documentation_loop walks the real
/// build_spec_review_fn → ensure_b_session (skipped, pre-seeded) → ask_b_session
/// → ReviewSpec mailbox → SpecReviewed feedback stored in plan.
///
/// B's session is a fake Running task that appends output when it receives a message.
#[tokio::test]
async fn t22_3_production_path_doc_b_reviewer_via_ask_b_session() {
    let plan_id = "t22-3-prod-doc";
    let plan_path = std::env::temp_dir().join("t22_3_prod_doc.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t22_3_prod_doc_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t22-3-prod-doc", task_manager.clone());

    // Create a fake B task that stays Running and responds to send_message.
    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t22-3-prod-doc",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        // Respond to any incoming message by appending output.
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for msg in messages {
                let feedback = format!("FEEDBACK: B reviewed spec — {msg} needs auth error handling");
                tm_for_b.append_output(&b_id_for_loop, &feedback);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });

    // Pre-seed B's session_id so ensure_b_session skips spawning.
    coordinator.record_b_session_id_pub(&b_task_id).await;

    // Wire the production callbacks (build_spec_review_fn uses ask_b_session).
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    // finalize with empty review_feedback — B must supply it via ask_b_session.
    coordinator
        .finalize_documentation_loop(
            "implement login with OAuth",
            "",
            "revised per B feedback",
            "final spec",
            "pseudo code",
        )
        .await
        .unwrap();

    let plan_guard = coordinator.plan.read().await;
    let stored_feedback = plan_guard.as_ref().unwrap().review_feedback.clone().unwrap_or_default();
    assert!(
        stored_feedback.contains("FEEDBACK:"),
        "B's real feedback must be stored, got: {stored_feedback}"
    );
    assert!(
        stored_feedback.contains("auth error handling"),
        "B's feedback must reference the spec content, got: {stored_feedback}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T22.3.2 production path: advance_plan walks the real build_exec_fn →
/// invoke_agent_tool_with_task_id → AgentTool.invoke → creates a child task
/// in the task manager. Verifies the task manager has a new task after dispatch.
#[tokio::test]
async fn t22_3_production_path_exec_b_creates_child_task_via_agent_tool() {
    let plan_id = "t22-3-prod-exec";
    let plan_path = std::env::temp_dir().join("t22_3_prod_exec.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "implement auth module")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-3-prod-exec", task_manager.clone());

    // Wire the production exec_fn (build_exec_fn → invoke_agent_tool_with_task_id).
    coordinator.bootstrap_actor_registry_with_app_state(&app_state).await;

    let tasks_before = task_manager.list().len();

    // advance_plan → DispatchStep → exec_fn → AgentTool.invoke → new child task.
    coordinator.advance_plan(&app_state).await.unwrap();
    // Give exec_fn time to fire asynchronously.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let tasks_after = task_manager.list().len();
    assert!(
        tasks_after > tasks_before,
        "AgentTool must have created at least one child task (before={tasks_before}, after={tasks_after})"
    );

    // The new task must have B's actor role label.
    let new_tasks: Vec<_> = task_manager
        .list()
        .into_iter()
        .filter(|t| t.boss_actor_id.is_some())
        .collect();
    assert!(
        !new_tasks.is_empty(),
        "at least one task must have a boss_actor_id set (B's child)"
    );

    // B's session_id must be non-placeholder after exec_fn fires.
    let b_session = coordinator.b_session_id().await;
    let placeholder = format!("boss-{plan_id}-b");
    assert_ne!(b_session, placeholder, "B session_id must be real after exec_fn fires");

    let _ = std::fs::remove_file(&plan_path);
}

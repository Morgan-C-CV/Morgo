// R1 slice 2 — LisMAbSampleSink wired into boss runtime
// Tests that advance_plan automatically records samples on completed/aborted paths.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::boss::{BossCoordinator, save_plan};
use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
use rust_agent::core::boss_test_readiness::BossTestRunOutcome;
use rust_agent::core::lism_ab_sample::{LisMAbSampleSink, new_shared_ab_sink};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus, TaskType, TaskUsageSummary};
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

// ── helpers ───────────────────────────────────────────────────────────────────

fn unique_plan_path(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("r1-2-{tag}-{nanos}.json"))
}

fn boss_step(id: usize, desc: &str) -> BossPlanStep {
    BossPlanStep {
        id,
        description: desc.into(),
        objective: Some(format!("objective {id}")),
        acceptance: vec![format!("acceptance {id}")],
        completed: false,
        status: BossPlanStepStatus::Pending,
        result_diff: None,
        requires_approval: false,
        worker_task_id: None,
        attempt_count: 0,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        stage_continuation_context: None,
        executor_b_stage_memory: None,
        review_task_id: None,
        tool_execution_records: Vec::new(),
    }
}

fn all_completed_plan(plan_id: &str, n_steps: usize) -> BossPlan {
    BossPlan {
        plan_id: plan_id.into(),
        task_description: "Test task".into(),
        steps: (0..n_steps)
            .map(|i| BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(i, &format!("Step {i}"))
            })
            .collect(),
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

fn pending_plan_with_failed_step(plan_id: &str) -> BossPlan {
    BossPlan {
        plan_id: plan_id.into(),
        task_description: "Test task".into(),
        steps: vec![
            BossPlanStep {
                completed: false,
                status: BossPlanStepStatus::Failed,
                ..boss_step(0, "Failed step")
            },
            boss_step(1, "Pending step"),
        ],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

fn make_app_state(session_id: &str) -> Arc<AppState> {
    let task_manager = Arc::new(TaskManager::default());
    make_app_state_with_task_manager(session_id, task_manager)
}

fn make_app_state_with_task_manager(
    session_id: &str,
    task_manager: Arc<TaskManager>,
) -> Arc<AppState> {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager)
        .with_active_session_id(session_id)
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
            provider_id: "test".into(),
            protocol: "test".into(),
            compatibility_profile: "test".into(),
            base_url_host: "localhost".into(),
            model: "test-model".into(),
            auth_status: "none".into(),
        },
        active_session_id: session_id.into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    })
}

async fn coordinator_with_sink(
    plan: BossPlan,
    path: &std::path::Path,
    sink: std::sync::Arc<LisMAbSampleSink>,
) -> Arc<BossCoordinator> {
    save_plan(&plan, path).await.unwrap();
    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = BossCoordinator::restore_or_init_with_owner(path, owner)
        .await
        .unwrap()
        .with_lism_ab_sink(sink);
    Arc::new(coordinator)
}

// ── Scenario 1: advance_plan → PlanComplete → Completed sample recorded ───────

#[tokio::test]
async fn r1_2_plan_complete_records_completed_sample() {
    let plan_path = unique_plan_path("plan-complete");
    let sink = new_shared_ab_sink();
    let plan = all_completed_plan("plan-complete-test", 2);

    let coordinator = coordinator_with_sink(plan, &plan_path, sink.clone()).await;
    let app_state = make_app_state("session-complete-test");

    let msg = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(
        msg.as_deref().unwrap_or("").contains("complete"),
        "expected completion message"
    );
    assert_eq!(coordinator.get_stage().await, BossStage::Completed);

    // LisM A/B sink should have one record
    assert_eq!(sink.record_count(), 1);
    let records = sink.records();
    assert_eq!(records[0].outcome, BossTestRunOutcome::Completed);
    assert_eq!(records[0].run_id, "plan-complete-test");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn r1_2_plan_complete_records_full_context_worker_usage() {
    let plan_path = unique_plan_path("plan-full-context-usage");
    let sink = new_shared_ab_sink();
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = task_manager.create_with_type(
        "full-context worker",
        TaskType::Generic,
        "session-full-context-usage",
        InteractionSurface::Cli,
    );
    task_manager.complete_with_usage(
        &task.id,
        &dispatcher,
        Some(TaskUsageSummary {
            requests: 1,
            input_tokens: 1200,
            uncached_input_tokens: 688,
            output_tokens: 90,
            cache_creation_input_tokens: 256,
            cache_read_input_tokens: 512,
            original_prompt_chars: 0,
            sent_prompt_chars: 0,
            cache_hit_requests: 1,
            estimated_cost_micros_usd: 345,
        }),
    );

    let mut plan = all_completed_plan("plan-full-context-usage-test", 1);
    plan.steps[0].worker_task_id = Some(task.id.clone());
    let coordinator = coordinator_with_sink(plan, &plan_path, sink.clone()).await;
    let app_state =
        make_app_state_with_task_manager("session-full-context-usage", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();

    let records = sink.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].total_input_tokens, 1200);
    assert_eq!(records[0].total_output_tokens, 90);
    assert_eq!(records[0].cache_read_tokens, 512);
    assert_eq!(records[0].cache_write_tokens, 256);
    assert_eq!(records[0].cost_micros_usd, 345);
    assert_eq!(records[0].estimated_tokens_saved, 512);
    assert_eq!(records[0].cache_hit_ratio, Some(512.0 / (512.0 + 256.0)));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn r1_2_plan_complete_records_lism_disabled_when_not_in_session() {
    let plan_path = unique_plan_path("plan-lism-off");
    let sink = new_shared_ab_sink();
    let plan = all_completed_plan("plan-lism-off-test", 1);

    let coordinator = coordinator_with_sink(plan, &plan_path, sink.clone()).await;
    // Default app_state has lism_enabled = false (no LisM session flag)
    let app_state = make_app_state("session-lism-off");

    coordinator.advance_plan(&app_state).await.unwrap();

    let records = sink.records();
    assert_eq!(records.len(), 1);
    // LisM policy is Inherit + session lism_enabled=false → lism_enabled=false
    assert!(!records[0].lism_enabled);

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn r1_2_plan_complete_no_sink_does_not_panic() {
    let plan_path = unique_plan_path("plan-no-sink");
    let plan = all_completed_plan("plan-no-sink-test", 1);

    save_plan(&plan, &plan_path).await.unwrap();
    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = Arc::new(
        BossCoordinator::restore_or_init_with_owner(&plan_path, owner)
            .await
            .unwrap(),
    );
    // No sink attached — advance should complete without error
    let app_state = make_app_state("session-no-sink");
    let result = coordinator.advance_plan(&app_state).await;
    assert!(result.is_ok());
    assert_eq!(coordinator.get_stage().await, BossStage::Completed);

    let _ = std::fs::remove_file(plan_path);
}

// ── Scenario 2: advance_plan → TerminalFailure → Aborted sample recorded ──────

#[tokio::test]
async fn r1_2_terminal_failure_records_aborted_sample() {
    let plan_path = unique_plan_path("plan-failed");
    let sink = new_shared_ab_sink();
    let plan = pending_plan_with_failed_step("plan-failed-test");

    let coordinator = coordinator_with_sink(plan, &plan_path, sink.clone()).await;
    let app_state = make_app_state("session-failed-test");

    let msg = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(
        msg.as_deref()
            .unwrap_or("")
            .contains("terminal step failure"),
        "expected terminal failure message"
    );

    assert_eq!(sink.record_count(), 1);
    let records = sink.records();
    assert_eq!(records[0].outcome, BossTestRunOutcome::Aborted);
    assert_eq!(records[0].run_id, "plan-failed-test");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn r1_2_terminal_failure_no_sink_does_not_panic() {
    let plan_path = unique_plan_path("plan-failed-no-sink");
    let plan = pending_plan_with_failed_step("plan-failed-no-sink-test");

    save_plan(&plan, &plan_path).await.unwrap();
    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = Arc::new(
        BossCoordinator::restore_or_init_with_owner(&plan_path, owner)
            .await
            .unwrap(),
    );
    let app_state = make_app_state("session-no-sink-fail");
    let result = coordinator.advance_plan(&app_state).await;
    assert!(result.is_ok());

    let _ = std::fs::remove_file(plan_path);
}

// ── Scenario 3: rolled_back path via external lism_ab_sink() accessor ─────────

#[tokio::test]
async fn r1_2_rolled_back_via_external_sink_accessor() {
    use rust_agent::core::boss_test_readiness::BossRollbackPolicy;

    let plan_path = unique_plan_path("plan-rollback");
    let sink = new_shared_ab_sink();
    let plan = all_completed_plan("plan-rollback-test", 2);

    let coordinator = coordinator_with_sink(plan, &plan_path, sink.clone()).await;

    // Caller decides to rollback — use the accessor to record it
    if let Some(ab_sink) = coordinator.lism_ab_sink() {
        // Build a minimal report via report_progress
        let task_manager = TaskManager::default();
        let report = coordinator.report_progress(&task_manager).await.unwrap();
        let policy = BossRollbackPolicy::default();
        // Record via the general BossTestSampleSink-style API on the ab sink directly
        ab_sink.record_run(
            "plan-rollback-test",
            false,
            &report,
            BossTestRunOutcome::RolledBack,
            0,
        );
    }

    assert_eq!(sink.record_count(), 1);
    let records = sink.records();
    assert_eq!(records[0].outcome, BossTestRunOutcome::RolledBack);
    assert_eq!(records[0].run_id, "plan-rollback-test");

    let _ = std::fs::remove_file(plan_path);
}

// ── Scenario 4: sink accessor returns None when not configured ─────────────────

#[tokio::test]
async fn r1_2_lism_ab_sink_accessor_returns_none_when_not_set() {
    let coordinator = BossCoordinator::new();
    assert!(coordinator.lism_ab_sink().is_none());
}

#[tokio::test]
async fn r1_2_lism_ab_sink_accessor_returns_some_when_set() {
    let sink = new_shared_ab_sink();
    let coordinator = BossCoordinator::new().with_lism_ab_sink(sink);
    assert!(coordinator.lism_ab_sink().is_some());
}

// ── Scenario 5: with_lism_ab_sink builder preserves other coordinator state ────

#[tokio::test]
async fn r1_2_with_lism_ab_sink_builder_preserves_lism_policy() {
    use rust_agent::core::boss_state::BossLisMPolicy;

    let plan_path = unique_plan_path("plan-policy-preserve");
    let plan = all_completed_plan("plan-policy-preserve-test", 1);
    save_plan(&plan, &plan_path).await.unwrap();

    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = BossCoordinator::restore_or_init_with_owner(&plan_path, owner)
        .await
        .unwrap();
    coordinator.set_lism_policy(BossLisMPolicy::ForceOff).await;

    let sink = new_shared_ab_sink();
    let coordinator = coordinator.with_lism_ab_sink(sink);

    // Policy must survive the builder call
    assert_eq!(coordinator.lism_policy().await, BossLisMPolicy::ForceOff);

    let _ = std::fs::remove_file(plan_path);
}

// ── Scenario 6: set_lism_ab_sink mutation variant ────────────────────────────

#[tokio::test]
async fn r1_2_set_lism_ab_sink_attaches_sink_in_place() {
    let sink = new_shared_ab_sink();
    let mut coordinator = BossCoordinator::new();
    assert!(coordinator.lism_ab_sink().is_none());

    coordinator.set_lism_ab_sink(sink);
    assert!(coordinator.lism_ab_sink().is_some());
}

// ── Scenario 7: summarize after completed + aborted ──────────────────────────

#[tokio::test]
async fn r1_2_sink_summary_after_completed_and_aborted_runs() {
    let completed_path = unique_plan_path("plan-summary-ok");
    let failed_path = unique_plan_path("plan-summary-fail");
    let sink = new_shared_ab_sink();

    // Run 1: all steps complete → Completed
    {
        let plan = all_completed_plan("plan-summary-ok", 1);
        let coordinator = coordinator_with_sink(plan, &completed_path, sink.clone()).await;
        let app_state = make_app_state("session-summary-ok");
        coordinator.advance_plan(&app_state).await.unwrap();
    }

    // Run 2: step failed → Aborted
    {
        let plan = pending_plan_with_failed_step("plan-summary-fail");
        let coordinator = coordinator_with_sink(plan, &failed_path, sink.clone()).await;
        let app_state = make_app_state("session-summary-fail");
        coordinator.advance_plan(&app_state).await.unwrap();
    }

    assert_eq!(sink.record_count(), 2);
    let summary = sink.summarize();
    // Both runs have lism_enabled=false (default session has no LisM)
    assert_eq!(summary.off_runs, 2);
    assert_eq!(summary.on_runs, 0);
    let rate = summary
        .off_completion_rate
        .expect("completion rate should be Some");
    // 1 completed, 1 aborted → 0.5
    assert!((rate - 0.5).abs() < 1e-9);

    let _ = std::fs::remove_file(completed_path);
    let _ = std::fs::remove_file(failed_path);
}

// ── Scenario 8: init_lism_policy (R1 slice 3 — sync setter before Arc-wrap) ──

#[tokio::test]
async fn r1_3_init_lism_policy_sets_force_on() {
    use rust_agent::core::boss_state::BossLisMPolicy;
    let mut coordinator = BossCoordinator::new();
    coordinator.init_lism_policy(BossLisMPolicy::ForceOn);
    assert_eq!(coordinator.lism_policy().await, BossLisMPolicy::ForceOn);
}

#[tokio::test]
async fn r1_3_init_lism_policy_sets_force_off() {
    use rust_agent::core::boss_state::BossLisMPolicy;
    let mut coordinator = BossCoordinator::new();
    coordinator.init_lism_policy(BossLisMPolicy::ForceOff);
    assert_eq!(coordinator.lism_policy().await, BossLisMPolicy::ForceOff);
}

#[tokio::test]
async fn r1_3_init_lism_policy_defaults_to_inherit() {
    use rust_agent::core::boss_state::BossLisMPolicy;
    let coordinator = BossCoordinator::new();
    assert_eq!(coordinator.lism_policy().await, BossLisMPolicy::Inherit);
}

#[tokio::test]
async fn r1_3_init_lism_policy_sink_and_policy_both_wire_independently() {
    use rust_agent::core::boss_state::BossLisMPolicy;
    let sink = new_shared_ab_sink();
    let mut coordinator = BossCoordinator::new();
    coordinator.set_lism_ab_sink(sink);
    coordinator.init_lism_policy(BossLisMPolicy::ForceOn);
    assert!(coordinator.lism_ab_sink().is_some());
    assert_eq!(coordinator.lism_policy().await, BossLisMPolicy::ForceOn);
}

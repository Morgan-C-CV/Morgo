// R0 final drill — three end-to-end scenarios exercising the full beta security chain:
//   1. TUI  -> PendingApproval -> yes -> resume + audit
//   2. Telegram -> PendingApproval -> no -> deny + audit
//   3. /boss + skill/MCP in capability-exceeding context -> stops, audit + sample captured

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::core::boss_state::{
    BossActorHandle, BossActorRole, BossLisMPolicy, BossObservabilitySummary,
    BossPlanStepStatus, BossReportPayload, BossStage, BossStepReport,
};
use rust_agent::core::boss_test_readiness::{BossRollbackPolicy, BossTestRunOutcome};
use rust_agent::core::boss_test_sample_sink::BossTestSampleSink;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::audit::{AuditEvent, AuditLog};
use rust_agent::security::workspace_capability::{CapabilityTier, WorkspaceCapabilityConfig};
use rust_agent::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
};
use rust_agent::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

// ── helpers ───────────────────────────────────────────────────────────────────

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

fn make_app_state_with_capability(
    surface: InteractionSurface,
    audit_root: Option<PathBuf>,
    cap_config: Option<Arc<WorkspaceCapabilityConfig>>,
) -> AppState {
    let audit_log = match audit_root {
        Some(root) => AuditLog::file_backed(root),
        None => AuditLog::default(),
    };
    let mut ctx = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    if let Some(cap) = cap_config {
        ctx = ctx.with_workspace_capability(cap);
    }
    AppState {
        surface,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ctx,
        command_registry: Some(Arc::new(CommandRegistry::new())),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(audit_log)),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: ActiveModelProviderSummary {
            provider_id: "test".into(),
            protocol: "test".into(),
            compatibility_profile: "test".into(),
            base_url_host: "test".into(),
            model: "test".into(),
            auth_status: "none".into(),
        },
        active_session_id: "drill-session".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    }
}

fn set_bash_pending(app_state: &AppState, code: &str) {
    app_state.permission_context.set_pending_approval(Some(PendingApproval {
        tool_name: "Bash".to_string(),
        tool_input: r#"{"command":"rm -rf /project/dist"}"#.to_string(),
        message: format!("bash command requires approval [{}]", code),
        code: Some(code.to_string()),
        summary: Some("Bash pending approval".into()),
        detail: Some("command requires admin_bash capability but workspace allows write".into()),
        approval_kind: Some("capability_escalation".into()),
        escalation_reasons: vec![
            "capability.required=admin_bash".into(),
            "capability.allowed=write".into(),
            "capability.reason=destructive_pattern".into(),
        ],
    }));
}

fn audit_approval_events(app_state: &AppState) -> Vec<AuditEvent> {
    app_state
        .audit_log
        .lock()
        .unwrap()
        .events()
        .iter()
        .filter(|e| matches!(e, AuditEvent::ApprovalResolved { .. }))
        .cloned()
        .collect()
}

fn make_drill_report(completed: bool, cost_micros: u64) -> BossReportPayload {
    BossReportPayload {
        stage: BossStage::Execution,
        current_step: Some(if completed { 1 } else { 0 }),
        total_steps: Some(1),
        designer_a: BossActorHandle::new("boss-a", "boss-a", BossActorRole::DesignerA),
        executor_b: BossActorHandle::new("boss-b", "boss-b", BossActorRole::ExecutorB),
        active_children: vec![],
        steps: vec![BossStepReport {
            id: 0,
            status: if completed {
                BossPlanStepStatus::Completed
            } else {
                BossPlanStepStatus::Pending
            },
            worker_task_id: None,
            attempt_count: if completed { 1 } else { 0 },
            last_review_summary: None,
            action_required: None,
            blocker_reason: if completed {
                None
            } else {
                Some("capability_escalation: admin_bash required".into())
            },
            routed_metadata: None,
        }],
        history_summary: vec![],
        observability_summary: Some(BossObservabilitySummary {
            total_steps_routed: if completed { 1 } else { 0 },
            total_cache_read_tokens: 600,
            total_cache_write_tokens: 400,
            total_fallback_count: 0,
            total_projection_mismatch_count: 0,
            override_hit_count: 0,
            model_tier_counts: Default::default(),
            total_input_tokens: 1000,
            total_output_tokens: 200,
            estimated_cost_micros_usd: cost_micros,
        }),
        lism_policy: BossLisMPolicy::Inherit,
    }
}

// ── Scenario 1: TUI -> PendingApproval -> yes -> resume + audit ───────────────

#[tokio::test]
async fn r0_drill_tui_pending_approval_yes_emits_approved_audit() {
    let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
    let app_state = make_app_state_with_capability(InteractionSurface::Cli, None, Some(cap));

    set_bash_pending(&app_state, "capability_escalation");
    assert!(app_state.permission_context.pending_approval().is_some());

    // TUI user types "yes" -> resolve_pending_approval(true)
    // Tool registry has no Bash registered so dispatch fails, but audit fires first.
    let _ = app_state.resolve_pending_approval(true).await;

    assert!(app_state.permission_context.pending_approval().is_none());

    let events = audit_approval_events(&app_state);
    assert_eq!(events.len(), 1);
    if let AuditEvent::ApprovalResolved { decision, surface, tool_name, code, .. } = &events[0] {
        assert_eq!(decision, "approved");
        assert_eq!(surface, "cli");
        assert_eq!(tool_name, "Bash");
        assert_eq!(code.as_deref(), Some("capability_escalation"));
    } else {
        panic!("expected ApprovalResolved event");
    }
}

#[tokio::test]
async fn r0_drill_tui_yes_clears_pending_before_tool_dispatch() {
    let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
    let app_state = make_app_state_with_capability(InteractionSurface::Cli, None, Some(cap));
    set_bash_pending(&app_state, "capability_escalation");

    let _ = app_state.resolve_pending_approval(true).await;

    assert!(app_state.permission_context.pending_approval().is_none());
}

// ── Scenario 2: Telegram -> PendingApproval -> no -> deny + audit ─────────────

#[tokio::test]
async fn r0_drill_telegram_pending_approval_no_emits_denied_audit() {
    let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
    let app_state = make_app_state_with_capability(InteractionSurface::Telegram, None, Some(cap));

    set_bash_pending(&app_state, "capability_escalation");

    let result = app_state.resolve_pending_approval(false).await.unwrap();
    assert!(
        matches!(result, rust_agent::command::types::CommandResult::Message(ref m) if m.contains("Denied")),
        "expected Denied message, got {:?}",
        result
    );

    assert!(app_state.permission_context.pending_approval().is_none());

    let events = audit_approval_events(&app_state);
    assert_eq!(events.len(), 1);
    if let AuditEvent::ApprovalResolved {
        decision,
        surface,
        tool_name,
        escalation_reasons,
        ..
    } = &events[0]
    {
        assert_eq!(decision, "denied");
        assert_eq!(surface, "telegram");
        assert_eq!(tool_name, "Bash");
        assert!(escalation_reasons.iter().any(|r| r.contains("admin_bash")));
    } else {
        panic!("expected ApprovalResolved event");
    }
}

#[tokio::test]
async fn r0_drill_telegram_deny_persists_to_jsonl() {
    let root = unique_temp_path("r0-drill-telegram-deny");
    let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
    let app_state =
        make_app_state_with_capability(InteractionSurface::Telegram, Some(root.clone()), Some(cap));

    set_bash_pending(&app_state, "capability_escalation");
    app_state.resolve_pending_approval(false).await.unwrap();

    let reloaded = AuditLog::file_backed(root.clone());
    let records = reloaded.load_records();
    let record = records
        .iter()
        .find(|r| r.event_kind == "approval_resolved")
        .expect("expected approval_resolved on disk");
    assert_eq!(record.outcome, "denied");
    assert_eq!(record.surface.as_deref(), Some("telegram"));

    let _ = std::fs::remove_dir_all(root);
}

// ── Scenario 3: /boss + skill/MCP capability-exceeding -> stops, audit + sample

#[tokio::test]
async fn r0_drill_boss_capability_exceeded_aborts_and_records_sample() {
    let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
    let app_state = make_app_state_with_capability(InteractionSurface::Cli, None, Some(cap));

    set_bash_pending(&app_state, "capability_escalation");
    assert!(app_state.permission_context.pending_approval().is_some());

    // Boss aborts: pending approval = unresolved capability escalation
    let sink = BossTestSampleSink::in_memory();
    let report = make_drill_report(false, 0);
    let policy = BossRollbackPolicy::default();

    sink.record_run_aborted(
        "drill-run-001",
        &report,
        &policy,
        vec!["deploy-skill".to_string()],
        vec!["github-mcp".to_string()],
        0,
        1, // 1 pending_approval_count
    );

    assert_eq!(sink.record_count(), 1);
    let records = sink.records();
    let sample = &records[0];
    assert_eq!(sample.run_id, "drill-run-001");
    assert_eq!(sample.pending_approval_count, 1);
    assert_eq!(sample.skill_names, vec!["deploy-skill"]);
    assert_eq!(sample.mcp_server_names, vec!["github-mcp"]);
    assert_eq!(sample.outcome, BossTestRunOutcome::Aborted);

    // Deny the pending approval -> audit emitted
    app_state.resolve_pending_approval(false).await.unwrap();
    let events = audit_approval_events(&app_state);
    assert_eq!(events.len(), 1);
    if let AuditEvent::ApprovalResolved { decision, .. } = &events[0] {
        assert_eq!(decision, "denied");
    } else {
        panic!("expected ApprovalResolved");
    }
}

#[tokio::test]
async fn r0_drill_boss_capability_approved_records_completed_sample_with_cache_ratio() {
    let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
    let app_state = make_app_state_with_capability(InteractionSurface::Cli, None, Some(cap));

    set_bash_pending(&app_state, "capability_escalation");
    let _ = app_state.resolve_pending_approval(true).await;

    let sink = BossTestSampleSink::in_memory();
    let report = make_drill_report(true, 2000);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete(
        "drill-run-002",
        &report,
        &policy,
        vec!["deploy-skill".to_string()],
        vec!["github-mcp".to_string()],
        0,
        1,
    );

    let records = sink.records();
    let sample = &records[0];
    assert_eq!(sample.outcome, BossTestRunOutcome::Completed);
    assert_eq!(sample.pending_approval_count, 1);
    assert_eq!(sample.cost_micros_usd, 2000);
    // cache_hit_ratio = 600 / (600+400) = 0.6
    let ratio = sample.cache_hit_ratio.expect("cache_hit_ratio should be Some");
    assert!((ratio - 0.6).abs() < 1e-9);

    let events = audit_approval_events(&app_state);
    assert_eq!(events.len(), 1);
    if let AuditEvent::ApprovalResolved { decision, .. } = &events[0] {
        assert_eq!(decision, "approved");
    } else {
        panic!("expected ApprovalResolved");
    }
}

// ── Cross-scenario: deny-by-default config is active on all surfaces ──────────

#[tokio::test]
async fn r0_drill_beta_deny_by_default_config_active_on_all_surfaces() {
    for surface in [
        InteractionSurface::Cli,
        InteractionSurface::Telegram,
        InteractionSurface::Remote,
    ] {
        let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
        let app_state = make_app_state_with_capability(surface, None, Some(cap));

        let loaded = app_state.permission_context.workspace_capability();
        assert!(
            loaded.is_some(),
            "capability config should be set for surface {:?}",
            surface
        );
        let loaded = loaded.unwrap();
        assert_eq!(
            loaded.global_max_tier,
            CapabilityTier::Read,
            "beta preset should have Read as global max tier"
        );
        assert!(loaded.escalate_to_pending_approval);
        assert!(loaded.audit_capability_decisions);
    }
}

// ── Audit JSONL: approve (CLI) + deny (Telegram) land in same file ────────────

#[tokio::test]
async fn r0_drill_approve_and_deny_both_land_in_same_audit_file() {
    let root = unique_temp_path("r0-drill-both-decisions");

    {
        let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
        let app_state =
            make_app_state_with_capability(InteractionSurface::Cli, Some(root.clone()), Some(cap));
        set_bash_pending(&app_state, "cap1");
        let _ = app_state.resolve_pending_approval(true).await;
    }

    {
        let cap = Arc::new(WorkspaceCapabilityConfig::beta_deny_by_default());
        let app_state = make_app_state_with_capability(
            InteractionSurface::Telegram,
            Some(root.clone()),
            Some(cap),
        );
        set_bash_pending(&app_state, "cap2");
        app_state.resolve_pending_approval(false).await.unwrap();
    }

    let reloaded = AuditLog::file_backed(root.clone());
    let records = reloaded.load_records();
    let approval_records: Vec<_> = records
        .iter()
        .filter(|r| r.event_kind == "approval_resolved")
        .collect();
    assert_eq!(approval_records.len(), 2);

    let approved = approval_records.iter().find(|r| r.outcome == "approved");
    let denied = approval_records.iter().find(|r| r.outcome == "denied");
    assert!(approved.is_some(), "expected approved record");
    assert!(denied.is_some(), "expected denied record");
    assert_eq!(approved.unwrap().surface.as_deref(), Some("cli"));
    assert_eq!(denied.unwrap().surface.as_deref(), Some("telegram"));

    let _ = std::fs::remove_dir_all(root);
}

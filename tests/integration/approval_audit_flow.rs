use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::audit::{AuditEvent, AuditLog};
use rust_agent::state::app_state::{AppState, RuntimeRole};
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

fn make_app_state(surface: InteractionSurface, audit_root: Option<PathBuf>) -> AppState {
    let audit_log = match audit_root {
        Some(root) => AuditLog::file_backed(root),
        None => AuditLog::default(),
    };
    AppState {
        surface,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default()))
            .with_plan_manager(Arc::new(PlanManager::default())),
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
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "test".into(),
            protocol: "test".into(),
            compatibility_profile: "test".into(),
            base_url_host: "test".into(),
            model: "test".into(),
            auth_status: "none".into(),
        },
        active_session_id: "test-session".into(),
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

fn set_pending(app_state: &AppState, tool_name: &str, code: Option<&str>) {
    app_state.permission_context.set_pending_approval(Some(PendingApproval {
        tool_name: tool_name.to_string(),
        tool_input: r#"{"command":"rm -rf /tmp/test"}"#.to_string(),
        message: format!("bash command requires approval [{tool_name}]"),
        code: code.map(str::to_string),
        summary: Some("Bash pending approval".into()),
        detail: Some("destructive pattern detected".into()),
        approval_kind: Some("tool_permission".into()),
        escalation_reasons: vec!["destructive_pattern".into()],
    }));
}

fn audit_events(app_state: &AppState) -> Vec<AuditEvent> {
    app_state.audit_log.lock().unwrap().events().to_vec()
}

// ── approval audit: deny path ─────────────────────────────────────────────────

#[tokio::test]
async fn r0_3_deny_emits_approval_resolved_audit_event() {
    let app_state = make_app_state(InteractionSurface::Cli, None);
    set_pending(&app_state, "Bash", Some("capability_escalation"));

    app_state.resolve_pending_approval(false).await.unwrap();

    let events = audit_events(&app_state);
    let approval_event = events.iter().find(|e| {
        matches!(e, AuditEvent::ApprovalResolved { .. })
    });
    assert!(approval_event.is_some(), "expected ApprovalResolved event");
    if let Some(AuditEvent::ApprovalResolved {
        tool_name,
        decision,
        surface,
        session_id,
        code,
        ..
    }) = approval_event
    {
        assert_eq!(tool_name, "Bash");
        assert_eq!(decision, "denied");
        assert_eq!(surface, "cli");
        assert_eq!(session_id.as_deref(), Some("test-session"));
        assert_eq!(code.as_deref(), Some("capability_escalation"));
    }
}

// ── approval audit: approve path ─────────────────────────────────────────────

#[tokio::test]
async fn r0_3_approve_emits_approval_resolved_audit_event() {
    let app_state = make_app_state(InteractionSurface::Cli, None);
    set_pending(&app_state, "Bash", Some("policy_escalation"));

    // Approve — tool registry has no "Bash" registered so it returns an error,
    // but the audit event is emitted before the registry lookup result matters.
    // We check the audit log regardless of the command result.
    let _ = app_state.resolve_pending_approval(true).await;

    let events = audit_events(&app_state);
    let approval_event = events.iter().find(|e| {
        matches!(e, AuditEvent::ApprovalResolved { decision, .. } if decision == "approved")
    });
    assert!(approval_event.is_some(), "expected ApprovalResolved approved event");
}

// ── approval audit: surface is recorded correctly ─────────────────────────────

#[tokio::test]
async fn r0_3_telegram_surface_recorded_in_audit() {
    let app_state = make_app_state(InteractionSurface::Telegram, None);
    set_pending(&app_state, "Bash", None);

    app_state.resolve_pending_approval(false).await.unwrap();

    let events = audit_events(&app_state);
    let approval_event = events.iter().find(|e| {
        matches!(e, AuditEvent::ApprovalResolved { surface, .. } if surface == "telegram")
    });
    assert!(approval_event.is_some(), "expected telegram surface in audit");
}

#[tokio::test]
async fn r0_3_remote_surface_recorded_in_audit() {
    let app_state = make_app_state(InteractionSurface::Remote, None);
    set_pending(&app_state, "Bash", None);

    app_state.resolve_pending_approval(false).await.unwrap();

    let events = audit_events(&app_state);
    let approval_event = events.iter().find(|e| {
        matches!(e, AuditEvent::ApprovalResolved { surface, .. } if surface == "remote")
    });
    assert!(approval_event.is_some(), "expected remote surface in audit");
}

// ── approval audit: escalation_reasons preserved ─────────────────────────────

#[tokio::test]
async fn r0_3_escalation_reasons_preserved_in_audit() {
    let app_state = make_app_state(InteractionSurface::Cli, None);
    app_state.permission_context.set_pending_approval(Some(PendingApproval {
        tool_name: "Bash".to_string(),
        tool_input: "{}".to_string(),
        message: "requires approval".to_string(),
        code: None,
        summary: None,
        detail: None,
        approval_kind: None,
        escalation_reasons: vec!["destructive_pattern".into(), "shell_operator".into()],
    }));

    app_state.resolve_pending_approval(false).await.unwrap();

    let events = audit_events(&app_state);
    if let Some(AuditEvent::ApprovalResolved { escalation_reasons, .. }) =
        events.iter().find(|e| matches!(e, AuditEvent::ApprovalResolved { .. }))
    {
        assert!(escalation_reasons.contains(&"destructive_pattern".to_string()));
        assert!(escalation_reasons.contains(&"shell_operator".to_string()));
    } else {
        panic!("expected ApprovalResolved event");
    }
}

// ── approval audit: no pending → no audit event ──────────────────────────────

#[tokio::test]
async fn r0_3_no_pending_approval_emits_no_audit_event() {
    let app_state = make_app_state(InteractionSurface::Cli, None);
    // No pending approval set

    let result = app_state.resolve_pending_approval(false).await.unwrap();
    assert!(matches!(result, rust_agent::command::types::CommandResult::Denied(_)));

    let events = audit_events(&app_state);
    let approval_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, AuditEvent::ApprovalResolved { .. }))
        .collect();
    assert!(approval_events.is_empty(), "no audit event expected when no pending approval");
}

// ── approval audit: AuditRecord shape ────────────────────────────────────────

#[tokio::test]
async fn r0_3_audit_record_event_kind_and_outcome_correct() {
    let app_state = make_app_state(InteractionSurface::Cli, None);
    set_pending(&app_state, "Bash", Some("capability_escalation"));

    app_state.resolve_pending_approval(false).await.unwrap();

    let records = app_state.audit_log.lock().unwrap().records().to_vec();
    let record = records
        .iter()
        .find(|r| r.event_kind == "approval_resolved")
        .expect("expected approval_resolved record");
    assert_eq!(record.outcome, "denied");
    assert_eq!(record.surface.as_deref(), Some("cli"));
    assert_eq!(record.session_id.as_deref(), Some("test-session"));
}

// ── approval audit: JSONL file-backed persistence ────────────────────────────

#[tokio::test]
async fn r0_3_approval_audit_persists_to_jsonl() {
    let root = unique_temp_path("r0-3-approval-audit");
    let app_state = make_app_state(InteractionSurface::Cli, Some(root.clone()));
    set_pending(&app_state, "Bash", Some("capability_escalation"));

    app_state.resolve_pending_approval(false).await.unwrap();

    // Reload from disk
    let reloaded = AuditLog::file_backed(root.clone());
    let records = reloaded.load_records();
    let record = records
        .iter()
        .find(|r| r.event_kind == "approval_resolved")
        .expect("expected approval_resolved record on disk");
    assert_eq!(record.outcome, "denied");
    assert_eq!(record.surface.as_deref(), Some("cli"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn r0_3_both_approve_and_deny_persist_to_same_jsonl() {
    let root = unique_temp_path("r0-3-both-decisions");

    // First session: deny
    {
        let app_state = make_app_state(InteractionSurface::Cli, Some(root.clone()));
        set_pending(&app_state, "Bash", Some("cap1"));
        app_state.resolve_pending_approval(false).await.unwrap();
    }

    // Second session: approve (will fail at registry lookup, but audit is emitted first)
    {
        let app_state = make_app_state(InteractionSurface::Telegram, Some(root.clone()));
        set_pending(&app_state, "Bash", Some("cap2"));
        let _ = app_state.resolve_pending_approval(true).await;
    }

    let reloaded = AuditLog::file_backed(root.clone());
    let records = reloaded.load_records();
    let approval_records: Vec<_> = records
        .iter()
        .filter(|r| r.event_kind == "approval_resolved")
        .collect();
    assert_eq!(approval_records.len(), 2, "expected 2 approval_resolved records");

    let denied = approval_records.iter().find(|r| r.outcome == "denied");
    let approved = approval_records.iter().find(|r| r.outcome == "approved");
    assert!(denied.is_some(), "expected denied record");
    assert!(approved.is_some(), "expected approved record");
    assert_eq!(denied.unwrap().surface.as_deref(), Some("cli"));
    assert_eq!(approved.unwrap().surface.as_deref(), Some("telegram"));

    let _ = std::fs::remove_dir_all(root);
}

// ── approval audit: approval_kind preserved ──────────────────────────────────

#[tokio::test]
async fn r0_3_approval_kind_preserved_in_audit_record() {
    let app_state = make_app_state(InteractionSurface::Cli, None);
    set_pending(&app_state, "Bash", Some("capability_escalation"));

    app_state.resolve_pending_approval(false).await.unwrap();

    let records = app_state.audit_log.lock().unwrap().records().to_vec();
    let record = records
        .iter()
        .find(|r| r.event_kind == "approval_resolved")
        .expect("expected approval_resolved record");
    if let rust_agent::security::audit::AuditEvent::ApprovalResolved { approval_kind, .. } =
        &record.event
    {
        assert_eq!(approval_kind.as_deref(), Some("tool_permission"));
    } else {
        panic!("expected ApprovalResolved event in record");
    }
}

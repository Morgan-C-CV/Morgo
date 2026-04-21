use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{
    BootstrapCli, BootstrapPhase, BootstrapState, InteractionSurface, PromptAugmentationMetadata,
    RuntimeBootstrap, SessionMode, SessionSource, UserAccessDecision, is_tui_exit_input,
    tui_clear_screen_prefix,
};
use rust_agent::core::message::Message;
use rust_agent::history::resume::{RestoreRequest, RestoreSource, resolve_session_state};
use rust_agent::history::session::{
    FileBackedSessionStore, InMemorySessionStore, SessionHistory, SessionHistoryEntry, SessionId,
    SessionRestoreRequest, SessionSnapshot, SessionStore,
};
use rust_agent::hook::registry::{HookConfigSource, HookEvent, load_hook_registry};
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout,
};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::state::app_state::{AppState, AppStateRuntimeChange, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::state::store::AppStateStore;
use rust_agent::task::list_types::{TaskListItem, TaskListSnapshot, TaskListStatus};
use rust_agent::tool::registry::ToolAssemblyContext;

fn runtime_for_surface(surface: &str, interactive: bool, init_only: bool) -> RuntimeBootstrap {
    RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive,
        init_only,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: surface.into(),
    })
    .with_provider_config(test_model_provider_config())
}

fn test_model_provider_config() -> ModelProviderConfig {
    ModelProviderConfig {
        provider_id: "anthropic".into(),
        protocol: ProviderProtocol::Anthropic,
        compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
        base_url: "http://localhost".into(),
        auth_strategy: ProviderAuthStrategy::NoAuth,
        api_key: None,
        model_id: "test-model".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 30_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
        pricing: ModelPricing::default(),
    }
}

fn set_env_var(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) }
}

fn remove_env_var(key: &str) {
    unsafe { std::env::remove_var(key) }
}

fn bootstrap_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct BootstrapEnvGuard {
    keys: [&'static str; 5],
}

impl BootstrapEnvGuard {
    fn new() -> Self {
        let keys = [
            "RUST_AGENT_PROVIDER_ID",
            "RUST_AGENT_PROVIDER_BASE_URL",
            "RUST_AGENT_PROVIDER_API_KEY",
            "RUST_AGENT_PROVIDER_DEFAULT_MODEL",
            "RUST_AGENT_PROVIDER_MODEL",
        ];
        for key in keys {
            remove_env_var(key);
        }
        Self { keys }
    }
}

impl Drop for BootstrapEnvGuard {
    fn drop(&mut self) {
        for key in self.keys {
            remove_env_var(key);
        }
    }
}

fn initialized_tool_names(
    surface: InteractionSurface,
    session_mode: SessionMode,
    init_only: bool,
) -> Vec<&'static str> {
    let runtime = runtime_for_surface(
        match surface {
            InteractionSurface::Cli => "cli",
            InteractionSurface::Remote => "remote",
            InteractionSurface::Telegram => "telegram",
        },
        matches!(session_mode, SessionMode::Interactive),
        init_only,
    );
    let mut state = BootstrapState::new(surface, session_mode, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");
    let bundle = runtime
        .initialize_runtime(
            &state,
            format!("session-{surface:?}-{session_mode:?}"),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");
    let permission_context = ToolAssemblyContext::coordinator(surface, session_mode)
        .permission_context(if init_only {
            PermissionMode::Plan
        } else {
            PermissionMode::Default
        })
        .with_active_surface(surface);
    bundle
        .coordinator_tools
        .visible_tools(&permission_context)
        .iter()
        .map(|tool| tool.name)
        .collect()
}

fn unique_temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn bootstrap_state_records_phase_order() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, true);
    state.record_phase(BootstrapPhase::DetectSurface);
    state.record_phase(BootstrapPhase::ResolvePermissions);
    state = state.finalize();

    assert_eq!(
        state.startup_trace(),
        "DetectSurface -> ResolvePermissions -> FinalizeState"
    );
}

#[test]
fn bootstrap_phase_sequence_includes_all_phases() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Interactive, false);

    // Simulate full bootstrap sequence
    state.record_phase(BootstrapPhase::DetectSurface);
    state.record_phase(BootstrapPhase::InjectSessionMetadata);
    state.record_phase(BootstrapPhase::ResolvePermissions);
    state.record_phase(BootstrapPhase::BuildToolContext);
    state.record_phase(BootstrapPhase::AssembleTools);
    state.record_phase(BootstrapPhase::Setup);
    state.record_phase(BootstrapPhase::InitializeRuntime);
    state.record_phase(BootstrapPhase::InitializeSettings);
    state.record_phase(BootstrapPhase::AugmentPrompt);
    state.record_phase(BootstrapPhase::GateUserAccess);
    state.record_phase(BootstrapPhase::WarmupAndConvergence);
    state.record_phase(BootstrapPhase::AssembleAppState);
    state = state.finalize();

    let trace = state.startup_trace();

    // Verify all phases are present in order
    assert!(trace.contains("DetectSurface"));
    assert!(trace.contains("InjectSessionMetadata"));
    assert!(trace.contains("ResolvePermissions"));
    assert!(trace.contains("BuildToolContext"));
    assert!(trace.contains("AssembleTools"));
    assert!(trace.contains("Setup"));
    assert!(trace.contains("InitializeRuntime"));
    assert!(trace.contains("InitializeSettings"));
    assert!(trace.contains("AugmentPrompt"));
    assert!(trace.contains("GateUserAccess"));
    assert!(trace.contains("WarmupAndConvergence"));
    assert!(trace.contains("AssembleAppState"));
    assert!(trace.contains("FinalizeState"));

    // Verify phases appear in correct order
    let detect_pos = trace.find("DetectSurface").unwrap();
    let settings_pos = trace.find("InitializeSettings").unwrap();
    let warmup_pos = trace.find("WarmupAndConvergence").unwrap();
    let assemble_pos = trace.find("AssembleAppState").unwrap();
    let finalize_pos = trace.find("FinalizeState").unwrap();

    assert!(detect_pos < settings_pos);
    assert!(settings_pos < warmup_pos);
    assert!(warmup_pos < assemble_pos);
    assert!(assemble_pos < finalize_pos);
}

#[test]
fn bootstrap_infers_openai_family_provider_contract_from_env() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_ID", "openai");
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "http://localhost:4010");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    set_env_var("RUST_AGENT_PROVIDER_DEFAULT_MODEL", "gpt-test");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-env-openai".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize with inferred openai-compatible contract");

    assert_eq!(bundle.provider_config.provider_id, "openai");
    assert_eq!(
        bundle.provider_config.protocol,
        ProviderProtocol::OpenAICompatible
    );
    assert_eq!(
        bundle.provider_config.compatibility_profile,
        ProviderCompatibilityProfileKind::OpenAICompatible
    );
    assert_eq!(bundle.provider_config.base_url, "http://localhost:4010");
    assert_eq!(bundle.provider_config.model_id, "gpt-test");
    assert_eq!(
        bundle.provider_config.auth_strategy,
        ProviderAuthStrategy::BearerApiKey
    );
}

#[test]
fn bootstrap_rejects_unknown_provider_without_explicit_contract() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_ID", "custom-provider");
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "http://localhost:4010");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    set_env_var("RUST_AGENT_PROVIDER_DEFAULT_MODEL", "custom-model");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let error = runtime
        .initialize_runtime(
            &state,
            "provider-env-unknown".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect_err("runtime should reject unknown provider without explicit contract");

    assert!(
        error
            .to_string()
            .contains("invalid_configuration: unknown provider id custom-provider requires explicit protocol and compatibility_profile")
    );
}

fn bootstrap_provider_alias_matrix(
    alias: &str,
    expected_protocol: ProviderProtocol,
    expected_profile: ProviderCompatibilityProfileKind,
) {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_ID", alias);
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "http://localhost:4010");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    set_env_var("RUST_AGENT_PROVIDER_DEFAULT_MODEL", "test-model");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let bundle = runtime
        .initialize_runtime(
            &state,
            format!("provider-alias-{}", alias),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize with provider alias");

    assert_eq!(
        bundle.provider_config.provider_id, alias,
        "provider_id mismatch for alias {alias}"
    );
    assert_eq!(
        bundle.provider_config.protocol, expected_protocol,
        "protocol mismatch for alias {alias}"
    );
    assert_eq!(
        bundle.provider_config.compatibility_profile, expected_profile,
        "profile mismatch for alias {alias}"
    );
}

#[test]
fn bootstrap_infers_anthropic_provider_alias() {
    bootstrap_provider_alias_matrix(
        "anthropic",
        ProviderProtocol::Anthropic,
        ProviderCompatibilityProfileKind::Anthropic,
    );
}

#[test]
fn bootstrap_infers_default_provider_alias() {
    bootstrap_provider_alias_matrix(
        "default-provider",
        ProviderProtocol::Anthropic,
        ProviderCompatibilityProfileKind::Anthropic,
    );
}

#[test]
fn bootstrap_infers_kimi_provider_alias() {
    bootstrap_provider_alias_matrix(
        "kimi",
        ProviderProtocol::OpenAICompatible,
        ProviderCompatibilityProfileKind::OpenAICompatible,
    );
}

#[test]
fn bootstrap_infers_glm_provider_alias() {
    bootstrap_provider_alias_matrix(
        "glm",
        ProviderProtocol::OpenAICompatible,
        ProviderCompatibilityProfileKind::OpenAICompatible,
    );
}

#[test]
fn bootstrap_infers_minimax_provider_alias() {
    bootstrap_provider_alias_matrix(
        "minimax",
        ProviderProtocol::OpenAICompatible,
        ProviderCompatibilityProfileKind::OpenAICompatible,
    );
}

#[test]
fn bootstrap_infers_gemini_provider_alias() {
    bootstrap_provider_alias_matrix(
        "gemini",
        ProviderProtocol::GeminiNative,
        ProviderCompatibilityProfileKind::GeminiNativeUnsupported,
    );
}

#[test]
fn cli_telegram_remote_share_core_runtime_initialization() {
    // All surfaces should produce identical tool pools for core tools,
    // but may differ on surface-specific filtering (interactive/deferred/open-world)

    let cli_tools =
        initialized_tool_names(InteractionSurface::Cli, SessionMode::Interactive, false);
    let telegram_tools = initialized_tool_names(
        InteractionSurface::Telegram,
        SessionMode::Interactive,
        false,
    );
    let remote_tools =
        initialized_tool_names(InteractionSurface::Remote, SessionMode::Interactive, false);

    // Core non-interactive tools should be present in all surfaces
    let core_tools = [
        "Agent",
        "Read",
        "Edit",
        "Glob",
        "Grep",
        "TaskCreate",
        "TaskList",
    ];
    for tool in core_tools {
        assert!(cli_tools.contains(&tool), "CLI missing {}", tool);
        assert!(telegram_tools.contains(&tool), "Telegram missing {}", tool);
        assert!(remote_tools.contains(&tool), "Remote missing {}", tool);
    }

    // Bash is interactive/open-world, may be filtered on some surfaces
    assert!(cli_tools.contains(&"Bash"), "CLI should have Bash");

    // Mcp/WebFetch/WebSearch are deferred/open-world, may be filtered on bot surfaces
    assert!(cli_tools.contains(&"Mcp"), "CLI should have Mcp");
    assert!(cli_tools.contains(&"WebFetch"), "CLI should have WebFetch");
    assert!(
        cli_tools.contains(&"WebSearch"),
        "CLI should have WebSearch"
    );

    // Telegram filters interactive/deferred/open-world tools - this is intentional
    assert!(
        !telegram_tools.contains(&"Bash"),
        "Telegram should filter Bash (interactive)"
    );
    assert!(
        !telegram_tools.contains(&"Mcp"),
        "Telegram should filter Mcp (deferred/open-world)"
    );
    assert!(
        !telegram_tools.contains(&"WebFetch"),
        "Telegram should filter WebFetch (deferred/open-world)"
    );
    assert!(
        !telegram_tools.contains(&"WebSearch"),
        "Telegram should filter WebSearch (deferred/open-world)"
    );
}

#[test]
fn surface_init_respects_session_mode_consistently() {
    // Headless mode should filter interactive tools consistently across all surfaces

    let cli_headless =
        initialized_tool_names(InteractionSurface::Cli, SessionMode::Headless, false);
    let telegram_headless =
        initialized_tool_names(InteractionSurface::Telegram, SessionMode::Headless, false);
    let remote_headless =
        initialized_tool_names(InteractionSurface::Remote, SessionMode::Headless, false);

    // All surfaces should apply same headless filtering
    assert_eq!(cli_headless, telegram_headless);
    assert_eq!(cli_headless, remote_headless);

    // Agent should be visible (always_load)
    assert!(cli_headless.contains(&"Agent"));

    // Interactive tools should be filtered in headless
    // (This depends on actual metadata - adjust based on real behavior)
}

#[test]
fn app_state_store_notifies_subscribers_after_committed_update() {
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: None,
        runtime_tool_registry: None,
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_session_id: "session-1".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    let store = AppStateStore::new(app_state);
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_clone = observed.clone();
    store.subscribe(move |update| {
        observed_clone
            .lock()
            .expect("observation lock")
            .push((update.generation, update.current.permission_context.mode()));
    });

    let update = store.update(|state| {
        state.permission_context.set_mode(PermissionMode::Plan);
    });

    assert_eq!(update.generation, 1);
    assert_eq!(store.generation(), 1);
    assert_eq!(store.get().permission_context.mode(), PermissionMode::Plan);
    assert_eq!(
        observed.lock().expect("observation lock").as_slice(),
        &[(1, PermissionMode::Plan)]
    );
}

#[test]
fn app_state_classifies_runtime_visible_changes() {
    let previous = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: None,
        runtime_tool_registry: None,
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_session_id: "session-1".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    let mut current = AppState {
        surface: previous.surface,
        session_mode: previous.session_mode,
        client_type: previous.client_type,
        session_source: previous.session_source,
        runtime_role: previous.runtime_role,
        worker_role: previous.worker_role,
        permission_context: ToolPermissionContext::new(PermissionMode::Plan),
        command_registry: previous.command_registry.clone(),
        runtime_tool_registry: previous.runtime_tool_registry.clone(),
        skill_registry: previous.skill_registry.clone(),
        mcp_runtime: previous.mcp_runtime.clone(),
        plugin_load_result: previous.plugin_load_result.clone(),
        cost_tracker: previous.cost_tracker.clone(),
        service_observability_tracker: previous.service_observability_tracker.clone(),
        notification_dispatcher: previous.notification_dispatcher.clone(),
        audit_log: previous.audit_log.clone(),
        startup_trace: previous.startup_trace.clone(),
        active_session_id: previous.active_session_id.clone(),
        session_store: previous.session_store.clone(),
        session: previous.session.clone(),
        history: previous.history.clone(),
        restored_session: previous.restored_session.clone(),
        last_activity_ts: previous.last_activity_ts.clone(),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    current.bind_surface_session(
        InteractionSurface::Remote,
        rust_agent::bootstrap::ClientType::RemoteControl,
        SessionSource::RemoteControl,
        "remote-session",
    );

    let change_set = AppState::classify_runtime_changes(&previous, &current);

    assert!(
        change_set
            .changes
            .contains(&AppStateRuntimeChange::PermissionChanged)
    );
    assert!(
        change_set
            .changes
            .contains(&AppStateRuntimeChange::SurfaceBindingChanged)
    );
}

#[test]
fn in_memory_session_store_loads_latest_session_for_continue() {
    let store = InMemorySessionStore::default();
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-123".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/demo".into(),
            last_turn_at: Some("2026-04-11T00:00:00Z".into()),
            prompt_seed: Some("seed".into()),
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("restored"),
                timestamp: Some("2026-04-11T00:00:01Z".into()),
                tool_refs: vec!["Read".into()],
                milestone: None,
            }],
        },
    );

    let loaded = store.load(&SessionRestoreRequest {
        resume: None,
        continue_session: true,
    });

    let (snapshot, history) = loaded.expect("expected stored session");
    assert_eq!(snapshot.session_id.0, "session-123");
    assert_eq!(history.entries.len(), 1);
}

#[test]
fn in_memory_session_store_round_trips_task_lists_by_session() {
    let store = InMemorySessionStore::default();
    let session_id = SessionId("session-task-list".into());
    let snapshot = TaskListSnapshot {
        next_id: 3,
        tasks: vec![TaskListItem {
            id: "task-0".into(),
            subject: "persisted task".into(),
            description: "survives restore".into(),
            active_form: Some("Persisting".into()),
            status: TaskListStatus::InProgress,
            owner: Some("session-task-list".into()),
            plan_step_id: None,
            blocks: vec!["task-1".into()],
            blocked_by: vec![],
        }],
    };

    store.save_task_list(&session_id, snapshot.clone());

    assert_eq!(store.load_task_list(&session_id), Some(snapshot));
    assert_eq!(
        store.load_task_list(&SessionId("other-session".into())),
        None
    );
}

#[test]
fn in_memory_session_store_round_trips_external_and_nested_memory_by_session() {
    let store = InMemorySessionStore::default();
    let session_id = SessionId("session-memory".into());

    store.save_external_memory_entries(
        &session_id,
        vec!["linear:ABC-1 context".into(), "slack:#triage".into()],
    );
    store.save_nested_memory_lineage(
        &session_id,
        vec![
            "session:session-memory".into(),
            "agent:child:inherit_context=true".into(),
        ],
    );

    assert_eq!(
        store.load_external_memory_entries(&session_id),
        vec![
            "linear:ABC-1 context".to_string(),
            "slack:#triage".to_string()
        ]
    );
    assert_eq!(
        store.load_nested_memory_lineage(&session_id),
        vec![
            "session:session-memory".to_string(),
            "agent:child:inherit_context=true".to_string(),
        ]
    );
    assert!(
        store
            .load_external_memory_entries(&SessionId("other-session".into()))
            .is_empty()
    );
    assert!(
        store
            .load_nested_memory_lineage(&SessionId("other-session".into()))
            .is_empty()
    );
}

#[test]
fn resolve_session_state_reuses_store_for_continue_resume_and_fresh_start() {
    let store = InMemorySessionStore::default();
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-restore".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/restore".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("restored"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            }],
        },
    );

    let continued = resolve_session_state(
        &store,
        Some(&RestoreRequest {
            source: RestoreSource::ContinueSession,
            session_id: None,
        }),
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/fresh"),
    );
    assert_eq!(continued.snapshot.session_id.0, "session-restore");
    assert_eq!(continued.snapshot.surface, InteractionSurface::Remote);
    assert!(continued.restored_session.is_some());

    let resumed_missing = resolve_session_state(
        &store,
        Some(&RestoreRequest {
            source: RestoreSource::ResumeSession,
            session_id: Some("missing-session".into()),
        }),
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/fallback"),
    );
    assert_eq!(resumed_missing.snapshot.session_id.0, "missing-session");
    assert_eq!(resumed_missing.snapshot.surface, InteractionSurface::Cli);
    assert_eq!(resumed_missing.snapshot.session_mode, SessionMode::Headless);
    assert!(resumed_missing.restored_session.is_some());

    let fresh = resolve_session_state(
        &store,
        None,
        InteractionSurface::Cli,
        SessionMode::InitOnly,
        std::path::Path::new("/tmp/fresh-start"),
    );
    assert_eq!(fresh.snapshot.session_id.0, "local-session");
    assert_eq!(fresh.snapshot.surface, InteractionSurface::Cli);
    assert_eq!(fresh.snapshot.session_mode, SessionMode::InitOnly);
    assert!(fresh.history.entries.is_empty());
    assert!(fresh.restored_session.is_none());
}

#[test]
fn resolve_session_state_sanitizes_restored_memory_metadata() {
    let store = InMemorySessionStore::default();
    let session_id = SessionId("session-sanitized".into());
    store.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/restore".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    store.save_external_memory_entries(
        &session_id,
        vec![
            "  linear:ABC-1 context  ".into(),
            " ".into(),
            "x".repeat(300),
        ],
    );
    store.save_nested_memory_lineage(
        &session_id,
        vec![
            "agent:orphan:inherit_context=true".into(),
            "session:session-sanitized".into(),
            "agent:child:inherit_context=true".into(),
            "agent:child:inherit_context=true".into(),
            "bad marker".into(),
        ],
    );

    let resolved = resolve_session_state(
        &store,
        Some(&RestoreRequest {
            source: RestoreSource::ResumeSession,
            session_id: Some("session-sanitized".into()),
        }),
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/fresh"),
    );

    assert_eq!(
        resolved.external_memory_entries,
        vec!["linear:ABC-1 context".to_string(), "x".repeat(240)]
    );
    assert_eq!(
        resolved.nested_memory_lineage,
        vec![
            "session:session-sanitized".to_string(),
            "agent:child:inherit_context=true".to_string(),
        ]
    );
}

#[test]
fn initialize_runtime_builds_consistent_runtime_bundle_shape() {
    let runtime = runtime_for_surface("cli", false, false);
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let bundle = runtime
        .initialize_runtime(
            &state,
            "session-init".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");

    assert!(!bundle.command_registry.names().is_empty());
    assert!(!bundle.coordinator_tools.all_metadata().is_empty());
    assert_eq!(bundle.api_client.provider_config(), bundle.provider_config);
    assert_eq!(
        bundle.coordinator_tools.all_metadata(),
        bundle.runtime_tool_registry.blocking_read().all_metadata()
    );
}

#[test]
fn augment_prompt_depends_on_input_state_without_mutating_store() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config());
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");
    let bundle = runtime
        .initialize_runtime(
            &state,
            "session-prompts".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");
    let resolved = resolve_session_state(
        &InMemorySessionStore::default(),
        None,
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/prompt"),
    );
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: Some(bundle.command_registry.clone()),
        runtime_tool_registry: Some(bundle.runtime_tool_registry.clone()),
        skill_registry: Some(bundle.skill_registry.clone()),
        mcp_runtime: Some(bundle.mcp_runtime.clone()),
        plugin_load_result: Some(bundle.plugin_load_result.clone()),
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: bundle.notification_dispatcher.clone(),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_session_id: "session-prompts".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(resolved.snapshot.clone()),
        history: Some(resolved.history.clone()),
        restored_session: resolved.restored_session.clone(),
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    let store = AppStateStore::new(app_state.clone());
    let before = store.generation();
    let prompts = runtime.augment_prompts(&app_state, &bundle);
    let after = store.generation();

    assert_eq!(before, after);
    assert_eq!(
        prompts.metadata,
        PromptAugmentationMetadata {
            active_session_id: "session-prompts".into(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            visible_tool_count: bundle
                .coordinator_tools
                .visible_tools(&app_state.permission_context)
                .len(),
        }
    );
    assert!(!prompts.system_prompt.is_empty());
    assert!(!prompts.tools_prompt.is_empty());
    assert!(!prompts.context_prompt.is_empty());
}

#[test]
fn initialize_runtime_matrix_locks_surface_mode_env_flag_visibility() {
    let cli_interactive =
        initialized_tool_names(InteractionSurface::Cli, SessionMode::Interactive, false);
    assert!(cli_interactive.contains(&"Read"));
    assert!(cli_interactive.contains(&"Agent"));
    assert!(cli_interactive.contains(&"Bash"));
    assert!(cli_interactive.contains(&"WebSearch"));
    assert!(cli_interactive.contains(&"WebFetch"));
    assert!(cli_interactive.contains(&"Mcp"));

    let cli_headless =
        initialized_tool_names(InteractionSurface::Cli, SessionMode::Headless, false);
    assert!(cli_headless.contains(&"Read"));
    assert!(cli_headless.contains(&"Agent"));
    assert!(!cli_headless.contains(&"Bash"));
    assert!(!cli_headless.contains(&"WebSearch"));
    assert!(!cli_headless.contains(&"WebFetch"));
    assert!(!cli_headless.contains(&"Mcp"));

    let remote_interactive =
        initialized_tool_names(InteractionSurface::Remote, SessionMode::Interactive, false);
    assert!(remote_interactive.contains(&"Read"));
    assert!(remote_interactive.contains(&"Agent"));
    assert!(remote_interactive.contains(&"AskUserQuestion"));
    assert!(!remote_interactive.contains(&"Bash"));
    assert!(!remote_interactive.contains(&"WebSearch"));
    assert!(!remote_interactive.contains(&"WebFetch"));
    assert!(!remote_interactive.contains(&"Mcp"));

    let cli_init_only =
        initialized_tool_names(InteractionSurface::Cli, SessionMode::InitOnly, true);
    assert!(cli_init_only.contains(&"Read"));
    assert!(cli_init_only.contains(&"Agent"));
    assert!(!cli_init_only.contains(&"Bash"));
    assert!(!cli_init_only.contains(&"WebSearch"));
    assert!(!cli_init_only.contains(&"WebFetch"));
    assert!(!cli_init_only.contains(&"Mcp"));
}

#[test]
fn gate_user_access_matches_cli_remote_and_telegram_expectations() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config());

    let cli_state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Interactive, false);
    let cli_input = rust_agent::interaction::envelope::NormalizedInput::from_session_raw(
        InteractionSurface::Cli,
        "session-cli",
        "/permissions",
    );
    assert_eq!(
        runtime.gate_user_access(&cli_state, Some(&cli_input)),
        UserAccessDecision {
            allowed: true,
            reason: None,
        }
    );

    let remote_state =
        BootstrapState::new(InteractionSurface::Remote, SessionMode::Interactive, false);
    let remote_input = rust_agent::interaction::envelope::NormalizedInput::from_remote_raw(
        "session-remote",
        "actor-a",
        true,
        true,
        "/permissions",
    );
    assert_eq!(
        runtime
            .gate_user_access(&remote_state, Some(&remote_input))
            .allowed,
        false
    );

    let telegram_state = BootstrapState::new(
        InteractionSurface::Telegram,
        SessionMode::Interactive,
        false,
    );
    let telegram_input = rust_agent::interaction::envelope::NormalizedInput::from_session_raw(
        InteractionSurface::Telegram,
        "session-telegram",
        "hello",
    );
    assert_eq!(
        runtime.gate_user_access(&telegram_state, Some(&telegram_input)),
        UserAccessDecision {
            allowed: true,
            reason: None,
        }
    );
}

#[test]
fn file_backed_session_store_round_trips_across_store_instances() {
    let root = unique_temp_path("rust-agent-session-store");
    let store_a = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-file-backed".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/file-backed".into(),
        last_turn_at: Some("2026-04-11T12:00:00Z".into()),
        prompt_seed: Some("seed".into()),
    };
    let history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::assistant("persisted history"),
            timestamp: Some("2026-04-11T12:00:01Z".into()),
            tool_refs: vec!["TaskList".into()],
            milestone: None,
        }],
    };
    let task_list = TaskListSnapshot {
        next_id: 2,
        tasks: vec![TaskListItem {
            id: "task-0".into(),
            subject: "persisted task".into(),
            description: "from file-backed store".into(),
            active_form: Some("Persisting".into()),
            status: TaskListStatus::Pending,
            owner: Some("session-file-backed".into()),
            plan_step_id: None,
            blocks: vec![],
            blocked_by: vec![],
        }],
    };

    store_a.save(snapshot.clone(), history.clone());
    store_a.save_task_list(&session_id, task_list.clone());
    store_a.save_external_memory_entries(
        &session_id,
        vec!["linear:INGEST-42 investigate context layering".into()],
    );
    store_a.save_nested_memory_lineage(&session_id, vec!["session:session-file-backed".into()]);

    let store_b = FileBackedSessionStore::new(root.clone());
    let loaded = store_b.load(&SessionRestoreRequest {
        resume: Some("session-file-backed".into()),
        continue_session: false,
    });
    assert_eq!(loaded, Some((snapshot, history)));
    assert_eq!(store_b.load_task_list(&session_id), Some(task_list));
    assert_eq!(
        store_b.load_external_memory_entries(&session_id),
        vec!["linear:INGEST-42 investigate context layering".to_string()]
    );
    assert_eq!(
        store_b.load_nested_memory_lineage(&session_id),
        vec!["session:session-file-backed".to_string()]
    );

    std::fs::remove_dir_all(root).expect("cleanup file-backed session store");
}

#[test]
fn file_backed_session_store_loads_legacy_records_without_memory_fields() {
    let root = unique_temp_path("rust-agent-session-store-legacy");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-legacy".into());

    let legacy_json = r#"{
  "snapshot": {
    "session_id": "session-legacy",
    "surface": "Cli",
    "session_mode": "Interactive",
    "cwd": "/tmp/legacy",
    "last_turn_at": null,
    "prompt_seed": null
  },
  "history": {
    "entries": []
  },
  "task_list": null,
  "plan_state": null
}"#;
    let path = root.join("session-legacy.json");
    std::fs::write(path, legacy_json).expect("write legacy session record");

    let loaded = store.load(&SessionRestoreRequest {
        resume: Some("session-legacy".into()),
        continue_session: false,
    });
    assert!(loaded.is_some(), "legacy record should deserialize");
    assert!(store.load_external_memory_entries(&session_id).is_empty());
    assert!(store.load_nested_memory_lineage(&session_id).is_empty());

    std::fs::remove_dir_all(root).expect("cleanup legacy file-backed session store");
}

#[test]
fn finalize_runtime_state_is_single_writeback_entrypoint() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config());
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.record_phase(BootstrapPhase::InitializeRuntime);
    state.record_phase(BootstrapPhase::AugmentPrompt);
    state.record_phase(BootstrapPhase::GateUserAccess);
    let state = state.finalize();

    let resolved = resolve_session_state(
        &InMemorySessionStore::default(),
        None,
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/finalize"),
    );
    let bundle = runtime
        .initialize_runtime(
            &state,
            resolved.active_session_id(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");
    let prompt_state = AppState {
        surface: state.surface,
        session_mode: state.session_mode,
        client_type: state.client_type,
        session_source: state.session_source,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_active_session_id(resolved.active_session_id())
            .with_active_surface(state.surface),
        command_registry: Some(bundle.command_registry.clone()),
        runtime_tool_registry: Some(bundle.runtime_tool_registry.clone()),
        skill_registry: Some(bundle.skill_registry.clone()),
        mcp_runtime: Some(bundle.mcp_runtime.clone()),
        plugin_load_result: Some(bundle.plugin_load_result.clone()),
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: bundle.notification_dispatcher.clone(),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: state
            .phases
            .iter()
            .map(|phase| format!("{phase:?}"))
            .collect(),
        active_session_id: resolved.active_session_id(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(resolved.snapshot.clone()),
        history: Some(resolved.history.clone()),
        restored_session: resolved.restored_session.clone(),
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    };
    let prompts = runtime.augment_prompts(&prompt_state, &bundle);
    let finalized = runtime.finalize_runtime_state(
        &state,
        resolved.clone(),
        bundle,
        prompts.clone(),
        resolved.active_session_id(),
    );

    assert_eq!(
        finalized.app_state.active_session_id,
        resolved.active_session_id()
    );
    assert_eq!(finalized.store.generation(), 0);
    assert_eq!(
        finalized.engine.context.system_prompt,
        prompts.system_prompt
    );
    assert_eq!(
        finalized.engine.context.tools_prompt,
        rust_agent::prompt::tools::build_tools_prompt(
            &finalized.engine.context.tool_registry,
            &finalized.app_state.permission_context,
        )
    );
    assert_eq!(
        finalized.engine.context.context_prompt,
        prompts.context_prompt
    );
}

#[tokio::test]
async fn runtime_continue_session_uses_restored_snapshot() {
    let store = Arc::new(InMemorySessionStore::default());
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-continue".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            cwd: "/tmp/continue".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("hello again"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            }],
        },
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: true,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config())
    .with_session_store(store);

    runtime.run().await.expect("runtime should run");
}

#[tokio::test]
async fn runtime_resume_prefers_restored_surface_and_mode() {
    let store = Arc::new(InMemorySessionStore::default());
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-remote".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/resume".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: Some("session-remote".into()),
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config())
    .with_session_store(store);

    runtime
        .run()
        .await
        .expect("runtime should run with restored mode");
}

#[test]
fn initialize_runtime_tracks_surface_mode_visibility_matrix() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: true,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config());

    let mut cli_state =
        BootstrapState::new(InteractionSurface::Cli, SessionMode::Interactive, false);
    cli_state.current_cwd = std::env::current_dir().expect("cwd available");
    let cli_bundle = runtime
        .initialize_runtime(
            &cli_state,
            "session-cli-matrix".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");
    let cli_names = cli_bundle
        .coordinator_tools
        .visible_tools(
            &ToolPermissionContext::new(PermissionMode::Default)
                .with_active_surface(InteractionSurface::Cli)
                .with_deferred_tools(cli_state.session_mode == SessionMode::Interactive)
                .with_interactive_tools(cli_state.session_mode == SessionMode::Interactive),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(cli_names.contains(&"Agent"));
    assert!(cli_names.contains(&"WebSearch"));

    let mut remote_state =
        BootstrapState::new(InteractionSurface::Remote, SessionMode::Interactive, false);
    remote_state.current_cwd = std::env::current_dir().expect("cwd available");
    let remote_bundle = runtime
        .initialize_runtime(
            &remote_state,
            "session-remote-matrix".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");
    let remote_names = remote_bundle
        .coordinator_tools
        .visible_tools(
            &ToolPermissionContext::new(PermissionMode::Default)
                .with_active_surface(InteractionSurface::Remote)
                .with_deferred_tools(remote_state.session_mode == SessionMode::Interactive)
                .with_interactive_tools(remote_state.session_mode == SessionMode::Interactive),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(remote_names.contains(&"Agent"));
    assert!(!remote_names.contains(&"WebSearch"));

    let worker_names = remote_bundle
        .coordinator_tools
        .filter_for_worker()
        .visible_tools(&ToolPermissionContext::new(PermissionMode::Default))
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!worker_names.contains(&"Agent"));
    assert!(!worker_names.contains(&"WebSearch"));
}

#[tokio::test]
async fn runtime_resume_keeps_restored_surface_visibility_contract() {
    let store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("session-remote-contract".into());
    store.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-contract".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: Some("Read /tmp/demo".into()),
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: Some(session_id.0.clone()),
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config())
    .with_session_store(store.clone());

    let resolved = resolve_session_state(
        store.as_ref(),
        Some(&RestoreRequest {
            source: RestoreSource::ResumeSession,
            session_id: Some(session_id.0.clone()),
        }),
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/remote-contract"),
    );
    let mut state = BootstrapState::new(
        resolved.snapshot.surface,
        resolved.snapshot.session_mode,
        false,
    );
    state.current_cwd = std::env::current_dir().expect("cwd available");
    let bundle = runtime
        .initialize_runtime(
            &state,
            resolved.active_session_id(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize");

    let permission_context = ToolAssemblyContext::coordinator(state.surface, state.session_mode)
        .permission_context(PermissionMode::Default)
        .with_active_surface(state.surface);
    let visible_names = bundle
        .coordinator_tools
        .visible_tools(&permission_context)
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert_eq!(state.surface, InteractionSurface::Remote);
    assert_eq!(state.session_mode, SessionMode::Interactive);
    assert!(visible_names.contains(&"Agent"));
    assert!(!visible_names.contains(&"WebSearch"));
}

#[tokio::test]
async fn runtime_restores_persisted_task_list_for_resumed_session() {
    let store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("session-with-tasks".into());
    store.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/tasks".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    store.insert_task_list(
        session_id,
        TaskListSnapshot {
            next_id: 2,
            tasks: vec![TaskListItem {
                id: "task-0".into(),
                subject: "restored task".into(),
                description: "from persisted state".into(),
                active_form: Some("Restoring".into()),
                status: TaskListStatus::Pending,
                owner: Some("session-with-tasks".into()),
                plan_step_id: None,
                blocks: vec![],
                blocked_by: vec![],
            }],
        },
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: Some("TaskGet task-0".into()),
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: Some("session-with-tasks".into()),
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config())
    .with_session_store(store);

    runtime
        .run()
        .await
        .expect("runtime should restore persisted task list");
}

#[tokio::test]
async fn runtime_continue_restores_from_file_backed_store_across_instances() {
    let root = unique_temp_path("rust-agent-runtime-continue");
    let session_id = SessionId("session-continue-durable".into());
    let store_a = Arc::new(FileBackedSessionStore::new(root.clone()));
    store_a.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/runtime-continue".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("durable session"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            }],
        },
    );
    store_a.save_task_list(
        &session_id,
        TaskListSnapshot {
            next_id: 2,
            tasks: vec![TaskListItem {
                id: "task-0".into(),
                subject: "durable task".into(),
                description: "restored across instances".into(),
                active_form: Some("Durably restoring".into()),
                status: TaskListStatus::Pending,
                owner: Some("session-continue-durable".into()),
                plan_step_id: None,
                blocks: vec![],
                blocked_by: vec![],
            }],
        },
    );

    let store_b = Arc::new(FileBackedSessionStore::new(root.clone()));
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: Some("TaskGet task-0".into()),
        interactive: false,
        init_only: false,
        continue_session: true,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config())
    .with_session_store(store_b);

    runtime
        .run()
        .await
        .expect("runtime should continue from durable session store");

    std::fs::remove_dir_all(root).expect("cleanup durable runtime test store");
}

#[tokio::test]
async fn runtime_initializes_fresh_session_record_in_store() {
    let store = Arc::new(InMemorySessionStore::default());

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_provider_config(test_model_provider_config())
    .with_session_store(store.clone());

    runtime
        .run()
        .await
        .expect("runtime should initialize session");

    let loaded = store.load(&SessionRestoreRequest {
        resume: None,
        continue_session: true,
    });
    let (snapshot, history) = loaded.expect("expected initialized session record");
    assert_eq!(snapshot.session_id.0, "local-session");
    assert_eq!(snapshot.surface, InteractionSurface::Cli);
    assert_eq!(snapshot.session_mode, SessionMode::InitOnly);
    assert!(history.entries.is_empty());
}

#[test]
fn file_backed_session_store_persists_appended_turns_across_instances() {
    let root = unique_temp_path("rust-agent-session-history");
    let session_id = SessionId("session-history-durable".into());
    let store_a = FileBackedSessionStore::new(root.clone());
    store_a.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/history".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    store_a.append_entry(
        &session_id,
        SessionHistoryEntry {
            message: Message::user("hello"),
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: None,
        },
    );
    store_a.append_entry(
        &session_id,
        SessionHistoryEntry {
            message: Message::assistant("hi there"),
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: None,
        },
    );

    let store_b = FileBackedSessionStore::new(root.clone());
    let (_, history) = store_b
        .load(&SessionRestoreRequest {
            resume: Some("session-history-durable".into()),
            continue_session: false,
        })
        .expect("expected durable history after append");
    assert_eq!(history.entries.len(), 2);
    assert_eq!(history.entries[0].message, Message::user("hello"));
    assert_eq!(history.entries[1].message, Message::assistant("hi there"));

    std::fs::remove_dir_all(root).expect("cleanup durable history store");
}

#[test]
fn hook_event_enum_exposes_bootstrap_lifecycle_markers() {
    assert_eq!(HookEvent::SessionStart, HookEvent::SessionStart);
    assert_eq!(HookEvent::Setup, HookEvent::Setup);
}

#[test]
fn bootstrap_hook_loader_defaults_without_project_config() {
    let root = unique_temp_path("rust-agent-bootstrap-hooks");
    std::fs::create_dir_all(&root).expect("create bootstrap hook root");

    let registry = load_hook_registry(&root);
    let load_result = registry
        .config_load_result()
        .expect("bootstrap should retain hook load metadata");
    assert_eq!(load_result.source, HookConfigSource::Defaults);
    assert!(
        load_result
            .diagnostics
            .iter()
            .any(|line| line.contains("No .claude/hooks.json found"))
    );

    std::fs::remove_dir_all(root).expect("cleanup bootstrap hook root");
}

#[test]
fn tui_exit_input_matches_expected_commands() {
    assert!(is_tui_exit_input("/exit"));
    assert!(is_tui_exit_input("exit"));
    assert!(is_tui_exit_input("quit"));
    assert!(is_tui_exit_input("  quit  "));
    assert!(!is_tui_exit_input("/help"));
    assert!(!is_tui_exit_input(""));
}

#[test]
fn tui_clear_screen_prefix_uses_terminal_escape_sequence() {
    assert_eq!(tui_clear_screen_prefix(), "\x1B[2J\x1B[H");
}

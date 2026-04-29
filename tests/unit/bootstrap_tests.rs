use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::model_profiles::{
    build_model_profile_display_view, load_model_profiles_registry_from_root,
};
use rust_agent::bootstrap::teammate_registry::{
    load_teammate_registry_from_root, parse_teammate_registry,
};
use rust_agent::bootstrap::{
    BootstrapCli, BootstrapPhase, BootstrapState, InteractionSurface, PromptAugmentationMetadata,
    RuntimeBootstrap, SessionMode, SessionSource, ShutdownFailure, ShutdownOutcome, StartupWarning,
    UserAccessDecision, execute_runtime_shutdown_with_deadline, is_tui_exit_input,
    runtime_shutdown_timeout, tui_clear_screen_prefix,
};
use rust_agent::core::message::Message;
use rust_agent::history::resume::{RestoreRequest, RestoreSource, resolve_session_state};
use rust_agent::history::session::{
    FileBackedSessionStore, InMemorySessionStore, PersistedSessionRecord, SessionHistory,
    SessionHistoryEntry, SessionId, SessionLifecycleStatus, SessionRestoreRequest, SessionSnapshot,
    SessionStore, SessionStoreWriteError, SessionStoreWriteErrorKind,
};
use rust_agent::hook::registry::{HookConfigSource, HookEvent, load_hook_registry};
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout,
};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::state::app_state::{
    AppState, AppStateRuntimeChange, RuntimeRole, SessionPersistFailure,
};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::state::store::AppStateStore;
use rust_agent::task::list_types::{TaskListItem, TaskListSnapshot, TaskListStatus};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolAssemblyContext;
use std::time::Duration;

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
        attachments: Vec::new(),
        surface: surface.into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    })
    .with_provider_config(test_model_provider_config())
}

fn test_model_provider_config() -> ModelProviderConfig {
    ModelProviderConfig {
        provider_id: "anthropic".into(),
        protocol: ProviderProtocol::Anthropic,
        compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
        base_url: "http://localhost".into(),
        chat_completions_path: "/v1/chat/completions".into(),
        auth_strategy: ProviderAuthStrategy::NoAuth,
        api_key: None,
        api_key_env: None,
        model_id: "test-model".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 30_000,
            stream_timeout_ms: 120_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
        pricing: ModelPricing::default(),
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    }
}

fn set_env_var(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) }
}

fn remove_env_var(key: &str) {
    unsafe { std::env::remove_var(key) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyEnvScenario {
    NoProxyEnv,
    RustAgentProxyOnly,
    SystemProxyOnly,
    DualLayerProxyEnv,
}

fn apply_proxy_env_scenario(scenario: ProxyEnvScenario) {
    for key in [
        "RUST_AGENT_PROXY_URL",
        "RUST_AGENT_NO_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    ] {
        remove_env_var(key);
    }

    match scenario {
        ProxyEnvScenario::NoProxyEnv => {}
        ProxyEnvScenario::RustAgentProxyOnly => {
            set_env_var("RUST_AGENT_PROXY_URL", "http://rust-agent-proxy:3128");
            set_env_var("RUST_AGENT_NO_PROXY", "rust-agent.local");
        }
        ProxyEnvScenario::SystemProxyOnly => {
            set_env_var("HTTPS_PROXY", "http://system-https-proxy:8443");
            set_env_var("NO_PROXY", "example.local");
        }
        ProxyEnvScenario::DualLayerProxyEnv => {
            set_env_var("RUST_AGENT_PROXY_URL", "http://rust-agent-proxy:3128");
            set_env_var("RUST_AGENT_NO_PROXY", "rust-agent.local");
            set_env_var("HTTPS_PROXY", "http://system-https-proxy:8443");
            set_env_var("HTTP_PROXY", "http://system-http-proxy:8080");
            set_env_var("NO_PROXY", "system.local");
        }
    }
}

fn bootstrap_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct BootstrapEnvGuard {
    keys: [&'static str; 21],
}

impl BootstrapEnvGuard {
    fn new() -> Self {
        let keys = [
            "RUST_AGENT_PROVIDER_ID",
            "RUST_AGENT_PROVIDER_BASE_URL",
            "RUST_AGENT_PROVIDER_API_KEY",
            "RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH",
            "RUST_AGENT_PROVIDER_DEFAULT_MODEL",
            "RUST_AGENT_PROVIDER_MODEL",
            "RUST_AGENT_PROVIDER_PROTOCOL",
            "RUST_AGENT_PROVIDER_COMPATIBILITY_PROFILE",
            "RUST_AGENT_PROVIDER_AUTH_STRATEGY",
            "RUST_AGENT_PROVIDER_TIMEOUT_MS",
            "RUST_AGENT_PROVIDER_STREAM_TIMEOUT_MS",
            "RUST_AGENT_PROVIDER_RETRY_MAX_ATTEMPTS",
            "RUST_AGENT_PROVIDER_RETRY_INITIAL_BACKOFF_MS",
            "RUST_AGENT_PROVIDER_RETRY_MAX_BACKOFF_MS",
            "OPENAI_API_KEY",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
            "RUST_AGENT_PROXY_URL",
            "RUST_AGENT_NO_PROXY",
            "RUST_AGENT_CA_BUNDLE",
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

fn shutdown_test_app_state(
    session_store: Arc<InMemorySessionStore>,
    task_manager: Arc<TaskManager>,
) -> AppState {
    let session_id = SessionId("shutdown-session".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: ".".into(),
        last_turn_at: None,
        prompt_seed: None,
    };
    session_store.save(snapshot.clone(), SessionHistory::default());
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager)
        .with_active_session_id(session_id.0.clone())
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(
            rust_agent::interaction::dispatcher::NotificationDispatcher::new(
                rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
            ),
        );
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
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
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "none".into(),
        },
        active_session_id: session_id.0,
        session_store: Some(session_store),
        session: Some(snapshot),
        history: Some(SessionHistory::default()),
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    }
}

#[derive(Debug)]
struct FlakySessionStore {
    inner: InMemorySessionStore,
    transient_full_record_failures: AtomicUsize,
    full_record_attempts: AtomicUsize,
}

impl FlakySessionStore {
    fn new(transient_full_record_failures: usize) -> Self {
        Self {
            inner: InMemorySessionStore::default(),
            transient_full_record_failures: AtomicUsize::new(transient_full_record_failures),
            full_record_attempts: AtomicUsize::new(0),
        }
    }

    fn transient_error(operation: &'static str) -> SessionStoreWriteError {
        SessionStoreWriteError {
            operation,
            kind: SessionStoreWriteErrorKind::IoTransient,
            detail: "simulated transient write failure".into(),
        }
    }

    fn full_record_attempts(&self) -> usize {
        self.full_record_attempts.load(Ordering::SeqCst)
    }
}

impl SessionStore for FlakySessionStore {
    fn load(&self, request: &SessionRestoreRequest) -> Option<(SessionSnapshot, SessionHistory)> {
        self.inner.load(request)
    }

    fn save(
        &self,
        snapshot: SessionSnapshot,
        history: SessionHistory,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.save(snapshot, history)
    }

    fn save_full_record(
        &self,
        session_id: &SessionId,
        record: PersistedSessionRecord,
    ) -> Result<(), SessionStoreWriteError> {
        self.full_record_attempts.fetch_add(1, Ordering::SeqCst);
        let previous = self
            .transient_full_record_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                if value > 0 { Some(value - 1) } else { None }
            })
            .unwrap_or(0);
        if previous > 0 {
            return Err(Self::transient_error("save_full_record"));
        }
        self.inner.save_full_record(session_id, record)
    }

    fn append_entry(
        &self,
        session_id: &SessionId,
        entry: SessionHistoryEntry,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.append_entry(session_id, entry)
    }

    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot> {
        self.inner.load_task_list(session_id)
    }

    fn save_task_list(
        &self,
        session_id: &SessionId,
        snapshot: TaskListSnapshot,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.save_task_list(session_id, snapshot)
    }

    fn load_plan_state(
        &self,
        session_id: &SessionId,
    ) -> Option<rust_agent::plan::types::PlanState> {
        self.inner.load_plan_state(session_id)
    }

    fn save_plan_state(
        &self,
        session_id: &SessionId,
        state: rust_agent::plan::types::PlanState,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.save_plan_state(session_id, state)
    }

    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String> {
        self.inner.load_external_memory_entries(session_id)
    }

    fn save_external_memory_entries(
        &self,
        session_id: &SessionId,
        entries: Vec<String>,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.save_external_memory_entries(session_id, entries)
    }

    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String> {
        self.inner.load_nested_memory_lineage(session_id)
    }

    fn save_nested_memory_lineage(
        &self,
        session_id: &SessionId,
        lineage: Vec<String>,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.save_nested_memory_lineage(session_id, lineage)
    }

    fn load_lifecycle_status(&self, session_id: &SessionId) -> SessionLifecycleStatus {
        self.inner.load_lifecycle_status(session_id)
    }

    fn save_lifecycle_status(
        &self,
        session_id: &SessionId,
        status: SessionLifecycleStatus,
    ) -> Result<(), SessionStoreWriteError> {
        self.inner.save_lifecycle_status(session_id, status)
    }
}

#[test]
fn runtime_shutdown_timeout_uses_env_override() {
    let _guard = bootstrap_env_lock().lock().expect("env lock");
    let _env = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_RUNTIME_SHUTDOWN_TIMEOUT_MS", "2500");
    assert_eq!(runtime_shutdown_timeout(), Duration::from_millis(2500));
}

#[tokio::test]
async fn execute_runtime_shutdown_forces_hibernation_after_deadline() {
    let store = Arc::new(InMemorySessionStore::default());
    let tasks = Arc::new(TaskManager::default());
    let app_state = shutdown_test_app_state(store.clone(), tasks.clone());
    let task = tasks.create("shutdown task", "shutdown-session", InteractionSurface::Cli);
    tasks.launch(&task.id, "work", std::future::pending::<()>());

    let outcome = execute_runtime_shutdown_with_deadline(
        app_state.clone(),
        "test.shutdown_timeout",
        Duration::from_millis(10),
    )
    .await;

    assert_eq!(
        outcome,
        ShutdownOutcome::Forced {
            hibernated_task_ids: vec![task.id.clone()]
        }
    );

    assert!(app_state.cancellation_token.is_cancelled());
    assert_eq!(
        tasks.status(&task.id),
        Some(rust_agent::task::types::TaskStatus::Killed)
    );
    assert!(
        store
            .load(&SessionRestoreRequest {
                resume: Some("shutdown-session".into()),
                continue_session: false,
            })
            .is_some(),
        "shutdown should persist the current session state"
    );
}

#[tokio::test]
async fn execute_runtime_shutdown_records_observability_for_persist_failures() {
    let store = Arc::new(InMemorySessionStore::default());
    let tasks = Arc::new(TaskManager::default());
    let mut app_state = shutdown_test_app_state(store, tasks);
    app_state.session_store = None;

    let outcome = execute_runtime_shutdown_with_deadline(
        app_state.clone(),
        "test.shutdown_persist_failure",
        Duration::from_millis(10),
    )
    .await;

    assert_eq!(
        outcome,
        ShutdownOutcome::Failed {
            failure: ShutdownFailure::PersistBeforeShutdown(
                SessionPersistFailure::MissingSessionStore
            ),
            hibernated_task_ids: Vec::new(),
        }
    );

    let snapshot = app_state.service_observability_tracker.snapshot();
    assert_eq!(
        snapshot
            .runtime_lifecycle_failures_by_phase
            .get("shutdown.persist_before"),
        Some(&1)
    );
    assert_eq!(
        snapshot
            .runtime_lifecycle_failures_by_reason
            .get("persist_before_shutdown:missing_session_store"),
        Some(&1)
    );
}

#[test]
fn persist_current_session_state_retries_transient_store_write_failures() {
    let store = Arc::new(InMemorySessionStore::default());
    let tasks = Arc::new(TaskManager::default());
    let mut app_state = shutdown_test_app_state(store, tasks);
    let flaky_store = Arc::new(FlakySessionStore::new(2));
    app_state.session_store = Some(flaky_store.clone());

    assert_eq!(app_state.persist_current_session_state(), Ok(()));
    assert_eq!(flaky_store.full_record_attempts(), 3);
    assert!(
        flaky_store
            .load(&SessionRestoreRequest {
                resume: Some("shutdown-session".into()),
                continue_session: false,
            })
            .is_some(),
        "final retry should persist the session after transient failures"
    );
}

#[test]
fn persist_resolved_session_state_retries_transient_store_write_failures() {
    let store = Arc::new(InMemorySessionStore::default());
    let tasks = Arc::new(TaskManager::default());
    let mut app_state = shutdown_test_app_state(store, tasks);
    let flaky_store = Arc::new(FlakySessionStore::new(2));
    app_state.session_store = Some(flaky_store.clone());
    let snapshot = app_state.session.clone().expect("test session snapshot");
    let history = app_state.history.clone().unwrap_or_default();
    let resolved = rust_agent::history::resume::ResolvedSessionState {
        snapshot,
        history,
        restored_session: None,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        external_memory_entries: Vec::new(),
        nested_memory_lineage: Vec::new(),
    };

    assert_eq!(app_state.persist_resolved_session_state(&resolved), Ok(()));
    assert_eq!(flaky_store.full_record_attempts(), 3);
    assert!(
        flaky_store
            .load(&SessionRestoreRequest {
                resume: Some("shutdown-session".into()),
                continue_session: false,
            })
            .is_some(),
        "final retry should persist the resolved session after transient failures"
    );
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
fn bootstrap_uses_models_toml_active_when_no_env_override() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-models-active");
    fs::create_dir_all(root.join(".claude")).expect("create config root");
    fs::write(
        root.join(".claude/models.toml"),
        r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"
"#,
    )
    .expect("write models.toml");
    set_env_var("OPENAI_API_KEY", "models-key");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = root;

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-models-active".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize from models.toml");

    assert_eq!(bundle.provider_config.provider_id, "openai");
    assert_eq!(bundle.provider_config.model_id, "gpt-4.1-mini");
    assert_eq!(
        bundle.provider_config.api_key.as_deref(),
        Some("models-key")
    );
    assert_eq!(
        bundle.provider_config.chat_completions_path,
        "/v1/chat/completions"
    );
    assert_eq!(
        bundle.active_model_profile_name.as_deref(),
        Some("openai-fast")
    );
    assert_eq!(
        bundle.active_model_profile_source,
        rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml
    );
}

#[test]
fn bootstrap_models_toml_missing_file_falls_back_to_existing_bootstrap_defaults() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-models-missing");
    fs::create_dir_all(root.join(".claude")).expect("create config root");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = root;

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-models-missing".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should keep existing defaults when models.toml is absent");

    assert_eq!(bundle.provider_config.provider_id, "anthropic");
    assert_eq!(bundle.provider_config.base_url, "http://localhost");
    assert_eq!(bundle.provider_config.model_id, "default-model");
    assert_eq!(bundle.active_model_profile_name, None);
    assert_eq!(
        bundle.active_model_profile_source,
        rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault
    );
}

#[test]
fn bootstrap_ignores_models_toml_when_explicit_provider_env_exists() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-models-env-override");
    fs::create_dir_all(root.join(".claude")).expect("create config root");
    fs::write(
        root.join(".claude/models.toml"),
        r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"
"#,
    )
    .expect("write models.toml");
    set_env_var("OPENAI_API_KEY", "models-key");
    set_env_var("RUST_AGENT_PROVIDER_ID", "openai");
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "http://localhost:4010");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "env-key");
    set_env_var("RUST_AGENT_PROVIDER_DEFAULT_MODEL", "env-model");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = root;

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-models-env-override".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should ignore models.toml when explicit env exists");

    assert_eq!(bundle.provider_config.base_url, "http://localhost:4010");
    assert_eq!(bundle.provider_config.model_id, "env-model");
    assert_eq!(bundle.provider_config.api_key.as_deref(), Some("env-key"));
    assert_eq!(bundle.active_model_profile_name, None);
    assert_eq!(
        bundle.active_model_profile_source,
        rust_agent::state::app_state::ActiveModelProfileSource::EnvOverride
    );
}

#[test]
fn bootstrap_api_key_env_resolves_into_api_key() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-models-api-key-env");
    fs::create_dir_all(root.join(".claude")).expect("create config root");
    fs::write(
        root.join(".claude/models.toml"),
        r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"
"#,
    )
    .expect("write models.toml");
    set_env_var("OPENAI_API_KEY", "resolved-secret");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = root;

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-models-api-key-env".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should resolve api_key_env into api_key");

    assert_eq!(
        bundle.provider_config.api_key.as_deref(),
        Some("resolved-secret")
    );
}

#[test]
fn bootstrap_model_registry_loader_returns_profiles_and_active_name() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("OPENAI_API_KEY", "models-key");
    let root = unique_temp_path("rust-agent-model-registry-loader");
    fs::create_dir_all(root.join(".claude")).expect("create config root");
    fs::write(
        root.join(".claude/models.toml"),
        r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"

[profiles.local-dev]
provider_id = "local"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "http://localhost:1234"
model = "local-model"
auth_strategy = "none"
"#,
    )
    .expect("write models.toml");

    let registry = load_model_profiles_registry_from_root(&root.join(".claude"))
        .expect("registry should load")
        .expect("models.toml should exist");

    assert_eq!(registry.active, "openai-fast");
    assert_eq!(registry.profiles.len(), 2);
    assert!(registry.profiles.contains_key("openai-fast"));
    assert!(registry.profiles.contains_key("local-dev"));

    fs::remove_dir_all(root).expect("cleanup model registry loader root");
}

#[test]
fn bootstrap_model_profile_display_view_redacts_secret_and_reports_env_status() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("OPENAI_API_KEY", "resolved-secret");
    let registry = rust_agent::bootstrap::model_profiles::parse_model_profiles_registry(
        r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"
request_timeout_ms = 10000
stream_timeout_ms = 90000
retry_max_attempts = 2
retry_initial_backoff_ms = 100
retry_max_backoff_ms = 500
"#,
    )
    .expect("registry should parse");
    let spec = registry
        .profiles
        .get("openai-fast")
        .expect("profile should exist");

    let view = build_model_profile_display_view("openai-fast", spec).expect("view should build");
    remove_env_var("OPENAI_API_KEY");

    assert_eq!(view.api_key_env.as_deref(), Some("OPENAI_API_KEY"));
    assert_eq!(view.api_key_env_status.as_deref(), Some("set"));
    assert_eq!(view.request_timeout_ms, 10_000);
    assert_eq!(view.stream_timeout_ms, 90_000);
    assert_eq!(view.retry_max_attempts, 2);
    assert_eq!(view.retry_initial_backoff_ms, 100);
    assert_eq!(view.retry_max_backoff_ms, 500);
}

#[test]
fn bootstrap_model_registry_loader_reports_missing_file_cleanly() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-model-registry-missing");
    fs::create_dir_all(root.join(".claude")).expect("create config root");

    let registry = load_model_profiles_registry_from_root(&root.join(".claude"))
        .expect("missing models.toml should not error");

    assert!(registry.is_none());

    fs::remove_dir_all(root).expect("cleanup model registry missing root");
}

#[test]
fn bootstrap_gemini_openai_profile_does_not_infer_native_unsupported() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-models-gemini-openai");
    fs::create_dir_all(root.join(".claude")).expect("create config root");
    fs::write(
        root.join(".claude/models.toml"),
        r#"
active = "gemini-flash"

[profiles.gemini-flash]
provider_id = "gemini-openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://generativelanguage.googleapis.com/v1beta/openai"
chat_completions_path = "/chat/completions"
model = "gemini-2.5-flash"
api_key_env = "OPENAI_API_KEY"
"#,
    )
    .expect("write models.toml");
    set_env_var("OPENAI_API_KEY", "gemini-key");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = root;

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-models-gemini-openai".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should accept gemini openai-compatible profile");

    assert_eq!(bundle.provider_config.provider_id, "gemini-openai");
    assert_eq!(
        bundle.provider_config.protocol,
        ProviderProtocol::OpenAICompatible
    );
    assert_eq!(
        bundle.provider_config.compatibility_profile,
        ProviderCompatibilityProfileKind::OpenAICompatible
    );
    assert_eq!(
        bundle.provider_config.chat_completions_path,
        "/chat/completions"
    );
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
fn bootstrap_invalid_models_toml_fails_fast() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    let root = unique_temp_path("rust-agent-models-invalid");
    fs::create_dir_all(root.join(".claude")).expect("create config root");
    fs::write(
        root.join(".claude/models.toml"),
        r#"
active = "bad"

[profiles.bad]
provider_id = "custom-local"
protocol = "anthropic"
compatibility_profile = "openai_compatible"
base_url = "http://localhost:8080"
model = "local-model"
auth_strategy = "none"
"#,
    )
    .expect("write models.toml");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = root;

    let error = runtime
        .initialize_runtime(
            &state,
            "provider-models-invalid".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect_err("invalid models.toml should fail fast");

    assert!(error.to_string().contains("incompatible protocol/profile"));
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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

#[test]
fn bootstrap_uses_default_chat_completions_path_when_env_unset() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_ID", "openai");
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "https://api.openai.com");
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-env-default-path".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize with default chat completions path");

    assert_eq!(
        bundle.provider_config.chat_completions_path,
        "/v1/chat/completions"
    );
}

#[test]
fn bootstrap_accepts_custom_chat_completions_path_for_custom_provider() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_ID", "custom-provider");
    set_env_var(
        "RUST_AGENT_PROVIDER_BASE_URL",
        "https://generativelanguage.googleapis.com/v1beta/openai",
    );
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    set_env_var("RUST_AGENT_PROVIDER_DEFAULT_MODEL", "gemini-2.5-flash");
    set_env_var("RUST_AGENT_PROVIDER_PROTOCOL", "openai-compatible");
    set_env_var(
        "RUST_AGENT_PROVIDER_COMPATIBILITY_PROFILE",
        "openai-compatible",
    );
    set_env_var(
        "RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH",
        "/chat/completions",
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let bundle = runtime
        .initialize_runtime(
            &state,
            "provider-env-custom-path".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("runtime should initialize with custom chat completions path");

    assert_eq!(bundle.provider_config.provider_id, "custom-provider");
    assert_eq!(
        bundle.provider_config.protocol,
        ProviderProtocol::OpenAICompatible
    );
    assert_eq!(
        bundle.provider_config.compatibility_profile,
        ProviderCompatibilityProfileKind::OpenAICompatible
    );
    assert_eq!(
        bundle.provider_config.chat_completions_path,
        "/chat/completions"
    );
}

#[test]
fn bootstrap_rejects_invalid_chat_completions_path_env() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_ID", "openai");
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "https://api.openai.com");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    set_env_var("RUST_AGENT_PROVIDER_DEFAULT_MODEL", "gpt-test");
    set_env_var(
        "RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH",
        "https://example.com/v1/chat/completions",
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::InitOnly, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let error = runtime
        .initialize_runtime(
            &state,
            "provider-env-invalid-path".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect_err("runtime should reject full URL chat completions path");

    assert!(error.to_string().contains(
        "invalid_configuration: RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH must not be a full URL"
    ));
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "session-1".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
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
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "session-1".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
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
        active_model_runtime: previous.active_model_runtime.clone(),
        active_model_profile_name: previous.active_model_profile_name.clone(),
        active_model_profile_source: previous.active_model_profile_source.clone(),
        active_model_provider_summary: previous.active_model_provider_summary.clone(),
        active_session_id: previous.active_session_id.clone(),
        session_store: previous.session_store.clone(),
        session: previous.session.clone(),
        history: previous.history.clone(),
        restored_session: previous.restored_session.clone(),
        last_activity_ts: previous.last_activity_ts.clone(),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "session-prompts".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(resolved.snapshot.clone()),
        history: Some(resolved.history.clone()),
        restored_session: resolved.restored_session.clone(),
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
fn persist_current_session_state_commits_all_fields_in_one_record_write() {
    let root = unique_temp_path("rust-agent-persist-current-state");
    let store = Arc::new(FileBackedSessionStore::new(root.clone()));
    let session_id = SessionId("session-current-aggregate".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/current-aggregate".into(),
        last_turn_at: Some("2026-04-22T10:00:00Z".into()),
        prompt_seed: Some("seed-current".into()),
    };
    let history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::assistant("persist current state"),
            timestamp: Some("2026-04-22T10:00:01Z".into()),
            tool_refs: vec!["TaskCreate".into()],
            milestone: None,
        }],
    };
    let task_list = TaskListSnapshot {
        next_id: 2,
        tasks: vec![TaskListItem {
            id: "task-0".into(),
            subject: "persisted task list".into(),
            description: "kept across aggregate write".into(),
            active_form: Some("Persisting".into()),
            status: TaskListStatus::Pending,
            owner: Some(session_id.0.clone()),
            plan_step_id: None,
            blocks: vec![],
            blocked_by: vec![],
        }],
    };
    store.save(snapshot.clone(), history.clone());
    store.save_task_list(&session_id, task_list.clone());
    store.save_plan_state(&session_id, rust_agent::plan::types::PlanState::default());

    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_active_session_id(session_id.0.clone())
            .with_active_surface(InteractionSurface::Cli)
            .with_external_memory_entries(vec!["linear:ABC-1".into(), "slack:#ops".into()])
            .with_nested_memory_lineage(vec!["session:root".into(), "agent:child".into()]),
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
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "none".into(),
        },
        active_session_id: session_id.0.clone(),
        session_store: Some(store.clone()),
        session: Some(snapshot.clone()),
        history: Some(history.clone()),
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };

    assert_eq!(app_state.persist_current_session_state(), Ok(()));

    let store_b = FileBackedSessionStore::new(root.clone());
    let loaded = store_b
        .load(&SessionRestoreRequest {
            resume: Some("session-current-aggregate".into()),
            continue_session: false,
        })
        .expect("session record should exist");
    assert_eq!(loaded, (snapshot, history));
    assert_eq!(store_b.load_task_list(&session_id), Some(task_list));
    assert_eq!(
        store_b.load_external_memory_entries(&session_id),
        vec!["linear:ABC-1".to_string(), "slack:#ops".to_string()]
    );
    assert_eq!(
        store_b.load_nested_memory_lineage(&session_id),
        app_state.permission_context.nested_memory_lineage()
    );

    std::fs::remove_dir_all(root).expect("cleanup current aggregate test store");
}

#[test]
fn persist_resolved_session_state_commits_all_fields_in_one_record_write() {
    let root = unique_temp_path("rust-agent-persist-resolved-state");
    let store = Arc::new(FileBackedSessionStore::new(root.clone()));
    let session_id = SessionId("session-resolved-aggregate".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/resolved-aggregate".into(),
        last_turn_at: Some("2026-04-22T11:00:00Z".into()),
        prompt_seed: Some("seed-resolved".into()),
    };
    let history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::user("resume me"),
            timestamp: Some("2026-04-22T11:00:01Z".into()),
            tool_refs: vec![],
            milestone: None,
        }],
    };
    let task_list = TaskListSnapshot {
        next_id: 3,
        tasks: vec![TaskListItem {
            id: "task-1".into(),
            subject: "restored task".into(),
            description: "kept across resolved aggregate write".into(),
            active_form: Some("Restoring".into()),
            status: TaskListStatus::Pending,
            owner: Some(session_id.0.clone()),
            plan_step_id: None,
            blocks: vec![],
            blocked_by: vec![],
        }],
    };
    store.save_task_list(&session_id, task_list.clone());

    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_active_session_id(session_id.0.clone())
            .with_active_surface(InteractionSurface::Remote)
            .with_external_memory_entries(vec!["linear:XYZ-9".into()])
            .with_nested_memory_lineage(vec!["session:resolved".into()]),
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
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "none".into(),
        },
        active_session_id: session_id.0.clone(),
        session_store: Some(store.clone()),
        session: Some(snapshot.clone()),
        history: Some(history.clone()),
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };

    let resolved = rust_agent::history::resume::ResolvedSessionState {
        snapshot: snapshot.clone(),
        history: history.clone(),
        restored_session: None,
        client_type: rust_agent::bootstrap::ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        external_memory_entries: vec!["linear:XYZ-9".into()],
        nested_memory_lineage: vec!["session:resolved".into()],
    };

    assert_eq!(app_state.persist_resolved_session_state(&resolved), Ok(()));

    let store_b = FileBackedSessionStore::new(root.clone());
    let loaded = store_b
        .load(&SessionRestoreRequest {
            resume: Some("session-resolved-aggregate".into()),
            continue_session: false,
        })
        .expect("resolved session record should exist");
    assert_eq!(loaded, (snapshot, history));
    assert_eq!(store_b.load_task_list(&session_id), Some(task_list));
    assert_eq!(
        store_b.load_external_memory_entries(&session_id),
        vec!["linear:XYZ-9".to_string()]
    );
    assert_eq!(
        store_b.load_nested_memory_lineage(&session_id),
        vec!["session:resolved".to_string()]
    );
    assert_eq!(
        store_b.load_lifecycle_status(&session_id),
        SessionLifecycleStatus::Active
    );

    std::fs::remove_dir_all(root).expect("cleanup resolved aggregate test store");
}

#[test]
fn save_lifecycle_status_preserves_existing_snapshot_and_history() {
    let root = unique_temp_path("rust-agent-session-lifecycle-preserve");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-lifecycle-preserve".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/lifecycle-preserve".into(),
        last_turn_at: Some("2026-04-22T12:00:00Z".into()),
        prompt_seed: Some("seed-lifecycle".into()),
    };
    let history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::assistant("keep me"),
            timestamp: Some("2026-04-22T12:00:01Z".into()),
            tool_refs: vec![],
            milestone: None,
        }],
    };
    store.save(snapshot.clone(), history.clone());
    store.save_lifecycle_status(&session_id, SessionLifecycleStatus::Hibernating);

    let loaded = store
        .load(&SessionRestoreRequest {
            resume: Some("session-lifecycle-preserve".into()),
            continue_session: false,
        })
        .expect("session should still load after lifecycle update");
    assert_eq!(loaded, (snapshot, history));
    assert_eq!(
        store.load_lifecycle_status(&session_id),
        SessionLifecycleStatus::Hibernating
    );

    std::fs::remove_dir_all(root).expect("cleanup lifecycle preserve test store");
}

#[test]
fn update_record_preserves_existing_fields_when_mutating_one_section() {
    let root = unique_temp_path("rust-agent-session-update-preserve");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-update-preserve".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/update-preserve".into(),
        last_turn_at: Some("2026-04-22T13:00:00Z".into()),
        prompt_seed: Some("seed-update".into()),
    };
    let history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::user("preserve everything else"),
            timestamp: Some("2026-04-22T13:00:01Z".into()),
            tool_refs: vec!["TaskGet".into()],
            milestone: None,
        }],
    };
    let task_list = TaskListSnapshot {
        next_id: 2,
        tasks: vec![TaskListItem {
            id: "task-0".into(),
            subject: "persist me".into(),
            description: "must survive section update".into(),
            active_form: Some("Persisting".into()),
            status: TaskListStatus::Pending,
            owner: Some(session_id.0.clone()),
            plan_step_id: None,
            blocks: vec![],
            blocked_by: vec![],
        }],
    };
    store.save_full_record(
        &session_id,
        PersistedSessionRecord {
            snapshot: snapshot.clone(),
            history: history.clone(),
            task_list: Some(task_list.clone()),
            plan_state: Some(rust_agent::plan::types::PlanState::default()),
            external_memory_entries: Some(vec!["linear:KEEP-1".into()]),
            nested_memory_lineage: Some(vec!["session:update-preserve".into()]),
            lifecycle_status: SessionLifecycleStatus::Active,
        },
    );

    store.save_external_memory_entries(&session_id, vec!["linear:KEEP-2".into()]);

    let loaded = store
        .load(&SessionRestoreRequest {
            resume: Some("session-update-preserve".into()),
            continue_session: false,
        })
        .expect("session should still load after partial update");
    assert_eq!(loaded, (snapshot, history));
    assert_eq!(store.load_task_list(&session_id), Some(task_list));
    assert_eq!(
        store.load_external_memory_entries(&session_id),
        vec!["linear:KEEP-2".to_string()]
    );
    assert_eq!(
        store.load_nested_memory_lineage(&session_id),
        vec!["session:update-preserve".to_string()]
    );
    assert_eq!(
        store.load_lifecycle_status(&session_id),
        SessionLifecycleStatus::Active
    );

    std::fs::remove_dir_all(root).expect("cleanup update preserve test store");
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        active_model_runtime: Some(bundle.active_model_runtime.clone()),
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: resolved.active_session_id(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(resolved.snapshot.clone()),
        history: Some(resolved.history.clone()),
        restored_session: resolved.restored_session.clone(),
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
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

// ── Startup warnings ──────────────────────────────────────────────────────────

#[test]
fn startup_warning_provider_base_url_localhost_fires_when_url_is_default() {
    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "http://localhost",
        &[],
        std::path::Path::new("/some/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        warnings.has(|w| matches!(w, StartupWarning::ProviderBaseUrlIsLocalhost)),
        "expected ProviderBaseUrlIsLocalhost warning"
    );
}

#[test]
fn startup_warning_provider_base_url_localhost_does_not_fire_for_real_url() {
    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/some/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        !warnings.has(|w| matches!(w, StartupWarning::ProviderBaseUrlIsLocalhost)),
        "should not warn for real provider URL"
    );
}

#[test]
fn startup_warning_mcp_config_parse_failure_fires_when_diagnostics_present() {
    let diags = vec!["server 'bad-server': missing required field 'command'".into()];
    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &diags,
        std::path::Path::new("/some/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        warnings.has(
            |w| matches!(w, StartupWarning::McpConfigParseFailure { count, .. } if *count == 1)
        ),
        "expected McpConfigParseFailure warning"
    );
}

#[test]
fn startup_warning_filesystem_policy_missing_fires_when_no_policy() {
    let _guard = bootstrap_env_lock()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    remove_env_var("RUST_AGENT_CONFIG_ROOT");
    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/some/.claude"),
        true, // filesystem_policy_missing = true
        "anthropic",
        false,
    );
    assert!(
        warnings.has(|w| matches!(w, StartupWarning::FilesystemPolicyMissing)),
        "expected FilesystemPolicyMissing warning"
    );
}

#[test]
fn startup_warning_config_root_default_fires_when_env_var_unset() {
    let _guard = bootstrap_env_lock()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    remove_env_var("RUST_AGENT_CONFIG_ROOT");
    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/project/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        warnings.has(|w| matches!(w, StartupWarning::ConfigRootIsDefault { .. })),
        "expected ConfigRootIsDefault warning when RUST_AGENT_CONFIG_ROOT is unset"
    );
}

#[test]
fn startup_warning_config_root_default_does_not_fire_when_env_var_set() {
    let _guard = bootstrap_env_lock()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    set_env_var("RUST_AGENT_CONFIG_ROOT", "/custom/config");
    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/custom/config"),
        false,
        "anthropic",
        false,
    );
    remove_env_var("RUST_AGENT_CONFIG_ROOT");
    assert!(
        !warnings.has(|w| matches!(w, StartupWarning::ConfigRootIsDefault { .. })),
        "should not warn when RUST_AGENT_CONFIG_ROOT is explicitly set"
    );
}

#[tokio::test]
async fn startup_warnings_are_present_in_initialize_runtime_bundle() {
    // initialize_runtime with http://localhost base_url and no filesystem policy
    // should produce at least ProviderBaseUrlIsLocalhost and FilesystemPolicyMissing.
    let _guard = bootstrap_env_lock()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    remove_env_var("RUST_AGENT_CONFIG_ROOT");

    let runtime = runtime_for_surface("cli", false, true);
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.current_cwd = std::env::temp_dir(); // temp dir has no .claude/ or filesystem-policy.json

    let bundle = runtime
        .initialize_runtime(
            &state,
            "session-warnings-test".into(),
            Arc::new(rust_agent::task::manager::TaskManager::default()),
            Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
            Arc::new(rust_agent::plan::manager::PlanManager::default()),
        )
        .expect("initialize_runtime should succeed despite warnings");

    assert!(
        bundle
            .startup_warnings
            .has(|w| matches!(w, StartupWarning::ProviderBaseUrlIsLocalhost)),
        "expected ProviderBaseUrlIsLocalhost in bundle warnings"
    );
    assert!(
        !bundle.startup_warnings.warnings.is_empty(),
        "bundle should have at least one startup warning"
    );
}

#[test]
fn migration_success_writes_readable_upgraded_session_record() {
    let root = unique_temp_path("rust-agent-migration-upgrade");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-upgrade-write".into());

    // Write a legacy record missing all three new fields.
    let legacy_json = r#"{
  "snapshot": {
    "session_id": "session-upgrade-write",
    "surface": "Cli",
    "session_mode": "Interactive",
    "cwd": "/tmp/upgrade-write",
    "last_turn_at": null,
    "prompt_seed": null
  },
  "history": { "entries": [] },
  "task_list": null,
  "plan_state": null
}"#;
    let path = root.join("session-upgrade-write.json");
    std::fs::write(&path, legacy_json).expect("write legacy record");

    // Load triggers the upgrade write-back.
    let loaded = store.load(&SessionRestoreRequest {
        resume: Some("session-upgrade-write".into()),
        continue_session: false,
    });
    assert!(loaded.is_some(), "legacy record must deserialize");

    // The file on disk must now contain the new fields.
    let upgraded_raw = std::fs::read_to_string(&path).expect("read upgraded file");
    assert!(
        upgraded_raw.contains("external_memory_entries"),
        "upgraded file must contain external_memory_entries"
    );
    assert!(
        upgraded_raw.contains("nested_memory_lineage"),
        "upgraded file must contain nested_memory_lineage"
    );
    assert!(
        upgraded_raw.contains("lifecycle_status"),
        "upgraded file must contain lifecycle_status"
    );

    // A second load must parse cleanly without triggering another write-back.
    let store2 = FileBackedSessionStore::new(root.clone());
    let loaded2 = store2.load(&SessionRestoreRequest {
        resume: Some("session-upgrade-write".into()),
        continue_session: false,
    });
    assert!(loaded2.is_some(), "upgraded record must round-trip");

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn legacy_record_upgrade_preserves_existing_sections() {
    let root = unique_temp_path("rust-agent-migration-preserve");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-upgrade-preserve".into());

    // Legacy record with a non-empty history entry and a non-null task_list.
    let legacy_json = r#"{
  "snapshot": {
    "session_id": "session-upgrade-preserve",
    "surface": "Cli",
    "session_mode": "Interactive",
    "cwd": "/tmp/upgrade-preserve",
    "last_turn_at": "2026-04-22T09:00:00Z",
    "prompt_seed": "seed-preserve"
  },
  "history": {
    "entries": [
      {
        "message": { "role": "Assistant", "content": "legacy entry" },
        "timestamp": "2026-04-22T09:00:01Z",
        "tool_refs": [],
        "milestone": null
      }
    ]
  },
  "task_list": {
    "next_id": 3,
    "tasks": [
      {
        "id": "task-legacy-0",
        "subject": "legacy task",
        "description": "must survive upgrade",
        "active_form": null,
        "status": "Pending",
        "owner": null,
        "plan_step_id": null,
        "blocks": [],
        "blocked_by": []
      }
    ]
  },
  "plan_state": null
}"#;
    let path = root.join("session-upgrade-preserve.json");
    std::fs::write(&path, legacy_json).expect("write legacy record");

    let loaded = store.load(&SessionRestoreRequest {
        resume: Some("session-upgrade-preserve".into()),
        continue_session: false,
    });
    let (snapshot, history) = loaded.expect("legacy record must load");

    assert_eq!(snapshot.session_id, session_id);
    assert_eq!(snapshot.cwd, "/tmp/upgrade-preserve");
    assert_eq!(snapshot.prompt_seed.as_deref(), Some("seed-preserve"));
    assert_eq!(history.entries.len(), 1);

    // task_list must survive the upgrade write-back.
    let task_list = store.load_task_list(&session_id);
    assert!(task_list.is_some(), "task_list must survive upgrade");
    let tasks = task_list.unwrap();
    assert_eq!(tasks.next_id, 3);
    assert_eq!(tasks.tasks.len(), 1);
    assert_eq!(tasks.tasks[0].id, "task-legacy-0");

    // Upgraded file must still contain the original content.
    let upgraded_raw = std::fs::read_to_string(&path).expect("read upgraded file");
    assert!(
        upgraded_raw.contains("legacy entry"),
        "upgraded file must preserve original history content"
    );
    assert!(
        upgraded_raw.contains("seed-preserve"),
        "upgraded file must preserve original snapshot fields"
    );
    assert!(
        upgraded_raw.contains("task-legacy-0"),
        "upgraded file must preserve original task_list"
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn resume_fallback_does_not_corrupt_old_session_file() {
    let root = unique_temp_path("rust-agent-corrupt-no-overwrite");
    std::fs::create_dir_all(&root).expect("create root");

    // Write a corrupt (unparseable) JSON file.
    let corrupt_json = b"{ this is not valid json !!!";
    let path = root.join("session-corrupt.json");
    std::fs::write(&path, corrupt_json).expect("write corrupt file");

    let store = FileBackedSessionStore::new(root.clone());

    // load() must return None — no panic, no overwrite.
    let loaded = store.load(&SessionRestoreRequest {
        resume: Some("session-corrupt".into()),
        continue_session: false,
    });
    assert!(loaded.is_none(), "corrupt record must not deserialize");

    // The file on disk must be byte-for-byte identical to what we wrote.
    let after = std::fs::read(&path).expect("read file after failed load");
    assert_eq!(
        after, corrupt_json,
        "corrupt session file must not be modified by a failed load"
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn legacy_upgrade_does_not_change_latest_session() {
    let root = unique_temp_path("rust-agent-upgrade-no-latest");
    std::fs::create_dir_all(&root).expect("create root");

    // Seed latest_session pointing to a different session.
    let latest_path = root.join("latest_session");
    std::fs::write(&latest_path, "session-other").expect("write latest_session");

    // Write a legacy record for a different session.
    let legacy_json = r#"{
  "snapshot": {
    "session_id": "session-legacy-notouch",
    "surface": "Cli",
    "session_mode": "Interactive",
    "cwd": "/tmp/notouch",
    "last_turn_at": null,
    "prompt_seed": null
  },
  "history": { "entries": [] },
  "task_list": null,
  "plan_state": null
}"#;
    let session_path = root.join("session-legacy-notouch.json");
    std::fs::write(&session_path, legacy_json).expect("write legacy record");

    let store = FileBackedSessionStore::new(root.clone());
    let loaded = store.load(&SessionRestoreRequest {
        resume: Some("session-legacy-notouch".into()),
        continue_session: false,
    });
    assert!(loaded.is_some(), "legacy record must load");

    // latest_session must still point to the original value — upgrade must not overwrite it.
    let latest_after = std::fs::read_to_string(&latest_path).expect("read latest_session");
    assert_eq!(
        latest_after, "session-other",
        "legacy schema upgrade must not update latest_session"
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

// ── T18.1.B: advisory lock tests ─────────────────────────────────────────────

#[test]
fn atomic_write_uses_file_lock_for_session_record() {
    // Verify that writing a session record creates a sibling .lock file.
    let root = unique_temp_path("rust-agent-lock-session-record");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-lock-record".into());
    let record = minimal_persisted_record(&session_id);

    store
        .save_full_record(&session_id, record)
        .expect("save_full_record must succeed");

    let lock_path = root.join("session-lock-record.json.lock");
    assert!(
        lock_path.exists(),
        "advisory lock sentinel file must exist after write: {:?}",
        lock_path
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn atomic_write_uses_file_lock_for_latest_session() {
    // Verify that writing latest_session creates a sibling .lock file.
    let root = unique_temp_path("rust-agent-lock-latest");
    let store = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-lock-latest".into());
    let record = minimal_persisted_record(&session_id);

    store
        .save_full_record(&session_id, record)
        .expect("save_full_record must succeed");

    let lock_path = root.join("latest_session.lock");
    assert!(
        lock_path.exists(),
        "advisory lock sentinel file must exist for latest_session: {:?}",
        lock_path
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn concurrent_session_writes_do_not_leave_partial_json() {
    use std::sync::Arc;
    use std::thread;

    let root = Arc::new(unique_temp_path("rust-agent-concurrent-writes"));
    let session_id = SessionId("session-concurrent".into());

    // Spawn 8 threads, each writing a distinct record to the same session file.
    let handles: Vec<_> = (0u32..8)
        .map(|i| {
            let root = Arc::clone(&root);
            let session_id = session_id.clone();
            thread::spawn(move || {
                let store = FileBackedSessionStore::new((*root).clone());
                let mut record = minimal_persisted_record(&session_id);
                record.snapshot.cwd = format!("/tmp/concurrent/{i}");
                store
                    .save_full_record(&session_id, record)
                    .expect("concurrent write must not fail");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread must not panic");
    }

    // The file on disk must be valid JSON — no partial writes.
    let path = root.join("session-concurrent.json");
    let raw = std::fs::read_to_string(&path).expect("session file must exist");
    let parsed: serde_json::Value = serde_json::from_str(&raw)
        .expect("session file must be valid JSON after concurrent writes");
    assert!(
        parsed.get("snapshot").is_some(),
        "parsed record must have a snapshot field"
    );

    std::fs::remove_dir_all((*root).clone()).expect("cleanup");
}

fn minimal_persisted_record(session_id: &SessionId) -> PersistedSessionRecord {
    use rust_agent::bootstrap::{InteractionSurface, SessionMode};
    use rust_agent::history::session::{SessionLifecycleStatus, SessionSnapshot};

    PersistedSessionRecord {
        snapshot: SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/minimal".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        history: Default::default(),
        task_list: None,
        plan_state: None,
        external_memory_entries: None,
        nested_memory_lineage: None,
        lifecycle_status: SessionLifecycleStatus::Active,
    }
}

// ── T18.1.D: teammate registry tests ─────────────────────────────────────────

#[test]
fn teammate_registry_loader_returns_none_when_missing() {
    let root = unique_temp_path("rust-agent-missing-teammate-registry");
    std::fs::create_dir_all(&root).expect("create temp root");

    let registry = load_teammate_registry_from_root(&root).expect("loader should succeed");
    assert!(registry.is_none(), "missing registry should return None");

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn teammate_registry_parser_accepts_valid_registry() {
    let registry = parse_teammate_registry(
        r#"{
  "profiles": [
    {
      "id": "impl-1",
      "name": "Implementer",
      "description": "Builds code changes",
      "role": "implement",
      "default_model_profile": "openai-fast",
      "allowed_tools": ["Read", "Edit"],
      "max_turns": 4
    }
  ]
}"#,
    )
    .expect("valid registry should parse");

    assert_eq!(registry.profiles.len(), 1);
    assert_eq!(registry.profiles[0].id, "impl-1");
    assert_eq!(registry.profiles[0].allowed_tools, vec!["Read", "Edit"]);
    assert_eq!(registry.profiles[0].max_turns, 4);
}

#[test]
fn teammate_registry_rejects_duplicate_id() {
    let error = parse_teammate_registry(
        r#"{
  "profiles": [
    {
      "id": "dup",
      "name": "One",
      "description": "First",
      "role": "implement",
      "default_model_profile": null,
      "allowed_tools": [],
      "max_turns": 1
    },
    {
      "id": "dup",
      "name": "Two",
      "description": "Second",
      "role": "verify",
      "default_model_profile": null,
      "allowed_tools": [],
      "max_turns": 1
    }
  ]
}"#,
    )
    .expect_err("duplicate id should fail");

    assert!(error.to_string().contains("duplicate teammate id 'dup'"));
}

#[test]
fn teammate_registry_rejects_empty_required_field() {
    let error = parse_teammate_registry(
        r#"{
  "profiles": [
    {
      "id": "impl-1",
      "name": "",
      "description": "Builds code changes",
      "role": "implement",
      "default_model_profile": null,
      "allowed_tools": [],
      "max_turns": 1
    }
  ]
}"#,
    )
    .expect_err("empty required field should fail");

    assert!(error.to_string().contains("teammate 'impl-1' missing name"));
}

#[test]
fn teammate_registry_rejects_zero_max_turns() {
    let error = parse_teammate_registry(
        r#"{
  "profiles": [
    {
      "id": "impl-1",
      "name": "Implementer",
      "description": "Builds code changes",
      "role": "implement",
      "default_model_profile": null,
      "allowed_tools": [],
      "max_turns": 0
    }
  ]
}"#,
    )
    .expect_err("zero max_turns should fail");

    assert!(error.to_string().contains("max_turns must be > 0"));
}

#[test]
fn teammate_registry_rejects_unknown_field() {
    let error = parse_teammate_registry(
        r#"{
  "profiles": [
    {
      "id": "impl-1",
      "name": "Implementer",
      "description": "Builds code changes",
      "role": "implement",
      "default_model_profile": null,
      "allowed_tools": [],
      "max_turns": 1,
      "extra": true
    }
  ]
}"#,
    )
    .expect_err("unknown field should fail");

    assert!(error.to_string().contains("invalid agents.json"));
    assert!(error.to_string().contains("unknown field"));
}

// ── T19.3.B proxy config tests ────────────────────────────────────────────────

#[test]
fn proxy_env_var_is_read_into_config() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "https://api.anthropic.com");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    apply_proxy_env_scenario(ProxyEnvScenario::NoProxyEnv);
    set_env_var("RUST_AGENT_PROXY_URL", "http://proxy.corp.example:3128");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let config = runtime
        .build_model_provider_config_from_env_for_test()
        .expect("config should build");
    assert_eq!(
        config.proxy_url.as_deref(),
        Some("http://proxy.corp.example:3128")
    );
}

#[test]
fn no_proxy_env_var_is_read_into_config() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "https://api.anthropic.com");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    apply_proxy_env_scenario(ProxyEnvScenario::NoProxyEnv);
    set_env_var("RUST_AGENT_NO_PROXY", "localhost,127.0.0.1");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let config = runtime
        .build_model_provider_config_from_env_for_test()
        .expect("config should build");
    assert_eq!(config.no_proxy, None);
}

#[test]
fn ca_bundle_env_var_is_read_into_config() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "https://api.anthropic.com");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");
    set_env_var("RUST_AGENT_CA_BUNDLE", "/etc/ssl/certs/corp-ca.pem");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let config = runtime
        .build_model_provider_config_from_env_for_test()
        .expect("config should build");
    assert_eq!(
        config.ca_bundle_path.as_deref(),
        Some("/etc/ssl/certs/corp-ca.pem")
    );
}

#[test]
fn no_proxy_env_unset_leaves_field_none() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROVIDER_BASE_URL", "https://api.anthropic.com");
    set_env_var("RUST_AGENT_PROVIDER_API_KEY", "test-key");

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let config = runtime
        .build_model_provider_config_from_env_for_test()
        .expect("config should build");
    assert!(config.proxy_url.is_none());
    assert!(config.no_proxy.is_none());
    assert!(config.ca_bundle_path.is_none());
}

#[test]
fn startup_warning_invalid_proxy_url_fires_for_bad_url() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::NoProxyEnv);
    set_env_var("RUST_AGENT_PROXY_URL", "not-a-valid-proxy-url");

    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/some/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        warnings.has(|w| matches!(w, StartupWarning::InvalidProxyUrl { .. })),
        "expected InvalidProxyUrl warning for malformed proxy URL"
    );
}

#[test]
fn startup_warning_invalid_proxy_url_does_not_fire_for_valid_url() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::NoProxyEnv);
    set_env_var("RUST_AGENT_PROXY_URL", "http://proxy.corp.example:3128");

    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/some/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        !warnings.has(|w| matches!(w, StartupWarning::InvalidProxyUrl { .. })),
        "should not warn for valid proxy URL"
    );
}

#[test]
fn startup_warning_invalid_proxy_url_does_not_fire_when_unset() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::NoProxyEnv);

    let warnings = rust_agent::bootstrap::warnings::collect_startup_warnings(
        "https://api.anthropic.com",
        &[],
        std::path::Path::new("/some/.claude"),
        false,
        "anthropic",
        false,
    );
    assert!(
        !warnings.has(|w| matches!(w, StartupWarning::InvalidProxyUrl { .. })),
        "should not warn when RUST_AGENT_PROXY_URL is unset"
    );
}

#[test]
fn redact_proxy_url_strips_password() {
    let redacted =
        rust_agent::service::api::client::redact_proxy_url("http://user:secret@proxy.corp:3128");
    assert!(
        !redacted.contains("secret"),
        "password should be redacted: {redacted}"
    );
    assert!(
        redacted.contains("user"),
        "username should be preserved: {redacted}"
    );
    assert!(
        redacted.contains("***"),
        "redacted marker should be present: {redacted}"
    );
}

#[test]
fn redact_proxy_url_no_op_when_no_userinfo() {
    let url = "http://proxy.corp.example:3128";
    let redacted = rust_agent::service::api::client::redact_proxy_url(url);
    assert_eq!(redacted.trim_end_matches('/'), url.trim_end_matches('/'));
}

#[test]
fn redact_proxy_url_no_op_for_invalid_url() {
    let url = "not-a-url";
    let redacted = rust_agent::service::api::client::redact_proxy_url(url);
    assert_eq!(redacted, url);
}

#[test]
fn startup_warning_invalid_proxy_url_message_redacts_userinfo() {
    let warning = StartupWarning::InvalidProxyUrl {
        redacted_url: "http://user:***@proxy.corp:3128".into(),
    };
    let msg = warning.message();
    assert!(
        !msg.contains("secret"),
        "warning message must not contain raw password"
    );
    assert!(
        msg.contains("***"),
        "warning message should contain redacted marker"
    );
}

#[test]
fn rust_agent_proxy_env_overrides_system_env() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::DualLayerProxyEnv);

    let resolution = rust_agent::bootstrap::proxy_env::resolve_proxy_env_contract();
    assert_eq!(resolution.source, rust_agent::bootstrap::proxy_env::ProxySource::RustAgentEnv);
    assert_eq!(resolution.proxy_url.as_deref(), Some("http://rust-agent-proxy:3128"));
    assert_eq!(resolution.no_proxy.as_deref(), Some("rust-agent.local"));
}

#[test]
fn https_proxy_falls_back_when_rust_agent_proxy_unset() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::SystemProxyOnly);

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let config = runtime
        .build_model_provider_config_from_env_for_test()
        .expect("config should build");
    assert_eq!(config.proxy_url.as_deref(), Some("http://system-https-proxy:8443"));
    assert_eq!(config.no_proxy.as_deref(), Some("example.local"));
}

#[test]
fn http_proxy_used_when_https_proxy_missing() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::NoProxyEnv);
    set_env_var("HTTP_PROXY", "http://system-http-proxy:8080");

    let resolution = rust_agent::bootstrap::proxy_env::resolve_proxy_env_contract();
    assert_eq!(resolution.source, rust_agent::bootstrap::proxy_env::ProxySource::SystemEnv);
    assert_eq!(resolution.proxy_url.as_deref(), Some("http://system-http-proxy:8080"));
    assert!(resolution.no_proxy.is_none());
}

#[test]
fn webfetch_uses_same_proxy_resolution_contract() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    apply_proxy_env_scenario(ProxyEnvScenario::SystemProxyOnly);

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        attachments: Vec::new(),
        surface: "cli".into(),
        lism_ab_sample: None,
    lism_ab_summarize: None,
    lism_policy: None,
    });
    let config = runtime
        .build_model_provider_config_from_env_for_test()
        .expect("config should build");
    let (webfetch_proxy, webfetch_no_proxy) =
        rust_agent::tool::builtin::web_fetch::resolved_web_fetch_proxy_for_test();

    assert_eq!(config.proxy_url, webfetch_proxy);
    assert_eq!(config.no_proxy, webfetch_no_proxy);
}

#[test]
fn no_proxy_resolution_tracks_selected_proxy_source() {
    let _env_lock = bootstrap_env_lock().lock().expect("bootstrap env lock");
    let _guard = BootstrapEnvGuard::new();
    set_env_var("RUST_AGENT_PROXY_URL", "http://rust-agent-proxy:3128");
    set_env_var("RUST_AGENT_NO_PROXY", "rust-agent.local");
    set_env_var("NO_PROXY", "system.local");

    let resolution = rust_agent::bootstrap::proxy_env::resolve_proxy_env_contract();
    assert_eq!(resolution.source, rust_agent::bootstrap::proxy_env::ProxySource::RustAgentEnv);
    assert_eq!(resolution.no_proxy.as_deref(), Some("rust-agent.local"));
}

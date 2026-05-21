use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use rust_agent::bootstrap::{
    BootstrapCli, BootstrapState, ClientType, InteractionSurface, RuntimeBootstrap, SessionMode,
    SessionSource,
};
use rust_agent::command::builtin::permissions::PermissionsCommand;
use rust_agent::command::types::{Command, CommandResult};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::resume::{FreshSessionRequest, build_fresh_session_state};
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::approval_protocol::ApprovalResponse;
use rust_agent::security::audit::AuditLog;
use rust_agent::security::workspace_capability::{
    WORKSPACE_PERMISSIONS_FILENAME, WorkspacePermissionConfig, WorkspacePermissionLevel,
    default_workspace_permissions_path,
};
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout,
};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::state::active_model_runtime::ActiveModelRuntime;
use rust_agent::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
};
use rust_agent::state::permission_context::{
    PendingApproval, PermissionMode, ToolPermissionContext,
};
use rust_agent::task::list_manager::TaskListManager;
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::{file_edit::FileEditTool, file_write::FileWriteTool};
use rust_agent::tool::definition::{PermissionDecision, Tool, ToolCall, ToolResult};
use rust_agent::tool::registry::ToolRegistry;
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn set_env(key: &str, value: &std::ffi::OsStr) {
    // SAFETY: integration tests serialize environment mutation with a global mutex.
    unsafe { std::env::set_var(key, value) }
}

fn remove_env(key: &str) {
    // SAFETY: integration tests serialize environment mutation with a global mutex.
    unsafe { std::env::remove_var(key) }
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
        max_tokens_param: None,
        prompt_cache_key: None,
        prompt_cache_retention: None,
    }
}

struct EnvSnapshot {
    home: Option<std::ffi::OsString>,
    config_root: Option<std::ffi::OsString>,
    workspace_capability: Option<std::ffi::OsString>,
    beta_deny: Option<std::ffi::OsString>,
    cwd: PathBuf,
}

impl EnvSnapshot {
    fn capture() -> Self {
        Self {
            home: std::env::var_os("HOME"),
            config_root: std::env::var_os("RUST_AGENT_CONFIG_ROOT"),
            workspace_capability: std::env::var_os("RUST_AGENT_WORKSPACE_CAPABILITY_CONFIG"),
            beta_deny: std::env::var_os("RUST_AGENT_BETA_DENY_BY_DEFAULT"),
            cwd: std::env::current_dir().expect("read cwd"),
        }
    }

    fn restore(self) {
        restore_env("HOME", self.home);
        restore_env("RUST_AGENT_CONFIG_ROOT", self.config_root);
        restore_env(
            "RUST_AGENT_WORKSPACE_CAPABILITY_CONFIG",
            self.workspace_capability,
        );
        restore_env("RUST_AGENT_BETA_DENY_BY_DEFAULT", self.beta_deny);
        std::env::set_current_dir(self.cwd).expect("restore cwd");
    }
}

fn restore_env(key: &str, value: Option<std::ffi::OsString>) {
    match value {
        Some(value) => set_env(key, &value),
        None => remove_env(key),
    }
}

struct LockedEnv<'a> {
    _guard: MutexGuard<'a, ()>,
    snapshot: Option<EnvSnapshot>,
}

impl LockedEnv<'_> {
    fn acquire() -> Self {
        Self {
            _guard: env_lock().lock().expect("env lock poisoned"),
            snapshot: Some(EnvSnapshot::capture()),
        }
    }
}

impl Drop for LockedEnv<'_> {
    fn drop(&mut self) {
        if let Some(snapshot) = self.snapshot.take() {
            snapshot.restore();
        }
    }
}

struct TestWorld {
    _root: TempDir,
    home: PathBuf,
    config_root: PathBuf,
    workspace: PathBuf,
    outside: PathBuf,
}

impl TestWorld {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create temp root");
        let home = root.path().join("home");
        let config_root = root.path().join("config");
        let workspace = root.path().join("workspace");
        let outside = root.path().join("outside");
        fs::create_dir_all(&home).expect("create home");
        fs::create_dir_all(&config_root).expect("create config root");
        fs::create_dir_all(&workspace).expect("create workspace");
        fs::create_dir_all(&outside).expect("create outside");
        fs::write(workspace.join("note.txt"), "hello").expect("seed note");
        fs::write(workspace.join("haystack.txt"), "needle\n").expect("seed grep target");
        Self {
            _root: root,
            home,
            config_root,
            workspace,
            outside,
        }
    }

    fn apply_env(&self) {
        set_env("HOME", self.home.as_os_str());
        set_env("RUST_AGENT_CONFIG_ROOT", self.config_root.as_os_str());
        remove_env("RUST_AGENT_WORKSPACE_CAPABILITY_CONFIG");
        remove_env("RUST_AGENT_BETA_DENY_BY_DEFAULT");
        std::env::set_current_dir(&self.workspace).expect("enter workspace");
    }

    fn global_permissions_path(&self) -> PathBuf {
        self.home
            .join(".morgo")
            .join(WORKSPACE_PERMISSIONS_FILENAME)
    }

    fn write_global_permissions(&self, workspace: &Path, permission: WorkspacePermissionLevel) {
        let mut config = WorkspacePermissionConfig::default();
        config.trust_workspace(workspace, permission);
        config
            .save_to_path(&self.global_permissions_path())
            .expect("write workspace permissions");
    }

    fn write_legacy_capability(&self, json: &str) {
        fs::write(self.config_root.join("workspace-capability.json"), json)
            .expect("write legacy workspace capability");
    }
}

fn bootstrap() -> RuntimeBootstrap {
    RuntimeBootstrap::from_cli(BootstrapCli::default())
        .with_provider_config(test_model_provider_config())
}

fn bootstrap_state(workspace: &Path, mode: SessionMode) -> BootstrapState {
    let mut state = BootstrapState::new(InteractionSurface::Cli, mode, false);
    state.current_cwd = workspace.to_path_buf();
    state.original_cwd = workspace.to_path_buf();
    state
}

fn runtime_app_state(world: &TestWorld, mode: SessionMode) -> AppState {
    let runtime_bootstrap = bootstrap();
    let state = bootstrap_state(&world.workspace, mode);
    let session_state = build_fresh_session_state(FreshSessionRequest {
        parent_session_id: None,
        surface: InteractionSurface::Cli,
        session_mode: mode,
        cwd: world.workspace.display().to_string(),
    });
    let active_session_id = session_state.active_session_id();
    let bundle = runtime_bootstrap
        .initialize_runtime(
            &state,
            active_session_id.clone(),
            Arc::new(TaskManager::default()),
            Arc::new(TaskListManager::default()),
            Arc::new(PlanManager::default()),
        )
        .expect("runtime should initialize");
    let prompts = rust_agent::bootstrap::PromptAugmentation {
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
        metadata: rust_agent::bootstrap::PromptAugmentationMetadata {
            active_session_id: active_session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: mode,
            visible_tool_count: bundle.coordinator_tools.all_metadata().len(),
        },
    };
    runtime_bootstrap
        .finalize_runtime_state(&state, session_state, bundle, prompts, active_session_id)
        .app_state
}

fn build_registry(world: &TestWorld) -> ToolRegistry {
    bootstrap()
        .initialize_runtime(
            &bootstrap_state(&world.workspace, SessionMode::Headless),
            "registry-session".into(),
            Arc::new(TaskManager::default()),
            Arc::new(TaskListManager::default()),
            Arc::new(PlanManager::default()),
        )
        .expect("runtime should initialize")
        .coordinator_tools
}

fn build_app_state(
    world: &TestWorld,
    registry: ToolRegistry,
    permission_context: ToolPermissionContext,
) -> AppState {
    let session_state = build_fresh_session_state(FreshSessionRequest {
        parent_session_id: None,
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        cwd: world.workspace.display().to_string(),
    });
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context
            .with_active_session_id(session_state.active_session_id())
            .with_active_surface(InteractionSurface::Cli),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(registry))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(Mutex::new(AuditLog::default())),
        startup_trace: Vec::new(),
        active_model_runtime: None::<ActiveModelRuntime>,
        active_model_profile_name: None,
        active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: ActiveModelProviderSummary {
            provider_id: "test".into(),
            protocol: "test".into(),
            compatibility_profile: "test".into(),
            base_url_host: "localhost".into(),
            model: "test".into(),
            auth_status: "none".into(),
        },
        active_session_id: session_state.active_session_id(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(session_state.snapshot),
        history: Some(session_state.history),
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    }
}

fn trust_config(
    path: &Path,
    permission: WorkspacePermissionLevel,
) -> Arc<WorkspacePermissionConfig> {
    let mut config = WorkspacePermissionConfig::default();
    config.trust_workspace(path, permission);
    Arc::new(config)
}

fn pending_from_tool_result(result: ToolResult, tool_input: String) -> PendingApproval {
    match result {
        ToolResult::PendingApproval {
            tool_name,
            approval,
            message,
        } => PendingApproval {
            tool_name,
            tool_input,
            message,
            code: approval.code,
            summary: Some(approval.summary),
            detail: approval.detail,
            approval_kind: approval.approval_kind,
            escalation_reasons: approval.escalation_reasons,
        },
        other => panic!("expected pending approval, got {other:?}"),
    }
}

#[test]
fn workspace_permission_bootstrap_loads_global_legacy_and_precedence() {
    let _env = LockedEnv::acquire();
    let world = TestWorld::new();
    world.apply_env();

    world.write_global_permissions(&world.workspace, WorkspacePermissionLevel::Edit);
    let app_state = runtime_app_state(&world, SessionMode::Headless);
    let loaded = app_state
        .permission_context
        .workspace_permissions()
        .expect("workspace permissions loaded");
    assert_eq!(
        loaded
            .effective_permission(&world.workspace)
            .expect("workspace permission")
            .permission,
        WorkspacePermissionLevel::Edit
    );

    fs::remove_file(world.global_permissions_path()).expect("remove global permissions");
    world.write_legacy_capability(
        r#"{"global_max_tier":"read","scopes":[],"escalate_to_pending_approval":true,"audit_capability_decisions":false}"#,
    );
    let app_state = runtime_app_state(&world, SessionMode::Headless);
    let loaded = app_state
        .permission_context
        .workspace_permissions()
        .expect("legacy permissions loaded");
    assert_eq!(
        loaded
            .effective_permission(&world.workspace)
            .expect("legacy workspace permission")
            .permission,
        WorkspacePermissionLevel::View
    );
    assert!(
        !world.global_permissions_path().exists(),
        "legacy compatibility load must not write new global config"
    );

    world.write_global_permissions(&world.workspace, WorkspacePermissionLevel::Worker);
    let app_state = runtime_app_state(&world, SessionMode::Headless);
    let loaded = app_state
        .permission_context
        .workspace_permissions()
        .expect("global permissions loaded");
    assert_eq!(
        loaded
            .effective_permission(&world.workspace)
            .expect("global permission")
            .permission,
        WorkspacePermissionLevel::Worker,
        "global workspace-permissions.json should take precedence over legacy capability"
    );
}

#[tokio::test]
async fn workspace_permission_tool_gates_return_pending_approval() {
    let _env = LockedEnv::acquire();
    let world = TestWorld::new();
    world.apply_env();
    let registry = build_registry(&world);

    let untrusted = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(Arc::new(WorkspacePermissionConfig::default()));
    let read_call = ToolCall::new(
        "Read",
        serde_json::json!({ "file_path": world.workspace.join("note.txt") }).to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&read_call, &untrusted)
            .await
            .expect("invoke read"),
        ToolResult::PendingApproval { tool_name, .. } if tool_name == "Read"
    ));

    let view = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(trust_config(
            &world.workspace,
            WorkspacePermissionLevel::View,
        ));
    assert!(matches!(
        registry
            .invoke(&read_call, &view)
            .await
            .expect("invoke read"),
        ToolResult::Text(text) if text.contains("hello")
    ));

    let glob_call = ToolCall::new(
        "Glob",
        serde_json::json!({ "pattern": "*.txt", "path": world.workspace }).to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&glob_call, &view)
            .await
            .expect("invoke glob"),
        ToolResult::Text(text) if text.contains("note.txt")
    ));

    let grep_call = ToolCall::new(
        "Grep",
        serde_json::json!({ "pattern": "needle", "path": world.workspace }).to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&grep_call, &view)
            .await
            .expect("invoke grep"),
        ToolResult::Text(text) if text.contains("haystack.txt")
    ));

    let write_call = ToolCall::new(
        "Write",
        serde_json::json!({ "file_path": world.workspace.join("created.txt"), "content": "x" })
            .to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&write_call, &view)
            .await
            .expect("invoke write"),
        ToolResult::PendingApproval { tool_name, .. } if tool_name == "Write"
    ));

    let edit_call = ToolCall::new(
        "Edit",
        serde_json::json!({
            "file_path": world.workspace.join("note.txt"),
            "old_string": "hello",
            "new_string": "hello edited"
        })
        .to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&edit_call, &view)
            .await
            .expect("invoke edit"),
        ToolResult::PendingApproval { tool_name, .. } if tool_name == "Edit"
    ));

    let edit = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(trust_config(
            &world.workspace,
            WorkspacePermissionLevel::Edit,
        ));
    assert!(matches!(
        FileWriteTool.check_permissions(&write_call, &edit).await,
        PermissionDecision::Allow
    ));
    assert!(matches!(
        FileEditTool.check_permissions(&edit_call, &edit).await,
        PermissionDecision::Allow
    ));
}

#[tokio::test]
async fn workspace_permission_bash_worker_and_admin_gates_are_real() {
    let _env = LockedEnv::acquire();
    let world = TestWorld::new();
    world.apply_env();
    let registry = build_registry(&world);

    let worker = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(trust_config(
            &world.workspace,
            WorkspacePermissionLevel::Worker,
        ));
    let admin = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(trust_config(
            &world.workspace,
            WorkspacePermissionLevel::Admin,
        ));

    let worker_call = ToolCall::new(
        "Bash",
        serde_json::json!({ "command": "printf ok" }).to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&worker_call, &worker)
            .await
            .expect("invoke worker bash"),
        ToolResult::Text(text) if text.contains("stdout:\nok")
    ));

    let pipe_call = ToolCall::new(
        "Bash",
        serde_json::json!({ "command": "printf ok | cat" }).to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&pipe_call, &worker)
            .await
            .expect("invoke worker pipe"),
        ToolResult::PendingApproval { tool_name, .. } if tool_name == "Bash"
    ));
    assert!(matches!(
        registry
            .invoke(&pipe_call, &admin)
            .await
            .expect("invoke admin pipe"),
        ToolResult::Text(text) if text.contains("stdout:\nok")
    ));

    let outside_call = ToolCall::new(
        "Bash",
        serde_json::json!({ "command": format!("cat {}", world.outside.join("missing.txt").display()) })
            .to_string(),
    );
    assert!(matches!(
        registry
            .invoke(&outside_call, &admin)
            .await
            .expect("invoke outside bash"),
        ToolResult::PendingApproval { tool_name, .. } if tool_name == "Bash"
    ));
}

#[tokio::test]
async fn workspace_permission_approval_responses_do_not_persist_global_trust() {
    let _env = LockedEnv::acquire();
    let world = TestWorld::new();
    world.apply_env();
    let registry = build_registry(&world);

    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(Arc::new(WorkspacePermissionConfig::default()));
    let app_state = build_app_state(&world, registry.clone(), permissions.clone());
    let marker = world.workspace.join("approval-marker.txt");
    let write_call = ToolCall::new(
        "Write",
        serde_json::json!({ "file_path": marker, "content": "approved" }).to_string(),
    );
    let pending = pending_from_tool_result(
        registry
            .invoke(&write_call, &app_state.permission_context)
            .await
            .expect("invoke pending write"),
        write_call.input.clone(),
    );
    app_state
        .permission_context
        .set_pending_approval(Some(pending));

    let result = app_state
        .resolve_pending_approval_response(ApprovalResponse::ApproveOnce)
        .await
        .expect("approve once");
    assert!(
        matches!(result, CommandResult::ContinueToQueryWithPrompt(text) if text.contains("wrote"))
    );
    assert!(marker.exists(), "approve once should run the pending write");
    assert!(
        app_state.permission_context.pending_approval().is_none(),
        "approve once should clear pending approval"
    );
    assert!(
        !world.global_permissions_path().exists(),
        "approve once should not persist global workspace trust"
    );

    let always_marker = world.workspace.join("always-marker.txt");
    let always_call = ToolCall::new(
        "Write",
        serde_json::json!({ "file_path": always_marker, "content": "always" }).to_string(),
    );
    let pending = pending_from_tool_result(
        registry
            .invoke(&always_call, &app_state.permission_context)
            .await
            .expect("invoke pending write"),
        always_call.input.clone(),
    );
    app_state
        .permission_context
        .set_pending_approval(Some(pending));
    let result = app_state
        .resolve_pending_approval_response(ApprovalResponse::ApproveAlways)
        .await
        .expect("approve always");
    assert!(
        matches!(result, CommandResult::ContinueToQueryWithPrompt(text) if text.contains("wrote"))
    );
    assert!(
        always_marker.exists(),
        "approve always should run pending write"
    );
    assert!(
        app_state
            .permission_context
            .always_allow_rules()
            .iter()
            .any(|rule| rule == "Write"),
        "approve always should add a session allow rule"
    );
    assert!(
        !world.global_permissions_path().exists(),
        "approve always should not persist global workspace trust"
    );

    let deny_marker = world.workspace.join("deny-marker.txt");
    let deny_call = ToolCall::new(
        "Write",
        serde_json::json!({ "file_path": deny_marker, "content": "denied" }).to_string(),
    );
    let deny_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_active_surface(InteractionSurface::Cli)
        .with_workspace_permissions(Arc::new(WorkspacePermissionConfig::default()));
    let deny_app_state = build_app_state(&world, registry.clone(), deny_permissions);
    let pending = pending_from_tool_result(
        registry
            .invoke(&deny_call, &deny_app_state.permission_context)
            .await
            .expect("invoke pending write"),
        deny_call.input.clone(),
    );
    deny_app_state
        .permission_context
        .set_pending_approval(Some(pending));
    let result = deny_app_state
        .resolve_pending_approval_response(ApprovalResponse::Deny)
        .await
        .expect("deny");
    assert!(matches!(result, CommandResult::Message(text) if text.contains("Denied approval")));
    assert!(
        !deny_marker.exists(),
        "deny should not run the pending write"
    );
    assert!(
        !world.global_permissions_path().exists(),
        "deny should not persist global workspace trust"
    );
}

#[tokio::test]
async fn permissions_trust_command_writes_global_schema_and_bootstrap_is_read_only() {
    let _env = LockedEnv::acquire();
    let world = TestWorld::new();
    world.apply_env();

    let app_state = runtime_app_state(&world, SessionMode::Headless);
    assert!(
        !world.global_permissions_path().exists(),
        "headless bootstrap should not create global workspace permissions"
    );

    let command = PermissionsCommand;
    let input = NormalizedInput::from_raw(
        InteractionSurface::Cli,
        format!("/permissions trust {} worker", world.workspace.display()),
    );
    let result = command
        .execute(&input, &app_state)
        .await
        .expect("trust command");
    assert!(matches!(result, CommandResult::Message(text) if text.contains("Trusted workspace")));
    let json =
        fs::read_to_string(world.global_permissions_path()).expect("read global permissions");
    let value: serde_json::Value = serde_json::from_str(&json).expect("parse permissions json");
    assert_eq!(value["version"], 1);
    assert_eq!(value["workspaces"].as_array().expect("workspaces").len(), 1);
    assert_eq!(value["workspaces"][0]["permission"], "worker");

    let before = json;
    let _ = runtime_app_state(&world, SessionMode::Headless);
    let after =
        fs::read_to_string(world.global_permissions_path()).expect("read permissions again");
    assert_eq!(
        before, after,
        "trusted workspace bootstrap should not rewrite the config"
    );
    assert_eq!(
        default_workspace_permissions_path().expect("default path"),
        world.global_permissions_path()
    );
}

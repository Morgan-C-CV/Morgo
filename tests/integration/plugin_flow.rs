use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{
    BootstrapCli, ClientType, InteractionSurface, RuntimeBootstrap, SessionMode, SessionSource,
};
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::builtin::plugins::{PluginSlashCommand, PluginsCommand};
use rust_agent::command::builtin::status::StatusCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::Command;
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plugins::loader::load_plugins;
use rust_agent::plugins::runtime::{
    augment_hook_registry_with_plugins, augment_tool_registry_with_plugins,
};
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout,
};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::definition::{ToolCall, ToolResult};
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn write_wasm_fixture(destination: &PathBuf, fixture_name: &str) {
    let bytes = wat::parse_file(fixture_path(fixture_name)).expect("fixture wat should parse");
    fs::write(destination, bytes).expect("fixture wasm should be written");
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

#[tokio::test]
async fn plugin_runtime_exposes_command_hook_tool_and_diagnostics() {
    let root = unique_temp_path("rust-agent-plugin-runtime");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands", "hooks", "tools"],
  "diagnostics": {
    "docs": "https://example.com/docs",
    "issues": "https://example.com/issues",
    "support_level": "community"
  },
  "commands": [
    {
      "name": "demo-plugin-cmd",
      "description": "Demo plugin command",
      "prompt": "Do plugin command work"
    }
  ],
  "tools": [
    {
      "name": "demo_tool",
      "description": "Demo plugin tool",
      "prompt": "Inspect plugin-owned files",
      "read_only": true,
      "search_hint": "plugin demo tool"
    }
  ],
  "hooks": [
    {
      "event": "stop",
      "append_message": "plugin stop hook fired"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let session_store = Arc::new(InMemorySessionStore::default());
    let previous_cwd = std::env::current_dir().expect("cwd should resolve");
    std::env::set_current_dir(&root).expect("should switch cwd to plugin root");

    let bootstrap = RuntimeBootstrap::from_cli(BootstrapCli {
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
    })
    .with_session_store(session_store)
    .with_provider_config(test_model_provider_config());

    bootstrap.run().await.expect("bootstrap should succeed");

    let plugin_load_result = Arc::new(load_plugins(&root));
    let (tool_registry, plugin_tool_diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), plugin_load_result.as_ref());
    let plugin_load_result = Arc::new(rust_agent::plugins::types::PluginLoadResult {
        root: plugin_load_result.root.clone(),
        source: plugin_load_result.source,
        plugins: plugin_load_result.plugins.clone(),
        diagnostics: plugin_load_result
            .diagnostics
            .iter()
            .cloned()
            .chain(plugin_tool_diagnostics)
            .collect(),
        orphaned_governance_entries: plugin_load_result.orphaned_governance_entries.clone(),
    });
    let _hook_registry = augment_hook_registry_with_plugins(
        rust_agent::hook::registry::HookRegistry::default(),
        plugin_load_result.as_ref(),
    );
    let command_registry = Arc::new(
        plugin_load_result
            .plugins
            .iter()
            .flat_map(|plugin| plugin.commands.iter().cloned())
            .fold(
                CommandRegistry::new()
                    .register(Arc::new(HelpCommand))
                    .register(Arc::new(StatusCommand))
                    .register(Arc::new(PluginsCommand)),
                |registry, command| registry.register(Arc::new(PluginSlashCommand::new(command))),
            ),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(tool_registry))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: Some(plugin_load_result),
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "anthropic".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "test-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "plugin-test-session".into(),
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

    let help = HelpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/help"),
            &app_state,
        )
        .await
        .expect("help should render");
    let status = StatusCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/status"),
            &app_state,
        )
        .await
        .expect("status should render");
    let plugins = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins"),
            &app_state,
        )
        .await
        .expect("plugins should render");

    let help_text = help.to_plain_text().expect("help should render text");
    let status_text = status.to_plain_text().expect("status should render text");
    let plugins_text = plugins.to_plain_text().expect("plugins should render text");

    assert!(help_text.contains("/demo-plugin-cmd — Demo plugin command"));
    assert!(status_text.contains("Observability:"));
    assert!(status_text.contains("- retryable_count: 0"));
    assert!(status_text.contains("- terminal_count: 0"));
    assert!(status_text.contains("- by_failure_code: none"));
    assert!(status_text.contains("- by_provider_kind: none"));
    assert!(status_text.contains("- compact_recovery_hits: none"));
    assert!(!status_text.contains("service_failures_total"));
    assert!(!status_text.contains("api_errors_by_kind"));
    assert!(!status_text.contains("mcp_failures_by_kind"));
    assert!(status_text.contains("normalized runtime failure signals"));
    assert!(
        help_text
            .contains("/plugins — Inspect plugin inventory, diagnostics, and governance state")
    );
    assert!(status_text.contains("demo-plugin v0.1.0 — state=enabled, applied=applied, enabled=yes, active(commands=1, hooks=1, tools=1), discovered(commands=1, hooks=1, tools=1), capabilities=commands,hooks,tools"));
    assert!(status_text.contains("- discovered_plugin_tools: 1"));
    assert!(status_text.contains("- discovered_plugin_hooks: 1"));
    assert!(plugins_text.contains("Plugins:"));
    assert!(
        plugins_text.contains("demo-plugin v0.1.0 — state=enabled, applied=applied, enabled=yes")
    );

    std::env::set_current_dir(previous_cwd).expect("should restore cwd");
    fs::remove_dir_all(root).expect("temp plugin root should be removed");
}

#[tokio::test]
async fn wasm_plugin_tool_executes_happy_path() {
    let root = unique_temp_path("rust-agent-plugin-runtime-executor-flow");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    write_wasm_fixture(
        &plugin_dir.join("dist").join("plugin.wasm"),
        "plugin_runtime_echo.wat",
    );
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["tools"],
  "runtime": {
    "kind": "wasm",
    "artifact": "dist/plugin.wasm",
    "entry": "run_tool",
    "timeout_ms": 1000,
    "output_cap_bytes": 4096
  },
  "tools": [
    {
      "name": "demo_tool",
      "description": "Demo plugin tool",
      "prompt": "ignored by executor",
      "read_only": true,
      "search_hint": "plugin demo tool"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let plugin_load_result = Arc::new(load_plugins(&root));
    let (tool_registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), plugin_load_result.as_ref());
    assert!(diagnostics.is_empty());

    let result = tool_registry
        .invoke(
            &ToolCall::new("plugin.demo-plugin.demo_tool", "{\"query\":\"hello\"}"),
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("executor invoke should succeed");
    let ToolResult::Text(message) = result else {
        panic!("expected text result");
    };
    assert_eq!(message, "echo:{\"query\":\"hello\"}");

    fs::remove_dir_all(root).expect("temp plugin root should be removed");
}

#[tokio::test]
async fn wasm_plugin_tool_with_static_data_segment_executes_happy_path() {
    let root = unique_temp_path("rust-agent-plugin-runtime-static-data-flow");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    write_wasm_fixture(
        &plugin_dir.join("dist").join("plugin.wasm"),
        "plugin_runtime_static_data.wat",
    );
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["tools"],
  "runtime": {
    "kind": "wasm",
    "artifact": "dist/plugin.wasm",
    "entry": "run_tool",
    "timeout_ms": 1000,
    "output_cap_bytes": 4096
  },
  "tools": [
    {
      "name": "demo_tool",
      "description": "Demo plugin tool",
      "prompt": "ignored by executor",
      "read_only": true,
      "search_hint": "plugin demo tool"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let plugin_load_result = Arc::new(load_plugins(&root));
    let (tool_registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), plugin_load_result.as_ref());
    assert!(diagnostics.is_empty());

    let result = tool_registry
        .invoke(
            &ToolCall::new("plugin.demo-plugin.demo_tool", "{\"query\":\"hello\"}"),
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("executor invoke should succeed with static data fixture");
    let ToolResult::Text(message) = result else {
        panic!("expected text result");
    };
    assert_eq!(message, "static:{\"query\":\"hello\"}");

    fs::remove_dir_all(root).expect("temp plugin root should be removed");
}

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{
    BootstrapCli, BootstrapState, InteractionSurface, RuntimeBootstrap, SessionMode,
};
use rust_agent::plan::manager::PlanManager;
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout,
};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::state::permission_context::ToolPermissionContext;
use rust_agent::task::list_manager::TaskListManager;
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::file_write::FileWriteTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

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
    }
}

#[tokio::test]
async fn bootstrap_env_policy_is_attached_and_enforced_by_file_tools() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    let root = unique_temp_path("rust-agent-fs-policy-flow");
    let home = root.join("home");
    let allowed_dir = root.join("allowed");
    let readonly_dir = root.join("readonly");
    fs::create_dir_all(home.join(".claude")).expect("create home policy dir");
    fs::create_dir_all(&allowed_dir).expect("create allowed dir");
    fs::create_dir_all(&readonly_dir).expect("create readonly dir");
    fs::write(readonly_dir.join("note.txt"), "hello from readonly").expect("seed readonly file");

    let policy_path = home.join(".claude").join("filesystem-policy.json");
    fs::write(
        &policy_path,
        format!(
            r#"{{
  "protected_paths": [],
  "rules": [
    {{ "path": "{}", "level": "allow" }},
    {{ "path": "{}", "level": "read_only" }}
  ]
}}"#,
            allowed_dir.display(),
            readonly_dir.display(),
        ),
    )
    .expect("write filesystem policy");

    let original_home = std::env::var_os("HOME");
    let original_policy = std::env::var_os("RUST_AGENT_FILESYSTEM_POLICY");
    set_env("HOME", home.as_os_str());
    remove_env("RUST_AGENT_FILESYSTEM_POLICY");

    let bootstrap = RuntimeBootstrap::from_cli(BootstrapCli {
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
    let state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    let bundle = bootstrap
        .initialize_runtime(
            &state,
            "fs-policy-session".into(),
            Arc::new(TaskManager::default()),
            Arc::new(TaskListManager::default()),
            Arc::new(PlanManager::default()),
        )
        .expect("runtime should initialize");

    let permissions: ToolPermissionContext =
        ToolPermissionContext::new(rust_agent::state::permission_context::PermissionMode::Default)
            .with_filesystem_policy(
                bundle
                    .filesystem_policy
                    .clone()
                    .expect("filesystem policy should load from HOME default path"),
            );

    let read_result = FileReadTool
        .invoke(
            &ToolCall::new(
                "Read",
                readonly_dir.join("note.txt").to_string_lossy().to_string(),
            ),
            &permissions,
        )
        .await
        .expect("read in readonly dir should succeed");
    assert_eq!(read_result, ToolResult::Text("hello from readonly".into()));

    let denied = FileWriteTool
        .invoke(
            &ToolCall::new(
                "Write",
                serde_json::json!({
                    "file_path": readonly_dir.join("note.txt").to_string_lossy(),
                    "content": "mutated"
                })
                .to_string(),
            ),
            &permissions,
        )
        .await
        .expect_err("write in readonly dir should be denied");
    assert!(denied.to_string().contains("read_only"));

    let allow_result = FileWriteTool
        .invoke(
            &ToolCall::new(
                "Write",
                serde_json::json!({
                    "file_path": allowed_dir.join("new.txt").to_string_lossy(),
                    "content": "created"
                })
                .to_string(),
            ),
            &permissions,
        )
        .await
        .expect("write in allowed dir should succeed");
    assert_eq!(
        allow_result,
        ToolResult::Text(format!("wrote {}", allowed_dir.join("new.txt").display()))
    );

    match original_home {
        Some(value) => set_env("HOME", &value),
        None => remove_env("HOME"),
    }
    match original_policy {
        Some(value) => set_env("RUST_AGENT_FILESYSTEM_POLICY", &value),
        None => remove_env("RUST_AGENT_FILESYSTEM_POLICY"),
    }
    fs::remove_dir_all(root).expect("cleanup temp root");
}

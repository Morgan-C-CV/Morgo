use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rust_agent::bootstrap::{InteractionSurface, SessionMode};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::ask_user::AskUserQuestionTool;
use rust_agent::tool::builtin::bash::BashTool;
use rust_agent::tool::builtin::file_edit::FileEditTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::file_write::FileWriteTool;
use rust_agent::tool::builtin::glob::GlobTool;
use rust_agent::tool::builtin::grep::GrepTool;
use rust_agent::tool::builtin::task_create::TaskCreateTool;
use rust_agent::tool::builtin::task_stop::TaskStopTool;
use rust_agent::tool::builtin::task_update::TaskUpdateTool;
use rust_agent::tool::builtin::tool_search::ToolSearchTool;
use rust_agent::tool::builtin::web_fetch::{WebFetchTool, fetch_text_with};
use rust_agent::tool::builtin::web_search::WebSearchTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use rust_agent::tool::permission::is_tool_allowed;
use rust_agent::tool::registry::{ToolAssemblyContext, ToolAssemblyEnvironment, ToolRegistry};
use tokio::fs;

struct MetadataFixtureTool {
    metadata: ToolMetadata,
}

#[async_trait]
impl Tool for MetadataFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        self.metadata.clone()
    }

    async fn invoke(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Text("fixture".into()))
    }
}

fn metadata_fixture(
    name: &'static str,
    always_load: bool,
    should_defer: bool,
    requires_user_interaction: bool,
    is_open_world: bool,
) -> Arc<dyn Tool> {
    Arc::new(MetadataFixtureTool {
        metadata: ToolMetadata {
            name,
            description: "metadata fixture",
            aliases: &[],
            search_hint: None,
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load,
            should_defer,
            requires_auth: false,
            requires_user_interaction,
            is_open_world,
            is_search_or_read_command: false,
        },
    })
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn cwd_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn read_tool_returns_file_contents() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-read"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let file = dir.join("sample.txt");
    fs::write(&file, "hello from read tool")
        .await
        .expect("write sample file");

    let result = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: file.to_string_lossy().into_owned(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("read should succeed");

    assert_eq!(result, ToolResult::Text("hello from read tool".into()));

    fs::remove_dir_all(&dir).await.expect("cleanup dir");
}

#[tokio::test]
async fn glob_tool_matches_nested_files() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-glob"));
    let nested = dir.join("nested");
    fs::create_dir_all(&nested)
        .await
        .expect("create nested dir");
    fs::write(dir.join("alpha.rs"), "fn alpha() {}")
        .await
        .expect("write alpha");
    fs::write(nested.join("beta.rs"), "fn beta() {}")
        .await
        .expect("write beta");
    fs::write(nested.join("gamma.txt"), "ignore me")
        .await
        .expect("write gamma");

    let dir_for_call = dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _guard = cwd_lock().lock().expect("cwd lock poisoned");
        let original = std::env::current_dir().expect("get current dir");
        std::env::set_current_dir(&dir_for_call).expect("enter temp dir");

        let runtime = tokio::runtime::Runtime::new().expect("create runtime");
        let result = runtime.block_on(async {
            GlobTool
                .invoke(
                    &ToolCall {
                        name: "Glob".into(),
                        input: "*.rs".into(),
                    },
                    &ToolPermissionContext::new(PermissionMode::Default),
                )
                .await
        });

        std::env::set_current_dir(&original).expect("restore current dir");
        result
    })
    .await
    .expect("join blocking glob task")
    .expect("glob should succeed");

    fs::remove_dir_all(&dir).await.expect("cleanup dir");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("alpha.rs"));
    assert!(text.contains("nested/beta.rs"));
    assert!(!text.contains("gamma.txt"));
}

#[tokio::test]
async fn grep_tool_reports_matching_lines() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-grep"));
    let nested = dir.join("nested");
    fs::create_dir_all(&nested)
        .await
        .expect("create nested dir");
    fs::write(dir.join("alpha.txt"), "first\nneedle here\nlast")
        .await
        .expect("write alpha");
    fs::write(nested.join("beta.txt"), "needle there too")
        .await
        .expect("write beta");

    let dir_for_call = dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _guard = cwd_lock().lock().expect("cwd lock poisoned");
        let original = std::env::current_dir().expect("get current dir");
        std::env::set_current_dir(&dir_for_call).expect("enter temp dir");

        let runtime = tokio::runtime::Runtime::new().expect("create runtime");
        let result = runtime.block_on(async {
            GrepTool
                .invoke(
                    &ToolCall {
                        name: "Grep".into(),
                        input: "needle".into(),
                    },
                    &ToolPermissionContext::new(PermissionMode::Default),
                )
                .await
        });

        std::env::set_current_dir(&original).expect("restore current dir");
        result
    })
    .await
    .expect("join blocking grep task")
    .expect("grep should succeed");

    fs::remove_dir_all(&dir).await.expect("cleanup dir");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("alpha.txt:2:needle here"));
    assert!(text.contains("nested/beta.txt:1:needle there too"));
}

#[tokio::test]
async fn edit_tool_replaces_unique_match() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-edit"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let file = dir.join("sample.txt");
    fs::write(&file, "before\nneedle\nafter")
        .await
        .expect("write sample file");

    let input = serde_json::json!({
        "file_path": file.to_string_lossy(),
        "old_string": "needle",
        "new_string": "replacement"
    })
    .to_string();

    let result = FileEditTool
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input,
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("edit should succeed");

    assert_eq!(
        result,
        ToolResult::Text(format!("edited {}", file.display()))
    );
    let updated = fs::read_to_string(&file).await.expect("read edited file");
    assert_eq!(updated, "before\nreplacement\nafter");

    fs::remove_dir_all(&dir).await.expect("cleanup dir");
}

#[tokio::test]
async fn edit_tool_rejects_non_unique_match_without_replace_all() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-edit-duplicate"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let file = dir.join("sample.txt");
    fs::write(&file, "needle\nneedle")
        .await
        .expect("write sample file");

    let input = serde_json::json!({
        "file_path": file.to_string_lossy(),
        "old_string": "needle",
        "new_string": "replacement"
    })
    .to_string();

    let error = FileEditTool
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input,
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect_err("edit should fail for duplicate match");

    assert!(error.to_string().contains("old_string is not unique"));
    fs::remove_dir_all(&dir).await.expect("cleanup dir");
}

#[tokio::test]
async fn web_fetch_tool_returns_response_body() {
    let body = fetch_text_with("https://example.com", |_url| async {
        Ok((200, "hello client".into()))
    })
    .await
    .expect("fake fetch should succeed");
    assert_eq!(body, "hello client");
}

#[tokio::test]
async fn web_fetch_tool_rejects_invalid_url() {
    let error = WebFetchTool
        .invoke(
            &ToolCall {
                name: "WebFetch".into(),
                input: "not-a-url".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect_err("invalid url should fail");

    assert!(error.to_string().contains("invalid URL"));
}

#[tokio::test]
async fn web_fetch_seam_reports_http_errors_without_socket_bind() {
    let error = fetch_text_with("https://example.com", |_url| async {
        Ok((503, "unavailable".into()))
    })
    .await
    .expect_err("http error should fail");

    assert!(error.to_string().contains("HTTP 503"));
}

#[tokio::test]
async fn bash_tool_executes_safe_command() {
    let result = BashTool
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "printf 'hello from bash'"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("bash should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("command: printf 'hello from bash'"));
    assert!(text.contains("cwd: "));
    assert!(text.contains("sandbox_policy: WorkspaceWrite"));
    assert!(text.contains("exit_code: 0"));
    assert!(text.contains("stdout:\nhello from bash"));
}

#[tokio::test]
async fn registry_denies_unsafe_bash_in_plan_mode() {
    let registry = ToolRegistry::new().register(Arc::new(BashTool));
    let denied = registry
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "echo hi > out.txt"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Plan),
        )
        .await
        .expect("registry should return denied result");

    assert_eq!(
        denied,
        ToolResult::Denied("bash command is not allowed in plan mode".into())
    );
}

#[tokio::test]
async fn registry_returns_pending_approval_for_ask_only_bash() {
    let registry = ToolRegistry::new().register(Arc::new(BashTool));
    let result = registry
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "sudo whoami"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("registry should return pending approval result");

    assert_eq!(
        result,
        ToolResult::PendingApproval {
            tool_name: "Bash".into(),
            message:
                "bash command warning [privileged_system]: command touches privileged system state"
                    .into(),
        }
    );
}

#[tokio::test]
async fn bash_tool_launches_background_task() {
    let manager = Arc::new(rust_agent::task::manager::TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("session-bash");

    let result = BashTool
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "printf 'background hello'",
                    "run_in_background": true,
                    "description": "background demo"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("background bash should launch");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("background bash task task-0 launched"));

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let task = manager.get("task-0").expect("task should exist");
            if task.status == rust_agent::task::types::TaskStatus::Completed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background task should complete");

    let output = manager
        .get_output("task-0", 0)
        .expect("background task output should exist");
    assert!(output.content.contains("description: background demo"));
    assert!(output.content.contains("stdout:\nbackground hello"));
}

#[tokio::test]
async fn registry_rejects_non_json_input_for_schema_backed_tools() {
    let registry = ToolRegistry::new().register(Arc::new(FileEditTool));
    let error = registry
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input: "not-json".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect_err("schema-backed tool should reject non-json input");

    assert!(
        error
            .to_string()
            .contains("tool Edit requires JSON-structured input")
    );
}

#[tokio::test]
async fn registry_allows_safe_bash_in_plan_mode() {
    let _guard = cwd_lock().lock().expect("cwd lock poisoned");
    let registry = ToolRegistry::new().register(Arc::new(BashTool));
    let permissions = ToolPermissionContext::new(PermissionMode::Plan);
    let result = registry
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "pwd"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("safe bash should execute in plan mode");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("command: pwd"));
    assert!(text.contains("sandbox_policy: ReadOnly"));
    assert!(text.contains("exit_code: 0"));
}

#[tokio::test]
async fn registry_allows_normalized_safe_bash_in_plan_mode() {
    let _guard = cwd_lock().lock().expect("cwd lock poisoned");
    let registry = ToolRegistry::new().register(Arc::new(BashTool));
    let permissions = ToolPermissionContext::new(PermissionMode::Plan);
    let result = registry
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "env FOO=bar pwd"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("normalized safe bash should execute in plan mode");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("command: env FOO=bar pwd"));
    assert!(text.contains("sandbox_policy: ReadOnly"));
    assert!(text.contains("exit_code: 0"));
}

#[tokio::test]
async fn read_only_bash_blocks_file_writes() {
    let result = BashTool
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "pwd > should-not-exist.txt"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("bash command should execute");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("sandbox_policy: WorkspaceWrite"));
}

#[tokio::test]
async fn tool_search_filters_catalog() {
    let result = ToolSearchTool
        .invoke(
            &ToolCall {
                name: "ToolSearch".into(),
                input: "edit".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("tool search should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("Edit - Edit existing files with safety rails"));
    assert!(!text.contains("WebFetch - Fetch remote web content"));
}

#[tokio::test]
async fn tool_search_prefers_runtime_registry_when_available() {
    let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
    let permissions =
        ToolPermissionContext::new(PermissionMode::Default).with_inherited_tool_registry(registry);

    let result = ToolSearchTool
        .invoke(
            &ToolCall {
                name: "ToolSearch".into(),
                input: "read".into(),
            },
            &permissions,
        )
        .await
        .expect("tool search should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("Read - Read files from disk"));
    assert!(!text.contains("Edit - Edit existing files with safety rails"));
}

#[tokio::test]
async fn tool_search_matches_search_hint() {
    let registry = ToolRegistry::new().register(Arc::new(FileWriteTool));
    let permissions =
        ToolPermissionContext::new(PermissionMode::Default).with_inherited_tool_registry(registry);

    let result = ToolSearchTool
        .invoke(
            &ToolCall {
                name: "ToolSearch".into(),
                input: "create file".into(),
            },
            &permissions,
        )
        .await
        .expect("tool search should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("Write - Write file contents to disk"));
}

#[test]
fn auth_gated_tools_stay_visible_after_deferred_loading() {
    let context = ToolPermissionContext::new(PermissionMode::Default).with_deferred_tools(true);
    assert!(is_tool_allowed(&WebFetchTool.metadata(), &context));
}

#[test]
fn visible_tools_include_ask_only_tools() {
    let registry = ToolRegistry::new()
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebFetchTool));

    let visible = registry.visible_tools(
        &ToolPermissionContext::new(PermissionMode::Default).with_deferred_tools(true),
    );
    let names = visible.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert!(names.contains(&"Bash"));
    assert!(names.contains(&"Read"));
    assert!(names.contains(&"WebFetch"));
}

#[test]
fn worker_tool_filter_excludes_agent_and_interactive_tools() {
    let registry = ToolRegistry::new()
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(AgentTool))
        .register(Arc::new(AskUserQuestionTool))
        .register(Arc::new(TaskCreateTool))
        .register(Arc::new(TaskStopTool))
        .register(Arc::new(TaskUpdateTool))
        .register(Arc::new(WebSearchTool));

    let filtered = registry.filter_for_worker();
    let names = filtered
        .visible_tools(&ToolPermissionContext::new(PermissionMode::Default))
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(names.contains(&"TaskCreate"));
    assert!(names.contains(&"TaskStop"));
    assert!(names.contains(&"TaskUpdate"));
    assert!(!names.contains(&"Agent"));
    assert!(!names.contains(&"AskUserQuestion"));
    assert!(!names.contains(&"Bash"));
    assert!(!names.contains(&"WebSearch"));
}

#[test]
fn assembly_context_controls_deferred_and_interactive_visibility() {
    let registry = ToolRegistry::new()
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebSearchTool));

    let headless_remote = registry.assemble(ToolAssemblyContext::worker(
        rust_agent::bootstrap::InteractionSurface::Remote,
        rust_agent::bootstrap::SessionMode::Headless,
    ));
    let names = headless_remote
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(!names.contains(&"Bash"));
    assert!(!names.contains(&"WebSearch"));
}

#[test]
fn coordinator_assembly_keeps_always_load_and_deferred_tools_visible() {
    let registry = ToolRegistry::new()
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebSearchTool));

    let assembled = registry.assemble(ToolAssemblyContext::coordinator(
        rust_agent::bootstrap::InteractionSurface::Cli,
        rust_agent::bootstrap::SessionMode::Interactive,
    ));
    let names = assembled
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"Bash"));
    assert!(names.contains(&"Read"));
    assert!(names.contains(&"WebSearch"));
}

#[test]
fn open_world_tools_are_filtered_from_remote_and_headless_assembly() {
    let registry = ToolRegistry::new()
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebSearchTool))
        .register(Arc::new(WebFetchTool));

    let remote = registry.assemble(ToolAssemblyContext::coordinator(
        rust_agent::bootstrap::InteractionSurface::Remote,
        rust_agent::bootstrap::SessionMode::Interactive,
    ));
    let remote_names = remote
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(remote_names.contains(&"Read"));
    assert!(!remote_names.contains(&"WebSearch"));
    assert!(!remote_names.contains(&"WebFetch"));

    let cli_headless = registry.assemble(ToolAssemblyContext::coordinator(
        rust_agent::bootstrap::InteractionSurface::Cli,
        rust_agent::bootstrap::SessionMode::Headless,
    ));
    let cli_headless_names = cli_headless
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(cli_headless_names.contains(&"Read"));
    assert!(!cli_headless_names.contains(&"WebSearch"));
    assert!(!cli_headless_names.contains(&"WebFetch"));
}

#[test]
fn assembly_environment_can_explicitly_disable_open_world_tools() {
    let registry = ToolRegistry::new()
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebSearchTool));

    let assembled = registry.assemble(ToolAssemblyContext {
        runtime_role: rust_agent::state::app_state::RuntimeRole::Coordinator,
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Interactive,
        environment: ToolAssemblyEnvironment::Restricted,
        include_deferred_tools: true,
        include_interactive_tools: true,
        include_open_world_tools: false,
    });
    let names = assembled
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(!names.contains(&"WebSearch"));
}

#[test]
fn always_load_overrides_defer_but_not_interactive_gating() {
    let registry = ToolRegistry::new()
        .register(metadata_fixture(
            "DeferredAlwaysLoaded",
            true,
            true,
            false,
            false,
        ))
        .register(metadata_fixture(
            "InteractiveAlwaysLoaded",
            true,
            false,
            true,
            false,
        ));

    let visible = registry.visible_tools(
        &ToolPermissionContext::new(PermissionMode::Default)
            .with_deferred_tools(false)
            .with_interactive_tools(false),
    );
    let names = visible.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert!(names.contains(&"DeferredAlwaysLoaded"));
    assert!(!names.contains(&"InteractiveAlwaysLoaded"));
}

#[test]
fn combined_always_load_defer_and_interactive_flags_follow_context() {
    let registry =
        ToolRegistry::new().register(metadata_fixture("HybridFixture", true, true, true, false));

    let coordinator = registry.assemble(ToolAssemblyContext::coordinator(
        rust_agent::bootstrap::InteractionSurface::Cli,
        rust_agent::bootstrap::SessionMode::Interactive,
    ));
    let coordinator_names = coordinator
        .visible_tools(
            &ToolAssemblyContext::coordinator(
                rust_agent::bootstrap::InteractionSurface::Cli,
                rust_agent::bootstrap::SessionMode::Interactive,
            )
            .permission_context(PermissionMode::Default),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(coordinator_names.contains(&"HybridFixture"));

    let worker = registry.assemble(ToolAssemblyContext::worker(
        rust_agent::bootstrap::InteractionSurface::Cli,
        rust_agent::bootstrap::SessionMode::Headless,
    ));
    let worker_names = worker
        .visible_tools(
            &ToolAssemblyContext::worker(
                rust_agent::bootstrap::InteractionSurface::Cli,
                rust_agent::bootstrap::SessionMode::Headless,
            )
            .permission_context(PermissionMode::Default),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!worker_names.contains(&"HybridFixture"));
}

#[test]
fn real_builtin_metadata_flags_follow_runtime_context() {
    let registry = ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(AskUserQuestionTool))
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebFetchTool))
        .register(Arc::new(WebSearchTool));

    let cli_interactive = registry.assemble(ToolAssemblyContext::coordinator(
        InteractionSurface::Cli,
        SessionMode::Interactive,
    ));
    let cli_interactive_names = cli_interactive
        .visible_tools(
            &ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Interactive)
                .permission_context(PermissionMode::Default),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(cli_interactive_names.contains(&"Agent"));
    assert!(cli_interactive_names.contains(&"AskUserQuestion"));
    assert!(cli_interactive_names.contains(&"Bash"));
    assert!(cli_interactive_names.contains(&"Read"));
    assert!(cli_interactive_names.contains(&"WebFetch"));
    assert!(cli_interactive_names.contains(&"WebSearch"));

    let remote_interactive = registry.assemble(ToolAssemblyContext::coordinator(
        InteractionSurface::Remote,
        SessionMode::Interactive,
    ));
    let remote_interactive_names = remote_interactive
        .visible_tools(
            &ToolAssemblyContext::coordinator(InteractionSurface::Remote, SessionMode::Interactive)
                .permission_context(PermissionMode::Default),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(remote_interactive_names.contains(&"Agent"));
    assert!(remote_interactive_names.contains(&"AskUserQuestion"));
    assert!(!remote_interactive_names.contains(&"Bash"));
    assert!(!remote_interactive_names.contains(&"WebFetch"));
    assert!(!remote_interactive_names.contains(&"WebSearch"));

    let cli_headless = registry.assemble(ToolAssemblyContext::coordinator(
        InteractionSurface::Cli,
        SessionMode::Headless,
    ));
    let cli_headless_names = cli_headless
        .visible_tools(
            &ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Headless)
                .permission_context(PermissionMode::Default),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(cli_headless_names.contains(&"Agent"));
    assert!(cli_headless_names.contains(&"AskUserQuestion"));
    assert!(!cli_headless_names.contains(&"Bash"));
    assert!(cli_headless_names.contains(&"Read"));
    assert!(!cli_headless_names.contains(&"WebFetch"));
    assert!(!cli_headless_names.contains(&"WebSearch"));
}

#[test]
fn real_builtin_worker_assembly_excludes_interactive_and_open_world_tools() {
    let registry = ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(AskUserQuestionTool))
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebFetchTool))
        .register(Arc::new(WebSearchTool));

    let worker = registry.assemble(ToolAssemblyContext::worker(
        InteractionSurface::Cli,
        SessionMode::Headless,
    ));
    let names = worker
        .visible_tools(
            &ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless)
                .permission_context(PermissionMode::Default),
        )
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(!names.contains(&"Agent"));
    assert!(!names.contains(&"AskUserQuestion"));
    assert!(!names.contains(&"Bash"));
    assert!(!names.contains(&"WebFetch"));
    assert!(!names.contains(&"WebSearch"));
}

#[test]
fn open_world_remains_independent_assembly_gate_under_always_load() {
    let registry = ToolRegistry::new().register(metadata_fixture(
        "OpenWorldAlwaysLoaded",
        true,
        false,
        false,
        true,
    ));

    let cli_interactive = registry.assemble(ToolAssemblyContext::coordinator(
        rust_agent::bootstrap::InteractionSurface::Cli,
        rust_agent::bootstrap::SessionMode::Interactive,
    ));
    let cli_names = cli_interactive
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(cli_names.contains(&"OpenWorldAlwaysLoaded"));

    let remote = registry.assemble(ToolAssemblyContext::coordinator(
        rust_agent::bootstrap::InteractionSurface::Remote,
        rust_agent::bootstrap::SessionMode::Interactive,
    ));
    let remote_names = remote
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!remote_names.contains(&"OpenWorldAlwaysLoaded"));

    let restricted = registry.assemble(ToolAssemblyContext {
        runtime_role: rust_agent::state::app_state::RuntimeRole::Coordinator,
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Interactive,
        environment: ToolAssemblyEnvironment::Restricted,
        include_deferred_tools: true,
        include_interactive_tools: true,
        include_open_world_tools: false,
    });
    let restricted_names = restricted
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!restricted_names.contains(&"OpenWorldAlwaysLoaded"));
}

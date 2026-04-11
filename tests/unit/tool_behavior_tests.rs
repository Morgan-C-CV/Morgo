use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::bash::BashTool;
use rust_agent::tool::builtin::file_edit::FileEditTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::glob::GlobTool;
use rust_agent::tool::builtin::grep::GrepTool;
use rust_agent::tool::builtin::task_stop::TaskStopTool;
use rust_agent::tool::builtin::tool_search::ToolSearchTool;
use rust_agent::tool::builtin::web_fetch::WebFetchTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};
use rust_agent::tool::permission::is_tool_allowed;
use rust_agent::tool::registry::ToolRegistry;
use tokio::fs;

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
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept connection");
        let mut buffer = [0_u8; 1024];
        let _ = stream.read(&mut buffer);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\nConnection: close\r\n\r\nhello client",
            )
            .expect("write response");
    });

    let result = WebFetchTool
        .invoke(
            &ToolCall {
                name: "WebFetch".into(),
                input: format!("http://{addr}"),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("web fetch should succeed");

    handle.join().expect("server should exit cleanly");
    assert_eq!(result, ToolResult::Text("hello client".into()));
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
async fn registry_allows_safe_bash_in_plan_mode() {
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
    assert!(text.contains("exit_code: 0"));
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

#[test]
fn auth_gated_tools_stay_visible_for_explicit_approval() {
    let context = ToolPermissionContext::new(PermissionMode::Default);
    assert!(is_tool_allowed(&WebFetchTool.metadata(), &context));
}

#[test]
fn visible_tools_include_ask_only_tools() {
    let registry = ToolRegistry::new()
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebFetchTool));

    let visible = registry.visible_tools(&ToolPermissionContext::new(PermissionMode::Default));
    let names = visible.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert!(names.contains(&"Bash"));
    assert!(names.contains(&"Read"));
    assert!(names.contains(&"WebFetch"));
}

#[test]
fn worker_tool_filter_excludes_agent_tool() {
    let registry = ToolRegistry::new()
        .register(Arc::new(BashTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(AgentTool))
        .register(Arc::new(TaskStopTool));

    let filtered = registry.filter_for_worker();
    let names = filtered
        .visible_tools(&ToolPermissionContext::new(PermissionMode::Default))
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"Bash"));
    assert!(names.contains(&"Read"));
    assert!(names.contains(&"TaskStop"));
    assert!(!names.contains(&"Agent"));
}

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
use rust_agent::tool::builtin::notebook_edit::NotebookEditTool;
use rust_agent::tool::builtin::task_create::TaskCreateTool;
use rust_agent::tool::builtin::task_get::TaskGetTool;
use rust_agent::tool::builtin::task_list::TaskListTool;
use rust_agent::tool::builtin::task_output::TaskOutputTool;
use rust_agent::tool::builtin::task_stop::TaskStopTool;
use rust_agent::tool::builtin::task_update::TaskUpdateTool;
use rust_agent::tool::builtin::todo_write::TodoWriteTool;
use rust_agent::tool::builtin::tool_search::ToolSearchTool;
use rust_agent::tool::builtin::web_fetch::{WebFetchTool, fetch_text_with};
use rust_agent::tool::builtin::web_search::WebSearchTool;
use rust_agent::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};
use rust_agent::tool::permission::{evaluate_tool_permission, is_tool_allowed};
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

fn restore_cwd(original: &std::path::Path) {
    if std::env::set_current_dir(original).is_ok() {
        return;
    }
    let _ = std::env::set_current_dir("/");
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
async fn read_tool_raw_string_input_keeps_legacy_bare_text_shape() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-read-raw-legacy"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let file = dir.join("sample.txt");
    fs::write(&file, "legacy raw read output")
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
        .expect("raw-string read should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result for raw-string read");
    };
    assert_eq!(text, "legacy raw read output");
    assert!(
        !text.contains("path=") && !text.contains("offset=") && !text.contains("returned_chars="),
        "legacy raw-string input should keep bare text shape until that compatibility path is intentionally removed; text={text:?}"
    );

    fs::remove_dir_all(&dir).await.expect("cleanup dir");
}

#[tokio::test]
async fn read_tool_truncates_large_files_and_supports_offsets() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-read-large"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let file = dir.join("large.txt");
    let content = "a".repeat(9_000);
    fs::write(&file, content).await.expect("write sample file");

    let first = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({ "file_path": file }).to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("read should succeed");
    let ToolResult::Text(first_text) = first else {
        panic!("expected text result");
    };
    assert!(first_text.contains("[Read truncated:"));
    assert!(first_text.contains("offset=0"));
    assert!(first_text.contains("total_chars=9000"));

    let second = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({
                    "file_path": file,
                    "offset": 8_000,
                    "limit": 1_000
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("read offset should succeed");
    let ToolResult::Text(second_text) = second else {
        panic!("expected text result");
    };
    assert!(second_text.contains(&format!("path={}", file.display())));
    assert!(second_text.contains("offset=8000"));
    assert!(second_text.contains("returned_chars=1000"));
    assert!(second_text.contains(&"a".repeat(1_000)));
    assert!(second_text.contains("[Read truncated:"));

    fs::remove_dir_all(&dir).await.expect("cleanup dir");
}

#[tokio::test]
async fn read_tool_blocks_structured_data_paging_after_schema_sample() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-read-jsonl"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let file = dir.join("samples.jsonl");
    let content = (0..600)
        .map(|i| format!(r#"{{"case":"u{i}","cost":{i}}}"#))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&file, content).await.expect("write sample file");

    let first = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({ "file_path": file }).to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("initial schema sample should succeed");
    assert!(matches!(first, ToolResult::Text(_)));

    let second = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({
                    "file_path": file,
                    "offset": 5_000,
                    "limit": 5_000
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("structured paging should return a structured tool result");
    let ToolResult::ResultTooLarge(message) = second else {
        panic!("expected ResultTooLarge for structured data paging");
    };
    assert!(message.contains("structured data paging stopped"));
    assert!(message.contains("Use Bash or a local script"));

    fs::remove_dir_all(&dir).await.expect("cleanup dir");
}

#[tokio::test]
async fn read_tool_result_shape_stays_locatable_truncation_aware_and_failure_explicit() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-read-contract"));
    fs::create_dir_all(&dir).await.expect("create dir");

    let full_file = dir.join("full.txt");
    fs::write(&full_file, "alpha\nbeta\ngamma\n")
        .await
        .expect("write full sample");

    let large_file = dir.join("large.txt");
    fs::write(&large_file, "z".repeat(9_000))
        .await
        .expect("write large sample");

    let full = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({ "file_path": full_file }).to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("full read should succeed");
    let ToolResult::Text(full_text) = full else {
        panic!("expected text result for full read");
    };

    assert!(
        full_text.contains(&format!("path={}", full_file.display()))
            || full_text.contains(&format!("file_path={}", full_file.display()))
            || full_text.contains(&full_file.display().to_string()),
        "successful read should include stable file location context; text={full_text:?}"
    );
    assert!(
        full_text.contains("line")
            || full_text.contains("offset=")
            || full_text.contains("range="),
        "successful read should include line/range/offset-style locator, not just bare contents; text={full_text:?}"
    );
    assert!(
        full_text.contains("alpha") && full_text.contains("beta") && full_text.contains("gamma"),
        "successful read should still include file contents; text={full_text:?}"
    );

    let truncated = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({ "file_path": large_file }).to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("truncated read should succeed with structured text");
    let ToolResult::Text(truncated_text) = truncated else {
        panic!("expected text result for truncated read");
    };
    assert!(
        truncated_text.contains("[Read truncated:")
            || truncated_text.contains("truncated")
            || truncated_text.contains("omitted"),
        "truncated read must explicitly say content was truncated/omitted; text={truncated_text:?}"
    );
    assert!(
        truncated_text.contains("offset=") && truncated_text.contains("total_chars="),
        "truncated read should preserve continuation context; text={truncated_text:?}"
    );

    let missing = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: serde_json::json!({ "file_path": dir.join("missing.txt") }).to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await;
    let error = missing.expect_err("missing file should surface a read failure");
    let message = error.to_string();
    assert!(
        message.contains("failed to read") || message.contains("No such file"),
        "read failure should read like a file-read failure, not a silent success; message={message:?}"
    );

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

        restore_cwd(&original);
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
async fn glob_tool_supports_path_and_ignores_target_directory() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-glob-path"));
    let src = dir.join("src");
    let target = dir.join("target");
    fs::create_dir_all(&src).await.expect("create src dir");
    fs::create_dir_all(&target)
        .await
        .expect("create target dir");
    fs::write(src.join("alpha.rs"), "fn alpha() {}")
        .await
        .expect("write src file");
    fs::write(target.join("beta.rs"), "fn beta() {}")
        .await
        .expect("write target file");

    let dir_for_call = dir.clone();
    let src_for_call = src.clone();
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
                        input: serde_json::json!({
                            "pattern": "*.rs",
                            "path": src_for_call.to_string_lossy()
                        })
                        .to_string(),
                    },
                    &ToolPermissionContext::new(PermissionMode::Default),
                )
                .await
        });

        restore_cwd(&original);
        result
    })
    .await
    .expect("join blocking glob task")
    .expect("glob should succeed");

    fs::remove_dir_all(&dir).await.expect("cleanup dir");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("src/alpha.rs"));
    assert!(!text.contains("target/beta.rs"));
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

        restore_cwd(&original);
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
async fn grep_tool_supports_path_and_ignores_target_directory() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-grep-path"));
    let docs = dir.join("docs");
    let target = dir.join("target");
    fs::create_dir_all(&docs).await.expect("create docs dir");
    fs::create_dir_all(&target)
        .await
        .expect("create target dir");
    fs::write(docs.join("alpha.txt"), "needle in docs")
        .await
        .expect("write docs file");
    fs::write(target.join("beta.txt"), "needle in target")
        .await
        .expect("write target file");

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
                        input: serde_json::json!({
                            "pattern": "needle",
                            "path": "docs"
                        })
                        .to_string(),
                    },
                    &ToolPermissionContext::new(PermissionMode::Default),
                )
                .await
        });

        restore_cwd(&original);
        result
    })
    .await
    .expect("join blocking grep task")
    .expect("grep should succeed");

    fs::remove_dir_all(&dir).await.expect("cleanup dir");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("docs/alpha.txt:1:needle in docs"));
    assert!(!text.contains("target/beta.txt"));
}

#[tokio::test]
async fn grep_tool_returns_result_too_large_for_oversized_search() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-grep-large"));
    fs::create_dir_all(&dir).await.expect("create dir");
    let mut contents = String::new();
    for index in 0..400 {
        contents.push_str(&format!("needle line {index}\n"));
    }
    fs::write(dir.join("large.txt"), contents)
        .await
        .expect("write large file");

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
                        input: serde_json::json!({
                            "pattern": "needle"
                        })
                        .to_string(),
                    },
                    &ToolPermissionContext::new(PermissionMode::Default),
                )
                .await
        });

        restore_cwd(&original);
        result
    })
    .await
    .expect("join blocking grep task")
    .expect("grep should succeed");

    fs::remove_dir_all(&dir).await.expect("cleanup dir");

    let ToolResult::ResultTooLarge(message) = result else {
        panic!("expected result-too-large");
    };
    assert!(message.contains("Grep matched"));
    assert!(message.contains("Narrow the query or provide a path"));
    assert!(message.contains("Preview:"));
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
async fn edit_tool_result_shape_stays_locatable_noop_distinguishable_and_failure_explicit() {
    let dir = std::env::temp_dir().join(unique_name("rust-agent-edit-contract"));
    fs::create_dir_all(&dir).await.expect("create dir");

    let file = dir.join("sample.txt");
    fs::write(&file, "before\nneedle\nafter\n")
        .await
        .expect("write sample file");

    let success = FileEditTool
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input: serde_json::json!({
                    "file_path": file,
                    "old_string": "needle",
                    "new_string": "replacement"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("edit should succeed");
    let ToolResult::Text(success_text) = success else {
        panic!("expected text result for successful edit");
    };
    assert!(
        success_text.contains(&format!("path={}", file.display()))
            || success_text.contains(&file.display().to_string()),
        "successful edit should include stable file location context; text={success_text:?}"
    );
    assert!(
        success_text.contains("old_text=")
            || success_text.contains("new_text=")
            || success_text.contains("replacements=")
            || success_text.contains("occurrences=")
            || success_text.contains("lines="),
        "successful edit should include a locatable change summary, not just a generic success string; text={success_text:?}"
    );

    let noop_error = FileEditTool
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input: serde_json::json!({
                    "file_path": file,
                    "old_string": "replacement",
                    "new_string": "replacement"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect_err("equivalent edit should not be reported as a successful change");
    let noop_text = noop_error.to_string();
    assert!(
        noop_text.contains("no change")
            || noop_text.contains("already")
            || noop_text.contains("must differ")
            || noop_text.contains("unchanged"),
        "noop edit should be explicitly distinguishable from a real edit; text={noop_text:?}"
    );

    let missing_target_error = FileEditTool
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input: serde_json::json!({
                    "file_path": file,
                    "old_string": "does-not-exist",
                    "new_string": "replacement"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect_err("missing target should fail explicitly");
    let missing_target_text = missing_target_error.to_string();
    assert!(
        missing_target_text.contains("not found")
            || missing_target_text.contains("failed")
            || missing_target_text.contains("invalid")
            || missing_target_text.contains("ambiguous"),
        "edit failure should be explicit, not look like a silent noop or vague success; text={missing_target_text:?}"
    );

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
async fn bash_tool_result_format_stays_stable_across_success_and_failure() {
    fn line_index_with_prefix(lines: &[&str], prefix: &str) -> usize {
        lines
            .iter()
            .position(|line| line.starts_with(prefix))
            .unwrap_or_else(|| panic!("missing line with prefix {prefix}; lines={lines:?}"))
    }

    let success = BashTool
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "printf 'bash-format-ok'"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("bash success case should execute");
    let ToolResult::Text(success_text) = success else {
        panic!("expected text result for bash success case");
    };

    let failure = BashTool
        .invoke(
            &ToolCall {
                name: "Bash".into(),
                input: serde_json::json!({
                    "command": "printf 'bash-format-fail' >&2; exit 7"
                })
                .to_string(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("bash failure case should still return structured text");
    let ToolResult::Text(failure_text) = failure else {
        panic!("expected text result for bash failure case");
    };

    let success_lines = success_text.lines().collect::<Vec<_>>();
    let failure_lines = failure_text.lines().collect::<Vec<_>>();
    for lines in [&success_lines, &failure_lines] {
        let command_index = line_index_with_prefix(lines, "command: ");
        let normalized_index = line_index_with_prefix(lines, "normalized_variants: ");
        let cwd_index = line_index_with_prefix(lines, "cwd: ");
        let sandbox_index = line_index_with_prefix(lines, "sandbox_policy: ");
        let exit_code_index = line_index_with_prefix(lines, "exit_code: ");
        assert!(
            command_index < normalized_index
                && normalized_index < cwd_index
                && cwd_index < sandbox_index
                && sandbox_index < exit_code_index,
            "bash result header drifted; lines={lines:?}"
        );
    }

    assert!(success_text.contains("command: printf 'bash-format-ok'"));
    assert!(success_text.contains("exit_code: 0"));
    assert!(success_text.contains("stdout:\nbash-format-ok"));
    assert!(
        !success_text.contains("stderr:\n"),
        "success shape should not invent stderr output; text={success_text:?}"
    );

    assert!(failure_text.contains("command: printf 'bash-format-fail' >&2; exit 7"));
    assert!(failure_text.contains("exit_code: 7"));
    assert!(failure_text.contains("stderr:\nbash-format-fail"));
    assert!(
        !failure_text.contains("stdout:\n"),
        "failure shape should preserve stderr-only output instead of fabricating stdout; text={failure_text:?}"
    );
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
        ToolResult::Denied("bash command denied [plan_mode]: command is not allowed in plan mode".into())
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
            approval: rust_agent::tool::result::PendingApprovalPayload {
                code: Some("privileged_system".into()),
                summary: "Bash pending approval".into(),
                detail: Some(
                    "command: sudo whoami\nreason: command touches privileged system state\nnext_step: approve or deny this Bash command"
                        .into(),
                ),
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["classifier.privileged_system".into()],
            },
        }
    );
}

#[tokio::test]
async fn bash_tool_pending_approval_and_denied_copy_stay_distinguishable() {
    let registry = ToolRegistry::new().register(Arc::new(BashTool));

    let approval = registry
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
        .expect("approval-shaped bash request should return a structured result");

    let ToolResult::PendingApproval {
        tool_name,
        message,
        approval,
    } = approval
    else {
        panic!("expected pending approval result for privileged bash request");
    };

    assert_eq!(tool_name, "Bash");
    assert!(
        message.starts_with("bash command warning ["),
        "approval copy should read like a warning, not an execution failure; message={message:?}"
    );
    assert!(
        message.contains("privileged_system"),
        "approval copy should expose the policy category; message={message:?}"
    );
    assert!(
        approval.summary.contains("pending approval"),
        "approval summary should tell the user this is awaiting approval; summary={:?}",
        approval.summary
    );
    assert!(
        approval.detail.as_deref().unwrap_or_default().contains("command"),
        "approval detail should preserve command-context reasoning; detail={:?}",
        approval.detail
    );
    assert_eq!(approval.approval_kind.as_deref(), Some("tool_permission"));
    assert!(
        approval
            .escalation_reasons
            .iter()
            .any(|reason| reason.contains("classifier.") || reason.contains("capability")),
        "approval payload should preserve next-step approval context; escalation_reasons={:?}",
        approval.escalation_reasons
    );

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
        .expect("denied bash request should return a structured result");

    let ToolResult::Denied(message) = denied else {
        panic!("expected denied result for plan-mode write bash request");
    };
    assert!(
        message.contains("not allowed in plan mode"),
        "denied copy should read as policy rejection, not command execution failure; message={message:?}"
    );
    assert!(
        !message.contains("exit_code")
            && !message.contains("stdout:")
            && !message.contains("stderr:"),
        "denied copy must not look like a command execution result; message={message:?}"
    );
    assert!(
        !message.starts_with("bash command warning ["),
        "denied copy must stay distinct from pending approval copy; message={message:?}"
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
    let result = registry
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input: "not-json".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("schema-backed tool should surface structured failure");

    let ToolResult::Interrupted(message) = result else {
        panic!("expected interrupted result");
    };
    assert!(message.contains("tool Edit requires JSON-structured input"));
}

#[tokio::test]
async fn registry_surfaces_unknown_tool_as_structured_failure() {
    let result = ToolRegistry::new()
        .invoke(
            &ToolCall {
                name: "MissingTool".into(),
                input: "{}".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("unknown tool should surface structured failure");

    let ToolResult::Interrupted(message) = result else {
        panic!("expected interrupted result");
    };
    assert!(message.contains("unknown tool MissingTool"));
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
fn delegated_write_path_allows_only_scoped_write_without_global_bypass() {
    let delegated = std::env::temp_dir()
        .join(format!(
            "delegated-write-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ))
        .join("report.md");
    let outside = delegated
        .parent()
        .expect("delegated path has parent")
        .join("outside.md");
    let permissions = ToolPermissionContext::new(PermissionMode::Default);
    permissions.add_delegated_write_path(&delegated);
    let metadata = FileWriteTool.metadata();

    let delegated_decision = evaluate_tool_permission(
        &metadata,
        &ToolCall::new(
            "Write",
            serde_json::json!({
                "file_path": delegated,
                "content": "ok"
            })
            .to_string(),
        ),
        &permissions,
    );
    assert_eq!(delegated_decision, PermissionDecision::Allow);

    let outside_decision = evaluate_tool_permission(
        &metadata,
        &ToolCall::new(
            "Write",
            serde_json::json!({
                "file_path": outside,
                "content": "no"
            })
            .to_string(),
        ),
        &permissions,
    );
    assert!(matches!(outside_decision, PermissionDecision::Ask { .. }));
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
    assert!(names.contains(&"Bash"));
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
    assert!(names.contains(&"Bash"));
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
fn tool_registry_resolves_tool_aliases_in_find_and_worker_allowlist() {
    let registry = ToolRegistry::new()
        .register(Arc::new(FileReadTool))
        .register(Arc::new(FileWriteTool));

    let alias_call = ToolCall {
        name: "FileWrite".into(),
        input: "payload".into(),
    };
    let resolved = registry.find(&alias_call).expect("alias should resolve");
    assert_eq!(resolved.metadata().name, "Write");

    let worker = registry.assemble_worker_registry(Some(&["FileWrite".to_string()]));
    let names = worker
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["Write"]);
}

#[test]
fn builtins_preserve_search_or_read_metadata_classification() {
    assert!(FileReadTool.metadata().is_search_or_read_command);
    assert!(GlobTool.metadata().is_search_or_read_command);
    assert!(GrepTool.metadata().is_search_or_read_command);
    assert!(WebSearchTool.metadata().is_search_or_read_command);
    assert!(!FileWriteTool.metadata().is_search_or_read_command);
    assert!(!FileEditTool.metadata().is_search_or_read_command);
    assert!(!BashTool.metadata().is_search_or_read_command);
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
        boss_actor_policy: None,
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
    assert!(!cli_headless_names.contains(&"AskUserQuestion"));
    assert!(cli_headless_names.contains(&"Bash"));
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
        boss_actor_policy: None,
    });
    let restricted_names = restricted
        .all_metadata()
        .iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(!restricted_names.contains(&"OpenWorldAlwaysLoaded"));
}

#[test]
fn v1_default_coding_model_surface_stays_local_and_core() {
    let registry = ToolRegistry::new()
        .register(Arc::new(AskUserQuestionTool))
        .register(Arc::new(BashTool))
        .register(Arc::new(FileEditTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(FileWriteTool))
        .register(Arc::new(GlobTool))
        .register(Arc::new(GrepTool))
        .register(Arc::new(NotebookEditTool))
        .register(Arc::new(WebFetchTool))
        .register(Arc::new(WebSearchTool));

    let context = ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Headless);
    let assembled = registry.assemble(context);
    let visible_model_tool_names = assembled
        .visible_model_tools(&context.permission_context(PermissionMode::Default))
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    for required in ["Read", "Edit", "Write", "Bash", "Grep", "Glob"] {
        assert!(
            visible_model_tool_names.iter().any(|name| name == required),
            "V1 default coding surface is missing required local tool {required}; visible={visible_model_tool_names:?}"
        );
    }

    for excluded in ["WebSearch", "WebFetch", "AskUserQuestion", "NotebookEdit"] {
        assert!(
            !visible_model_tool_names.iter().any(|name| name == excluded),
            "V1 default coding surface leaked non-core tool {excluded}; visible={visible_model_tool_names:?}"
        );
    }
}

#[test]
fn v1_default_coding_model_surface_has_stable_complete_model_tool_contract() {
    let registry = ToolRegistry::new()
        .register(Arc::new(AskUserQuestionTool))
        .register(Arc::new(BashTool))
        .register(Arc::new(FileEditTool))
        .register(Arc::new(FileReadTool))
        .register(Arc::new(FileWriteTool))
        .register(Arc::new(GlobTool))
        .register(Arc::new(GrepTool))
        .register(Arc::new(NotebookEditTool))
        .register(Arc::new(TaskCreateTool))
        .register(Arc::new(TaskGetTool))
        .register(Arc::new(TaskListTool))
        .register(Arc::new(TaskOutputTool))
        .register(Arc::new(TaskStopTool))
        .register(Arc::new(TaskUpdateTool))
        .register(Arc::new(TodoWriteTool))
        .register(Arc::new(ToolSearchTool))
        .register(Arc::new(WebFetchTool))
        .register(Arc::new(WebSearchTool));

    let context = ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Headless);
    let assembled = registry.assemble(context);
    let mut visible_model_tool_names = assembled
        .visible_model_tools(&context.permission_context(PermissionMode::Default))
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    visible_model_tool_names.sort();

    let expected_visible_model_tool_names = vec![
        "Bash".to_string(),
        "Edit".to_string(),
        "Glob".to_string(),
        "Grep".to_string(),
        "Read".to_string(),
        "Write".to_string(),
    ];

    assert_eq!(
        visible_model_tool_names, expected_visible_model_tool_names,
        "V1 default coding model-tool surface drifted; this test locks both the required local core tools and the current absence of deferred/non-schema tools"
    );

    for deferred in ["WebSearch", "WebFetch", "AskUserQuestion", "NotebookEdit"] {
        assert!(
            !visible_model_tool_names.iter().any(|name| name == deferred),
            "deferred tool {deferred} unexpectedly became model-visible; visible={visible_model_tool_names:?}"
        );
    }

    for non_model_builtin in [
        "TaskCreate",
        "TaskGet",
        "TaskList",
        "TaskOutput",
        "TaskStop",
        "TaskUpdate",
        "TodoWrite",
        "ToolSearch",
    ] {
        assert!(
            !visible_model_tool_names
                .iter()
                .any(|name| name == non_model_builtin),
            "builtin tool {non_model_builtin} unexpectedly entered the V1 model-tool surface; visible={visible_model_tool_names:?}"
        );
    }
}

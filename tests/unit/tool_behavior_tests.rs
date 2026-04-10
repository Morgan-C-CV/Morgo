use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::glob::GlobTool;
use rust_agent::tool::builtin::grep::GrepTool;
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

    let original = std::env::current_dir().expect("get current dir");
    std::env::set_current_dir(&dir).expect("enter temp dir");

    let result = GlobTool
        .invoke(
            &ToolCall {
                name: "Glob".into(),
                input: "*.rs".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("glob should succeed");

    std::env::set_current_dir(&original).expect("restore current dir");
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

    let original = std::env::current_dir().expect("get current dir");
    std::env::set_current_dir(&dir).expect("enter temp dir");

    let result = GrepTool
        .invoke(
            &ToolCall {
                name: "Grep".into(),
                input: "needle".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("grep should succeed");

    std::env::set_current_dir(&original).expect("restore current dir");
    fs::remove_dir_all(&dir).await.expect("cleanup dir");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("alpha.txt:2:needle here"));
    assert!(text.contains("nested/beta.txt:1:needle there too"));
}

#[tokio::test]
async fn tool_search_filters_catalog() {
    let result = ToolSearchTool
        .invoke(
            &ToolCall {
                name: "ToolSearch".into(),
                input: "read".into(),
            },
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("tool search should succeed");

    let ToolResult::Text(text) = result else {
        panic!("expected text result");
    };
    assert!(text.contains("Read - Read files from disk"));
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
        .register(Arc::new(FileReadTool))
        .register(Arc::new(WebFetchTool));

    let visible = registry.visible_tools(&ToolPermissionContext::new(PermissionMode::Default));
    let names = visible.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(names.contains(&"WebFetch"));
}

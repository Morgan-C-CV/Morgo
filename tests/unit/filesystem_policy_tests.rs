use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::security::filesystem_policy::{
    FilesystemAccessKind, FilesystemPermissionLevel, FilesystemPolicy, FilesystemPolicyConfig,
    FilesystemPolicyRule,
};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::file_edit::FileEditTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::file_write::FileWriteTool;
use rust_agent::tool::builtin::glob::GlobTool;
use rust_agent::tool::builtin::grep::GrepTool;
use rust_agent::tool::definition::{Tool, ToolCall, ToolResult};
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

fn policy_for(root: &PathBuf) -> FilesystemPolicy {
    FilesystemPolicy::from_config(FilesystemPolicyConfig {
        protected_paths: vec![root.join("protected").display().to_string()],
        rules: vec![
            FilesystemPolicyRule {
                path: root.join("allowed").display().to_string(),
                level: FilesystemPermissionLevel::Allow,
            },
            FilesystemPolicyRule {
                path: root.join("readonly").display().to_string(),
                level: FilesystemPermissionLevel::ReadOnly,
            },
        ],
    })
    .expect("policy should build")
}

#[test]
fn existing_path_read_is_allowed_in_read_only_directory() {
    let root = std::env::temp_dir().join(unique_name("fs-policy-readonly-read"));
    std::fs::create_dir_all(root.join("readonly")).expect("create readonly dir");
    std::fs::write(root.join("readonly").join("sample.txt"), "hello").expect("write file");

    let policy = policy_for(&root);
    let decision = policy.check_existing_path_for_read(&root.join("readonly").join("sample.txt"));
    assert!(decision.is_allowed());

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn existing_or_create_write_is_rejected_in_read_only_directory() {
    let root = std::env::temp_dir().join(unique_name("fs-policy-readonly-write"));
    std::fs::create_dir_all(root.join("readonly")).expect("create readonly dir");
    std::fs::write(root.join("readonly").join("sample.txt"), "hello").expect("write file");

    let policy = policy_for(&root);
    let decision = policy
        .check_existing_or_create_path_for_write(&root.join("readonly").join("sample.txt"));
    assert!(!decision.is_allowed());
    assert!(
        decision
            .deny_reason()
            .expect("deny reason")
            .contains("read_only")
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn protected_path_is_always_rejected() {
    let root = std::env::temp_dir().join(unique_name("fs-policy-protected"));
    std::fs::create_dir_all(root.join("protected")).expect("create protected dir");
    std::fs::write(root.join("protected").join("secret.txt"), "shh").expect("write file");

    let policy = policy_for(&root);
    let decision = policy.check_existing_or_create_path_for_write(
        &root.join("protected").join("secret.txt"),
    );
    assert!(!decision.is_allowed());
    assert!(
        decision
            .deny_reason()
            .expect("deny reason")
            .contains("protected path")
    );

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn create_target_uses_existing_parent_directory_rule() {
    let root = std::env::temp_dir().join(unique_name("fs-policy-create-parent"));
    std::fs::create_dir_all(root.join("allowed")).expect("create allowed dir");

    let policy = policy_for(&root);
    let decision = policy
        .check_existing_or_create_path_for_write(&root.join("allowed").join("new.txt"));
    assert!(decision.is_allowed());

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn existing_target_resolves_symlink_final_target() {
    let root = std::env::temp_dir().join(unique_name("fs-policy-symlink"));
    std::fs::create_dir_all(root.join("allowed")).expect("create allowed dir");
    std::fs::write(root.join("allowed").join("real.txt"), "hello").expect("write file");
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        root.join("allowed").join("real.txt"),
        root.join("allowed").join("link.txt"),
    )
    .expect("create symlink");

    let policy = policy_for(&root);
    #[cfg(unix)]
    {
        let decision = policy.check_existing_path_for_read(&root.join("allowed").join("link.txt"));
        assert!(decision.is_allowed());
    }

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn read_tool_allows_read_only_directory() {
    let root = std::env::temp_dir().join(unique_name("fs-tool-read"));
    fs::create_dir_all(root.join("readonly")).await.expect("create dir");
    let file = root.join("readonly").join("sample.txt");
    fs::write(&file, "hello policy").await.expect("write file");
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_filesystem_policy(std::sync::Arc::new(policy_for(&root)));

    let result = FileReadTool
        .invoke(
            &ToolCall {
                name: "Read".into(),
                input: file.to_string_lossy().into_owned(),
            },
            &permissions,
        )
        .await
        .expect("read should succeed");

    assert_eq!(result, ToolResult::Text("hello policy".into()));
    fs::remove_dir_all(root).await.expect("cleanup");
}

#[tokio::test]
async fn write_tool_rejects_read_only_directory() {
    let root = std::env::temp_dir().join(unique_name("fs-tool-write-readonly"));
    fs::create_dir_all(root.join("readonly")).await.expect("create dir");
    let file = root.join("readonly").join("sample.txt");
    fs::write(&file, "hello policy").await.expect("write file");
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_filesystem_policy(std::sync::Arc::new(policy_for(&root)));

    let error = FileWriteTool
        .invoke(
            &ToolCall {
                name: "Write".into(),
                input: serde_json::json!({
                    "file_path": file.to_string_lossy(),
                    "content": "changed"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect_err("write should be denied");

    assert!(error.to_string().contains("read_only"));
    fs::remove_dir_all(root).await.expect("cleanup");
}

#[tokio::test]
async fn edit_tool_rejects_protected_path() {
    let root = std::env::temp_dir().join(unique_name("fs-tool-edit-protected"));
    fs::create_dir_all(root.join("protected")).await.expect("create dir");
    let file = root.join("protected").join("sample.txt");
    fs::write(&file, "before").await.expect("write file");
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_filesystem_policy(std::sync::Arc::new(policy_for(&root)));

    let error = FileEditTool
        .invoke(
            &ToolCall {
                name: "Edit".into(),
                input: serde_json::json!({
                    "file_path": file.to_string_lossy(),
                    "old_string": "before",
                    "new_string": "after"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect_err("edit should be denied");

    assert!(error.to_string().contains("protected path"));
    fs::remove_dir_all(root).await.expect("cleanup");
}

#[tokio::test]
async fn write_tool_uses_parent_rule_for_new_file() {
    let root = std::env::temp_dir().join(unique_name("fs-tool-write-create"));
    fs::create_dir_all(root.join("allowed")).await.expect("create dir");
    let file = root.join("allowed").join("new.txt");
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_filesystem_policy(std::sync::Arc::new(policy_for(&root)));

    let result = FileWriteTool
        .invoke(
            &ToolCall {
                name: "Write".into(),
                input: serde_json::json!({
                    "file_path": file.to_string_lossy(),
                    "content": "created"
                })
                .to_string(),
            },
            &permissions,
        )
        .await
        .expect("write should succeed");

    assert_eq!(result, ToolResult::Text(format!("wrote {}", file.display())));
    fs::remove_dir_all(root).await.expect("cleanup");
}

#[tokio::test]
async fn glob_tool_rejects_policy_external_matches() {
    let root = std::env::temp_dir().join(unique_name("fs-tool-glob"));
    fs::create_dir_all(root.join("allowed")).await.expect("create allowed");
    fs::create_dir_all(root.join("outside")).await.expect("create outside");
    fs::write(root.join("allowed").join("ok.rs"), "fn ok() {}")
        .await
        .expect("write ok");
    fs::write(root.join("outside").join("bad.rs"), "fn bad() {}")
        .await
        .expect("write bad");

    let dir_for_call = root.clone();
    let policy = std::sync::Arc::new(FilesystemPolicy::from_config(FilesystemPolicyConfig {
        protected_paths: vec![],
        rules: vec![FilesystemPolicyRule {
            path: root.join("allowed").display().to_string(),
            level: FilesystemPermissionLevel::Allow,
        }],
    })
    .expect("policy should build"));

    let error = tokio::task::spawn_blocking(move || {
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
                    &ToolPermissionContext::new(PermissionMode::Default)
                        .with_filesystem_policy(policy),
                )
                .await
        });

        std::env::set_current_dir(&original).expect("restore current dir");
        result
    })
    .await
    .expect("join glob task")
    .expect_err("glob should be denied");

    assert!(error.to_string().contains("no matching rule"));
    fs::remove_dir_all(root).await.expect("cleanup");
}

#[tokio::test]
async fn grep_tool_rejects_policy_external_matches() {
    let root = std::env::temp_dir().join(unique_name("fs-tool-grep"));
    fs::create_dir_all(root.join("allowed")).await.expect("create allowed");
    fs::create_dir_all(root.join("outside")).await.expect("create outside");
    fs::write(root.join("allowed").join("ok.txt"), "needle here")
        .await
        .expect("write ok");
    fs::write(root.join("outside").join("bad.txt"), "needle there")
        .await
        .expect("write bad");

    let dir_for_call = root.clone();
    let policy = std::sync::Arc::new(FilesystemPolicy::from_config(FilesystemPolicyConfig {
        protected_paths: vec![],
        rules: vec![FilesystemPolicyRule {
            path: root.join("allowed").display().to_string(),
            level: FilesystemPermissionLevel::Allow,
        }],
    })
    .expect("policy should build"));

    let error = tokio::task::spawn_blocking(move || {
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
                    &ToolPermissionContext::new(PermissionMode::Default)
                        .with_filesystem_policy(policy),
                )
                .await
        });

        std::env::set_current_dir(&original).expect("restore current dir");
        result
    })
    .await
    .expect("join grep task")
    .expect_err("grep should be denied");

    assert!(error.to_string().contains("no matching rule"));
    fs::remove_dir_all(root).await.expect("cleanup");
}

#[test]
fn discovered_paths_search_requires_all_paths_within_policy() {
    let root = std::env::temp_dir().join(unique_name("fs-policy-discovered"));
    std::fs::create_dir_all(root.join("allowed")).expect("create allowed dir");
    std::fs::create_dir_all(root.join("outside")).expect("create outside dir");
    std::fs::write(root.join("allowed").join("ok.txt"), "ok").expect("write ok");
    std::fs::write(root.join("outside").join("bad.txt"), "bad").expect("write bad");

    let policy = policy_for(&root);
    let decision = policy.check_discovered_paths_for_read(
        [
            root.join("allowed").join("ok.txt"),
            root.join("outside").join("bad.txt"),
        ],
        FilesystemAccessKind::Search,
    );
    assert!(!decision.is_allowed());

    std::fs::remove_dir_all(root).expect("cleanup");
}

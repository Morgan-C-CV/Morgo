use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::service::mcp::config::load_server_configs_from_root;

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

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    env_lock().lock().unwrap_or_else(|e| e.into_inner())
}

fn set_env(key: &str, value: &str) {
    // SAFETY: integration tests serialize environment mutation with a global mutex.
    unsafe { std::env::set_var(key, value) }
}

fn remove_env(key: &str) {
    // SAFETY: same serialization guarantee.
    unsafe { std::env::remove_var(key) }
}

/// Verifies that `resolve_config_root` returns the override path when
/// `RUST_AGENT_CONFIG_ROOT` is set to an absolute path.
#[test]
fn config_root_override_is_used_when_env_var_is_set() {
    let _guard = lock_env();

    let custom_root = unique_temp_path("config-root-override-test");
    fs::create_dir_all(&custom_root).expect("create custom config root");

    // Write an mcp_servers.json with a recognizable server id into the custom root.
    let mcp_json = r#"[{"id":"override-server","name":"override-server","command":"echo","args":[],"env":{}}]"#;
    fs::write(custom_root.join("mcp_servers.json"), mcp_json)
        .expect("write mcp_servers.json to custom root");

    set_env("RUST_AGENT_CONFIG_ROOT", custom_root.to_str().unwrap());

    let cwd = std::env::current_dir().expect("cwd");
    let resolved = rust_agent::bootstrap::config_root::resolve_config_root(&cwd)
        .expect("resolve_config_root should succeed");

    remove_env("RUST_AGENT_CONFIG_ROOT");
    fs::remove_dir_all(&custom_root).ok();

    assert_eq!(resolved, custom_root);
}

/// Verifies that MCP server configs are loaded from the override root, not cwd/.claude.
#[test]
fn mcp_config_loads_from_override_root() {
    let _guard = lock_env();

    let custom_root = unique_temp_path("mcp-config-root-test");
    fs::create_dir_all(&custom_root).expect("create custom config root");

    let mcp_json = r#"[{"id":"sentinel-server","name":"sentinel-server","command":"echo","args":[],"env":{}}]"#;
    fs::write(custom_root.join("mcp_servers.json"), mcp_json).expect("write mcp_servers.json");

    let result = load_server_configs_from_root(&custom_root);
    fs::remove_dir_all(&custom_root).ok();

    assert_eq!(result.configs.len(), 1);
    assert_eq!(result.configs[0].id, "sentinel-server");
}

/// Verifies that a relative RUST_AGENT_CONFIG_ROOT is rejected with an error.
#[test]
fn relative_config_root_is_rejected() {
    let _guard = lock_env();

    set_env("RUST_AGENT_CONFIG_ROOT", "relative/path");
    let cwd = std::env::current_dir().expect("cwd");
    let result = rust_agent::bootstrap::config_root::resolve_config_root(&cwd);
    remove_env("RUST_AGENT_CONFIG_ROOT");

    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("absolute"), "expected 'absolute' in: {msg}");
}

/// Verifies that when RUST_AGENT_CONFIG_ROOT is unset, cwd/.claude is used (no regression).
#[test]
fn unset_config_root_falls_back_to_cwd_dot_morgo() {
    let _guard = lock_env();

    remove_env("RUST_AGENT_CONFIG_ROOT");
    let cwd = PathBuf::from("/tmp/fake-project");
    let resolved = rust_agent::bootstrap::config_root::resolve_config_root(&cwd)
        .expect("resolve_config_root should succeed");

    assert_eq!(resolved, PathBuf::from("/tmp/fake-project/.morgo"));
}

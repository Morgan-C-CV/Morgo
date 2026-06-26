use std::path::{Path, PathBuf};

pub const PRIMARY_CONFIG_DIR: &str = ".morgo";
pub const LEGACY_CONFIG_DIR: &str = ".claude";

pub fn preferred_workspace_config_root(cwd: &Path) -> PathBuf {
    cwd.join(PRIMARY_CONFIG_DIR)
}

pub fn preferred_home_config_root() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let primary = PathBuf::from(&home).join(PRIMARY_CONFIG_DIR);
    if primary.exists() {
        return Some(primary);
    }

    let legacy = PathBuf::from(home).join(LEGACY_CONFIG_DIR);
    if legacy.exists() {
        return Some(legacy);
    }

    Some(primary)
}

pub fn is_managed_config_root(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|value| value.to_str()),
        Some(PRIMARY_CONFIG_DIR | LEGACY_CONFIG_DIR)
    )
}

/// Resolves the agent config root directory.
///
/// If `RUST_AGENT_CONFIG_ROOT` is set, it must be an absolute path — relative paths
/// are rejected with an error to prevent silent misconfiguration.
///
/// If unset, falls back to `cwd/.morgo`. Legacy `cwd/.claude` is never selected
/// implicitly; use `RUST_AGENT_CONFIG_ROOT` for a deliberate legacy override.
pub fn resolve_config_root(cwd: &Path) -> anyhow::Result<PathBuf> {
    if let Ok(raw) = std::env::var("RUST_AGENT_CONFIG_ROOT") {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            anyhow::bail!("RUST_AGENT_CONFIG_ROOT is set but empty");
        }
        let path = PathBuf::from(trimmed);
        if !path.is_absolute() {
            anyhow::bail!(
                "RUST_AGENT_CONFIG_ROOT must be an absolute path, got: {}",
                path.display()
            );
        }
        return Ok(path);
    }
    Ok(preferred_workspace_config_root(cwd))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        env_lock().lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn unset_falls_back_to_cwd_dot_morgo() {
        let _guard = lock_env();
        // SAFETY: serialized by env_lock.
        unsafe { std::env::remove_var("RUST_AGENT_CONFIG_ROOT") };
        let cwd = Path::new("/some/project");
        let root = resolve_config_root(cwd).unwrap();
        assert_eq!(root, Path::new("/some/project/.morgo"));
    }

    #[test]
    fn legacy_claude_dir_is_not_selected_implicitly() {
        let _guard = lock_env();
        unsafe { std::env::remove_var("RUST_AGENT_CONFIG_ROOT") };
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        std::fs::create_dir_all(cwd.join(".claude")).unwrap();

        let root = resolve_config_root(cwd).unwrap();

        assert_eq!(root, cwd.join(".morgo"));
    }

    #[test]
    fn absolute_override_is_used_verbatim() {
        let _guard = lock_env();
        // SAFETY: serialized by env_lock.
        unsafe { std::env::set_var("RUST_AGENT_CONFIG_ROOT", "/custom/config") };
        let cwd = Path::new("/some/project");
        let root = resolve_config_root(cwd).unwrap();
        unsafe { std::env::remove_var("RUST_AGENT_CONFIG_ROOT") };
        assert_eq!(root, Path::new("/custom/config"));
    }

    #[test]
    fn relative_path_is_rejected() {
        let _guard = lock_env();
        // SAFETY: serialized by env_lock.
        unsafe { std::env::set_var("RUST_AGENT_CONFIG_ROOT", "relative/path") };
        let cwd = Path::new("/some/project");
        let result = resolve_config_root(cwd);
        unsafe { std::env::remove_var("RUST_AGENT_CONFIG_ROOT") };
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("absolute"), "expected 'absolute' in: {msg}");
    }

    #[test]
    fn empty_value_is_rejected() {
        let _guard = lock_env();
        // SAFETY: serialized by env_lock.
        unsafe { std::env::set_var("RUST_AGENT_CONFIG_ROOT", "   ") };
        let cwd = Path::new("/some/project");
        let result = resolve_config_root(cwd);
        unsafe { std::env::remove_var("RUST_AGENT_CONFIG_ROOT") };
        assert!(result.is_err());
    }
}

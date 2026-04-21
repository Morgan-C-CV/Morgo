use std::path::{Path, PathBuf};

/// Resolves the agent config root directory.
///
/// If `RUST_AGENT_CONFIG_ROOT` is set, it must be an absolute path — relative paths
/// are rejected with an error to prevent silent misconfiguration.
///
/// If unset, falls back to `cwd/.claude` (existing behavior, unchanged).
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
    Ok(cwd.join(".claude"))
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
    fn unset_falls_back_to_cwd_dot_claude() {
        let _guard = lock_env();
        // SAFETY: serialized by env_lock.
        unsafe { std::env::remove_var("RUST_AGENT_CONFIG_ROOT") };
        let cwd = Path::new("/some/project");
        let root = resolve_config_root(cwd).unwrap();
        assert_eq!(root, Path::new("/some/project/.claude"));
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

use std::path::{Component, Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxConfig {
    pub enabled: bool,
    pub fail_if_unavailable: bool,
    pub allow_unsandboxed_commands: bool,
    pub filesystem: SandboxFilesystemConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxFilesystemConfig {
    pub allow_write: Vec<PathBuf>,
    pub deny_write: Vec<PathBuf>,
    pub deny_read: Vec<PathBuf>,
    pub allow_read: Vec<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RawSandboxConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_fail_if_unavailable")]
    fail_if_unavailable: bool,
    #[serde(default)]
    allow_unsandboxed_commands: bool,
    #[serde(default)]
    filesystem: RawSandboxFilesystemConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RawSandboxFilesystemConfig {
    #[serde(default)]
    allow_write: Vec<String>,
    #[serde(default)]
    deny_write: Vec<String>,
    #[serde(default)]
    deny_read: Vec<String>,
    #[serde(default)]
    allow_read: Vec<String>,
}

fn default_fail_if_unavailable() -> bool {
    true
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            fail_if_unavailable: true,
            allow_unsandboxed_commands: true,
            filesystem: SandboxFilesystemConfig::default(),
        }
    }
}

impl SandboxConfig {
    pub fn load_from_config_root(config_root: &Path, workspace: &Path) -> anyhow::Result<Self> {
        let path = config_root.join("sandbox.json");
        if !path.exists() {
            return Ok(Self::default_for_roots(config_root, workspace));
        }
        Self::load_from_path(&path, config_root, workspace)
    }

    pub fn load_from_path(
        path: &Path,
        config_root: &Path,
        workspace: &Path,
    ) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("failed to read sandbox config {}: {error}", path.display())
        })?;
        let raw: RawSandboxConfig = serde_json::from_str(&contents).map_err(|error| {
            anyhow::anyhow!("failed to parse sandbox config {}: {error}", path.display())
        })?;
        Self::from_raw(raw, config_root, workspace)
    }

    fn default_for_roots(config_root: &Path, workspace: &Path) -> Self {
        let mut config = Self::default();
        config.filesystem.deny_write = default_deny_write_paths(config_root, workspace);
        config
    }

    fn from_raw(
        raw: RawSandboxConfig,
        config_root: &Path,
        workspace: &Path,
    ) -> anyhow::Result<Self> {
        let mut filesystem = SandboxFilesystemConfig {
            allow_write: normalize_path_list(raw.filesystem.allow_write, config_root, workspace)?,
            deny_write: normalize_path_list(raw.filesystem.deny_write, config_root, workspace)?,
            deny_read: normalize_path_list(raw.filesystem.deny_read, config_root, workspace)?,
            allow_read: normalize_path_list(raw.filesystem.allow_read, config_root, workspace)?,
        };
        filesystem
            .deny_write
            .extend(default_deny_write_paths(config_root, workspace));
        sort_dedup_paths(&mut filesystem.allow_write);
        sort_dedup_paths(&mut filesystem.deny_write);
        sort_dedup_paths(&mut filesystem.deny_read);
        sort_dedup_paths(&mut filesystem.allow_read);

        Ok(Self {
            enabled: raw.enabled,
            fail_if_unavailable: raw.fail_if_unavailable,
            allow_unsandboxed_commands: raw.allow_unsandboxed_commands,
            filesystem,
        })
    }
}

fn normalize_path_list(
    paths: Vec<String>,
    config_root: &Path,
    workspace: &Path,
) -> anyhow::Result<Vec<PathBuf>> {
    paths
        .into_iter()
        .map(|path| normalize_config_path(&path, config_root, workspace))
        .collect()
}

pub fn normalize_config_path(
    raw: &str,
    config_root: &Path,
    workspace: &Path,
) -> anyhow::Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("sandbox path entry cannot be empty");
    }
    let path = PathBuf::from(trimmed);
    let absolute = if path == Path::new(".") {
        workspace.to_path_buf()
    } else if path.is_absolute() {
        path
    } else {
        config_root.join(path)
    };
    Ok(normalize_path_lexically(&absolute))
}

fn default_deny_write_paths(config_root: &Path, workspace: &Path) -> Vec<PathBuf> {
    [
        config_root.to_path_buf(),
        workspace.join(".morgo"),
        workspace.join(".claude").join("settings.json"),
        workspace.join(".claude").join("settings.local.json"),
        workspace.join(".claude").join("skills"),
        workspace.join(".claude").join("agents"),
        workspace.join(".claude").join("commands"),
    ]
    .into_iter()
    .map(|path| normalize_path_lexically(&path))
    .collect()
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn sort_dedup_paths(paths: &mut Vec<PathBuf>) {
    paths.sort();
    paths.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_defaults_disabled_with_default_denies() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let config = SandboxConfig::load_from_config_root(&temp.path().join(".morgo"), &workspace)
            .expect("load sandbox config");

        assert!(!config.enabled);
        assert!(config.fail_if_unavailable);
        assert!(config.allow_unsandboxed_commands);
        assert!(
            config
                .filesystem
                .deny_write
                .contains(&workspace.join(".morgo"))
        );
    }

    #[test]
    fn relative_paths_resolve_against_config_root_and_dot_resolves_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let config_root = temp.path().join(".morgo");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&config_root).unwrap();
        std::fs::write(
            config_root.join("sandbox.json"),
            r#"{
                "enabled": true,
                "allow_unsandboxed_commands": false,
                "filesystem": {
                    "allow_write": ["."],
                    "deny_write": ["skills"],
                    "deny_read": ["/private/secret"]
                }
            }"#,
        )
        .unwrap();

        let config = SandboxConfig::load_from_config_root(&config_root, &workspace).unwrap();

        assert!(config.enabled);
        assert!(!config.allow_unsandboxed_commands);
        assert_eq!(config.filesystem.allow_write, vec![workspace]);
        assert!(
            config
                .filesystem
                .deny_write
                .contains(&config_root.join("skills"))
        );
        assert!(
            config
                .filesystem
                .deny_read
                .contains(&PathBuf::from("/private/secret"))
        );
    }
}

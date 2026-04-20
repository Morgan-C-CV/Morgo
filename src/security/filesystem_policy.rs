use std::env;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemPermissionLevel {
    Allow,
    ReadOnly,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemPolicyRule {
    pub path: String,
    pub level: FilesystemPermissionLevel,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemPolicyConfig {
    #[serde(default)]
    pub protected_paths: Vec<String>,
    #[serde(default)]
    pub rules: Vec<FilesystemPolicyRule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilesystemAccessKind {
    Read,
    Write,
    Create,
    Search,
}

impl FilesystemAccessKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Create => "create",
            Self::Search => "search",
        }
    }

    fn allowed_by(self, level: FilesystemPermissionLevel) -> bool {
        match level {
            FilesystemPermissionLevel::Allow => true,
            FilesystemPermissionLevel::ReadOnly => {
                matches!(
                    self,
                    FilesystemAccessKind::Read | FilesystemAccessKind::Search
                )
            }
            FilesystemPermissionLevel::Deny => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilesystemPolicyDecision {
    Allow {
        matched_path: PathBuf,
        level: FilesystemPermissionLevel,
    },
    Deny {
        reason: String,
    },
}

impl FilesystemPolicyDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow { .. })
    }

    pub fn into_result(self) -> anyhow::Result<()> {
        match self {
            Self::Allow { .. } => Ok(()),
            Self::Deny { reason } => anyhow::bail!(reason),
        }
    }

    pub fn deny_reason(&self) -> Option<&str> {
        match self {
            Self::Allow { .. } => None,
            Self::Deny { reason } => Some(reason.as_str()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilesystemPolicy {
    protected_paths: Vec<PathBuf>,
    rules: Vec<NormalizedFilesystemPolicyRule>,
}

#[derive(Debug, Clone)]
struct NormalizedFilesystemPolicyRule {
    path: PathBuf,
    level: FilesystemPermissionLevel,
}

impl FilesystemPolicy {
    pub fn load_from_path(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!(
                "failed to read filesystem policy {}: {error}",
                path.display()
            )
        })?;
        let config: FilesystemPolicyConfig = serde_json::from_str(&contents).map_err(|error| {
            anyhow::anyhow!(
                "failed to parse filesystem policy {}: {error}",
                path.display()
            )
        })?;
        let base_dir = path.parent().unwrap_or_else(|| Path::new("/"));
        Self::from_config_with_base(config, base_dir)
    }

    pub fn from_config(config: FilesystemPolicyConfig) -> anyhow::Result<Self> {
        let cwd = env::current_dir().map_err(|error| {
            anyhow::anyhow!("failed to resolve cwd for filesystem policy: {error}")
        })?;
        Self::from_config_with_base(config, &cwd)
    }

    pub fn protected_path_match(&self, target: &Path) -> Option<&PathBuf> {
        self.protected_paths
            .iter()
            .find(|protected| target == protected.as_path() || target.starts_with(protected))
    }

    pub fn check_existing_target(
        &self,
        path: &Path,
        access: FilesystemAccessKind,
    ) -> FilesystemPolicyDecision {
        match resolve_existing_target(path) {
            Ok(target) => self.evaluate_target(&target, access),
            Err(error) => FilesystemPolicyDecision::Deny {
                reason: format!(
                    "filesystem policy could not resolve existing target {}: {error}",
                    path.display()
                ),
            },
        }
    }

    pub fn check_create_target(
        &self,
        path: &Path,
        access: FilesystemAccessKind,
    ) -> FilesystemPolicyDecision {
        match resolve_create_target(path) {
            Ok(target) => self.evaluate_target(&target, access),
            Err(error) => FilesystemPolicyDecision::Deny {
                reason: format!(
                    "filesystem policy could not resolve create target {}: {error}",
                    path.display()
                ),
            },
        }
    }

    pub fn match_rule(&self, target: &Path) -> Option<FilesystemPolicyRule> {
        self.match_normalized_rule(target)
            .map(|rule| FilesystemPolicyRule {
                path: rule.path.display().to_string(),
                level: rule.level,
            })
    }

    pub fn check_existing_path_for_read(&self, path: &Path) -> FilesystemPolicyDecision {
        self.check_existing_target(path, FilesystemAccessKind::Read)
    }

    pub fn check_existing_path_for_search(&self, path: &Path) -> FilesystemPolicyDecision {
        self.check_existing_target(path, FilesystemAccessKind::Search)
    }

    pub fn check_existing_or_create_path_for_write(&self, path: &Path) -> FilesystemPolicyDecision {
        if path.exists() {
            self.check_existing_target(path, FilesystemAccessKind::Write)
        } else {
            self.check_create_target(path, FilesystemAccessKind::Create)
        }
    }

    pub fn check_discovered_paths_for_read<I, P>(
        &self,
        paths: I,
        access: FilesystemAccessKind,
    ) -> FilesystemPolicyDecision
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        for path in paths {
            let decision = self.check_existing_target(path.as_ref(), access);
            if !decision.is_allowed() {
                return decision;
            }
        }
        FilesystemPolicyDecision::Allow {
            matched_path: PathBuf::from("<all_paths_within_policy>"),
            level: FilesystemPermissionLevel::Allow,
        }
    }

    fn from_config_with_base(
        config: FilesystemPolicyConfig,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        let mut protected_paths = config
            .protected_paths
            .into_iter()
            .map(|path| normalize_policy_path(&path, base_dir))
            .collect::<anyhow::Result<Vec<_>>>()?;
        protected_paths.sort_by(|left, right| {
            component_count(right)
                .cmp(&component_count(left))
                .then_with(|| left.cmp(right))
        });

        let mut rules = config
            .rules
            .into_iter()
            .map(|rule| {
                Ok(NormalizedFilesystemPolicyRule {
                    path: normalize_policy_path(&rule.path, base_dir)?,
                    level: rule.level,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        rules.sort_by(|left, right| {
            component_count(&right.path)
                .cmp(&component_count(&left.path))
                .then_with(|| left.path.cmp(&right.path))
        });

        Ok(Self {
            protected_paths,
            rules,
        })
    }

    fn evaluate_target(
        &self,
        target: &Path,
        access: FilesystemAccessKind,
    ) -> FilesystemPolicyDecision {
        if let Some(protected) = self.protected_path_match(target) {
            return FilesystemPolicyDecision::Deny {
                reason: format!(
                    "filesystem policy denied {} on protected path {}",
                    access.as_str(),
                    protected.display()
                ),
            };
        }

        let Some(rule) = self.match_normalized_rule(target) else {
            return FilesystemPolicyDecision::Deny {
                reason: format!(
                    "filesystem policy denied {} on {}: no matching rule",
                    access.as_str(),
                    target.display()
                ),
            };
        };

        if access.allowed_by(rule.level) {
            FilesystemPolicyDecision::Allow {
                matched_path: rule.path.clone(),
                level: rule.level,
            }
        } else {
            FilesystemPolicyDecision::Deny {
                reason: format!(
                    "filesystem policy denied {} on {}: rule {} is {}",
                    access.as_str(),
                    target.display(),
                    rule.path.display(),
                    permission_level_name(rule.level)
                ),
            }
        }
    }

    fn match_normalized_rule(&self, target: &Path) -> Option<&NormalizedFilesystemPolicyRule> {
        self.rules
            .iter()
            .find(|rule| target == rule.path.as_path() || target.starts_with(&rule.path))
    }
}

fn permission_level_name(level: FilesystemPermissionLevel) -> &'static str {
    match level {
        FilesystemPermissionLevel::Allow => "allow",
        FilesystemPermissionLevel::ReadOnly => "read_only",
        FilesystemPermissionLevel::Deny => "deny",
    }
}

fn resolve_existing_target(path: &Path) -> anyhow::Result<PathBuf> {
    let absolute = absolutize_input_path(path)?;
    std::fs::canonicalize(&absolute).map_err(|error| {
        anyhow::anyhow!(
            "failed to canonicalize existing target {}: {error}",
            absolute.display()
        )
    })
}

fn resolve_create_target(path: &Path) -> anyhow::Result<PathBuf> {
    let absolute = absolutize_input_path(path)?;
    let mut current = absolute.as_path();

    loop {
        if current.exists() {
            let canonical_base = std::fs::canonicalize(current).map_err(|error| {
                anyhow::anyhow!(
                    "failed to canonicalize existing ancestor {}: {error}",
                    current.display()
                )
            })?;
            let remainder = absolute.strip_prefix(current).map_err(|error| {
                anyhow::anyhow!(
                    "failed to derive create remainder for {} from {}: {error}",
                    absolute.display(),
                    current.display()
                )
            })?;
            return Ok(normalize_join(&canonical_base, remainder));
        }

        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }

    Err(anyhow::anyhow!(
        "no existing ancestor found for create target {}",
        absolute.display()
    ))
}

fn normalize_policy_path(raw: &str, base_dir: &Path) -> anyhow::Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("filesystem policy path cannot be empty")
    }
    let expanded = expand_home(trimmed)?;
    let path = PathBuf::from(expanded);
    let absolute = if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    };
    if absolute.exists() {
        return std::fs::canonicalize(&absolute).map_err(|error| {
            anyhow::anyhow!(
                "failed to canonicalize filesystem policy path {}: {error}",
                absolute.display()
            )
        });
    }
    Ok(normalize_path_lexically(&absolute))
}

fn absolutize_input_path(path: &Path) -> anyhow::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| anyhow::anyhow!("failed to resolve cwd: {error}"))?
            .join(path)
    };
    Ok(normalize_path_lexically(&absolute))
}

fn expand_home(raw: &str) -> anyhow::Result<String> {
    if raw == "~" {
        let home = env::var("HOME")
            .map_err(|_| anyhow::anyhow!("filesystem policy path uses ~ but HOME is not set"))?;
        return Ok(home);
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        let home = env::var("HOME")
            .map_err(|_| anyhow::anyhow!("filesystem policy path uses ~ but HOME is not set"))?;
        return Ok(format!("{home}/{rest}"));
    }
    Ok(raw.to_string())
}

fn normalize_join(base: &Path, remainder: &Path) -> PathBuf {
    normalize_path_lexically(&base.join(remainder))
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn component_count(path: &Path) -> usize {
    path.components().count()
}

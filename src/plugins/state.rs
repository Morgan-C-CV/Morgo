use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::plugins::types::{PluginGovernanceSource, PluginGovernanceState};

#[derive(Debug, Clone)]
pub struct PluginStateLoadResult {
    pub path: PathBuf,
    pub source: PluginStateSource,
    pub states: BTreeMap<String, PluginGovernanceState>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginStateSource {
    Defaults,
    File,
}

impl PluginStateSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::File => "file",
        }
    }
}

pub fn plugin_state_path(cwd: &Path) -> PathBuf {
    cwd.join(".claude").join("plugin-state.json")
}

pub fn load_plugin_state_with_diagnostics(cwd: &Path) -> PluginStateLoadResult {
    load_plugin_state_from_root(&cwd.join(".claude"))
}

pub fn load_plugin_state_from_root(config_root: &Path) -> PluginStateLoadResult {
    let path = config_root.join("plugin-state.json");
    let mut diagnostics = Vec::new();

    match std::fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<Vec<PluginStateEntry>>(&raw) {
            Ok(entries) => PluginStateLoadResult {
                path,
                source: PluginStateSource::File,
                states: entries
                    .into_iter()
                    .map(|entry| {
                        (
                            entry.name,
                            PluginGovernanceState {
                                enabled: entry.enabled,
                                disable_reason: entry.reason,
                                source: PluginGovernanceSource::File,
                            },
                        )
                    })
                    .collect(),
                diagnostics,
            },
            Err(error) => {
                diagnostics.push(format!(
                    "Failed to parse .claude/plugin-state.json: {error}; using default plugin governance state."
                ));
                PluginStateLoadResult {
                    path,
                    source: PluginStateSource::Defaults,
                    states: BTreeMap::new(),
                    diagnostics,
                }
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            diagnostics.push(
                "No .claude/plugin-state.json found; using default enabled plugin governance state."
                    .to_string(),
            );
            PluginStateLoadResult {
                path,
                source: PluginStateSource::Defaults,
                states: BTreeMap::new(),
                diagnostics,
            }
        }
        Err(error) => {
            diagnostics.push(format!(
                "Failed to read .claude/plugin-state.json: {error}; using default plugin governance state."
            ));
            PluginStateLoadResult {
                path,
                source: PluginStateSource::Defaults,
                states: BTreeMap::new(),
                diagnostics,
            }
        }
    }
}

pub fn write_plugin_state(
    cwd: &Path,
    states: &BTreeMap<String, PluginGovernanceState>,
) -> anyhow::Result<PathBuf> {
    let path = plugin_state_path(cwd);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let entries = states
        .iter()
        .map(|(name, state)| PluginStateEntry {
            name: name.clone(),
            enabled: state.enabled,
            reason: state.disable_reason.clone(),
        })
        .collect::<Vec<_>>();
    let raw = serde_json::to_string_pretty(&entries)?;
    std::fs::write(&path, raw)?;
    Ok(path)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PluginStateEntry {
    name: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default)]
    reason: Option<String>,
}

fn default_enabled() -> bool {
    true
}

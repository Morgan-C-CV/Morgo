use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct McpGovernanceStateLoadResult {
    pub path: PathBuf,
    pub source: McpGovernanceStateSource,
    pub states: BTreeMap<String, McpGovernanceStateEntry>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpGovernanceStateSource {
    Defaults,
    File,
}

impl McpGovernanceStateSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::File => "file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct McpGovernanceStateEntry {
    pub server_id: String,
    #[serde(default)]
    pub approved: bool,
    pub fingerprint: u64,
    #[serde(default)]
    pub reason: Option<String>,
}

pub fn mcp_governance_state_path(cwd: &Path) -> PathBuf {
    cwd.join(".claude").join("mcp-governance.json")
}

pub fn load_mcp_governance_state_with_diagnostics(cwd: &Path) -> McpGovernanceStateLoadResult {
    load_mcp_governance_state_from_root(&cwd.join(".claude"))
}

pub fn load_mcp_governance_state_from_root(config_root: &Path) -> McpGovernanceStateLoadResult {
    let path = config_root.join("mcp-governance.json");
    let mut diagnostics = Vec::new();

    match std::fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<Vec<McpGovernanceStateEntry>>(&raw) {
            Ok(entries) => McpGovernanceStateLoadResult {
                path,
                source: McpGovernanceStateSource::File,
                states: entries
                    .into_iter()
                    .map(|entry| (entry.server_id.clone(), entry))
                    .collect(),
                diagnostics,
            },
            Err(error) => {
                diagnostics.push(format!(
                    "Failed to parse .claude/mcp-governance.json: {error}; using default MCP governance state."
                ));
                McpGovernanceStateLoadResult {
                    path,
                    source: McpGovernanceStateSource::Defaults,
                    states: BTreeMap::new(),
                    diagnostics,
                }
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            diagnostics.push(
                "No .claude/mcp-governance.json found; MCP servers require review by default."
                    .to_string(),
            );
            McpGovernanceStateLoadResult {
                path,
                source: McpGovernanceStateSource::Defaults,
                states: BTreeMap::new(),
                diagnostics,
            }
        }
        Err(error) => {
            diagnostics.push(format!(
                "Failed to read .claude/mcp-governance.json: {error}; using default MCP governance state."
            ));
            McpGovernanceStateLoadResult {
                path,
                source: McpGovernanceStateSource::Defaults,
                states: BTreeMap::new(),
                diagnostics,
            }
        }
    }
}

pub fn write_mcp_governance_state(
    cwd: &Path,
    states: &BTreeMap<String, McpGovernanceStateEntry>,
) -> anyhow::Result<PathBuf> {
    write_mcp_governance_state_to_path(&mcp_governance_state_path(cwd), states)
}

pub fn write_mcp_governance_state_from_root(
    config_root: &Path,
    states: &BTreeMap<String, McpGovernanceStateEntry>,
) -> anyhow::Result<PathBuf> {
    write_mcp_governance_state_to_path(&config_root.join("mcp-governance.json"), states)
}

fn write_mcp_governance_state_to_path(
    path: &Path,
    states: &BTreeMap<String, McpGovernanceStateEntry>,
) -> anyhow::Result<PathBuf> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let entries = states.values().cloned().collect::<Vec<_>>();
    let raw = serde_json::to_string_pretty(&entries)?;
    std::fs::write(path, raw)?;
    Ok(path.to_path_buf())
}

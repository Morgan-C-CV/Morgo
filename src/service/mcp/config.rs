use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::service::mcp::types::{McpServerConfig, McpTransportKind};

#[derive(Debug, Clone)]
pub struct McpConfigLoadResult {
    pub path: PathBuf,
    pub source: McpConfigSource,
    pub configs: Vec<McpServerConfig>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpConfigSource {
    Defaults,
    File,
}

impl McpConfigSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::File => "file",
        }
    }
}

pub fn load_server_configs(cwd: &Path) -> Vec<McpServerConfig> {
    load_server_configs_with_diagnostics(cwd).configs
}

pub fn load_server_configs_with_diagnostics(cwd: &Path) -> McpConfigLoadResult {
    load_server_configs_from_root(&cwd.join(".claude"))
}

pub fn load_server_configs_from_root(config_root: &Path) -> McpConfigLoadResult {
    let path = config_root.join("mcp_servers.json");
    let mut diagnostics = Vec::new();

    match std::fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<Vec<McpServerConfig>>(&raw) {
            Ok(configs) if !configs.is_empty() => McpConfigLoadResult {
                path,
                source: McpConfigSource::File,
                configs,
                diagnostics,
            },
            Ok(_) => {
                diagnostics.push("Config file was empty; using bundled defaults.".to_string());
                McpConfigLoadResult {
                    path,
                    source: McpConfigSource::Defaults,
                    configs: default_server_configs(),
                    diagnostics,
                }
            }
            Err(error) => {
                diagnostics.push(format!(
                    "Failed to parse .claude/mcp_servers.json: {error}; using bundled defaults."
                ));
                McpConfigLoadResult {
                    path,
                    source: McpConfigSource::Defaults,
                    configs: default_server_configs(),
                    diagnostics,
                }
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            diagnostics
                .push("No .claude/mcp_servers.json found; using bundled defaults.".to_string());
            McpConfigLoadResult {
                path,
                source: McpConfigSource::Defaults,
                configs: default_server_configs(),
                diagnostics,
            }
        }
        Err(error) => {
            diagnostics.push(format!(
                "Failed to read .claude/mcp_servers.json: {error}; using bundled defaults."
            ));
            McpConfigLoadResult {
                path,
                source: McpConfigSource::Defaults,
                configs: default_server_configs(),
                diagnostics,
            }
        }
    }
}

pub fn default_server_configs() -> Vec<McpServerConfig> {
    vec![McpServerConfig {
        id: "local-test".to_string(),
        name: "local-test".to_string(),
        command: "mock-mcp".to_string(),
        args: Vec::new(),
        env: BTreeMap::new(),
        transport: McpTransportKind::Mock,
        governance: Default::default(),
        connect_timeout_ms: 10_000,
    }]
}

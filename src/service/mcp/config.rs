use std::collections::BTreeMap;
use std::path::Path;

use crate::service::mcp::types::McpServerConfig;

pub fn load_server_configs(cwd: &Path) -> Vec<McpServerConfig> {
    let path = cwd.join(".claude").join("mcp_servers.json");
    let from_file = std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Vec<McpServerConfig>>(&raw).ok())
        .unwrap_or_default();
    if from_file.is_empty() {
        default_server_configs()
    } else {
        from_file
    }
}

pub fn default_server_configs() -> Vec<McpServerConfig> {
    vec![McpServerConfig {
        id: "local-test".to_string(),
        name: "local-test".to_string(),
        command: "mock-mcp".to_string(),
        args: Vec::new(),
        env: BTreeMap::new(),
    }]
}

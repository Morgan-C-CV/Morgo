use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Hash)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportKind {
    #[default]
    Mock,
    StdioProcess,
}

impl McpTransportKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::StdioProcess => "stdio_process",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub id: String,
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub transport: McpTransportKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum McpConnectionStatus {
    Disconnected,
    Connecting,
    Reconnecting,
    Connected,
    Failed,
}

impl McpConnectionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Reconnecting => "reconnecting",
            Self::Connected => "connected",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpToolInfo {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub input_schema: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpResourceInfo {
    pub name: String,
    pub uri: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpCapabilityEntry {
    #[serde(default)]
    pub declared: bool,
    #[serde(default)]
    pub details: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpCapabilities {
    #[serde(default)]
    pub tools: Option<McpCapabilityEntry>,
    #[serde(default)]
    pub resources: Option<McpCapabilityEntry>,
    #[serde(default)]
    pub prompts: Option<McpCapabilityEntry>,
    #[serde(default)]
    pub experimental: Option<McpCapabilityEntry>,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

impl McpCapabilities {
    pub fn from_initialize_result(value: Option<&Value>) -> Self {
        let Some(Value::Object(object)) = value else {
            return Self::default();
        };

        let mut capabilities = Self::default();
        for (key, value) in object {
            let normalized = normalize_capability_entry(value);
            match key.as_str() {
                "tools" => capabilities.tools = normalized,
                "resources" => capabilities.resources = normalized,
                "prompts" => capabilities.prompts = normalized,
                "experimental" => capabilities.experimental = normalized,
                other => {
                    capabilities.extensions.insert(other.to_string(), value.clone());
                }
            }
        }
        capabilities
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_none()
            && self.resources.is_none()
            && self.prompts.is_none()
            && self.experimental.is_none()
            && self.extensions.is_empty()
    }
}

fn normalize_capability_entry(value: &Value) -> Option<McpCapabilityEntry> {
    match value {
        Value::Null => None,
        Value::Object(details) => Some(McpCapabilityEntry {
            declared: true,
            details: details.clone(),
        }),
        _ => Some(McpCapabilityEntry {
            declared: true,
            details: Map::new(),
        }),
    }
}

impl std::fmt::Display for McpCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        push_capability_label(&mut parts, "tools", self.tools.as_ref());
        push_capability_label(&mut parts, "resources", self.resources.as_ref());
        push_capability_label(&mut parts, "prompts", self.prompts.as_ref());
        push_capability_label(&mut parts, "experimental", self.experimental.as_ref());
        if !self.extensions.is_empty() {
            let names = self.extensions.keys().cloned().collect::<Vec<_>>().join(",");
            parts.push(format!("extensions={names}"));
        }
        if parts.is_empty() {
            write!(f, "none")
        } else {
            write!(f, "{}", parts.join(", "))
        }
    }
}

fn push_capability_label(
    parts: &mut Vec<String>,
    label: &str,
    entry: Option<&McpCapabilityEntry>,
) {
    if let Some(entry) = entry {
        if entry.details.is_empty() {
            parts.push(label.to_string());
        } else {
            let detail_keys = entry.details.keys().cloned().collect::<Vec<_>>().join(",");
            parts.push(format!("{label}({detail_keys})"));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpPeerInfo {
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub protocol_version: Option<String>,
    pub capabilities: McpCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpConnectInfo {
    pub protocol_initialized: bool,
    pub pid: Option<u32>,
    pub peer: McpPeerInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerState {
    pub config: McpServerConfig,
    pub status: McpConnectionStatus,
    pub tool_count: usize,
    pub resource_count: usize,
    pub tool_names_preview: Vec<String>,
    pub resource_names_preview: Vec<String>,
    pub last_error: Option<String>,
    pub last_error_kind: Option<String>,
    pub last_error_detail: Option<String>,
    pub protocol_initialized: bool,
    pub pid: Option<u32>,
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub server_protocol_version: Option<String>,
    pub server_capabilities: McpCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpAction {
    ListTools,
    ListResources,
    CallTool,
    ReadResource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpRequest {
    pub action: McpAction,
    pub server: String,
    pub tool: Option<String>,
    pub resource: Option<String>,
    pub input: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum McpResponse {
    ToolList(Vec<McpToolInfo>),
    ResourceList(Vec<McpResourceInfo>),
    ToolResult(Value),
    ResourceContent(String),
}

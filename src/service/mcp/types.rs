use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    Connected,
    Failed,
}

impl McpConnectionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
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
pub struct McpPeerInfo {
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub protocol_version: Option<String>,
    pub capabilities: Option<Value>,
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
    pub last_error: Option<String>,
    pub protocol_initialized: bool,
    pub pid: Option<u32>,
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub server_protocol_version: Option<String>,
    pub server_capabilities: Option<Value>,
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

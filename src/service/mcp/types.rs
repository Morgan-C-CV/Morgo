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

fn default_connect_timeout_ms() -> u64 {
    10_000
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
    #[serde(default)]
    pub governance: McpServerGovernanceConfig,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Explicit proxy URL for HTTP-based transports (SSE). Ignored for stdio.
    #[serde(default)]
    pub proxy_url: Option<String>,
    /// Comma-separated no-proxy bypass list.
    #[serde(default)]
    pub no_proxy: Option<String>,
    /// Path to a PEM CA bundle for TLS verification on HTTP-based transports.
    #[serde(default)]
    pub ca_bundle_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerGovernanceConfig {
    #[serde(default = "default_review_required")]
    pub review_required: bool,
    #[serde(default)]
    pub notes: Option<String>,
}

impl Default for McpServerGovernanceConfig {
    fn default() -> Self {
        Self {
            review_required: true,
            notes: None,
        }
    }
}

fn default_review_required() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpServerRiskLevel {
    Low,
    Moderate,
    High,
}

impl McpServerRiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Moderate => "moderate",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerClassification {
    pub risk_level: McpServerRiskLevel,
    pub reasons: Vec<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpGovernanceApprovalStatus {
    NotReviewed,
    Approved,
    Denied,
    Stale,
}

impl McpGovernanceApprovalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotReviewed => "not_reviewed",
            Self::Approved => "approved",
            Self::Denied => "denied",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpGovernanceSource {
    Default,
    File,
}

impl McpGovernanceSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::File => "file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerRuntimeGovernance {
    pub classification: McpServerClassification,
    pub approval_status: McpGovernanceApprovalStatus,
    pub approval_source: McpGovernanceSource,
    pub approved_fingerprint: Option<u64>,
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
                    capabilities
                        .extensions
                        .insert(other.to_string(), value.clone());
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
            let names = self
                .extensions
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(",");
            parts.push(format!("extensions={names}"));
        }
        if parts.is_empty() {
            write!(f, "none")
        } else {
            write!(f, "{}", parts.join(", "))
        }
    }
}

fn push_capability_label(parts: &mut Vec<String>, label: &str, entry: Option<&McpCapabilityEntry>) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpOperationKind {
    Connect,
    Disconnect,
    ListTools,
    ListResources,
    CallTool,
    ReadResource,
    RequestValidation,
    ConfigLookup,
}

impl McpOperationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Disconnect => "disconnect",
            Self::ListTools => "list_tools",
            Self::ListResources => "list_resources",
            Self::CallTool => "call_tool",
            Self::ReadResource => "read_resource",
            Self::RequestValidation => "request_validation",
            Self::ConfigLookup => "config_lookup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpFailureCode {
    UnknownServer,
    MissingTool,
    MissingResource,
    Transport,
    Protocol,
    Execution,
    Inventory,
    RequestValidation,
    GovernanceReviewRequired,
    ConnectionTimeout,
    /// Subprocess failed to spawn (command not found, permission denied, etc.).
    ProcessStartup,
    /// Server rejected the connection due to authentication / authorization.
    AuthFailure,
    /// Server config is malformed or missing required fields.
    ConfigurationError,
    /// Governance fingerprint changed since last approval — re-review required.
    StaleGovernance,
}

impl McpFailureCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UnknownServer => "unknown_server",
            Self::MissingTool => "missing_tool",
            Self::MissingResource => "missing_resource",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
            Self::Execution => "execution",
            Self::Inventory => "inventory",
            Self::RequestValidation => "request_validation",
            Self::GovernanceReviewRequired => "mcp_governance_review_required",
            Self::ConnectionTimeout => "connection_timeout",
            Self::ProcessStartup => "process_startup",
            Self::AuthFailure => "auth_failure",
            Self::ConfigurationError => "configuration_error",
            Self::StaleGovernance => "stale_governance",
        }
    }

    /// True if the failure is likely transient and worth retrying.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport | Self::ConnectionTimeout | Self::Inventory
        )
    }

    /// True if the failure requires user action before retrying.
    pub fn requires_user_action(&self) -> bool {
        matches!(
            self,
            Self::GovernanceReviewRequired
                | Self::StaleGovernance
                | Self::AuthFailure
                | Self::ConfigurationError
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpFailureNotice {
    pub operation: McpOperationKind,
    pub code: McpFailureCode,
    pub detail: Option<String>,
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
    pub last_failure: Option<McpFailureNotice>,
    pub protocol_initialized: bool,
    pub pid: Option<u32>,
    pub server_name: Option<String>,
    pub server_version: Option<String>,
    pub server_protocol_version: Option<String>,
    pub server_capabilities: McpCapabilities,
    pub governance: McpServerRuntimeGovernance,
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

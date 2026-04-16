use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::service::mcp::client::{McpClient, RoutingMcpClient};
use crate::service::mcp::config::{McpConfigLoadResult, McpConfigSource, default_server_configs};
use crate::service::mcp::types::{
    McpAction, McpConnectInfo, McpConnectionStatus, McpRequest, McpResourceInfo, McpResponse,
    McpServerConfig, McpServerState, McpToolInfo,
};

#[derive(Clone)]
pub struct McpRuntime {
    client: Arc<dyn McpClient>,
    servers: Arc<RwLock<Vec<McpServerState>>>,
    cached_tools: Arc<RwLock<BTreeMap<String, Vec<McpToolInfo>>>>,
    cached_resources: Arc<RwLock<BTreeMap<String, Vec<McpResourceInfo>>>>,
    config_fingerprints: Arc<RwLock<BTreeMap<String, u64>>>,
    config_load_result: Arc<McpConfigLoadResult>,
}

impl std::fmt::Debug for McpRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpRuntime")
            .field("server_count", &self.server_count())
            .finish()
    }
}

impl Default for McpRuntime {
    fn default() -> Self {
        Self::new_with_config_result(
            Arc::new(RoutingMcpClient::default()),
            McpConfigLoadResult {
                path: std::path::PathBuf::from(".claude/mcp_servers.json"),
                source: McpConfigSource::Defaults,
                configs: default_server_configs(),
                diagnostics: Vec::new(),
            },
        )
    }
}

impl McpRuntime {
    pub fn new(client: Arc<dyn McpClient>, configs: Vec<McpServerConfig>) -> Self {
        Self::new_with_config_result(
            client,
            McpConfigLoadResult {
                path: std::path::PathBuf::from(".claude/mcp_servers.json"),
                source: McpConfigSource::Defaults,
                configs,
                diagnostics: Vec::new(),
            },
        )
    }

    pub fn new_with_config_result(
        client: Arc<dyn McpClient>,
        config_load_result: McpConfigLoadResult,
    ) -> Self {
        let servers = config_load_result
            .configs
            .iter()
            .cloned()
            .map(|config| McpServerState {
                config,
                status: McpConnectionStatus::Disconnected,
                tool_count: 0,
                resource_count: 0,
                tool_names_preview: Vec::new(),
                resource_names_preview: Vec::new(),
                last_error: None,
                last_error_kind: None,
                last_error_detail: None,
                protocol_initialized: false,
                pid: None,
                server_name: None,
                server_version: None,
                server_protocol_version: None,
                server_capabilities: None,
            })
            .collect::<Vec<_>>();
        let fingerprints = config_load_result
            .configs
            .iter()
            .map(|config| (config.id.clone(), fingerprint_config(config)))
            .collect::<BTreeMap<_, _>>();
        Self {
            client,
            servers: Arc::new(RwLock::new(servers)),
            cached_tools: Arc::new(RwLock::new(BTreeMap::new())),
            cached_resources: Arc::new(RwLock::new(BTreeMap::new())),
            config_fingerprints: Arc::new(RwLock::new(fingerprints)),
            config_load_result: Arc::new(config_load_result),
        }
    }

    pub async fn list_servers(&self) -> Vec<McpServerState> {
        self.servers.read().await.clone()
    }

    pub fn config_load_result(&self) -> Arc<McpConfigLoadResult> {
        self.config_load_result.clone()
    }

    pub async fn reconnect(&self, server: &str) -> anyhow::Result<McpServerState> {
        self.set_status(server, McpConnectionStatus::Reconnecting, None, None, None)
            .await?;
        self.invalidate_server_cache(server).await;
        let _ = self.disconnect(server).await;
        self.connect(server).await
    }

    pub async fn connect(&self, server: &str) -> anyhow::Result<McpServerState> {
        self.refresh_stale_server_config(server).await?;
        let config = self.server_config(server).await?;
        self.set_status(server, McpConnectionStatus::Connecting, None, None, None)
            .await?;
        let connect_info = match self.client.connect(&config).await {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_server(
                        server,
                        "connect",
                        error.to_string(),
                        Some(error.to_string()),
                    )
                    .await;
            }
        };
        let tools = match self.client.list_tools(&config).await {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_server(
                        server,
                        "connect",
                        error.to_string(),
                        Some(error.to_string()),
                    )
                    .await;
            }
        };
        let resources = match self.client.list_resources(&config).await {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_server(
                        server,
                        "connect",
                        error.to_string(),
                        Some(error.to_string()),
                    )
                    .await;
            }
        };
        self.cached_tools
            .write()
            .await
            .insert(config.id.clone(), tools.clone());
        self.cached_resources
            .write()
            .await
            .insert(config.id.clone(), resources.clone());
        self.update_connected(server, &tools, &resources, connect_info)
            .await
    }

    pub async fn disconnect(&self, server: &str) -> anyhow::Result<McpServerState> {
        self.refresh_stale_server_config(server).await?;
        let config = self.server_config(server).await?;
        if let Err(error) = self.client.disconnect(&config).await {
            return self
                .fail_server(
                    server,
                    "disconnect",
                    error.to_string(),
                    Some(error.to_string()),
                )
                .await;
        }
        self.invalidate_server_cache(server).await;
        self.update_disconnected(server).await
    }

    pub async fn dispatch(&self, request: McpRequest) -> anyhow::Result<McpResponse> {
        self.refresh_stale_server_config(&request.server).await?;
        self.ensure_connected(&request.server).await?;
        let config = self.server_config(&request.server).await?;
        match request.action {
            McpAction::ListTools => {
                if let Some(tools) = self.cached_tools.read().await.get(&config.id).cloned() {
                    return Ok(McpResponse::ToolList(tools));
                }
                match self.client.list_tools(&config).await {
                    Ok(tools) => {
                        self.cached_tools
                            .write()
                            .await
                            .insert(config.id.clone(), tools.clone());
                        self.update_inventory_counts(&request.server, Some(&tools), None)
                            .await?;
                        self.clear_last_error(&request.server).await?;
                        Ok(McpResponse::ToolList(tools))
                    }
                    Err(error) => {
                        self.fail_server(
                            &request.server,
                            "list_tools",
                            error.to_string(),
                            Some(error.to_string()),
                        )
                        .await
                    }
                }
            }
            McpAction::ListResources => {
                if let Some(resources) = self.cached_resources.read().await.get(&config.id).cloned()
                {
                    return Ok(McpResponse::ResourceList(resources));
                }
                match self.client.list_resources(&config).await {
                    Ok(resources) => {
                        self.cached_resources
                            .write()
                            .await
                            .insert(config.id.clone(), resources.clone());
                        self.update_inventory_counts(&request.server, None, Some(&resources))
                            .await?;
                        self.clear_last_error(&request.server).await?;
                        Ok(McpResponse::ResourceList(resources))
                    }
                    Err(error) => {
                        self.fail_server(
                            &request.server,
                            "list_resources",
                            error.to_string(),
                            Some(error.to_string()),
                        )
                        .await
                    }
                }
            }
            McpAction::CallTool => {
                let tool = request
                    .tool
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("tool is required for call_tool"))?;
                match self.client.call_tool(&config, tool, request.input).await {
                    Ok(value) => {
                        self.clear_last_error(&request.server).await?;
                        Ok(McpResponse::ToolResult(value))
                    }
                    Err(error) => {
                        self.fail_server(
                            &request.server,
                            "call_tool",
                            error.to_string(),
                            Some(error.to_string()),
                        )
                        .await
                    }
                }
            }
            McpAction::ReadResource => {
                let resource = request
                    .resource
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("resource is required for read_resource"))?;
                match self.client.read_resource(&config, resource).await {
                    Ok(content) => {
                        self.clear_last_error(&request.server).await?;
                        Ok(McpResponse::ResourceContent(content))
                    }
                    Err(error) => {
                        self.fail_server(
                            &request.server,
                            "read_resource",
                            error.to_string(),
                            Some(error.to_string()),
                        )
                        .await
                    }
                }
            }
        }
    }

    pub fn server_count(&self) -> usize {
        self.servers.blocking_read().len()
    }

    async fn ensure_connected(&self, server: &str) -> anyhow::Result<()> {
        let state = self.find_server(server).await?;
        if matches!(state.status, McpConnectionStatus::Connected) {
            return Ok(());
        }
        self.connect(server).await.map(|_| ())
    }

    async fn find_server(&self, server: &str) -> anyhow::Result<McpServerState> {
        self.servers
            .read()
            .await
            .iter()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))
    }

    async fn server_config(&self, server: &str) -> anyhow::Result<McpServerConfig> {
        Ok(self.find_server(server).await?.config)
    }

    async fn set_status(
        &self,
        server: &str,
        status: McpConnectionStatus,
        last_error: Option<String>,
        last_error_kind: Option<String>,
        last_error_detail: Option<String>,
    ) -> anyhow::Result<McpServerState> {
        let mut servers = self.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))?;
        state.status = status;
        state.last_error = last_error;
        state.last_error_kind = last_error_kind;
        state.last_error_detail = last_error_detail;
        Ok(state.clone())
    }

    async fn clear_last_error(&self, server: &str) -> anyhow::Result<McpServerState> {
        let state = self.find_server(server).await?;
        self.set_status(server, state.status, None, None, None)
            .await
    }

    async fn fail_server<T>(
        &self,
        server: &str,
        kind: &str,
        message: String,
        detail: Option<String>,
    ) -> anyhow::Result<T> {
        self.invalidate_server_cache(server).await;
        let _ = self
            .set_status(
                server,
                McpConnectionStatus::Failed,
                Some(message.clone()),
                Some(kind.to_string()),
                detail,
            )
            .await;
        anyhow::bail!(message)
    }

    async fn update_connected(
        &self,
        server: &str,
        tools: &[McpToolInfo],
        resources: &[McpResourceInfo],
        connect_info: McpConnectInfo,
    ) -> anyhow::Result<McpServerState> {
        let mut servers = self.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))?;
        state.status = McpConnectionStatus::Connected;
        state.tool_count = tools.len();
        state.resource_count = resources.len();
        state.tool_names_preview = preview_names_from_tools(tools);
        state.resource_names_preview = preview_names_from_resources(resources);
        state.last_error = None;
        state.last_error_kind = None;
        state.last_error_detail = None;
        state.protocol_initialized = connect_info.protocol_initialized;
        state.pid = connect_info.pid;
        state.server_name = connect_info.peer.server_name;
        state.server_version = connect_info.peer.server_version;
        state.server_protocol_version = connect_info.peer.protocol_version;
        state.server_capabilities = connect_info.peer.capabilities;
        Ok(state.clone())
    }

    async fn update_inventory_counts(
        &self,
        server: &str,
        tools: Option<&[McpToolInfo]>,
        resources: Option<&[McpResourceInfo]>,
    ) -> anyhow::Result<McpServerState> {
        let mut servers = self.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))?;
        if let Some(tools) = tools {
            state.tool_count = tools.len();
            state.tool_names_preview = preview_names_from_tools(tools);
        }
        if let Some(resources) = resources {
            state.resource_count = resources.len();
            state.resource_names_preview = preview_names_from_resources(resources);
        }
        Ok(state.clone())
    }

    async fn update_disconnected(&self, server: &str) -> anyhow::Result<McpServerState> {
        let mut servers = self.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))?;
        state.status = McpConnectionStatus::Disconnected;
        state.tool_count = 0;
        state.resource_count = 0;
        state.tool_names_preview.clear();
        state.resource_names_preview.clear();
        state.last_error = None;
        state.last_error_kind = None;
        state.last_error_detail = None;
        state.protocol_initialized = false;
        state.pid = None;
        state.server_name = None;
        state.server_version = None;
        state.server_protocol_version = None;
        state.server_capabilities = None;
        Ok(state.clone())
    }

    async fn invalidate_server_cache(&self, server: &str) {
        if let Ok(config) = self.server_config(server).await {
            self.cached_tools.write().await.remove(&config.id);
            self.cached_resources.write().await.remove(&config.id);
        }
    }

    async fn refresh_stale_server_config(&self, server: &str) -> anyhow::Result<()> {
        let state = self.find_server(server).await?;
        let current_fingerprint = fingerprint_config(&state.config);
        let known_fingerprint = self
            .config_fingerprints
            .read()
            .await
            .get(&state.config.id)
            .copied();
        if known_fingerprint == Some(current_fingerprint) {
            return Ok(());
        }
        self.invalidate_server_cache(server).await;
        self.config_fingerprints
            .write()
            .await
            .insert(state.config.id.clone(), current_fingerprint);
        let _ = self.update_disconnected(server).await;
        Ok(())
    }
}

fn preview_names_from_tools(tools: &[McpToolInfo]) -> Vec<String> {
    tools.iter().take(3).map(|tool| tool.name.clone()).collect()
}

fn preview_names_from_resources(resources: &[McpResourceInfo]) -> Vec<String> {
    resources
        .iter()
        .take(3)
        .map(|resource| resource.name.clone())
        .collect()
}

pub async fn replace_runtime_server_config(
    runtime: &McpRuntime,
    config: McpServerConfig,
) -> anyhow::Result<()> {
    {
        let mut servers = runtime.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == config.id || entry.config.name == config.name)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {}", config.id))?;
        state.config = config.clone();
    }
    runtime.invalidate_server_cache(&config.id).await;
    let _ = runtime.update_disconnected(&config.id).await;
    runtime
        .config_fingerprints
        .write()
        .await
        .insert(config.id.clone(), fingerprint_config(&config));
    Ok(())
}

pub fn fingerprint_config(config: &McpServerConfig) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    config.id.hash(&mut hasher);
    config.name.hash(&mut hasher);
    config.command.hash(&mut hasher);
    config.args.hash(&mut hasher);
    config.transport.hash(&mut hasher);
    for (key, value) in &config.env {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::{McpRuntime, fingerprint_config, replace_runtime_server_config};
    use crate::service::mcp::client::{McpClient, MockMcpClient};
    use crate::service::mcp::types::{
        McpAction, McpConnectInfo, McpRequest, McpResourceInfo, McpServerConfig, McpToolInfo,
        McpTransportKind,
    };
    use async_trait::async_trait;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[derive(Debug, Default)]
    struct FailingListToolsClient;

    #[async_trait]
    impl McpClient for FailingListToolsClient {
        async fn connect(&self, _config: &McpServerConfig) -> anyhow::Result<McpConnectInfo> {
            Ok(McpConnectInfo::default())
        }

        async fn disconnect(&self, _config: &McpServerConfig) -> anyhow::Result<()> {
            Ok(())
        }

        async fn list_tools(&self, _config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
            anyhow::bail!("list_tools exploded")
        }

        async fn list_resources(
            &self,
            _config: &McpServerConfig,
        ) -> anyhow::Result<Vec<McpResourceInfo>> {
            Ok(Vec::new())
        }

        async fn call_tool(
            &self,
            _config: &McpServerConfig,
            _tool: &str,
            _input: Option<Value>,
        ) -> anyhow::Result<Value> {
            Ok(Value::Null)
        }

        async fn read_resource(
            &self,
            _config: &McpServerConfig,
            _resource: &str,
        ) -> anyhow::Result<String> {
            Ok(String::new())
        }
    }

    fn test_config(id: &str, name: &str) -> McpServerConfig {
        McpServerConfig {
            id: id.into(),
            name: name.into(),
            command: "mock-mcp".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            transport: McpTransportKind::Mock,
        }
    }

    #[tokio::test]
    async fn replace_runtime_server_config_updates_fingerprint_and_disconnects() {
        let config = test_config("local-test", "Local Test");
        let runtime = McpRuntime::new(Arc::new(MockMcpClient), vec![config.clone()]);
        runtime
            .connect("local-test")
            .await
            .expect("connect should succeed");

        let mut updated = config.clone();
        updated.command = "mock-mcp-updated".into();
        replace_runtime_server_config(&runtime, updated.clone())
            .await
            .expect("replace should succeed");

        let servers = runtime.list_servers().await;
        assert_eq!(servers[0].status.as_str(), "disconnected");
        assert!(servers[0].last_error.is_none());
        let fingerprints = runtime.config_fingerprints.read().await;
        assert_eq!(
            fingerprints.get("local-test").copied(),
            Some(fingerprint_config(&updated))
        );
    }

    #[tokio::test]
    async fn dispatch_failure_records_error_kind_and_detail() {
        let config = test_config("local-test", "Local Test");
        let runtime = McpRuntime::new(Arc::new(FailingListToolsClient), vec![config]);

        let error = runtime
            .dispatch(McpRequest {
                action: McpAction::ListTools,
                server: "local-test".into(),
                tool: None,
                resource: None,
                input: None,
            })
            .await
            .expect_err("list tools should fail");
        assert!(error.to_string().contains("list_tools exploded"));

        let servers = runtime.list_servers().await;
        assert_eq!(servers[0].last_error_kind.as_deref(), Some("connect"));
        assert_eq!(
            servers[0].last_error_detail.as_deref(),
            Some("list_tools exploded")
        );
    }
}

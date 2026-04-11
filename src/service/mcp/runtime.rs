use std::sync::Arc;

use tokio::sync::RwLock;

use crate::service::mcp::client::{McpClient, MockMcpClient};
use crate::service::mcp::config::default_server_configs;
use crate::service::mcp::types::{
    McpAction, McpConnectionStatus, McpRequest, McpResourceInfo, McpResponse, McpServerConfig,
    McpServerState, McpToolInfo,
};

#[derive(Clone)]
pub struct McpRuntime {
    client: Arc<dyn McpClient>,
    servers: Arc<RwLock<Vec<McpServerState>>>,
    cached_tools: Arc<RwLock<std::collections::BTreeMap<String, Vec<McpToolInfo>>>>,
    cached_resources: Arc<RwLock<std::collections::BTreeMap<String, Vec<McpResourceInfo>>>>,
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
        Self::new(Arc::new(MockMcpClient), default_server_configs())
    }
}

impl McpRuntime {
    pub fn new(client: Arc<dyn McpClient>, configs: Vec<McpServerConfig>) -> Self {
        let servers = configs
            .into_iter()
            .map(|config| McpServerState {
                config,
                status: McpConnectionStatus::Disconnected,
                tool_count: 0,
                resource_count: 0,
                last_error: None,
            })
            .collect();
        Self {
            client,
            servers: Arc::new(RwLock::new(servers)),
            cached_tools: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            cached_resources: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
        }
    }

    pub async fn list_servers(&self) -> Vec<McpServerState> {
        self.servers.read().await.clone()
    }

    pub async fn reconnect(&self, server: &str) -> anyhow::Result<McpServerState> {
        let _ = self.disconnect(server).await;
        self.connect(server).await
    }

    pub async fn connect(&self, server: &str) -> anyhow::Result<McpServerState> {
        let config = self.server_config(server).await?;
        self.set_status(server, McpConnectionStatus::Connecting, None).await?;
        if let Err(error) = self.client.connect(&config).await {
            let message = error.to_string();
            self.set_status(server, McpConnectionStatus::Failed, Some(message.clone()))
                .await?;
            anyhow::bail!(message);
        }
        let tools = match self.client.list_tools(&config).await {
            Ok(value) => value,
            Err(error) => {
                let message = error.to_string();
                self.set_status(server, McpConnectionStatus::Failed, Some(message.clone()))
                    .await?;
                anyhow::bail!(message);
            }
        };
        let resources = match self.client.list_resources(&config).await {
            Ok(value) => value,
            Err(error) => {
                let message = error.to_string();
                self.set_status(server, McpConnectionStatus::Failed, Some(message.clone()))
                    .await?;
                anyhow::bail!(message);
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
        self.update_connected(server, tools.len(), resources.len()).await
    }

    pub async fn disconnect(&self, server: &str) -> anyhow::Result<McpServerState> {
        let config = self.server_config(server).await?;
        self.client.disconnect(&config).await?;
        self.cached_tools.write().await.remove(&config.id);
        self.cached_resources.write().await.remove(&config.id);
        self.update_disconnected(server).await
    }

    pub async fn dispatch(&self, request: McpRequest) -> anyhow::Result<McpResponse> {
        self.ensure_connected(&request.server).await?;
        let config = self.server_config(&request.server).await?;
        match request.action {
            McpAction::ListTools => Ok(McpResponse::ToolList(self.client.list_tools(&config).await?)),
            McpAction::ListResources => Ok(McpResponse::ResourceList(
                self.client.list_resources(&config).await?,
            )),
            McpAction::CallTool => {
                let tool = request
                    .tool
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("tool is required for call_tool"))?;
                Ok(McpResponse::ToolResult(
                    self.client.call_tool(&config, tool, request.input).await?,
                ))
            }
            McpAction::ReadResource => {
                let resource = request
                    .resource
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("resource is required for read_resource"))?;
                Ok(McpResponse::ResourceContent(
                    self.client.read_resource(&config, resource).await?,
                ))
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
    ) -> anyhow::Result<McpServerState> {
        let mut servers = self.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))?;
        state.status = status;
        state.last_error = last_error;
        Ok(state.clone())
    }

    async fn update_connected(
        &self,
        server: &str,
        tool_count: usize,
        resource_count: usize,
    ) -> anyhow::Result<McpServerState> {
        let mut servers = self.servers.write().await;
        let state = servers
            .iter_mut()
            .find(|entry| entry.config.id == server || entry.config.name == server)
            .ok_or_else(|| anyhow::anyhow!("unknown MCP server: {server}"))?;
        state.status = McpConnectionStatus::Connected;
        state.tool_count = tool_count;
        state.resource_count = resource_count;
        state.last_error = None;
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
        state.last_error = None;
        Ok(state.clone())
    }
}

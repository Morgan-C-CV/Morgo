use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::service::mcp::types::{
    McpResourceInfo, McpServerConfig, McpToolInfo, McpTransportKind,
};

#[async_trait]
pub trait McpClient: Send + Sync {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<()>;
    async fn disconnect(&self, config: &McpServerConfig) -> anyhow::Result<()>;
    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>>;
    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>>;
    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value>;
    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String>;
}

#[derive(Debug, Default)]
pub struct RoutingMcpClient {
    mock: MockMcpClient,
    stdio: StdioProcessMcpClient,
}

impl RoutingMcpClient {
    fn client_for(&self, transport: McpTransportKind) -> &dyn McpClient {
        match transport {
            McpTransportKind::Mock => &self.mock,
            McpTransportKind::StdioProcess => &self.stdio,
        }
    }
}

#[async_trait]
impl McpClient for RoutingMcpClient {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        self.client_for(config.transport).connect(config).await
    }

    async fn disconnect(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        self.client_for(config.transport).disconnect(config).await
    }

    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        self.client_for(config.transport).list_tools(config).await
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        self.client_for(config.transport).list_resources(config).await
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        self.client_for(config.transport)
            .call_tool(config, tool, input)
            .await
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        self.client_for(config.transport)
            .read_resource(config, resource)
            .await
    }
}

#[derive(Debug, Default)]
pub struct MockMcpClient;

#[async_trait]
impl McpClient for MockMcpClient {
    async fn connect(&self, _config: &McpServerConfig) -> anyhow::Result<()> {
        Ok(())
    }

    async fn disconnect(&self, _config: &McpServerConfig) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        Ok(vec![McpToolInfo {
            name: format!("{}__echo", config.id),
            description: format!("Echo tool exposed by {}", config.name),
        }])
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        Ok(vec![McpResourceInfo {
            name: format!("{}-readme", config.id),
            uri: format!("mcp://{}/readme", config.id),
        }])
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        Ok(json!({
            "server": config.id,
            "tool": tool,
            "input": input,
            "ok": true,
            "transport": config.transport.as_str()
        }))
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        Ok(format!("resource:{}:{}", config.id, resource))
    }
}

#[derive(Debug, Default)]
pub struct StdioProcessMcpClient {
    processes: Arc<Mutex<BTreeMap<String, Child>>>,
}

#[async_trait]
impl McpClient for StdioProcessMcpClient {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        let mut processes = self.processes.lock().await;
        if processes.contains_key(&config.id) {
            return Ok(());
        }

        let mut command = Command::new(&config.command);
        command.args(&config.args);
        command.envs(&config.env);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let child = command.spawn().with_context(|| {
            format!(
                "Failed to spawn MCP stdio process '{}' for server {}",
                config.command, config.id
            )
        })?;
        processes.insert(config.id.clone(), child);
        Ok(())
    }

    async fn disconnect(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        let mut child = {
            let mut processes = self.processes.lock().await;
            processes.remove(&config.id)
        };
        if let Some(child) = child.as_mut() {
            child.kill().await.with_context(|| {
                format!("Failed to stop MCP stdio process for server {}", config.id)
            })?;
        }
        Ok(())
    }

    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        self.ensure_process(config).await?;
        Ok(vec![McpToolInfo {
            name: format!("{}__process_tool", config.id),
            description: format!(
                "Process-backed MCP placeholder exposed by {}",
                config.name
            ),
        }])
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        self.ensure_process(config).await?;
        Ok(vec![McpResourceInfo {
            name: format!("{}-process-status", config.id),
            uri: format!("mcp://{}/process-status", config.id),
        }])
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        self.ensure_process(config).await?;
        Ok(json!({
            "server": config.id,
            "tool": tool,
            "input": input,
            "ok": true,
            "transport": config.transport.as_str(),
            "note": "stdio transport process is connected; protocol exchange is not implemented yet"
        }))
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        self.ensure_process(config).await?;
        Ok(format!(
            "resource:{}:{}:{}",
            config.id,
            config.transport.as_str(),
            resource
        ))
    }
}

impl StdioProcessMcpClient {
    async fn ensure_process(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        self.connect(config).await
    }
}

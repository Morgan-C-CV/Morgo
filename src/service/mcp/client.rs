use async_trait::async_trait;
use serde_json::{json, Value};

use crate::service::mcp::types::{McpResourceInfo, McpServerConfig, McpToolInfo};

#[async_trait]
pub trait McpClient: Send + Sync {
    async fn connect(&self, _config: &McpServerConfig) -> anyhow::Result<()>;
    async fn disconnect(&self, _config: &McpServerConfig) -> anyhow::Result<()>;
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
            "ok": true
        }))
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        Ok(format!("resource:{}:{}", config.id, resource))
    }
}

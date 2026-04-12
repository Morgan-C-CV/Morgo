use async_trait::async_trait;

use crate::service::mcp::types::{McpRequest, McpResponse};
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct McpTool;

#[async_trait]
impl Tool for McpTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Mcp".into(),
            description: "Interact with configured MCP servers".into(),
            aliases: &["MCP"],
            search_hint: Some("model context protocol server tools and resources"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: false,
            should_defer: true,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: true,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let runtime = permissions
            .mcp_runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MCP runtime is unavailable in this session"))?;
        let request: McpRequest = serde_json::from_str(call.input.trim())?;
        let response = runtime.dispatch(request).await?;
        Ok(ToolResult::Text(render_response(response)))
    }
}

fn render_response(response: McpResponse) -> String {
    match response {
        McpResponse::ToolList(tools) => {
            let mut lines = vec!["MCP tools:".to_string()];
            for tool in tools {
                lines.push(format!("- {}: {}", tool.name, tool.description));
                if let Some(schema) = tool.input_schema.as_ref() {
                    lines.push(format!("  schema: {}", schema));
                }
            }
            lines.join("\n")
        }
        McpResponse::ResourceList(resources) => {
            let mut lines = vec!["MCP resources:".to_string()];
            for resource in resources {
                lines.push(format!("- {} ({})", resource.name, resource.uri));
                if !resource.description.trim().is_empty() {
                    lines.push(format!("  description: {}", resource.description));
                }
                if let Some(mime_type) = resource.mime_type.as_deref() {
                    lines.push(format!("  mime_type: {}", mime_type));
                }
            }
            lines.join("\n")
        }
        McpResponse::ToolResult(value) => format!("MCP tool result:\n{}", value),
        McpResponse::ResourceContent(content) => format!("MCP resource content:\n{}", content),
    }
}

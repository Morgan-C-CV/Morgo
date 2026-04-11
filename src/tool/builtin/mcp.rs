use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct McpTool;

#[async_trait]
impl Tool for McpTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Mcp",
            description: "Interact with configured MCP servers",
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
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let request = call.input.trim();
        Ok(ToolResult::Text(if request.is_empty() {
            "mcp request queued".into()
        } else {
            format!("mcp request queued: {request}")
        }))
    }
}

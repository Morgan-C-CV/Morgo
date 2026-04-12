use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct WebSearchTool;

#[async_trait]
impl Tool for WebSearchTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "WebSearch".into(),
            description: "Search the public web for current information".into(),
            aliases: &[],
            search_hint: Some("search internet or web results"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: false,
            should_defer: true,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: true,
            is_search_or_read_command: true,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let query = call.input.trim();
        if query.is_empty() {
            anyhow::bail!("search query cannot be empty");
        }
        Ok(ToolResult::Text(format!("web search queued: {query}")))
    }
}

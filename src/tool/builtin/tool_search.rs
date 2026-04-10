use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct ToolSearchTool;

#[async_trait]
impl Tool for ToolSearchTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "ToolSearch",
            description: "Search the available tool catalog",
            read_only: true,
            destructive: false,
            always_load: true,
            should_defer: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Text(format!(
            "tool search scaffold: {}",
            call.input
        )))
    }
}

use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GrepTool;

#[async_trait]
impl Tool for GrepTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Grep",
            description: "Search file contents by regex",
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
        Ok(ToolResult::Text(format!("grep scaffold: {}", call.input)))
    }
}

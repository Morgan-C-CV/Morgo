use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GlobTool;

#[async_trait]
impl Tool for GlobTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Glob",
            description: "Match file paths by glob pattern",
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
        Ok(ToolResult::Text(format!("glob scaffold: {}", call.input)))
    }
}

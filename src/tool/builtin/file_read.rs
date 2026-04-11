use std::path::Path;

use async_trait::async_trait;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileReadTool;

#[async_trait]
impl Tool for FileReadTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Read",
            description: "Read files from disk",
            aliases: &[],
            search_hint: Some("read file contents"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
        }
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        if call.input.trim().is_empty() {
            anyhow::bail!("read target cannot be empty")
        }
        Ok(())
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let path = Path::new(call.input.trim());
        let contents = fs::read_to_string(path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
        Ok(ToolResult::Text(contents))
    }
}

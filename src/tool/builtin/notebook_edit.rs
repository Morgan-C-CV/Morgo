use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct NotebookEditTool;

#[async_trait]
impl Tool for NotebookEditTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "NotebookEdit",
            description: "Edit a notebook cell by id or index",
            aliases: &[],
            search_hint: Some("edit jupyter notebook cell"),
            read_only: false,
            destructive: true,
            concurrency_safe: false,
            always_load: false,
            should_defer: true,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let input = call.input.trim();
        if input.is_empty() {
            anyhow::bail!("notebook edit input cannot be empty");
        }
        Ok(ToolResult::Text(format!("notebook edit queued: {input}")))
    }
}

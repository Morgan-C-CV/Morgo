use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileWriteTool;

#[derive(Debug, Deserialize)]
struct WriteInput {
    file_path: String,
    content: String,
}

fn parse_input(call: &ToolCall) -> anyhow::Result<WriteInput> {
    let json = call
        .json_input()
        .ok_or_else(|| anyhow::anyhow!("file write requires JSON input"))?;
    serde_json::from_value(json)
        .map_err(|error| anyhow::anyhow!("invalid file write input: {error}"))
}

#[async_trait]
impl Tool for FileWriteTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Write".into(),
            description: "Write file contents to disk".into(),
            aliases: &["FileWrite"],
            search_hint: Some("write or create file on disk"),
            read_only: false,
            destructive: true,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    fn input_schema(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "object",
            "required": ["file_path", "content"],
            "properties": {
                "file_path": {"type": "string"},
                "content": {"type": "string"}
            }
        }))
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let input = parse_input(call)?;
        let path = Path::new(input.file_path.trim());
        if let Some(policy) = permissions.filesystem_policy() {
            policy
                .check_existing_or_create_path_for_write(path)
                .into_result()?;
        }
        fs::write(path, input.content)
            .await
            .map_err(|error| anyhow::anyhow!("failed to write {}: {error}", path.display()))?;
        Ok(ToolResult::Text(format!("wrote {}", path.display())))
    }
}

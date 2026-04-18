use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileReadTool;

#[derive(Debug, Deserialize)]
struct ReadInput {
    file_path: String,
}

fn parse_input(call: &ToolCall) -> anyhow::Result<String> {
    if let Some(json) = call.json_input() {
        let input: ReadInput = serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid read input: {error}"))?;
        return Ok(input.file_path);
    }
    Ok(call.input.trim().to_string())
}

#[async_trait]
impl Tool for FileReadTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Read".into(),
            description: "Read files from disk".into(),
            aliases: &[],
            search_hint: Some("read file contents"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: true,
        }
    }

    fn input_schema(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "object",
            "required": ["file_path"],
            "properties": {
                "file_path": {"type": "string"}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        let path = parse_input(call)?;
        if path.trim().is_empty() {
            anyhow::bail!("read target cannot be empty")
        }
        Ok(())
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let raw_path = parse_input(call)?;
        let path = Path::new(raw_path.trim());
        if let Some(policy) = permissions.filesystem_policy() {
            policy.check_existing_path_for_read(path).into_result()?;
        }
        let contents = fs::read_to_string(path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
        Ok(ToolResult::Text(contents))
    }
}

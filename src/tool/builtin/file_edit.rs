use std::path::PathBuf;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileEditTool;

#[derive(Debug, Deserialize)]
struct EditInput {
    file_path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}

#[async_trait]
impl Tool for FileEditTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Edit",
            description: "Edit existing files with safety rails",
            aliases: &[],
            read_only: false,
            destructive: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
        }
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        let input = parse_input(&call.input)?;
        if input.file_path.trim().is_empty() {
            anyhow::bail!("edit target cannot be empty")
        }
        if input.old_string.is_empty() {
            anyhow::bail!("old_string cannot be empty")
        }
        if input.old_string == input.new_string {
            anyhow::bail!("new_string must differ from old_string")
        }
        Ok(())
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let input = parse_input(&call.input)?;
        let path = PathBuf::from(input.file_path.trim());
        let metadata = fs::metadata(&path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to access {}: {error}", path.display()))?;
        if !metadata.is_file() {
            anyhow::bail!("edit target is not a file: {}", path.display())
        }

        let original = fs::read_to_string(&path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;

        let occurrences = original.matches(&input.old_string).count();
        if occurrences == 0 {
            anyhow::bail!("old_string not found in {}", path.display())
        }
        if occurrences > 1 && !input.replace_all {
            anyhow::bail!(
                "old_string is not unique in {} ({} matches)",
                path.display(),
                occurrences
            )
        }

        let updated = if input.replace_all {
            original.replace(&input.old_string, &input.new_string)
        } else {
            original.replacen(&input.old_string, &input.new_string, 1)
        };

        fs::write(&path, updated)
            .await
            .map_err(|error| anyhow::anyhow!("failed to write {}: {error}", path.display()))?;

        Ok(ToolResult::Text(format!("edited {}", path.display())))
    }
}

fn parse_input(raw: &str) -> anyhow::Result<EditInput> {
    serde_json::from_str(raw).map_err(|error| anyhow::anyhow!("invalid edit input: {error}"))
}

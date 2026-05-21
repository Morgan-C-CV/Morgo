use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

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

    async fn check_permissions(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> PermissionDecision {
        if super::workspace_permission::session_allow_rule_matches(
            self.metadata().name,
            call,
            permissions,
        ) {
            return PermissionDecision::Allow;
        }
        let Ok(input) = parse_input(call) else {
            return PermissionDecision::Allow;
        };
        let Some(config) = permissions.workspace_permissions() else {
            return PermissionDecision::Allow;
        };
        super::workspace_permission::decision_for_path(
            self.metadata().name,
            &config,
            Path::new(input.file_path.trim()),
            crate::security::workspace_capability::WorkspacePermissionLevel::Edit,
        )
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
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await.map_err(|error| {
                    anyhow::anyhow!(
                        "failed to create parent directory {}: {error}",
                        parent.display()
                    )
                })?;
            }
        }
        fs::write(path, input.content)
            .await
            .map_err(|error| anyhow::anyhow!("failed to write {}: {error}", path.display()))?;
        Ok(ToolResult::Text(format!("wrote {}", path.display())))
    }
}

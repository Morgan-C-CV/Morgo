use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileEditTool;

const EDIT_SNIPPET_PREVIEW_CHARS: usize = 40;

#[derive(Debug, Deserialize)]
struct EditInput {
    file_path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}

fn preview_text(text: &str) -> String {
    let mut preview: String = text.chars().take(EDIT_SNIPPET_PREVIEW_CHARS).collect();
    if text.chars().count() > EDIT_SNIPPET_PREVIEW_CHARS {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n")
}

fn format_edit_success(
    path: &PathBuf,
    replacements: usize,
    replace_all: bool,
    old_text: &str,
    new_text: &str,
) -> String {
    format!(
        "path={}\nreplacements={}\nreplace_all={}\nold_text={}\nnew_text={}\n\nEdit completed successfully.",
        path.display(),
        replacements,
        replace_all,
        preview_text(old_text),
        preview_text(new_text)
    )
}

#[async_trait]
impl Tool for FileEditTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Edit".into(),
            description: "Edit existing files with safety rails".into(),
            aliases: &[],
            search_hint: Some("edit file contents"),
            read_only: false,
            destructive: false,
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
            "required": ["file_path", "old_string", "new_string"],
            "properties": {
                "file_path": {"type": "string"},
                "old_string": {"type": "string"},
                "new_string": {"type": "string"},
                "replace_all": {"type": "boolean"}
            }
        }))
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
            anyhow::bail!("No changes to make: new_string is unchanged from old_string.")
        }
        Ok(())
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
        let Ok(input) = parse_input(&call.input) else {
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
        let input = parse_input(&call.input)?;
        if input.old_string == input.new_string {
            anyhow::bail!("No changes to make: new_string is unchanged from old_string.")
        }
        let path = PathBuf::from(input.file_path.trim());
        if let Some(policy) = permissions.filesystem_policy() {
            policy
                .check_existing_or_create_path_for_write(&path)
                .into_result()?;
        }
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
            anyhow::bail!(
                "String to replace not found in {}. No changes were made.",
                path.display()
            )
        }
        if occurrences > 1 && !input.replace_all {
            anyhow::bail!(
                "Found {} matches for old_string in {}. Please provide more context or set replace_all=true.",
                occurrences,
                path.display()
            )
        }

        let replacements = if input.replace_all { occurrences } else { 1 };
        let updated = if input.replace_all {
            original.replace(&input.old_string, &input.new_string)
        } else {
            original.replacen(&input.old_string, &input.new_string, 1)
        };

        fs::write(&path, updated)
            .await
            .map_err(|error| anyhow::anyhow!("failed to write {}: {error}", path.display()))?;

        Ok(ToolResult::Text(format_edit_success(
            &path,
            replacements,
            input.replace_all,
            &input.old_string,
            &input.new_string,
        )))
    }
}

fn parse_input(raw: &str) -> anyhow::Result<EditInput> {
    serde_json::from_str(raw).map_err(|error| anyhow::anyhow!("invalid edit input: {error}"))
}

use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileReadTool;

const DEFAULT_READ_LIMIT_CHARS: usize = 3_000;
const MAX_READ_LIMIT_CHARS: usize = 5_000;

#[derive(Debug, Deserialize)]
struct ReadInput {
    file_path: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

fn parse_input(call: &ToolCall) -> anyhow::Result<ReadInput> {
    if let Some(json) = call.json_input() {
        return Ok(serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid read input: {error}"))?);
    }
    Ok(ReadInput {
        file_path: call.input.trim().to_string(),
        offset: None,
        limit: None,
    })
}

fn slice_contents(contents: &str, offset: usize, limit: usize) -> (String, bool, usize) {
    let total_chars = contents.chars().count();
    let start = offset.min(total_chars);
    let end = start.saturating_add(limit).min(total_chars);
    let text = contents.chars().skip(start).take(end - start).collect();
    (text, end < total_chars, total_chars)
}

fn format_read_result(path: &Path, offset: usize, slice: &str) -> String {
    format!(
        "path={}\noffset={}\nreturned_chars={}\n\n{}",
        path.display(),
        offset,
        slice.chars().count(),
        slice
    )
}

fn should_block_structured_data_paging(path: &Path, offset: usize) -> bool {
    offset > 0
        && path
            .extension()
            .and_then(|value| value.to_str())
            .map(|extension| {
                matches!(
                    extension.to_ascii_lowercase().as_str(),
                    "jsonl" | "csv" | "tsv" | "log" | "ndjson"
                )
            })
            .unwrap_or(false)
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
                "file_path": {"type": "string"},
                "offset": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 5000}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        let input = parse_input(call)?;
        if input.file_path.trim().is_empty() {
            anyhow::bail!("read target cannot be empty")
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
            crate::security::workspace_capability::WorkspacePermissionLevel::View,
        )
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let structured_input = call.json_input().is_some();
        let input = parse_input(call)?;
        let raw_path = input.file_path;
        let path = Path::new(raw_path.trim());
        if let Some(policy) = permissions.filesystem_policy() {
            policy.check_existing_path_for_read(path).into_result()?;
        }
        let contents = fs::read_to_string(path)
            .await
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
        let offset = input.offset.unwrap_or(0);
        let requested_limit = input.limit.unwrap_or(DEFAULT_READ_LIMIT_CHARS);
        let limit = requested_limit.clamp(1, MAX_READ_LIMIT_CHARS);
        if should_block_structured_data_paging(path, offset) {
            return Ok(ToolResult::ResultTooLarge(format!(
                "structured data paging stopped for {} at offset {}. Use Bash or a local script to aggregate/process the full file instead of paging it with Read.",
                path.display(),
                offset
            )));
        }
        let (slice, truncated, total_chars) = slice_contents(&contents, offset, limit);
        let formatted = format_read_result(path, offset, &slice);
        if truncated || offset > 0 || total_chars > slice.chars().count() {
            return Ok(ToolResult::Text(format!(
                "{formatted}\n\n[Read truncated: path={}, offset={}, returned_chars={}, total_chars={}. Use Read with offset={} and limit<={} to continue.]",
                path.display(),
                offset,
                slice.chars().count(),
                total_chars,
                offset.saturating_add(slice.chars().count()),
                MAX_READ_LIMIT_CHARS
            )));
        }
        if structured_input {
            return Ok(ToolResult::Text(formatted));
        }
        Ok(ToolResult::Text(slice))
    }
}

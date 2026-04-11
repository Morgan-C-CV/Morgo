use std::fs;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GrepTool;

#[derive(Debug, Deserialize)]
struct GrepInput {
    pattern: String,
}

fn parse_pattern(call: &ToolCall) -> anyhow::Result<String> {
    if let Some(json) = call.json_input() {
        let input: GrepInput = serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid grep input: {error}"))?;
        return Ok(input.pattern);
    }
    Ok(call.input.trim().to_string())
}

#[async_trait]
impl Tool for GrepTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Grep",
            description: "Search file contents by regex",
            aliases: &[],
            search_hint: Some("search file contents"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
        }
    }

    fn input_schema(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "object",
            "required": ["pattern"],
            "properties": {
                "pattern": {"type": "string"}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        if parse_pattern(call)?.trim().is_empty() {
            anyhow::bail!("grep query cannot be empty")
        }
        Ok(())
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let root = std::env::current_dir()
            .map_err(|error| anyhow::anyhow!("failed to resolve cwd: {error}"))?;
        let query = parse_pattern(call)?;
        let mut matches = Vec::new();
        collect_matches(&root, &root, query.trim(), &mut matches)?;
        matches.sort();
        Ok(ToolResult::Text(matches.join("\n")))
    }
}

fn collect_matches(
    root: &Path,
    current: &Path,
    query: &str,
    matches: &mut Vec<String>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(current).map_err(|error| {
        anyhow::anyhow!("failed to read directory {}: {error}", current.display())
    })? {
        let entry = entry.map_err(|error| {
            anyhow::anyhow!("failed to iterate directory {}: {error}", current.display())
        })?;
        let entry_path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            anyhow::anyhow!(
                "failed to read file type for {}: {error}",
                entry_path.display()
            )
        })?;

        if file_type.is_dir() {
            collect_matches(root, &entry_path, query, matches)?;
            continue;
        }

        let Ok(contents) = fs::read_to_string(&entry_path) else {
            continue;
        };

        let relative = entry_path
            .strip_prefix(root)
            .unwrap_or(&entry_path)
            .to_string_lossy()
            .replace('\\', "/");
        for (index, line) in contents.lines().enumerate() {
            if line.contains(query) {
                matches.push(format!("{}:{}:{}", relative, index + 1, line.trim()));
            }
        }
    }

    Ok(())
}

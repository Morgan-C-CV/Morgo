use std::fs;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GlobTool;

#[derive(Debug, Deserialize)]
struct GlobInput {
    pattern: String,
}

fn parse_pattern(call: &ToolCall) -> anyhow::Result<String> {
    if let Some(json) = call.json_input() {
        let input: GlobInput = serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid glob input: {error}"))?;
        return Ok(input.pattern);
    }
    Ok(call.input.trim().to_string())
}

#[async_trait]
impl Tool for GlobTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Glob".into(),
            description: "Match file paths by glob pattern".into(),
            aliases: &[],
            search_hint: Some("files glob pattern"),
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
            "required": ["pattern"],
            "properties": {
                "pattern": {"type": "string"}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        if parse_pattern(call)?.trim().is_empty() {
            anyhow::bail!("glob pattern cannot be empty")
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
        let pattern = parse_pattern(call)?;
        let mut matches = Vec::new();
        collect_matches(&root, &root, pattern.trim(), &mut matches)?;
        matches.sort();
        Ok(ToolResult::Text(matches.join("\n")))
    }
}

fn collect_matches(
    root: &Path,
    current: &Path,
    pattern: &str,
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
            collect_matches(root, &entry_path, pattern, matches)?;
            continue;
        }

        let relative = entry_path
            .strip_prefix(root)
            .unwrap_or(&entry_path)
            .to_string_lossy()
            .replace('\\', "/");
        if pattern_matches(pattern, &relative) {
            matches.push(relative);
        }
    }

    Ok(())
}

fn pattern_matches(pattern: &str, candidate: &str) -> bool {
    if pattern == "*" || pattern == "**" {
        return true;
    }

    let pattern_parts: Vec<&str> = pattern.split('*').collect();
    if pattern_parts.len() == 1 {
        return candidate == pattern;
    }

    let starts_with_wildcard = pattern.starts_with('*');
    let ends_with_wildcard = pattern.ends_with('*');
    let mut remainder = candidate;

    for (index, part) in pattern_parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if index == 0 && !starts_with_wildcard {
            let Some(stripped) = remainder.strip_prefix(part) else {
                return false;
            };
            remainder = stripped;
            continue;
        }

        if index == pattern_parts.len() - 1 && !ends_with_wildcard {
            return remainder.ends_with(part);
        }

        let Some(position) = remainder.find(part) else {
            return false;
        };
        remainder = &remainder[position + part.len()..];
    }

    true
}

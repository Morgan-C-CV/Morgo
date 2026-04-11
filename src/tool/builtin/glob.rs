use std::fs;
use std::path::Path;

use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GlobTool;

#[async_trait]
impl Tool for GlobTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Glob",
            description: "Match file paths by glob pattern",
            aliases: &[],
            search_hint: Some("files glob pattern"),
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
        let mut matches = Vec::new();
        collect_matches(&root, &root, call.input.trim(), &mut matches)?;
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

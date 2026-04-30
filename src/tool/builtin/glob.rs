use std::fs;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GlobTool;

const MAX_SEARCH_RESULT_ITEMS: usize = 200;
const MAX_SEARCH_RESULT_CHARS: usize = 12_000;
const SEARCH_PREVIEW_ITEMS: usize = 20;
const IGNORED_DIR_NAMES: &[&str] = &[
    ".git",
    ".rust-agent",
    "node_modules",
    "target",
    "dist",
    "build",
    ".venv",
];

#[derive(Debug, Deserialize)]
struct GlobInput {
    pattern: String,
    path: Option<String>,
}

fn parse_input(call: &ToolCall) -> anyhow::Result<GlobInput> {
    if let Some(json) = call.json_input() {
        let input: GlobInput = serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid glob input: {error}"))?;
        return Ok(input);
    }
    Ok(GlobInput {
        pattern: call.input.trim().to_string(),
        path: None,
    })
}

fn resolve_search_root(root: &Path, path: Option<&str>) -> anyhow::Result<std::path::PathBuf> {
    let Some(path) = path.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(root.to_path_buf());
    };
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        let resolved = candidate.to_path_buf();
        if resolved.exists() {
            return Ok(resolved.canonicalize().unwrap_or(resolved));
        }
        anyhow::bail!("glob path does not exist: {}", resolved.display());
    }
    Ok(candidate.to_path_buf())
}

fn should_skip_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| IGNORED_DIR_NAMES.contains(&name))
        .unwrap_or(false)
}

fn finalize_matches(tool_name: &str, mut matches: Vec<String>) -> ToolResult {
    matches.sort();
    let output = matches.join("\n");
    if matches.len() > MAX_SEARCH_RESULT_ITEMS || output.len() > MAX_SEARCH_RESULT_CHARS {
        let preview = matches
            .iter()
            .take(SEARCH_PREVIEW_ITEMS)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let mut message = format!(
            "{tool_name} matched {} files ({} chars). Narrow the pattern or provide a path.",
            matches.len(),
            output.len()
        );
        if !preview.is_empty() {
            message.push_str("\nPreview:\n");
            message.push_str(&preview);
        }
        return ToolResult::ResultTooLarge(message);
    }
    ToolResult::Text(output)
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
                "pattern": {"type": "string"},
                "path": {"type": "string"}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        if parse_input(call)?.pattern.trim().is_empty() {
            anyhow::bail!("glob pattern cannot be empty")
        }
        Ok(())
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let root = std::env::current_dir()
            .map_err(|error| anyhow::anyhow!("failed to resolve cwd: {error}"))?;
        let input = parse_input(call)?;
        let search_root = resolve_search_root(&root, input.path.as_deref())?;
        let mut matches = Vec::new();
        collect_matches(&root, &search_root, input.pattern.trim(), &mut matches)?;
        if let Some(policy) = permissions.filesystem_policy() {
            let absolute_matches = matches
                .iter()
                .map(|matched| root.join(matched))
                .collect::<Vec<_>>();
            policy
                .check_discovered_paths_for_read(
                    &absolute_matches,
                    crate::security::filesystem_policy::FilesystemAccessKind::Search,
                )
                .into_result()?;
        }
        Ok(finalize_matches("Glob", matches))
    }
}

fn collect_matches(
    root: &Path,
    current: &Path,
    pattern: &str,
    matches: &mut Vec<String>,
) -> anyhow::Result<()> {
    if current.is_dir() && should_skip_dir(current) && current != root {
        return Ok(());
    }
    if current.is_file() {
        let relative = current
            .strip_prefix(root)
            .unwrap_or(current)
            .to_string_lossy()
            .replace('\\', "/");
        if pattern_matches(pattern, &relative) {
            matches.push(relative);
        }
        return Ok(());
    }

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
            if should_skip_dir(&entry_path) {
                continue;
            }
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

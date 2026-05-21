use std::fs;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

pub struct GrepTool;

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
struct GrepInput {
    pattern: String,
    path: Option<String>,
}

fn parse_input(call: &ToolCall) -> anyhow::Result<GrepInput> {
    if let Some(json) = call.json_input() {
        let input: GrepInput = serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid grep input: {error}"))?;
        return Ok(input);
    }
    Ok(GrepInput {
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
        anyhow::bail!("grep path does not exist: {}", resolved.display());
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
            "{tool_name} matched {} lines ({} chars). Narrow the query or provide a path.",
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
impl Tool for GrepTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Grep".into(),
            description: "Search file contents by regex".into(),
            aliases: &[],
            search_hint: Some("search file contents"),
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
            anyhow::bail!("grep query cannot be empty")
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
        let Some(config) = permissions.workspace_permissions() else {
            return PermissionDecision::Allow;
        };
        let Ok(input) = parse_input(call) else {
            return PermissionDecision::Allow;
        };
        let root = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let search_root =
            resolve_search_root(&root, input.path.as_deref()).unwrap_or_else(|_| root.clone());
        let target = if search_root.is_absolute() {
            search_root
        } else {
            root.join(search_root)
        };
        super::workspace_permission::decision_for_path(
            self.metadata().name,
            &config,
            &target,
            crate::security::workspace_capability::WorkspacePermissionLevel::View,
        )
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
        let mut searched_paths = Vec::new();
        collect_matches(
            &root,
            &search_root,
            input.pattern.trim(),
            &mut searched_paths,
            &mut matches,
        )?;
        if let Some(policy) = permissions.filesystem_policy() {
            policy
                .check_discovered_paths_for_read(
                    &searched_paths,
                    crate::security::filesystem_policy::FilesystemAccessKind::Search,
                )
                .into_result()?;
        }
        Ok(finalize_matches("Grep", matches))
    }
}

fn collect_matches(
    root: &Path,
    current: &Path,
    query: &str,
    searched_paths: &mut Vec<std::path::PathBuf>,
    matches: &mut Vec<String>,
) -> anyhow::Result<()> {
    if current.is_dir() && should_skip_dir(current) && current != root {
        return Ok(());
    }
    if current.is_file() {
        searched_paths.push(current.to_path_buf());
        let Ok(contents) = fs::read_to_string(current) else {
            return Ok(());
        };
        let relative = current
            .strip_prefix(root)
            .unwrap_or(current)
            .to_string_lossy()
            .replace('\\', "/");
        for (index, line) in contents.lines().enumerate() {
            if line.contains(query) {
                matches.push(format!("{}:{}:{}", relative, index + 1, line.trim()));
            }
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
            collect_matches(root, &entry_path, query, searched_paths, matches)?;
            continue;
        }

        searched_paths.push(entry_path.clone());
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

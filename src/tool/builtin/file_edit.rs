use std::path::{Path, PathBuf};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use tokio::fs;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

pub struct FileEditTool;

const EDIT_SNIPPET_PREVIEW_CHARS: usize = 40;
const EDIT_CANDIDATE_LIMIT: usize = 8;
const EDIT_CANDIDATE_PREVIEW_CHARS: usize = 120;

#[derive(Debug, Deserialize)]
struct EditInput {
    file_path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EditMatchCandidate {
    index: usize,
    line: usize,
    text: String,
}

fn preview_text(text: &str) -> String {
    let mut preview: String = text.chars().take(EDIT_SNIPPET_PREVIEW_CHARS).collect();
    if text.chars().count() > EDIT_SNIPPET_PREVIEW_CHARS {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n")
}

fn preview_candidate_line(text: &str) -> String {
    let trimmed = text.trim();
    let mut preview: String = trimmed.chars().take(EDIT_CANDIDATE_PREVIEW_CHARS).collect();
    if trimmed.chars().count() > EDIT_CANDIDATE_PREVIEW_CHARS {
        preview.push_str("...");
    }
    preview
}

fn line_number_for_byte(text: &str, byte_index: usize) -> usize {
    text.as_bytes()[..byte_index]
        .iter()
        .filter(|byte| **byte == b'\n')
        .count()
        + 1
}

fn line_text_at_byte(text: &str, byte_index: usize) -> &str {
    let line_start = text[..byte_index]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line_end = text[byte_index..]
        .find('\n')
        .map(|index| byte_index + index)
        .unwrap_or(text.len());
    &text[line_start..line_end]
}

fn edit_match_candidates(original: &str, old_text: &str) -> Vec<EditMatchCandidate> {
    if old_text.is_empty() {
        return Vec::new();
    }

    original
        .match_indices(old_text)
        .take(EDIT_CANDIDATE_LIMIT)
        .enumerate()
        .map(|(index, (byte_index, _))| EditMatchCandidate {
            index: index + 1,
            line: line_number_for_byte(original, byte_index),
            text: preview_candidate_line(line_text_at_byte(original, byte_index)),
        })
        .collect()
}

fn format_edit_success(
    path: &PathBuf,
    replacements: usize,
    replace_all: bool,
    old_text: &str,
    new_text: &str,
) -> String {
    format!(
        "path={}\nreplacements={}\nreplace_all={}\nold_text={}\nnew_text={}\nold_text_b64={}\nnew_text_b64={}\n\nEdit completed successfully.",
        path.display(),
        replacements,
        replace_all,
        preview_text(old_text),
        preview_text(new_text),
        STANDARD.encode(old_text),
        STANDARD.encode(new_text)
    )
}

fn edit_failure_next_action(reason: &str) -> &'static str {
    match reason {
        "ambiguous_old_string" => {
            "Read the file around the intended candidate and retry Edit with a more specific old_string, or set replace_all=true only if every match should change."
        }
        "old_string_not_found" => {
            "Read the file around the intended location and retry Edit with the exact current text."
        }
        "empty_old_string" => {
            "Retry Edit with a non-empty old_string copied exactly from the file."
        }
        "no_changes" => "Skip the Edit call or provide a new_string that differs from old_string.",
        "empty_file_path" => "Retry Edit with a non-empty file_path.",
        _ => "Read the target file and retry Edit with corrected arguments.",
    }
}

fn format_edit_failure(
    path: &Path,
    reason: &str,
    matches: usize,
    replace_all: bool,
    old_text: &str,
    new_text: &str,
    candidates: &[EditMatchCandidate],
) -> String {
    let mut rendered = format!(
        "status=failed\npath={}\nreason={}\nmatches={}\nreplace_all={}\nold_text={}\nnew_text={}\nold_text_b64={}\nnew_text_b64={}",
        path.display(),
        reason,
        matches,
        replace_all,
        preview_text(old_text),
        preview_text(new_text),
        STANDARD.encode(old_text),
        STANDARD.encode(new_text)
    );
    for candidate in candidates {
        rendered.push_str(&format!(
            "\ncandidate={} line={} text={}",
            candidate.index, candidate.line, candidate.text
        ));
    }
    rendered.push_str(&format!(
        "\nnext_action={}\n\nEdit was not applied.",
        edit_failure_next_action(reason)
    ));
    rendered
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
        parse_input(&call.input)?;
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
        let path = PathBuf::from(input.file_path.trim());
        if input.file_path.trim().is_empty() {
            return Ok(ToolResult::Text(format_edit_failure(
                &path,
                "empty_file_path",
                0,
                input.replace_all,
                &input.old_string,
                &input.new_string,
                &[],
            )));
        }
        if input.old_string.is_empty() {
            return Ok(ToolResult::Text(format_edit_failure(
                &path,
                "empty_old_string",
                0,
                input.replace_all,
                &input.old_string,
                &input.new_string,
                &[],
            )));
        }
        if input.old_string == input.new_string {
            return Ok(ToolResult::Text(format_edit_failure(
                &path,
                "no_changes",
                0,
                input.replace_all,
                &input.old_string,
                &input.new_string,
                &[],
            )));
        }
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
            return Ok(ToolResult::Text(format_edit_failure(
                &path,
                "old_string_not_found",
                occurrences,
                input.replace_all,
                &input.old_string,
                &input.new_string,
                &[],
            )));
        }
        if occurrences > 1 && !input.replace_all {
            return Ok(ToolResult::Text(format_edit_failure(
                &path,
                "ambiguous_old_string",
                occurrences,
                input.replace_all,
                &input.old_string,
                &input.new_string,
                &edit_match_candidates(&original, &input.old_string),
            )));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use tempfile::tempdir;

    #[test]
    fn edit_success_includes_full_base64_payloads() {
        let path = PathBuf::from("/tmp/example.rs");
        let old_text = "let value = \"old text that is longer than the preview limit\";";
        let new_text = "let value = \"new text that is longer than the preview limit\";";
        let rendered = format_edit_success(&path, 1, false, old_text, new_text);

        assert!(rendered.contains("old_text=let value = \"old text"));
        assert!(rendered.contains("new_text=let value = \"new text"));
        assert!(rendered.contains("..."));
        assert!(rendered.contains(&format!("old_text_b64={}", STANDARD.encode(old_text))));
        assert!(rendered.contains(&format!("new_text_b64={}", STANDARD.encode(new_text))));
    }

    #[test]
    fn edit_match_candidates_include_line_numbers_and_snippets() {
        let original = "first alpha\nsecond beta\nthird alpha\n";
        let candidates = edit_match_candidates(original, "alpha");

        assert_eq!(
            candidates,
            vec![
                EditMatchCandidate {
                    index: 1,
                    line: 1,
                    text: "first alpha".into(),
                },
                EditMatchCandidate {
                    index: 2,
                    line: 3,
                    text: "third alpha".into(),
                },
            ]
        );
    }

    #[test]
    fn edit_failure_includes_repairable_details() {
        let path = PathBuf::from("/tmp/example.rs");
        let old_text = "let value = old;";
        let new_text = "let value = new;";
        let rendered = format_edit_failure(
            &path,
            "ambiguous_old_string",
            2,
            false,
            old_text,
            new_text,
            &[EditMatchCandidate {
                index: 1,
                line: 12,
                text: "let value = old;".into(),
            }],
        );

        assert!(rendered.contains("status=failed"));
        assert!(rendered.contains("reason=ambiguous_old_string"));
        assert!(rendered.contains("matches=2"));
        assert!(rendered.contains("candidate=1 line=12 text=let value = old;"));
        assert!(rendered.contains("next_action=Read the file around the intended candidate"));
        assert!(rendered.contains(&format!("old_text_b64={}", STANDARD.encode(old_text))));
        assert!(rendered.contains("Edit was not applied."));
    }

    #[tokio::test]
    async fn ambiguous_edit_returns_text_instead_of_error_and_does_not_write() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("example.txt");
        fs::write(&path, "alpha\nbeta\nalpha\n")
            .await
            .expect("seed");
        let call = ToolCall::new(
            "Edit",
            serde_json::json!({
                "file_path": path,
                "old_string": "alpha",
                "new_string": "omega"
            })
            .to_string(),
        );
        let permissions = ToolPermissionContext::new(PermissionMode::Default);

        let result = FileEditTool
            .invoke(&call, &permissions)
            .await
            .expect("ambiguous edit should be model-visible");

        match result {
            ToolResult::Text(text) => {
                assert!(text.contains("status=failed"));
                assert!(text.contains("reason=ambiguous_old_string"));
                assert!(text.contains("matches=2"));
                assert!(text.contains("candidate=1 line=1 text=alpha"));
                assert!(text.contains("candidate=2 line=3 text=alpha"));
            }
            other => panic!("expected text failure, got {other:?}"),
        }
        let content = fs::read_to_string(&path).await.expect("read");
        assert_eq!(content, "alpha\nbeta\nalpha\n");
    }

    #[tokio::test]
    async fn missing_old_string_returns_text_instead_of_error_and_does_not_write() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("example.txt");
        fs::write(&path, "alpha\n").await.expect("seed");
        let call = ToolCall::new(
            "Edit",
            serde_json::json!({
                "file_path": path,
                "old_string": "missing",
                "new_string": "omega"
            })
            .to_string(),
        );
        let permissions = ToolPermissionContext::new(PermissionMode::Default);

        let result = FileEditTool
            .invoke(&call, &permissions)
            .await
            .expect("missing old_string should be model-visible");

        match result {
            ToolResult::Text(text) => {
                assert!(text.contains("status=failed"));
                assert!(text.contains("reason=old_string_not_found"));
                assert!(text.contains("matches=0"));
                assert!(text.contains("next_action=Read the file around the intended location"));
            }
            other => panic!("expected text failure, got {other:?}"),
        }
        let content = fs::read_to_string(&path).await.expect("read");
        assert_eq!(content, "alpha\n");
    }
}

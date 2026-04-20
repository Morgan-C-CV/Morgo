use std::path::{Component, Path, PathBuf};

use crate::security::filesystem_policy::{FilesystemAccessKind, FilesystemPolicy};
use crate::tool::builtin::bash::readonly_validation::is_read_only_command;
use crate::tool::builtin::bash::scanner::{ShellOperator, scan_bash_command};
use crate::tool::builtin::bash::security::contains_write_redirection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BashPathAssessment {
    pub safe: bool,
    pub findings: Vec<String>,
}

pub fn is_safe_path(path: &str) -> bool {
    let candidate = Path::new(path);
    !candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
}

pub fn command_uses_only_safe_paths(command: &str) -> bool {
    command_path_assessment(command)
        .iter()
        .all(|finding| !finding.starts_with("unsafe:"))
}

pub fn command_path_assessment(command: &str) -> Vec<String> {
    assess_command_paths(
        command,
        &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        None,
    )
    .findings
}

pub fn assess_command_paths(
    command: &str,
    cwd: &Path,
    filesystem_policy: Option<&FilesystemPolicy>,
) -> BashPathAssessment {
    let scan = scan_bash_command(command);
    let access = infer_access_kind(command, &scan.operators);
    let mut findings = Vec::new();
    let mut safe = true;

    for token in candidate_path_tokens(&scan.words) {
        let candidate = Path::new(token.as_str());
        if candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            safe = false;
            findings.push(format!("unsafe:{token}"));
            findings.push("path.parent_traversal".into());
            continue;
        }

        let absolute = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            cwd.join(candidate)
        };

        if let Some(policy) = filesystem_policy {
            let decision = match access {
                FilesystemAccessKind::Read => policy.check_existing_path_for_read(&absolute),
                FilesystemAccessKind::Search => policy.check_existing_path_for_search(&absolute),
                FilesystemAccessKind::Write | FilesystemAccessKind::Create => {
                    policy.check_existing_or_create_path_for_write(&absolute)
                }
            };
            if decision.is_allowed() {
                findings.push(format!("safe:{token}"));
            } else {
                safe = false;
                findings.push(format!("unsafe:{token}"));
                findings.push("path.policy_denied".into());
            }
            continue;
        }

        if candidate.is_absolute() && !absolute.starts_with(cwd) {
            safe = false;
            findings.push(format!("unsafe:{token}"));
            findings.push("path.absolute_outside_workspace".into());
        } else {
            findings.push(format!("safe:{token}"));
        }
    }

    BashPathAssessment { safe, findings }
}

fn candidate_path_tokens(words: &[String]) -> Vec<String> {
    words
        .iter()
        .filter(|token| token.contains('/') || token.starts_with('.') || token.starts_with('~'))
        .cloned()
        .collect()
}

fn infer_access_kind(command: &str, operators: &[ShellOperator]) -> FilesystemAccessKind {
    if operators.iter().any(|operator| operator.is_redirection())
        || contains_write_redirection(command)
    {
        return FilesystemAccessKind::Create;
    }
    if command.contains("sed -i") || command.trim_start().starts_with("sed -i") {
        return FilesystemAccessKind::Write;
    }
    if is_read_only_command(command) {
        return FilesystemAccessKind::Read;
    }
    FilesystemAccessKind::Write
}

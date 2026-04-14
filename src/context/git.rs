use std::path::{Path, PathBuf};
use std::process::Command;

use crate::state::app_state::AppState;

#[derive(Debug, Clone)]
struct GitProbe {
    repository: bool,
    repo_root: Option<String>,
    worktree: Option<String>,
    branch: Option<String>,
    dirty: Option<bool>,
}

pub fn describe_git_context(app_state: &AppState) -> String {
    let cwd = app_state.current_working_directory();
    let cwd_display = cwd.to_string_lossy().to_string();
    let probe = probe_git_context(&cwd);

    let mut lines = vec![
        "Git context:".to_string(),
        format!("- cwd: {cwd_display}"),
        format!(
            "- repository: {}",
            if probe.repository { "yes" } else { "no" }
        ),
        format!(
            "- branch: {}",
            probe.branch.as_deref().unwrap_or("<unknown>")
        ),
        format!(
            "- dirty: {}",
            match probe.dirty {
                Some(true) => "yes",
                Some(false) => "no",
                None => "<unknown>",
            }
        ),
    ];

    if let Some(repo_root) = probe.repo_root.as_deref() {
        lines.push(format!("- repo_root: {repo_root}"));
    }
    if let Some(worktree) = probe.worktree.as_deref() {
        lines.push(format!("- worktree: {worktree}"));
    }

    lines.join("\n")
}

fn probe_git_context(cwd: &Path) -> GitProbe {
    let inside_work_tree = git_output(cwd, ["rev-parse", "--is-inside-work-tree"])
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if !inside_work_tree {
        return GitProbe {
            repository: false,
            repo_root: None,
            worktree: None,
            branch: None,
            dirty: None,
        };
    }

    let worktree = git_output(cwd, ["rev-parse", "--show-toplevel"]);
    let repo_root = git_output(cwd, ["rev-parse", "--git-common-dir"])
        .and_then(|path| resolve_git_path(cwd, &path))
        .and_then(|path| {
            if path.file_name().is_some_and(|name| name == ".git") {
                path.parent().map(|parent| parent.to_string_lossy().to_string())
            } else {
                Some(path.to_string_lossy().to_string())
            }
        });
    let branch = git_output(cwd, ["symbolic-ref", "--quiet", "--short", "HEAD"]).or_else(|| {
        git_output(cwd, ["rev-parse", "--short", "HEAD"]).map(|sha| format!("detached@{sha}"))
    });
    let dirty = git_has_porcelain_changes(cwd);

    GitProbe {
        repository: true,
        repo_root,
        worktree,
        branch,
        dirty,
    }
}

fn resolve_git_path(cwd: &Path, value: &str) -> Option<PathBuf> {
    let raw = PathBuf::from(value);
    let resolved = if raw.is_absolute() { raw } else { cwd.join(raw) };
    resolved.canonicalize().ok().or(Some(resolved))
}

fn git_has_porcelain_changes(cwd: &Path) -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(!output.stdout.is_empty())
}

fn git_output<const N: usize>(cwd: &Path, args: [&str; N]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

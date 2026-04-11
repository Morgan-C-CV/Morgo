use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;

use crate::tool::builtin::bash::readonly_validation::is_read_only_command;
use crate::tool::builtin::bash::security::{contains_destructive_pattern, contains_shell_operator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPolicy {
    Disabled,
    WorkspaceWrite,
    ReadOnly,
}

pub fn select_sandbox_policy(command: &str) -> SandboxPolicy {
    if contains_destructive_pattern(command) || contains_shell_operator(command) {
        SandboxPolicy::WorkspaceWrite
    } else if is_read_only_command(command) {
        SandboxPolicy::ReadOnly
    } else {
        SandboxPolicy::WorkspaceWrite
    }
}

pub async fn execute_with_sandbox(
    command: &str,
    cwd: &Path,
    policy: SandboxPolicy,
) -> anyhow::Result<std::process::Output> {
    let wrapped_command = match policy {
        SandboxPolicy::Disabled | SandboxPolicy::WorkspaceWrite => command.to_string(),
        SandboxPolicy::ReadOnly => format!("umask 077; {command}"),
    };

    Command::new("/bin/sh")
        .arg("-lc")
        .arg(wrapped_command)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|error| anyhow::anyhow!("failed to execute bash command: {error}"))
}

use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;

use crate::tool::builtin::bash::clamped_reader::{ClampedOutput, read_clamped};
use crate::tool::builtin::bash::readonly_validation::is_read_only_command;
use crate::tool::builtin::bash::security::{contains_destructive_pattern, contains_shell_operator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPolicy {
    Disabled,
    WorkspaceWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxRunner {
    DirectShell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SandboxExecutionPlan {
    runner: SandboxRunner,
    policy: SandboxPolicy,
}

pub struct ClampedProcessOutput {
    pub status: std::process::ExitStatus,
    pub stdout: ClampedOutput,
    pub stderr: ClampedOutput,
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
) -> anyhow::Result<ClampedProcessOutput> {
    let plan = build_execution_plan(policy);
    let mut child = build_command(plan, command, cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to execute bash command: {e}"))?;

    // Spawn readers before wait() to avoid pipe-buffer deadlock.
    let stdout_task = child.stdout.take().map(|s| tokio::spawn(read_clamped(s)));
    let stderr_task = child.stderr.take().map(|s| tokio::spawn(read_clamped(s)));

    let status = child
        .wait()
        .await
        .map_err(|e| anyhow::anyhow!("failed to wait for bash command: {e}"))?;

    let empty = || ClampedOutput {
        head: vec![],
        tail: vec![],
        truncated: false,
        total_bytes_read: 0,
    };
    let stdout = match stdout_task {
        Some(t) => t.await.map_err(|e| anyhow::anyhow!("stdout join: {e}"))?,
        None => empty(),
    };
    let stderr = match stderr_task {
        Some(t) => t.await.map_err(|e| anyhow::anyhow!("stderr join: {e}"))?,
        None => empty(),
    };

    Ok(ClampedProcessOutput {
        status,
        stdout,
        stderr,
    })
}

fn build_execution_plan(policy: SandboxPolicy) -> SandboxExecutionPlan {
    let runner = match policy {
        SandboxPolicy::Disabled | SandboxPolicy::WorkspaceWrite | SandboxPolicy::ReadOnly => {
            SandboxRunner::DirectShell
        }
    };
    SandboxExecutionPlan { runner, policy }
}

fn build_command(plan: SandboxExecutionPlan, command: &str, cwd: &Path) -> Command {
    let mut process = match plan.runner {
        SandboxRunner::DirectShell => {
            let mut process = Command::new("/bin/sh");
            process.kill_on_drop(true);
            process.arg("-lc").arg(command);
            process
        }

    };

    process
        .current_dir(cwd)
        .stdin(Stdio::null())
        .env("RUST_AGENT_SANDBOX_POLICY", format!("{:?}", plan.policy));
    process
}

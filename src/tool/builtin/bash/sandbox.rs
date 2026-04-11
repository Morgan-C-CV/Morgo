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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxRunner {
    DirectShell,
    MacOsSandboxExec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SandboxExecutionPlan {
    runner: SandboxRunner,
    policy: SandboxPolicy,
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
    let plan = build_execution_plan(policy);
    let mut process = build_command(plan, command, cwd);
    process
        .output()
        .await
        .map_err(|error| anyhow::anyhow!("failed to execute bash command: {error}"))
}

fn build_execution_plan(policy: SandboxPolicy) -> SandboxExecutionPlan {
    let runner = match policy {
        SandboxPolicy::ReadOnly if Path::new("/usr/bin/sandbox-exec").exists() => {
            SandboxRunner::MacOsSandboxExec
        }
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
            process.arg("-lc").arg(command);
            process
        }
        SandboxRunner::MacOsSandboxExec => {
            let mut process = Command::new("/usr/bin/sandbox-exec");
            process
                .arg("-p")
                .arg(sandbox_profile(plan.policy))
                .arg("/bin/sh")
                .arg("-lc")
                .arg(command);
            process
        }
    };

    process
        .current_dir(cwd)
        .stdin(Stdio::null())
        .env("RUST_AGENT_SANDBOX_POLICY", format!("{:?}", plan.policy));
    process
}

fn sandbox_profile(policy: SandboxPolicy) -> &'static str {
    match policy {
        SandboxPolicy::Disabled | SandboxPolicy::WorkspaceWrite => {
            "(version 1) (allow default)"
        }
        SandboxPolicy::ReadOnly => {
            "(version 1) (deny default) (allow process*) (allow sysctl-read) (allow file-read*)"
        }
    }
}

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

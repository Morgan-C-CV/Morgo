use crate::tool::builtin::bash::path_validation::command_uses_only_safe_paths;
use crate::tool::builtin::bash::readonly_validation::is_read_only_command;
use crate::tool::builtin::bash::sandbox::{SandboxPolicy, select_sandbox_policy};
use crate::tool::builtin::bash::security::{contains_destructive_pattern, contains_shell_operator};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BashPolicyDecision {
    pub read_only: bool,
    pub safe_in_plan_mode: bool,
    pub path_safe: bool,
    pub requires_escalation: bool,
    pub sandbox_policy: SandboxPolicy,
}

pub fn evaluate_bash_policy(command: &str) -> BashPolicyDecision {
    let read_only = is_read_only_command(command);
    let path_safe = command_uses_only_safe_paths(command);
    let destructive = contains_destructive_pattern(command);
    let has_shell_operator = contains_shell_operator(command);
    let sandbox_policy = select_sandbox_policy(command);

    BashPolicyDecision {
        read_only,
        safe_in_plan_mode: read_only && path_safe && !has_shell_operator,
        path_safe,
        requires_escalation: destructive || has_shell_operator || !path_safe,
        sandbox_policy,
    }
}

pub fn is_plan_mode_safe(command: &str) -> bool {
    evaluate_bash_policy(command).safe_in_plan_mode
}

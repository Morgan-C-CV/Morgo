use rust_agent::tool::builtin::bash::path_validation::{
    command_uses_only_safe_paths, is_safe_path,
};
use rust_agent::tool::builtin::bash::permissions::{evaluate_bash_policy, is_plan_mode_safe};
use rust_agent::tool::builtin::bash::sandbox::{SandboxPolicy, select_sandbox_policy};
use rust_agent::tool::builtin::bash::security::{
    contains_destructive_pattern, contains_shell_operator,
};

#[test]
fn unsafe_paths_are_rejected() {
    assert!(!is_safe_path("../etc/passwd"));
    assert!(is_safe_path("relative/file.txt"));
    assert!(!command_uses_only_safe_paths("cat ../secret.txt"));
}

#[test]
fn plan_mode_allows_only_safe_shell_patterns() {
    assert!(is_plan_mode_safe("git status"));
    assert!(!is_plan_mode_safe("cat file.txt | grep needle"));
    assert!(!is_plan_mode_safe("rm -rf /tmp/test"));
}

#[test]
fn destructive_patterns_are_detected() {
    assert!(contains_destructive_pattern("rm -rf build"));
    assert!(!contains_destructive_pattern("git status"));
}

#[test]
fn shell_operators_require_escalation() {
    assert!(contains_shell_operator("cat file.txt | grep needle"));
    let decision = evaluate_bash_policy("cat file.txt | grep needle");
    assert!(decision.requires_escalation);
}

#[test]
fn sandbox_policy_prefers_read_only_for_safe_commands() {
    assert_eq!(select_sandbox_policy("git diff"), SandboxPolicy::ReadOnly);
    assert_eq!(
        select_sandbox_policy("echo hi > out.txt"),
        SandboxPolicy::WorkspaceWrite
    );
}

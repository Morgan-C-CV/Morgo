use rust_agent::tool::builtin::bash::path_validation::{
    command_path_assessment, command_uses_only_safe_paths, is_safe_path,
};
use rust_agent::tool::builtin::bash::permissions::{evaluate_bash_policy, is_plan_mode_safe};
use rust_agent::tool::builtin::bash::sandbox::{SandboxPolicy, select_sandbox_policy};
use rust_agent::tool::builtin::bash::security::{
    contains_destructive_pattern, contains_shell_operator, extract_shell_operators,
};
use rust_agent::tool::builtin::bash::sed_validation::{SedSafety, analyze_sed_safety};

#[test]
fn unsafe_paths_are_rejected() {
    assert!(!is_safe_path("../etc/passwd"));
    assert!(is_safe_path("relative/file.txt"));
    assert!(is_safe_path("/tmp/file.txt"));
    assert!(!command_uses_only_safe_paths("cat ../secret.txt"));
    assert!(command_uses_only_safe_paths("cat /tmp/file.txt"));
}

#[test]
fn plan_mode_allows_only_safe_shell_patterns() {
    assert!(is_plan_mode_safe("git status"));
    assert!(is_plan_mode_safe("env FOO=bar pwd"));
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

#[test]
fn path_assessment_reports_unsafe_and_absolute_tokens() {
    let findings = command_path_assessment("cat ../secret /tmp/file");
    assert!(findings.iter().any(|item| item == "unsafe:../secret"));
    assert!(findings.iter().any(|item| item == "safe:/tmp/file"));
}

#[test]
fn security_extracts_shell_operators() {
    let operators = extract_shell_operators("cat foo | grep bar && pwd");
    assert!(operators.contains(&"|".to_string()));
    assert!(operators.contains(&"&&".to_string()));
}

#[test]
fn sed_safety_detects_unsafe_expression() {
    assert!(matches!(
        analyze_sed_safety("sed -e 's/x/y/e' file.txt"),
        SedSafety::Unsafe(_)
    ));
}

#[test]
fn bash_policy_tracks_structured_findings() {
    let decision = evaluate_bash_policy("sed -i -e 's/x/y/' ../file.txt");
    assert!(!decision.path_safe);
    assert!(!decision.path_findings.is_empty());
    assert!(decision.requires_escalation);
    assert!(
        decision
            .escalation_reasons
            .iter()
            .any(|reason| reason.starts_with("path:"))
    );
}

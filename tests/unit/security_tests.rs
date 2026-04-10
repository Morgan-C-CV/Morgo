use rust_agent::tool::builtin::bash::path_validation::is_safe_path;
use rust_agent::tool::builtin::bash::permissions::is_plan_mode_safe;
use rust_agent::tool::builtin::bash::security::contains_destructive_pattern;

#[test]
fn unsafe_paths_are_rejected() {
    assert!(!is_safe_path("../etc/passwd"));
    assert!(is_safe_path("relative/file.txt"));
}

#[test]
fn plan_mode_allows_only_safe_shell_patterns() {
    assert!(is_plan_mode_safe("git status"));
    assert!(!is_plan_mode_safe("rm -rf /tmp/test"));
}

#[test]
fn destructive_patterns_are_detected() {
    assert!(contains_destructive_pattern("rm -rf build"));
    assert!(!contains_destructive_pattern("git status"));
}

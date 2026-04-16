use crate::tool::builtin::bash::path_validation::{
    command_path_assessment, command_uses_only_safe_paths,
};
use crate::tool::builtin::bash::readonly_validation::classify_read_only_level;
use crate::tool::builtin::bash::sandbox::{SandboxPolicy, select_sandbox_policy};
use crate::tool::builtin::bash::security::{
    contains_destructive_pattern, contains_shell_operator, extract_shell_operators,
};
use crate::tool::builtin::bash::sed_validation::{SedSafety, analyze_sed_safety};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BashPolicyDecision {
    pub read_only: bool,
    pub safe_in_plan_mode: bool,
    pub path_safe: bool,
    pub requires_escalation: bool,
    pub sandbox_policy: SandboxPolicy,
    pub shell_operators: Vec<String>,
    pub path_findings: Vec<String>,
    pub sed_safe: bool,
    pub escalation_reasons: Vec<String>,
}

pub fn evaluate_bash_policy(command: &str) -> BashPolicyDecision {
    let read_only = matches!(
        classify_read_only_level(command),
        crate::tool::builtin::bash::readonly_validation::ReadOnlyLevel::ReadOnly
    );
    let path_safe = command_uses_only_safe_paths(command);
    let destructive = contains_destructive_pattern(command);
    let has_shell_operator = contains_shell_operator(command);
    let shell_operators = extract_shell_operators(command);
    let path_findings = command_path_assessment(command);
    let sed_analysis = analyze_sed_safety(command);
    let sed_safe = !matches!(sed_analysis, SedSafety::Unsafe(_));
    let sandbox_policy = select_sandbox_policy(command);
    let mut escalation_reasons = Vec::new();
    if destructive {
        escalation_reasons.push("destructive_pattern".into());
    }
    if has_shell_operator {
        escalation_reasons.push(format!("shell_operators={}", shell_operators.join(",")));
    }
    if !path_safe {
        escalation_reasons.extend(
            path_findings
                .iter()
                .map(|finding| format!("path:{finding}")),
        );
    }
    if let SedSafety::Unsafe(reason) = sed_analysis {
        escalation_reasons.push(format!("sed:{reason}"));
    }

    BashPolicyDecision {
        read_only,
        safe_in_plan_mode: read_only && path_safe && !has_shell_operator && sed_safe,
        path_safe,
        requires_escalation: !escalation_reasons.is_empty(),
        sandbox_policy,
        shell_operators,
        path_findings,
        sed_safe,
        escalation_reasons,
    }
}

pub fn is_plan_mode_safe(command: &str) -> bool {
    evaluate_bash_policy(command).safe_in_plan_mode
}

use std::path::Path;

use crate::security::filesystem_policy::FilesystemPolicy;
use crate::tool::builtin::bash::path_validation::{
    assess_command_paths, command_path_assessment,
};
use crate::tool::builtin::bash::readonly_validation::classify_read_only_level;
use crate::tool::builtin::bash::sandbox::{SandboxPolicy, select_sandbox_policy};
use crate::tool::builtin::bash::security::{
    contains_destructive_pattern, contains_shell_operator, extract_shell_operators,
    shell_operator_reason_codes,
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
    evaluate_bash_policy_with_context(command, &std::env::current_dir().unwrap_or_else(|_| ".".into()), None)
}

pub fn evaluate_bash_policy_with_context(
    command: &str,
    cwd: &Path,
    filesystem_policy: Option<&FilesystemPolicy>,
) -> BashPolicyDecision {
    let read_only = matches!(
        classify_read_only_level(command),
        crate::tool::builtin::bash::readonly_validation::ReadOnlyLevel::ReadOnly
    );
    let path_assessment = assess_command_paths(command, cwd, filesystem_policy);
    let path_safe = path_assessment.safe;
    let destructive = contains_destructive_pattern(command);
    let has_shell_operator = contains_shell_operator(command);
    let shell_operators = extract_shell_operators(command);
    let path_findings = if filesystem_policy.is_some() || cwd != Path::new(".") {
        path_assessment.findings
    } else {
        command_path_assessment(command)
    };
    let sed_analysis = analyze_sed_safety(command);
    let sed_safe = !matches!(sed_analysis, SedSafety::Unsafe(_));
    let sandbox_policy = select_sandbox_policy(command);
    let mut escalation_reasons = Vec::new();
    if destructive {
        escalation_reasons.push("destructive_pattern".into());
    }
    if has_shell_operator {
        escalation_reasons.extend(shell_operator_reason_codes(command));
    }
    if !path_safe {
        escalation_reasons.extend(
            path_findings
                .iter()
                .filter(|finding| !finding.starts_with("safe:"))
                .cloned(),
        );
    }
    if let SedSafety::Unsafe(reason) = sed_analysis {
        escalation_reasons.push(format!("sed:{reason}"));
    }

    escalation_reasons.sort();
    escalation_reasons.dedup();

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

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::time::timeout;

use crate::security::workspace_capability::{
    CapabilityCheckOutcome, CapabilityRequirementReason, CapabilityTier, WorkspacePermissionLevel,
    check_bash_capability, requirement_from_policy,
};
use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::definition::{
    PermissionApprovalMetadata, PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult,
};

pub mod clamped_reader;
pub mod command_helpers;
pub mod path_validation;
pub mod permissions;
pub mod readonly_validation;
pub mod sandbox;
pub mod scanner;
pub mod security;
pub mod sed_validation;

use clamped_reader::clamped_to_string;
use sandbox::ClampedProcessOutput;

use crate::tool::classifier::auto_classifier::{ClassifierDecision, classify_bash_command};
use command_helpers::{command_matches_rule, normalized_command_variants};
use permissions::{evaluate_bash_policy, evaluate_bash_policy_with_context};
use sandbox::{SandboxPolicy, execute_with_sandbox_config};

pub struct BashTool;

#[derive(Debug, Deserialize)]
struct BashInput {
    command: String,
    #[serde(default)]
    timeout: Option<u64>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    run_in_background: bool,
    #[serde(default)]
    dangerously_disable_sandbox: bool,
}

#[async_trait]
impl Tool for BashTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Bash".into(),
            description: "Execute shell commands with policy checks".into(),
            aliases: &[],
            search_hint: Some("shell command execution"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: true,
            is_open_world: true,
            is_search_or_read_command: false,
        }
    }

    fn input_schema(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {"type": "string"},
                "timeout": {"type": "integer"},
                "description": {"type": "string"},
                "run_in_background": {"type": "boolean"},
                "dangerously_disable_sandbox": {"type": "boolean"}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        let input = parse_input(&call.input)?;
        if input.command.trim().is_empty() {
            anyhow::bail!("bash command cannot be empty")
        }
        if input.run_in_background && input.dangerously_disable_sandbox {
            anyhow::bail!("background bash execution cannot disable sandbox protections")
        }
        if let Some(timeout) = input.timeout {
            if timeout == 0 {
                anyhow::bail!("bash timeout must be greater than zero")
            }
        }
        Ok(())
    }

    async fn check_permissions(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> PermissionDecision {
        let Ok(input) = parse_input(&call.input) else {
            return PermissionDecision::Deny {
                message: "invalid bash input".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Tool,
            };
        };

        let cwd = resolve_cwd(permissions).unwrap_or_else(|_| PathBuf::from("."));
        let filesystem_policy = permissions.filesystem_policy();
        let policy =
            evaluate_bash_policy_with_context(&input.command, &cwd, filesystem_policy.as_deref());
        let variants = normalized_command_variants(&input.command);

        if permissions
            .always_deny_rules()
            .iter()
            .any(|rule| rule == self.metadata().name || rule == call.name.as_str())
            || permissions.always_deny_rules().iter().any(|rule| {
                variants
                    .iter()
                    .any(|variant| command_matches_rule(variant, rule))
            })
        {
            return bash_deny(
                "explicit_rule",
                "command denied by explicit Bash policy rule",
                crate::tool::definition::PermissionDecisionReason::Rule,
            );
        }

        if matches!(permissions.mode(), PermissionMode::Plan) && !policy.safe_in_plan_mode {
            return bash_deny(
                "plan_mode",
                "command is not allowed in plan mode",
                crate::tool::definition::PermissionDecisionReason::Mode,
            );
        }

        if permissions
            .always_ask_rules()
            .iter()
            .any(|rule| rule == self.metadata().name || rule == call.name.as_str())
            || permissions.always_ask_rules().iter().any(|rule| {
                variants
                    .iter()
                    .any(|variant| command_matches_rule(variant, rule))
            })
        {
            return bash_ask(
                &input.command,
                "bash_explicit_ask_rule",
                "command requires approval by explicit Bash policy rule",
                vec!["explicit_ask_rule".into()],
            );
        }

        if permissions
            .always_allow_rules()
            .iter()
            .any(|rule| rule == self.metadata().name || rule == call.name.as_str())
            || permissions.always_allow_rules().iter().any(|rule| {
                variants
                    .iter()
                    .any(|variant| command_matches_rule(variant, rule))
            })
        {
            return PermissionDecision::Allow;
        }

        if input.dangerously_disable_sandbox {
            let sandbox_config = permissions.sandbox_config();
            if sandbox_config.enabled && !sandbox_config.allow_unsandboxed_commands {
                return bash_deny(
                    "sandbox_unsandboxed_disabled",
                    "command requests disabling sandbox protections but unsandboxed Bash commands are disabled",
                    crate::tool::definition::PermissionDecisionReason::Safety,
                );
            }
            return bash_ask(
                &input.command,
                "sandbox_disable",
                "command requests disabling sandbox protections",
                vec!["sandbox_disable".into()],
            );
        }

        match classify_bash_command(&input.command) {
            ClassifierDecision::Deny { code, warning } => {
                return bash_deny(
                    code,
                    &warning,
                    crate::tool::definition::PermissionDecisionReason::Safety,
                );
            }
            ClassifierDecision::Ask { code, warning } => {
                return bash_ask(
                    &input.command,
                    code,
                    &warning,
                    vec![format!("classifier.{code}")],
                );
            }
            ClassifierDecision::Allow => {}
        }

        let requirement = requirement_from_policy(&policy);
        if let Some(workspace_permissions) = permissions.workspace_permissions() {
            let required_permission = workspace_permission_for_bash_tier(requirement.required_tier);
            if !policy.path_safe
                || requirement.reason == CapabilityRequirementReason::OutOfScopePath
            {
                return bash_ask(
                    &input.command,
                    "workspace_out_of_scope_path",
                    "command references a path outside the trusted workspace",
                    policy.escalation_reasons.clone(),
                );
            }
            match workspace_permissions.check_path(&cwd, required_permission) {
                crate::security::workspace_capability::WorkspacePermissionCheck::Allowed {
                    ..
                } => {}
                crate::security::workspace_capability::WorkspacePermissionCheck::RequiresApproval {
                    target_path,
                    required,
                    current,
                    matched_path,
                    reason,
                } => {
                    return super::workspace_permission::workspace_ask(
                        "Bash",
                        target_path.display().to_string(),
                        required,
                        current,
                        matched_path.map(|path| path.display().to_string()),
                        reason,
                    );
                }
            }
        } else if policy.requires_escalation {
            // If a legacy WorkspaceCapabilityConfig is present, route through it.
            if let Some(cap_config) = permissions.workspace_capability() {
                let outcome = check_bash_capability(&requirement, &cap_config, &cwd);
                match outcome {
                    CapabilityCheckOutcome::Allowed => {}
                    CapabilityCheckOutcome::RequiresApproval {
                        required_tier,
                        allowed_tier,
                        reason,
                    } => {
                        return bash_ask(
                            &input.command,
                            "capability_escalation",
                            &format!(
                                "command requires {} capability but workspace allows {}; reason={}",
                                required_tier.as_str(),
                                allowed_tier.as_str(),
                                reason.as_str(),
                            ),
                            vec![
                                format!("capability.required={}", required_tier.as_str()),
                                format!("capability.allowed={}", allowed_tier.as_str()),
                                format!("capability.reason={}", reason.as_str()),
                            ],
                        );
                    }
                    CapabilityCheckOutcome::Denied {
                        required_tier,
                        allowed_tier,
                        reason,
                    } => {
                        return bash_deny(
                            "capability_denied",
                            &format!(
                                "requires {} but workspace allows {}; reason={}",
                                required_tier.as_str(),
                                allowed_tier.as_str(),
                                reason.as_str(),
                            ),
                            crate::tool::definition::PermissionDecisionReason::Safety,
                        );
                    }
                }
            } else {
                return bash_ask(
                    &input.command,
                    "policy_escalation",
                    &format_policy_warning(&policy),
                    policy.escalation_reasons.clone(),
                );
            }
        }

        PermissionDecision::Allow
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let input = parse_input(&call.input)?;
        let timeout_ms = input.timeout.unwrap_or(120_000).min(600_000);
        let policy = if input.dangerously_disable_sandbox {
            SandboxPolicy::Disabled
        } else {
            evaluate_bash_policy(&input.command).sandbox_policy
        };
        let cwd = resolve_cwd(permissions)?;
        let sandbox_config = permissions.sandbox_config();

        if input.run_in_background {
            return launch_background_command(&input, permissions, &cwd, policy, sandbox_config)
                .await;
        }

        let output = timeout(
            Duration::from_millis(timeout_ms),
            execute_with_sandbox_config(&input.command, &cwd, policy, sandbox_config),
        )
        .await
        .map_err(|_| anyhow::anyhow!("bash command timed out after {timeout_ms}ms"))??;

        Ok(ToolResult::Text(format_output(
            &input, output, &cwd, policy,
        )))
    }
}

fn workspace_permission_for_bash_tier(tier: CapabilityTier) -> WorkspacePermissionLevel {
    match tier {
        CapabilityTier::Read => WorkspacePermissionLevel::View,
        CapabilityTier::Write => WorkspacePermissionLevel::Worker,
        CapabilityTier::AdminBash => WorkspacePermissionLevel::Admin,
    }
}

fn parse_input(raw: &str) -> anyhow::Result<BashInput> {
    serde_json::from_str(raw).map_err(|error| anyhow::anyhow!("invalid bash input: {error}"))
}

pub fn always_allow_rule_for_tool_input(raw: &str) -> Option<String> {
    let input = parse_input(raw).ok()?;
    always_allow_rule_for_command(&input.command)
}

fn always_allow_rule_for_command(command: &str) -> Option<String> {
    const MULTI_COMMAND_PREFIXES: &[&str] = &[
        "cargo", "git", "npm", "pnpm", "yarn", "bun", "uv", "python", "python3", "pip", "brew",
        "docker", "kubectl", "go",
    ];

    let normalized = normalized_command_variants(command)
        .into_iter()
        .filter(|variant| !variant.trim().is_empty())
        .min_by_key(|variant| variant.len())?;
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let executable = *tokens.first()?;
    let prefix = if MULTI_COMMAND_PREFIXES.contains(&executable) {
        tokens
            .get(1)
            .filter(|token| is_allow_rule_subcommand_token(token))
            .map(|token| format!("{executable} {token}"))
            .unwrap_or_else(|| executable.to_string())
    } else {
        executable.to_string()
    };

    Some(match tokens.len() {
        0 => return None,
        1 => prefix,
        _ if prefix.contains(' ') => prefix,
        _ => format!("{prefix} *"),
    })
}

fn is_allow_rule_subcommand_token(token: &str) -> bool {
    !token.starts_with('-')
        && !token.starts_with('.')
        && !token.starts_with('/')
        && !token.contains('=')
}

fn format_bash_warning(code: &str, warning: &str) -> String {
    format!("bash command warning [{code}]: {warning}")
}

fn format_bash_denial(code: &str, warning: &str) -> String {
    format!("bash command denied [{code}]: {warning}")
}

fn format_bash_approval_detail(command: &str, warning: &str) -> String {
    format!(
        "Run: {}\nReason: {}\nAction: choose an approval option below",
        command.trim(),
        humanize_bash_warning(warning)
    )
}

fn bash_ask(
    command: &str,
    code: &str,
    warning: &str,
    escalation_reasons: Vec<String>,
) -> PermissionDecision {
    PermissionDecision::Ask {
        message: format_bash_warning(code, warning),
        reason: crate::tool::definition::PermissionDecisionReason::Safety,
        metadata: Some(PermissionApprovalMetadata {
            code: Some(code.to_string()),
            summary: Some("Bash pending approval".into()),
            detail: Some(format_bash_approval_detail(command, warning)),
            approval_kind: Some("tool_permission".into()),
            escalation_reasons,
        }),
    }
}

fn bash_deny(
    code: &str,
    warning: &str,
    reason: crate::tool::definition::PermissionDecisionReason,
) -> PermissionDecision {
    PermissionDecision::Deny {
        message: format_bash_denial(code, warning),
        reason,
    }
}

fn format_policy_warning(policy: &permissions::BashPolicyDecision) -> String {
    let reasons = humanize_policy_reasons(&policy.escalation_reasons);
    let sandbox_note = match policy.sandbox_policy {
        SandboxPolicy::Disabled => " It would run outside the sandbox.",
        SandboxPolicy::WorkspaceWrite => " It would run with workspace write access.",
        SandboxPolicy::ReadOnly => " It would run in read-only mode.",
    };
    if reasons.is_empty() {
        format!("This command needs explicit approval before it can run.{sandbox_note}")
    } else {
        format!(
            "This command needs approval because it {}.{sandbox_note}",
            human_join(&reasons)
        )
    }
}

fn humanize_bash_warning(warning: &str) -> String {
    let trimmed = warning.trim();
    if trimmed.eq_ignore_ascii_case("command requires approval by explicit Bash policy rule") {
        return "This workspace is configured to ask before running this Bash command.".into();
    }
    if trimmed.eq_ignore_ascii_case("command requests disabling sandbox protections") {
        return "This command wants to run outside the sandbox, so it needs explicit approval."
            .into();
    }
    if trimmed.contains("workspace allows") && trimmed.contains("capability") {
        return "This command needs more system access than the current workspace policy allows by default.".into();
    }
    let sentence = trimmed
        .strip_prefix("bash command warning:")
        .unwrap_or(trimmed)
        .trim();
    if sentence.starts_with("This command") {
        sentence.to_string()
    } else {
        sentence
            .chars()
            .next()
            .map(|first| first.to_uppercase().collect::<String>() + &sentence[first.len_utf8()..])
            .unwrap_or_default()
    }
}

fn humanize_policy_reasons(reasons: &[String]) -> Vec<String> {
    let mut humanized = reasons
        .iter()
        .filter_map(|reason| humanize_policy_reason(reason))
        .collect::<Vec<_>>();
    humanized.sort();
    humanized.dedup();
    humanized
}

fn humanize_policy_reason(reason: &str) -> Option<String> {
    let phrase = match reason {
        "destructive_pattern" => "looks like it can change or remove files".to_string(),
        "shell_operator.pipe" => "uses a pipe to connect multiple shell commands".to_string(),
        "shell_operator.and_if" => "chains commands with &&".to_string(),
        "shell_operator.or_if" => "chains commands with ||".to_string(),
        "shell_operator.sequence" => "sequences multiple commands with ;".to_string(),
        "shell_operator.redirect_write" => "writes output to a file with >".to_string(),
        "shell_operator.redirect_append" => "appends output to a file with >>".to_string(),
        "shell_operator.redirect_read" => "reads input through shell redirection".to_string(),
        "shell_operator.heredoc" => "uses a heredoc block".to_string(),
        "shell_operator.background" => "launches background work".to_string(),
        "command_substitution" | "command_substitution.backtick" => {
            "evaluates another command inside the shell".to_string()
        }
        "path.parent_traversal" => "uses .. path traversal".to_string(),
        "path.policy_denied" => "touches a path outside the allowed workspace policy".to_string(),
        "path.absolute_outside_workspace" => {
            "uses an absolute path outside the current workspace".to_string()
        }
        _ if reason.starts_with("unsafe:") => {
            let path = reason.trim_start_matches("unsafe:").trim();
            format!("references `{path}`")
        }
        _ if reason.starts_with("sed:") => {
            let detail = reason.trim_start_matches("sed:").replace('_', " ");
            format!("uses a risky sed edit ({detail})")
        }
        _ => return None,
    };
    Some(phrase)
}

fn human_join(parts: &[String]) -> String {
    match parts {
        [] => String::new(),
        [only] => only.clone(),
        [left, right] => format!("{left} and {right}"),
        _ => {
            let mut rendered = parts[..parts.len() - 1].join(", ");
            rendered.push_str(", and ");
            rendered.push_str(&parts[parts.len() - 1]);
            rendered
        }
    }
}

async fn launch_background_command(
    input: &BashInput,
    permissions: &ToolPermissionContext,
    cwd: &std::path::Path,
    policy: SandboxPolicy,
    sandbox_config: std::sync::Arc<crate::security::sandbox_config::SandboxConfig>,
) -> anyhow::Result<ToolResult> {
    let task_manager = permissions
        .task_manager
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("background bash execution requires task manager"))?
        .clone();
    let session_id = permissions
        .active_session_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("background bash execution requires active session id"))?;
    let owner_surface = permissions
        .active_surface
        .unwrap_or(crate::bootstrap::InteractionSurface::Cli);
    let task = task_manager.create_with_type(
        input
            .description
            .clone()
            .unwrap_or_else(|| format!("bash: {}", input.command.trim())),
        crate::task::types::TaskType::LocalBash,
        session_id,
        owner_surface,
    );

    let command = input.command.clone();
    let description = input.description.clone();
    let cwd = cwd.to_path_buf();
    let timeout_ms = input.timeout.unwrap_or(120_000).min(600_000);
    let dispatcher = permissions
        .notification_dispatcher
        .clone()
        .unwrap_or_default();
    let manager = task_manager.clone();
    let task_id = task.id.clone();
    task_manager.launch(&task.id, command.clone(), async move {
        let result =
            run_background_process(&command, &cwd, policy, timeout_ms, sandbox_config).await;
        match result {
            Ok(output) => {
                manager.append_output(
                    &task_id,
                    format_output_background(
                        description.as_deref(),
                        &command,
                        &cwd,
                        policy,
                        output,
                    ),
                );
                manager.complete(&task_id, &dispatcher);
            }
            Err(error) => {
                manager.append_output(
                    &task_id,
                    format!(
                        "command: {}\ncwd: {}\nsandbox_policy: {:?}\nerror: {}\n",
                        command.trim(),
                        cwd.display(),
                        policy,
                        error
                    ),
                );
                manager.fail(&task_id, &dispatcher);
            }
        }
    });

    Ok(ToolResult::Text(format!(
        "background bash task {} launched\noutput_file: {}",
        task.id, task.output_file
    )))
}

async fn run_background_process(
    command: &str,
    cwd: &std::path::Path,
    policy: SandboxPolicy,
    timeout_ms: u64,
    sandbox_config: std::sync::Arc<crate::security::sandbox_config::SandboxConfig>,
) -> anyhow::Result<ClampedProcessOutput> {
    timeout(
        Duration::from_millis(timeout_ms),
        execute_with_sandbox_config(command, cwd, policy, sandbox_config),
    )
    .await
    .map_err(|_| anyhow::anyhow!("bash command timed out after {timeout_ms}ms"))?
}

fn format_output_background(
    description: Option<&str>,
    command: &str,
    cwd: &std::path::Path,
    policy: SandboxPolicy,
    output: ClampedProcessOutput,
) -> String {
    let temp_input = BashInput {
        command: command.to_string(),
        timeout: None,
        description: description.map(str::to_string),
        run_in_background: true,
        dangerously_disable_sandbox: matches!(policy, SandboxPolicy::Disabled),
    };
    format!("{}\n", format_output(&temp_input, output, cwd, policy))
}

fn resolve_cwd(permissions: &ToolPermissionContext) -> anyhow::Result<PathBuf> {
    if let Some(session_id) = &permissions.active_session_id {
        if let Some(registry) = &permissions.inherited_tool_registry {
            let _ = registry.all_metadata();
        }
        if session_id.is_empty() {
            anyhow::bail!("active session id cannot be empty");
        }
    }
    std::env::current_dir().map_err(|error| anyhow::anyhow!("failed to resolve cwd: {error}"))
}

fn format_output(
    input: &BashInput,
    output: ClampedProcessOutput,
    cwd: &std::path::Path,
    _policy: SandboxPolicy,
) -> String {
    let stdout = clamped_to_string(output.stdout).trim().to_string();
    let stderr = clamped_to_string(output.stderr).trim().to_string();
    let status = output
        .status
        .code()
        .map_or_else(|| "signal".to_string(), |code| code.to_string());

    let mut parts = Vec::new();
    if let Some(description) = input
        .description
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(format!("description: {}", description.trim()));
    }
    parts.push(format!("command: {}", input.command.trim()));
    parts.push(format!(
        "normalized_variants: {:?}",
        normalized_command_variants(&input.command)
    ));
    parts.push(format!("cwd: {}", cwd.display()));
    parts.push(format!("sandbox_policy: {:?}", output.sandbox_policy));
    parts.push(format!("sandbox_enabled: {}", output.sandbox_enabled));
    parts.push(format!("sandbox_runner: {}", output.runner.as_str()));
    parts.push(format!("exit_code: {status}"));
    if !stdout.is_empty() {
        parts.push(format!("stdout:\n{stdout}"));
    }
    if !stderr.is_empty() {
        parts.push(format!("stderr:\n{stderr}"));
    }
    if stdout.is_empty() && stderr.is_empty() {
        parts.push("no output".into());
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        always_allow_rule_for_tool_input, format_bash_approval_detail, format_policy_warning,
    };
    use crate::tool::builtin::bash::permissions::BashPolicyDecision;
    use crate::tool::builtin::bash::sandbox::SandboxPolicy;

    #[test]
    fn bash_approval_detail_uses_run_and_human_reason_lines() {
        let detail = format_bash_approval_detail(
            "find . -type f | head",
            "This command needs approval because it uses a pipe to connect multiple shell commands. It would run with workspace write access.",
        );
        assert!(detail.contains("Run: find . -type f | head"));
        assert!(detail.contains("Reason: This command needs approval because it uses a pipe to connect multiple shell commands."));
        assert!(detail.contains("Action: choose an approval option below"));
        assert!(!detail.contains("command:"));
        assert!(!detail.contains("reason:"));
    }

    #[test]
    fn bash_policy_warning_is_humanized() {
        let warning = format_policy_warning(&BashPolicyDecision {
            read_only: false,
            safe_in_plan_mode: false,
            path_safe: true,
            requires_escalation: true,
            sandbox_policy: SandboxPolicy::WorkspaceWrite,
            shell_operators: vec!["|".into()],
            path_findings: Vec::new(),
            sed_safe: true,
            escalation_reasons: vec!["shell_operator.pipe".into()],
        });
        assert!(warning.contains("uses a pipe to connect multiple shell commands"));
        assert!(warning.contains("workspace write access"));
        assert!(!warning.contains("sandbox=WorkspaceWrite"));
    }

    #[test]
    fn bash_allow_rule_prefers_command_family() {
        assert_eq!(
            always_allow_rule_for_tool_input(r#"{"command":"find . -type f | head"}"#),
            Some("find *".into())
        );
        assert_eq!(
            always_allow_rule_for_tool_input(r#"{"command":"cargo test --lib"}"#),
            Some("cargo test".into())
        );
    }
}

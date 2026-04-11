use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::time::timeout;

use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

pub mod path_validation;
pub mod permissions;
pub mod readonly_validation;
pub mod sandbox;
pub mod security;
pub mod sed_validation;

use permissions::evaluate_bash_policy;
use sandbox::{execute_with_sandbox, SandboxPolicy};

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
            name: "Bash",
            description: "Execute shell commands with policy checks",
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
        if input.run_in_background {
            anyhow::bail!("background bash execution is not implemented yet")
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

        let policy = evaluate_bash_policy(&input.command);

        if permissions
            .always_deny_rules
            .iter()
            .any(|rule| rule == self.metadata().name || rule == call.name.as_str())
        {
            return PermissionDecision::Deny {
                message: "tool Bash denied by explicit rule".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Rule,
            };
        }

        if matches!(permissions.mode, PermissionMode::Plan) && !policy.safe_in_plan_mode {
            return PermissionDecision::Deny {
                message: "bash command is not allowed in plan mode".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Mode,
            };
        }

        if permissions
            .always_allow_rules
            .iter()
            .any(|rule| rule == self.metadata().name || rule == call.name.as_str())
        {
            return PermissionDecision::Allow;
        }

        if input.dangerously_disable_sandbox {
            return PermissionDecision::Ask {
                message: "bash command requests disabling sandbox protections".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Safety,
            };
        }

        if policy.requires_escalation {
            return PermissionDecision::Ask {
                message: "bash command requires explicit approval due to shell semantics or path risk"
                    .into(),
                reason: crate::tool::definition::PermissionDecisionReason::Safety,
            };
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
        let output = timeout(
            Duration::from_millis(timeout_ms),
            execute_with_sandbox(&input.command, &cwd, policy),
        )
        .await
        .map_err(|_| anyhow::anyhow!("bash command timed out after {timeout_ms}ms"))??;

        Ok(ToolResult::Text(format_output(&input, output, &cwd, policy)))
    }
}

fn parse_input(raw: &str) -> anyhow::Result<BashInput> {
    serde_json::from_str(raw).map_err(|error| anyhow::anyhow!("invalid bash input: {error}"))
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
    output: std::process::Output,
    cwd: &std::path::Path,
    policy: SandboxPolicy,
) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
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
    parts.push(format!("cwd: {}", cwd.display()));
    parts.push(format!("sandbox_policy: {:?}", policy));
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

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::definition::{PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};

pub mod command_helpers;
pub mod path_validation;
pub mod permissions;
pub mod readonly_validation;
pub mod sandbox;
pub mod security;
pub mod sed_validation;

use command_helpers::{command_matches_rule, normalized_command_variants};
use permissions::evaluate_bash_policy;
use sandbox::{SandboxPolicy, execute_with_sandbox};

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

        let policy = evaluate_bash_policy(&input.command);
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
            return PermissionDecision::Deny {
                message: "tool Bash denied by explicit rule".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Rule,
            };
        }

        if matches!(permissions.mode(), PermissionMode::Plan) && !policy.safe_in_plan_mode {
            return PermissionDecision::Deny {
                message: "bash command is not allowed in plan mode".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Mode,
            };
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
            return PermissionDecision::Ask {
                message: "bash command requests disabling sandbox protections".into(),
                reason: crate::tool::definition::PermissionDecisionReason::Safety,
            };
        }

        match crate::tool::classifier::auto_classifier::classify_bash_command(&input.command) {
            crate::tool::classifier::auto_classifier::ClassifierDecision::Deny(message) => {
                return PermissionDecision::Deny {
                    message,
                    reason: crate::tool::definition::PermissionDecisionReason::Safety,
                };
            }
            crate::tool::classifier::auto_classifier::ClassifierDecision::Ask(message) => {
                return PermissionDecision::Ask {
                    message,
                    reason: crate::tool::definition::PermissionDecisionReason::Safety,
                };
            }
            crate::tool::classifier::auto_classifier::ClassifierDecision::Allow => {}
        }

        if policy.requires_escalation {
            return PermissionDecision::Ask {
                message:
                    "bash command requires explicit approval due to shell semantics or path risk"
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

        if input.run_in_background {
            return launch_background_command(&input, permissions, &cwd, policy).await;
        }

        let output = timeout(
            Duration::from_millis(timeout_ms),
            execute_with_sandbox(&input.command, &cwd, policy),
        )
        .await
        .map_err(|_| anyhow::anyhow!("bash command timed out after {timeout_ms}ms"))??;

        Ok(ToolResult::Text(format_output(
            &input, output, &cwd, policy,
        )))
    }
}

fn parse_input(raw: &str) -> anyhow::Result<BashInput> {
    serde_json::from_str(raw).map_err(|error| anyhow::anyhow!("invalid bash input: {error}"))
}

async fn launch_background_command(
    input: &BashInput,
    permissions: &ToolPermissionContext,
    cwd: &std::path::Path,
    policy: SandboxPolicy,
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
        let result = run_background_process(&command, &cwd, policy, timeout_ms).await;
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
) -> anyhow::Result<std::process::Output> {
    let mut process = Command::new("/bin/sh");
    process
        .arg("-lc")
        .arg(command)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("RUST_AGENT_SANDBOX_POLICY", format!("{:?}", policy));

    let mut child = process
        .spawn()
        .map_err(|error| anyhow::anyhow!("failed to spawn background bash command: {error}"))?;

    let stdout_task = child.stdout.take().map(|mut stdout| {
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let _ = stdout.read_to_end(&mut buffer).await;
            buffer
        })
    });
    let stderr_task = child.stderr.take().map(|mut stderr| {
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let _ = stderr.read_to_end(&mut buffer).await;
            buffer
        })
    });

    let status = timeout(Duration::from_millis(timeout_ms), child.wait())
        .await
        .map_err(|_| anyhow::anyhow!("bash command timed out after {timeout_ms}ms"))??;
    let stdout = match stdout_task {
        Some(task) => task
            .await
            .map_err(|error| anyhow::anyhow!("stdout join failed: {error}"))?,
        None => Vec::new(),
    };
    let stderr = match stderr_task {
        Some(task) => task
            .await
            .map_err(|error| anyhow::anyhow!("stderr join failed: {error}"))?,
        None => Vec::new(),
    };

    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

fn format_output_background(
    description: Option<&str>,
    command: &str,
    cwd: &std::path::Path,
    policy: SandboxPolicy,
    output: std::process::Output,
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
    parts.push(format!(
        "normalized_variants: {:?}",
        normalized_command_variants(&input.command)
    ));
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

use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct McpCommand;

#[async_trait]
impl Command for McpCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "mcp".into(),
            description: "Manage MCP servers".into(),
            source: CommandSource::Mcp,
            category: "integration".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let runtime = app_state
            .mcp_runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MCP runtime is unavailable"))?;
        let args = input.command_args.trim();

        if args.is_empty() || args == "list" || args == "status" {
            let servers = runtime.list_servers().await;
            let config_load = runtime.config_load_result();
            let governance_load = runtime.governance_load_result();
            let mut lines = vec![
                "MCP servers:".to_string(),
                format!("  config source: {}", config_load.source.as_str()),
                format!("  config path: {}", config_load.path.display()),
                format!("  governance source: {}", governance_load.source.as_str()),
                format!("  governance path: {}", governance_load.path.display()),
            ];
            for diagnostic in &config_load.diagnostics {
                lines.push(format!("  diagnostic: {}", diagnostic));
            }
            for diagnostic in &governance_load.diagnostics {
                lines.push(format!("  governance_diagnostic: {}", diagnostic));
            }
            for server in servers {
                lines.push(String::new());
                lines.push(format!("- {} ({})", server.config.name, server.config.id));
                lines.push(format!("  status: {}", server.status.as_str()));
                lines.push(format!("  transport: {}", server.config.transport.as_str()));
                lines.push(format!(
                    "  command: {} {}",
                    server.config.command,
                    server.config.args.join(" ").trim()
                ));
                lines.push(format!(
                    "  protocol: {}{}",
                    if server.protocol_initialized {
                        "initialized"
                    } else {
                        "not-initialized"
                    },
                    server
                        .server_protocol_version
                        .as_deref()
                        .map(|value| format!(" ({value})"))
                        .unwrap_or_default()
                ));
                if let Some(pid) = server.pid {
                    lines.push(format!("  pid: {}", pid));
                }
                if server.server_name.is_some() || server.server_version.is_some() {
                    lines.push(format!(
                        "  peer: {}{}",
                        server.server_name.as_deref().unwrap_or("unknown"),
                        server
                            .server_version
                            .as_deref()
                            .map(|value| format!(" v{value}"))
                            .unwrap_or_default()
                    ));
                }
                if !server.server_capabilities.is_empty() {
                    lines.push(format!("  capabilities: {}", server.server_capabilities));
                }
                lines.push(format!(
                    "  inventory: tools={}, resources={}",
                    server.tool_count, server.resource_count
                ));
                if !server.tool_names_preview.is_empty() {
                    lines.push(format!("  tools: {}", server.tool_names_preview.join(", ")));
                }
                if !server.resource_names_preview.is_empty() {
                    lines.push(format!(
                        "  resources: {}",
                        server.resource_names_preview.join(", ")
                    ));
                }
                if let Some(error) = server
                    .last_error
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                {
                    lines.push(format!("  last_error: {}", error.trim()));
                }
                if let Some(failure) = server.last_failure.as_ref() {
                    lines.push(format!(
                        "  last_failure: operation={}, code={}",
                        failure.operation.as_str(),
                        failure.code.as_str()
                    ));
                }
                lines.push(format!(
                    "  governance: status={}, source={}, risk={}",
                    server.governance.approval_status.as_str(),
                    server.governance.approval_source.as_str(),
                    server.governance.classification.risk_level.as_str()
                ));
                lines.push(format!(
                    "  governance_reasons: {}",
                    server.governance.classification.reasons.join(", ")
                ));
                lines.push(format!(
                    "  governance_summary: {}",
                    server.governance.classification.summary
                ));
                if let Some(fingerprint) = server.governance.approved_fingerprint {
                    lines.push(format!("  approved_fingerprint: {}", fingerprint));
                }
            }
            return Ok(CommandResult::Message(lines.join("\n")));
        }

        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or_default();
        let remainder = parts.collect::<Vec<_>>();
        let server = remainder.join(" ");
        if server.trim().is_empty() {
            return Ok(CommandResult::Message(
                "Usage: /mcp [list|status|connect <server>|disconnect <server>|reconnect <server>|approve <server>|deny <server> [reason]]"
                    .to_string(),
            ));
        }

        let result = match action {
            "connect" => runtime.connect(server.trim()).await.map(|state| {
                format!(
                    "Connected MCP server {} ({}) via {}. protocol={}{}{}; tools={}; resources={}",
                    state.config.name,
                    state.config.id,
                    state.config.transport.as_str(),
                    if state.protocol_initialized { "initialized" } else { "not-initialized" },
                    state
                        .server_protocol_version
                        .as_deref()
                        .map(|value| format!("/{value}"))
                        .unwrap_or_default(),
                    state
                        .server_name
                        .as_deref()
                        .map(|value| format!("; peer={value}"))
                        .unwrap_or_default(),
                    state.tool_count,
                    state.resource_count
                )
            }),
            "disconnect" => runtime.disconnect(server.trim()).await.map(|state| {
                format!(
                    "Disconnected MCP server {} ({}).",
                    state.config.name, state.config.id
                )
            }),
            "reconnect" => runtime.reconnect(server.trim()).await.map(|state| {
                format!(
                    "Reconnected MCP server {} ({}) via {}. protocol={}{}{}; tools={}; resources={}",
                    state.config.name,
                    state.config.id,
                    state.config.transport.as_str(),
                    if state.protocol_initialized { "initialized" } else { "not-initialized" },
                    state
                        .server_protocol_version
                        .as_deref()
                        .map(|value| format!("/{value}"))
                        .unwrap_or_default(),
                    state
                        .server_name
                        .as_deref()
                        .map(|value| format!("; peer={value}"))
                        .unwrap_or_default(),
                    state.tool_count,
                    state.resource_count
                )
            }),
            "approve" => {
                let cwd = app_state.current_working_directory();
                runtime.approve_server(server.trim(), &cwd).await.map(|(state, path)| {
                    format!(
                        "Approved MCP server {} ({}). governance_path={}; fingerprint={}",
                        state.config.name,
                        state.config.id,
                        path.display(),
                        state.governance.approved_fingerprint.unwrap_or_default()
                    )
                })
            }
            "deny" => {
                let cwd = app_state.current_working_directory();
                let target = remainder.first().copied().unwrap_or_default();
                let reason = remainder.iter().skip(1).copied().collect::<Vec<_>>().join(" ");
                runtime
                    .deny_server(
                        target,
                        &cwd,
                        (!reason.trim().is_empty()).then_some(reason),
                    )
                    .await
                    .map(|(state, path)| {
                        format!(
                            "Denied MCP server {} ({}). governance_path={}; status={}",
                            state.config.name,
                            state.config.id,
                            path.display(),
                            state.governance.approval_status.as_str()
                        )
                    })
            }
            _ => anyhow::bail!("Unknown /mcp action: {action}"),
        }?;

        Ok(CommandResult::Message(result))
    }
}

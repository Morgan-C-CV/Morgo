use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct McpCommand;

#[async_trait]
impl Command for McpCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "mcp",
            description: "Manage MCP servers",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &[],
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
            let mut lines = vec![
                "MCP servers:".to_string(),
                format!("  config source: {}", config_load.source.as_str()),
                format!("  config path: {}", config_load.path.display()),
            ];
            for diagnostic in &config_load.diagnostics {
                lines.push(format!("  diagnostic: {}", diagnostic));
            }
            for server in servers {
                lines.push(String::new());
                lines.push(format!("- {} ({})", server.config.name, server.config.id));
                lines.push(format!("  status: {}", server.status.as_str()));
                lines.push(format!("  transport: {}", server.config.transport.as_str()));
                lines.push(format!("  command: {} {}", server.config.command, server.config.args.join(" ").trim()));
                lines.push(format!(
                    "  protocol: {}{}",
                    if server.protocol_initialized { "initialized" } else { "not-initialized" },
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
                lines.push(format!(
                    "  inventory: tools={}, resources={}",
                    server.tool_count, server.resource_count
                ));
                if let Some(error) = server.last_error.as_deref().filter(|value| !value.trim().is_empty()) {
                    lines.push(format!("  last_error: {}", error.trim()));
                }
            }
            return Ok(CommandResult::Message(lines.join("\n")));
        }

        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or_default();
        let server = parts.collect::<Vec<_>>().join(" ");
        if server.trim().is_empty() {
            return Ok(CommandResult::Message(
                "Usage: /mcp [list|status|connect <server>|disconnect <server>|reconnect <server>]".to_string(),
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
            _ => anyhow::bail!("Unknown /mcp action: {action}"),
        }?;

        Ok(CommandResult::Message(result))
    }
}

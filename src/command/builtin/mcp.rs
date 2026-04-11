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
                format!(
                    "config_source={} path={}",
                    config_load.source.as_str(),
                    config_load.path.display()
                ),
            ];
            for diagnostic in &config_load.diagnostics {
                lines.push(format!("diagnostic: {}", diagnostic));
            }
            for server in servers {
                let error_suffix = server
                    .last_error
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| format!("; last_error={}", value.trim()))
                    .unwrap_or_default();
                lines.push(format!(
                    "- {} ({}) cmd={} transport={} status={} tools={} resources={}{}",
                    server.config.name,
                    server.config.id,
                    server.config.command,
                    server.config.transport.as_str(),
                    server.status.as_str(),
                    server.tool_count,
                    server.resource_count,
                    error_suffix
                ));
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
                    "Connected MCP server {} ({}) with {} tools and {} resources.",
                    state.config.name, state.config.id, state.tool_count, state.resource_count
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
                    "Reconnected MCP server {} ({}) with {} tools and {} resources.",
                    state.config.name, state.config.id, state.tool_count, state.resource_count
                )
            }),
            _ => anyhow::bail!("Unknown /mcp action: {action}"),
        }?;

        Ok(CommandResult::Message(result))
    }
}

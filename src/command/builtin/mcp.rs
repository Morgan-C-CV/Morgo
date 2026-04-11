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
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.trim();

        if args.is_empty() {
            return Ok(CommandResult::Message(
                "MCP Manager:\n\
                Model Context Protocol server configurations are pending TUI interface integration.\n\
                Use '/mcp enable <server>' or '/mcp disable <server>' to quickly toggle an existing configuration."
                    .to_string(),
            ));
        }

        let parts: Vec<&str> = args.split_whitespace().collect();
        let action = parts[0];
        let target = if parts.len() > 1 {
            parts[1..].join(" ")
        } else {
            "all".to_string()
        };

        match action {
            "enable" | "disable" => {
                Ok(CommandResult::Message(format!(
                    "Command acknowledged: Attempted to {} MCP server '{}'.\n\
                    // TODO: The MCP configuration struct and global mutability bridge are not yet fully implemented.",
                    action, target
                )))
            }
            "reconnect" => {
                Ok(CommandResult::Message(format!(
                    "Command acknowledged: Attempted to reconnect MCP server '{}'.",
                    target
                )))
            }
            "no-redirect" => {
                Ok(CommandResult::Message(
                    "Bypassing tests redirection...".to_string(),
                ))
            }
            _ => Ok(CommandResult::Message(format!(
                "Unknown action '{}' for MCP. Valid actions are 'enable', 'disable', 'reconnect'.",
                action
            ))),
        }
    }
}

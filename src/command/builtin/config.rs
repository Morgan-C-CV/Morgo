use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ConfigCommand;

#[async_trait]
impl Command for ConfigCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "config",
            description: "Open config panel to change models and settings",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &["settings", "model"],
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
        if input.command_args.is_empty() {
            Ok(CommandResult::Message(
                "Config & Model switching:\nCurrently running in standard mode. \nTo change models (e.g. from Claude-3.5-Sonnet to Haiku for fast tasks), restart the agent with appropriate environment variables.\n(Dynamic TUI model switching requires 'LocalTui' component rendering interface)".into(),
            ))
        } else {
            Ok(CommandResult::Message(format!(
                "Cannot dynamically set model/config to '{}'. TUI component bridge not yet implemented.",
                input.command_args
            )))
        }
    }
}

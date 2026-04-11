use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct HelpCommand;

#[async_trait]
impl Command for HelpCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "help",
            description: "Show the available commands",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &["h"],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "Available commands: /help, /cost, /compact, /plan, /permissions".into(),
        ))
    }
}

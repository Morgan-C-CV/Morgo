use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ClearCommand;

#[async_trait]
impl Command for ClearCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "clear",
            description: "Clear conversation history and free up context",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &["c", "reset", "new"],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "\x1b[2J\x1b[3J\x1b[HConversation cleared.".into(),
        ))
    }
}

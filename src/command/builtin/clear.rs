use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ClearCommand;

#[async_trait]
impl Command for ClearCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "clear".into(),
            description: "Clear conversation history and free up context".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["c".into(), "reset".into(), "new".into()],
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

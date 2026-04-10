use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct CompactCommand;

#[async_trait]
impl Command for CompactCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "compact",
            description: "Compact the current conversation state",
            command_type: CommandType::Prompt,
            availability: CommandAvailability::Everywhere,
            aliases: &[],
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
        Ok(CommandResult::Prompt(
            "Please compact the current conversation while preserving relevant context.".into(),
        ))
    }
}

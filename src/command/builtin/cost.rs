use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct CostCommand;

#[async_trait]
impl Command for CostCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "cost",
            description: "Show current session cost summary",
            command_type: CommandType::Local,
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
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            app_state.cost_tracker.format_report(),
        ))
    }
}

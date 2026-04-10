use async_trait::async_trait;

use crate::command::types::{Command, CommandResult};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct HelpCommand;

#[async_trait]
impl Command for HelpCommand {
    fn name(&self) -> &'static str {
        "help"
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "Available commands: /help, /cost, /compact".into(),
        ))
    }
}

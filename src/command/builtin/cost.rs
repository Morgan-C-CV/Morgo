use async_trait::async_trait;

use crate::command::types::{Command, CommandResult};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct CostCommand;

#[async_trait]
impl Command for CostCommand {
    fn name(&self) -> &'static str {
        "cost"
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "Cost tracking is scaffolded but not yet connected to model usage.".into(),
        ))
    }
}

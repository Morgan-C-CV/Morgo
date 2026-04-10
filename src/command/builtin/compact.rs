use async_trait::async_trait;

use crate::command::types::{Command, CommandResult};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct CompactCommand;

#[async_trait]
impl Command for CompactCommand {
    fn name(&self) -> &'static str {
        "compact"
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "Compaction pipeline is scaffolded and will be backed by reactive compaction services."
                .into(),
        ))
    }
}

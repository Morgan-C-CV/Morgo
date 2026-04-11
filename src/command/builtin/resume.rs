use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ResumeCommand;

#[async_trait]
impl Command for ResumeCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "resume",
            description: "Resume a previous conversation",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &["continue"],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let current_id = &app_state.active_session_id;
        Ok(CommandResult::Message(format!(
            "Current Session ID: {}\n\nSession auto-saves locally (SQLite/JSON persistence). To restore later:\n  rust-agent --resume <SESSION_ID>\nOptionally use --continue-session to auto-resume the latest one.",
            current_id
        )))
    }
}

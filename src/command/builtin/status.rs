use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct StatusCommand;

#[async_trait]
impl Command for StatusCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "status",
            description: "Show Claude Code status including session, role, and connectivity",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &[],
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
        let status = format!(
            "Session ID: {}\nRole: {:?}\nCost: {}\nSurface: {:?}",
            app_state.active_session_id,
            app_state.runtime_role,
            app_state.cost_tracker.format_report(),
            app_state.surface
        );
        Ok(CommandResult::Message(status))
    }
}

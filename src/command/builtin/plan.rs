use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::state::permission_context::PermissionMode;

pub struct PlanCommand;

#[async_trait]
impl Command for PlanCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "plan",
            description: "Enable plan mode or view the current session plan",
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
        let current_mode = app_state.permission_context.mode();
        if current_mode != PermissionMode::Plan {
            app_state.permission_context.set_mode(PermissionMode::Plan);
            return Ok(CommandResult::Message("Enabled plan mode.".into()));
        }
        
        Ok(CommandResult::Message("Already in plan mode. No plan written yet. (Type /plan open to view plan if supported)".into()))
    }
}

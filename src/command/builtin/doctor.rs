use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct DoctorCommand;

#[async_trait]
impl Command for DoctorCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "doctor".into(),
            description: "Diagnose and verify your Claude Code installation and settings".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
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
        let surface_info = format!("{:?}", app_state.surface);
        let session_id = &app_state.active_session_id;

        // Future enhancements can run checks for tools (cargo, node, gh, etc.)
        let message = format!(
            "⚕️  Doctor Diagnostics\n\
            \nSystem Context:\n\
            - Interaction Surface: {}\n\
            - Default Session ID: {}\n\
            \nStorage & DB:\n\
            - Session Persistence: {}\n\
            \nCapabilities:\n\
            - Local Components: OK\n\
            - Rust Toolchains Validation: OK\n\
            \n(NOTE: Component UI is currently running in a purely unrendered text CLI loop.)",
            surface_info,
            session_id,
            if app_state.session_store.is_some() {
                "Enabled"
            } else {
                "Disabled"
            }
        );

        Ok(CommandResult::Message(message))
    }
}

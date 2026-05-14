use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct SessionCommand;

#[async_trait]
impl Command for SessionCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "session".into(),
            description: "Show current session info and persistence status".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
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
        let surface = format!("{:?}", app_state.surface);
        let store_status = if app_state.session_store.is_some() {
            "Active (FileBackedSessionStore)"
        } else {
            "Inactive"
        };
        let parent_id = app_state
            .session_store
            .as_ref()
            .and_then(|store| {
                let current_id = app_state.current_session_id();
                let sessions = store.list_sessions();
                sessions
                    .into_iter()
                    .find(|session| session.session_id == current_id)
                    .and_then(|session| session.parent_session_id)
            })
            .map(|session_id| session_id.0)
            .unwrap_or_else(|| "none".into());
        let last_turn_at = app_state
            .session
            .as_ref()
            .and_then(|session| session.last_turn_at.as_deref())
            .unwrap_or("unknown");

        Ok(CommandResult::Message(format!(
            "Session Diagnostics:\n- Session ID: {}\n- Parent Session ID: {}\n- Surface: {}\n- Persistence: {}\n- Last Activity: {}\n\n(Tip: use /resume to switch sessions, or /new to start a fresh session)",
            current_id, parent_id, surface, store_status, last_turn_at
        )))
    }
}

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
        // TODO: 可补充实现 TS Reference 中的 Remote 模式终端打印 QR Code 配对能力
        let current_id = &app_state.active_session_id;
        let surface = format!("{:?}", app_state.surface);
        let store_status = if app_state.session_store.is_some() {
            "Active (FileBackedSessionStore)"
        } else {
            "Inactive"
        };

        Ok(CommandResult::Message(format!(
            "Session Diagnostics:\n- Session ID: {}\n- Surface: {}\n- Persistence: {}\n\n(Tip: use /resume to learn how to restore sessions)",
            current_id, surface, store_status
        )))
    }
}

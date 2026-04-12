use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ContextCommand;

#[async_trait]
impl Command for ContextCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "context".into(),
            description: "Manage pinned files and directories in the session context".into(),
            source: CommandSource::Coding,
            category: "context".into(),
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
        input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.trim();
        
        if args.is_empty() {
            Ok(CommandResult::Message(
                "Context Manager:\nCurrently, the agent automatically maintains the context window contextually through tool interactions.\n(TUI interactive file picker for manual pinning is pending implementation)".into(),
            ))
        } else {
            Ok(CommandResult::Message(format!(
                "Received request to pin '{}' to context. This requires semantic memory module support or the explicit Context component injection which is currently pending.",
                args
            )))
        }
    }
}

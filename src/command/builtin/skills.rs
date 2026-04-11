use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct SkillsCommand;

#[async_trait]
impl Command for SkillsCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "skills",
            description: "List available skills",
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
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "Skills Manager:\n\
            Custom skills (macros mapping to PromptCommands) require the TUI list picker to view and modify configurations dynamically.\n\
            Use the '/help' command to view natively mapped built-in skills.\n\
            \n// TODO: Add `dialoguer` logic to iterate through dynamic skills in `.claude/skills` directory."
                .to_string(),
        ))
    }
}

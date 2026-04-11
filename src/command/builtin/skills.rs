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
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let Some(skill_registry) = app_state.skill_registry.as_ref() else {
            return Ok(CommandResult::Message("No skills registry is available.".to_string()));
        };
        let cwd = app_state
            .session
            .as_ref()
            .map(|session| session.cwd.as_str())
            .unwrap_or_default();
        let skills = skill_registry.list_user_invocable(cwd);
        if skills.is_empty() {
            return Ok(CommandResult::Message("No skills discovered.".to_string()));
        }

        let mut lines = vec!["Available skills:".to_string()];
        for skill in skills {
            let when = skill
                .when_to_use
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!(" — when to use: {}", value.trim()))
                .unwrap_or_default();
            lines.push(format!("- {}: {}{}", skill.name, skill.description, when));
        }

        Ok(CommandResult::Message(lines.join("\n")))
    }
}

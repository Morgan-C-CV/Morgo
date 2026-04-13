use std::path::Path;

use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::tool::builtin::skill::load_skill_prompt;

pub struct SkillsCommand;

pub fn build_skill_commands(app_state: &AppState) -> Vec<SkillSlashCommand> {
    let Some(skill_registry) = app_state.skill_registry.as_ref() else {
        return Vec::new();
    };
    let cwd = app_state
        .session
        .as_ref()
        .map(|session| Path::new(session.cwd.as_str()))
        .unwrap_or_else(|| Path::new(""));
    skill_registry
        .list_user_invocable(cwd)
        .into_iter()
        .map(SkillSlashCommand::from_skill)
        .collect()
}

pub struct SkillSlashCommand {
    skill_name: String,
    description: String,
    category: String,
    disable_model_invocation: bool,
    aliases: Vec<String>,
}

impl SkillSlashCommand {
    pub fn from_skill(skill: crate::skills::types::SkillDefinition) -> Self {
        let description = skill.augmented_description();
        Self {
            skill_name: skill.name,
            description,
            category: "skill".into(),
            disable_model_invocation: skill.disable_model_invocation,
            aliases: skill.aliases,
        }
    }
}

#[async_trait]
impl Command for SkillSlashCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: self.skill_name.clone(),
            description: self.description.clone(),
            source: CommandSource::Skill,
            category: self.category.clone(),
            command_type: CommandType::Prompt,
            availability: CommandAvailability::Everywhere,
            aliases: self.aliases.clone(),
            is_hidden: false,
            disable_model_invocation: self.disable_model_invocation,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let Some(skill_registry) = app_state.skill_registry.as_ref() else {
            return Ok(CommandResult::Message(
                "No skills registry is available.".into(),
            ));
        };
        let cwd = app_state
            .session
            .as_ref()
            .map(|session| Path::new(session.cwd.as_str()))
            .unwrap_or_else(|| Path::new(""));
        Ok(CommandResult::Prompt(load_skill_prompt(
            skill_registry,
            cwd,
            &self.skill_name,
            input.command_args.trim(),
        )?))
    }
}

#[async_trait]
impl Command for SkillsCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "skills".into(),
            description: "List available skills".into(),
            source: CommandSource::Builtin,
            category: "integration".into(),
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
        let Some(skill_registry) = app_state.skill_registry.as_ref() else {
            return Ok(CommandResult::Message(
                "No skills registry is available.".to_string(),
            ));
        };
        let cwd = app_state
            .session
            .as_ref()
            .map(|session| Path::new(session.cwd.as_str()))
            .unwrap_or_else(|| Path::new(""));
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
            let workflow = skill
                .workflow_summary
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!(" — workflow: {}", value.trim()))
                .unwrap_or_default();
            let source = format!(" [{}]", skill.source.as_str());
            lines.push(format!(
                "- {}{}: {}{}{}",
                skill.name, source, skill.description, when, workflow
            ));
        }

        Ok(CommandResult::Message(lines.join("\n")))
    }
}

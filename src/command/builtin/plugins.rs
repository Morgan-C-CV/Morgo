use async_trait::async_trait;

use crate::command::types::{
    Command, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::plugins::types::PluginCommandDefinition;
use crate::state::app_state::AppState;

pub struct PluginSlashCommand {
    definition: PluginCommandDefinition,
}

impl PluginSlashCommand {
    pub fn new(definition: PluginCommandDefinition) -> Self {
        Self { definition }
    }
}

#[async_trait]
impl Command for PluginSlashCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: self.definition.name.clone(),
            description: self.definition.description.clone(),
            source: CommandSource::Plugin,
            category: self.definition.category.clone(),
            command_type: CommandType::Prompt,
            availability: self.definition.availability,
            aliases: self.definition.aliases.clone(),
            is_hidden: false,
            disable_model_invocation: self.definition.disable_model_invocation,
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
        let args_line = if args.is_empty() {
            "Arguments: (none)".to_string()
        } else {
            format!("Arguments: {args}")
        };
        Ok(CommandResult::Prompt(format!(
            "Loaded plugin command: {}\nPlugin: {}\nDescription: {}\n{}\nManifest: {}\n\nPlugin instructions:\n{}",
            self.definition.name,
            self.definition.plugin_name,
            self.definition.description,
            args_line,
            self.definition.manifest_path.display(),
            self.definition.prompt
        )))
    }
}

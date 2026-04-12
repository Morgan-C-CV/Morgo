use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct HelpCommand;

#[async_trait]
impl Command for HelpCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "help".into(),
            description: "Show the available commands".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["h".into()],
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
        let Some(registry) = app_state.command_registry.as_ref() else {
            return Ok(CommandResult::Message("Command registry is unavailable.".into()));
        };

        let mut metadata = registry.metadata();
        metadata.retain(|command| !command.is_hidden);
        metadata.sort_by(|left, right| {
            left.source
                .cmp(&right.source)
                .then_with(|| left.category.cmp(&right.category))
                .then_with(|| left.name.cmp(&right.name))
        });

        let mut lines = vec!["Available commands:".to_string()];
        let mut current_source = None;
        for command in metadata {
            if current_source != Some(command.source) {
                lines.push(String::new());
                lines.push(format!("{}:", command.source.display_name()));
                current_source = Some(command.source);
            }
            let aliases = if command.aliases.is_empty() {
                String::new()
            } else {
                format!(" (aliases: {})", command.aliases.join(", "))
            };
            let availability = match command.availability {
                CommandAvailability::Everywhere => String::new(),
                CommandAvailability::CliOnly => " [cli-only]".to_string(),
                CommandAvailability::RemoteSafe => " [remote-safe]".to_string(),
            };
            lines.push(format!(
                "- /{} — {} [{}:{}]{}{}",
                command.name,
                command.description,
                command.source.as_str(),
                command.category,
                aliases,
                availability
            ));
        }

        Ok(CommandResult::Message(lines.join("\n")))
    }
}

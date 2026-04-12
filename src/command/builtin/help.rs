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

        let mut lines = vec![
            "Available commands:".to_string(),
            "Legend: [type=<prompt|local>] [availability=<cli-only|remote-safe>] [source:category] [sensitive] [model_invocation=disabled] [immediate]".to_string(),
        ];
        let mut current_source = None;
        for command in &metadata {
            if current_source != Some(command.source) {
                current_source = Some(command.source);
                let source_count = metadata.iter().filter(|item| item.source == command.source).count();
                lines.push(String::new());
                lines.push(format!("{} ({source_count}):", command.source.display_name()));
            }
            let aliases = if command.aliases.is_empty() {
                String::new()
            } else {
                format!(" aliases={}", command.aliases.join(", "))
            };
            let availability = command
                .availability
                .short_label()
                .map(|label| format!(" [availability={label}]"))
                .unwrap_or_default();
            let sensitivity = if command.is_sensitive {
                " [sensitive]"
            } else {
                ""
            };
            let invocation = if command.disable_model_invocation {
                " [model_invocation=disabled]"
            } else {
                ""
            };
            let immediacy = if command.immediate {
                " [immediate]"
            } else {
                ""
            };
            lines.push(format!(
                "- /{} — {} [type={}] [{}:{}]{}{}{}{}{}",
                command.name,
                command.description,
                command.command_type.as_str(),
                command.source.as_str(),
                command.category,
                aliases,
                availability,
                sensitivity,
                invocation,
                immediacy
            ));
        }

        if let Some(plugin_load_result) = app_state.plugin_load_result.as_ref() {
            if !plugin_load_result.diagnostics.is_empty() {
                let warning_count = plugin_load_result
                    .diagnostic_count_for_severity(crate::plugins::types::PluginDiagnosticSeverity::Warning);
                let error_count = plugin_load_result
                    .diagnostic_count_for_severity(crate::plugins::types::PluginDiagnosticSeverity::Error);
                lines.push(String::new());
                lines.push(format!(
                    "Plugin diagnostics: {} issue(s) detected (warnings={}, errors={}); run /status for details.",
                    plugin_load_result.diagnostics.len(),
                    warning_count,
                    error_count
                ));
            }
        }

        if lines.len() == 2 {
            lines.push(String::new());
            lines.push("No commands are currently visible.".to_string());
        }

        Ok(CommandResult::Message(lines.join("\n")))
    }
}

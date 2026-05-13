use async_trait::async_trait;
use std::collections::BTreeMap;

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
            return Ok(CommandResult::Message(
                "Command registry is unavailable.".into(),
            ));
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
            "RustAgent is optimized for coding tasks.".to_string(),
            "Ask RustAgent to inspect code, edit files, or run verification commands."
                .to_string(),
            "Coding workflow: read/search -> edit -> verify -> approve if needed -> resume."
                .to_string(),
            String::new(),
            "Available commands:".to_string(),
            "Legend: [type=<prompt|local>] [availability=<cli-only|remote-safe>] [source:category] [sensitive] [model_invocation=disabled] [immediate]".to_string(),
        ];
        let (coding_commands, advanced_commands): (Vec<_>, Vec<_>) =
            metadata.into_iter().partition(is_coding_command);

        append_command_section(&mut lines, "Coding commands:", &coding_commands);
        append_command_section(&mut lines, "Advanced commands:", &advanced_commands);

        if let Some(plugin_load_result) = app_state.plugin_load_result.as_ref() {
            if !plugin_load_result.diagnostics.is_empty() {
                let warning_count = plugin_load_result.diagnostic_count_for_severity(
                    crate::plugins::types::PluginDiagnosticSeverity::Warning,
                );
                let error_count = plugin_load_result.diagnostic_count_for_severity(
                    crate::plugins::types::PluginDiagnosticSeverity::Error,
                );
                lines.push(String::new());
                lines.push(format!(
                    "Plugin diagnostics: {} issue(s) detected (warnings={}, errors={}); run /plugins or /status for details.",
                    plugin_load_result.diagnostics.len(),
                    warning_count,
                    error_count
                ));
            }
        }

        if coding_commands.is_empty() && advanced_commands.is_empty() {
            lines.push(String::new());
            lines.push("No commands are currently visible.".to_string());
        }

        Ok(CommandResult::Message(lines.join("\n")))
    }
}

fn is_coding_command(command: &CommandMetadata) -> bool {
    match command.name.as_str() {
        "help" | "permissions" | "resume" | "tasks" | "plan" | "status" | "session" | "model"
        | "compact" => true,
        "plugins" | "swarm" | "LisM" | "UM" | "skills" | "computer" => false,
        _ => command.source == CommandSource::Coding,
    }
}

fn append_command_section(lines: &mut Vec<String>, title: &str, commands: &[CommandMetadata]) {
    if commands.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(format!("{title} ({})", commands.len()));

    let mut grouped: BTreeMap<CommandSource, Vec<&CommandMetadata>> = BTreeMap::new();
    for command in commands {
        grouped.entry(command.source).or_default().push(command);
    }
    let show_source_headers = grouped.len() > 1;

    for (source, entries) in grouped {
        if show_source_headers {
            lines.push(format!("{} ({}):", source.display_name(), entries.len()));
        }
        for command in entries {
            lines.push(format_command_line(command));
        }
    }
}

fn format_command_line(command: &CommandMetadata) -> String {
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

    format!(
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
    )
}

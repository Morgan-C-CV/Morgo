use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct HelpCommand;

fn ansi(text: impl AsRef<str>, code: &str) -> String {
    format!("\u{1b}[{code}m{}\u{1b}[0m", text.as_ref())
}

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
            ansi("RustAgent is optimized for coding tasks.", "1;34"),
            "Ask RustAgent to inspect code, edit files, or run verification commands.".to_string(),
            format!(
                "{} {} {} {} {}",
                ansi("Coding workflow:", "1;36"),
                ansi("read/search", "2;37"),
                ansi("->", "2;36"),
                ansi("edit", "2;37"),
                ansi("-> verify -> approve if needed -> resume", "2;37")
            ),
            String::new(),
            ansi("Available commands", "1;34"),
            format!(
                "{}",
                ansi(
                    "Tags: prompt/local, cli-only, sensitive, immediate, no-model, aliases",
                    "2;37"
                )
            ),
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
                    "{} {} detected {}",
                    ansi("Plugin diagnostics:", "1;33"),
                    ansi(
                        format!(
                            "{} issue(s) (warnings={}, errors={})",
                            plugin_load_result.diagnostics.len(),
                            warning_count,
                            error_count
                        ),
                        "33"
                    ),
                    ansi("run /plugins or /status for details.", "2;37")
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
        | "compact" | "boss" => true,
        "plugins" | "swarm" | "LisM" | "UM" | "skills" | "computer" => false,
        _ => command.source == CommandSource::Coding,
    }
}

fn append_command_section(lines: &mut Vec<String>, title: &str, commands: &[CommandMetadata]) {
    if commands.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(format!(
        "{} {}",
        ansi(title, "1;36"),
        ansi(format!("({})", commands.len()), "2;37")
    ));

    let mut grouped: BTreeMap<CommandSource, Vec<&CommandMetadata>> = BTreeMap::new();
    for command in commands {
        grouped.entry(command.source).or_default().push(command);
    }
    let show_source_headers = grouped.len() > 1;

    for (source, entries) in grouped {
        if show_source_headers {
            lines.push(format!(
                "{} {}",
                ansi(format!("{}:", source.display_name()), "1;37"),
                ansi(format!("({})", entries.len()), "2;37")
            ));
        }
        for command in entries {
            lines.push(format_command_line(command));
        }
    }
}

fn format_command_line(command: &CommandMetadata) -> String {
    let mut tags = vec![
        ansi(command.command_type.as_str(), "2;36"),
        ansi(
            format!("{}:{}", command.source.as_str(), command.category),
            "2;37",
        ),
    ];

    if let Some(label) = command.availability.short_label() {
        tags.push(ansi(label, "33"));
    }
    if command.is_sensitive {
        tags.push(ansi("sensitive", "31"));
    }
    if command.disable_model_invocation {
        tags.push(ansi("no-model", "35"));
    }
    if command.immediate {
        tags.push(ansi("immediate", "32"));
    }
    if !command.aliases.is_empty() {
        tags.push(ansi(
            format!("aliases: {}", command.aliases.join(", ")),
            "2;37",
        ));
    }

    format!(
        "{} {}",
        ansi(format!("/{}", command.name), "1;34"),
        command.description
    ) + &format!("\n    {}", tags.join("  "))
}

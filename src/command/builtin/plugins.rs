use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::plugins::runtime_state::rebuild_runtime_plugin_state;
use crate::plugins::state::{load_plugin_state_with_diagnostics, write_plugin_state};
use crate::plugins::types::{
    PluginCommandDefinition, PluginDiagnostic, PluginDiagnosticSeverity, PluginGovernanceSource,
    PluginGovernanceState, PluginRuntimeApplyOutcome,
};
use crate::state::app_state::AppState;

pub struct PluginsCommand;

pub struct PluginSlashCommand {
    definition: PluginCommandDefinition,
}

impl PluginSlashCommand {
    pub fn new(definition: PluginCommandDefinition) -> Self {
        Self { definition }
    }
}

#[async_trait]
impl Command for PluginsCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "plugins".into(),
            description: "Inspect plugin inventory, diagnostics, and governance state".into(),
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
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let Some(plugin_load_result) = app_state.plugin_load_result.as_ref() else {
            return Ok(CommandResult::Message("Plugins are unavailable in this runtime.".into()));
        };
        let cwd = app_state
            .session
            .as_ref()
            .map(|session| Path::new(session.cwd.as_str()))
            .unwrap_or_else(|| Path::new("."));
        let args = input.command_args.trim();
        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or("list");

        match action {
            "" | "list" | "status" => Ok(CommandResult::Message(render_plugin_list(
                plugin_load_result.as_ref(),
            ))),
            "show" => {
                let plugin_name = parts.collect::<Vec<_>>().join(" ");
                if plugin_name.trim().is_empty() {
                    return Ok(CommandResult::Message(
                        "Usage: /plugins [list|show <plugin>|diagnostics [plugin]|reload [plugin|all]|enable <plugin>|disable <plugin> [reason]]"
                            .into(),
                    ));
                }
                let Some(plugin) = plugin_load_result
                    .plugins
                    .iter()
                    .find(|plugin| plugin.name == plugin_name.trim())
                else {
                    return Ok(CommandResult::Message(format!(
                        "Plugin not found: {}",
                        plugin_name.trim()
                    )));
                };
                let last_apply_report = if let Some(state) =
                    app_state.permission_context.runtime_plugin_state.as_ref()
                {
                    state.last_apply_report().await
                } else {
                    None
                };
                Ok(CommandResult::Message(render_plugin_show(
                    plugin,
                    &plugin_load_result.diagnostics,
                    last_apply_report,
                )))
            }
            "diagnostics" => {
                let plugin_name = parts.collect::<Vec<_>>().join(" ");
                let filtered = if plugin_name.trim().is_empty() {
                    plugin_load_result.diagnostics.clone()
                } else {
                    plugin_load_result
                        .diagnostics
                        .iter()
                        .filter(|diagnostic| diagnostic.plugin_name.as_deref() == Some(plugin_name.trim()))
                        .cloned()
                        .collect()
                };
                Ok(CommandResult::Message(render_diagnostics(
                    plugin_load_result.as_ref(),
                    plugin_name.trim(),
                    &filtered,
                )))
            }
            "reload" => {
                let target = parts.next().unwrap_or("all").trim().to_string();
                let report = rebuild_runtime_plugin_state(app_state).await?;
                Ok(CommandResult::Message(format!(
                    "Reloaded plugins for target {}. Runtime outcome={} generation={}. {}{}",
                    target,
                    report.outcome.as_str(),
                    report.generation,
                    report.message,
                    if report.orphaned_governance_entries.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " Orphaned governance entries: {}.",
                            report.orphaned_governance_entries.join(", ")
                        )
                    }
                )))
            }
            "enable" => {
                let plugin_name = parts.collect::<Vec<_>>().join(" ");
                if plugin_name.trim().is_empty() {
                    return Ok(CommandResult::Message(
                        "Usage: /plugins enable <plugin>".into(),
                    ));
                }
                let Some(plugin) = plugin_load_result
                    .plugins
                    .iter()
                    .find(|plugin| plugin.name == plugin_name.trim())
                else {
                    return Ok(CommandResult::Message(format!(
                        "Plugin not found: {}",
                        plugin_name.trim()
                    )));
                };
                let path = update_plugin_state(cwd, plugin.name.as_str(), true, None)?;
                let report = rebuild_runtime_plugin_state(app_state).await?;
                Ok(CommandResult::Message(render_governance_apply_message(
                    format!("Enabled plugin {}.", plugin.name),
                    &path,
                    &report,
                )))
            }
            "disable" => {
                let plugin_name = parts.next().unwrap_or_default().trim().to_string();
                if plugin_name.is_empty() {
                    return Ok(CommandResult::Message(
                        "Usage: /plugins disable <plugin> [reason]".into(),
                    ));
                }
                let reason = parts.collect::<Vec<_>>().join(" ");
                let Some(plugin) = plugin_load_result
                    .plugins
                    .iter()
                    .find(|plugin| plugin.name == plugin_name)
                else {
                    return Ok(CommandResult::Message(format!("Plugin not found: {plugin_name}")));
                };
                let disable_reason = if reason.trim().is_empty() {
                    None
                } else {
                    Some(reason.trim().to_string())
                };
                let path = update_plugin_state(cwd, plugin.name.as_str(), false, disable_reason.clone())?;
                let report = rebuild_runtime_plugin_state(app_state).await?;
                Ok(CommandResult::Message(render_governance_apply_message(
                    format!(
                        "Disabled plugin {}{}.",
                        plugin.name,
                        disable_reason
                            .as_deref()
                            .map(|value| format!(" (reason: {value})"))
                            .unwrap_or_default()
                    ),
                    &path,
                    &report,
                )))
            }
            _ => Ok(CommandResult::Message(format!(
                "Unknown /plugins action: {action}. Usage: /plugins [list|show <plugin>|diagnostics [plugin]|reload [plugin|all]|enable <plugin>|disable <plugin> [reason]]"
            ))),
        }
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
            immediate: self.definition.immediate,
            is_sensitive: self.definition.is_sensitive,
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

fn render_plugin_list(plugin_load_result: &crate::plugins::types::PluginLoadResult) -> String {
    let mut lines = vec![
        "Plugins:".to_string(),
        format!(
            "- discovery: {} (root={})",
            plugin_load_result.source.as_str(),
            plugin_load_result.root.display()
        ),
        format!(
            "- inventory: discovered={}, enabled={}, disabled={}, error={}",
            plugin_load_result.plugins.len(),
            plugin_load_result.active_plugin_count(),
            plugin_load_result.disabled_plugin_count(),
            plugin_load_result.error_plugin_count()
        ),
        format!(
            "- activation: commands={}, tools={}, hooks={}",
            plugin_load_result.active_command_count(),
            plugin_load_result.active_tool_count(),
            plugin_load_result.active_hook_count()
        ),
        format!(
            "- diagnostics: total={}, info={}, warnings={}, errors={}",
            plugin_load_result.diagnostics.len(),
            plugin_load_result.diagnostic_count_for_severity(PluginDiagnosticSeverity::Info),
            plugin_load_result.diagnostic_count_for_severity(PluginDiagnosticSeverity::Warning),
            plugin_load_result.diagnostic_count_for_severity(PluginDiagnosticSeverity::Error)
        ),
    ];

    if !plugin_load_result.orphaned_governance_entries.is_empty() {
        lines.push(format!(
            "- orphaned_governance_entries: {}",
            plugin_load_result.orphaned_governance_entries.join(", ")
        ));
    }

    if plugin_load_result.plugins.is_empty() {
        lines.push("- plugins: none discovered".to_string());
    } else {
        lines.push("- plugins:".to_string());
        for plugin in &plugin_load_result.plugins {
            let capabilities = if plugin.capabilities.is_empty() {
                "none".to_string()
            } else {
                plugin
                    .capabilities
                    .iter()
                    .map(|capability| capability.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            lines.push(format!(
                "  - {} v{} — state={}, applied={}, enabled={}, active(commands={}, hooks={}, tools={}), discovered(commands={}, hooks={}, tools={}), capabilities={}",
                plugin.name,
                plugin.version.as_deref().unwrap_or("unknown"),
                plugin.lifecycle_state.as_str(),
                plugin.apply_status.as_str(),
                if plugin.governance.enabled { "yes" } else { "no" },
                plugin.activation.commands,
                plugin.activation.hooks,
                plugin.activation.tools,
                plugin.commands.len(),
                plugin.hooks.len(),
                plugin.tools.len(),
                capabilities,
            ));
        }
    }

    lines.join("\n")
}

fn render_plugin_show(
    plugin: &crate::plugins::types::PluginDefinition,
    diagnostics: &[PluginDiagnostic],
    last_apply_report: Option<crate::plugins::types::PluginRuntimeApplyReport>,
) -> String {
    let capabilities = if plugin.capabilities.is_empty() {
        "none".to_string()
    } else {
        plugin
            .capabilities
            .iter()
            .map(|capability| capability.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };
    let plugin_diagnostics = diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.plugin_name.as_deref() == Some(plugin.name.as_str()))
        .collect::<Vec<_>>();
    let mut lines = vec![
        format!("Plugin: {}", plugin.name),
        format!("- version: {}", plugin.version.as_deref().unwrap_or("unknown")),
        format!("- description: {}", plugin.description),
        format!("- manifest: {}", plugin.manifest_path.display()),
        format!("- lifecycle_state: {}", plugin.lifecycle_state.as_str()),
        format!("- apply_status: {}", plugin.apply_status.as_str()),
        format!("- enabled: {}", if plugin.governance.enabled { "yes" } else { "no" }),
        format!("- governance_source: {}", plugin.governance.source.as_str()),
        format!(
            "- disable_reason: {}",
            plugin.governance.disable_reason.as_deref().unwrap_or("none")
        ),
        format!("- capabilities: {}", capabilities),
        format!(
            "- activation: commands={}, tools={}, hooks={}",
            plugin.activation.commands, plugin.activation.tools, plugin.activation.hooks
        ),
        format!(
            "- discovered: commands={}, tools={}, hooks={}",
            plugin.commands.len(), plugin.tools.len(), plugin.hooks.len()
        ),
    ];

    if let Some(metadata) = plugin.diagnostics_metadata.as_ref() {
        lines.push("- diagnostics_metadata:".to_string());
        if let Some(homepage) = metadata.homepage.as_deref() {
            lines.push(format!("  - homepage: {homepage}"));
        }
        if let Some(docs) = metadata.docs.as_deref() {
            lines.push(format!("  - docs: {docs}"));
        }
        if let Some(issues) = metadata.issues.as_deref() {
            lines.push(format!("  - issues: {issues}"));
        }
        if let Some(support_level) = metadata.support_level.as_deref() {
            lines.push(format!("  - support_level: {support_level}"));
        }
    }

    if let Some(report) = last_apply_report {
        lines.push("- runtime_apply:".to_string());
        lines.push(format!(
            "  - outcome: {}",
            report.outcome.as_str()
        ));
        lines.push(format!("  - generation: {}", report.generation));
        lines.push(format!("  - summary: {}", report.message));
    }

    lines.push(format!("- diagnostics: {}", plugin_diagnostics.len()));
    if plugin_diagnostics.is_empty() {
        lines.push("  - none".to_string());
    } else {
        for diagnostic in plugin_diagnostics {
            lines.push(format!("  - {}", diagnostic.render_line()));
        }
    }

    lines.join("\n")
}

fn render_diagnostics(
    plugin_load_result: &crate::plugins::types::PluginLoadResult,
    plugin_name: &str,
    diagnostics: &[PluginDiagnostic],
) -> String {
    let mut lines = if plugin_name.is_empty() {
        vec!["Plugin diagnostics:".to_string()]
    } else {
        vec![format!("Plugin diagnostics for {}:", plugin_name)]
    };
    lines.push(format!(
        "- total={}, info={}, warnings={}, errors={}",
        diagnostics.len(),
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == PluginDiagnosticSeverity::Info)
            .count(),
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == PluginDiagnosticSeverity::Warning)
            .count(),
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == PluginDiagnosticSeverity::Error)
            .count()
    ));

    if diagnostics.is_empty() {
        if plugin_name.is_empty() && plugin_load_result.plugins.is_empty() {
            lines.push("- no plugins discovered".to_string());
        } else {
            lines.push("- none".to_string());
        }
    } else {
        for diagnostic in diagnostics {
            lines.push(format!("- {}", diagnostic.render_line()));
        }
    }

    lines.join("\n")
}

fn render_governance_apply_message(
    prefix: String,
    path: &Path,
    report: &crate::plugins::types::PluginRuntimeApplyReport,
) -> String {
    let outcome = match report.outcome {
        PluginRuntimeApplyOutcome::Applied => "applied to the current runtime",
        PluginRuntimeApplyOutcome::RetainedPreviousSnapshot => {
            "persisted, but the previous runtime snapshot was retained"
        }
    };
    let orphaned = if report.orphaned_governance_entries.is_empty() {
        String::new()
    } else {
        format!(
            " Orphaned governance entries: {}.",
            report.orphaned_governance_entries.join(", ")
        )
    };
    format!(
        "{} Persisted governance to {} and {} (generation={}). {}{}",
        prefix,
        path.display(),
        outcome,
        report.generation,
        report.message,
        orphaned
    )
}

fn update_plugin_state(
    cwd: &Path,
    plugin_name: &str,
    enabled: bool,
    disable_reason: Option<String>,
) -> anyhow::Result<std::path::PathBuf> {
    let mut states: BTreeMap<String, PluginGovernanceState> =
        load_plugin_state_with_diagnostics(cwd).states;
    states.insert(
        plugin_name.to_string(),
        PluginGovernanceState {
            enabled,
            disable_reason,
            source: PluginGovernanceSource::File,
        },
    );
    write_plugin_state(cwd, &states)
}

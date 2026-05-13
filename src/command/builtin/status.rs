use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::core::output::OutputBlock;
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::state::permission_context::PermissionMode;

pub struct StatusCommand;

#[async_trait]
impl Command for StatusCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "status".into(),
            description: "Show Morgo status including session, role, and connectivity".into(),
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
        _input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let cwd = app_state.current_working_directory();
        let permission_mode = match app_state.permission_context.mode() {
            PermissionMode::Default => "default",
            PermissionMode::AcceptEdits => "accept_edits",
            PermissionMode::BypassPermissions => "bypass_permissions",
            PermissionMode::Plan => "plan",
        };
        let pending_approval = app_state.permission_context.pending_approval();
        let tasks = app_state
            .permission_context
            .task_manager
            .as_ref()
            .map(|manager| manager.list())
            .unwrap_or_default();
        let pending_orchestration = app_state
            .permission_context
            .task_manager
            .as_ref()
            .is_some_and(|manager| manager.has_pending_orchestration(&app_state.active_session_id));
        let running_count = tasks
            .iter()
            .filter(|task| matches!(task.status, crate::task::types::TaskStatus::Running))
            .count();
        let completed_count = tasks
            .iter()
            .filter(|task| matches!(task.status, crate::task::types::TaskStatus::Completed))
            .count();
        let failed_count = tasks
            .iter()
            .filter(|task| matches!(task.status, crate::task::types::TaskStatus::Failed))
            .count();
        let killed_count = tasks
            .iter()
            .filter(|task| matches!(task.status, crate::task::types::TaskStatus::Killed))
            .count();
        let pending_verification_count = tasks
            .iter()
            .filter(|task| {
                task.validation_state
                    == Some(crate::task::types::ValidationState::PendingVerification)
            })
            .count();
        let group_count = tasks
            .iter()
            .filter_map(|task| task.orchestration_group_id.as_deref())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let skill_count = app_state
            .skill_registry
            .as_ref()
            .map(|registry| {
                let cwd = app_state
                    .session
                    .as_ref()
                    .map(|session| std::path::Path::new(session.cwd.as_str()))
                    .unwrap_or_else(|| std::path::Path::new(""));
                registry.list_user_invocable(cwd).len()
            })
            .unwrap_or(0);
        let mcp_config = app_state
            .mcp_runtime
            .as_ref()
            .map(|runtime| runtime.config_load_result());
        let registry_total = app_state
            .command_registry
            .as_ref()
            .map(|registry| registry.metadata().len())
            .unwrap_or(0);
        let command_source_counts = app_state
            .command_registry
            .as_ref()
            .map(|registry| registry.count_by_source())
            .unwrap_or_default();
        let command_type_counts = app_state
            .command_registry
            .as_ref()
            .map(|registry| registry.count_by_type())
            .unwrap_or_default();
        let metadata = app_state
            .command_registry
            .as_ref()
            .map(|registry| registry.metadata())
            .unwrap_or_default();
        let prompt_command_count = metadata
            .iter()
            .filter(|command| command.command_type == CommandType::Prompt)
            .count();
        let immediate_command_count = metadata.iter().filter(|command| command.immediate).count();
        let sensitive_command_count = metadata
            .iter()
            .filter(|command| command.is_sensitive)
            .count();
        let model_invocation_disabled_count = metadata
            .iter()
            .filter(|command| command.disable_model_invocation)
            .count();

        // Runtime section
        let active_model_snapshot = app_state
            .active_model_runtime
            .as_ref()
            .map(|runtime| runtime.snapshot_blocking());
        let active_model_profile = active_model_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.active_profile_name.as_deref())
            .or(app_state.active_model_profile_name.as_deref())
            .unwrap_or("default");
        let active_model_source = active_model_snapshot
            .as_ref()
            .map(|snapshot| snapshot.source.as_str())
            .unwrap_or_else(|| app_state.active_model_profile_source.as_str());
        let active_model_level = active_model_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.active_level)
            .map(|level| level.as_str())
            .unwrap_or("none");
        let active_model_summary = active_model_snapshot
            .as_ref()
            .map(|snapshot| &snapshot.summary)
            .unwrap_or(&app_state.active_model_provider_summary);
        let runtime_items = vec![
            OutputBlock::kv("session_id", &app_state.active_session_id),
            OutputBlock::kv("surface", format!("{:?}", app_state.surface)),
            OutputBlock::kv("runtime_role", format!("{:?}", app_state.runtime_role)),
            OutputBlock::kv(
                "worker_role",
                app_state
                    .worker_role
                    .map(|role| role.as_str())
                    .unwrap_or("none"),
            ),
            OutputBlock::kv("cost", app_state.cost_tracker.format_report()),
            OutputBlock::kv(
                "active_model_profile",
                active_model_profile,
            ),
            OutputBlock::kv("active_model_level", active_model_level),
            OutputBlock::kv(
                "active_model_source",
                active_model_source,
            ),
            OutputBlock::kv(
                "active_model_summary",
                format!(
                    "provider_id={}, protocol={}, compatibility_profile={}, base_url_host={}, model={}, auth_status={}",
                    active_model_summary.provider_id,
                    active_model_summary.protocol,
                    active_model_summary.compatibility_profile,
                    active_model_summary.base_url_host,
                    active_model_summary.model,
                    active_model_summary.auth_status,
                ),
            ),
        ];

        // Observability section
        let observability = app_state.service_observability_tracker.snapshot();
        let mut obs_items = vec![
            OutputBlock::kv("retryable_count", observability.retryable_count.to_string()),
            OutputBlock::kv("terminal_count", observability.terminal_count.to_string()),
        ];
        if observability.by_failure_code.is_empty() {
            obs_items.push(OutputBlock::kv("by_failure_code", "none"));
        } else {
            obs_items.push(OutputBlock::section(
                "by_failure_code",
                observability
                    .by_failure_code
                    .iter()
                    .map(|(code, count)| OutputBlock::kv(code.as_str(), count.to_string()))
                    .collect(),
            ));
        }
        if observability.by_provider_kind.is_empty() {
            obs_items.push(OutputBlock::kv("by_provider_kind", "none"));
        } else {
            obs_items.push(OutputBlock::section(
                "by_provider_kind",
                observability
                    .by_provider_kind
                    .iter()
                    .map(|(kind, count)| OutputBlock::kv(kind.as_str(), count.to_string()))
                    .collect(),
            ));
        }
        if observability.compact_recovery_hits.is_empty() {
            obs_items.push(OutputBlock::kv("compact_recovery_hits", "none"));
        } else {
            obs_items.push(OutputBlock::section(
                "compact_recovery_hits",
                observability
                    .compact_recovery_hits
                    .iter()
                    .map(|(kind, count)| OutputBlock::kv(kind.as_str(), count.to_string()))
                    .collect(),
            ));
        }
        obs_items.push(OutputBlock::kv(
            "note",
            "buckets count normalized runtime failure signals, not unique error instances",
        ));

        // Commands section
        let mut cmd_items = vec![OutputBlock::kv("total", registry_total.to_string())];
        if command_source_counts.is_empty() {
            cmd_items.push(OutputBlock::kv("by_source", "none"));
        } else {
            for (source, count) in &command_source_counts {
                cmd_items.push(OutputBlock::kv(
                    format!("source {}", source.as_str()),
                    count.to_string(),
                ));
            }
        }
        if command_type_counts.is_empty() {
            cmd_items.push(OutputBlock::kv("by_type", "none"));
        } else {
            for (command_type, count) in &command_type_counts {
                cmd_items.push(OutputBlock::kv(
                    format!("type {}", command_type.as_str()),
                    count.to_string(),
                ));
            }
        }
        cmd_items.push(OutputBlock::kv(
            "contract",
            format!(
                "prompt={}, immediate={}, sensitive={}, model_invocation_disabled={}",
                prompt_command_count,
                immediate_command_count,
                sensitive_command_count,
                model_invocation_disabled_count
            ),
        ));

        // Orchestration section
        let orch_items = vec![
            OutputBlock::kv(
                "pending_orchestration",
                if pending_orchestration { "yes" } else { "no" },
            ),
            OutputBlock::kv(
                "tasks",
                format!(
                    "total={}, running={}, completed={}, failed={}, killed={}",
                    tasks.len(),
                    running_count,
                    completed_count,
                    failed_count,
                    killed_count
                ),
            ),
            OutputBlock::kv(
                "pending_verification",
                pending_verification_count.to_string(),
            ),
            OutputBlock::kv("orchestration_groups", group_count.to_string()),
        ];

        // Integrations section
        let mut integ_items = vec![OutputBlock::kv(
            "skills_registry",
            format!(
                "{} (user_invocable={})",
                if app_state.skill_registry.is_some() {
                    "available"
                } else {
                    "unavailable"
                },
                skill_count
            ),
        )];
        if let Some(config) = mcp_config {
            integ_items.push(OutputBlock::kv(
                "mcp_runtime",
                format!(
                    "available (source={}, path={}, diagnostics={})",
                    config.source.as_str(),
                    config.path.display(),
                    config.diagnostics.len()
                ),
            ));
        } else {
            integ_items.push(OutputBlock::kv("mcp_runtime", "unavailable"));
        }

        // Plugins section
        let last_apply_report = if let Some(runtime_plugin_state) =
            app_state.permission_context.runtime_plugin_state.as_ref()
        {
            runtime_plugin_state.last_apply_report().await
        } else {
            None
        };
        let mut plugin_items: Vec<OutputBlock> = Vec::new();
        if let Some(plugin_load_result) = app_state.plugin_load_result.as_ref() {
            let registered_plugin_commands = app_state
                .command_registry
                .as_ref()
                .map(|registry| {
                    registry
                        .metadata()
                        .into_iter()
                        .filter(|command| command.source == CommandSource::Plugin)
                        .count()
                })
                .unwrap_or(0);
            let registered_plugin_tools =
                if let Some(registry) = app_state.runtime_tool_registry.as_ref() {
                    registry
                        .read()
                        .await
                        .all_metadata()
                        .into_iter()
                        .filter(|tool| tool.name.starts_with("plugin."))
                        .count()
                } else {
                    0
                };
            let discovered_plugin_commands = plugin_load_result.discovered_command_count();
            let discovered_plugin_tools = plugin_load_result.discovered_tool_count();
            let discovered_plugin_hooks = plugin_load_result.discovered_hook_count();
            let enabled_plugins = plugin_load_result.active_plugin_count();
            let disabled_plugins = plugin_load_result.disabled_plugin_count();
            let error_plugins = plugin_load_result.error_plugin_count();
            let warning_count = plugin_load_result.diagnostic_count_for_severity(
                crate::plugins::types::PluginDiagnosticSeverity::Warning,
            );
            let error_count = plugin_load_result.diagnostic_count_for_severity(
                crate::plugins::types::PluginDiagnosticSeverity::Error,
            );
            let info_count = plugin_load_result.diagnostic_count_for_severity(
                crate::plugins::types::PluginDiagnosticSeverity::Info,
            );
            plugin_items.push(OutputBlock::kv(
                "plugin_discovery",
                format!(
                    "{} (root={})",
                    plugin_load_result.source.as_str(),
                    plugin_load_result.root.display()
                ),
            ));
            plugin_items.push(OutputBlock::kv(
                "discovered_plugins",
                plugin_load_result.plugins.len().to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "orphaned_governance_entries",
                plugin_load_result
                    .orphaned_governance_entries
                    .len()
                    .to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "enabled_plugins",
                enabled_plugins.to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "disabled_plugins",
                disabled_plugins.to_string(),
            ));
            plugin_items.push(OutputBlock::kv("error_plugins", error_plugins.to_string()));
            plugin_items.push(OutputBlock::kv(
                "discovered_plugin_commands",
                discovered_plugin_commands.to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "discovered_plugin_tools",
                discovered_plugin_tools.to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "discovered_plugin_hooks",
                discovered_plugin_hooks.to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "active_plugin_commands",
                plugin_load_result.active_command_count().to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "active_plugin_tools",
                plugin_load_result.active_tool_count().to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "active_plugin_hooks",
                plugin_load_result.active_hook_count().to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "registered_plugin_commands",
                registered_plugin_commands.to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "registered_plugin_tools",
                registered_plugin_tools.to_string(),
            ));
            plugin_items.push(OutputBlock::kv(
                "diagnostics",
                format!(
                    "total={}, info={}, warnings={}, errors={}",
                    plugin_load_result.diagnostics.len(),
                    info_count,
                    warning_count,
                    error_count
                ),
            ));
            if let Some(report) = last_apply_report.as_ref() {
                plugin_items.push(OutputBlock::kv(
                    "runtime_apply",
                    format!(
                        "outcome={}, generation={}",
                        report.outcome.as_str(),
                        report.generation
                    ),
                ));
                plugin_items.push(OutputBlock::kv("runtime_apply_summary", &report.message));
            }
            if !plugin_load_result.plugins.is_empty() {
                let inventory: Vec<OutputBlock> = plugin_load_result
                    .plugins
                    .iter()
                    .map(|plugin| {
                        let version = plugin.version.as_deref().unwrap_or("unknown");
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
                        let disable_reason =
                            plugin.governance.disable_reason.as_deref().unwrap_or("none");
                        OutputBlock::text(format!(
                            "{} v{} — state={}, applied={}, enabled={}, active(commands={}, hooks={}, tools={}), discovered(commands={}, hooks={}, tools={}), capabilities={}, governance_source={}, disable_reason={} (manifest={})",
                            plugin.name,
                            version,
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
                            plugin.governance.source.as_str(),
                            disable_reason,
                            plugin.manifest_path.display()
                        ))
                    })
                    .collect();
                plugin_items.push(OutputBlock::section("plugin_inventory", inventory));
            }
            if !plugin_load_result.diagnostics.is_empty() {
                let preview: Vec<OutputBlock> = plugin_load_result
                    .diagnostics
                    .iter()
                    .take(3)
                    .map(|d| OutputBlock::text(d.render_line()))
                    .collect();
                plugin_items.push(OutputBlock::section("diagnostic_preview", preview));
            }
            if !plugin_load_result.orphaned_governance_entries.is_empty() {
                let preview: Vec<OutputBlock> = plugin_load_result
                    .orphaned_governance_entries
                    .iter()
                    .take(3)
                    .map(|entry| OutputBlock::text(entry.to_string()))
                    .collect();
                plugin_items.push(OutputBlock::section("orphaned_governance_preview", preview));
            }
        } else {
            plugin_items.push(OutputBlock::kv("plugin_discovery", "unavailable"));
            plugin_items.push(OutputBlock::kv("discovered_plugins", "0"));
            plugin_items.push(OutputBlock::kv("discovered_plugin_commands", "0"));
            plugin_items.push(OutputBlock::kv("registered_plugin_commands", "0"));
            plugin_items.push(OutputBlock::kv("diagnostics", "0"));
        }

        let blocks = vec![
            OutputBlock::text("Status"),
            OutputBlock::section(
                "Working status",
                {
                    let mode_summary = if let Some(pending) = pending_approval {
                        format!(
                            "{permission_mode} | pending approval: {} ({})",
                            pending.tool_name, pending.message
                        )
                    } else {
                        permission_mode.to_string()
                    };
                    vec![
                        OutputBlock::kv("cwd", cwd.display().to_string()),
                        OutputBlock::kv("mode", mode_summary),
                    ]
                },
            ),
            OutputBlock::text("Diagnostics:"),
            OutputBlock::section("Runtime", runtime_items),
            OutputBlock::section("Observability", obs_items),
            OutputBlock::section("Commands", cmd_items),
            OutputBlock::section("Orchestration", orch_items),
            OutputBlock::section("Integrations", integ_items),
            OutputBlock::section("Plugins", plugin_items),
        ];

        Ok(CommandResult::Blocks(blocks))
    }
}

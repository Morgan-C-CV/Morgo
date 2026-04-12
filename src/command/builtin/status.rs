use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct StatusCommand;

#[async_trait]
impl Command for StatusCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "status".into(),
            description: "Show Claude Code status including session, role, and connectivity".into(),
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
                    .map(|session| session.cwd.as_str())
                    .unwrap_or_default();
                registry.list_user_invocable(cwd).len()
            })
            .unwrap_or(0);
        let mcp_config = app_state.mcp_runtime.as_ref().map(|runtime| runtime.config_load_result());
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
        let sensitive_command_count = metadata.iter().filter(|command| command.is_sensitive).count();
        let model_invocation_disabled_count = metadata
            .iter()
            .filter(|command| command.disable_model_invocation)
            .count();

        let mut lines = vec!["Status".to_string(), String::new(), "Runtime:".to_string()];
        lines.push(format!("- session_id: {}", app_state.active_session_id));
        lines.push(format!("- surface: {:?}", app_state.surface));
        lines.push(format!("- runtime_role: {:?}", app_state.runtime_role));
        lines.push(format!(
            "- worker_role: {}",
            app_state.worker_role.map(|role| role.as_str()).unwrap_or("none")
        ));
        lines.push(format!("- cost: {}", app_state.cost_tracker.format_report()));

        lines.push(String::new());
        lines.push("Commands:".to_string());
        lines.push(format!("- total: {}", registry_total));
        if command_source_counts.is_empty() {
            lines.push("- by_source: none".to_string());
        } else {
            for (source, count) in command_source_counts {
                lines.push(format!("- source {}: {}", source.as_str(), count));
            }
        }
        if command_type_counts.is_empty() {
            lines.push("- by_type: none".to_string());
        } else {
            for (command_type, count) in command_type_counts {
                lines.push(format!("- type {}: {}", command_type.as_str(), count));
            }
        }
        lines.push(format!(
            "- contract: prompt={}, immediate={}, sensitive={}, model_invocation_disabled={}",
            prompt_command_count,
            immediate_command_count,
            sensitive_command_count,
            model_invocation_disabled_count
        ));

        lines.push(String::new());
        lines.push("Orchestration:".to_string());
        lines.push(format!(
            "- pending_orchestration: {}",
            if pending_orchestration { "yes" } else { "no" }
        ));
        lines.push(format!(
            "- tasks: total={}, running={}, completed={}, failed={}, killed={}",
            tasks.len(), running_count, completed_count, failed_count, killed_count
        ));
        lines.push(format!("- pending_verification: {}", pending_verification_count));
        lines.push(format!("- orchestration_groups: {}", group_count));

        lines.push(String::new());
        lines.push("Integrations:".to_string());
        lines.push(format!(
            "- skills_registry: {} (user_invocable={})",
            if app_state.skill_registry.is_some() { "available" } else { "unavailable" },
            skill_count
        ));
        if let Some(config) = mcp_config {
            lines.push(format!(
                "- mcp_runtime: available (source={}, path={}, diagnostics={})",
                config.source.as_str(),
                config.path.display(),
                config.diagnostics.len()
            ));
        } else {
            lines.push("- mcp_runtime: unavailable".to_string());
        }

        lines.push(String::new());
        lines.push("Plugins:".to_string());
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
            lines.push(format!(
                "- plugin_discovery: {} (root={})",
                plugin_load_result.source.as_str(),
                plugin_load_result.root.display()
            ));
            lines.push(format!("- discovered_plugins: {}", plugin_load_result.plugins.len()));
            lines.push(format!("- registered_plugin_commands: {}", registered_plugin_commands));
            lines.push(format!("- diagnostics: {}", plugin_load_result.diagnostics.len()));
        } else {
            lines.push("- plugin_discovery: unavailable".to_string());
            lines.push("- discovered_plugins: 0".to_string());
            lines.push("- registered_plugin_commands: 0".to_string());
            lines.push("- diagnostics: 0".to_string());
        }

        Ok(CommandResult::Message(lines.join("\n")))
    }
}

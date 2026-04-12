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

        let status = format!(
            "Session ID: {}\nRuntime role: {:?}\nWorker role: {}\nSurface: {:?}\nPending orchestration: {}\nRuntime tasks: total={}, running={}, completed={}, failed={}, killed={}\nPending verification: {}\nOrchestration groups: {}\nSkills registry: {}\nMCP runtime: {}\nCost: {}",
            app_state.active_session_id,
            app_state.runtime_role,
            app_state.worker_role.map(|role| role.as_str()).unwrap_or("none"),
            app_state.surface,
            if pending_orchestration { "yes" } else { "no" },
            tasks.len(),
            running_count,
            completed_count,
            failed_count,
            killed_count,
            pending_verification_count,
            group_count,
            if app_state.skill_registry.is_some() { "available" } else { "unavailable" },
            if app_state.mcp_runtime.is_some() { "available" } else { "unavailable" },
            app_state.cost_tracker.format_report(),
        );
        Ok(CommandResult::Message(status))
    }
}

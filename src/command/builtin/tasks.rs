use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct TasksCommand;

#[async_trait]
impl Command for TasksCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "tasks".into(),
            description: "Manage, list and view active sub-agent tasks".into(),
            source: CommandSource::Builtin,
            category: "orchestration".into(),
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
        if let Some(task_manager) = &app_state.permission_context.task_manager {
            let tasks = task_manager.list();
            if tasks.is_empty() {
                return Ok(CommandResult::Message("No active or completed child tasks.".into()));
            }

            let mut summary = String::from("Agent Tasks:\n");
            for task in tasks {
                summary.push_str(&format!(
                    "- [{}] {} (Status: {:?})\n",
                    task.id, task.description, task.status
                ));
                summary.push_str(&format!(
                    "  worker_role: {}\n",
                    task.worker_role.map(|role| role.as_str()).unwrap_or("none")
                ));
                summary.push_str(&format!(
                    "  phase: {}\n",
                    task.phase.map(|phase| phase.as_str()).unwrap_or("none")
                ));
                summary.push_str(&format!(
                    "  validation_state: {}\n",
                    task.validation_state
                        .map(|state| state.as_str())
                        .unwrap_or("none")
                ));
                if let Some(parent_task_id) = task.parent_task_id.as_deref() {
                    summary.push_str(&format!("  parent_task_id: {}\n", parent_task_id));
                }
                if let Some(group_id) = task.orchestration_group_id.as_deref() {
                    summary.push_str(&format!("  orchestration_group_id: {}\n", group_id));
                }
            }
            Ok(CommandResult::Message(summary))
        } else {
            Ok(CommandResult::Message(
                "Task manager is not attached to current session.".into(),
            ))
        }
    }
}

use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct TasksCommand;

#[async_trait]
impl Command for TasksCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "tasks",
            description: "Manage, list and view active sub-agent tasks",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &[],
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
            }
            Ok(CommandResult::Message(summary))
        } else {
            Ok(CommandResult::Message(
                "Task manager is not attached to current session.".into(),
            ))
        }
    }
}

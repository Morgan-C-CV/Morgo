use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskCreateTool;

#[async_trait]
impl Tool for TaskCreateTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskCreate".into(),
            description: "Create a planning task-list item".into(),
            aliases: &[],
            search_hint: Some("create task list item"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let task_list = permissions
            .task_list_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task list manager is not configured"))?;
        let mut parts = call.input.splitn(3, ':');
        let subject = parts.next().unwrap_or_default().trim();
        let description = parts.next().unwrap_or_default().trim();
        let active_form = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        if subject.is_empty() {
            anyhow::bail!("task subject cannot be empty");
        }
        if description.is_empty() {
            anyhow::bail!("task description cannot be empty");
        }

        let task = task_list.create(subject, description, active_form, None);
        Ok(ToolResult::Text(format!(
            "id: {}\nsubject: {}\nstatus: {:?}",
            task.id, task.subject, task.status
        )))
    }
}

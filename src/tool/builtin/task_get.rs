use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskGetTool;

#[async_trait]
impl Tool for TaskGetTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskGet",
            description: "Get a planning task-list item",
            aliases: &[],
            read_only: true,
            destructive: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
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
        let task_id = call.input.trim();
        if task_id.is_empty() {
            anyhow::bail!("task id cannot be empty");
        }

        let task = task_list
            .get(task_id)
            .ok_or_else(|| anyhow::anyhow!("task {task_id} is unknown"))?;

        Ok(ToolResult::Text(format!(
            "id: {}\nsubject: {}\ndescription: {}\nactive_form: {}\nstatus: {:?}\nowner: {}\nblocked_by: {}\nblocks: {}",
            task.id,
            task.subject,
            task.description,
            task.active_form.as_deref().unwrap_or(""),
            task.status,
            task.owner.as_deref().unwrap_or(""),
            task.blocked_by.join(","),
            task.blocks.join(",")
        )))
    }
}

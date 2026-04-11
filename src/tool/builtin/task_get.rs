use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskGetTool;

#[async_trait]
impl Tool for TaskGetTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskGet",
            description: "Get a task owned by the active session",
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
        let tasks = permissions
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task manager is not configured"))?;
        let session_id = permissions
            .active_session_id
            .clone()
            .unwrap_or_else(|| "local-session".into());
        let task_id = call.input.trim();
        if task_id.is_empty() {
            anyhow::bail!("task id cannot be empty");
        }

        let task = tasks
            .get(task_id)
            .filter(|task| task.owner.session_id == session_id)
            .ok_or_else(|| {
                anyhow::anyhow!("task {task_id} is unknown or not owned by this session")
            })?;

        Ok(ToolResult::Text(format!(
            "id: {}\ndescription: {}\nstatus: {:?}\nowner_session_id: {}\noutput_file: {}\noutput_offset: {}",
            task.id,
            task.description,
            task.status,
            task.owner.session_id,
            task.output_file,
            task.output_offset
        )))
    }
}

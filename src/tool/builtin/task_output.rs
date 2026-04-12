use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskOutputTool;

#[async_trait]
impl Tool for TaskOutputTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskOutput".into(),
            description: "Read task output by task id and offset".into(),
            aliases: &[],
            search_hint: Some("read task output"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: true,
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

        let mut parts = call.input.splitn(2, ':');
        let task_id = parts.next().unwrap_or_default().trim();
        let offset = parts
            .next()
            .unwrap_or("0")
            .trim()
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("offset must be a non-negative integer"))?;

        if task_id.is_empty() {
            anyhow::bail!("task id cannot be empty");
        }

        let task = tasks
            .get(task_id)
            .filter(|task| task.owner.session_id == session_id)
            .ok_or_else(|| {
                anyhow::anyhow!("task {task_id} is unknown or not owned by this session")
            })?;
        let output = tasks
            .get_output(task_id, offset)
            .ok_or_else(|| anyhow::anyhow!("task {task_id} output is unavailable"))?;

        Ok(ToolResult::Text(format!(
            "task_id: {}\nnext_offset: {}\ncontent:\n{}",
            task.id, output.next_offset, output.content
        )))
    }
}

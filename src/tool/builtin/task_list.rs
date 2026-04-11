use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskListTool;

#[async_trait]
impl Tool for TaskListTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskList",
            description: "List tasks owned by the active session",
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
        _call: &ToolCall,
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

        let owned = tasks
            .list()
            .into_iter()
            .filter(|task| task.owner.session_id == session_id)
            .map(|task| {
                format!(
                    "id: {}\ndescription: {}\nstatus: {:?}\noutput_file: {}\noutput_offset: {}",
                    task.id, task.description, task.status, task.output_file, task.output_offset
                )
            })
            .collect::<Vec<_>>();

        Ok(ToolResult::Text(if owned.is_empty() {
            "no tasks".into()
        } else {
            owned.join("\n\n")
        }))
    }
}

use async_trait::async_trait;

use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskStopTool;

#[async_trait]
impl Tool for TaskStopTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskStop",
            description: "Stop a running task owned by the active session",
            aliases: &["KillTask"],
            read_only: false,
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
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
        let task_id = call.input.trim();
        if task_id.is_empty() {
            anyhow::bail!("task id cannot be empty");
        }
        if !tasks.kill(task_id, &session_id, &dispatcher) {
            anyhow::bail!("task {task_id} is not running or not owned by this session");
        }
        Ok(ToolResult::Text(format!("task {} stopped", task_id)))
    }
}

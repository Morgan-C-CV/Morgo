use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct SendMessageTool;

#[async_trait]
impl Tool for SendMessageTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "SendMessage",
            description: "Send a message to a running task owned by the active session",
            aliases: &[],
            search_hint: Some("message running task"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
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

        let mut parts = call.input.splitn(2, ':');
        let task_id = parts.next().unwrap_or_default().trim();
        let message = parts.next().unwrap_or_default().trim();

        if task_id.is_empty() {
            anyhow::bail!("task id cannot be empty");
        }
        if message.is_empty() {
            anyhow::bail!("message cannot be empty");
        }
        if !tasks.send_message(task_id, &session_id, message) {
            anyhow::bail!("task {task_id} is not running or not owned by this session");
        }

        Ok(ToolResult::Text(format!(
            "task {} accepted message {}",
            task_id, message
        )))
    }
}

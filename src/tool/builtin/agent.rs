use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::task::manager::TaskManager;
use crate::task::types::TaskStatus;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct AgentTool;

#[async_trait]
impl Tool for AgentTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Agent",
            description: "Launch a subagent with isolated context",
            aliases: &["TaskAgent"],
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
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let tasks = TaskManager::default();
        let task = tasks.register("agent-task", format!("Spawned agent for {}", call.input));
        tasks.transition(&task.id, TaskStatus::Completed);
        Ok(ToolResult::Text(format!(
            "agent task {} completed with isolated scaffold for {}",
            task.id, call.input
        )))
    }
}

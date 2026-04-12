use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct ExitPlanModeTool;

#[async_trait]
impl Tool for ExitPlanModeTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "ExitPlanMode".into(),
            description: "Present a completed plan for user approval".into(),
            aliases: &[],
            search_hint: Some("exit planning mode and request approval"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: true,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(crate::state::plan_mode::request_exit_plan_mode(
            permissions,
            call.input.as_str(),
        ))
    }
}

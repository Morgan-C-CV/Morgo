use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct ExitPlanModeTool;

#[async_trait]
impl Tool for ExitPlanModeTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "ExitPlanMode",
            description: "Present a completed plan for user approval",
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
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let summary = call.input.trim();
        Ok(ToolResult::Text(if summary.is_empty() {
            "plan ready for approval".into()
        } else {
            format!("plan ready for approval: {summary}")
        }))
    }
}

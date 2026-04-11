use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct EnterPlanModeTool;

#[async_trait]
impl Tool for EnterPlanModeTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "EnterPlanMode",
            description: "Request plan mode before a non-trivial implementation task",
            aliases: &[],
            search_hint: Some("enter planning mode before implementation"),
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
        let reason = call.input.trim();
        Ok(ToolResult::Text(if reason.is_empty() {
            "plan mode requested".into()
        } else {
            format!("plan mode requested: {reason}")
        }))
    }
}

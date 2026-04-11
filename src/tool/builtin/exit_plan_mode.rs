use async_trait::async_trait;

use crate::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
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
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let summary = call.input.trim();
        if !matches!(permissions.mode(), PermissionMode::Plan) {
            return Ok(ToolResult::Denied("cannot exit plan mode when plan mode is inactive".into()));
        }

        let message = if summary.is_empty() {
            "approve exiting plan mode".to_string()
        } else {
            format!("approve exiting plan mode: {summary}")
        };
        permissions.set_pending_approval(Some(PendingApproval {
            tool_name: self.metadata().name.to_string(),
            tool_input: call.input.clone(),
            message: message.clone(),
        }));

        Ok(ToolResult::PendingApproval {
            tool_name: self.metadata().name.to_string(),
            message,
        })
    }
}

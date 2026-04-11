use async_trait::async_trait;

use crate::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
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
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let reason = call.input.trim();
        if matches!(permissions.mode(), PermissionMode::Plan) {
            return Ok(ToolResult::Text("already in plan mode".into()));
        }

        let message = if reason.is_empty() {
            "approve entering plan mode".to_string()
        } else {
            format!("approve entering plan mode: {reason}")
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

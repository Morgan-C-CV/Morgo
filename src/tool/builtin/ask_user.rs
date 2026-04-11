use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct AskUserQuestionTool;

#[async_trait]
impl Tool for AskUserQuestionTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "AskUserQuestion",
            description: "Ask the user a structured follow-up question",
            aliases: &["AskUser"],
            search_hint: Some("ask user clarification question interactively"),
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
        let question = call.input.trim();
        if question.is_empty() {
            anyhow::bail!("question cannot be empty");
        }
        Ok(ToolResult::Text(format!(
            "interactive question queued: {question}"
        )))
    }
}

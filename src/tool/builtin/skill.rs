use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct SkillTool;

#[async_trait]
impl Tool for SkillTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Skill",
            description: "Invoke a user-invocable skill by name",
            aliases: &[],
            search_hint: Some("run slash-command skill"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let skill = call.input.trim();
        if skill.is_empty() {
            anyhow::bail!("skill name cannot be empty");
        }
        Ok(ToolResult::Text(format!("skill queued: {skill}")))
    }
}

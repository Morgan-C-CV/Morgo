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
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let raw = call.input.trim();
        if raw.is_empty() {
            anyhow::bail!("skill name cannot be empty");
        }

        let mut parts = raw.splitn(2, char::is_whitespace);
        let skill_name = parts.next().unwrap_or_default().trim();
        let args = parts.next().unwrap_or_default().trim();
        let skill_registry = permissions
            .skill_registry
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("skill registry is unavailable in this session"))?;
        let skill = skill_registry
            .find(skill_name)
            .ok_or_else(|| anyhow::anyhow!("unknown skill: {skill_name}"))?;
        if !skill.is_model_invocable() {
            anyhow::bail!("skill {skill_name} cannot be invoked by the model");
        }

        let args_line = if args.is_empty() {
            "Arguments: (none)".to_string()
        } else {
            format!("Arguments: {args}")
        };
        let when_to_use = skill
            .when_to_use
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("When to use: {}\n", value.trim()))
            .unwrap_or_default();
        let argument_hint = skill
            .argument_hint
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("Argument hint: {}\n", value.trim()))
            .unwrap_or_default();

        Ok(ToolResult::Text(format!(
            "Loaded skill: {}\n{}{}{}\nSkill instructions:\n{}",
            skill.name,
            when_to_use,
            argument_hint,
            args_line,
            skill.content
        )))
    }
}

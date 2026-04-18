use std::path::Path;

use async_trait::async_trait;

use crate::skills::registry::SkillRegistry;
use crate::skills::types::{SkillDefinition, SkillWorkflowExecution};
use crate::state::app_state::WorkerRole;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct SkillTool;

pub fn load_skill_prompt(
    skill_registry: &SkillRegistry,
    cwd: &Path,
    skill_name: &str,
    args: &str,
) -> anyhow::Result<String> {
    let skill = skill_registry
        .find(skill_name)
        .ok_or_else(|| anyhow::anyhow!("unknown skill: {skill_name}"))?;
    if !skill.matches_project_context(cwd) {
        anyhow::bail!("skill {} is not active for {}", skill.name, cwd.display());
    }
    format_skill_invocation(&skill, args)
}

pub fn format_skill_invocation(skill: &SkillDefinition, args: &str) -> anyhow::Result<String> {
    if !skill.is_model_invocable() {
        anyhow::bail!("skill {} cannot be invoked by the model", skill.name);
    }

    match skill.workflow_execution {
        SkillWorkflowExecution::PromptOnly => format_skill_prompt(skill, args),
        SkillWorkflowExecution::Agent => format_skill_agent_request(skill, args),
    }
}

pub fn format_skill_prompt(skill: &SkillDefinition, args: &str) -> anyhow::Result<String> {
    let args_line = if args.trim().is_empty() {
        "Arguments: (none)".to_string()
    } else {
        format!("Arguments: {}", args.trim())
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
    let workflow_hint = skill
        .workflow_summary
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("Workflow hint: {}\n", value.trim()))
        .unwrap_or_default();
    let allowed_tools = if skill.allowed_tools.is_empty() {
        String::new()
    } else {
        format!("Allowed tools: {}\n", skill.allowed_tools.join(", "))
    };
    let source = format!("Source: {}\n", skill.source.as_str());

    Ok(format!(
        "Loaded skill: {}\n{}{}{}{}{}{}\nSkill instructions:\n{}",
        skill.name,
        when_to_use,
        argument_hint,
        workflow_hint,
        allowed_tools,
        source,
        args_line,
        skill.content
    ))
}

fn format_skill_agent_request(skill: &SkillDefinition, args: &str) -> anyhow::Result<String> {
    let task = if args.trim().is_empty() {
        format!(
            "Follow the loaded skill instructions below.\n\nSkill: {}\n\nSkill instructions:\n{}",
            skill.name, skill.content
        )
    } else {
        format!(
            "Follow the loaded skill instructions below.\n\nSkill: {}\nUser arguments: {}\n\nSkill instructions:\n{}",
            skill.name,
            args.trim(),
            skill.content
        )
    };

    let payload = serde_json::json!({
        "task": task,
        "role": WorkerRole::Research.as_str(),
        "inherit_context": true,
        "allowed_tools": (!skill.allowed_tools.is_empty()).then_some(skill.allowed_tools.clone()),
        "reuse_strategy": "running_only"
    });

    Ok(format!(
        "Skill workflow: agent\nAgent request:\n{}",
        serde_json::to_string_pretty(&payload)?
    ))
}

#[async_trait]
impl Tool for SkillTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Skill".into(),
            description: "Invoke a user-invocable skill by name".into(),
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
        let cwd = std::env::current_dir()
            .map_err(|error| anyhow::anyhow!("failed to resolve current directory: {error}"))?;
        Ok(ToolResult::Text(load_skill_prompt(
            skill_registry,
            &cwd,
            skill_name,
            args,
        )?))
    }
}

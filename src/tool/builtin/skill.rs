use std::path::Path;

use async_trait::async_trait;

use crate::skills::registry::SkillRegistry;
use crate::skills::types::{SkillDefinition, SkillWorkflowExecution};
use crate::state::app_state::WorkerRole;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct SkillTool;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PromptOnlyOutputContract {
    final_answer_only: bool,
    max_lines: Option<usize>,
    required_line_prefixes: Vec<String>,
}

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
    let execution_hint = "Execution hint: use the minimum evidence needed, avoid repeating the same search after a non-empty result, and answer directly once you have enough evidence for the requested output.\n";
    let output_contract = render_prompt_only_output_contract(args);

    Ok(format!(
        "Loaded skill: {}\n{}{}{}{}{}{}{}{}\nSkill instructions:\n{}",
        skill.name,
        when_to_use,
        argument_hint,
        workflow_hint,
        allowed_tools,
        source,
        execution_hint,
        output_contract,
        args_line,
        skill.content
    ))
}

fn render_prompt_only_output_contract(args: &str) -> String {
    let Some(contract) = infer_prompt_only_output_contract(args) else {
        return String::new();
    };
    let mut lines = vec!["Output contract:".to_string()];
    if contract.final_answer_only {
        lines.push("- final answer only".to_string());
    }
    if let Some(max_lines) = contract.max_lines {
        lines.push(format!("- max_lines: {max_lines}"));
    }
    if !contract.required_line_prefixes.is_empty() {
        lines.push(format!(
            "- required_line_prefixes: {}",
            contract.required_line_prefixes.join(" | ")
        ));
    }
    lines.push("- do not broaden scope beyond this contract".to_string());
    format!("{}\n", lines.join("\n"))
}

fn infer_prompt_only_output_contract(args: &str) -> Option<PromptOnlyOutputContract> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lowered = trimmed.to_ascii_lowercase();
    let final_answer_only = trimmed.contains("只给出")
        || trimmed.contains("只输出")
        || lowered.contains("only give")
        || lowered.contains("only output");
    let max_lines = extract_max_lines(trimmed);
    let required_line_prefixes = extract_required_line_prefixes(trimmed);
    if !final_answer_only && max_lines.is_none() && required_line_prefixes.is_empty() {
        None
    } else {
        Some(PromptOnlyOutputContract {
            final_answer_only,
            max_lines,
            required_line_prefixes,
        })
    }
}

fn extract_max_lines(args: &str) -> Option<usize> {
    let chars = args.chars().collect::<Vec<_>>();
    for i in 0..chars.len() {
        let is_line_marker = chars[i] == '行'
            || (chars[i].eq_ignore_ascii_case(&'l')
                && chars
                    .get(i..i + 5)
                    .map(|slice| {
                        slice
                            .iter()
                            .collect::<String>()
                            .to_ascii_lowercase()
                            .starts_with("lines")
                    })
                    .unwrap_or(false));
        if !is_line_marker {
            continue;
        }
        let mut end = i;
        while end > 0 && chars[end - 1].is_whitespace() {
            end -= 1;
        }
        let mut start = end;
        while start > 0 && chars[start - 1].is_ascii_digit() {
            start -= 1;
        }
        if start < end {
            let digits = chars[start..end].iter().collect::<String>();
            if let Ok(value) = digits.parse::<usize>() {
                return Some(value);
            }
        }
    }
    None
}

fn extract_required_line_prefixes(args: &str) -> Vec<String> {
    let Some((_, tail)) = args.split_once('：').or_else(|| args.split_once(':')) else {
        return Vec::new();
    };
    tail.split(['、', '，', ',', '/', ';', '|'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| part.trim_matches('。').trim_matches('.').to_string())
        .filter(|part| !part.is_empty())
        .take(8)
        .collect()
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

use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::json;

use crate::bootstrap::config_root::resolve_config_root;
use crate::bootstrap::teammate_registry::{
    TeammateProfile, TeammateRegistry, load_teammate_registry_from_root,
};
use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::{AppState, WorkerRole};
use crate::tool::builtin::agent::AgentTool;
use crate::tool::definition::{Tool, ToolCall, ToolResult};

pub struct SwarmCommand;

#[async_trait]
impl Command for SwarmCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "swarm".into(),
            description: "Show multi-agent swarm topology (read-only)".into(),
            source: CommandSource::Builtin,
            category: "orchestration".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let sub = input
            .raw
            .trim_start_matches("/swarm")
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("status");

        match sub {
            "status" => render_swarm_status(app_state),
            "teammates" | "list" => render_swarm_teammates(app_state),
            "spawn" => render_swarm_spawn(input, app_state).await,
            _ => Ok(CommandResult::Message(format!(
                "Unknown subcommand '{sub}'. Usage: /swarm status | /swarm teammates | /swarm spawn <teammate_id> <task>"
            ))),
        }
    }
}

fn render_swarm_status(app_state: &AppState) -> anyhow::Result<CommandResult> {
    let Some(task_manager) = &app_state.permission_context.task_manager else {
        return Ok(CommandResult::Message(
            "Swarm status\nActive tasks: 0\n\nNo task manager attached to current session.".into(),
        ));
    };

    let tasks = task_manager.list();
    if tasks.is_empty() {
        return Ok(CommandResult::Message(
            "Swarm status\nActive tasks: 0".into(),
        ));
    }

    let (groups, standalone) = task_manager.grouped_tasks();
    let mut lines = vec![
        "Swarm status".to_string(),
        format!("Active tasks: {}", tasks.len()),
    ];

    if !groups.is_empty() {
        for group in &groups {
            lines.push(String::new());
            lines.push(format!("Group {}", group.group_id));

            let group_ids: std::collections::HashSet<&str> =
                group.tasks.iter().map(|t| t.id.as_str()).collect();
            let roots: Vec<_> = group
                .tasks
                .iter()
                .filter(|t| {
                    t.parent_task_id
                        .as_deref()
                        .map(|p| !group_ids.contains(p))
                        .unwrap_or(true)
                })
                .collect();

            for root in roots {
                render_task_tree(root, &group.tasks, &mut lines, 0);
            }
        }
    }

    if !standalone.is_empty() {
        lines.push(String::new());
        lines.push("Standalone".to_string());
        for task in &standalone {
            render_task_tree(task, &[], &mut lines, 0);
        }
    }

    Ok(CommandResult::Message(lines.join("\n")))
}

fn render_swarm_teammates(app_state: &AppState) -> anyhow::Result<CommandResult> {
    let cwd = app_state.current_working_directory();
    let config_root = resolve_config_root(&cwd)?;
    let registry_path = config_root.join("buddies").join("agents.json");
    let registry = load_teammate_registry_from_root(&config_root)?;
    let Some(registry) = registry else {
        return Ok(CommandResult::Message(format!(
            "No teammate registry found at {}",
            registry_path.display()
        )));
    };

    Ok(CommandResult::Message(render_teammate_registry(
        &registry,
        registry_path,
    )))
}

async fn render_swarm_spawn(
    input: &NormalizedInput,
    app_state: &AppState,
) -> anyhow::Result<CommandResult> {
    let args = input
        .raw
        .trim_start_matches("/swarm")
        .trim()
        .strip_prefix("spawn")
        .unwrap_or_default()
        .trim();
    let mut parts = args.splitn(2, char::is_whitespace);
    let teammate_id = parts.next().unwrap_or_default().trim();
    let task_description = parts.next().unwrap_or_default().trim();

    if teammate_id.is_empty() {
        return Ok(CommandResult::Message(
            "Usage: /swarm spawn <teammate_id> <task description>".into(),
        ));
    }
    if task_description.is_empty() {
        return Ok(CommandResult::Message(format!(
            "Missing task description for teammate '{teammate_id}'. Usage: /swarm spawn <teammate_id> <task description>"
        )));
    }

    let cwd = app_state.current_working_directory();
    let config_root = resolve_config_root(&cwd)?;
    let registry_path = config_root.join("buddies").join("agents.json");
    let registry = load_teammate_registry_from_root(&config_root)?;
    let Some(registry) = registry else {
        return Ok(CommandResult::Message(format!(
            "No teammate registry found at {}; cannot spawn teammate.",
            registry_path.display()
        )));
    };

    let Some(profile) = registry.profiles.iter().find(|p| p.id == teammate_id) else {
        let available_ids = registry
            .profiles
            .iter()
            .map(|p| p.id.as_str())
            .collect::<Vec<_>>();
        return Ok(CommandResult::Message(format!(
            "Unknown teammate id '{teammate_id}'. Available ids: {}",
            if available_ids.is_empty() {
                "(none)".to_string()
            } else {
                available_ids.join(", ")
            }
        )));
    };

    let role = map_teammate_role(profile)?;
    let orchestration_group_id = format!(
        "swarm:{}:{}",
        profile.id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let prompt = build_teammate_prompt(profile, task_description);
    let agent_input = json!({
        "task": prompt,
        "role": role.as_str(),
        "inherit_context": true,
        "max_turns": profile.max_turns as usize,
        "allowed_tools": profile.allowed_tools,
        "reuse_strategy": "fresh",
        "orchestration_group_id": orchestration_group_id,
    })
    .to_string();
    let permissions = app_state
        .permission_context
        .clone()
        .with_active_session_id(app_state.active_session_id.clone())
        .with_active_surface(app_state.surface)
        .with_notification_dispatcher(app_state.notification_dispatcher.clone());
    let result = AgentTool
        .invoke(&ToolCall::new("Agent", agent_input), &permissions)
        .await?;

    match result {
        ToolResult::Text(text) => Ok(CommandResult::Message(text)),
        ToolResult::Denied(text) => Ok(CommandResult::Message(text)),
        ToolResult::Interrupted(text) => Ok(CommandResult::Message(text)),
        ToolResult::Progress(text) => Ok(CommandResult::Message(text)),
        ToolResult::ResultTooLarge(text) => Ok(CommandResult::Message(text)),
        ToolResult::PendingApproval { message, .. } => Ok(CommandResult::Message(message)),
    }
}

fn render_teammate_registry(registry: &TeammateRegistry, registry_path: PathBuf) -> String {
    let mut lines = vec![
        "Swarm teammates".to_string(),
        format!("Registry: {}", registry_path.display()),
        format!("Profiles: {}", registry.profiles.len()),
    ];

    for profile in &registry.profiles {
        lines.push(String::new());
        lines.push(format!("- {} ({})", profile.id, profile.name));
        lines.push(format!("  description: {}", profile.description));
        lines.push(format!("  role: {}", profile.role));
        lines.push(format!(
            "  default_model_profile: {}",
            profile.default_model_profile.as_deref().unwrap_or("none")
        ));
        lines.push(format!(
            "  allowed_tools: {}",
            if profile.allowed_tools.is_empty() {
                "[]".to_string()
            } else {
                profile.allowed_tools.join(", ")
            }
        ));
        lines.push(format!("  max_turns: {}", profile.max_turns));
    }

    lines.join("\n")
}

fn map_teammate_role(profile: &TeammateProfile) -> anyhow::Result<WorkerRole> {
    match profile.role.trim() {
        "research" => Ok(WorkerRole::Research),
        "implement" => Ok(WorkerRole::Implement),
        "verify" => Ok(WorkerRole::Verify),
        other => anyhow::bail!(
            "invalid_configuration: invalid agents.json: teammate '{}' has unsupported role '{}'",
            profile.id,
            other
        ),
    }
}

fn build_teammate_prompt(profile: &TeammateProfile, task_description: &str) -> String {
    let mut lines = vec![
        format!("teammate_id: {}", profile.id),
        format!("teammate_name: {}", profile.name),
        format!("teammate_description: {}", profile.description),
        format!("teammate_role: {}", profile.role),
    ];
    if let Some(default_model_profile) = profile.default_model_profile.as_deref() {
        lines.push(format!("default_model_profile: {default_model_profile}"));
    }
    lines.push(format!("user_task: {task_description}"));
    lines.join("\n")
}

fn render_task_tree(
    task: &crate::task::types::TaskRecord,
    siblings: &[crate::task::types::TaskRecord],
    lines: &mut Vec<String>,
    depth: usize,
) {
    let indent = "  ".repeat(depth);
    let role = task
        .worker_role
        .map(|r| format!(" role={}", r.as_str()))
        .unwrap_or_default();
    let step = task
        .step_id
        .map(|s| format!(" step={s}"))
        .unwrap_or_default();
    let group = task
        .orchestration_group_id
        .as_deref()
        .map(|g| format!(" group={g}"))
        .unwrap_or_default();
    lines.push(format!(
        "{indent}- {} {:?} {:?}{role}{step}{group}",
        task.id, task.task_type, task.status
    ));

    let children: Vec<_> = siblings
        .iter()
        .filter(|t| t.parent_task_id.as_deref() == Some(task.id.as_str()))
        .collect();
    for child in children {
        render_task_tree(child, siblings, lines, depth + 1);
    }
}

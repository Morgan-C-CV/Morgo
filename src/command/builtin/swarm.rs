use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

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

        if sub != "status" {
            return Ok(CommandResult::Message(format!(
                "Unknown subcommand '{sub}'. Usage: /swarm status"
            )));
        }

        let Some(task_manager) = &app_state.permission_context.task_manager else {
            return Ok(CommandResult::Message(
                "Swarm status\nActive tasks: 0\n\nNo task manager attached to current session."
                    .into(),
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

                // Collect root tasks (no parent, or parent not in this group).
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
}

fn render_task_tree(
    task: &crate::task::types::TaskRecord,
    siblings: &[crate::task::types::TaskRecord],
    lines: &mut Vec<String>,
    depth: usize,
) {
    let indent = "  ".repeat(depth);
    let prefix = if depth == 0 { "-" } else { "-" };
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
        "{indent}{prefix} {} {:?} {:?}{role}{step}{group}",
        task.id, task.task_type, task.status
    ));

    // Render children.
    let children: Vec<_> = siblings
        .iter()
        .filter(|t| t.parent_task_id.as_deref() == Some(task.id.as_str()))
        .collect();
    for child in children {
        render_task_tree(child, siblings, lines, depth + 1);
    }
}

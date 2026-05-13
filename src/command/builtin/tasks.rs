use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::task::types::{TaskRecord, TaskStatus};

pub struct TasksCommand;

fn push_user_facing_task_section(
    lines: &mut Vec<String>,
    title: &str,
    tasks: &[TaskRecord],
    task_manager: &crate::task::manager::TaskManager,
) {
    if tasks.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(title.to_string());
    for task in tasks {
        lines.push(format!("- [{}] {}", task.id, task.description));
        lines.push(format!("  status: {:?}", task.status));
        lines.push(format!("  next: {}", task_manager.task_hint(task)));
    }
}

#[async_trait]
impl Command for TasksCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "tasks".into(),
            description: "Manage, list and view active sub-agent tasks".into(),
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
        _input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        if let Some(task_manager) = &app_state.permission_context.task_manager {
            let tasks = task_manager.list();
            if tasks.is_empty() {
                return Ok(CommandResult::Message(
                    "No active or completed child tasks.".into(),
                ));
            }

            let (groups, standalone_tasks) = task_manager.grouped_tasks();
            let mut status_counts = std::collections::BTreeMap::<String, usize>::new();
            let mut role_counts = std::collections::BTreeMap::<String, usize>::new();
            let mut validation_counts = std::collections::BTreeMap::<String, usize>::new();
            let mut phase_counts = std::collections::BTreeMap::<String, usize>::new();
            let fan_in_ready_groups = groups
                .iter()
                .filter(|group| group.hint.contains("ready for synthesis"))
                .count();
            let verification_waiting_groups = groups
                .iter()
                .filter(|group| group.hint.contains("waiting for verification"))
                .count();
            let in_progress_groups = groups
                .iter()
                .filter(|group| group.hint.contains("still in progress"))
                .count();
            for task in &tasks {
                *status_counts
                    .entry(format!("{:?}", task.status))
                    .or_insert(0) += 1;
                *role_counts
                    .entry(
                        task.worker_role
                            .map(|role| role.as_str())
                            .unwrap_or("none")
                            .to_string(),
                    )
                    .or_insert(0) += 1;
                *validation_counts
                    .entry(
                        task.validation_state
                            .map(|state| state.as_str())
                            .unwrap_or("none")
                            .to_string(),
                    )
                    .or_insert(0) += 1;
                *phase_counts
                    .entry(
                        task.phase
                            .map(|phase| phase.as_str())
                            .unwrap_or("none")
                            .to_string(),
                    )
                    .or_insert(0) += 1;
            }
            let running_tasks = tasks
                .iter()
                .filter(|task| matches!(task.status, TaskStatus::Running | TaskStatus::Pending))
                .cloned()
                .collect::<Vec<_>>();
            let failed_tasks = tasks
                .iter()
                .filter(|task| matches!(task.status, TaskStatus::Failed | TaskStatus::Killed))
                .cloned()
                .collect::<Vec<_>>();
            let completed_tasks = tasks
                .iter()
                .filter(|task| matches!(task.status, TaskStatus::Completed))
                .cloned()
                .collect::<Vec<_>>();

            let mut lines = vec![
                "Agent Tasks:".to_string(),
                String::new(),
                "Summary:".to_string(),
            ];
            lines.push(format!("- total: {}", tasks.len()));
            lines.push(format!("- orchestration_groups: {}", groups.len()));
            lines.push(format!(
                "- by_status: {}",
                status_counts
                    .iter()
                    .map(|(status, count)| format!("{status}={count}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            lines.push(format!(
                "- by_worker_role: {}",
                role_counts
                    .iter()
                    .map(|(role, count)| format!("{role}={count}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            lines.push(format!(
                "- by_validation_state: {}",
                validation_counts
                    .iter()
                    .map(|(state, count)| format!("{state}={count}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            lines.push(format!(
                "- by_phase: {}",
                phase_counts
                    .iter()
                    .map(|(phase, count)| format!("{phase}={count}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            lines.push(format!(
                "- orchestration_contract: groups_in_progress={}, waiting_for_verification={}, ready_for_synthesis={}",
                in_progress_groups,
                verification_waiting_groups,
                fan_in_ready_groups
            ));
            push_user_facing_task_section(
                &mut lines,
                "Running tasks:",
                &running_tasks,
                task_manager,
            );
            push_user_facing_task_section(
                &mut lines,
                "Failed tasks:",
                &failed_tasks,
                task_manager,
            );
            push_user_facing_task_section(
                &mut lines,
                "Completed tasks:",
                &completed_tasks,
                task_manager,
            );

            if !groups.is_empty() {
                lines.push(String::new());
                lines.push("Orchestration groups:".to_string());
                for group in groups {
                    lines.push(format!("- {} — {}", group.group_id, group.hint));
                    for task in group.tasks {
                        lines.push(format!(
                            "  - [{}] {} (Status: {:?})",
                            task.id, task.description, task.status
                        ));
                        lines.push(format!(
                            "    worker_role: {}",
                            task.worker_role.map(|role| role.as_str()).unwrap_or("none")
                        ));
                        lines.push(format!(
                            "    phase: {}",
                            task.phase.map(|phase| phase.as_str()).unwrap_or("none")
                        ));
                        lines.push(format!(
                            "    validation_state: {}",
                            task.validation_state
                                .map(|state| state.as_str())
                                .unwrap_or("none")
                        ));
                        lines.push(format!("    hint: {}", task_manager.task_hint(&task)));
                        if let Some(parent_task_id) = task.parent_task_id.as_deref() {
                            lines.push(format!("    parent_task_id: {}", parent_task_id));
                        }
                    }
                }
            }

            if !standalone_tasks.is_empty() {
                lines.push(String::new());
                lines.push("Standalone tasks:".to_string());
                for task in standalone_tasks {
                    lines.push(format!(
                        "- [{}] {} (Status: {:?})",
                        task.id, task.description, task.status
                    ));
                    lines.push(format!(
                        "  worker_role: {}",
                        task.worker_role.map(|role| role.as_str()).unwrap_or("none")
                    ));
                    lines.push(format!(
                        "  phase: {}",
                        task.phase.map(|phase| phase.as_str()).unwrap_or("none")
                    ));
                    lines.push(format!(
                        "  validation_state: {}",
                        task.validation_state
                            .map(|state| state.as_str())
                            .unwrap_or("none")
                    ));
                    lines.push(format!("  hint: {}", task_manager.task_hint(&task)));
                    if let Some(parent_task_id) = task.parent_task_id.as_deref() {
                        lines.push(format!("  parent_task_id: {}", parent_task_id));
                    }
                }
            }

            Ok(CommandResult::Message(lines.join("\n")))
        } else {
            Ok(CommandResult::Message(
                "Task manager is not attached to current session.".into(),
            ))
        }
    }
}

use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskListTool;

#[async_trait]
impl Tool for TaskListTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskList".into(),
            description: "List tasks owned by the active session".into(),
            aliases: &[],
            search_hint: Some("list tasks"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: true,
        }
    }

    async fn invoke(
        &self,
        _call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let task_list = permissions
            .task_list_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task list manager is not configured"))?;

        let all_tasks = task_list.list();
        let tasks = all_tasks
            .iter()
            .map(|task| {
                let visible_blocked_by = task
                    .blocked_by
                    .iter()
                    .filter(|blocker_id| {
                        all_tasks
                            .iter()
                            .find(|candidate| candidate.id == blocker_id.as_str())
                            .map(|candidate| {
                                candidate.status
                                    != crate::task::list_types::TaskListStatus::Completed
                            })
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                format!(
                    "id: {}\nsubject: {}\ndescription: {}\nstatus: {:?}\nowner: {}\nplan_step_id: {}\nblocked_by: {}\nblocks: {}",
                    task.id,
                    task.subject,
                    task.description,
                    task.status,
                    task.owner.as_deref().unwrap_or(""),
                    task.plan_step_id.as_deref().unwrap_or(""),
                    visible_blocked_by.join(","),
                    task.blocks.join(",")
                )
            })
            .collect::<Vec<_>>();

        let owned = tasks;

        Ok(ToolResult::Text(if owned.is_empty() {
            "no tasks".into()
        } else {
            owned.join("\n\n")
        }))
    }
}

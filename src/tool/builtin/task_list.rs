use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskListTool;

#[async_trait]
impl Tool for TaskListTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskList",
            description: "List tasks owned by the active session",
            aliases: &[],
            read_only: true,
            destructive: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
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
                    "id: {}\nsubject: {}\ndescription: {}\nstatus: {:?}\nowner: {}\nblocked_by: {}\nblocks: {}",
                    task.id,
                    task.subject,
                    task.description,
                    task.status,
                    task.owner.as_deref().unwrap_or(""),
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

use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::task::list_types::TaskListStatus;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TaskUpdateTool;

#[async_trait]
impl Tool for TaskUpdateTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TaskUpdate",
            description: "Update a planning task-list item",
            aliases: &[],
            read_only: false,
            destructive: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let task_list = permissions
            .task_list_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task list manager is not configured"))?;

        let mut parts = call.input.splitn(6, ':');
        let task_id = parts.next().unwrap_or_default().trim();
        let subject = parse_optional_field(parts.next());
        let description = parse_optional_field(parts.next());
        let active_form = parse_nested_optional_field(parts.next());
        let status = parse_optional_status(parts.next())?;
        let owner = parse_nested_optional_field(parts.next());

        if task_id.is_empty() {
            anyhow::bail!("task id cannot be empty");
        }

        let updated = task_list
            .update(task_id, subject, description, active_form, status, owner)
            .ok_or_else(|| anyhow::anyhow!("task {task_id} is unknown"))?;

        Ok(ToolResult::Text(format!(
            "id: {}\nsubject: {}\ndescription: {}\nactive_form: {}\nstatus: {:?}\nowner: {}",
            updated.id,
            updated.subject,
            updated.description,
            updated.active_form.as_deref().unwrap_or(""),
            updated.status,
            updated.owner.as_deref().unwrap_or("")
        )))
    }
}

fn parse_optional_field(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
        .map(str::to_string)
}

fn parse_nested_optional_field(value: Option<&str>) -> Option<Option<String>> {
    value.map(|value| {
        let value = value.trim();
        if value.is_empty() || value == "-" {
            None
        } else {
            Some(value.to_string())
        }
    })
}

fn parse_optional_status(value: Option<&str>) -> anyhow::Result<Option<TaskListStatus>> {
    match value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
    {
        None => Ok(None),
        Some("pending") => Ok(Some(TaskListStatus::Pending)),
        Some("in_progress") => Ok(Some(TaskListStatus::InProgress)),
        Some("completed") => Ok(Some(TaskListStatus::Completed)),
        Some(other) => anyhow::bail!("unknown task status {other}"),
    }
}

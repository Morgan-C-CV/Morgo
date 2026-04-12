use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::task::list_manager::TaskListManager;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct TodoWriteTool;

#[async_trait]
impl Tool for TodoWriteTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "TodoWrite".into(),
            description: "Create or update the structured task list in one call".into(),
            aliases: &["Todo"],
            search_hint: Some("write task list or todo list"),
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
        let task_list = permissions
            .task_list_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task list manager is not configured"))?;
        let created = apply_lines(task_list, &call.input)?;
        Ok(ToolResult::Text(format!("todo entries applied: {created}")))
    }
}

fn apply_lines(task_list: &TaskListManager, input: &str) -> anyhow::Result<usize> {
    let mut created = 0;
    for line in input.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let mut parts = line.splitn(3, '|');
        let subject = parts.next().unwrap_or_default().trim();
        let description = parts.next().unwrap_or(subject).trim();
        let active_form = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if subject.is_empty() {
            anyhow::bail!("todo entry subject cannot be empty");
        }
        task_list.create(subject, description, active_form, None, None);
        created += 1;
    }
    Ok(created)
}

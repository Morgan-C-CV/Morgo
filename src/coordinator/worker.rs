use crate::interaction::notification::Notification;
use crate::task::types::{TaskEvent, TaskStatus};
use crate::tool::definition::ToolMetadata;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskNotification {
    pub task_id: String,
    pub status: TaskStatus,
    pub summary: String,
    pub output_file: String,
}

impl TaskNotification {
    pub fn from_task_event(event: &TaskEvent) -> Self {
        Self {
            task_id: event.task_id.clone(),
            status: event.status.clone(),
            summary: event.summary.clone(),
            output_file: event.output_file.clone(),
        }
    }

    pub fn format_as_user_message(&self) -> String {
        format!(
            "<task-notification>\n<task-id>{}</task-id>\n<status>{:?}</status>\n<summary>{}</summary>\n<output-file>{}</output-file>\n</task-notification>",
            self.task_id, self.status, self.summary, self.output_file
        )
    }
}

pub fn filter_tools_for_worker(all_tools: &[ToolMetadata]) -> Vec<ToolMetadata> {
    all_tools
        .iter()
        .filter(|tool| tool.name != "Agent" && tool.name != "SendMessage")
        .filter(|tool| !tool.requires_user_interaction)
        .filter(|tool| !tool.should_defer || tool.always_load)
        .cloned()
        .collect()
}

pub fn notification_to_task_notification(notification: &Notification) -> Option<TaskNotification> {
    Some(TaskNotification {
        task_id: notification.task_id.clone()?,
        status: match notification.status.as_deref() {
            Some("Pending") => TaskStatus::Pending,
            Some("Running") => TaskStatus::Running,
            Some("Failed") => TaskStatus::Failed,
            Some("Killed") => TaskStatus::Killed,
            _ => TaskStatus::Completed,
        },
        summary: notification.body.clone(),
        output_file: notification.output_file.clone().unwrap_or_default(),
    })
}

use crate::interaction::notification::Notification;
use crate::task::types::{TaskEvent, TaskStatus};
use crate::tool::definition::ToolMetadata;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskNotification {
    pub task_id: String,
    pub status: TaskStatus,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<crate::state::app_state::WorkerRole>,
    pub output_file: String,
}

impl TaskNotification {
    pub fn from_task_event(event: &TaskEvent) -> Self {
        Self {
            task_id: event.task_id.clone(),
            status: event.status.clone(),
            summary: event.summary.clone(),
            result: event.result.clone(),
            next_action: event.next_action.clone(),
            worker_role: event.worker_role,
            output_file: event.output_file.clone(),
        }
    }

    pub fn format_as_user_message(&self) -> String {
        TaskEvent {
            owner: crate::task::types::TaskOwner {
                session_id: String::new(),
                surface: crate::bootstrap::InteractionSurface::Cli,
            },
            target_task_id: None,
            task_id: self.task_id.clone(),
            status: self.status.clone(),
            summary: self.summary.clone(),
            result: self.result.clone(),
            next_action: self.next_action.clone(),
            worker_role: self.worker_role,
            output_file: self.output_file.clone(),
        }
        .format_notification()
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
        result: notification.title.clone(),
        next_action: "inspect task notification".to_string(),
        worker_role: None,
        output_file: notification.output_file.clone().unwrap_or_default(),
    })
}

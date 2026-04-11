use crate::interaction::notification::Notification;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Killed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDeliveryState {
    pub notified: bool,
    pub notification: Option<Notification>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: String,
    pub description: String,
    pub status: TaskStatus,
    pub output_file: String,
    pub output_offset: usize,
    pub delivery: TaskDeliveryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOutputSlice {
    pub content: String,
    pub next_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskNotification {
    pub session_id: String,
    pub task_id: String,
    pub status: TaskStatus,
    pub summary: String,
    pub output_file: String,
}

impl TaskNotification {
    pub fn as_task_notification_message(&self) -> String {
        format!(
            "<task-notification>\n<task-id>{}</task-id>\n<status>{:?}</status>\n<summary>{}</summary>\n<output-file>{}</output-file>\n</task-notification>",
            self.task_id, self.status, self.summary, self.output_file
        )
    }
}

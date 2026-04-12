use crate::bootstrap::InteractionSurface;
use crate::interaction::notification::Notification;
use crate::state::app_state::WorkerRole;

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
pub struct TaskOwner {
    pub session_id: String,
    pub surface: InteractionSurface,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: String,
    pub description: String,
    pub status: TaskStatus,
    pub owner: TaskOwner,
    pub worker_role: Option<WorkerRole>,
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
pub struct TaskEvent {
    pub owner: TaskOwner,
    pub target_task_id: Option<String>,
    pub task_id: String,
    pub status: TaskStatus,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<WorkerRole>,
    pub output_file: String,
}

impl TaskEvent {
    pub fn format_notification(&self) -> String {
        format!(
            "<task-notification>\n<task-id>{}</task-id>\n<status>{:?}</status>\n<summary>{}</summary>\n<result>{}</result>\n<next-action>{}</next-action>\n<worker-role>{}</worker-role>\n</task-notification>",
            self.task_id,
            self.status,
            self.summary,
            self.result,
            self.next_action,
            self.worker_role.map(|role| role.as_str()).unwrap_or("none")
        )
    }
}

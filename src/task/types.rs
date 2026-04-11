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

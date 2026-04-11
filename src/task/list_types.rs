#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskListStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListItem {
    pub id: String,
    pub subject: String,
    pub description: String,
    pub active_form: Option<String>,
    pub status: TaskListStatus,
    pub owner: Option<String>,
    pub blocks: Vec<String>,
    pub blocked_by: Vec<String>,
}

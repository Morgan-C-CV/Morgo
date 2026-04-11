#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationType {
    TaskUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub session_id: String,
    pub title: String,
    pub body: String,
    pub notification_type: NotificationType,
    pub task_id: Option<String>,
    pub status: Option<String>,
    pub output_file: Option<String>,
    pub wake_up: bool,
    pub target: Option<String>,
}

impl Notification {
    pub fn task_update(
        session_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
        task_id: impl Into<String>,
        status: impl Into<String>,
        output_file: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            title: title.into(),
            body: body.into(),
            notification_type: NotificationType::TaskUpdate,
            task_id: Some(task_id.into()),
            status: Some(status.into()),
            output_file: Some(output_file.into()),
            wake_up: true,
            target: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub session_id: String,
    pub title: String,
    pub body: String,
}

impl Notification {
    pub fn task_update(
        session_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            title: title.into(),
            body: body.into(),
        }
    }
}

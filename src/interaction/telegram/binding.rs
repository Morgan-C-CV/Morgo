#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBinding {
    pub actor_id: String,
    pub session_id: String,
    pub delivery_target: Option<String>,
}

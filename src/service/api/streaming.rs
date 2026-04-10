#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEvent {
    pub kind: String,
    pub payload: String,
}

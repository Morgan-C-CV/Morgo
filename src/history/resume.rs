use crate::history::session::{SessionHistory, SessionSnapshot};
use crate::history::transcript::Transcript;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreSource {
    ContinueSession,
    ResumeSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub source: RestoreSource,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredSession {
    pub snapshot: SessionSnapshot,
    pub history: SessionHistory,
    pub transcript: Transcript,
}

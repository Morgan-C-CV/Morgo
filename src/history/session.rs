use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::bootstrap::{InteractionSurface, SessionMode};
use crate::core::message::Message;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRestoreRequest {
    pub resume: Option<String>,
    pub continue_session: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub cwd: String,
    pub last_turn_at: Option<String>,
    pub prompt_seed: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHistoryEntry {
    pub message: Message,
    pub timestamp: Option<String>,
    pub tool_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionHistory {
    pub entries: Vec<SessionHistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub session_id: String,
    pub title: String,
}

pub trait SessionStore: Send + Sync {
    fn load(&self, request: &SessionRestoreRequest) -> Option<(SessionSnapshot, SessionHistory)>;
    fn save(&self, snapshot: SessionSnapshot, history: SessionHistory);
    fn append_entry(&self, session_id: &SessionId, entry: SessionHistoryEntry);
}

#[derive(Debug, Clone, Default)]
pub struct InMemorySessionStore {
    sessions: Arc<RwLock<HashMap<SessionId, (SessionSnapshot, SessionHistory)>>>,
    latest_session: Arc<RwLock<Option<SessionId>>>,
}

impl InMemorySessionStore {
    pub fn insert(&self, snapshot: SessionSnapshot, history: SessionHistory) {
        self.save(snapshot, history);
    }
}

impl SessionStore for InMemorySessionStore {
    fn load(&self, request: &SessionRestoreRequest) -> Option<(SessionSnapshot, SessionHistory)> {
        let target = if request.continue_session {
            self.latest_session.read().ok()?.clone()
        } else {
            request
                .resume
                .as_ref()
                .map(|session_id| SessionId(session_id.clone()))
        }?;

        self.sessions.read().ok()?.get(&target).cloned()
    }

    fn save(&self, snapshot: SessionSnapshot, history: SessionHistory) {
        if let Ok(mut latest) = self.latest_session.write() {
            *latest = Some(snapshot.session_id.clone());
        }
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.insert(snapshot.session_id.clone(), (snapshot, history));
        }
    }

    fn append_entry(&self, session_id: &SessionId, entry: SessionHistoryEntry) {
        if let Ok(mut sessions) = self.sessions.write() {
            if let Some((_, history)) = sessions.get_mut(session_id) {
                history.entries.push(entry);
            }
        }
    }
}

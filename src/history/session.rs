use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::bootstrap::{InteractionSurface, SessionMode};
use crate::core::events::SessionMilestone;
use crate::core::message::Message;
use crate::plan::types::PlanState;
use crate::task::list_types::TaskListSnapshot;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRestoreRequest {
    pub resume: Option<String>,
    pub continue_session: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub cwd: String,
    pub last_turn_at: Option<String>,
    pub prompt_seed: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHistoryEntry {
    pub message: Message,
    pub timestamp: Option<String>,
    pub tool_refs: Vec<String>,
    pub milestone: Option<SessionMilestone>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHistory {
    pub entries: Vec<SessionHistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub title: String,
}

pub trait SessionStore: Send + Sync {
    fn load(&self, request: &SessionRestoreRequest) -> Option<(SessionSnapshot, SessionHistory)>;
    fn save(&self, snapshot: SessionSnapshot, history: SessionHistory);
    fn append_entry(&self, session_id: &SessionId, entry: SessionHistoryEntry);
    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot>;
    fn save_task_list(&self, session_id: &SessionId, snapshot: TaskListSnapshot);
    fn load_plan_state(&self, session_id: &SessionId) -> Option<PlanState>;
    fn save_plan_state(&self, session_id: &SessionId, state: PlanState);
    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String>;
    fn save_external_memory_entries(&self, session_id: &SessionId, entries: Vec<String>);
    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String>;
    fn save_nested_memory_lineage(&self, session_id: &SessionId, lineage: Vec<String>);
}

#[derive(Debug, Clone, Default)]
pub struct InMemorySessionStore {
    sessions: Arc<RwLock<HashMap<SessionId, (SessionSnapshot, SessionHistory)>>>,
    task_lists: Arc<RwLock<HashMap<SessionId, TaskListSnapshot>>>,
    plan_states: Arc<RwLock<HashMap<SessionId, PlanState>>>,
    external_memory_entries: Arc<RwLock<HashMap<SessionId, Vec<String>>>>,
    nested_memory_lineage: Arc<RwLock<HashMap<SessionId, Vec<String>>>>,
    latest_session: Arc<RwLock<Option<SessionId>>>,
}

impl InMemorySessionStore {
    pub fn insert(&self, snapshot: SessionSnapshot, history: SessionHistory) {
        self.save(snapshot, history);
    }

    pub fn insert_task_list(&self, session_id: SessionId, snapshot: TaskListSnapshot) {
        self.save_task_list(&session_id, snapshot);
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

    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot> {
        self.task_lists.read().ok()?.get(session_id).cloned()
    }

    fn save_task_list(&self, session_id: &SessionId, snapshot: TaskListSnapshot) {
        if let Ok(mut task_lists) = self.task_lists.write() {
            task_lists.insert(session_id.clone(), snapshot);
        }
    }

    fn load_plan_state(&self, session_id: &SessionId) -> Option<PlanState> {
        self.plan_states.read().ok()?.get(session_id).cloned()
    }

    fn save_plan_state(&self, session_id: &SessionId, state: PlanState) {
        if let Ok(mut plan_states) = self.plan_states.write() {
            plan_states.insert(session_id.clone(), state);
        }
    }

    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String> {
        self.external_memory_entries
            .read()
            .ok()
            .and_then(|entries| entries.get(session_id).cloned())
            .unwrap_or_default()
    }

    fn save_external_memory_entries(&self, session_id: &SessionId, entries: Vec<String>) {
        if let Ok(mut external_memory_entries) = self.external_memory_entries.write() {
            external_memory_entries.insert(session_id.clone(), entries);
        }
    }

    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String> {
        self.nested_memory_lineage
            .read()
            .ok()
            .and_then(|lineage| lineage.get(session_id).cloned())
            .unwrap_or_default()
    }

    fn save_nested_memory_lineage(&self, session_id: &SessionId, lineage: Vec<String>) {
        if let Ok(mut nested_memory_lineage) = self.nested_memory_lineage.write() {
            nested_memory_lineage.insert(session_id.clone(), lineage);
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileBackedSessionStore {
    root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSessionRecord {
    snapshot: SessionSnapshot,
    history: SessionHistory,
    task_list: Option<TaskListSnapshot>,
    plan_state: Option<PlanState>,
    external_memory_entries: Option<Vec<String>>,
    nested_memory_lineage: Option<Vec<String>>,
}

impl FileBackedSessionStore {
    pub fn new(root: PathBuf) -> Self {
        let store = Self { root };
        store.ensure_root();
        store
    }

    pub fn default_root() -> PathBuf {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".rust-agent")
            .join("sessions")
    }

    fn ensure_root(&self) {
        let _ = std::fs::create_dir_all(&self.root);
    }

    fn latest_path(&self) -> PathBuf {
        self.root.join("latest_session")
    }

    fn session_path(&self, session_id: &SessionId) -> PathBuf {
        self.root
            .join(format!("{}.json", sanitize_session_id(&session_id.0)))
    }

    fn load_latest_session_id(&self) -> Option<SessionId> {
        let content = std::fs::read_to_string(self.latest_path()).ok()?;
        let trimmed = content.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(SessionId(trimmed.to_string()))
        }
    }

    fn write_latest_session_id(&self, session_id: &SessionId) {
        let _ = std::fs::write(self.latest_path(), &session_id.0);
    }

    fn read_record(&self, session_id: &SessionId) -> Option<PersistedSessionRecord> {
        let path = self.session_path(session_id);
        let raw = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&raw).ok()
    }

    fn write_record(&self, session_id: &SessionId, record: &PersistedSessionRecord) {
        self.ensure_root();
        let path = self.session_path(session_id);
        let raw = serde_json::to_string_pretty(record)
            .expect("session record serialization should succeed");
        let _ = std::fs::write(path, raw);
        self.write_latest_session_id(session_id);
    }

    fn update_record(
        &self,
        session_id: &SessionId,
        update: impl FnOnce(&mut PersistedSessionRecord),
    ) {
        let mut record = self
            .read_record(session_id)
            .unwrap_or_else(|| PersistedSessionRecord {
                snapshot: SessionSnapshot {
                    session_id: session_id.clone(),
                    surface: InteractionSurface::Cli,
                    session_mode: SessionMode::Headless,
                    cwd: String::new(),
                    last_turn_at: None,
                    prompt_seed: None,
                },
                history: SessionHistory::default(),
                task_list: None,
                plan_state: None,
                external_memory_entries: None,
                nested_memory_lineage: None,
            });
        update(&mut record);
        self.write_record(session_id, &record);
    }
}

impl Default for FileBackedSessionStore {
    fn default() -> Self {
        Self::new(Self::default_root())
    }
}

impl SessionStore for FileBackedSessionStore {
    fn load(&self, request: &SessionRestoreRequest) -> Option<(SessionSnapshot, SessionHistory)> {
        let target = if request.continue_session {
            self.load_latest_session_id()
        } else {
            request
                .resume
                .as_ref()
                .map(|session_id| SessionId(session_id.clone()))
        }?;
        let record = self.read_record(&target)?;
        Some((record.snapshot, record.history))
    }

    fn save(&self, snapshot: SessionSnapshot, history: SessionHistory) {
        let session_id = snapshot.session_id.clone();
        let record = self.read_record(&session_id);
        let task_list = record.as_ref().and_then(|record| record.task_list.clone());
        let plan_state = record.as_ref().and_then(|record| record.plan_state.clone());
        let external_memory_entries = record
            .as_ref()
            .and_then(|record| record.external_memory_entries.clone());
        let nested_memory_lineage = record.and_then(|record| record.nested_memory_lineage);
        self.write_record(
            &session_id,
            &PersistedSessionRecord {
                snapshot,
                history,
                task_list,
                plan_state,
                external_memory_entries,
                nested_memory_lineage,
            },
        );
    }

    fn append_entry(&self, session_id: &SessionId, entry: SessionHistoryEntry) {
        self.update_record(session_id, |record| {
            record.history.entries.push(entry);
        });
    }

    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot> {
        self.read_record(session_id)
            .and_then(|record| record.task_list)
    }

    fn save_task_list(&self, session_id: &SessionId, snapshot: TaskListSnapshot) {
        self.update_record(session_id, |record| {
            record.task_list = Some(snapshot);
        });
    }

    fn load_plan_state(&self, session_id: &SessionId) -> Option<PlanState> {
        self.read_record(session_id)
            .and_then(|record| record.plan_state)
    }

    fn save_plan_state(&self, session_id: &SessionId, state: PlanState) {
        self.update_record(session_id, |record| {
            record.plan_state = Some(state);
        });
    }

    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String> {
        self.read_record(session_id)
            .and_then(|record| record.external_memory_entries)
            .unwrap_or_default()
    }

    fn save_external_memory_entries(&self, session_id: &SessionId, entries: Vec<String>) {
        self.update_record(session_id, |record| {
            record.external_memory_entries = Some(entries);
        });
    }

    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String> {
        self.read_record(session_id)
            .and_then(|record| record.nested_memory_lineage)
            .unwrap_or_default()
    }

    fn save_nested_memory_lineage(&self, session_id: &SessionId, lineage: Vec<String>) {
        self.update_record(session_id, |record| {
            record.nested_memory_lineage = Some(lineage);
        });
    }
}

fn sanitize_session_id(session_id: &str) -> String {
    session_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

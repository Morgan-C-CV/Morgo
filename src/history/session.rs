use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionLifecycleStatus {
    #[default]
    Active,
    Stale,
    Hibernating,
    Expired,
}

impl SessionLifecycleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Hibernating => "hibernating",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStoreWriteErrorKind {
    LockPoisoned,
    Serialize,
    IoTransient,
    IoPermanent,
}

impl SessionStoreWriteErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LockPoisoned => "lock_poisoned",
            Self::Serialize => "serialize",
            Self::IoTransient => "io_transient",
            Self::IoPermanent => "io_permanent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStoreWriteError {
    pub operation: &'static str,
    pub kind: SessionStoreWriteErrorKind,
    pub detail: String,
}

impl SessionStoreWriteError {
    pub fn is_transient(&self) -> bool {
        self.kind == SessionStoreWriteErrorKind::IoTransient
    }

    pub fn as_str(&self) -> &'static str {
        self.kind.as_str()
    }

    fn lock_poisoned(operation: &'static str) -> Self {
        Self {
            operation,
            kind: SessionStoreWriteErrorKind::LockPoisoned,
            detail: "session store lock poisoned".into(),
        }
    }

    fn serialize(operation: &'static str, error: serde_json::Error) -> Self {
        Self {
            operation,
            kind: SessionStoreWriteErrorKind::Serialize,
            detail: error.to_string(),
        }
    }

    fn from_io(operation: &'static str, error: std::io::Error) -> Self {
        let kind = match error.kind() {
            std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WriteZero => SessionStoreWriteErrorKind::IoTransient,
            _ => SessionStoreWriteErrorKind::IoPermanent,
        };
        Self {
            operation,
            kind,
            detail: error.to_string(),
        }
    }
}

pub trait SessionStore: Send + Sync {
    fn load(&self, request: &SessionRestoreRequest) -> Option<(SessionSnapshot, SessionHistory)>;
    fn save(
        &self,
        snapshot: SessionSnapshot,
        history: SessionHistory,
    ) -> Result<(), SessionStoreWriteError>;
    fn save_full_record(
        &self,
        session_id: &SessionId,
        record: PersistedSessionRecord,
    ) -> Result<(), SessionStoreWriteError>;
    fn append_entry(
        &self,
        session_id: &SessionId,
        entry: SessionHistoryEntry,
    ) -> Result<(), SessionStoreWriteError>;
    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot>;
    fn save_task_list(
        &self,
        session_id: &SessionId,
        snapshot: TaskListSnapshot,
    ) -> Result<(), SessionStoreWriteError>;
    fn load_plan_state(&self, session_id: &SessionId) -> Option<PlanState>;
    fn save_plan_state(
        &self,
        session_id: &SessionId,
        state: PlanState,
    ) -> Result<(), SessionStoreWriteError>;
    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String>;
    fn save_external_memory_entries(
        &self,
        session_id: &SessionId,
        entries: Vec<String>,
    ) -> Result<(), SessionStoreWriteError>;
    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String>;
    fn save_nested_memory_lineage(
        &self,
        session_id: &SessionId,
        lineage: Vec<String>,
    ) -> Result<(), SessionStoreWriteError>;
    fn load_lifecycle_status(&self, session_id: &SessionId) -> SessionLifecycleStatus;
    fn save_lifecycle_status(
        &self,
        session_id: &SessionId,
        status: SessionLifecycleStatus,
    ) -> Result<(), SessionStoreWriteError>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemorySessionStore {
    sessions: Arc<RwLock<HashMap<SessionId, (SessionSnapshot, SessionHistory)>>>,
    task_lists: Arc<RwLock<HashMap<SessionId, TaskListSnapshot>>>,
    plan_states: Arc<RwLock<HashMap<SessionId, PlanState>>>,
    external_memory_entries: Arc<RwLock<HashMap<SessionId, Vec<String>>>>,
    nested_memory_lineage: Arc<RwLock<HashMap<SessionId, Vec<String>>>>,
    lifecycle_statuses: Arc<RwLock<HashMap<SessionId, SessionLifecycleStatus>>>,
    latest_session: Arc<RwLock<Option<SessionId>>>,
}

impl InMemorySessionStore {
    pub fn insert(&self, snapshot: SessionSnapshot, history: SessionHistory) {
        self.save(snapshot, history)
            .expect("in-memory session save should not fail");
    }

    pub fn insert_task_list(&self, session_id: SessionId, snapshot: TaskListSnapshot) {
        self.save_task_list(&session_id, snapshot)
            .expect("in-memory task-list save should not fail");
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

    fn save(
        &self,
        snapshot: SessionSnapshot,
        history: SessionHistory,
    ) -> Result<(), SessionStoreWriteError> {
        let mut latest = self
            .latest_session
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save.latest_session"))?;
        *latest = Some(snapshot.session_id.clone());
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save.sessions"))?;
        sessions.insert(snapshot.session_id.clone(), (snapshot, history));
        Ok(())
    }

    fn save_full_record(
        &self,
        session_id: &SessionId,
        record: PersistedSessionRecord,
    ) -> Result<(), SessionStoreWriteError> {
        let mut latest = self.latest_session.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_full_record.latest_session")
        })?;
        *latest = Some(session_id.clone());
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save_full_record.sessions"))?;
        sessions.insert(
            session_id.clone(),
            (record.snapshot.clone(), record.history.clone()),
        );
        drop(sessions);

        let mut task_lists = self
            .task_lists
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save_full_record.task_lists"))?;
        if let Some(task_list) = record.task_list {
            task_lists.insert(session_id.clone(), task_list);
        } else {
            task_lists.remove(session_id);
        }
        drop(task_lists);

        let mut plan_states = self
            .plan_states
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save_full_record.plan_states"))?;
        if let Some(plan_state) = record.plan_state {
            plan_states.insert(session_id.clone(), plan_state);
        } else {
            plan_states.remove(session_id);
        }
        drop(plan_states);

        let mut external_memory_entries = self.external_memory_entries.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_full_record.external_memory_entries")
        })?;
        if let Some(entries) = record.external_memory_entries {
            external_memory_entries.insert(session_id.clone(), entries);
        } else {
            external_memory_entries.remove(session_id);
        }
        drop(external_memory_entries);

        let mut nested_memory_lineage = self.nested_memory_lineage.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_full_record.nested_memory_lineage")
        })?;
        if let Some(lineage) = record.nested_memory_lineage {
            nested_memory_lineage.insert(session_id.clone(), lineage);
        } else {
            nested_memory_lineage.remove(session_id);
        }
        drop(nested_memory_lineage);

        let mut lifecycle_statuses = self.lifecycle_statuses.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_full_record.lifecycle_statuses")
        })?;
        lifecycle_statuses.insert(session_id.clone(), record.lifecycle_status);
        Ok(())
    }

    fn append_entry(
        &self,
        session_id: &SessionId,
        entry: SessionHistoryEntry,
    ) -> Result<(), SessionStoreWriteError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("append_entry.sessions"))?;
        if let Some((_, history)) = sessions.get_mut(session_id) {
            history.entries.push(entry);
        }
        Ok(())
    }

    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot> {
        self.task_lists.read().ok()?.get(session_id).cloned()
    }

    fn save_task_list(
        &self,
        session_id: &SessionId,
        snapshot: TaskListSnapshot,
    ) -> Result<(), SessionStoreWriteError> {
        let mut task_lists = self
            .task_lists
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save_task_list.task_lists"))?;
        task_lists.insert(session_id.clone(), snapshot);
        Ok(())
    }

    fn load_plan_state(&self, session_id: &SessionId) -> Option<PlanState> {
        self.plan_states.read().ok()?.get(session_id).cloned()
    }

    fn save_plan_state(
        &self,
        session_id: &SessionId,
        state: PlanState,
    ) -> Result<(), SessionStoreWriteError> {
        let mut plan_states = self
            .plan_states
            .write()
            .map_err(|_| SessionStoreWriteError::lock_poisoned("save_plan_state.plan_states"))?;
        plan_states.insert(session_id.clone(), state);
        Ok(())
    }

    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String> {
        self.external_memory_entries
            .read()
            .ok()
            .and_then(|entries| entries.get(session_id).cloned())
            .unwrap_or_default()
    }

    fn save_external_memory_entries(
        &self,
        session_id: &SessionId,
        entries: Vec<String>,
    ) -> Result<(), SessionStoreWriteError> {
        let mut external_memory_entries = self.external_memory_entries.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_external_memory_entries.entries")
        })?;
        external_memory_entries.insert(session_id.clone(), entries);
        Ok(())
    }

    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String> {
        self.nested_memory_lineage
            .read()
            .ok()
            .and_then(|lineage| lineage.get(session_id).cloned())
            .unwrap_or_default()
    }

    fn save_nested_memory_lineage(
        &self,
        session_id: &SessionId,
        lineage: Vec<String>,
    ) -> Result<(), SessionStoreWriteError> {
        let mut nested_memory_lineage = self.nested_memory_lineage.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_nested_memory_lineage.lineage")
        })?;
        nested_memory_lineage.insert(session_id.clone(), lineage);
        Ok(())
    }

    fn load_lifecycle_status(&self, session_id: &SessionId) -> SessionLifecycleStatus {
        self.lifecycle_statuses
            .read()
            .ok()
            .and_then(|statuses| statuses.get(session_id).copied())
            .unwrap_or_default()
    }

    fn save_lifecycle_status(
        &self,
        session_id: &SessionId,
        status: SessionLifecycleStatus,
    ) -> Result<(), SessionStoreWriteError> {
        let mut lifecycle_statuses = self.lifecycle_statuses.write().map_err(|_| {
            SessionStoreWriteError::lock_poisoned("save_lifecycle_status.lifecycle_statuses")
        })?;
        lifecycle_statuses.insert(session_id.clone(), status);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FileBackedSessionStore {
    root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSessionRecord {
    pub snapshot: SessionSnapshot,
    pub history: SessionHistory,
    pub task_list: Option<TaskListSnapshot>,
    pub plan_state: Option<PlanState>,
    pub external_memory_entries: Option<Vec<String>>,
    pub nested_memory_lineage: Option<Vec<String>>,
    #[serde(default)]
    pub lifecycle_status: SessionLifecycleStatus,
}

impl FileBackedSessionStore {
    pub fn new(root: PathBuf) -> Self {
        let store = Self { root };
        let _ = store.ensure_root();
        store
    }

    fn default_record(session_id: &SessionId) -> PersistedSessionRecord {
        PersistedSessionRecord {
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
            lifecycle_status: SessionLifecycleStatus::Active,
        }
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    pub fn default_root() -> PathBuf {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".rust-agent")
            .join("sessions")
    }

    fn ensure_root(&self) -> Result<(), SessionStoreWriteError> {
        std::fs::create_dir_all(&self.root)
            .map_err(|error| SessionStoreWriteError::from_io("ensure_root", error))
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

    fn write_latest_session_id(
        &self,
        session_id: &SessionId,
    ) -> Result<(), SessionStoreWriteError> {
        self.ensure_root()?;
        write_atomic(&self.latest_path(), session_id.0.as_bytes())
            .map_err(|error| SessionStoreWriteError::from_io("write_latest_session_id", error))
    }

    fn read_record(&self, session_id: &SessionId) -> Option<PersistedSessionRecord> {
        let path = self.session_path(session_id);
        let raw = std::fs::read_to_string(path).ok()?;
        let record: PersistedSessionRecord = serde_json::from_str(&raw).ok()?;
        if is_legacy_record(&raw) {
            let _ = self.write_record(session_id, &record);
        }
        Some(record)
    }

    fn write_record(
        &self,
        session_id: &SessionId,
        record: &PersistedSessionRecord,
    ) -> Result<(), SessionStoreWriteError> {
        self.ensure_root()?;
        let path = self.session_path(session_id);
        let raw = serde_json::to_string_pretty(record)
            .map_err(|error| SessionStoreWriteError::serialize("write_record.serialize", error))?;
        write_atomic(&path, raw.as_bytes())
            .map_err(|error| SessionStoreWriteError::from_io("write_record.atomic", error))?;
        self.write_latest_session_id(session_id)
    }

    fn update_record(
        &self,
        session_id: &SessionId,
        update: impl FnOnce(&mut PersistedSessionRecord),
    ) -> Result<(), SessionStoreWriteError> {
        let mut record = self
            .read_record(session_id)
            .unwrap_or_else(|| Self::default_record(session_id));
        update(&mut record);
        self.write_record(session_id, &record)
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic write target must have a parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;

    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic write target must have a file name",
        )
    })?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_path = parent.join(format!(
        ".{}.tmp-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        nonce
    ));

    let write_result = (|| -> std::io::Result<()> {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        fs::rename(&temp_path, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
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

    fn save(
        &self,
        snapshot: SessionSnapshot,
        history: SessionHistory,
    ) -> Result<(), SessionStoreWriteError> {
        let session_id = snapshot.session_id.clone();
        let record = self.read_record(&session_id);
        let task_list = record.as_ref().and_then(|record| record.task_list.clone());
        let plan_state = record.as_ref().and_then(|record| record.plan_state.clone());
        let external_memory_entries = record
            .as_ref()
            .and_then(|record| record.external_memory_entries.clone());
        let nested_memory_lineage = record.and_then(|record| record.nested_memory_lineage);
        let lifecycle_status = self.load_lifecycle_status(&session_id);
        self.write_record(
            &session_id,
            &PersistedSessionRecord {
                snapshot,
                history,
                task_list,
                plan_state,
                external_memory_entries,
                nested_memory_lineage,
                lifecycle_status,
            },
        )
    }

    fn save_full_record(
        &self,
        session_id: &SessionId,
        record: PersistedSessionRecord,
    ) -> Result<(), SessionStoreWriteError> {
        self.write_record(session_id, &record)
    }

    fn append_entry(
        &self,
        session_id: &SessionId,
        entry: SessionHistoryEntry,
    ) -> Result<(), SessionStoreWriteError> {
        self.update_record(session_id, |record| {
            record.history.entries.push(entry);
        })
    }

    fn load_task_list(&self, session_id: &SessionId) -> Option<TaskListSnapshot> {
        self.read_record(session_id)
            .and_then(|record| record.task_list)
    }

    fn save_task_list(
        &self,
        session_id: &SessionId,
        snapshot: TaskListSnapshot,
    ) -> Result<(), SessionStoreWriteError> {
        self.update_record(session_id, |record| {
            record.task_list = Some(snapshot);
        })
    }

    fn load_plan_state(&self, session_id: &SessionId) -> Option<PlanState> {
        self.read_record(session_id)
            .and_then(|record| record.plan_state)
    }

    fn save_plan_state(
        &self,
        session_id: &SessionId,
        state: PlanState,
    ) -> Result<(), SessionStoreWriteError> {
        self.update_record(session_id, |record| {
            record.plan_state = Some(state);
        })
    }

    fn load_external_memory_entries(&self, session_id: &SessionId) -> Vec<String> {
        self.read_record(session_id)
            .and_then(|record| record.external_memory_entries)
            .unwrap_or_default()
    }

    fn save_external_memory_entries(
        &self,
        session_id: &SessionId,
        entries: Vec<String>,
    ) -> Result<(), SessionStoreWriteError> {
        self.update_record(session_id, |record| {
            record.external_memory_entries = Some(entries);
        })
    }

    fn load_nested_memory_lineage(&self, session_id: &SessionId) -> Vec<String> {
        self.read_record(session_id)
            .and_then(|record| record.nested_memory_lineage)
            .unwrap_or_default()
    }

    fn save_nested_memory_lineage(
        &self,
        session_id: &SessionId,
        lineage: Vec<String>,
    ) -> Result<(), SessionStoreWriteError> {
        self.update_record(session_id, |record| {
            record.nested_memory_lineage = Some(lineage);
        })
    }

    fn load_lifecycle_status(&self, session_id: &SessionId) -> SessionLifecycleStatus {
        self.read_record(session_id)
            .map(|record| record.lifecycle_status)
            .unwrap_or_default()
    }

    fn save_lifecycle_status(
        &self,
        session_id: &SessionId,
        status: SessionLifecycleStatus,
    ) -> Result<(), SessionStoreWriteError> {
        self.update_record(session_id, |record| {
            record.lifecycle_status = status;
        })
    }
}

fn is_legacy_record(raw: &str) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    v.get("external_memory_entries").is_none()
        || v.get("nested_memory_lineage").is_none()
        || v.get("lifecycle_status").is_none()
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

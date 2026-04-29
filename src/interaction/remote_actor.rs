use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RemoteActorStatus {
    #[default]
    Active,
    Idle,
    Terminated,
}

impl RemoteActorStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
            Self::Terminated => "terminated",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteActorRecord {
    pub actor_id: String,
    pub session_id: String,
    pub is_authenticated: bool,
    pub from_trusted_surface: bool,
    pub created_at: String,
    pub last_active_at: String,
    pub request_count: u64,
    pub status: RemoteActorStatus,
}

fn actor_key(session_id: &str, actor_id: &str) -> (String, String) {
    (session_id.to_string(), actor_id.to_string())
}

fn now_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}Z", h, m, s)
}

#[derive(Debug, Clone)]
struct RemoteActorLedger {
    path: PathBuf,
}

impl RemoteActorLedger {
    fn new(root: &Path) -> Self {
        Self {
            path: root.join("actor-records.jsonl"),
        }
    }

    fn ensure_parent(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }
    }

    fn append(&self, record: &RemoteActorRecord) {
        self.ensure_parent();
        let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        else {
            return;
        };
        let Ok(serialized) = serde_json::to_string(record) else {
            return;
        };
        let _ = writeln!(file, "{serialized}");
    }

    fn load(&self) -> Vec<RemoteActorRecord> {
        let Ok(file) = fs::File::open(&self.path) else {
            return Vec::new();
        };
        BufReader::new(file)
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<RemoteActorRecord>(&line).ok())
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct RemoteActorStore {
    records: Arc<RwLock<HashMap<(String, String), RemoteActorRecord>>>,
    ledger: Option<RemoteActorLedger>,
}

impl RemoteActorStore {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn file_backed(root: PathBuf) -> Self {
        let ledger = RemoteActorLedger::new(&root);
        let loaded = ledger.load();
        let mut map = HashMap::new();
        for record in loaded {
            map.insert(actor_key(&record.session_id, &record.actor_id), record);
        }
        Self {
            records: Arc::new(RwLock::new(map)),
            ledger: Some(ledger),
        }
    }

    pub fn default_root_from(base_dir: &Path) -> PathBuf {
        base_dir.join(".rust-agent").join("remote-actors")
    }

    /// Upsert actor record. Returns `true` if this is a newly created actor.
    pub fn upsert(&self, mut incoming: RemoteActorRecord) -> bool {
        let key = actor_key(&incoming.session_id, &incoming.actor_id);
        let mut records = self.records.write().unwrap_or_else(|e| e.into_inner());

        let is_new = if let Some(existing) = records.get(&key) {
            incoming.created_at = existing.created_at.clone();
            incoming.request_count = existing.request_count + 1;
            false
        } else {
            incoming.created_at = incoming.last_active_at.clone();
            incoming.request_count = 1;
            true
        };

        if let Some(ledger) = &self.ledger {
            ledger.append(&incoming);
        }
        records.insert(key, incoming);
        is_new
    }

    pub fn get(&self, session_id: &str, actor_id: &str) -> Option<RemoteActorRecord> {
        self.records
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&actor_key(session_id, actor_id))
            .cloned()
    }

    pub fn all(&self) -> Vec<RemoteActorRecord> {
        self.records
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
}

pub fn make_actor_record(
    session_id: &str,
    actor_id: &str,
    is_authenticated: bool,
    from_trusted_surface: bool,
) -> RemoteActorRecord {
    let ts = now_timestamp();
    RemoteActorRecord {
        actor_id: actor_id.to_string(),
        session_id: session_id.to_string(),
        is_authenticated,
        from_trusted_surface,
        created_at: ts.clone(),
        last_active_at: ts,
        request_count: 0,
        status: RemoteActorStatus::Active,
    }
}

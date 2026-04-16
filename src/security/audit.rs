use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEvent {
    ToolChecked { tool_name: String },
    ToolDenied { tool_name: String, reason: String },
    TaskStarted { task_id: String },
    TaskFinished { task_id: String, status: String },
    SurfaceDenied { actor_id: String, reason: String },
    RemoteRequestAccepted {
        session_id: String,
        actor_id: String,
        from_trusted_surface: bool,
    },
    RemoteRequestDenied {
        session_id: String,
        actor_id: String,
        reason: String,
        outcome: String,
    },
    RemoteNotificationQueued {
        session_id: String,
        actor_id: Option<String>,
        notification_type: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub timestamp: String,
    pub event_kind: String,
    pub session_id: Option<String>,
    pub actor_id: Option<String>,
    pub surface: Option<String>,
    pub outcome: String,
    pub event: AuditEvent,
}

impl AuditRecord {
    pub fn from_event(event: AuditEvent) -> Self {
        let timestamp = chrono_like_timestamp();
        match &event {
            AuditEvent::ToolChecked { .. } => Self {
                timestamp,
                event_kind: "tool_checked".into(),
                session_id: None,
                actor_id: None,
                surface: None,
                outcome: "checked".into(),
                event,
            },
            AuditEvent::ToolDenied { .. } => Self {
                timestamp,
                event_kind: "tool_denied".into(),
                session_id: None,
                actor_id: None,
                surface: None,
                outcome: "denied".into(),
                event,
            },
            AuditEvent::TaskStarted { .. } => Self {
                timestamp,
                event_kind: "task_started".into(),
                session_id: None,
                actor_id: None,
                surface: None,
                outcome: "started".into(),
                event,
            },
            AuditEvent::TaskFinished { status, .. } => Self {
                timestamp,
                event_kind: "task_finished".into(),
                session_id: None,
                actor_id: None,
                surface: None,
                outcome: status.clone(),
                event,
            },
            AuditEvent::SurfaceDenied { actor_id, .. } => Self {
                timestamp,
                event_kind: "surface_denied".into(),
                session_id: None,
                actor_id: Some(actor_id.clone()),
                surface: None,
                outcome: "denied".into(),
                event,
            },
            AuditEvent::RemoteRequestAccepted {
                session_id,
                actor_id,
                ..
            } => Self {
                timestamp,
                event_kind: "remote_request_accepted".into(),
                session_id: Some(session_id.clone()),
                actor_id: Some(actor_id.clone()),
                surface: Some("remote".into()),
                outcome: "accepted".into(),
                event,
            },
            AuditEvent::RemoteRequestDenied {
                session_id,
                actor_id,
                outcome,
                ..
            } => Self {
                timestamp,
                event_kind: format!("remote_request_denied_{}", outcome),
                session_id: Some(session_id.clone()),
                actor_id: Some(actor_id.clone()),
                surface: Some("remote".into()),
                outcome: outcome.clone(),
                event,
            },
            AuditEvent::RemoteNotificationQueued {
                session_id,
                actor_id,
                ..
            } => Self {
                timestamp,
                event_kind: "remote_notification_queued".into(),
                session_id: Some(session_id.clone()),
                actor_id: actor_id.clone(),
                surface: Some("remote".into()),
                outcome: "queued".into(),
                event,
            },
        }
    }
}

#[derive(Debug, Clone)]
struct AuditLedger {
    path: PathBuf,
}

impl AuditLedger {
    fn new(root: PathBuf) -> Self {
        Self {
            path: root.join("audit-records.jsonl"),
        }
    }

    fn ensure_parent(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }
    }

    fn append(&self, record: &AuditRecord) {
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

    fn load(&self) -> Vec<AuditRecord> {
        let Ok(file) = fs::File::open(&self.path) else {
            return Vec::new();
        };
        BufReader::new(file)
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<AuditRecord>(&line).ok())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct AuditLog {
    events: Vec<AuditEvent>,
    records: Vec<AuditRecord>,
    ledger: Option<AuditLedger>,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            records: Vec::new(),
            ledger: None,
        }
    }
}

impl AuditLog {
    pub fn file_backed(root: PathBuf) -> Self {
        let ledger = AuditLedger::new(root);
        let records = ledger.load();
        let events = records.iter().cloned().map(|record| record.event).collect();
        Self {
            events,
            records: ledger.load(),
            ledger: Some(ledger),
        }
    }

    pub fn default_root_from(base_dir: &Path) -> PathBuf {
        base_dir.join(".rust-agent").join("audit")
    }

    pub fn record(&mut self, event: AuditEvent) {
        let record = AuditRecord::from_event(event.clone());
        if let Some(ledger) = &self.ledger {
            ledger.append(&record);
        }
        self.events.push(event);
        self.records.push(record);
    }

    pub fn events(&self) -> &[AuditEvent] {
        &self.events
    }

    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }

    pub fn load_records(&self) -> Vec<AuditRecord> {
        self.ledger
            .as_ref()
            .map(|ledger| ledger.load())
            .unwrap_or_else(|| self.records.clone())
    }
}

fn chrono_like_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", duration.as_secs(), duration.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::{AuditEvent, AuditLog};

    #[test]
    fn audit_log_records_event_shape_in_memory() {
        let mut audit_log = AuditLog::default();
        audit_log.record(AuditEvent::RemoteRequestDenied {
            session_id: "session-1".into(),
            actor_id: "actor-1".into(),
            reason: "rate_limited: actor actor-1 exceeded request rate for Remote surface".into(),
            outcome: "rate_limited".into(),
        });

        assert_eq!(audit_log.events().len(), 1);
        assert_eq!(audit_log.records().len(), 1);
        let record = &audit_log.records()[0];
        assert_eq!(record.event_kind, "remote_request_denied_rate_limited");
        assert_eq!(record.session_id.as_deref(), Some("session-1"));
        assert_eq!(record.actor_id.as_deref(), Some("actor-1"));
        assert_eq!(record.surface.as_deref(), Some("remote"));
        assert_eq!(record.outcome, "rate_limited");
    }

    #[test]
    fn audit_log_appends_persists_and_loads_records() {
        let temp_root = std::env::temp_dir().join(format!(
            "rust-agent-audit-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should be after unix epoch")
                .as_nanos()
        ));
        let mut audit_log = AuditLog::file_backed(temp_root.clone());
        audit_log.record(AuditEvent::RemoteRequestDenied {
            session_id: "session-1".into(),
            actor_id: "actor-1".into(),
            reason: "not_allowlisted: actor actor-1 is not allowlisted for Remote surface".into(),
            outcome: "not_allowlisted".into(),
        });
        audit_log.record(AuditEvent::RemoteNotificationQueued {
            session_id: "session-1".into(),
            actor_id: Some("actor-1".into()),
            notification_type: "approval_required".into(),
        });

        let reloaded = AuditLog::file_backed(temp_root.clone());
        let records = reloaded.load_records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].event_kind, "remote_request_denied_not_allowlisted");
        assert_eq!(records[1].event_kind, "remote_notification_queued");

        let _ = std::fs::remove_dir_all(temp_root);
    }
}

use crate::bootstrap::model_profiles::ModelLevel;
use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::history::session::{
    SessionHistory, SessionId, SessionRestoreRequest, SessionSnapshot, SessionStore,
};
use crate::history::transcript::Transcript;
use crate::state::permission_context::{
    sanitize_external_memory_entries, sanitize_nested_memory_lineage,
};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static FRESH_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

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
pub struct FreshSessionRequest {
    pub parent_session_id: Option<SessionId>,
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub cwd: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredSession {
    pub snapshot: SessionSnapshot,
    pub history: SessionHistory,
    pub transcript: Transcript,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSessionState {
    pub snapshot: SessionSnapshot,
    pub parent_session_id: Option<SessionId>,
    pub history: SessionHistory,
    pub restored_session: Option<RestoredSession>,
    pub client_type: ClientType,
    pub session_source: SessionSource,
    pub model_level_override: Option<ModelLevel>,
    pub external_memory_entries: Vec<String>,
    pub nested_memory_lineage: Vec<String>,
}

impl ResolvedSessionState {
    pub fn active_session_id(&self) -> String {
        self.snapshot.session_id.0.clone()
    }
}

pub fn resolve_session_state(
    session_store: &dyn SessionStore,
    request: Option<&RestoreRequest>,
    detected_surface: InteractionSurface,
    detected_mode: SessionMode,
    current_cwd: &Path,
) -> ResolvedSessionState {
    if let Some(request) = request {
        let store_request = SessionRestoreRequest {
            resume: request.session_id.clone(),
            continue_session: matches!(request.source, RestoreSource::ContinueSession),
        };
        if let Some((snapshot, history)) = session_store.load(&store_request) {
            let session_id = snapshot.session_id.clone();
            return resolved_from_snapshot(
                snapshot,
                history,
                true,
                session_store.load_model_level_override(&session_id),
                session_store.load_external_memory_entries(&session_id),
                session_store.load_nested_memory_lineage(&session_id),
            );
        }

        let fallback_session_id = request
            .session_id
            .clone()
            .unwrap_or_else(|| "latest-session".into());
        return resolved_from_snapshot(
            SessionSnapshot {
                session_id: SessionId(fallback_session_id),
                surface: detected_surface,
                session_mode: detected_mode,
                cwd: current_cwd.display().to_string(),
                last_turn_at: None,
                prompt_seed: None,
            },
            SessionHistory::default(),
            true,
            None,
            Vec::new(),
            Vec::new(),
        );
    }

    resolved_from_snapshot(
        SessionSnapshot {
            session_id: generate_fresh_session_id(),
            surface: detected_surface,
            session_mode: detected_mode,
            cwd: current_cwd.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
        false,
        None,
        Vec::new(),
        Vec::new(),
    )
}

pub fn resolved_from_snapshot(
    snapshot: SessionSnapshot,
    history: SessionHistory,
    restored: bool,
    model_level_override: Option<ModelLevel>,
    external_memory_entries: Vec<String>,
    nested_memory_lineage: Vec<String>,
) -> ResolvedSessionState {
    let restored_session = restored.then(|| RestoredSession {
        snapshot: snapshot.clone(),
        history: history.clone(),
        transcript: Transcript::from(history.clone()),
    });
    let (client_type, session_source) = surface_binding(snapshot.surface);
    ResolvedSessionState {
        snapshot,
        parent_session_id: None,
        history,
        restored_session,
        client_type,
        session_source,
        model_level_override,
        external_memory_entries: sanitize_external_memory_entries(external_memory_entries),
        nested_memory_lineage: sanitize_nested_memory_lineage(nested_memory_lineage),
    }
}

pub fn build_fresh_session_state(request: FreshSessionRequest) -> ResolvedSessionState {
    let snapshot = SessionSnapshot {
        session_id: generate_fresh_session_id(),
        surface: request.surface,
        session_mode: request.session_mode,
        cwd: request.cwd,
        last_turn_at: None,
        prompt_seed: None,
    };
    let (client_type, session_source) = surface_binding(snapshot.surface);
    ResolvedSessionState {
        snapshot,
        parent_session_id: request.parent_session_id,
        history: SessionHistory::default(),
        restored_session: None,
        client_type,
        session_source,
        model_level_override: None,
        external_memory_entries: Vec::new(),
        nested_memory_lineage: Vec::new(),
    }
}

pub fn surface_binding(surface: InteractionSurface) -> (ClientType, SessionSource) {
    match surface {
        InteractionSurface::Cli => (ClientType::Cli, SessionSource::LocalCli),
        InteractionSurface::Telegram => (ClientType::Bot, SessionSource::Telegram),
        InteractionSurface::Remote => (ClientType::RemoteControl, SessionSource::RemoteControl),
    }
}

fn generate_fresh_session_id() -> SessionId {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let counter = FRESH_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    SessionId(format!(
        "session-{:x}-{:x}-{:x}",
        now_ms,
        std::process::id(),
        counter
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_fresh_session_state_records_parent_and_starts_empty() {
        let resolved = build_fresh_session_state(FreshSessionRequest {
            parent_session_id: Some(SessionId("parent-1".into())),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/work".into(),
        });

        assert_eq!(
            resolved.parent_session_id.as_ref().map(|id| id.0.as_str()),
            Some("parent-1")
        );
        assert!(resolved.history.entries.is_empty());
        assert!(resolved.restored_session.is_none());
        assert!(resolved.external_memory_entries.is_empty());
        assert!(resolved.nested_memory_lineage.is_empty());
    }
}

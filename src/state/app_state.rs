use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::cost::tracker::CostTracker;
use std::sync::Arc;

use crate::history::resume::RestoredSession;
use crate::history::session::{SessionHistory, SessionSnapshot, SessionStore};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::state::permission_context::ToolPermissionContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRole {
    Coordinator,
    Worker,
}

#[derive(Clone)]
pub struct AppState {
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub client_type: ClientType,
    pub session_source: SessionSource,
    pub runtime_role: RuntimeRole,
    pub permission_context: ToolPermissionContext,
    pub cost_tracker: CostTracker,
    pub notification_dispatcher: NotificationDispatcher,
    pub startup_trace: Vec<String>,
    pub active_session_id: String,
    pub session_store: Option<Arc<dyn SessionStore>>,
    pub session: Option<SessionSnapshot>,
    pub history: Option<SessionHistory>,
    pub restored_session: Option<RestoredSession>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("surface", &self.surface)
            .field("session_mode", &self.session_mode)
            .field("client_type", &self.client_type)
            .field("session_source", &self.session_source)
            .field("runtime_role", &self.runtime_role)
            .field("permission_context", &self.permission_context)
            .field("cost_tracker", &self.cost_tracker)
            .field("notification_dispatcher", &self.notification_dispatcher)
            .field("startup_trace", &self.startup_trace)
            .field("active_session_id", &self.active_session_id)
            .field("has_session_store", &self.session_store.is_some())
            .field("session", &self.session)
            .field("history", &self.history)
            .field("restored_session", &self.restored_session)
            .finish()
    }
}

use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::cost::tracker::CostTracker;
use crate::history::resume::RestoredSession;
use crate::history::session::{SessionHistory, SessionSnapshot};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::state::permission_context::ToolPermissionContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRole {
    Coordinator,
    Worker,
}

#[derive(Debug, Clone)]
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
    pub session: Option<SessionSnapshot>,
    pub history: Option<SessionHistory>,
    pub restored_session: Option<RestoredSession>,
}

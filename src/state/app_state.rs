use crate::bootstrap::{InteractionSurface, SessionMode};
use crate::state::permission_context::ToolPermissionContext;

#[derive(Debug, Clone)]
pub struct AppState {
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub permission_context: ToolPermissionContext,
    pub startup_trace: Vec<String>,
}

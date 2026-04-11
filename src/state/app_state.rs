use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::command::types::CommandResult;
use crate::cost::tracker::CostTracker;
use crate::tool::definition::{ToolCall, ToolResult};
use crate::tool::registry::ToolRegistry;
use std::sync::Arc;

use crate::history::resume::RestoredSession;
use crate::history::session::{SessionHistory, SessionSnapshot, SessionStore};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::state::permission_context::{PermissionMode, ToolPermissionContext};

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
    pub runtime_tool_registry: Option<ToolRegistry>,
    pub cost_tracker: CostTracker,
    pub notification_dispatcher: NotificationDispatcher,
    pub startup_trace: Vec<String>,
    pub active_session_id: String,
    pub session_store: Option<Arc<dyn SessionStore>>,
    pub session: Option<SessionSnapshot>,
    pub history: Option<SessionHistory>,
    pub restored_session: Option<RestoredSession>,
}

impl AppState {
    pub async fn resolve_pending_approval(&self, approved: bool) -> anyhow::Result<CommandResult> {
        let Some(pending) = self.permission_context.pending_approval() else {
            return Ok(CommandResult::Denied("no pending approval in this session".into()));
        };

        if !approved {
            self.permission_context.set_pending_approval(None);
            return Ok(CommandResult::Message(format!(
                "Denied approval for {}",
                pending.tool_name
            )));
        }

        match pending.tool_name.as_str() {
            "EnterPlanMode" => {
                self.permission_context.set_mode(PermissionMode::Plan);
                self.permission_context.set_pending_approval(None);
                Ok(CommandResult::Message(if pending.tool_input.trim().is_empty() {
                    "entered plan mode".into()
                } else {
                    format!("entered plan mode: {}", pending.tool_input.trim())
                }))
            }
            "ExitPlanMode" => {
                self.permission_context.set_mode(PermissionMode::Default);
                self.permission_context.set_pending_approval(None);
                Ok(CommandResult::Message(if pending.tool_input.trim().is_empty() {
                    "plan approved; exited plan mode".into()
                } else {
                    format!("plan approved; exited plan mode: {}", pending.tool_input.trim())
                }))
            }
            tool_name => {
                let result = self
                    .runtime_tool_registry
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("runtime tool registry unavailable for approval"))?
                    .invoke_with_approval(
                        &ToolCall::new(tool_name, pending.tool_input.clone()),
                        &self.permission_context,
                    )
                    .await?;
                self.permission_context.set_pending_approval(None);
                match result {
                    ToolResult::Text(text) => Ok(CommandResult::Message(text)),
                    ToolResult::Denied(reason) => Ok(CommandResult::Denied(reason)),
                    ToolResult::PendingApproval { message, .. } => {
                        Ok(CommandResult::Message(format!("approval still required: {message}")))
                    }
                    ToolResult::Interrupted(reason) => {
                        Ok(CommandResult::Message(format!("Interrupted: {reason}")))
                    }
                    ToolResult::Progress(progress) => Ok(CommandResult::Message(progress)),
                    ToolResult::ResultTooLarge(reason) => {
                        Ok(CommandResult::Message(format!("Result too large: {reason}")))
                    }
                }
            }
        }
    }
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
            .field("has_runtime_tool_registry", &self.runtime_tool_registry.is_some())
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

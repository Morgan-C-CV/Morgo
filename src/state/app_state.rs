use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::command::registry::CommandRegistry;
use crate::command::types::CommandResult;
use crate::cost::tracker::CostTracker;
use crate::plugins::types::PluginLoadResult;
use crate::service::mcp::runtime::McpRuntime;
use crate::skills::registry::SkillRegistry;
use crate::tool::definition::{ToolCall, ToolResult};
use crate::tool::registry::ToolRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::history::resume::{ResolvedSessionState, RestoredSession};
use crate::history::session::{SessionHistory, SessionId, SessionSnapshot, SessionStore};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::state::permission_context::ToolPermissionContext;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppStateRuntimeChange {
    PermissionChanged,
    SurfaceBindingChanged,
    SessionLifecycleChanged,
    PluginSnapshotChanged,
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStateChangeSet {
    pub changes: Vec<AppStateRuntimeChange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRole {
    Coordinator,
    Worker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRole {
    Research,
    Implement,
    Verify,
}

impl WorkerRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Research => "research",
            Self::Implement => "implement",
            Self::Verify => "verify",
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub client_type: ClientType,
    pub session_source: SessionSource,
    pub runtime_role: RuntimeRole,
    pub worker_role: Option<WorkerRole>,
    pub permission_context: ToolPermissionContext,
    pub command_registry: Option<Arc<CommandRegistry>>,
    pub runtime_tool_registry: Option<Arc<RwLock<ToolRegistry>>>,
    pub skill_registry: Option<Arc<SkillRegistry>>,
    pub mcp_runtime: Option<Arc<McpRuntime>>,
    pub plugin_load_result: Option<Arc<PluginLoadResult>>,
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
    pub fn current_working_directory(&self) -> PathBuf {
        self.session
            .as_ref()
            .map(|session| PathBuf::from(session.cwd.clone()))
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."))
    }

    pub fn classify_runtime_changes(previous: &Self, current: &Self) -> AppStateChangeSet {
        let mut changes = Vec::new();
        if previous.permission_context.mode() != current.permission_context.mode() {
            changes.push(AppStateRuntimeChange::PermissionChanged);
        }
        if previous.surface != current.surface
            || previous.client_type != current.client_type
            || previous.session_source != current.session_source
            || previous.active_session_id != current.active_session_id
        {
            changes.push(AppStateRuntimeChange::SurfaceBindingChanged);
        }
        if previous.session != current.session
            || previous.history != current.history
            || previous.restored_session != current.restored_session
        {
            changes.push(AppStateRuntimeChange::SessionLifecycleChanged);
        }
        let previous_plugin = previous.plugin_load_result.as_ref().map(Arc::as_ptr);
        let current_plugin = current.plugin_load_result.as_ref().map(Arc::as_ptr);
        if previous_plugin != current_plugin {
            changes.push(AppStateRuntimeChange::PluginSnapshotChanged);
        }
        if changes.is_empty() {
            changes.push(AppStateRuntimeChange::Noop);
        }
        AppStateChangeSet { changes }
    }

    pub fn bind_surface_session(
        &mut self,
        surface: InteractionSurface,
        client_type: ClientType,
        session_source: SessionSource,
        active_session_id: impl Into<String>,
    ) {
        self.surface = surface;
        self.client_type = client_type;
        self.session_source = session_source;
        self.active_session_id = active_session_id.into();
        self.permission_context = self
            .permission_context
            .clone()
            .with_active_surface(surface)
            .with_active_session_id(self.active_session_id.clone());
    }

    pub fn apply_restored_session(&mut self, restored_session: Option<RestoredSession>) {
        self.restored_session = restored_session.clone();
        self.session = restored_session.as_ref().map(|restored| restored.snapshot.clone());
        self.history = restored_session.map(|restored| restored.history);
    }

    pub fn apply_resolved_session_state(&mut self, resolved: &ResolvedSessionState) {
        self.bind_surface_session(
            resolved.snapshot.surface,
            resolved.client_type,
            resolved.session_source,
            resolved.snapshot.session_id.0.clone(),
        );
        self.session_mode = resolved.snapshot.session_mode;
        self.session = Some(resolved.snapshot.clone());
        self.history = Some(resolved.history.clone());
        self.restored_session = resolved.restored_session.clone();
    }

    pub fn persist_resolved_session_state(&self, resolved: &ResolvedSessionState) {
        let Some(session_store) = &self.session_store else {
            return;
        };
        session_store.save(resolved.snapshot.clone(), resolved.history.clone());
    }

    pub fn current_session_id(&self) -> SessionId {
        self.session
            .as_ref()
            .map(|session| session.session_id.clone())
            .unwrap_or_else(|| SessionId(self.active_session_id.clone()))
    }

    pub async fn resolve_pending_approval(&self, approved: bool) -> anyhow::Result<CommandResult> {
        let Some(pending) = self.permission_context.pending_approval() else {
            return Ok(CommandResult::Denied(
                "no pending approval in this session".into(),
            ));
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
                let message = crate::state::plan_mode::apply_enter_plan_mode(
                    &self.permission_context,
                    &pending.tool_input,
                );
                self.permission_context.set_pending_approval(None);
                Ok(CommandResult::Message(message))
            }
            "ExitPlanMode" => {
                let message = crate::state::plan_mode::apply_exit_plan_mode(
                    &self.permission_context,
                    &pending.tool_input,
                )?;
                self.permission_context.set_pending_approval(None);
                Ok(CommandResult::Message(message))
            }
            tool_name => {
                let registry = self
                    .runtime_tool_registry
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("runtime tool registry unavailable for approval")
                    })?
                    .read()
                    .await;
                let result = registry
                    .invoke_with_approval(
                        &ToolCall::new(tool_name, pending.tool_input.clone()),
                        &self.permission_context,
                    )
                    .await?;
                self.permission_context.set_pending_approval(None);
                match result {
                    ToolResult::Text(text) => Ok(CommandResult::Message(text)),
                    ToolResult::Denied(reason) => Ok(CommandResult::Denied(reason)),
                    ToolResult::PendingApproval { message, .. } => Ok(CommandResult::Message(
                        format!("approval still required: {message}"),
                    )),
                    ToolResult::Interrupted(reason) => {
                        Ok(CommandResult::Message(format!("Interrupted: {reason}")))
                    }
                    ToolResult::Progress(progress) => Ok(CommandResult::Message(progress)),
                    ToolResult::ResultTooLarge(reason) => Ok(CommandResult::Message(format!(
                        "Result too large: {reason}"
                    ))),
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
            .field("worker_role", &self.worker_role)
            .field("permission_context", &self.permission_context)
            .field("has_command_registry", &self.command_registry.is_some())
            .field(
                "has_runtime_tool_registry",
                &self.runtime_tool_registry.is_some(),
            )
            .field("has_skill_registry", &self.skill_registry.is_some())
            .field("has_mcp_runtime", &self.mcp_runtime.is_some())
            .field("has_plugin_load_result", &self.plugin_load_result.is_some())
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

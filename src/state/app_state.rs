use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::command::registry::CommandRegistry;
use crate::command::types::CommandResult;
use crate::core::boss::BossCoordinator;
use crate::core::concurrency::SubagentLimiter;
use crate::cost::tracker::CostTracker;
use crate::interaction::remote_actor::RemoteActorStore;
use crate::plugins::types::PluginLoadResult;
use crate::service::mcp::runtime::McpRuntime;
use crate::service::observability::ServiceObservabilityTracker;
use crate::skills::registry::SkillRegistry;
use crate::tool::definition::{ToolCall, ToolResult};
use crate::tool::registry::ToolRegistry;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::history::resume::{ResolvedSessionState, RestoredSession};
use crate::history::session::{
    PersistedSessionRecord, SessionHistory, SessionHistoryEntry, SessionId, SessionLifecycleStatus,
    SessionRestoreRequest, SessionSnapshot, SessionStore, SessionStoreWriteError,
};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::security::approval_protocol::{
    ApprovalDecision, ApprovalResponse, ApprovalSurface, approval_always_allow_notice,
    approval_always_allow_rule,
};
use crate::security::audit::{AuditEvent, AuditLog};
use crate::state::active_model_runtime::ActiveModelRuntime;
use crate::state::permission_context::ToolPermissionContext;

const SESSION_PERSIST_MAX_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionPersistFailure {
    MissingSessionStore,
    MissingSessionSnapshot,
    StoreWrite(SessionStoreWriteError),
}

impl SessionPersistFailure {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingSessionStore => "missing_session_store",
            Self::MissingSessionSnapshot => "missing_session_snapshot",
            Self::StoreWrite(error) => error.as_str(),
        }
    }

    pub fn reason(&self) -> String {
        match self {
            Self::MissingSessionStore => "missing_session_store".into(),
            Self::MissingSessionSnapshot => "missing_session_snapshot".into(),
            Self::StoreWrite(error) => {
                format!("store_write:{}:{}", error.operation, error.kind.as_str())
            }
        }
    }

    pub fn is_transient(&self) -> bool {
        matches!(self, Self::StoreWrite(error) if error.is_transient())
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveModelProfileSource {
    EnvOverride,
    SessionOverride,
    WorkspaceModelsToml,
    HomeModelsToml,
    ModelsToml,
    BootstrapDefault,
}

impl ActiveModelProfileSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EnvOverride => "env_override",
            Self::SessionOverride => "session_override",
            Self::WorkspaceModelsToml => "workspace_models_toml",
            Self::HomeModelsToml => "home_models_toml",
            Self::ModelsToml => "models_toml",
            Self::BootstrapDefault => "bootstrap_default",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveModelProviderSummary {
    pub provider_id: String,
    pub protocol: String,
    pub compatibility_profile: String,
    pub base_url_host: String,
    pub model: String,
    pub auth_status: String,
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
    pub service_observability_tracker: ServiceObservabilityTracker,
    pub notification_dispatcher: NotificationDispatcher,
    pub audit_log: Arc<Mutex<AuditLog>>,
    pub startup_trace: Vec<String>,
    pub active_model_runtime: Option<ActiveModelRuntime>,
    pub active_model_profile_name: Option<String>,
    pub active_model_profile_source: ActiveModelProfileSource,
    pub active_model_provider_summary: ActiveModelProviderSummary,
    pub active_session_id: String,
    pub session_store: Option<Arc<dyn SessionStore>>,
    pub session: Option<SessionSnapshot>,
    pub history: Option<SessionHistory>,
    pub restored_session: Option<RestoredSession>,
    pub last_activity_ts: Arc<AtomicU64>,
    pub cancellation_token: CancellationToken,
    pub subagent_limiter: Option<Arc<SubagentLimiter>>,
    pub boss_coordinator: Option<Arc<BossCoordinator>>,
    pub remote_actor_store: Option<Arc<RemoteActorStore>>,
}

impl AppState {
    fn stored_session_history(&self, session_id: &SessionId) -> Option<SessionHistory> {
        self.session_store.as_ref().and_then(|session_store| {
            session_store
                .load(&SessionRestoreRequest {
                    resume: Some(session_id.0.clone()),
                    continue_session: false,
                })
                .map(|(_, history)| history)
        })
    }

    pub fn canonical_session_history(&self) -> SessionHistory {
        let session_id = self.current_session_id();
        self.canonical_session_history_for(&session_id, self.history.as_ref())
    }

    pub fn canonical_session_history_entries(&self) -> Vec<SessionHistoryEntry> {
        self.canonical_session_history().entries
    }

    pub fn canonical_session_history_for(
        &self,
        session_id: &SessionId,
        fallback: Option<&SessionHistory>,
    ) -> SessionHistory {
        self.stored_session_history(session_id)
            .or_else(|| fallback.cloned())
            .unwrap_or_default()
    }

    pub fn append_current_session_history_entry(
        &mut self,
        entry: SessionHistoryEntry,
    ) -> Result<(), SessionPersistFailure> {
        self.append_current_session_history_entries(std::iter::once(entry))
    }

    pub fn append_current_session_history_entries<I>(
        &mut self,
        entries: I,
    ) -> Result<(), SessionPersistFailure>
    where
        I: IntoIterator<Item = SessionHistoryEntry>,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(());
        }

        let session_id = self.current_session_id();
        if let Some(session_store) = &self.session_store {
            for entry in &entries {
                persist_store_write_with_retry("append_current_session_history_entries", || {
                    session_store.append_entry(&session_id, entry.clone())
                })?;
            }
        }

        self.history
            .get_or_insert_with(SessionHistory::default)
            .entries
            .extend(entries.iter().cloned());

        if let Some(restored_session) = self.restored_session.as_mut() {
            if restored_session.snapshot.session_id == session_id {
                restored_session.history.entries.extend(entries);
            }
        }

        Ok(())
    }

    pub fn current_working_directory(&self) -> PathBuf {
        self.session
            .as_ref()
            .map(|session| PathBuf::from(session.cwd.clone()))
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."))
    }

    pub fn record_activity(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Use Release ordering to ensure other threads see the updated timestamp
        self.last_activity_ts.store(now, Ordering::Release);
    }

    pub fn get_last_activity_ts(&self) -> u64 {
        // Use Acquire ordering to synchronize with record_activity
        self.last_activity_ts.load(Ordering::Acquire)
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn actor_snapshot(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> Option<crate::interaction::remote_actor::RemoteActorRecord> {
        self.remote_actor_store.as_ref()?.get(session_id, actor_id)
    }

    pub fn persist_current_session_state(&self) -> Result<(), SessionPersistFailure> {
        let Some(session_store) = &self.session_store else {
            return Err(SessionPersistFailure::MissingSessionStore);
        };
        let Some(snapshot) = &self.session else {
            return Err(SessionPersistFailure::MissingSessionSnapshot);
        };
        let session_id = snapshot.session_id.clone();
        let history = self.canonical_session_history_for(&session_id, self.history.as_ref());
        let record = PersistedSessionRecord {
            snapshot: snapshot.clone(),
            history,
            task_list: session_store.load_task_list(&session_id),
            plan_state: session_store.load_plan_state(&session_id),
            external_memory_entries: Some(self.permission_context.external_memory_entries()),
            nested_memory_lineage: Some(self.permission_context.nested_memory_lineage()),
            lifecycle_status: session_store.load_lifecycle_status(&session_id),
            model_level_override: session_store.load_model_level_override(&session_id),
        };
        persist_store_write_with_retry("persist_current_session_state", || {
            session_store.save_full_record(&session_id, record.clone())
        })
    }

    pub fn persist_session_lifecycle(
        &self,
        status: SessionLifecycleStatus,
    ) -> Result<(), SessionPersistFailure> {
        let Some(session_store) = &self.session_store else {
            return Err(SessionPersistFailure::MissingSessionStore);
        };
        let session_id = self.current_session_id();
        persist_store_write_with_retry("persist_session_lifecycle", || {
            session_store.save_lifecycle_status(&session_id, status)
        })
    }

    pub fn current_session_lifecycle(&self) -> SessionLifecycleStatus {
        self.session_store
            .as_ref()
            .map(|store| store.load_lifecycle_status(&self.current_session_id()))
            .unwrap_or_default()
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
        self.session = restored_session
            .as_ref()
            .map(|restored| restored.snapshot.clone());
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
        self.permission_context
            .set_external_memory_entries(resolved.external_memory_entries.clone());
        self.permission_context
            .set_nested_memory_lineage(resolved.nested_memory_lineage.clone());
    }

    pub fn persist_resolved_session_state(
        &self,
        resolved: &ResolvedSessionState,
    ) -> Result<(), SessionPersistFailure> {
        let Some(session_store) = &self.session_store else {
            return Err(SessionPersistFailure::MissingSessionStore);
        };
        let session_id = resolved.snapshot.session_id.clone();
        let history = self.canonical_session_history_for(&session_id, Some(&resolved.history));
        let record = PersistedSessionRecord {
            snapshot: resolved.snapshot.clone(),
            history,
            task_list: session_store.load_task_list(&session_id),
            plan_state: session_store.load_plan_state(&session_id),
            external_memory_entries: Some(self.permission_context.external_memory_entries()),
            nested_memory_lineage: Some(self.permission_context.nested_memory_lineage()),
            lifecycle_status: SessionLifecycleStatus::Active,
            model_level_override: session_store.load_model_level_override(&session_id),
        };
        persist_store_write_with_retry("persist_resolved_session_state", || {
            session_store.save_full_record(&session_id, record.clone())
        })
    }

    pub fn current_session_id(&self) -> SessionId {
        self.session
            .as_ref()
            .map(|session| session.session_id.clone())
            .unwrap_or_else(|| SessionId(self.active_session_id.clone()))
    }

    pub async fn resolve_pending_approval(&self, approved: bool) -> anyhow::Result<CommandResult> {
        let response = if approved {
            ApprovalResponse::ApproveOnce
        } else {
            ApprovalResponse::Deny
        };
        self.resolve_pending_approval_response(response).await
    }

    pub async fn resolve_pending_approval_response(
        &self,
        response: ApprovalResponse,
    ) -> anyhow::Result<CommandResult> {
        let Some(pending) = self.permission_context.pending_approval() else {
            return Ok(CommandResult::Denied(
                "no pending approval in this session".into(),
            ));
        };

        let decision = response.decision();
        let surface = ApprovalSurface::from_interaction_surface(self.surface);

        let emit_audit = |decision: ApprovalDecision| {
            self.audit_log.lock().expect("audit log poisoned").record(
                AuditEvent::ApprovalResolved {
                    tool_name: pending.tool_name.clone(),
                    decision: decision.as_str().to_string(),
                    surface: surface.as_str().to_string(),
                    session_id: Some(self.active_session_id.clone()),
                    actor_id: None,
                    code: pending.code.clone(),
                    approval_kind: pending.approval_kind.clone(),
                    escalation_reasons: pending.escalation_reasons.clone(),
                },
            );
        };

        if matches!(response, ApprovalResponse::Deny) {
            self.permission_context.set_pending_approval(None);
            emit_audit(decision);
            return Ok(CommandResult::Message(format!(
                "Denied approval for {}. The command was not executed. Tell me what to run instead, or ask me to make it safer.",
                pending.tool_name
            )));
        }

        let allow_rule = if matches!(response, ApprovalResponse::ApproveAlways) {
            approval_always_allow_rule(&pending)
        } else {
            None
        };
        let allow_notice = if matches!(response, ApprovalResponse::ApproveAlways) {
            let added = allow_rule
                .as_deref()
                .map(|rule| self.permission_context.add_always_allow_rule(rule))
                .unwrap_or(false);
            Some(approval_always_allow_notice(
                &pending,
                allow_rule.as_deref(),
                added,
            ))
        } else {
            None
        };

        match pending.tool_name.as_str() {
            "EnterPlanMode" => {
                let message = crate::state::plan_mode::apply_enter_plan_mode(
                    &self.permission_context,
                    &pending.tool_input,
                );
                self.permission_context.set_pending_approval(None);
                emit_audit(decision);
                Ok(CommandResult::Message(prepend_notice(allow_notice, message)))
            }
            "ExitPlanMode" => {
                let message = crate::state::plan_mode::apply_exit_plan_mode(
                    &self.permission_context,
                    &pending.tool_input,
                )?;
                self.permission_context.set_pending_approval(None);
                emit_audit(decision);
                Ok(CommandResult::Message(prepend_notice(allow_notice, message)))
            }
            tool_name => {
                let registry_result = self.runtime_tool_registry.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("runtime tool registry unavailable for approval")
                });
                // Emit audit before any fallible operation so approve is always recorded.
                self.permission_context.set_pending_approval(None);
                emit_audit(decision);
                let registry = registry_result?.read().await;
                let result = registry
                    .invoke_with_approval(
                        &ToolCall::new(tool_name, pending.tool_input.clone()),
                        &self.permission_context,
                    )
                    .await?;
                match result {
                    ToolResult::Text(text) => {
                        Ok(CommandResult::Message(prepend_notice(allow_notice, text)))
                    }
                    ToolResult::Denied(reason) => Ok(CommandResult::Denied(reason)),
                    ToolResult::PendingApproval { message, .. } => Ok(CommandResult::Message(
                        prepend_notice(allow_notice, format!("approval still required: {message}")),
                    )),
                    ToolResult::Interrupted(reason) => {
                        Ok(CommandResult::Message(prepend_notice(
                            allow_notice,
                            format!("Interrupted: {reason}"),
                        )))
                    }
                    ToolResult::Progress(progress) => Ok(CommandResult::Message(prepend_notice(
                        allow_notice,
                        progress,
                    ))),
                    ToolResult::ResultTooLarge(reason) => Ok(CommandResult::Message(format!(
                        "{}",
                        prepend_notice(allow_notice, format!("Result too large: {reason}"))
                    ))),
                }
            }
        }
    }
}

fn prepend_notice(notice: Option<String>, body: String) -> String {
    match notice {
        Some(notice) if body.trim().is_empty() => notice,
        Some(notice) => format!("{notice}\n{body}"),
        None => body,
    }
}

fn persist_store_write_with_retry(
    operation: &'static str,
    mut write: impl FnMut() -> Result<(), SessionStoreWriteError>,
) -> Result<(), SessionPersistFailure> {
    let mut attempt = 1;
    loop {
        match write() {
            Ok(()) => return Ok(()),
            Err(error) if error.is_transient() && attempt < SESSION_PERSIST_MAX_ATTEMPTS => {
                tracing::warn!(
                    "session persist transient write failure: operation={} store_operation={} attempt={} detail={}",
                    operation,
                    error.operation,
                    attempt,
                    error.detail
                );
                std::thread::sleep(persist_retry_delay(attempt));
                attempt += 1;
            }
            Err(error) => return Err(SessionPersistFailure::StoreWrite(error)),
        }
    }
}

fn persist_retry_delay(attempt: usize) -> Duration {
    match attempt {
        1 => Duration::from_millis(10),
        _ => Duration::from_millis(25),
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
            .field(
                "service_observability_tracker",
                &self.service_observability_tracker,
            )
            .field("notification_dispatcher", &self.notification_dispatcher)
            .field("startup_trace", &self.startup_trace)
            .field("active_session_id", &self.active_session_id)
            .field("has_session_store", &self.session_store.is_some())
            .field("session", &self.session)
            .field("history", &self.history)
            .field("restored_session", &self.restored_session)
            .field("has_boss_coordinator", &self.boss_coordinator.is_some())
            .finish()
    }
}

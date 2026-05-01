use crate::core::message::{Message, Role};
use crate::hook::registry::HookRegistry;
use crate::prompt::{
    context::build_context_prompt, system::build_system_prompt, tools::build_tools_prompt,
};
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;
use crate::service::compact::ReactiveCompactor;
use crate::state::active_model_runtime::ActiveModelRuntime;
use crate::state::app_state::{AppState, RuntimeRole, WorkerRole};
use crate::state::permission_context::{
    BossActorPolicy, MAX_NESTED_MEMORY_DEPTH, sanitize_nested_memory_lineage,
};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerLisMPolicy {
    Inherit,
    ForceOn,
    ForceOff,
}

impl WorkerLisMPolicy {
    pub fn default_for_role(_worker_role: WorkerRole) -> Self {
        Self::ForceOn
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::ForceOn => "force-on",
            Self::ForceOff => "force-off",
        }
    }

    fn resolve(self, parent_enabled: bool) -> bool {
        match self {
            Self::Inherit => parent_enabled,
            Self::ForceOn => true,
            Self::ForceOff => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubagentConfig {
    pub worker_role: WorkerRole,
    pub inherit_context: bool,
    pub max_turns: Option<usize>,
    pub allowed_tools: Option<Vec<String>>,
    pub lism_policy: WorkerLisMPolicy,
    /// When set, the subagent runtime is assembled with ExecutorB policy and sees Agent tool.
    pub boss_actor_policy: Option<BossActorPolicy>,
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub app_state: AppState,
    pub tool_registry: ToolRegistry,
    pub api_client: ModelProviderClient,
    pub compactor: ReactiveCompactor,
    pub hook_registry: HookRegistry,
    pub agent_id: Option<String>,
    pub system_prompt: String,
    pub tools_prompt: String,
    pub context_prompt: String,
}

impl QueryContext {
    pub fn is_subagent(&self) -> bool {
        self.app_state.runtime_role == RuntimeRole::Worker
    }

    pub fn current_system_prompt(&self) -> String {
        build_system_prompt(&self.app_state)
    }

    pub fn current_tools_prompt(&self) -> String {
        build_tools_prompt(&self.tool_registry, &self.app_state.permission_context)
    }

    pub fn current_context_prompt(&self) -> String {
        build_context_prompt(&self.app_state)
    }

    pub fn compose_turn_prompt(&self, user_input: &str) -> String {
        [
            self.current_system_prompt(),
            self.current_tools_prompt(),
            self.current_context_prompt(),
            user_input.to_string(),
        ]
        .join("\n")
    }

    pub fn compose_turn_prompt_from_messages(&self, messages: &[Message]) -> String {
        let mut sections = vec![
            self.current_system_prompt(),
            self.current_tools_prompt(),
            self.current_context_prompt(),
        ];
        let transcript = render_transcript(messages);
        if !transcript.is_empty() {
            sections.push(transcript);
        }
        sections.join("\n")
    }

    pub fn create_subagent_context(
        &self,
        agent_id: impl Into<String>,
        scripted_turns: Vec<Vec<StreamEvent>>,
        config: SubagentConfig,
    ) -> Self {
        let child_agent_id = agent_id.into();
        let mut app_state = self.app_state.clone();
        app_state.active_session_id = child_agent_id.clone();
        app_state.runtime_role = RuntimeRole::Worker;
        app_state.worker_role = Some(config.worker_role);
        if !config.inherit_context {
            app_state.history = None;
            app_state.restored_session = None;
        }
        let inherited_active_model_snapshot = app_state
            .active_model_runtime
            .as_ref()
            .map(|runtime| runtime.snapshot_blocking());
        app_state.active_model_runtime = inherited_active_model_snapshot
            .as_ref()
            .cloned()
            .map(ActiveModelRuntime::new);
        if let Some(active_model_snapshot) = inherited_active_model_snapshot.as_ref() {
            app_state.active_model_profile_name = active_model_snapshot.active_profile_name.clone();
            app_state.active_model_profile_source = active_model_snapshot.source.clone();
            app_state.active_model_provider_summary = active_model_snapshot.summary.clone();
        }
        let mut permission_context = app_state.permission_context.fork_for_subagent();
        permission_context.set_pending_approval(None);
        permission_context.set_lism_enabled(
            config
                .lism_policy
                .resolve(self.app_state.permission_context.lism_enabled()),
        );
        if let Some(active_model_snapshot) = inherited_active_model_snapshot {
            permission_context =
                permission_context.with_inherited_active_model_snapshot(active_model_snapshot);
        }
        if let Some(policy) = config.boss_actor_policy {
            permission_context = permission_context.with_boss_actor_policy(policy);
            if policy.may_spawn() {
                // ExecutorB is the production execution worker. It may see interactive/open-world
                // tools such as Bash, while execution is still governed by permission and
                // workspace-capability checks at invocation time.
                permission_context = permission_context.with_interactive_tools(true);
            }
        }
        let lineage = build_nested_memory_lineage(self, &child_agent_id, config.inherit_context);
        permission_context.set_nested_memory_lineage(lineage);
        let tool_registry = if permission_context.boss_actor_policy.is_some() {
            use crate::bootstrap::InteractionSurface;
            use crate::bootstrap::SessionMode;
            let assembled = self.tool_registry.assemble(
                crate::tool::registry::ToolAssemblyContext::executor_b(
                    InteractionSurface::Cli,
                    SessionMode::Headless,
                ),
            );
            if assembled
                .all_metadata()
                .iter()
                .any(|metadata| metadata.name == "Bash")
            {
                assembled
            } else {
                // Headless coordinators may have already filtered open-world tools out of the
                // inherited registry. ExecutorB still needs Bash for production execution and
                // verification; invocation remains permission/workspace-gated.
                assembled.register(std::sync::Arc::new(crate::tool::builtin::bash::BashTool))
            }
        } else {
            self.tool_registry
                .assemble_worker_registry(config.allowed_tools.as_deref())
        };
        permission_context.inherited_tool_registry = Some(tool_registry.clone());
        app_state.permission_context = permission_context;

        use std::sync::Arc;
        use tokio::sync::RwLock;
        app_state.runtime_tool_registry = Some(Arc::new(RwLock::new(tool_registry.clone())));

        let api_client = if scripted_turns.is_empty() {
            app_state
                .active_model_runtime
                .as_ref()
                .map(|runtime| runtime.snapshot_blocking().client)
                .unwrap_or_else(ModelProviderClient::default)
        } else {
            ModelProviderClient::with_scripted_turns(scripted_turns)
        };

        Self {
            system_prompt: build_system_prompt(&app_state),
            tools_prompt: build_tools_prompt(&tool_registry, &app_state.permission_context),
            context_prompt: build_context_prompt(&app_state),
            app_state,
            tool_registry,
            api_client,
            compactor: self.compactor.clone(),
            hook_registry: self.hook_registry.clone(),
            agent_id: Some(child_agent_id),
        }
    }
}

fn render_transcript(messages: &[Message]) -> String {
    let mut lines = Vec::new();
    for message in messages {
        let text = message.text();
        if text.trim().is_empty() {
            continue;
        }
        let role = match message.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        };
        if lines.is_empty() {
            lines.push("Conversation transcript:".to_string());
        }
        lines.push(format!("<{role}>"));
        lines.push(text);
        lines.push(format!("</{role}>"));
    }
    lines.join("\n")
}

fn build_nested_memory_lineage(
    parent: &QueryContext,
    child_agent_id: &str,
    inherit_context: bool,
) -> Vec<String> {
    let parent_marker = format!("session:{}", parent.app_state.active_session_id);
    let child_marker = format!("agent:{child_agent_id}:inherit_context={inherit_context}");
    let lineage =
        sanitize_nested_memory_lineage(parent.app_state.permission_context.nested_memory_lineage());
    let mut agent_markers = lineage
        .into_iter()
        .skip_while(|entry| entry.starts_with("session:"))
        .collect::<Vec<_>>();
    if agent_markers.len() >= MAX_NESTED_MEMORY_DEPTH.saturating_sub(1) {
        let keep = MAX_NESTED_MEMORY_DEPTH.saturating_sub(2);
        let skip = agent_markers.len().saturating_sub(keep);
        agent_markers = agent_markers.into_iter().skip(skip).collect();
    }
    let mut bounded = vec![parent_marker];
    bounded.extend(agent_markers);
    bounded.push(child_marker);
    bounded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
    use crate::cost::tracker::CostTracker;
    use crate::interaction::dispatcher::NotificationDispatcher;
    use crate::interaction::telegram::gateway::TelegramGateway;
    use crate::service::compact::ReactiveCompactor;
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::app_state::{
        ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
    };
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    fn test_query_context() -> QueryContext {
        let permission_context = crate::state::permission_context::ToolPermissionContext::new(
            crate::state::permission_context::PermissionMode::Default,
        );
        let tool_registry = ToolRegistry::new();
        let app_state = AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(tool_registry.clone()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(Mutex::new(crate::security::audit::AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "default-provider".into(),
                protocol: "Anthropic".into(),
                compatibility_profile: "Anthropic".into(),
                base_url_host: "localhost".into(),
                model: "default-model".into(),
                auth_status: "unset".into(),
            },
            active_session_id: "parent-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(AtomicU64::new(0)),
            cancellation_token: CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
        };
        QueryContext {
            system_prompt: String::new(),
            tools_prompt: String::new(),
            context_prompt: String::new(),
            app_state,
            tool_registry,
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: HookRegistry::default(),
            agent_id: None,
        }
    }

    #[test]
    fn worker_lism_policy_defaults_to_force_on() {
        assert_eq!(
            WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            WorkerLisMPolicy::ForceOn
        );
        assert_eq!(
            WorkerLisMPolicy::default_for_role(WorkerRole::Implement),
            WorkerLisMPolicy::ForceOn
        );
        assert_eq!(
            WorkerLisMPolicy::default_for_role(WorkerRole::Verify),
            WorkerLisMPolicy::ForceOn
        );
    }

    #[test]
    fn create_subagent_context_force_on_enables_lism_without_mutating_parent() {
        let parent = test_query_context();
        parent.app_state.permission_context.set_lism_enabled(false);

        let child = parent.create_subagent_context(
            "child-agent",
            Vec::new(),
            SubagentConfig {
                worker_role: WorkerRole::Implement,
                inherit_context: false,
                max_turns: None,
                allowed_tools: None,
                lism_policy: WorkerLisMPolicy::ForceOn,
                boss_actor_policy: None,
            },
        );

        assert!(!parent.app_state.permission_context.lism_enabled());
        assert!(child.app_state.permission_context.lism_enabled());
    }

    #[test]
    fn create_subagent_context_inherit_preserves_parent_lism_state() {
        let parent = test_query_context();
        parent.app_state.permission_context.set_lism_enabled(false);

        let child = parent.create_subagent_context(
            "child-agent",
            Vec::new(),
            SubagentConfig {
                worker_role: WorkerRole::Research,
                inherit_context: false,
                max_turns: None,
                allowed_tools: None,
                lism_policy: WorkerLisMPolicy::Inherit,
                boss_actor_policy: None,
            },
        );

        assert!(!child.app_state.permission_context.lism_enabled());
    }
}

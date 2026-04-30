use crate::hook::registry::HookRegistry;
use crate::prompt::{
    context::build_context_prompt, system::build_system_prompt, tools::build_tools_prompt,
};
use crate::core::message::{Message, Role};
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;
use crate::service::compact::ReactiveCompactor;
use crate::state::active_model_runtime::ActiveModelRuntime;
use crate::state::app_state::{AppState, RuntimeRole, WorkerRole};
use crate::state::permission_context::{
    BossActorPolicy, MAX_NESTED_MEMORY_DEPTH, sanitize_nested_memory_lineage,
};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone)]
pub struct SubagentConfig {
    pub worker_role: WorkerRole,
    pub inherit_context: bool,
    pub max_turns: Option<usize>,
    pub allowed_tools: Option<Vec<String>>,
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
        let mut permission_context = app_state.permission_context.clone();
        permission_context.set_pending_approval(None);
        if let Some(active_model_snapshot) = inherited_active_model_snapshot {
            permission_context =
                permission_context.with_inherited_active_model_snapshot(active_model_snapshot);
        }
        if let Some(policy) = config.boss_actor_policy {
            permission_context = permission_context.with_boss_actor_policy(policy);
        }
        let lineage = build_nested_memory_lineage(self, &child_agent_id, config.inherit_context);
        permission_context.set_nested_memory_lineage(lineage);
        let tool_registry = if permission_context.boss_actor_policy.is_some() {
            use crate::bootstrap::InteractionSurface;
            use crate::bootstrap::SessionMode;
            self.tool_registry
                .assemble(crate::tool::registry::ToolAssemblyContext::executor_b(
                    InteractionSurface::Cli,
                    SessionMode::Headless,
                ))
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

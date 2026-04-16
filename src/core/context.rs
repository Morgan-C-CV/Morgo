use crate::hook::registry::HookRegistry;
use crate::prompt::{
    context::build_context_prompt, system::build_system_prompt, tools::build_tools_prompt,
};
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;
use crate::service::compact::ReactiveCompactor;
use crate::state::app_state::{AppState, RuntimeRole, WorkerRole};
use crate::state::permission_context::{
    MAX_NESTED_MEMORY_DEPTH, sanitize_nested_memory_lineage,
};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone)]
pub struct SubagentConfig {
    pub worker_role: WorkerRole,
    pub inherit_context: bool,
    pub max_turns: Option<usize>,
    pub allowed_tools: Option<Vec<String>>,
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
        let mut permission_context = app_state.permission_context.clone();
        permission_context.set_pending_approval(None);
        let lineage = build_nested_memory_lineage(self, &child_agent_id, config.inherit_context);
        permission_context.set_nested_memory_lineage(lineage);
        let tool_registry = self
            .tool_registry
            .assemble_worker_registry(config.allowed_tools.as_deref());
        permission_context.inherited_tool_registry = Some(tool_registry.clone());
        app_state.permission_context = permission_context;

        use std::sync::Arc;
        use tokio::sync::RwLock;
        app_state.runtime_tool_registry = Some(Arc::new(RwLock::new(tool_registry.clone())));

        Self {
            system_prompt: build_system_prompt(&app_state),
            tools_prompt: build_tools_prompt(&tool_registry, &app_state.permission_context),
            context_prompt: build_context_prompt(&app_state),
            app_state,
            tool_registry,
            api_client: ModelProviderClient::with_scripted_turns(scripted_turns),
            compactor: self.compactor.clone(),
            hook_registry: self.hook_registry.clone(),
            agent_id: Some(child_agent_id),
        }
    }
}

fn build_nested_memory_lineage(
    parent: &QueryContext,
    child_agent_id: &str,
    inherit_context: bool,
) -> Vec<String> {
    let parent_marker = format!("session:{}", parent.app_state.active_session_id);
    let child_marker = format!("agent:{child_agent_id}:inherit_context={inherit_context}");
    let lineage = sanitize_nested_memory_lineage(
        parent.app_state.permission_context.nested_memory_lineage(),
    );
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
    sanitize_nested_memory_lineage(bounded)
}

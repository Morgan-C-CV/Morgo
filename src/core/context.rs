use crate::hook::registry::HookRegistry;
use crate::prompt::{context::build_context_prompt, system::build_system_prompt, tools::build_tools_prompt};
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::{AppState, RuntimeRole};
use crate::tool::registry::ToolRegistry;

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

    pub fn create_subagent_context(
        &self,
        agent_id: impl Into<String>,
        scripted_turns: Vec<Vec<StreamEvent>>,
    ) -> Self {
        let mut app_state = self.app_state.clone();
        app_state.runtime_role = RuntimeRole::Worker;
        app_state.history = None;
        app_state.restored_session = None;
        let mut permission_context = app_state.permission_context.clone();
        permission_context.set_pending_approval(None);
        permission_context.inherited_tool_registry = Some(
            self.tool_registry
                .assemble_for_role(RuntimeRole::Worker),
        );
        app_state.permission_context = permission_context;
        let tool_registry = self.tool_registry.assemble_for_role(RuntimeRole::Worker);
        app_state.runtime_tool_registry = Some(tool_registry.clone());
        Self {
            system_prompt: build_system_prompt(&app_state),
            tools_prompt: build_tools_prompt(&tool_registry, &app_state.permission_context),
            context_prompt: build_context_prompt(&app_state),
            app_state,
            tool_registry,
            api_client: ModelProviderClient::with_scripted_turns(scripted_turns),
            compactor: self.compactor.clone(),
            hook_registry: self.hook_registry.clone(),
            agent_id: Some(agent_id.into()),
        }
    }
}

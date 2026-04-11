use crate::hook::registry::HookRegistry;
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
        permission_context.inherited_tool_registry = Some(
            self.tool_registry
                .assemble_for_role(RuntimeRole::Worker),
        );
        app_state.permission_context = permission_context;
        Self {
            app_state,
            tool_registry: self.tool_registry.assemble_for_role(RuntimeRole::Worker),
            api_client: ModelProviderClient::with_scripted_turns(scripted_turns),
            compactor: self.compactor.clone(),
            hook_registry: self.hook_registry.clone(),
            agent_id: Some(agent_id.into()),
        }
    }
}

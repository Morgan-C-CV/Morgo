use crate::hook::registry::HookRegistry;
use crate::service::api::client::AnthropicClient;
use crate::service::api::streaming::StreamEvent;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::AppState;
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub app_state: AppState,
    pub tool_registry: ToolRegistry,
    pub api_client: AnthropicClient,
    pub compactor: ReactiveCompactor,
    pub hook_registry: HookRegistry,
    pub agent_id: Option<String>,
}

impl QueryContext {
    pub fn is_subagent(&self) -> bool {
        self.agent_id.is_some()
    }

    pub fn create_subagent_context(
        &self,
        agent_id: impl Into<String>,
        scripted_turns: Vec<Vec<StreamEvent>>,
    ) -> Self {
        Self {
            app_state: self.app_state.clone(),
            tool_registry: self.tool_registry.clone(),
            api_client: AnthropicClient::with_scripted_turns(scripted_turns),
            compactor: self.compactor.clone(),
            hook_registry: self.hook_registry.clone(),
            agent_id: Some(agent_id.into()),
        }
    }
}

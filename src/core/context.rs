use crate::service::api::client::AnthropicClient;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::AppState;
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub app_state: AppState,
    pub tool_registry: ToolRegistry,
    pub api_client: AnthropicClient,
    pub compactor: ReactiveCompactor,
}

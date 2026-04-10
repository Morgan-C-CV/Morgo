use crate::command::registry::CommandRegistry;
use crate::command::types::CommandResult;
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    ExecuteCommand(String),
    ContinueToQuery,
}

#[derive(Clone)]
pub struct CommandRouter {
    registry: CommandRegistry,
}

impl CommandRouter {
    pub fn new(registry: CommandRegistry) -> Self {
        Self { registry }
    }

    pub fn decide(&self, input: &NormalizedInput) -> RouteDecision {
        match input.command_name.as_deref() {
            Some(name) if self.registry.get(name).is_some() => {
                RouteDecision::ExecuteCommand(name.to_string())
            }
            Some(_) => RouteDecision::ContinueToQuery,
            None => RouteDecision::ContinueToQuery,
        }
    }

    pub async fn route(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        match self.decide(input) {
            RouteDecision::ExecuteCommand(ref name) => {
                let command = self
                    .registry
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("command disappeared during routing"))?;
                command.execute(input, app_state).await
            }
            RouteDecision::ContinueToQuery => Ok(CommandResult::ContinueToQuery),
        }
    }
}

use crate::bootstrap::InteractionSurface;
use crate::command::registry::CommandRegistry;
use crate::command::types::{CommandAvailability, CommandResult, CommandType};
use crate::interaction::envelope::NormalizedInput;
use crate::security::authorizer::{AuthDecision, SurfaceAuthorizer};
use crate::state::app_state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    ExecuteCommand(String),
    ContinueToQuery,
    Deny(String),
}

pub struct CommandRouter {
    registry: CommandRegistry,
    authorizer: Box<dyn SurfaceAuthorizer>,
}

impl CommandRouter {
    pub fn new(registry: CommandRegistry, authorizer: Box<dyn SurfaceAuthorizer>) -> Self {
        Self {
            registry,
            authorizer,
        }
    }

    pub fn decide(&self, input: &NormalizedInput) -> RouteDecision {
        match self
            .authorizer
            .authorize(input.surface, &input.actor, &input.raw)
        {
            AuthDecision::Deny { reason } => return RouteDecision::Deny(reason),
            AuthDecision::Allow => {}
        }

        match input.command_name.as_deref() {
            Some(name) => {
                let Some(command) = self.registry.get(name) else {
                    return RouteDecision::ContinueToQuery;
                };
                let metadata = command.metadata();
                if !command.is_enabled() {
                    return RouteDecision::Deny(format!("command {} is disabled", metadata.name));
                }
                if !Self::is_available(metadata.availability, input.surface) {
                    return RouteDecision::Deny(format!(
                        "command {} is not available on this surface",
                        metadata.name
                    ));
                }
                RouteDecision::ExecuteCommand(metadata.name.to_string())
            }
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
                let metadata = command.metadata();
                let result = command.execute(input, app_state).await?;
                match (metadata.command_type, result) {
                    (CommandType::Prompt, CommandResult::Prompt(prompt)) => {
                        Ok(CommandResult::Prompt(prompt))
                    }
                    (_, result) => Ok(result),
                }
            }
            RouteDecision::ContinueToQuery => Ok(CommandResult::ContinueToQuery),
            RouteDecision::Deny(reason) => Ok(CommandResult::Denied(reason)),
        }
    }

    fn is_available(availability: CommandAvailability, surface: InteractionSurface) -> bool {
        match availability {
            CommandAvailability::Everywhere => true,
            CommandAvailability::CliOnly => matches!(surface, InteractionSurface::Cli),
            CommandAvailability::RemoteSafe => !matches!(surface, InteractionSurface::Cli),
        }
    }
}

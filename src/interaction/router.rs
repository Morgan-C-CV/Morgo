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
    ContinueToQueryWithPrompt(String),
    ApprovalResponse { approved: bool },
    Deny(String),
}

use std::sync::Arc;

pub struct CommandRouter {
    registry: Arc<CommandRegistry>,
    authorizer: Box<dyn SurfaceAuthorizer>,
}

impl CommandRouter {
    pub fn new(registry: Arc<CommandRegistry>, authorizer: Box<dyn SurfaceAuthorizer>) -> Self {
        Self {
            registry,
            authorizer,
        }
    }

    pub async fn decide(&self, input: &NormalizedInput) -> RouteDecision {
        match self
            .authorizer
            .authorize(input.surface, &input.actor, &input.raw)
        {
            AuthDecision::Deny { reason } => return RouteDecision::Deny(reason),
            AuthDecision::Allow => {}
        }

        let lowered = input.raw.trim().to_ascii_lowercase();
        if matches!(lowered.as_str(), "approve" | "yes" | "y") {
            return RouteDecision::ApprovalResponse { approved: true };
        }
        if matches!(lowered.as_str(), "deny" | "no" | "n") {
            return RouteDecision::ApprovalResponse { approved: false };
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
                if metadata.disable_model_invocation {
                    return RouteDecision::Deny(format!(
                        "command {} cannot invoke the model on this surface",
                        metadata.name
                    ));
                }
                RouteDecision::ExecuteCommand(metadata.name.to_string())
            }
            None => RouteDecision::ContinueToQueryWithPrompt(input.raw.clone()),
        }
    }

    pub async fn route(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        match self.decide(input).await {
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
            RouteDecision::ApprovalResponse { approved } => {
                app_state.resolve_pending_approval(approved).await
            }
            RouteDecision::ContinueToQuery => Ok(CommandResult::ContinueToQuery),
            RouteDecision::ContinueToQueryWithPrompt(prompt) => Ok(CommandResult::Prompt(prompt)),
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

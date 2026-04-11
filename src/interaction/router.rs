use crate::bootstrap::InteractionSurface;
use crate::command::registry::CommandRegistry;
use crate::command::types::{CommandAvailability, CommandResult, CommandType};
use crate::interaction::envelope::NormalizedInput;
use crate::security::authorizer::{AuthDecision, SurfaceAuthorizer};
use crate::state::app_state::AppState;
use crate::state::permission_context::PermissionMode;
use crate::tool::definition::ToolCall;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    ExecuteCommand(String),
    ContinueToQuery,
    ContinueToQueryWithPrompt(String),
    ApprovalResponse { approved: bool },
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
            RouteDecision::ApprovalResponse { approved } => {
                self.resolve_pending_approval(app_state, approved).await
            }
            RouteDecision::ContinueToQuery => Ok(CommandResult::ContinueToQuery),
            RouteDecision::ContinueToQueryWithPrompt(prompt) => Ok(CommandResult::Prompt(prompt)),
            RouteDecision::Deny(reason) => Ok(CommandResult::Denied(reason)),
        }
    }

    async fn resolve_pending_approval(
        &self,
        app_state: &AppState,
        approved: bool,
    ) -> anyhow::Result<CommandResult> {
        let Some(pending) = app_state.permission_context.pending_approval() else {
            return Ok(CommandResult::Denied("no pending approval in this session".into()));
        };

        if !approved {
            app_state.permission_context.set_pending_approval(None);
            return Ok(CommandResult::Message(format!(
                "Denied approval for {}",
                pending.tool_name
            )));
        }

        match pending.tool_name.as_str() {
            "EnterPlanMode" => {
                app_state.permission_context.set_mode(PermissionMode::Plan);
                app_state.permission_context.set_pending_approval(None);
                Ok(CommandResult::Message(if pending.tool_input.trim().is_empty() {
                    "entered plan mode".into()
                } else {
                    format!("entered plan mode: {}", pending.tool_input.trim())
                }))
            }
            "ExitPlanMode" => {
                app_state.permission_context.set_mode(PermissionMode::Default);
                app_state.permission_context.set_pending_approval(None);
                Ok(CommandResult::Message(if pending.tool_input.trim().is_empty() {
                    "plan approved; exited plan mode".into()
                } else {
                    format!("plan approved; exited plan mode: {}", pending.tool_input.trim())
                }))
            }
            tool_name => {
                let result = app_state
                    .permission_context
                    .inherited_tool_registry
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("tool registry unavailable for approval"))?
                    .invoke_with_approval(
                        &ToolCall::new(tool_name, pending.tool_input.clone()),
                        &app_state.permission_context,
                    )
                    .await?;
                app_state.permission_context.set_pending_approval(None);
                match result {
                    crate::tool::definition::ToolResult::Text(text) => Ok(CommandResult::Message(text)),
                    crate::tool::definition::ToolResult::Denied(reason) => Ok(CommandResult::Denied(reason)),
                    crate::tool::definition::ToolResult::PendingApproval { message, .. } => {
                        Ok(CommandResult::Message(format!("approval still required: {message}")))
                    }
                    crate::tool::definition::ToolResult::Interrupted(reason) => {
                        Ok(CommandResult::Message(format!("Interrupted: {reason}")))
                    }
                    crate::tool::definition::ToolResult::Progress(progress) => {
                        Ok(CommandResult::Message(progress))
                    }
                    crate::tool::definition::ToolResult::ResultTooLarge(reason) => {
                        Ok(CommandResult::Message(format!("Result too large: {reason}")))
                    }
                }
            }
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

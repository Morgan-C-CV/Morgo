use crate::bootstrap::InteractionSurface;
use crate::command::registry::CommandRegistry;
use crate::command::types::{CommandAvailability, CommandMetadata, CommandResult, CommandType};
use crate::core::message::Message;
use crate::interaction::envelope::NormalizedInput;
use crate::security::approval_protocol::{ApprovalResponse, parse_approval_response};
use crate::security::authorizer::{AuthDecision, SurfaceAuthorizer};
use crate::state::app_state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRoutePolicy {
    pub availability: CommandAvailability,
    pub command_type: CommandType,
    pub disable_model_invocation: bool,
    pub immediate: bool,
    pub is_sensitive: bool,
    pub enters_query_engine: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedCommand {
    pub name: String,
    pub policy: CommandRoutePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuerySource {
    PlainPrompt,
    UnknownSlashFallback { command_name: String },
    PromptCommand { command: RoutedCommand },
}

impl QuerySource {
    pub fn to_user_message(&self, input: &NormalizedInput, prompt: &str) -> Message {
        match self {
            Self::PlainPrompt | Self::UnknownSlashFallback { .. } => {
                Message::user(input.raw.clone())
            }
            Self::PromptCommand { .. } => Message::user(prompt.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownCommandPolicy {
    FallbackToPrompt,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    ExecuteCommand(RoutedCommand),
    EnterQuery { prompt: String, source: QuerySource },
    ApprovalResponse { response: ApprovalResponse },
    RejectUnknownCommand { command_name: String },
    Deny(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteExecution {
    CommandResult(CommandResult),
    EnterQuery { prompt: String, source: QuerySource },
}

use std::sync::Arc;

pub struct CommandRouter {
    registry: Arc<CommandRegistry>,
    authorizer: Box<dyn SurfaceAuthorizer>,
    unknown_command_policy: UnknownCommandPolicy,
}

impl CommandRouter {
    pub fn new(registry: Arc<CommandRegistry>, authorizer: Box<dyn SurfaceAuthorizer>) -> Self {
        Self {
            registry,
            authorizer,
            unknown_command_policy: UnknownCommandPolicy::FallbackToPrompt,
        }
    }

    pub fn with_unknown_command_policy(
        registry: Arc<CommandRegistry>,
        authorizer: Box<dyn SurfaceAuthorizer>,
        unknown_command_policy: UnknownCommandPolicy,
    ) -> Self {
        Self {
            registry,
            authorizer,
            unknown_command_policy,
        }
    }

    pub async fn decide(&self, input: &NormalizedInput) -> RouteDecision {
        match self.authorizer.authorize(input) {
            AuthDecision::Deny { reason, .. } => return RouteDecision::Deny(reason),
            AuthDecision::Allow => {}
        }

        if let Some(response) = parse_approval_response(&input.raw) {
            return RouteDecision::ApprovalResponse { response };
        }

        match input.command_name.as_deref() {
            Some(name) => self.decide_command(input, name),
            None => RouteDecision::EnterQuery {
                prompt: input.raw.clone(),
                source: QuerySource::PlainPrompt,
            },
        }
    }

    pub async fn route(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<RouteExecution> {
        match self.decide(input).await {
            RouteDecision::ExecuteCommand(routed) => {
                let command = self
                    .registry
                    .get(&routed.name)
                    .ok_or_else(|| anyhow::anyhow!("command disappeared during routing"))?;
                let result = command.execute(input, app_state).await?;
                match (routed.policy.command_type, result) {
                    (CommandType::Prompt, CommandResult::Prompt(prompt)) => {
                        if !routed.policy.enters_query_engine {
                            Ok(RouteExecution::CommandResult(CommandResult::Denied(
                                format!(
                                    "command {} cannot invoke the model on this surface",
                                    routed.name
                                ),
                            )))
                        } else {
                            Ok(RouteExecution::EnterQuery {
                                prompt,
                                source: QuerySource::PromptCommand { command: routed },
                            })
                        }
                    }
                    (_, result) => Ok(RouteExecution::CommandResult(result)),
                }
            }
            RouteDecision::ApprovalResponse { response } => Ok(RouteExecution::CommandResult(
                app_state
                    .resolve_pending_approval_response(response)
                    .await?,
            )),
            RouteDecision::EnterQuery { prompt, source } => {
                Ok(RouteExecution::EnterQuery { prompt, source })
            }
            RouteDecision::RejectUnknownCommand { command_name } => Ok(
                RouteExecution::CommandResult(CommandResult::Denied(format!(
                    "unknown command /{} rejected by strict policy",
                    command_name
                ))),
            ),
            RouteDecision::Deny(reason) => {
                Ok(RouteExecution::CommandResult(CommandResult::Denied(reason)))
            }
        }
    }

    fn decide_command(&self, input: &NormalizedInput, name: &str) -> RouteDecision {
        let Some(command) = self.registry.get(name) else {
            return match self.unknown_command_policy {
                UnknownCommandPolicy::FallbackToPrompt => RouteDecision::EnterQuery {
                    prompt: input.raw.clone(),
                    source: QuerySource::UnknownSlashFallback {
                        command_name: name.to_string(),
                    },
                },
                UnknownCommandPolicy::Reject => RouteDecision::RejectUnknownCommand {
                    command_name: name.to_string(),
                },
            };
        };
        let metadata = command.metadata();
        if !command.is_enabled() {
            return RouteDecision::Deny(format!("command {} is disabled", metadata.name));
        }
        if let Some(reason) = Self::policy_denial_reason(&metadata, input) {
            return RouteDecision::Deny(reason);
        }
        let policy = Self::policy_from_metadata(&metadata);
        RouteDecision::ExecuteCommand(RoutedCommand {
            name: metadata.name,
            policy,
        })
    }

    fn policy_from_metadata(metadata: &CommandMetadata) -> CommandRoutePolicy {
        let enters_query_engine = matches!(metadata.command_type, CommandType::Prompt)
            && !metadata.disable_model_invocation;
        CommandRoutePolicy {
            availability: metadata.availability,
            command_type: metadata.command_type,
            disable_model_invocation: metadata.disable_model_invocation,
            immediate: metadata.immediate && !enters_query_engine,
            is_sensitive: metadata.is_sensitive,
            enters_query_engine,
        }
    }

    fn policy_denial_reason(metadata: &CommandMetadata, input: &NormalizedInput) -> Option<String> {
        if !Self::is_available(metadata.availability, input.surface) {
            return Some(format!(
                "command {} is not available on this surface",
                metadata.name
            ));
        }
        if matches!(input.surface, InteractionSurface::Remote)
            && (!input.metadata.from_trusted_surface || metadata.is_sensitive)
        {
            return Some(format!(
                "command {} is not allowed on remote surface",
                metadata.name
            ));
        }
        None
    }

    fn is_available(availability: CommandAvailability, surface: InteractionSurface) -> bool {
        match availability {
            CommandAvailability::Everywhere => true,
            CommandAvailability::CliOnly => matches!(surface, InteractionSurface::Cli),
            CommandAvailability::RemoteSafe => !matches!(surface, InteractionSurface::Cli),
        }
    }
}

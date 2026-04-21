use std::sync::Arc;

use tokio::sync::RwLock;

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::compact::CompactCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::message::Message;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::router::{
    CommandRouter, QuerySource, RouteDecision, RouteExecution, RoutedCommand, UnknownCommandPolicy,
};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::security::authorizer::DefaultSurfaceAuthorizer;
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;

struct PromptCommand;

#[async_trait]
impl Command for PromptCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "prompt-cmd".into(),
            description: "Prompt command for acceptance routing".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Prompt,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Prompt("expanded prompt body".into()))
    }
}

fn test_app_state() -> AppState {
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default())),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_session_id: "acceptance-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
    }
}

fn test_engine() -> QueryEngine {
    let app_state = test_app_state();
    let tool_registry = ToolRegistry::new();
    QueryEngine::new(QueryContext {
        system_prompt: rust_agent::prompt::system::build_system_prompt(&app_state),
        tools_prompt: rust_agent::prompt::tools::build_tools_prompt(
            &tool_registry,
            &app_state.permission_context,
        ),
        context_prompt: rust_agent::prompt::context::build_context_prompt(&app_state),
        app_state,
        tool_registry,
        api_client: ModelProviderClient::with_scripted_turns(Vec::new()),
        compactor: ReactiveCompactor,
        hook_registry: rust_agent::hook::registry::HookRegistry::default(),
        agent_id: None,
    })
}

#[tokio::test]
async fn unknown_slash_command_still_falls_back_to_query() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/unknown foo");
    assert_eq!(
        router.decide(&input).await,
        RouteDecision::EnterQuery {
            prompt: "/unknown foo".into(),
            source: QuerySource::UnknownSlashFallback {
                command_name: "unknown".into(),
            },
        }
    );
}

#[tokio::test]
async fn plain_user_input_routes_through_query_prompt_path() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "hello world");
    assert_eq!(
        router.decide(&input).await,
        RouteDecision::EnterQuery {
            prompt: "hello world".into(),
            source: QuerySource::PlainPrompt,
        }
    );
}

#[tokio::test]
async fn strict_unknown_slash_command_does_not_enter_query_path() {
    let router = CommandRouter::with_unknown_command_policy(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
        UnknownCommandPolicy::Reject,
    );
    let app_state = test_app_state();
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/unknown foo");

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::RejectUnknownCommand {
            command_name: "unknown".into(),
        }
    );
    assert_eq!(
        router
            .route(&input, &app_state)
            .await
            .expect("route should succeed"),
        RouteExecution::CommandResult(CommandResult::Denied(
            "unknown command /unknown rejected by strict policy".into()
        ))
    );
}

#[tokio::test]
async fn prompt_command_is_interpreted_before_query_engine() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PromptCommand))),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let app_state = test_app_state();
    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/prompt-cmd"),
            &app_state,
        )
        .await
        .expect("route should succeed");
    assert_eq!(
        result,
        RouteExecution::EnterQuery {
            prompt: "expanded prompt body".into(),
            source: QuerySource::PromptCommand {
                command: RoutedCommand {
                    name: "prompt-cmd".into(),
                    policy: rust_agent::interaction::router::CommandRoutePolicy {
                        availability: CommandAvailability::Everywhere,
                        command_type: CommandType::Prompt,
                        disable_model_invocation: false,
                        immediate: false,
                        is_sensitive: false,
                        enters_query_engine: true,
                    },
                },
            },
        }
    );
}

#[tokio::test]
async fn compact_builtin_is_interpreted_before_query_engine() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(CompactCommand))),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let app_state = test_app_state();
    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/compact"),
            &app_state,
        )
        .await
        .expect("route should succeed");
    assert_eq!(
        result,
        RouteExecution::EnterQuery {
            prompt: "Please compact the current conversation while preserving relevant context."
                .into(),
            source: QuerySource::PromptCommand {
                command: RoutedCommand {
                    name: "compact".into(),
                    policy: rust_agent::interaction::router::CommandRoutePolicy {
                        availability: CommandAvailability::Everywhere,
                        command_type: CommandType::Prompt,
                        disable_model_invocation: false,
                        immediate: false,
                        is_sensitive: false,
                        enters_query_engine: true,
                    },
                },
            },
        }
    );
}

#[test]
fn query_context_builds_non_empty_prompt_layers() {
    let engine = test_engine();
    assert!(engine.context.system_prompt.contains("surface=Cli"));
    assert!(
        engine
            .context
            .context_prompt
            .contains("Runtime context summary:")
    );
    assert!(engine.context.context_prompt.contains("- client_type: Cli"));
}

#[test]
fn query_source_to_user_message_is_shared_for_unknown_and_prompt_commands() {
    let unknown_input = NormalizedInput::from_raw(InteractionSurface::Cli, "/unknown foo");
    let unknown_source = QuerySource::UnknownSlashFallback {
        command_name: "unknown".into(),
    };
    assert_eq!(
        unknown_source.to_user_message(&unknown_input, "/ignored"),
        Message::user("/unknown foo")
    );

    let prompt_input = NormalizedInput::from_raw(InteractionSurface::Cli, "/prompt-cmd");
    let prompt_source = QuerySource::PromptCommand {
        command: RoutedCommand {
            name: "prompt-cmd".into(),
            policy: rust_agent::interaction::router::CommandRoutePolicy {
                availability: CommandAvailability::Everywhere,
                command_type: CommandType::Prompt,
                disable_model_invocation: false,
                immediate: false,
                is_sensitive: false,
                enters_query_engine: true,
            },
        },
    };
    assert_eq!(
        prompt_source.to_user_message(&prompt_input, "expanded prompt body"),
        Message::user("expanded prompt body")
    );
}

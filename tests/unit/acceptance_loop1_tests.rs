use std::sync::Arc;

use tokio::sync::RwLock;

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{Command, CommandAvailability, CommandMetadata, CommandResult, CommandType};
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::router::{CommandRouter, RouteDecision};
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
            name: "prompt-cmd",
            description: "Prompt command for acceptance routing",
            command_type: CommandType::Prompt,
            availability: CommandAvailability::Everywhere,
            aliases: &[],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
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
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default())),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: Vec::new(),
        active_session_id: "acceptance-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    }
}

fn test_engine() -> QueryEngine {
    let app_state = test_app_state();
    let tool_registry = ToolRegistry::new();
    QueryEngine::new(QueryContext {
        system_prompt: rust_agent::prompt::system::build_system_prompt(&app_state),
        tools_prompt: rust_agent::prompt::tools::build_tools_prompt(&tool_registry, &app_state.permission_context),
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
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/unknown foo");
    assert_eq!(router.decide(&input).await, RouteDecision::ContinueToQuery);
}

#[tokio::test]
async fn plain_user_input_routes_through_query_prompt_path() {
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "hello world");
    assert_eq!(
        router.decide(&input).await,
        RouteDecision::ContinueToQueryWithPrompt("hello world".into())
    );
}

#[tokio::test]
async fn prompt_command_is_interpreted_before_query_engine() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PromptCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let app_state = test_app_state();
    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/prompt-cmd"),
            &app_state,
        )
        .await
        .expect("route should succeed");
    assert_eq!(result, CommandResult::Prompt("expanded prompt body".into()));
}

#[test]
fn query_context_builds_non_empty_prompt_layers() {
    let engine = test_engine();
    assert!(engine.context.system_prompt.contains("surface=Cli"));
    assert!(engine.context.context_prompt.contains("client_type=Cli"));
}

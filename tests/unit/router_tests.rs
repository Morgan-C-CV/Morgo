use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::cli::repl::handle_cli_inputs;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::router::{CommandRouter, RouteDecision};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::security::authorizer::DefaultSurfaceAuthorizer;
use rust_agent::service::api::client::AnthropicClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;

#[test]
fn router_executes_known_commands_before_query() {
    let registry = CommandRegistry::new().register(Arc::new(HelpCommand));
    let router = CommandRouter::new(registry, Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/help");

    assert_eq!(
        router.decide(&input),
        RouteDecision::ExecuteCommand("help".into())
    );
}

#[test]
fn router_falls_back_for_unknown_commands() {
    let router = CommandRouter::new(CommandRegistry::new(), Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/missing foo");

    assert_eq!(router.decide(&input), RouteDecision::ContinueToQuery);
}

#[test]
fn router_denies_unauthenticated_remote_actor() {
    let router = CommandRouter::new(CommandRegistry::new(), Box::new(DefaultSurfaceAuthorizer));
    let mut input = NormalizedInput::from_raw(InteractionSurface::Remote, "/help");
    input.actor.is_authenticated = false;

    assert_eq!(
        router.decide(&input),
        RouteDecision::Deny("unauthenticated actor for remote surface".into())
    );
}

#[tokio::test]
async fn cli_repl_handles_multiple_inputs_in_sequence() {
    let router = CommandRouter::new(
        CommandRegistry::new().register(Arc::new(HelpCommand)),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session: None,
        history: None,
        restored_session: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: AnthropicClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("second reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
        });

    let outputs = handle_cli_inputs(&router, &engine, &app_state, vec!["/help", "hello"])
        .await
        .expect("cli repl should handle sequential inputs");

    assert_eq!(outputs.len(), 2);
    assert!(outputs[0].primary_text.contains("help"));
    assert!(outputs[1].primary_text.contains("second reply"));
    assert!(outputs[0].events.is_empty());
    assert!(outputs[1].events.is_empty());
}

#[tokio::test]
async fn cli_repl_surfaces_task_events_for_active_session() {
    let router = CommandRouter::new(
        CommandRegistry::new().register(Arc::new(HelpCommand)),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let manager = Arc::new(TaskManager::default());
    let permission_context =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session: None,
        history: None,
        restored_session: None,
    };
    let task = manager.create("queued task");
    manager.complete(
        &task.id,
        &app_state.active_session_id,
        &app_state.notification_dispatcher,
    );
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: AnthropicClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
        });

    let output = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("cli repl should surface notifications");

    assert_eq!(output.len(), 1);
    assert!(output[0].primary_text.contains("Available commands"));
    assert_eq!(output[0].events.len(), 1);
    let rust_agent::interaction::cli::repl::CliDisplayEvent::TaskEvent(task_event) =
        &output[0].events[0];
    assert_eq!(task_event.task_id, "task-0");
    assert_eq!(task_event.owner_session_id, "cli-session");
}

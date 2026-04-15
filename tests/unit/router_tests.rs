use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::builtin::permissions::PermissionsCommand;
use rust_agent::command::builtin::plan::PlanCommand;
use rust_agent::command::builtin::plugins::PluginsCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{InMemorySessionStore, SessionRestoreRequest, SessionStore};
use rust_agent::interaction::cli::repl::handle_cli_inputs;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::remote::{RemoteRequest, handle_remote_request};
use rust_agent::interaction::router::{
    CommandRoutePolicy, CommandRouter, QuerySource, RouteDecision, RouteExecution, RoutedCommand,
};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::plugins::runtime_state::{
    RuntimePluginState, build_runtime_plugin_snapshot, build_turn_engine, build_turn_router,
    hydrate_app_state_from_snapshot, rebuild_runtime_plugin_state,
};
use rust_agent::security::authorizer::{AuthDecision, DefaultSurfaceAuthorizer, SurfaceAuthorizer};
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::list_manager::TaskListManager;
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskOwner, ValidationState, WorkerPhase};
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

struct DenyingAuthorizer;

impl SurfaceAuthorizer for DenyingAuthorizer {
    fn authorize(
        &self,
        _surface: InteractionSurface,
        _actor: &rust_agent::interaction::envelope::ActorIdentity,
        _raw_input: &str,
    ) -> AuthDecision {
        AuthDecision::Deny {
            reason: "blocked by authorizer".into(),
        }
    }
}

struct RemoteSafeTestCommand;
struct SensitiveRemoteCommand;
struct SensitiveEverywhereCommand;
struct CliOnlyTestCommand;
struct PromptImmediateMetadataCommand;

#[async_trait]
impl Command for RemoteSafeTestCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "remote-safe".into(),
            description: "Test remote-safe command".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::RemoteSafe,
            aliases: Vec::new(),
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
        Ok(CommandResult::Message("remote safe response".into()))
    }
}

struct PromptNoModelCommand;

#[async_trait]
impl Command for SensitiveRemoteCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "remote-sensitive".into(),
            description: "Sensitive test remote command".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::RemoteSafe,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: true,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message("sensitive remote response".into()))
    }
}

#[async_trait]
impl Command for PromptNoModelCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "prompt-no-model".into(),
            description: "Prompt command with model invocation disabled".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Prompt,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: true,
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

#[async_trait]
impl Command for SensitiveEverywhereCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "sensitive-everywhere".into(),
            description: "Sensitive command available on all surfaces".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: true,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        Ok(CommandResult::Message(
            "sensitive everywhere response".into(),
        ))
    }
}

#[async_trait]
impl Command for CliOnlyTestCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "cli-only".into(),
            description: "CLI-only test command".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::CliOnly,
            aliases: Vec::new(),
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
        Ok(CommandResult::Message("cli only response".into()))
    }
}

#[async_trait]
impl Command for PromptImmediateMetadataCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "prompt-immediate".into(),
            description: "Prompt command with immediate metadata enabled".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Prompt,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
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
        Ok(CommandResult::Prompt("prompt immediate body".into()))
    }
}

#[tokio::test]
async fn router_executes_known_commands_before_query() {
    let registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let router = CommandRouter::new(registry, Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/help");

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::ExecuteCommand(RoutedCommand {
            name: "help".into(),
            policy: CommandRoutePolicy {
                availability: CommandAvailability::Everywhere,
                command_type: CommandType::Local,
                disable_model_invocation: false,
                immediate: true,
                is_sensitive: false,
                enters_query_engine: false,
            },
        })
    );
}

#[tokio::test]
async fn router_falls_back_for_unknown_commands() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/missing foo");

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::EnterQuery {
            prompt: "/missing foo".into(),
            source: QuerySource::UnknownSlashFallback {
                command_name: "missing".into(),
            },
        }
    );
}

#[tokio::test]
async fn router_unknown_slash_fallback_is_shared_by_cli_remote_and_telegram() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let cli = NormalizedInput::from_raw(InteractionSurface::Cli, "/missing foo");
    let telegram = NormalizedInput::from_raw(InteractionSurface::Telegram, "/missing foo");
    let remote = NormalizedInput::from_remote_raw(
        "remote-session",
        "remote-actor",
        true,
        true,
        "/missing foo",
    );

    let cli_decision = router.decide(&cli).await;
    assert_eq!(cli_decision, router.decide(&telegram).await);
    assert_eq!(cli_decision, router.decide(&remote).await);
}

#[tokio::test]
async fn router_denies_unauthenticated_remote_actor() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let input =
        NormalizedInput::from_remote_raw("remote-session", "remote-actor", false, true, "/help");

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::Deny("unauthenticated actor for remote surface".into())
    );
}

#[tokio::test]
async fn router_denies_untrusted_remote_command() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(RemoteSafeTestCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let input = NormalizedInput::from_remote_raw(
        "remote-session",
        "remote-actor",
        true,
        false,
        "/remote-safe",
    );

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::Deny("command remote-safe is not allowed on remote surface".into())
    );
}

#[tokio::test]
async fn router_denies_sensitive_remote_command() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(SensitiveRemoteCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let input = NormalizedInput::from_remote_raw(
        "remote-session",
        "remote-actor",
        true,
        true,
        "/remote-sensitive",
    );

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::Deny("command remote-sensitive is not allowed on remote surface".into())
    );
}

#[tokio::test]
async fn router_execute_command_decision_is_shared_by_cli_remote_and_telegram() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let cli = NormalizedInput::from_raw(InteractionSurface::Cli, "/help");
    let telegram = NormalizedInput::from_raw(InteractionSurface::Telegram, "/help");
    let remote =
        NormalizedInput::from_remote_raw("remote-session", "remote-actor", true, true, "/help");

    let cli_decision = router.decide(&cli).await;
    assert_eq!(cli_decision, router.decide(&telegram).await);
    assert_eq!(cli_decision, router.decide(&remote).await);
}

#[tokio::test]
async fn router_plain_prompt_decision_is_shared_by_cli_remote_and_telegram() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let cli = NormalizedInput::from_raw(InteractionSurface::Cli, "hello world");
    let telegram = NormalizedInput::from_raw(InteractionSurface::Telegram, "hello world");
    let remote = NormalizedInput::from_remote_raw(
        "remote-session",
        "remote-actor",
        true,
        true,
        "hello world",
    );

    let cli_decision = router.decide(&cli).await;
    assert_eq!(cli_decision, router.decide(&telegram).await);
    assert_eq!(cli_decision, router.decide(&remote).await);
}

#[tokio::test]
async fn router_availability_policy_is_shared_by_cli_and_telegram_but_denies_remote() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(CliOnlyTestCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let cli = NormalizedInput::from_raw(InteractionSurface::Cli, "/cli-only");
    let telegram = NormalizedInput::from_raw(InteractionSurface::Telegram, "/cli-only");
    let remote =
        NormalizedInput::from_remote_raw("remote-session", "remote-actor", true, true, "/cli-only");

    assert_eq!(
        router.decide(&cli).await,
        RouteDecision::ExecuteCommand(RoutedCommand {
            name: "cli-only".into(),
            policy: CommandRoutePolicy {
                availability: CommandAvailability::CliOnly,
                command_type: CommandType::Local,
                disable_model_invocation: false,
                immediate: true,
                is_sensitive: false,
                enters_query_engine: false,
            },
        })
    );
    assert_eq!(
        router.decide(&telegram).await,
        RouteDecision::Deny("command cli-only is not available on this surface".into())
    );
    assert_eq!(
        router.decide(&remote).await,
        RouteDecision::Deny("command cli-only is not available on this surface".into())
    );
}

#[tokio::test]
async fn router_sensitive_command_policy_is_carried_in_allowed_surface_decision() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(SensitiveEverywhereCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let cli = NormalizedInput::from_raw(InteractionSurface::Cli, "/sensitive-everywhere");
    let telegram = NormalizedInput::from_raw(InteractionSurface::Telegram, "/sensitive-everywhere");

    let allowed = RouteDecision::ExecuteCommand(RoutedCommand {
        name: "sensitive-everywhere".into(),
        policy: CommandRoutePolicy {
            availability: CommandAvailability::Everywhere,
            command_type: CommandType::Local,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: true,
            enters_query_engine: false,
        },
    });
    assert_eq!(router.decide(&cli).await, allowed);
    assert_eq!(router.decide(&telegram).await, allowed);
}

#[tokio::test]
async fn router_normalizes_prompt_commands_away_from_immediate_execution() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PromptImmediateMetadataCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/prompt-immediate");

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::ExecuteCommand(RoutedCommand {
            name: "prompt-immediate".into(),
            policy: CommandRoutePolicy {
                availability: CommandAvailability::Everywhere,
                command_type: CommandType::Prompt,
                disable_model_invocation: false,
                immediate: false,
                is_sensitive: false,
                enters_query_engine: true,
            },
        })
    );
}

#[tokio::test]
async fn prompt_command_with_model_invocation_disabled_never_enters_query_engine() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PromptNoModelCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "test-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/prompt-no-model"),
            &app_state,
        )
        .await
        .expect("route should succeed");

    assert_eq!(
        result,
        RouteExecution::CommandResult(CommandResult::Denied(
            "command prompt-no-model cannot invoke the model on this surface".into()
        ))
    );
}

#[tokio::test]
async fn cli_repl_handles_multiple_inputs_in_sequence() {
    let command_registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let router = CommandRouter::new(command_registry.clone(), Box::new(DefaultSurfaceAuthorizer));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("second reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let outputs = handle_cli_inputs(&router, &engine, &app_state, vec!["/help", "hello"])
        .await
        .expect("cli repl should handle sequential inputs");

    assert_eq!(outputs.len(), 2);
    assert!(outputs[0].primary_text.contains("Available commands"));
    assert!(outputs[1].primary_text.contains("second reply"));
    assert!(outputs[0].events.is_empty());
    assert!(!outputs[1].events.is_empty());
}

#[tokio::test]
async fn cli_repl_surfaces_task_events_for_active_session() {
    let command_registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let router = CommandRouter::new(command_registry.clone(), Box::new(DefaultSurfaceAuthorizer));
    let manager = Arc::new(TaskManager::default());
    let permission_context =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let task = manager.create(
        "queued task",
        app_state.active_session_id.clone(),
        InteractionSurface::Cli,
    );
    manager.complete(&task.id, &app_state.notification_dispatcher);
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let output = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("cli repl should surface notifications");

    assert_eq!(output.len(), 1);
    assert!(output[0].primary_text.contains("Available commands"));
    assert_eq!(output[0].events.len(), 1);
    let rust_agent::interaction::cli::repl::CliDisplayEvent::TaskEvent(task_event) =
        &output[0].events[0]
    else {
        panic!("expected task event");
    };
    assert_eq!(task_event.task_id, "task-0");
    assert_eq!(
        task_event.owner,
        TaskOwner {
            session_id: "cli-session".into(),
            surface: InteractionSurface::Cli,
        }
    );
}

#[tokio::test]
async fn cli_repl_persists_history_for_local_and_query_turns() {
    let command_registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let router = CommandRouter::new(command_registry.clone(), Box::new(DefaultSurfaceAuthorizer));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("cli-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/cli-history".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        rust_agent::history::session::SessionHistory::default(),
    );
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: Some(session_store.clone()),
        session: None,
        history: None,
        restored_session: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("second reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let outputs = handle_cli_inputs(&router, &engine, &app_state, vec!["/help", "hello"])
        .await
        .expect("cli repl should persist history");

    assert_eq!(outputs.len(), 2);
    let (_, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("cli-session".into()),
            continue_session: false,
        })
        .expect("expected persisted history");
    assert_eq!(history.entries.len(), 4);
    assert_eq!(
        history.entries[0].message,
        rust_agent::core::message::Message::user("/help")
    );
    assert!(
        history.entries[1]
            .message
            .content
            .contains("Available commands")
    );
    assert_eq!(
        history.entries[2].message,
        rust_agent::core::message::Message::user("hello")
    );
    assert_eq!(
        history.entries[3].message,
        rust_agent::core::message::Message::assistant("second reply")
    );
}

#[tokio::test]
async fn remote_handler_preserves_remote_actor_and_session_for_query_flow() {
    let command_registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(RemoteSafeTestCommand))
            .register(Arc::new(PluginsCommand)),
    );
    let router = CommandRouter::new(command_registry.clone(), Box::new(DefaultSurfaceAuthorizer));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-handler".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        rust_agent::history::session::SessionHistory::default(),
    );
    let mut app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "remote-session".into(),
        session_store: Some(session_store.clone()),
        session: None,
        history: None,
        restored_session: None,
    };
    let runtime_plugin_state = RuntimePluginState::new(build_runtime_plugin_snapshot(&app_state));
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state);
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("remote query reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "bound-remote-session".into(),
            actor_id: "actor-42".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "hello from remote".into(),
        },
    )
    .await
    .expect("remote handler should succeed");

    assert!(response.primary_text.contains("remote query reply"));

    let (_, default_history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-session".into()),
            continue_session: false,
        })
        .expect("default remote session should still exist");
    assert!(default_history.entries.is_empty());

    let (_, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("bound-remote-session".into()),
            continue_session: false,
        })
        .expect("expected bound remote query history");
    assert_eq!(history.entries.len(), 2);
    assert_eq!(
        history.entries[0].message,
        rust_agent::core::message::Message::user("hello from remote")
    );
    assert_eq!(
        history.entries[1].message,
        rust_agent::core::message::Message::assistant("remote query reply")
    );

    let normalized = NormalizedInput::from_remote_raw(
        "bound-remote-session",
        "actor-42",
        true,
        true,
        "/remote-safe",
    );
    assert_eq!(normalized.session_id, "bound-remote-session");
    assert_eq!(normalized.actor.actor_id, "actor-42");
    assert!(normalized.actor.is_authenticated);
    assert!(normalized.metadata.from_trusted_surface);
}

#[tokio::test]
async fn cli_repl_uses_next_turn_plugin_snapshot_after_reload_updates_manifest_surface() {
    let root = unique_temp_path("rust-agent-cli-plugin-reload-update");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    let manifest_path = plugin_dir.join("plugin.json");
    fs::write(
        &manifest_path,
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands"],
  "commands": [
    {
      "name": "demo-plugin-cmd",
      "description": "Demo plugin command",
      "prompt": "Do plugin command work"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let command_registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PluginsCommand)),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let mut app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry.clone()),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: Some(rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("cli-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: root.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        }),
        history: None,
        restored_session: None,
    };

    let initial_snapshot = build_runtime_plugin_snapshot(&app_state);
    let runtime_plugin_state = RuntimePluginState::new(initial_snapshot.clone());
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state.clone());
    hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);

    let router = build_turn_router(&initial_snapshot);
    let base_engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: initial_snapshot.hook_registry.clone(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });
    let engine = build_turn_engine(&app_state, &initial_snapshot, &base_engine);

    let first = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("first turn should succeed");
    assert!(
        first[0]
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );
    assert!(
        !first[0]
            .primary_text
            .contains("/demo-plugin-cmd-v2 — Updated plugin command")
    );

    fs::write(
        &manifest_path,
        r#"{
  "name": "demo-plugin",
  "version": "0.1.1",
  "description": "Demo plugin",
  "capabilities": ["commands"],
  "commands": [
    {
      "name": "demo-plugin-cmd-v2",
      "description": "Updated plugin command",
      "prompt": "Do updated plugin command work"
    }
  ]
}"#,
    )
    .expect("updated plugin manifest should be written");
    let report = rebuild_runtime_plugin_state(&app_state)
        .await
        .expect("reload should succeed after manifest update");
    assert_eq!(report.outcome.as_str(), "applied");
    assert_eq!(report.generation, 1);

    let second = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("second turn should succeed");
    assert!(
        !second[0]
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );
    assert!(
        second[0]
            .primary_text
            .contains("/demo-plugin-cmd-v2 — Updated plugin command")
    );

    fs::remove_dir_all(root).expect("cleanup plugin reload update root");
}

#[tokio::test]
async fn cli_repl_uses_next_turn_plugin_snapshot_after_reload_removes_deleted_plugin() {
    let root = unique_temp_path("rust-agent-cli-plugin-reload-removal");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands"],
  "commands": [
    {
      "name": "demo-plugin-cmd",
      "description": "Demo plugin command",
      "prompt": "Do plugin command work"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let command_registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PluginsCommand)),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let mut app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry.clone()),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: Some(rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("cli-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: root.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        }),
        history: None,
        restored_session: None,
    };

    let initial_snapshot = build_runtime_plugin_snapshot(&app_state);
    let runtime_plugin_state = RuntimePluginState::new(initial_snapshot.clone());
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state.clone());
    hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);

    let router = build_turn_router(&initial_snapshot);
    let base_engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: initial_snapshot.hook_registry.clone(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });
    let engine = build_turn_engine(&app_state, &initial_snapshot, &base_engine);

    let first = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("first turn should succeed");
    assert!(
        first[0]
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );

    fs::remove_dir_all(&plugin_dir).expect("plugin dir should be removed");
    let report = rebuild_runtime_plugin_state(&app_state)
        .await
        .expect("reload should succeed after plugin deletion");
    assert_eq!(report.outcome.as_str(), "applied");
    assert_eq!(report.generation, 1);

    let second = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("second turn should succeed");
    assert!(second[0].primary_text.contains("Available commands"));
    assert!(
        !second[0]
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );

    fs::remove_dir_all(root).expect("cleanup plugin reload root");
}

#[tokio::test]
async fn cli_repl_applies_disable_and_enable_only_on_next_turn_boundaries() {
    let root = unique_temp_path("rust-agent-cli-plugin-visibility-matrix");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands"],
  "commands": [
    {
      "name": "demo-plugin-cmd",
      "description": "Demo plugin command",
      "prompt": "Do plugin command work"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let command_registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PluginsCommand)),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let mut app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry.clone()),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: Some(rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("cli-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: root.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        }),
        history: None,
        restored_session: None,
    };

    let initial_snapshot = build_runtime_plugin_snapshot(&app_state);
    let runtime_plugin_state = RuntimePluginState::new(initial_snapshot.clone());
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state.clone());
    hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);

    let router = build_turn_router(&initial_snapshot);
    let base_engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: initial_snapshot.hook_registry.clone(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });
    let engine = build_turn_engine(&app_state, &initial_snapshot, &base_engine);

    assert_eq!(
        router
            .decide(&NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/demo-plugin-cmd"
            ))
            .await,
        RouteDecision::ExecuteCommand(RoutedCommand {
            name: "demo-plugin-cmd".into(),
            policy: CommandRoutePolicy {
                availability: CommandAvailability::Everywhere,
                command_type: CommandType::Prompt,
                disable_model_invocation: false,
                immediate: false,
                is_sensitive: false,
                enters_query_engine: true,
            },
        })
    );

    let disable_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins disable demo-plugin"),
            &app_state,
        )
        .await
        .expect("disable should succeed");
    let CommandResult::Message(disable_text) = disable_result else {
        panic!("expected disable message");
    };
    assert!(disable_text.contains("Disabled plugin demo-plugin."));

    assert_eq!(
        router
            .decide(&NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/demo-plugin-cmd"
            ))
            .await,
        RouteDecision::ExecuteCommand(RoutedCommand {
            name: "demo-plugin-cmd".into(),
            policy: CommandRoutePolicy {
                availability: CommandAvailability::Everywhere,
                command_type: CommandType::Prompt,
                disable_model_invocation: false,
                immediate: false,
                is_sensitive: false,
                enters_query_engine: true,
            },
        })
    );

    let after_disable = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("help after disable should succeed");
    assert!(
        !after_disable[0]
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );

    let disabled_snapshot = runtime_plugin_state.snapshot().await;
    let disabled_router = build_turn_router(&disabled_snapshot);
    assert_eq!(
        disabled_router
            .decide(&NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/demo-plugin-cmd"
            ))
            .await,
        RouteDecision::EnterQuery {
            prompt: "/demo-plugin-cmd".into(),
            source: QuerySource::UnknownSlashFallback {
                command_name: "demo-plugin-cmd".into(),
            },
        }
    );

    let enable_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins enable demo-plugin"),
            &app_state,
        )
        .await
        .expect("enable should succeed");
    let CommandResult::Message(enable_text) = enable_result else {
        panic!("expected enable message");
    };
    assert!(enable_text.contains("Enabled plugin demo-plugin."));

    assert_eq!(
        disabled_router
            .decide(&NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/demo-plugin-cmd"
            ))
            .await,
        RouteDecision::EnterQuery {
            prompt: "/demo-plugin-cmd".into(),
            source: QuerySource::UnknownSlashFallback {
                command_name: "demo-plugin-cmd".into(),
            },
        }
    );

    let after_enable = handle_cli_inputs(&router, &engine, &app_state, vec!["/help"])
        .await
        .expect("help after enable should succeed");
    assert!(
        after_enable[0]
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );

    fs::remove_dir_all(root).expect("cleanup visibility matrix root");
}

#[tokio::test]
async fn remote_handler_uses_next_turn_plugin_snapshot_after_reload_removes_deleted_plugin() {
    let root = unique_temp_path("rust-agent-remote-plugin-reload-removal");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands"],
  "commands": [
    {
      "name": "demo-plugin-cmd",
      "description": "Demo plugin command",
      "prompt": "Do plugin command work"
    }
  ]
}"#,
    )
    .expect("plugin manifest should be written");

    let command_registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PluginsCommand)),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: root.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        },
        rust_agent::history::session::SessionHistory::default(),
    );
    let mut app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry.clone()),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "remote-session".into(),
        session_store: Some(session_store),
        session: Some(rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: root.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        }),
        history: None,
        restored_session: None,
    };

    let initial_snapshot = build_runtime_plugin_snapshot(&app_state);
    let runtime_plugin_state = RuntimePluginState::new(initial_snapshot.clone());
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state.clone());
    hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);

    let router = build_turn_router(&initial_snapshot);
    let base_engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: initial_snapshot.hook_registry.clone(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });
    let engine = build_turn_engine(&app_state, &initial_snapshot, &base_engine);

    let first = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-session".into(),
            actor_id: "actor-42".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "/help".into(),
        },
    )
    .await
    .expect("first remote turn should succeed");
    assert!(
        first
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );

    fs::remove_dir_all(&plugin_dir).expect("plugin dir should be removed");
    let report = rebuild_runtime_plugin_state(&app_state)
        .await
        .expect("reload should succeed after plugin deletion");
    assert_eq!(report.outcome.as_str(), "applied");
    assert_eq!(report.generation, 1);

    let second = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-session".into(),
            actor_id: "actor-42".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "/help".into(),
        },
    )
    .await
    .expect("second remote turn should succeed");
    assert!(second.primary_text.contains("Available commands"));
    assert!(
        !second
            .primary_text
            .contains("/demo-plugin-cmd — Demo plugin command")
    );

    fs::remove_dir_all(root).expect("cleanup remote plugin reload root");
}

#[tokio::test]
async fn cli_repl_persists_denied_turns() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(RemoteSafeTestCommand))),
        Box::new(DenyingAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        rust_agent::history::session::SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-history".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        rust_agent::history::session::SessionHistory::default(),
    );
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "remote-session".into(),
        session_store: Some(session_store.clone()),
        session: None,
        history: None,
        restored_session: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::default(),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let output = handle_cli_inputs(&router, &engine, &app_state, vec!["/remote-safe"])
        .await
        .expect("denied turn should still produce output");

    assert_eq!(output.len(), 1);
    assert!(output[0].primary_text.contains("Denied:"));
    let (_, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-session".into()),
            continue_session: false,
        })
        .expect("expected denied turn history");
    assert_eq!(history.entries.len(), 2);
    assert_eq!(
        history.entries[0].message,
        rust_agent::core::message::Message::user("/remote-safe")
    );
    assert!(history.entries[1].message.content.contains("Denied:"));
}

#[tokio::test]
async fn router_approves_pending_plan_mode_request() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_pending_approval(rust_agent::state::permission_context::PendingApproval {
            tool_name: "EnterPlanMode".into(),
            tool_input: "draft feature work".into(),
            message: "approve entering plan mode: draft feature work".into(),
        });
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_session_raw(InteractionSurface::Cli, "cli-session", "approve"),
            &app_state,
        )
        .await
        .expect("approval should resolve");

    assert_eq!(
        result,
        RouteExecution::CommandResult(CommandResult::Message(
            "entered plan mode: draft feature work".into()
        ))
    );
    assert_eq!(permission_context.mode(), PermissionMode::Plan);
    assert!(permission_context.pending_approval().is_none());
}

#[tokio::test]
async fn router_denies_pending_request_without_session_approval() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_pending_approval(rust_agent::state::permission_context::PendingApproval {
            tool_name: "Bash".into(),
            tool_input: serde_json::json!({"command": "sudo whoami"}).to_string(),
            message: "command touches privileged system state".into(),
        });
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_session_raw(InteractionSurface::Cli, "cli-session", "deny"),
            &app_state,
        )
        .await
        .expect("denial should resolve");

    assert_eq!(
        result,
        RouteExecution::CommandResult(CommandResult::Message("Denied approval for Bash".into()))
    );
    assert_eq!(permission_context.mode(), PermissionMode::Default);
    assert!(permission_context.pending_approval().is_none());
}

#[tokio::test]
async fn approval_replay_uses_runtime_tool_registry() {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("cwd lock poisoned");
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_pending_approval(rust_agent::state::permission_context::PendingApproval {
            tool_name: "Bash".into(),
            tool_input: serde_json::json!({"command": "pwd"}).to_string(),
            message: "approve pwd".into(),
        });
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(
            ToolRegistry::new().register(Arc::new(rust_agent::tool::builtin::bash::BashTool)),
        ))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_session_raw(InteractionSurface::Cli, "cli-session", "approve"),
            &app_state,
        )
        .await
        .expect("approval replay should resolve");

    let RouteExecution::CommandResult(CommandResult::Message(text)) = result else {
        panic!("expected approval replay message");
    };
    assert!(text.contains("command: pwd"));
    assert!(text.contains("exit_code: 0"));
    assert!(permission_context.pending_approval().is_none());
}

#[tokio::test]
async fn permissions_command_reports_session_permission_state() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PermissionsCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Plan)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_pending_approval(rust_agent::state::permission_context::PendingApproval {
            tool_name: "Bash".into(),
            tool_input: serde_json::json!({"command": "pwd"}).to_string(),
            message: "approve pwd".into(),
        });
    permission_context.add_always_allow_rule("Read");
    permission_context.add_always_deny_rule("Bash");
    permission_context.add_always_ask_rule("WebFetch");
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/permissions"),
            &app_state,
        )
        .await
        .expect("permissions summary should render");

    let RouteExecution::CommandResult(CommandResult::Message(text)) = result else {
        panic!("expected permissions summary message");
    };
    assert!(text.contains("Permission mode: plan"));
    assert!(text.contains("Allow rules: Read"));
    assert!(text.contains("Deny rules: Bash"));
    assert!(text.contains("Ask rules: WebFetch"));
    assert!(text.contains("Pending approval: Bash — approve pwd"));
}

#[tokio::test]
async fn plan_command_reports_inactive_status() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PlanCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan"),
            &app_state,
        )
        .await
        .expect("plan status should render");

    assert_eq!(
        result,
        RouteExecution::CommandResult(CommandResult::Message(
            "Plan mode is off. Use /plan enter [reason] to start planning.\nNo plan object exists for this session yet.".into()
        ))
    );
}

#[tokio::test]
async fn plan_command_enter_requests_approval_before_switching_mode() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PlanCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan enter draft feature work"),
            &app_state,
        )
        .await
        .expect("plan enter should request approval");

    assert_eq!(
        result,
        RouteExecution::CommandResult(CommandResult::Message(
            "approval required for EnterPlanMode: approve entering plan mode: draft feature work"
                .into(),
        ))
    );
    assert_eq!(permission_context.mode(), PermissionMode::Default);
    let pending = permission_context
        .pending_approval()
        .expect("pending approval should be set");
    assert_eq!(pending.tool_name, "EnterPlanMode");
    assert_eq!(pending.tool_input, "draft feature work");
}

#[tokio::test]
async fn plan_command_exit_requests_approval_and_approval_exits_mode() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PlanCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let plan_manager = Arc::new(PlanManager::default());
    plan_manager.ensure_draft(None);
    plan_manager.set_summary("implementation plan");
    plan_manager
        .add_step("Verify approval flow", None)
        .expect("add plan step");
    let task_list_manager = Arc::new(TaskListManager::default());
    let permission_context = ToolPermissionContext::new(PermissionMode::Plan)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_task_list_manager(task_list_manager.clone())
        .with_plan_manager(plan_manager);
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let request = router
        .route(
            &NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/plan exit implementation looks good",
            ),
            &app_state,
        )
        .await
        .expect("plan exit should request approval");
    assert_eq!(
        request,
        RouteExecution::CommandResult(CommandResult::Message(
            "approval required for ExitPlanMode: approve exiting plan mode: implementation looks good"
                .into(),
        ))
    );
    assert_eq!(permission_context.mode(), PermissionMode::Plan);

    let approved = router
        .route(
            &NormalizedInput::from_session_raw(InteractionSurface::Cli, "cli-session", "approve"),
            &app_state,
        )
        .await
        .expect("plan exit approval should resolve");
    assert_eq!(
        approved,
        RouteExecution::CommandResult(CommandResult::Message(
            "plan approved; exited plan mode: implementation looks good".into()
        ))
    );
    assert_eq!(permission_context.mode(), PermissionMode::Default);
    assert!(permission_context.pending_approval().is_none());
    let synced_tasks = task_list_manager.list();
    assert_eq!(synced_tasks.len(), 1);
    assert_eq!(synced_tasks[0].plan_step_id.as_deref(), Some("step-1"));
    assert_eq!(synced_tasks[0].subject, "Verify approval flow");
}

#[tokio::test]
async fn plan_command_handles_status_noop_and_denied_exit() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PlanCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let plan_manager = Arc::new(PlanManager::default());
    plan_manager.ensure_draft(None);
    plan_manager.set_summary("Track planning work");
    let step = plan_manager
        .add_step("Write tests", Some("cover manager and router flows"))
        .expect("add plan step");
    let task_list_manager = Arc::new(TaskListManager::default());
    let linked_task = task_list_manager.create(
        "Write tests",
        "cover manager and router flows",
        None,
        Some("planner".into()),
        Some(step.id.clone()),
    );
    let blocker = task_list_manager.create("Blocker", "upstream dependency", None, None, None);
    task_list_manager
        .update(
            &linked_task.id,
            rust_agent::task::list_manager::TaskListUpdate {
                status: Some(rust_agent::task::list_types::TaskListStatus::InProgress),
                add_blocked_by: vec![blocker.id.clone()],
                ..Default::default()
            },
        )
        .expect("mark linked task in progress with blocker");
    let runtime_task_manager = Arc::new(TaskManager::default());
    let runtime_task =
        runtime_task_manager.create("runtime patch task", "cli-session", InteractionSurface::Cli);
    runtime_task_manager.set_orchestration_group_id(&runtime_task.id, Some(step.id.clone()));
    runtime_task_manager.set_worker_role(&runtime_task.id, WorkerRole::Implement);
    runtime_task_manager.set_phase(&runtime_task.id, Some(WorkerPhase::Implement));
    runtime_task_manager
        .set_validation_state(&runtime_task.id, Some(ValidationState::PendingVerification));
    runtime_task_manager.start(&runtime_task.id);
    let active_context = ToolPermissionContext::new(PermissionMode::Plan)
        .with_task_manager(runtime_task_manager.clone())
        .with_task_list_manager(task_list_manager.clone())
        .with_plan_manager(plan_manager.clone());
    let active_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: active_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let status = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan status"),
            &active_state,
        )
        .await
        .expect("plan status should render");
    assert!(
        matches!(status, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("Plan mode is on."))
    );
    assert!(
        matches!(status, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("steps=1"))
    );

    let show = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan show"),
            &active_state,
        )
        .await
        .expect("plan show should render");
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("Execution: 0/1 completed (0%)"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("Step summary: total=1, completed=0, in_progress=0, pending=1, linked=1, unlinked=0"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains(&step.id))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("linked task:"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("owner=planner"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("blocked_by=task-1"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("warning: plan/task status mismatch"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("Runtime orchestration: groups=1, waiting_for_verification=0, ready_for_synthesis=0, still_in_progress=1"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains(&format!("runtime group: {} — group {} still in progress", step.id, step.id)))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("runtime task: task-0 [Running] role=implement phase=implement validation_state=pending_verification"))
    );
    assert!(
        matches!(show, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("hint: verification next for task-0"))
    );

    let add = router
        .route(
            &NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/plan add Review outputs | confirm summaries",
            ),
            &active_state,
        )
        .await
        .expect("plan add should succeed");
    assert!(
        matches!(add, RouteExecution::CommandResult(CommandResult::Message(message)) if message.contains("Added plan step step-2"))
    );

    let done = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, &format!("/plan done {}", step.id)),
            &active_state,
        )
        .await
        .expect("plan done should succeed");
    assert_eq!(
        done,
        RouteExecution::CommandResult(CommandResult::Message(format!(
            "Completed plan step {}",
            step.id
        )))
    );

    let history = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan history"),
            &active_state,
        )
        .await
        .expect("plan history should render");
    assert!(
        matches!(history, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("Plan history:"))
    );
    assert!(
        matches!(history, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("snapshot: steps="))
    );
    assert!(
        matches!(history, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("active_step="))
    );
    assert!(
        matches!(history, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("Current runtime overlay:"))
    );
    assert!(
        matches!(history, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("active_runtime_groups=1"))
    );
    assert!(
        matches!(history, RouteExecution::CommandResult(CommandResult::Message(ref message)) if message.contains("still_in_progress_groups=1"))
    );

    let reorder = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan reorder step-2 step-1"),
            &active_state,
        )
        .await
        .expect("plan reorder should succeed");
    assert_eq!(
        reorder,
        RouteExecution::CommandResult(CommandResult::Message("Reordered 2 plan steps".into()))
    );
    let reordered_state = plan_manager
        .state()
        .expect("reordered plan state should exist");
    let reordered_steps = &reordered_state.draft.expect("draft should exist").steps;
    assert_eq!(reordered_steps[0].id, "step-2");
    assert_eq!(reordered_steps[1].id, step.id);

    let no_op = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan enter"),
            &active_state,
        )
        .await
        .expect("plan enter in plan mode should no-op");
    assert_eq!(
        no_op,
        RouteExecution::CommandResult(CommandResult::Message("Already in plan mode.".into()))
    );

    let inactive_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_task_list_manager(Arc::new(TaskListManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let inactive_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: inactive_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let denied = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan exit"),
            &inactive_state,
        )
        .await
        .expect("inactive plan exit should resolve");
    assert_eq!(
        denied,
        RouteExecution::CommandResult(CommandResult::Denied("Plan mode is not active.".into()))
    );
}

#[tokio::test]
async fn permissions_command_mutates_mode_and_rule_lists() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PermissionsCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let mode_result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/permissions mode accept-edits"),
            &app_state,
        )
        .await
        .expect("mode update should succeed");
    assert_eq!(
        mode_result,
        RouteExecution::CommandResult(CommandResult::Message(
            "Permission mode set to accept-edits.".into()
        ))
    );
    assert_eq!(permission_context.mode(), PermissionMode::AcceptEdits);

    let allow_result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/permissions allow Read Bash"),
            &app_state,
        )
        .await
        .expect("allow update should succeed");
    assert_eq!(
        allow_result,
        RouteExecution::CommandResult(CommandResult::Message(
            "Added allow rule(s): Read, Bash".into()
        ))
    );
    assert_eq!(
        permission_context.always_allow_rules(),
        vec!["Read".to_string(), "Bash".to_string()]
    );

    let duplicate_result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/permissions allow Read"),
            &app_state,
        )
        .await
        .expect("duplicate allow should be handled");
    assert_eq!(
        duplicate_result,
        RouteExecution::CommandResult(CommandResult::Message("No new allow rules added.".into()))
    );

    let ask_result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/permissions ask WebFetch"),
            &app_state,
        )
        .await
        .expect("ask update should succeed");
    assert_eq!(
        ask_result,
        RouteExecution::CommandResult(CommandResult::Message("Added ask rule(s): WebFetch".into()))
    );
    assert_eq!(
        permission_context.always_ask_rules(),
        vec!["WebFetch".to_string()]
    );

    let deny_result = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/permissions deny Edit"),
            &app_state,
        )
        .await
        .expect("deny update should succeed");
    assert_eq!(
        deny_result,
        RouteExecution::CommandResult(CommandResult::Message("Added deny rule(s): Edit".into()))
    );
    assert_eq!(
        permission_context.always_deny_rules(),
        vec!["Edit".to_string()]
    );
}

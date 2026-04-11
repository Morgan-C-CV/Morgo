use std::sync::Arc;

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::builtin::permissions::PermissionsCommand;
use rust_agent::command::builtin::plan::PlanCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{InMemorySessionStore, SessionRestoreRequest, SessionStore};
use rust_agent::interaction::cli::repl::handle_cli_inputs;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::router::{CommandRouter, RouteDecision};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::security::authorizer::{AuthDecision, DefaultSurfaceAuthorizer, SurfaceAuthorizer};
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::TaskOwner;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

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

#[async_trait]
impl Command for RemoteSafeTestCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "remote-safe",
            description: "Test remote-safe command",
            command_type: CommandType::Local,
            availability: CommandAvailability::RemoteSafe,
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
        Ok(CommandResult::Message("remote safe response".into()))
    }
}

#[tokio::test]
async fn router_executes_known_commands_before_query() {
    let registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let router = CommandRouter::new(registry, Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/help");

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::ExecuteCommand("help".into())
    );
}

#[tokio::test]
async fn router_falls_back_for_unknown_commands() {
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/missing foo");

    assert_eq!(router.decide(&input).await, RouteDecision::ContinueToQuery);
}

#[tokio::test]
async fn router_denies_unauthenticated_remote_actor() {
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let mut input = NormalizedInput::from_raw(InteractionSurface::Remote, "/help");
    input.actor.is_authenticated = false;

    assert_eq!(
        router.decide(&input).await,
        RouteDecision::Deny("unauthenticated actor for remote surface".into())
    );
}

#[tokio::test]
async fn cli_repl_handles_multiple_inputs_in_sequence() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand))),
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
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
    assert!(outputs[0].primary_text.contains("help"));
    assert!(outputs[1].primary_text.contains("second reply"));
    assert!(outputs[0].events.is_empty());
    assert!(!outputs[1].events.is_empty());
}

#[tokio::test]
async fn cli_repl_surfaces_task_events_for_active_session() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand))),
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
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
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
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
async fn cli_repl_persists_denied_turns() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(RemoteSafeTestCommand))),
        Box::new(DenyingAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
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
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
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
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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

    assert_eq!(result, CommandResult::Message("entered plan mode: draft feature work".into()));
    assert_eq!(permission_context.mode(), PermissionMode::Plan);
    assert!(permission_context.pending_approval().is_none());
}

#[tokio::test]
async fn router_denies_pending_request_without_session_approval() {
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
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
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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

    assert_eq!(result, CommandResult::Message("Denied approval for Bash".into()));
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
    let router = CommandRouter::new(Arc::new(CommandRegistry::new()), Box::new(DefaultSurfaceAuthorizer));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
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
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new().register(Arc::new(rust_agent::tool::builtin::bash::BashTool))))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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

    let CommandResult::Message(text) = result else {
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
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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

    let CommandResult::Message(text) = result else {
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
        .with_task_manager(Arc::new(TaskManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
        CommandResult::Message(
            "Plan mode is off. Use /plan enter [reason] to start planning.".into()
        )
    );
}

#[tokio::test]
async fn plan_command_enter_requests_approval_before_switching_mode() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PlanCommand))),
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
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: Vec::new(),
        active_session_id: "cli-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };

    let result = router
        .route(
            &NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/plan enter draft feature work",
            ),
            &app_state,
        )
        .await
        .expect("plan enter should request approval");

    assert_eq!(
        result,
        CommandResult::Message(
            "approval required for EnterPlanMode: approve entering plan mode: draft feature work"
                .into(),
        )
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
    let permission_context = ToolPermissionContext::new(PermissionMode::Plan)
        .with_task_manager(Arc::new(TaskManager::default()));
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
        CommandResult::Message(
            "approval required for ExitPlanMode: approve exiting plan mode: implementation looks good"
                .into(),
        )
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
        CommandResult::Message("plan approved; exited plan mode: implementation looks good".into())
    );
    assert_eq!(permission_context.mode(), PermissionMode::Default);
    assert!(permission_context.pending_approval().is_none());
}

#[tokio::test]
async fn plan_command_handles_status_noop_and_denied_exit() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PlanCommand))),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let active_context = ToolPermissionContext::new(PermissionMode::Plan)
        .with_task_manager(Arc::new(TaskManager::default()));
    let active_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context: active_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
    assert_eq!(
        status,
        CommandResult::Message(
            "Plan mode is on. Use /plan exit [summary] when ready to leave.".into()
        )
    );

    let no_op = router
        .route(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plan enter"),
            &active_state,
        )
        .await
        .expect("plan enter in plan mode should no-op");
    assert_eq!(no_op, CommandResult::Message("Already in plan mode.".into()));

    let inactive_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let inactive_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context: inactive_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
    assert_eq!(denied, CommandResult::Denied("Plan mode is not active.".into()));
}

#[tokio::test]
async fn permissions_command_mutates_mode_and_rule_lists() {
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new().register(Arc::new(PermissionsCommand))),
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
        permission_context: permission_context.clone(),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
        CommandResult::Message("Permission mode set to accept-edits.".into())
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
        CommandResult::Message("Added allow rule(s): Read, Bash".into())
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
        CommandResult::Message("No new allow rules added.".into())
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
        CommandResult::Message("Added ask rule(s): WebFetch".into())
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
        CommandResult::Message("Added deny rule(s): Edit".into())
    );
    assert_eq!(
        permission_context.always_deny_rules(),
        vec!["Edit".to_string()]
    );
}

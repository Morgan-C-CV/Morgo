use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{InMemorySessionStore, SessionHistory, SessionRestoreRequest, SessionSnapshot, SessionStore};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::{Notification, NotificationTarget};
use rust_agent::interaction::remote::{
    RemoteEventPayload, RemoteRequest, drain_remote_notifications, handle_remote_request,
};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::authorizer::DefaultSurfaceAuthorizer;
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

#[tokio::test]
async fn remote_request_runs_minimal_query_chain() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-e2e-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-e2e".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    let app_state = AppState {
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
        startup_trace: Vec::new(),
        active_session_id: "remote-e2e-session".into(),
        session_store: Some(session_store.clone()),
        session: None,
        history: None,
        restored_session: None,
    };
    let engine = rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
        app_state: app_state.clone(),
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("remote integration reply".into()),
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
            session_id: "remote-bound-session".into(),
            actor_id: "remote-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "summarize remote chain".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.primary_text.contains("remote integration reply"));
    assert!(response
        .events
        .iter()
        .any(|event| event.event_type == "assistant_delta"));
    assert!(response
        .events
        .iter()
        .any(|event| event.event_type == "session_milestone"));

    let (_, default_history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-e2e-session".into()),
            continue_session: false,
        })
        .expect("default app-state session should still exist");
    assert!(
        default_history.entries.is_empty(),
        "query persistence should not fall back to app_state.active_session_id"
    );

    let (remote_snapshot, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-bound-session".into()),
            continue_session: false,
        })
        .expect("remote request session should persist");
    assert_eq!(remote_snapshot.session_id.0, "remote-bound-session");
    assert_eq!(remote_snapshot.surface, InteractionSurface::Remote);
    assert_eq!(history.entries.len(), 2);
    assert_eq!(
        history.entries[0].message,
        rust_agent::core::message::Message::user("summarize remote chain")
    );
    assert_eq!(
        history.entries[1].message,
        rust_agent::core::message::Message::assistant("remote integration reply")
    );
}

#[tokio::test]
async fn remote_request_drains_async_remote_notifications() {
    let command_registry = Arc::new(CommandRegistry::new());
    let _router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    let app_state = AppState {
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
        startup_trace: Vec::new(),
        active_session_id: "remote-async-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
    };

    let mut actor_notification = Notification::approval_required(
        "remote-async-session",
        "Bash",
        "requires explicit approval",
    );
    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-async-session".into(),
        actor_id: "remote-actor".into(),
    });
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, actor_notification);
    app_state.notification_dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice("remote-async-session", "tool", "background update"),
    );

    let drained = drain_remote_notifications(&app_state, "remote-async-session", Some("remote-actor"));
    assert_eq!(drained.len(), 2);
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::ApprovalRequired { tool_name, message }
            if tool_name == "Bash" && message == "requires explicit approval"
    )));
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::RuntimeNotice { kind, message }
            if kind == "tool" && message == "background update"
    )));
    assert!(drain_remote_notifications(&app_state, "remote-async-session", Some("remote-actor")).is_empty());
}

#[tokio::test]
async fn remote_request_preserves_response_boundary_and_async_inbox_semantics() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_pending_approval(rust_agent::state::permission_context::PendingApproval {
            tool_name: "Bash".into(),
            tool_input: serde_json::json!({"command": "ls"}).to_string(),
            message: "requires explicit approval".into(),
        });
    let session_store = Arc::new(InMemorySessionStore::default());
    let app_state = AppState {
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
        startup_trace: Vec::new(),
        active_session_id: "remote-boundary-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
    };
    app_state.notification_dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice("remote-boundary-session", "tool", "background only"),
    );
    let engine = rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
        app_state: app_state.clone(),
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("boundary reply".into()),
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
            session_id: "remote-boundary-session".into(),
            actor_id: "actor-a".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "hello boundary".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response
        .events
        .iter()
        .all(|event| !matches!(event.payload, RemoteEventPayload::RuntimeNotice { .. })));

    let drained = drain_remote_notifications(&app_state, "remote-boundary-session", Some("actor-a"));
    assert_eq!(drained.len(), 1);
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::RuntimeNotice { kind, message }
            if kind == "tool" && message == "background only"
    )));
    assert!(drain_remote_notifications(&app_state, "remote-boundary-session", Some("other-actor")).is_empty());
}

#[tokio::test]
async fn remote_request_returns_typed_remote_event_envelopes() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-event-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-events".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    let app_state = AppState {
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
        startup_trace: Vec::new(),
        active_session_id: "remote-event-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
    };
    let engine = rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
        app_state: app_state.clone(),
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("typed remote reply".into()),
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
            session_id: "remote-bound-events".into(),
            actor_id: "remote-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "typed remote chain".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response
        .events
        .iter()
        .any(|event| event.event_type == "assistant_delta"));
    assert!(response
        .events
        .iter()
        .any(|event| matches!(&event.payload, RemoteEventPayload::AssistantDelta { text } if text == "typed remote reply")));
    assert!(response
        .events
        .iter()
        .any(|event| matches!(&event.payload, RemoteEventPayload::SessionMilestone { kind } if kind == "assistant_message_committed")));
    assert!(response
        .events
        .iter()
        .all(|event| event.event_type != "task:task-0:remote task:inspect task output for task-0"));
}

use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::binding::{
    SessionBinding, TelegramDeliveryTarget, TelegramInboundBindingAuthorization,
};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::interaction::telegram::runtime::TelegramRuntimeResponse;
use rust_agent::interaction::telegram::transport::{
    TelegramMessage, TelegramMessageFrom, TelegramTransportMode, TelegramUpdate,
    TelegramUpdateIntake, TelegramUpdateOutcome, handle_telegram_update, normalize_telegram_update,
};
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::audit::AuditLog;
use rust_agent::security::authorizer::{DefaultSurfaceAuthorizer, SurfaceAdmissionPolicy};
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_tg_app_state() -> AppState {
    AppState {
        surface: InteractionSurface::Telegram,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default()))
            .with_plan_manager(Arc::new(PlanManager::default())),
        command_registry: Some(Arc::new(CommandRegistry::new())),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(AuditLog::default())),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "test".into(),
            protocol: "test".into(),
            compatibility_profile: "test".into(),
            base_url_host: "test".into(),
            model: "test".into(),
            auth_status: "none".into(),
        },
        active_session_id: "tg-session".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    }
}

fn make_tg_engine(app_state: &AppState, reply: &str) -> rust_agent::core::engine::QueryEngine {
    rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
        app_state: app_state.clone(),
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta(reply.into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]]),
        compactor: ReactiveCompactor,
        hook_registry: rust_agent::hook::registry::HookRegistry::default(),
        agent_id: None,
        system_prompt: "test".into(),
        tools_prompt: "test".into(),
        context_prompt: "test".into(),
    })
}

fn make_tg_router() -> rust_agent::interaction::router::CommandRouter {
    rust_agent::interaction::router::CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    )
}

fn make_bound_gateway(user_id: &str, bot_id: &str, chat_id: &str) -> TelegramGateway {
    let session_id = format!("tg:{user_id}:{chat_id}");
    let actor_id = format!("tg:{user_id}");
    TelegramGateway::default().with_bindings(vec![SessionBinding {
        actor_id,
        session_id,
        telegram_user_id: Some(user_id.into()),
        bot_id: Some(bot_id.into()),
        delivery_target: Some(TelegramDeliveryTarget {
            chat_id: chat_id.into(),
            thread_id: None,
        }),
    }])
}

fn make_update(
    update_id: u64,
    bot_id: &str,
    user_id: &str,
    chat_id: &str,
    text: &str,
) -> TelegramUpdate {
    TelegramUpdate {
        update_id,
        bot_id: bot_id.into(),
        message: Some(TelegramMessage {
            message_id: update_id,
            chat_id: chat_id.into(),
            from: Some(TelegramMessageFrom {
                user_id: user_id.into(),
                username: None,
            }),
            text: Some(text.into()),
        }),
    }
}

// ── normalize_telegram_update unit tests ─────────────────────────────────────

#[test]
fn r3_4_normalize_accepted_on_valid_update() {
    let update = make_update(1, "bot1", "user1", "chat1", "hello");
    let intake = normalize_telegram_update(&update);
    assert!(
        matches!(intake, TelegramUpdateIntake::Accepted(_)),
        "expected Accepted, got {intake:?}"
    );
    if let TelegramUpdateIntake::Accepted(env) = intake {
        assert_eq!(env.telegram_user_id, "user1");
        assert_eq!(env.bot_id, "bot1");
        assert_eq!(env.actor_id, "tg:user1");
        assert_eq!(env.session_id, "tg:user1:chat1");
        assert_eq!(env.raw_text, "hello");
    }
}

#[test]
fn r3_4_normalize_skipped_when_no_message() {
    let update = TelegramUpdate {
        update_id: 2,
        bot_id: "bot1".into(),
        message: None,
    };
    let intake = normalize_telegram_update(&update);
    assert!(
        matches!(
            intake,
            TelegramUpdateIntake::Skipped {
                reason: "no_message",
                ..
            }
        ),
        "expected Skipped(no_message), got {intake:?}"
    );
}

#[test]
fn r3_4_normalize_skipped_when_no_text() {
    let update = TelegramUpdate {
        update_id: 3,
        bot_id: "bot1".into(),
        message: Some(TelegramMessage {
            message_id: 3,
            chat_id: "chat1".into(),
            from: Some(TelegramMessageFrom {
                user_id: "user1".into(),
                username: None,
            }),
            text: None,
        }),
    };
    let intake = normalize_telegram_update(&update);
    assert!(
        matches!(
            intake,
            TelegramUpdateIntake::Skipped {
                reason: "no_text",
                ..
            }
        ),
        "expected Skipped(no_text), got {intake:?}"
    );
}

#[test]
fn r3_4_normalize_malformed_when_no_from() {
    let update = TelegramUpdate {
        update_id: 4,
        bot_id: "bot1".into(),
        message: Some(TelegramMessage {
            message_id: 4,
            chat_id: "chat1".into(),
            from: None,
            text: Some("hello".into()),
        }),
    };
    let intake = normalize_telegram_update(&update);
    assert!(
        matches!(
            intake,
            TelegramUpdateIntake::Malformed {
                reason: "missing_from",
                ..
            }
        ),
        "expected Malformed(missing_from), got {intake:?}"
    );
}

// ── handle_telegram_update integration tests ─────────────────────────────────

#[tokio::test]
async fn r3_4_webhook_update_authorized_returns_dispatched() {
    let app_state = make_tg_app_state();
    let router = make_tg_router();
    let engine = make_tg_engine(&app_state, "tg reply");
    let gateway = make_bound_gateway("user1", "bot1", "chat1");

    let response = handle_telegram_update(
        &router,
        &engine,
        &app_state,
        &gateway,
        make_update(10, "bot1", "user1", "chat1", "hello"),
        TelegramTransportMode::Webhook,
    )
    .await
    .expect("webhook dispatch should succeed");

    assert_eq!(response.update_id, 10);
    assert_eq!(response.transport_mode, TelegramTransportMode::Webhook);
    assert!(response.is_dispatched());
    assert!(response.is_authorized());
}

#[tokio::test]
async fn r3_4_polling_update_authorized_returns_dispatched() {
    let app_state = make_tg_app_state();
    let router = make_tg_router();
    let engine = make_tg_engine(&app_state, "poll reply");
    let gateway = make_bound_gateway("user2", "bot1", "chat2");

    let response = handle_telegram_update(
        &router,
        &engine,
        &app_state,
        &gateway,
        make_update(20, "bot1", "user2", "chat2", "poll msg"),
        TelegramTransportMode::Polling,
    )
    .await
    .expect("polling dispatch should succeed");

    assert_eq!(response.transport_mode, TelegramTransportMode::Polling);
    assert!(response.is_authorized());
}

#[tokio::test]
async fn r3_4_update_rejected_when_not_in_allowlist() {
    let app_state = make_tg_app_state();
    let router = make_tg_router();
    let engine = make_tg_engine(&app_state, "should not reach");
    // Gateway with no bindings — all inbound will be SessionNotBound
    let gateway = TelegramGateway::default();

    let response = handle_telegram_update(
        &router,
        &engine,
        &app_state,
        &gateway,
        make_update(30, "bot1", "user3", "chat3", "hello"),
        TelegramTransportMode::Webhook,
    )
    .await
    .expect("rejection should return Ok(response)");

    assert!(response.is_dispatched());
    assert!(!response.is_authorized());
    assert_eq!(
        response.rejection(),
        Some(&TelegramInboundBindingAuthorization::SessionNotBound)
    );
}

#[tokio::test]
async fn r3_4_update_skipped_when_no_text() {
    let app_state = make_tg_app_state();
    let router = make_tg_router();
    let engine = make_tg_engine(&app_state, "should not reach");
    let gateway = make_bound_gateway("user4", "bot1", "chat4");

    let update = TelegramUpdate {
        update_id: 40,
        bot_id: "bot1".into(),
        message: Some(TelegramMessage {
            message_id: 40,
            chat_id: "chat4".into(),
            from: Some(TelegramMessageFrom {
                user_id: "user4".into(),
                username: None,
            }),
            text: None,
        }),
    };

    let response = handle_telegram_update(
        &router,
        &engine,
        &app_state,
        &gateway,
        update,
        TelegramTransportMode::Webhook,
    )
    .await
    .expect("skip should return Ok(response)");

    assert_eq!(response.update_id, 40);
    assert!(matches!(
        response.outcome,
        TelegramUpdateOutcome::Skipped { reason: "no_text" }
    ));
}

#[tokio::test]
async fn r3_4_update_id_echoed_in_response() {
    let app_state = make_tg_app_state();
    let router = make_tg_router();
    let engine = make_tg_engine(&app_state, "echo reply");
    let gateway = make_bound_gateway("user5", "bot1", "chat5");

    let response = handle_telegram_update(
        &router,
        &engine,
        &app_state,
        &gateway,
        make_update(99999, "bot1", "user5", "chat5", "echo"),
        TelegramTransportMode::Polling,
    )
    .await
    .expect("dispatch should succeed");

    assert_eq!(response.update_id, 99999);
}

#[tokio::test]
async fn r3_4_authorized_response_contains_primary_text() {
    let app_state = make_tg_app_state();
    let router = make_tg_router();
    let engine = make_tg_engine(&app_state, "the answer");
    let gateway = make_bound_gateway("user6", "bot1", "chat6");

    let response = handle_telegram_update(
        &router,
        &engine,
        &app_state,
        &gateway,
        make_update(50, "bot1", "user6", "chat6", "question"),
        TelegramTransportMode::Webhook,
    )
    .await
    .expect("dispatch should succeed");

    if let TelegramUpdateOutcome::Dispatched(TelegramRuntimeResponse::Authorized {
        primary_text,
        ..
    }) = &response.outcome
    {
        assert_eq!(primary_text, "the answer");
    } else {
        panic!(
            "expected Dispatched(Authorized), got {:?}",
            response.outcome
        );
    }
}

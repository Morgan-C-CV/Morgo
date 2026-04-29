use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::InMemorySessionStore;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::remote::RemoteResponseOutcome;
use rust_agent::interaction::remote_actor::RemoteActorStore;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::interaction::web::{WebRequest, WebStatusCode, handle_web_request};
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

fn make_web_app_state(store: Option<Arc<RemoteActorStore>>) -> AppState {
    AppState {
        surface: InteractionSurface::Remote,
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
        active_session_id: "web-session".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: store,
    }
}

fn make_web_engine(app_state: &AppState, reply: &str) -> rust_agent::core::engine::QueryEngine {
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

fn make_web_router() -> rust_agent::interaction::router::CommandRouter {
    rust_agent::interaction::router::CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    )
}

#[tokio::test]
async fn r3_3_web_request_ok_returns_200_and_body() {
    let app_state = make_web_app_state(None);
    let router = make_web_router();
    let engine = make_web_engine(&app_state, "web reply");

    let response = handle_web_request(
        &router,
        &engine,
        &app_state,
        WebRequest::new("web-sess-ok", "web-actor-ok", true, "hello"),
    )
    .await
    .expect("web request should succeed");

    assert!(response.is_ok());
    assert_eq!(response.status_code(), 200);
    assert_eq!(response.body, "web reply");
    assert_eq!(response.meta.outcome, RemoteResponseOutcome::Ok);
}

#[tokio::test]
async fn r3_3_web_request_denied_returns_403() {
    let app_state = make_web_app_state(None);
    let router = rust_agent::interaction::router::CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(
            DefaultSurfaceAuthorizer::default().with_remote_policy(SurfaceAdmissionPolicy {
                allowlisted_actors: ["allowed-only".to_string()].into_iter().collect(),
                max_requests_per_window: None,
                window_seconds: 60,
                abuse_denial_threshold: None,
            }),
        ),
    );
    let engine = make_web_engine(&app_state, "should not reach");

    let response = handle_web_request(
        &router,
        &engine,
        &app_state,
        WebRequest::new("web-sess-denied", "not-allowed", false, "hello"),
    )
    .await
    .expect("denial should return Ok(response)");

    assert_eq!(response.status_code(), 403);
    assert_eq!(response.status, WebStatusCode::Forbidden403);
    assert_eq!(response.meta.outcome, RemoteResponseOutcome::Denied);
    assert!(!response.body.is_empty());
}

#[tokio::test]
async fn r3_3_web_request_correlation_id_echoed_in_meta() {
    let app_state = make_web_app_state(None);
    let router = make_web_router();
    let engine = make_web_engine(&app_state, "corr reply");

    let response = handle_web_request(
        &router,
        &engine,
        &app_state,
        WebRequest::new("web-sess-corr", "web-actor-corr", true, "hello")
            .with_correlation_id("web-req-456"),
    )
    .await
    .expect("request should succeed");

    assert_eq!(response.meta.correlation_id, Some("web-req-456".into()));
}

#[tokio::test]
async fn r3_3_web_request_actor_store_increments_request_count() {
    let store = Arc::new(RemoteActorStore::in_memory());
    let app_state = make_web_app_state(Some(store));
    let router = make_web_router();

    let engine1 = make_web_engine(&app_state, "first");
    handle_web_request(
        &router,
        &engine1,
        &app_state,
        WebRequest::new("web-sess-count", "web-actor-count", true, "first"),
    )
    .await
    .expect("first request should succeed");

    let engine2 = make_web_engine(&app_state, "second");
    let response2 = handle_web_request(
        &router,
        &engine2,
        &app_state,
        WebRequest::new("web-sess-count", "web-actor-count", true, "second"),
    )
    .await
    .expect("second request should succeed");

    assert_eq!(response2.meta.request_count, 2);
}

#[tokio::test]
async fn r3_3_web_request_audit_event_kind_created_on_first_request() {
    let store = Arc::new(RemoteActorStore::in_memory());
    let app_state = make_web_app_state(Some(store));
    let router = make_web_router();
    let engine = make_web_engine(&app_state, "first");

    let response = handle_web_request(
        &router,
        &engine,
        &app_state,
        WebRequest::new("web-sess-created", "web-actor-created", true, "hello"),
    )
    .await
    .expect("request should succeed");

    assert_eq!(response.meta.audit_event_kind, "remote_actor_created");
}

#[tokio::test]
async fn r3_3_web_request_actor_id_and_session_id_in_meta() {
    let app_state = make_web_app_state(None);
    let router = make_web_router();
    let engine = make_web_engine(&app_state, "meta check");

    let response = handle_web_request(
        &router,
        &engine,
        &app_state,
        WebRequest::new("web-sess-meta", "web-actor-meta", true, "hello"),
    )
    .await
    .expect("request should succeed");

    assert_eq!(response.meta.actor_id, "web-actor-meta");
    assert_eq!(response.meta.session_id, "web-sess-meta");
    assert!(response.meta.is_authenticated);
    assert!(response.meta.from_trusted_surface);
}

#[tokio::test]
async fn r3_3_web_status_code_from_outcome_mapping() {
    assert_eq!(WebStatusCode::from(RemoteResponseOutcome::Ok).as_u16(), 200);
    assert_eq!(
        WebStatusCode::from(RemoteResponseOutcome::Denied).as_u16(),
        403
    );
    assert_eq!(
        WebStatusCode::from(RemoteResponseOutcome::RuntimeError).as_u16(),
        500
    );
}

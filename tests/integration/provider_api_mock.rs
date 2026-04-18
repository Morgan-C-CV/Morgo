use std::net::SocketAddr;
use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::events::ServiceFailureCode;
use rust_agent::core::message::Message;
use rust_agent::core::query_loop::{QueryLoopState, Terminal};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::hook::registry::HookRegistry;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::service::api::client::{ModelProviderClient, ModelProviderConfig, ProviderTimeout};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::service::api::streaming::{ProviderFailureDisposition, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn query_engine_submit_turn_works_through_production_provider_path() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_single_response_server(listener));

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };
    let cost_tracker =
        CostTracker::with_default_pricing(config.model_id.clone(), config.pricing.clone());

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: cost_tracker.clone(),
            service_observability_tracker:
                rust_agent::service::observability::ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_session_id: "provider-it-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::from_config(config),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "system".into(),
        tools_prompt: "tools".into(),
        context_prompt: "context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("hello")).await;

    assert_eq!(
        result.messages,
        vec![Message::assistant("hello from mock provider")]
    );
    let report = cost_tracker.format_report();
    assert!(report.contains("model claude-test ->"));
    assert!(report.contains("output_tokens: 9"));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_assembles_partial_tool_use_payload_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_partial_tool_use_response_server(listener));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let events = ModelProviderClient::from_config(config)
        .stream_message(&Message::user("inspect file"))
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::TextDelta(text) if text == "planning..."
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::ToolUse { tool_name, input }
            if tool_name == "Agent" && input == "\"inspect file\""
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::MessageStop {
            stop_reason: rust_agent::service::api::streaming::StopReason::ToolUse
        }
    )));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_preserves_terminal_http_compatibility_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_single_error_response_server(listener));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let events = ModelProviderClient::from_config(config)
        .stream_message(&Message::user("hello"))
        .await;

    assert!(matches!(
        &events[0],
        StreamEvent::Error(error)
            if error.provider_id == "anthropic"
                && error.disposition == ProviderFailureDisposition::PreStreamTerminal
                && error.status_code == Some(400)
    ));

    server.await.expect("mock provider server finished");
}

async fn run_single_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let sse = concat!(
        "event: message\r\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":14}}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello from mock provider\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":9}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"message_stop\"}\r\n\r\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        sse.len(),
        sse
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write mock provider response");
    stream.flush().await.expect("flush mock provider response");
}

#[tokio::test]
async fn production_provider_surfaces_interrupted_stream_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_interrupted_stream_response_server(listener));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let events = ModelProviderClient::from_config(config)
        .stream_message(&Message::user("hello"))
        .await;

    assert!(matches!(
        &events[0],
        StreamEvent::Error(error)
            if error.provider_id == "anthropic"
                && error.kind == "overloaded_error"
                && error.retryable
                && error.disposition == ProviderFailureDisposition::StreamInterrupted
                && error.status_code.is_none()
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_surfaces_malformed_stream_as_protocol_failure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_malformed_stream_response_server(listener));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let events = ModelProviderClient::from_config(config)
        .stream_message(&Message::user("hello"))
        .await;

    assert!(matches!(
        &events[0],
        StreamEvent::Error(error)
            if error.provider_id == "anthropic"
                && error.kind == "sse_protocol"
                && !error.retryable
                && error.disposition == ProviderFailureDisposition::StreamTerminal
                && error.status_code.is_none()
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_maps_terminal_http_error_to_query_loop_failure_code() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_single_error_response_server(listener));

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker:
                rust_agent::service::observability::ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_session_id: "provider-terminal-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::from_config(config),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "system".into(),
        tools_prompt: "tools".into(),
        context_prompt: "context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("hello")).await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "provider request failed with status 400: {\"error\":\"bad request\"}".into(),
            code: Some(ServiceFailureCode::ApiProviderHttp4xx),
        }
    );

    server.await.expect("mock provider server finished");
}

async fn run_partial_tool_use_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let sse = concat!(
        "event: message\r\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":5}}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"planning...\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Agent\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"partial_json\":\"\\\"inspect file\\\"\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_stop\"}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"message_stop\"}\r\n\r\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        sse.len(),
        sse
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write mock provider response");
    stream.flush().await.expect("flush mock provider response");
}

async fn run_interrupted_stream_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let sse = concat!(
        "event: message\r\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"provider overloaded\"}}\r\n\r\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        sse.len(),
        sse
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write interrupted stream response");
    stream
        .flush()
        .await
        .expect("flush interrupted stream response");
}

async fn run_malformed_stream_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let sse = concat!(
        "event: message\r\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {not-json}\r\n\r\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        sse.len(),
        sse
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write malformed stream response");
    stream
        .flush()
        .await
        .expect("flush malformed stream response");
}

async fn run_single_error_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let body = "{\"error\":\"bad request\"}";
    let response = format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write mock provider error response");
    stream
        .flush()
        .await
        .expect("flush mock provider error response");
}

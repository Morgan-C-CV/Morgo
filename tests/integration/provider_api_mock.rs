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
use rust_agent::service::api::streaming::{ProviderFailureDisposition, StreamEvent, StopReason};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Duration, Instant, sleep};

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
async fn production_provider_request_envelope_stays_compatible() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_request_capture_response_server(listener));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: " ".into(),
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

    assert!(events.iter().any(|event| matches!(event, StreamEvent::MessageStart)));
    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::TextDelta(text) if text == "request captured"
    )));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_surfaces_unsupported_streaming_as_typed_failure() {
    let config = ModelProviderConfig {
        provider_id: "batch-provider".into(),
        base_url: "http://127.0.0.1:1".into(),
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
            if error.provider_id == "batch-provider"
                && error.kind == "capability_unsupported"
                && !error.retryable
                && error.disposition == ProviderFailureDisposition::PreStreamTerminal
                && error.message.contains("streaming")
    ));
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
async fn production_provider_normalizes_stringified_tool_use_alias_payload() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_stringified_tool_use_alias_response_server(listener));

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
        StreamEvent::ToolUse { tool_name, input }
            if tool_name == "Agent" && input == "{\"prompt\":\"inspect file\"}"
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
                && error.message == "provider request failed with status 400: bad request"
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_accepts_top_level_usage_envelope() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_top_level_usage_response_server(listener));

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

    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::Usage(usage) if usage.model == "claude-alt" && usage.input_tokens == 11
    )));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_accepts_delta_usage_envelope() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_delta_usage_response_server(listener));

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

    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::Usage(usage) if usage.model == "claude-test" && usage.output_tokens == 6
    )));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_extracts_nested_http_error_message() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_nested_error_response_server(listener));

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
                && error.kind == "http_status"
                && error.status_code == Some(400)
                && error.message == "provider request failed with status 400: nested provider error"
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
async fn production_provider_surfaces_tool_use_protocol_failure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_tool_stop_without_payload_response_server(listener));

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
                && error.kind == "tool_use_protocol"
                && !error.retryable
                && error.disposition == ProviderFailureDisposition::StreamTerminal
                && error.status_code.is_none()
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_surfaces_structured_output_protocol_failure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_incomplete_structured_output_response_server(listener));

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
                && error.kind == "structured_output_invalid"
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
            message: "provider request failed with status 400: bad request".into(),
            code: Some(ServiceFailureCode::ApiProviderHttp4xx),
        }
    );

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_rejects_wrong_content_type_as_invalid_response() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_wrong_content_type_response_server(listener));

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
                && error.kind == "bad_content_type"
                && !error.retryable
                && error.disposition == ProviderFailureDisposition::PreStreamTerminal
                && error.status_code.is_none()
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_rejects_empty_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_empty_stream_response_server(listener));

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
                && error.kind == "empty_body"
                && !error.retryable
                && error.disposition == ProviderFailureDisposition::PreStreamTerminal
                && error.status_code.is_none()
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_rejects_truncated_stream_as_protocol_failure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_truncated_stream_response_server(listener));

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
async fn production_provider_maps_timeout_after_retries_exhaust_to_query_loop_failure_code() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_delayed_timeout_response_server(listener, 2));

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 25,
        },
        retry_policy: RetryPolicy {
            max_attempts: 2,
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
            active_session_id: "provider-timeout-session".into(),
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
    assert!(matches!(
        result.terminal,
        Terminal::ModelError {
            ref message,
            code: Some(ServiceFailureCode::ApiProviderTimeout),
        } if message.contains("provider request timed out")
            || message.contains("provider request failed")
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_retries_429_then_succeeds() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_retry_then_success_response_server(listener, None));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let events = ModelProviderClient::from_config(config)
        .stream_message(&Message::user("hello"))
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::TextDelta(text) if text == "recovered after retry"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn
        }
    )));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_respects_retry_after_header_for_429_retry() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_retry_then_success_response_server(listener, Some("1")));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        },
        pricing: Default::default(),
    };

    let started = Instant::now();
    let events = ModelProviderClient::from_config(config)
        .stream_message(&Message::user("hello"))
        .await;
    let elapsed = started.elapsed();

    assert!(elapsed >= Duration::from_millis(900));
    assert!(events.iter().any(|event| matches!(
        event,
        StreamEvent::TextDelta(text) if text == "recovered after retry"
    )));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_does_not_retry_terminal_400_errors() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_retry_then_terminal_http_response_server(
        listener,
        "400 Bad Request",
        "{\"error\":\"bad request\"}",
    ));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 3,
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
                && error.kind == "http_status"
                && error.status_code == Some(400)
                && error.disposition == ProviderFailureDisposition::PreStreamTerminal
                && !error.retryable
    ));

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn production_provider_retries_503_then_surfaces_terminal_protocol_failure() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_retry_then_stream_protocol_failure_server(listener));

    let config = ModelProviderConfig {
        provider_id: "anthropic".into(),
        base_url: format!("http://{}", addr),
        api_key: Some("test-key".into()),
        model_id: "claude-test".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 5_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 2,
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
                && error.disposition == ProviderFailureDisposition::StreamTerminal
                && !error.retryable
                && error.status_code.is_none()
    ));

    server.await.expect("mock provider server finished");
}

async fn run_request_capture_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let read = stream.read(&mut buffer).await.expect("read request");
    let request = String::from_utf8_lossy(&buffer[..read]);
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .expect("request body");
    let parsed: serde_json::Value = serde_json::from_str(body).expect("valid request json");

    assert_eq!(parsed.get("model").and_then(|value| value.as_str()), Some("default-model"));
    assert_eq!(parsed.get("stream").and_then(|value| value.as_bool()), Some(true));
    assert_eq!(parsed["messages"][0]["role"].as_str(), Some("user"));
    assert_eq!(parsed["messages"][0]["content"][0]["type"].as_str(), Some("text"));
    assert_eq!(parsed["messages"][0]["content"][0]["text"].as_str(), Some("hello"));

    let sse = concat!(
        "event: message\r\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"request captured\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\r\n\r\n",
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
        .expect("write request capture response");
    stream.flush().await.expect("flush request capture response");
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

async fn run_stringified_tool_use_alias_response_server(listener: TcpListener) {
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
        "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Agent\",\"args\":\"{\\\"prompt\\\":\\\"inspect file\\\"}\"}}\r\n\r\n",
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

async fn run_tool_stop_without_payload_response_server(listener: TcpListener) {
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
        .expect("write malformed tool-use response");
    stream
        .flush()
        .await
        .expect("flush malformed tool-use response");
}

async fn run_incomplete_structured_output_response_server(listener: TcpListener) {
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
        "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"structured_output\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"partial_json\":\"{\\\"answer\\\":\"}}\r\n\r\n",
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
        .expect("write incomplete structured output response");
    stream
        .flush()
        .await
        .expect("flush incomplete structured output response");
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

async fn run_nested_error_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let body = "{\"error\":{\"message\":\"nested provider error\"}}";
    let response = format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write nested error response");
    stream.flush().await.expect("flush nested error response");
}

async fn run_top_level_usage_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let sse = concat!(
        "event: message\r\n",
        "data: {\"type\":\"message_start\",\"model\":\"claude-alt\",\"usage\":{\"inputTokens\":11}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello\"}}\r\n\r\n",
        "event: message\r\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\r\n\r\n",
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
        .expect("write top-level usage response");
    stream.flush().await.expect("flush top-level usage response");
}

async fn run_delta_usage_response_server(listener: TcpListener) {
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
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"usage\":{\"outputTokens\":6}}}\r\n\r\n",
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
        .expect("write delta usage response");
    stream.flush().await.expect("flush delta usage response");
}

async fn run_wrong_content_type_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let body = "{\"type\":\"message_start\"}";
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write wrong content-type response");
    stream
        .flush()
        .await
        .expect("flush wrong content-type response");
}

async fn run_empty_stream_response_server(listener: TcpListener) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let response = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write empty stream response");
    stream.flush().await.expect("flush empty stream response");
}

async fn run_truncated_stream_response_server(listener: TcpListener) {
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
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"partial\"}}"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        sse.len(),
        sse
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write truncated stream response");
    stream
        .flush()
        .await
        .expect("flush truncated stream response");
}

async fn run_delayed_timeout_response_server(listener: TcpListener, attempts: usize) {
    for _ in 0..attempts {
        let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
            .accept()
            .await
            .expect("accept mock provider request");
        let mut buffer = vec![0_u8; 16 * 1024];
        let _ = stream.read(&mut buffer).await.expect("read request");
        sleep(Duration::from_millis(100)).await;
        let _ = stream.shutdown().await;
    }
}

async fn run_retry_then_success_response_server(listener: TcpListener, retry_after: Option<&'static str>) {
    let mut served_retry = false;
    loop {
        let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
            .accept()
            .await
            .expect("accept mock provider request");
        let mut buffer = vec![0_u8; 16 * 1024];
        let _ = stream.read(&mut buffer).await.expect("read request");

        if !served_retry {
            served_retry = true;
            let body = "{\"error\":\"slow down\"}";
            let retry_header = retry_after
                .map(|value| format!("retry-after: {value}\r\n"))
                .unwrap_or_default();
            let response = format!(
                "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\n{}content-length: {}\r\nconnection: close\r\n\r\n{}",
                retry_header,
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write retry response");
            stream.flush().await.expect("flush retry response");
            continue;
        }

        let sse = concat!(
            "event: message\r\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\r\n\r\n",
            "event: message\r\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"recovered after retry\"}}\r\n\r\n",
            "event: message\r\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\r\n\r\n",
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
            .expect("write retry success response");
        stream.flush().await.expect("flush retry success response");
        break;
    }
}

async fn run_retry_then_terminal_http_response_server(listener: TcpListener, status_line: &'static str, body: &'static str) {
    let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
        .accept()
        .await
        .expect("accept retry terminal request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");

    let response = format!(
        "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        status_line,
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write retry terminal response");
    stream.flush().await.expect("flush retry terminal response");
}

async fn run_retry_then_stream_protocol_failure_server(listener: TcpListener) {
    let mut served_retry = false;
    loop {
        let (mut stream, _peer): (tokio::net::TcpStream, SocketAddr) = listener
            .accept()
            .await
            .expect("accept retry protocol request");
        let mut buffer = vec![0_u8; 16 * 1024];
        let _ = stream.read(&mut buffer).await.expect("read request");

        if !served_retry {
            served_retry = true;
            let body = "{\"error\":\"server overloaded\"}";
            let response = format!(
                "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write retryable 503 response");
            stream.flush().await.expect("flush retryable 503 response");
            continue;
        }

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
            .expect("write protocol failure response");
        stream.flush().await.expect("flush protocol failure response");
        break;
    }
}

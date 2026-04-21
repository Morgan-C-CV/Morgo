use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::cost::CostCommand;
use rust_agent::command::types::{Command, CommandResult};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderClient, parse_anthropic_sse_response,
};
use rust_agent::service::observability::ServiceObservabilityTracker;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;

#[test]
fn default_provider_client_uses_production_boundary() {
    let client = ModelProviderClient::default();
    assert!(!client.is_scripted());
}

#[tokio::test]
async fn cost_command_reports_tracked_usage() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let cost_tracker =
        CostTracker::with_default_pricing("default-model".into(), ModelPricing::default());
    cost_tracker.record_model_usage("default-model", 123, 45, 10, 5);

    let app_state = AppState {
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
        cost_tracker,
        service_observability_tracker: ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_session_id: "cost-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    let result = CostCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/cost"),
            &app_state,
        )
        .await
        .expect("cost command should succeed");

    let CommandResult::Message(text) = result else {
        panic!("expected cost command message");
    };
    assert!(text.contains("Session cost summary"));
    assert!(text.contains("requests: 1"));
    assert!(text.contains("input_tokens: 123"));
    assert!(text.contains("output_tokens: 45"));
    assert!(text.contains("cache_creation_input_tokens: 10"));
    assert!(text.contains("cache_read_input_tokens: 5"));
    assert!(text.contains("estimated_cost_usd:"));
    assert!(text.contains("model default-model -> requests: 1"));
}

#[test]
fn service_observability_tracker_counts_failures_and_compact_hits() {
    let tracker = ServiceObservabilityTracker::default();
    tracker.record_service_failure(&rust_agent::core::events::ServiceFailureNotice {
        service_failure_code: rust_agent::core::events::ServiceFailureCode::ApiProviderHttp5xx,
        provider_kind: Some("anthropic".into()),
        status_code: Some(503),
        retryable: true,
        surface_visible: true,
    });
    tracker.record_service_failure(&rust_agent::core::events::ServiceFailureNotice {
        service_failure_code: rust_agent::core::events::ServiceFailureCode::ApiStreamTerminal,
        provider_kind: None,
        status_code: None,
        retryable: false,
        surface_visible: true,
    });
    tracker.record_compact_recovery_hit(
        &rust_agent::service::compact::CompactPlanKind::ReactiveCompact,
    );
    tracker.record_compact_recovery_hit(&rust_agent::service::compact::CompactPlanKind::Exhausted);

    tracker.record_api_client_error(
        "anthropic",
        &rust_agent::service::api::errors::ApiError::http_status(
            503,
            "provider request failed with status 503",
        ),
    );
    tracker.record_api_client_error(
        "anthropic",
        &rust_agent::service::api::errors::ApiError::timeout("provider request timed out"),
    );
    tracker.record_mcp_server_failure("filesystem", "list_tools");

    let snapshot = tracker.snapshot();
    assert_eq!(snapshot.service_failures_total, 2);
    assert_eq!(
        snapshot.by_failure_code.get("api_provider_http_5xx"),
        Some(&1)
    );
    assert_eq!(
        snapshot.by_failure_code.get("api_stream_terminal"),
        Some(&1)
    );
    assert_eq!(snapshot.retryable_count, 1);
    assert_eq!(snapshot.terminal_count, 1);
    assert_eq!(snapshot.by_provider_kind.get("anthropic"), Some(&1));
    assert_eq!(
        snapshot.compact_recovery_hits.get("reactive_compact"),
        Some(&1)
    );
    assert!(!snapshot.compact_recovery_hits.contains_key("exhausted"));
    assert_eq!(snapshot.api_errors_by_kind.get("http_status"), Some(&1));
    assert_eq!(snapshot.api_errors_by_kind.get("timeout"), Some(&1));
    assert_eq!(snapshot.api_errors_by_provider.get("anthropic"), Some(&2));
    assert_eq!(snapshot.api_errors_by_status.get("503"), Some(&1));
    assert_eq!(snapshot.mcp_failures_by_kind.get("list_tools"), Some(&1));
    assert_eq!(snapshot.mcp_failures_by_server.get("filesystem"), Some(&1));
    assert_eq!(snapshot.recent_events.len(), 6);
    assert_eq!(snapshot.recent_events[0].category, "service_failure");
    assert_eq!(snapshot.recent_events[2].category, "compact_recovery");
    assert_eq!(snapshot.recent_events[3].category, "api_client_error");
    assert_eq!(snapshot.recent_events[5].category, "mcp_server_failure");
}

#[test]
fn parsed_usage_event_can_be_recorded_into_cost_tracker() {
    let cost_tracker =
        CostTracker::with_default_pricing("claude-test".into(), ModelPricing::default());
    let body = concat!(
        "event: message\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":50}}}\n\n",
        "event: message\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":12,\"cache_creation_input_tokens\":3,\"cache_read_input_tokens\":1}}\n\n"
    );

    let events = parse_anthropic_sse_response("anthropic", body, "claude-test")
        .expect("usage SSE should parse");
    for event in events {
        if let rust_agent::service::api::streaming::StreamEvent::Usage(usage) = event {
            cost_tracker.record_model_usage(
                &usage.model,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
            );
        }
    }

    let report = cost_tracker.format_report();
    assert!(report.contains("model claude-test -> requests: 1"));
    assert!(report.contains("input_tokens: 50"));
    assert!(report.contains("output_tokens: 12"));
    assert!(report.contains("cache_creation_input_tokens: 3"));
    assert!(report.contains("cache_read_input_tokens: 1"));
}

#[test]
fn latest_usage_wins_without_double_counting() {
    let cost_tracker =
        CostTracker::with_default_pricing("claude-test".into(), ModelPricing::default());
    let body = concat!(
        "event: message\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"prompt_tokens\":40}}}\n\n",
        "event: message\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"usage\":{\"completion_tokens\":5}}}\n\n",
        "event: message\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"usage\":{\"completion_tokens\":7}}}\n\n",
        "event: message\n",
        "data: {\"type\":\"message_stop\"}\n\n"
    );

    let events = parse_anthropic_sse_response("anthropic", body, "claude-test")
        .expect("usage SSE should parse");
    for event in events {
        if let rust_agent::service::api::streaming::StreamEvent::Usage(usage) = event {
            cost_tracker.record_model_usage(
                &usage.model,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
            );
        }
    }

    let snapshot = cost_tracker.snapshot();
    assert_eq!(snapshot.requests, 1);
    assert_eq!(snapshot.input_tokens, 40);
    assert_eq!(snapshot.output_tokens, 7);
}

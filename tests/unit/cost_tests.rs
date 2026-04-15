use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::cost::CostCommand;
use rust_agent::command::types::{Command, CommandResult};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::service::api::client::{ModelPricing, ModelProviderClient, parse_sse_response};
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
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "cost-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
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
fn parsed_usage_event_can_be_recorded_into_cost_tracker() {
    let cost_tracker =
        CostTracker::with_default_pricing("claude-test".into(), ModelPricing::default());
    let body = concat!(
        "event: message\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":50}}}\n\n",
        "event: message\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":12,\"cache_creation_input_tokens\":3,\"cache_read_input_tokens\":1}}\n\n"
    );

    let events = parse_sse_response(body, "claude-test").expect("usage SSE should parse");
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
    assert!(report.contains("model claude-test -> requests: 2"));
    assert!(report.contains("input_tokens: 50"));
    assert!(report.contains("output_tokens: 12"));
    assert!(report.contains("cache_creation_input_tokens: 3"));
    assert!(report.contains("cache_read_input_tokens: 1"));
}

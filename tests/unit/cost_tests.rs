use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::cost::CostCommand;
use rust_agent::command::types::{Command, CommandResult};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::service::api::client::AnthropicClient;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;

#[test]
fn default_anthropic_client_uses_production_boundary() {
    let client = AnthropicClient::default();
    assert!(!client.is_scripted());
}

#[tokio::test]
async fn cost_command_reports_tracked_usage() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let cost_tracker = CostTracker::default();
    cost_tracker.record_model_usage("claude-sonnet-4-6", 123, 45, 10, 5);

    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context,
        cost_tracker,
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
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
    assert!(text.contains("model claude-sonnet-4-6 -> requests: 1"));
}

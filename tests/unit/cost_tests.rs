use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::cost::CostCommand;
use rust_agent::command::types::{Command, CommandResult};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;

#[tokio::test]
async fn cost_command_reports_tracked_usage() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    let cost_tracker = CostTracker::default();
    cost_tracker.record_request(123, 45);

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

    assert_eq!(
        result,
        CommandResult::Message(
            "Session cost summary\nrequests: 1\ninput_tokens: 123\noutput_tokens: 45\nstatus: partial accounting"
                .into()
        )
    );
}

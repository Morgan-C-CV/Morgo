use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct UMCommand;

#[async_trait]
impl Command for UMCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "UM".into(),
            description: "Toggle shared step memory for verification-first boss flows".into(),
            source: CommandSource::Builtin,
            category: "system".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["um".into()],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let subcommand = input.command_args.split_whitespace().next().unwrap_or("");
        let Some(coordinator) = app_state.boss_coordinator.as_ref() else {
            return Ok(CommandResult::Message(
                "Shared step memory coordinator is unavailable.".into(),
            ));
        };

        let message = match subcommand {
            "on" => {
                coordinator.set_shared_memory_enabled(true).await;
                "Shared step memory enabled for this session.".to_string()
            }
            "off" => {
                coordinator.set_shared_memory_enabled(false).await;
                "Shared step memory disabled for this session.".to_string()
            }
            "status" => {
                let enabled = coordinator.shared_memory_enabled().await;
                format!(
                    "Shared step memory status: {}",
                    if enabled { "enabled" } else { "disabled" }
                )
            }
            _ => usage(),
        };

        Ok(CommandResult::Message(message))
    }
}

fn usage() -> String {
    "usage: /UM <subcommand>\n  on       enable shared step memory\n  off      disable shared step memory\n  status   show current shared step memory status".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
    use crate::core::boss::BossCoordinator;
    use crate::cost::tracker::CostTracker;
    use crate::interaction::envelope::NormalizedInput;
    use crate::interaction::dispatcher::NotificationDispatcher;
    use crate::interaction::telegram::gateway::TelegramGateway;
    use crate::security::audit::AuditLog;
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use crate::state::app_state::ActiveModelProviderSummary;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::AtomicU64;
    use tokio_util::sync::CancellationToken;

    fn test_app_state(coordinator: Option<Arc<BossCoordinator>>) -> AppState {
        AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: crate::state::app_state::RuntimeRole::Coordinator,
            worker_role: None,
            permission_context: ToolPermissionContext::new(PermissionMode::Default),
            command_registry: None,
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(
                TelegramGateway::default(),
            ),
            audit_log: Arc::new(Mutex::new(AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                crate::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "default-provider".into(),
                protocol: "MessagesApi".into(),
                compatibility_profile: "MessagesApi".into(),
                base_url_host: "localhost".into(),
                model: "default-model".into(),
                auth_status: "none".into(),
            },
            active_session_id: "session-um-test".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(AtomicU64::new(0)),
            cancellation_token: CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: coordinator,
            remote_actor_store: None,
        }
    }

    #[tokio::test]
    async fn um_on_off_status_round_trip_shared_memory_flag() {
        let coordinator = Arc::new(BossCoordinator::new());
        let app_state = test_app_state(Some(coordinator.clone()));
        let command = UMCommand;

        let on_result = command
            .execute(
                &NormalizedInput::from_raw(InteractionSurface::Cli, "/UM on"),
                &app_state,
            )
            .await
            .expect("execute /UM on");
        assert_eq!(
            on_result.to_plain_text().as_deref(),
            Some("Shared step memory enabled for this session.")
        );
        assert!(coordinator.shared_memory_enabled().await);

        let status_result = command
            .execute(
                &NormalizedInput::from_raw(InteractionSurface::Cli, "/UM status"),
                &app_state,
            )
            .await
            .expect("execute /UM status");
        assert_eq!(
            status_result.to_plain_text().as_deref(),
            Some("Shared step memory status: enabled")
        );

        let off_result = command
            .execute(
                &NormalizedInput::from_raw(InteractionSurface::Cli, "/UM off"),
                &app_state,
            )
            .await
            .expect("execute /UM off");
        assert_eq!(
            off_result.to_plain_text().as_deref(),
            Some("Shared step memory disabled for this session.")
        );
        assert!(!coordinator.shared_memory_enabled().await);
    }

    #[tokio::test]
    async fn um_status_without_coordinator_reports_unavailable() {
        let app_state = test_app_state(None);
        let command = UMCommand;

        let result = command
            .execute(
                &NormalizedInput::from_raw(InteractionSurface::Cli, "/UM status"),
                &app_state,
            )
            .await
            .expect("execute /UM status without coordinator");
        assert_eq!(
            result.to_plain_text().as_deref(),
            Some("Shared step memory coordinator is unavailable.")
        );
    }
}

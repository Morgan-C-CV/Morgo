use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use crate::bootstrap::config_root::resolve_config_root;
use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::core::boss::BossCoordinator;
use crate::core::boss_state::{BossControlRequest, BossControlResponse, BossStage};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct BossCommand;

#[async_trait]
impl Command for BossCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "boss".into(),
            description: "Start, inspect, report, resume, or stop the boss workflow".into(),
            source: CommandSource::Builtin,
            category: "orchestration".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::CliOnly,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: true,
            immediate: true,
            is_sensitive: true,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let Some(boss) = app_state.boss_coordinator.as_ref() else {
            return Ok(CommandResult::Message(
                "Boss runtime is unavailable in this session.".into(),
            ));
        };

        let args = input.command_args.trim();
        let subcommand = args.split_whitespace().next().unwrap_or_default();

        match subcommand {
            "" => report_or_help(boss, app_state).await,
            "start" => start_or_status(boss, app_state, args).await,
            "status" | "report" => report(boss, app_state).await,
            "resume" => resume(boss, app_state).await,
            "stop" => stop(boss, app_state).await,
            "approve" => approve(boss, app_state, args).await,
            "doc" | "documentation" => documentation(boss, app_state, args).await,
            other => {
                let objective = if other == "start" {
                    args.strip_prefix("start").unwrap_or("").trim()
                } else {
                    args
                };
                start_with_objective(boss, app_state, objective).await
            }
        }
    }
}

async fn start_or_status(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
    args: &str,
) -> anyhow::Result<CommandResult> {
    let objective = args.strip_prefix("start").unwrap_or("").trim();
    if objective.is_empty() {
        return report_or_help(boss, app_state).await;
    }
    start_with_objective(boss, app_state, objective).await
}

async fn start_with_objective(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
    objective: &str,
) -> anyhow::Result<CommandResult> {
    let objective = objective.trim();
    if objective.is_empty() {
        return Ok(CommandResult::Message(
            "Usage: /boss <objective> | /boss start <objective>".into(),
        ));
    }

    if boss.has_active_run().await && boss.get_stage().await != BossStage::Completed {
        return Ok(CommandResult::Message(
            "A boss run is already active. Use /boss report or /boss stop first.".into(),
        ));
    }

    let cwd = app_state.current_working_directory();
    let plan_path = boss_plan_path(&cwd);
    boss.configure_planning_file(&plan_path).await;
    boss.bind_app_state(Arc::new(app_state.clone())).await;
    boss.seed_documentation_plan_for_task(objective).await;
    let plan_id = boss.current_run_id().await;
    boss.persist_current_plan().await?;
    let draft_spec = boss
        .draft_spec_with_a(&Arc::new(app_state.clone()), objective)
        .await
        .unwrap_or_else(|_| objective.to_string());
    boss.finalize_documentation_loop(&draft_spec, "", "", &draft_spec, "")
        .await?;
    Ok(CommandResult::Message(format!(
        "Boss plan started.\n- plan_id: {plan_id}\n- plan_path: {}\n- objective: {objective}\n- stage: waiting_for_approval\n- next: /boss approve Y  or  /boss approve <feedback>\n\n{}",
        plan_path.display(),
        boss_snapshot_message(boss, app_state).await?
    )))
}

async fn report(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
) -> anyhow::Result<CommandResult> {
    let Some(task_manager) = app_state.permission_context.task_manager.as_ref() else {
        return Ok(CommandResult::Message(
            "Boss report unavailable: task manager not attached.".into(),
        ));
    };
    let payload = boss.report_progress(task_manager).await?;
    Ok(CommandResult::Message(payload.format_report()))
}

async fn resume(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
) -> anyhow::Result<CommandResult> {
    let cwd = app_state.current_working_directory();
    let plan_path = boss_plan_path(&cwd);
    let path = Path::new(&plan_path);
    if path.exists() {
        boss.bind_app_state(Arc::new(app_state.clone())).await;
        boss.restore_or_init_in_place(path, &Arc::new(app_state.clone()))
            .await?;
    } else {
        return Ok(CommandResult::Message(
            "No boss plan file found. Use /boss <objective> to start one.".into(),
        ));
    }
    let coordinator = boss;
    let payload = if let Some(task_manager) = app_state.permission_context.task_manager.as_ref() {
        coordinator.report_progress(task_manager).await?.format_report()
    } else {
        "Boss resumed, but no task manager is attached.".into()
    };
    Ok(CommandResult::Message(payload))
}

async fn stop(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
) -> anyhow::Result<CommandResult> {
    let Some(task_manager) = app_state.permission_context.task_manager.as_ref() else {
        return Ok(CommandResult::Message(
            "Boss stop unavailable: task manager not attached.".into(),
        ));
    };
    let outcome = boss
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: app_state.active_session_id.clone(),
                deadline_ms: 500,
            },
            task_manager,
            &app_state.notification_dispatcher,
        )
        .await?;
    match outcome {
        BossControlResponse::Stop(stop) => Ok(CommandResult::Message(format!(
            "Boss stopped.\n- killed: {}\n- stages: {:?}",
            stop.killed_task_ids.join(", "),
            stop.stages
        ))),
        _ => Ok(CommandResult::Message("Unexpected boss stop response.".into())),
    }
}

async fn approve(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
    args: &str,
) -> anyhow::Result<CommandResult> {
    let payload = args
        .strip_prefix("approve")
        .unwrap_or(args)
        .trim();
    let approved_input = if payload.is_empty() {
        "Y"
    } else {
        payload
    };
    let approved = boss.handle_user_approval(approved_input).await?;
    if approved {
        let _ = boss.advance_plan(&Arc::new(app_state.clone())).await;
    }
    Ok(CommandResult::Message(if approved {
        "Boss approval accepted.".into()
    } else {
        "Boss approval rejected; documentation loop reopened.".into()
    }))
}

async fn documentation(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
    args: &str,
) -> anyhow::Result<CommandResult> {
    let draft = args
        .strip_prefix("documentation")
        .or_else(|| args.strip_prefix("doc"))
        .unwrap_or(args)
        .trim();
    let draft = if draft.is_empty() { "" } else { draft };
    boss.bind_app_state(Arc::new(app_state.clone())).await;
    boss.finalize_documentation_loop(draft, "", "", draft, "").await?;
    let payload = if let Some(task_manager) = app_state.permission_context.task_manager.as_ref() {
        boss.report_progress(task_manager).await?.format_report()
    } else {
        "Documentation finalized.".into()
    };
    Ok(CommandResult::Message(payload))
}

async fn boss_snapshot_message(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
) -> anyhow::Result<String> {
    let Some(task_manager) = app_state.permission_context.task_manager.as_ref() else {
        return Ok("No task manager attached.".into());
    };
    Ok(boss.report_progress(task_manager).await?.format_report())
}

async fn report_or_help(
    boss: &Arc<BossCoordinator>,
    app_state: &AppState,
) -> anyhow::Result<CommandResult> {
    if boss.has_loaded_plan().await {
        report(boss, app_state).await
    } else {
        Ok(CommandResult::Message(render_boss_help()))
    }
}

fn boss_plan_path(cwd: &Path) -> std::path::PathBuf {
    match resolve_config_root(cwd) {
        Ok(root) => BossCoordinator::default_plan_path(&root),
        Err(_) => cwd.join(".morgo").join("boss").join("planning.json"),
    }
}

fn render_boss_help() -> String {
    [
        "Usage: /boss <objective>",
        "       /boss start <objective>",
        "       /boss status",
        "       /boss report",
        "       /boss resume",
        "       /boss stop",
        "       /boss approve [Y|feedback]",
        "       /boss doc <spec>",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
    use crate::command::builtin::register_builtin_commands;
    use crate::command::coding::register_coding_commands;
    use crate::command::registry::CommandRegistry;
    use crate::core::boss::BossCoordinator;
    use crate::cost::tracker::CostTracker;
    use crate::interaction::dispatcher::NotificationDispatcher;
    use crate::interaction::telegram::gateway::TelegramGateway;
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::app_state::{
        ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
    };
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use crate::task::manager::TaskManager;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    fn test_app_state() -> AppState {
        let boss = Arc::new(BossCoordinator::new());
        let task_manager = Arc::new(TaskManager::new_with_output_root(std::env::temp_dir()));
        let dispatcher =
            NotificationDispatcher::new(TelegramGateway::default()).with_boss_coordinator(
                boss.clone(),
            );
        AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context: ToolPermissionContext::new(PermissionMode::Default)
                .with_task_manager(task_manager)
                .with_active_session_id("test-session")
                .with_active_surface(InteractionSurface::Cli)
                .with_notification_dispatcher(dispatcher.clone())
                .with_boss_coordinator(boss.clone()),
            command_registry: Some(Arc::new(register_coding_commands(
                register_builtin_commands(CommandRegistry::new()),
            ))),
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: dispatcher,
            audit_log: Arc::new(Mutex::new(crate::security::audit::AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "test-provider".into(),
                protocol: "MessagesApi".into(),
                compatibility_profile: "MessagesApi".into(),
                base_url_host: "localhost".into(),
                model: "test-model".into(),
                auth_status: "unset".into(),
            },
            active_session_id: "test-session".into(),
            session_store: None,
            session: Some(crate::history::session::SessionSnapshot {
                session_id: crate::history::session::SessionId("test-session".into()),
                surface: InteractionSurface::Cli,
                session_mode: SessionMode::Interactive,
                cwd: temp_cwd().display().to_string(),
                last_turn_at: None,
                prompt_seed: None,
            }),
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(AtomicU64::new(0)),
            cancellation_token: CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: Some(boss),
            remote_actor_store: None,
        }
    }

    fn temp_cwd() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "boss-command-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("create temp cwd");
        path
    }

    #[tokio::test]
    async fn boss_start_persists_and_reports() {
        let command = BossCommand;
        let app_state = test_app_state();
        let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/boss write a report");
        let result = command
            .execute(&input, &app_state)
            .await
            .expect("boss command should execute");
        let text = result.to_plain_text().expect("message");
        assert!(text.contains("Boss plan started."));
        assert!(text.contains("write a report"));
        assert!(text.contains("stage="));
    }

    #[tokio::test]
    async fn boss_status_reports_without_objective() {
        let command = BossCommand;
        let app_state = test_app_state();
        let input = NormalizedInput::from_raw(InteractionSurface::Cli, "/boss status");
        let result = command
            .execute(&input, &app_state)
            .await
            .expect("boss status should execute");
        let text = result.to_plain_text().expect("message");
        assert!(text.contains("stage="));
    }

    #[tokio::test]
    async fn boss_second_start_is_blocked_while_active() {
        let command = BossCommand;
        let app_state = test_app_state();
        let first = NormalizedInput::from_raw(InteractionSurface::Cli, "/boss write a report");
        command
            .execute(&first, &app_state)
            .await
            .expect("first boss start should execute");

        let second = NormalizedInput::from_raw(InteractionSurface::Cli, "/boss ship it");
        let result = command
            .execute(&second, &app_state)
            .await
            .expect("second boss start should execute");
        let text = result.to_plain_text().expect("message");
        assert!(text.contains("already active"));
    }
}

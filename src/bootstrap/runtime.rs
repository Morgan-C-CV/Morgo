use std::io::{self, BufRead};
use std::sync::Arc;

use clap::Parser;

use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
use crate::command::builtin::{
    clear::ClearCommand, compact::CompactCommand, config::ConfigCommand, cost::CostCommand,
    help::HelpCommand, plan::PlanCommand, resume::ResumeCommand, session::SessionCommand,
    status::StatusCommand, tasks::TasksCommand,
};
use crate::command::registry::CommandRegistry;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::cost::tracker::CostTracker;
use crate::history::resume::{RestoreRequest, RestoreSource, RestoredSession};
use crate::history::session::{
    FileBackedSessionStore, SessionHistory, SessionId, SessionRestoreRequest, SessionSnapshot,
    SessionStore,
};
use crate::history::transcript::Transcript;
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry};
use crate::interaction::cli::renderer::{render_output, render_turn_output};
use crate::interaction::cli::repl::handle_cli_input;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::router::CommandRouter;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::security::authorizer::DefaultSurfaceAuthorizer;
use crate::service::api::client::{ModelProviderClient, ModelProviderConfig, ModelPricing};
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::{AppState, RuntimeRole};
use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::task::list_manager::TaskListManager;
use crate::task::manager::TaskManager;
use crate::tool::builtin::{
    agent::AgentTool, ask_user::AskUserQuestionTool, bash::BashTool,
    enter_plan_mode::EnterPlanModeTool, exit_plan_mode::ExitPlanModeTool,
    file_edit::FileEditTool, file_read::FileReadTool, file_write::FileWriteTool,
    glob::GlobTool, grep::GrepTool, mcp::McpTool, notebook_edit::NotebookEditTool,
    send_message::SendMessageTool, skill::SkillTool, task_create::TaskCreateTool,
    task_get::TaskGetTool, task_list::TaskListTool, task_output::TaskOutputTool,
    task_stop::TaskStopTool, task_update::TaskUpdateTool, todo_write::TodoWriteTool,
    tool_search::ToolSearchTool, web_fetch::WebFetchTool, web_search::WebSearchTool,
};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone, Parser)]
#[command(name = "rust-agent", about = "Rust agent runtime")]
pub struct BootstrapCli {
    #[arg(long)]
    pub print: Option<String>,
    #[arg(long)]
    pub interactive: bool,
    #[arg(long)]
    pub init_only: bool,
    #[arg(long)]
    pub continue_session: bool,
    #[arg(long)]
    pub resume: Option<String>,
    #[arg(long, default_value_t = false)]
    pub trace_startup: bool,
    #[arg(long, default_value_t = false)]
    pub show_tools: bool,
    #[arg(long, default_value = "cli")]
    pub surface: String,
}

#[derive(Clone)]
pub struct RuntimeBootstrap {
    cli: BootstrapCli,
    session_store: Arc<dyn SessionStore>,
}

impl std::fmt::Debug for RuntimeBootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeBootstrap")
            .field("cli", &self.cli)
            .finish()
    }
}

impl RuntimeBootstrap {
    pub fn from_cli(cli: BootstrapCli) -> Self {
        Self {
            cli,
            session_store: Arc::new(FileBackedSessionStore::default()),
        }
    }

    pub fn with_session_store(mut self, session_store: Arc<dyn SessionStore>) -> Self {
        self.session_store = session_store;
        self
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let detected_surface = self.detect_surface();
        let detected_mode = self.detect_session_mode();
        let mut state =
            BootstrapState::new(detected_surface, detected_mode, self.cli.trace_startup);

        state.record_phase(BootstrapPhase::DetectSurface);
        state.record_phase(BootstrapPhase::InjectSessionMetadata);
        state.record_phase(BootstrapPhase::ResolvePermissions);

        let task_manager = Arc::new(TaskManager::default());
        let hook_registry = HookRegistry::default();
        let _ = run_hook(&hook_registry, HookEvent::SessionStart);

        state.record_phase(BootstrapPhase::BuildToolContext);
        let tool_inventory = self.build_tool_registry();

        state.record_phase(BootstrapPhase::AssembleTools);
        let setup = SetupContext::detect();
        let _ = run_hook(&hook_registry, HookEvent::Setup);
        state.record_phase(BootstrapPhase::Setup);
        state.current_cwd = setup.working_directory.clone();

        let restore_request = self.restore_request();
        let restored_session = self.restore_session(&state, restore_request.as_ref());
        if let Some(restored) = &restored_session {
            state.surface = restored.snapshot.surface;
            state.session_mode = restored.snapshot.session_mode;
            let (client_type, session_source) = match restored.snapshot.surface {
                InteractionSurface::Cli => (
                    crate::bootstrap::ClientType::Cli,
                    crate::bootstrap::SessionSource::LocalCli,
                ),
                InteractionSurface::Telegram => (
                    crate::bootstrap::ClientType::Bot,
                    crate::bootstrap::SessionSource::Telegram,
                ),
                InteractionSurface::Remote => (
                    crate::bootstrap::ClientType::RemoteControl,
                    crate::bootstrap::SessionSource::RemoteControl,
                ),
            };
            state.client_type = client_type;
            state.session_source = session_source;
        }
        let active_session_id = restored_session
            .as_ref()
            .map(|session| session.snapshot.session_id.0.clone())
            .unwrap_or_else(|| "local-session".into());
        let session_snapshot = restored_session
            .as_ref()
            .map(|session| session.snapshot.clone());
        let session_history = restored_session
            .as_ref()
            .map(|session| session.history.clone());
        if session_snapshot.is_none() {
            self.session_store.save(
                SessionSnapshot {
                    session_id: SessionId(active_session_id.clone()),
                    surface: state.surface,
                    session_mode: state.session_mode,
                    cwd: state.current_cwd.display().to_string(),
                    last_turn_at: None,
                    prompt_seed: None,
                },
                SessionHistory::default(),
            );
        }
        let session_snapshot = session_snapshot.or_else(|| {
            Some(SessionSnapshot {
                session_id: SessionId(active_session_id.clone()),
                surface: state.surface,
                session_mode: state.session_mode,
                cwd: state.current_cwd.display().to_string(),
                last_turn_at: None,
                prompt_seed: None,
            })
        });
        let session_history = session_history.or_else(|| Some(SessionHistory::default()));
        let task_list_session_id = SessionId(active_session_id.clone());
        let task_list_snapshot = self.session_store.load_task_list(&task_list_session_id);
        let task_list_manager = Arc::new(
            task_list_snapshot
                .map(TaskListManager::from_snapshot)
                .unwrap_or_default()
                .with_persistence(self.session_store.clone(), task_list_session_id),
        );
        let coordinator_tools = tool_inventory.assemble_for_role(RuntimeRole::Coordinator);
        let permission_context = ToolPermissionContext::new(if self.cli.init_only {
            PermissionMode::Plan
        } else {
            PermissionMode::Default
        })
        .with_task_manager(task_manager.clone())
        .with_task_list_manager(task_list_manager.clone())
        .with_active_session_id(active_session_id.clone())
        .with_deferred_tools(true)
        .with_interactive_tools(true)
        .with_inherited_tool_registry(coordinator_tools.clone())
        .with_inherited_hook_registry(hook_registry.clone());

        state.record_phase(BootstrapPhase::InitializeRuntime);
        state.record_phase(BootstrapPhase::AugmentPrompt);
        state.record_phase(BootstrapPhase::GateUserAccess);
        let state = state.finalize();

        let provider_config = self.build_model_provider_config();
        let app_state = AppState {
            surface: state.surface,
            session_mode: state.session_mode,
            client_type: state.client_type,
            session_source: state.session_source,
            runtime_role: RuntimeRole::Coordinator,
            permission_context: permission_context.clone(),
            runtime_tool_registry: Some(coordinator_tools.clone()),
            cost_tracker: CostTracker::with_default_pricing(
                provider_config.model_id.clone(),
                provider_config.pricing.clone(),
            ),
            notification_dispatcher: NotificationDispatcher::new(self.build_telegram_gateway()),
            startup_trace: state
                .phases
                .iter()
                .map(|phase| format!("{phase:?}"))
                .collect(),
            active_session_id,
            session_store: Some(self.session_store.clone()),
            session: session_snapshot,
            history: session_history,
            restored_session,
        };

        if self.cli.show_tools {
            for tool in coordinator_tools.visible_tools(&permission_context) {
                println!("{} - {}", tool.name, tool.description);
            }
            return Ok(());
        }

        if self.cli.trace_startup {
            println!("startup: {}", state.startup_trace());
        }

        if self.cli.init_only {
            println!(
                "initialized {} runtime in {:?} mode",
                self.cli.surface, state.session_mode
            );
            return Ok(());
        }

        let registry = self.build_command_registry();
        let router = CommandRouter::new(registry, Box::new(DefaultSurfaceAuthorizer));
        let tools_prompt = crate::prompt::tools::build_tools_prompt(&coordinator_tools, &permission_context);
        let context_prompt = crate::prompt::context::build_context_prompt(&app_state);
        let system_prompt = crate::prompt::system::build_system_prompt(&app_state);
        let query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry: coordinator_tools,
            api_client: ModelProviderClient::from_config(provider_config),
            compactor: ReactiveCompactor,
            hook_registry,
            agent_id: None,
            system_prompt,
            tools_prompt,
            context_prompt,
        };
        let engine = QueryEngine::new(query_context);

        if let Some(prompt) = &self.cli.print {
            let output = handle_cli_input(&router, &engine, &app_state, prompt.clone()).await?;
            println!("{}", render_turn_output(&output));
            return Ok(());
        }

        if self.cli.continue_session {
            println!(
                "{}",
                render_output(&format!(
                    "continued session {}",
                    app_state.active_session_id
                ))
            );
            return Ok(());
        }

        if let Some(session_id) = &self.cli.resume {
            println!(
                "{}",
                render_output(&format!("resumed session {session_id}"))
            );
            return Ok(());
        }

        if self.cli.interactive {
            for line in io::stdin().lock().lines() {
                let line = line?;
                let output = handle_cli_input(&router, &engine, &app_state, line).await?;
                println!("{}", render_turn_output(&output));
            }
            return Ok(());
        }

        let output = handle_cli_input(&router, &engine, &app_state, "/help").await?;
        println!("{}", render_turn_output(&output));
        Ok(())
    }

    fn detect_surface(&self) -> InteractionSurface {
        match self.cli.surface.as_str() {
            "telegram" => InteractionSurface::Telegram,
            "remote" => InteractionSurface::Remote,
            _ => InteractionSurface::Cli,
        }
    }

    fn detect_session_mode(&self) -> SessionMode {
        if self.cli.init_only {
            SessionMode::InitOnly
        } else if self.cli.print.is_some() {
            SessionMode::Print
        } else if self.cli.interactive {
            SessionMode::Interactive
        } else {
            SessionMode::Headless
        }
    }

    fn build_command_registry(&self) -> CommandRegistry {
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(CostCommand))
            .register(Arc::new(CompactCommand))
            .register(Arc::new(ClearCommand))
            .register(Arc::new(ConfigCommand))
            .register(Arc::new(PlanCommand))
            .register(Arc::new(ResumeCommand))
            .register(Arc::new(SessionCommand))
            .register(Arc::new(StatusCommand))
            .register(Arc::new(TasksCommand))
    }

    fn build_tool_registry(&self) -> ToolRegistry {
        ToolRegistry::new()
            .register(Arc::new(AgentTool))
            .register(Arc::new(AskUserQuestionTool))
            .register(Arc::new(BashTool))
            .register(Arc::new(EnterPlanModeTool))
            .register(Arc::new(ExitPlanModeTool))
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool))
            .register(Arc::new(FileWriteTool))
            .register(Arc::new(GlobTool))
            .register(Arc::new(GrepTool))
            .register(Arc::new(McpTool))
            .register(Arc::new(NotebookEditTool))
            .register(Arc::new(SendMessageTool))
            .register(Arc::new(SkillTool))
            .register(Arc::new(TaskCreateTool))
            .register(Arc::new(TaskGetTool))
            .register(Arc::new(TaskListTool))
            .register(Arc::new(TaskOutputTool))
            .register(Arc::new(TaskStopTool))
            .register(Arc::new(TaskUpdateTool))
            .register(Arc::new(TodoWriteTool))
            .register(Arc::new(ToolSearchTool))
            .register(Arc::new(WebFetchTool))
            .register(Arc::new(WebSearchTool))
    }

    fn build_telegram_gateway(&self) -> TelegramGateway {
        TelegramGateway::default()
    }

    fn build_model_provider_config(&self) -> ModelProviderConfig {
        ModelProviderConfig {
            provider_id: "modelprovider".into(),
            base_url: "http://localhost".into(),
            model_id: "default-model".into(),
            pricing: ModelPricing::default(),
        }
    }

    fn restore_request(&self) -> Option<RestoreRequest> {
        if self.cli.continue_session {
            Some(RestoreRequest {
                source: RestoreSource::ContinueSession,
                session_id: None,
            })
        } else {
            self.cli.resume.as_ref().map(|session_id| RestoreRequest {
                source: RestoreSource::ResumeSession,
                session_id: Some(session_id.clone()),
            })
        }
    }

    fn restore_session(
        &self,
        state: &BootstrapState,
        request: Option<&RestoreRequest>,
    ) -> Option<RestoredSession> {
        let request = request?;
        let store_request = SessionRestoreRequest {
            resume: request.session_id.clone(),
            continue_session: matches!(request.source, RestoreSource::ContinueSession),
        };

        if let Some((snapshot, history)) = self.session_store.load(&store_request) {
            let transcript = Transcript::from(history.clone());
            return Some(RestoredSession {
                snapshot,
                history,
                transcript,
            });
        }

        let session_id = request
            .session_id
            .clone()
            .or_else(|| Some("latest-session".into()))?;
        let snapshot = SessionSnapshot {
            session_id: SessionId(session_id.clone()),
            surface: state.surface,
            session_mode: state.session_mode,
            cwd: state.current_cwd.display().to_string(),
            last_turn_at: None,
            prompt_seed: None,
        };
        let history = SessionHistory::default();
        Some(RestoredSession {
            snapshot,
            history,
            transcript: Transcript::default(),
        })
    }
}

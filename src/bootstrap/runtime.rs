use std::sync::Arc;

use clap::Parser;

use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
use crate::command::builtin::{compact::CompactCommand, cost::CostCommand, help::HelpCommand};
use crate::command::registry::CommandRegistry;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::cost::tracker::CostTracker;
use crate::history::resume::{RestoreRequest, RestoreSource, RestoredSession};
use crate::history::session::{
    InMemorySessionStore, SessionHistory, SessionId, SessionRestoreRequest, SessionSnapshot,
    SessionStore,
};
use crate::history::transcript::Transcript;
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry};
use crate::interaction::cli::renderer::render_output;
use crate::interaction::cli::repl::handle_cli_input;
use crate::interaction::router::CommandRouter;
use crate::security::authorizer::DefaultSurfaceAuthorizer;
use crate::service::api::client::AnthropicClient;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::AppState;
use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::task::manager::TaskManager;
use crate::tool::builtin::{
    agent::AgentTool, bash::BashTool, file_edit::FileEditTool, file_read::FileReadTool,
    glob::GlobTool, grep::GrepTool, tool_search::ToolSearchTool, web_fetch::WebFetchTool,
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
            session_store: Arc::new(InMemorySessionStore::default()),
        }
    }

    pub fn with_session_store(mut self, session_store: Arc<dyn SessionStore>) -> Self {
        self.session_store = session_store;
        self
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut state = BootstrapState::new(
            self.detect_surface(),
            self.detect_session_mode(),
            self.cli.trace_startup,
        );

        state.record_phase(BootstrapPhase::DetectSurface);
        state.record_phase(BootstrapPhase::InjectSessionMetadata);
        state.record_phase(BootstrapPhase::ResolvePermissions);

        let task_manager = Arc::new(TaskManager::default());
        let permission_context = ToolPermissionContext::new(if self.cli.init_only {
            PermissionMode::Plan
        } else {
            PermissionMode::Default
        })
        .with_task_manager(task_manager.clone());

        let hook_registry = HookRegistry::default();
        let _ = run_hook(&hook_registry, HookEvent::SessionStart);

        state.record_phase(BootstrapPhase::BuildToolContext);
        let tool_registry = self.build_tool_registry();

        state.record_phase(BootstrapPhase::AssembleTools);
        let setup = SetupContext::detect();
        let _ = run_hook(&hook_registry, HookEvent::Setup);
        state.record_phase(BootstrapPhase::Setup);
        state.current_cwd = setup.working_directory.clone();

        let restore_request = self.restore_request();
        let restored_session = self.restore_session(&state, restore_request.as_ref());
        let active_session_id = restored_session
            .as_ref()
            .map(|session| session.snapshot.session_id.0.clone())
            .unwrap_or_else(|| "local-session".into());

        state.record_phase(BootstrapPhase::InitializeRuntime);
        state.record_phase(BootstrapPhase::AugmentPrompt);
        state.record_phase(BootstrapPhase::GateUserAccess);
        let state = state.finalize();

        let app_state = AppState {
            surface: state.surface,
            session_mode: state.session_mode,
            client_type: state.client_type,
            session_source: state.session_source,
            permission_context: permission_context.clone(),
            cost_tracker: CostTracker::default(),
            startup_trace: state
                .phases
                .iter()
                .map(|phase| format!("{phase:?}"))
                .collect(),
            active_session_id,
            session: restored_session
                .as_ref()
                .map(|session| session.snapshot.clone()),
            history: restored_session
                .as_ref()
                .map(|session| session.history.clone()),
            restored_session,
        };

        if self.cli.show_tools {
            for tool in tool_registry.visible_tools(&permission_context) {
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
        let query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry,
            api_client: AnthropicClient::default(),
            compactor: ReactiveCompactor,
            hook_registry,
        };
        let engine = QueryEngine::new(query_context);

        let content = if let Some(prompt) = &self.cli.print {
            handle_cli_input(&router, &engine, &app_state, prompt.clone()).await?
        } else if self.cli.continue_session {
            format!("continued session {}", app_state.active_session_id)
        } else if let Some(session_id) = &self.cli.resume {
            format!("resumed session {session_id}")
        } else {
            handle_cli_input(&router, &engine, &app_state, "/help").await?
        };

        println!("{}", render_output(&content));
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
    }

    fn build_tool_registry(&self) -> ToolRegistry {
        ToolRegistry::new()
            .register(Arc::new(AgentTool))
            .register(Arc::new(BashTool))
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool))
            .register(Arc::new(GlobTool))
            .register(Arc::new(GrepTool))
            .register(Arc::new(ToolSearchTool))
            .register(Arc::new(WebFetchTool))
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

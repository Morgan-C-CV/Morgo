use std::sync::Arc;

use clap::Parser;

use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
use crate::command::builtin::{compact::CompactCommand, cost::CostCommand, help::HelpCommand};
use crate::command::registry::CommandRegistry;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::interaction::cli::renderer::render_output;
use crate::interaction::cli::repl::handle_cli_input;
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;
use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::tool::builtin::{
    agent::AgentTool, file_edit::FileEditTool, file_read::FileReadTool, glob::GlobTool,
    grep::GrepTool, tool_search::ToolSearchTool, web_fetch::WebFetchTool,
};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone, Parser)]
#[command(name = "rust-agent", about = "Rust agent runtime scaffold")]
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

#[derive(Debug, Clone)]
pub struct RuntimeBootstrap {
    cli: BootstrapCli,
}

impl RuntimeBootstrap {
    pub fn from_cli(cli: BootstrapCli) -> Self {
        Self { cli }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut state = BootstrapState::new(
            self.detect_surface(),
            self.detect_session_mode(),
            self.cli.trace_startup,
        );

        state.enter_phase(BootstrapPhase::DetectSurface);
        state.enter_phase(BootstrapPhase::InjectSessionMetadata);
        state.enter_phase(BootstrapPhase::ResolvePermissions);

        let permission_context = ToolPermissionContext::new(if self.cli.init_only {
            PermissionMode::Plan
        } else {
            PermissionMode::Default
        });

        state.enter_phase(BootstrapPhase::BuildToolContext);
        let tool_registry = self.build_tool_registry();

        state.enter_phase(BootstrapPhase::AssembleTools);
        let setup = SetupContext::detect();
        state.enter_phase(BootstrapPhase::Setup);
        state.current_cwd = setup.working_directory.clone();

        let app_state = AppState {
            surface: state.surface,
            session_mode: state.session_mode,
            permission_context: permission_context.clone(),
            startup_trace: state
                .phases
                .iter()
                .map(|phase| format!("{phase:?}"))
                .collect(),
        };

        state.enter_phase(BootstrapPhase::InitializeRuntime);
        state.enter_phase(BootstrapPhase::AugmentPrompt);
        state.enter_phase(BootstrapPhase::GateUserAccess);
        state.enter_phase(BootstrapPhase::FinalizeState);

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
        let router = CommandRouter::new(registry);
        let query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry,
        };
        let engine = QueryEngine::new(query_context);

        let content = if let Some(prompt) = &self.cli.print {
            handle_cli_input(&router, &engine, &app_state, prompt.clone()).await?
        } else if self.cli.continue_session {
            "continue mode scaffold".to_string()
        } else if let Some(session_id) = &self.cli.resume {
            format!("resume scaffold for session {session_id}")
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
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool))
            .register(Arc::new(GlobTool))
            .register(Arc::new(GrepTool))
            .register(Arc::new(ToolSearchTool))
            .register(Arc::new(WebFetchTool))
    }
}

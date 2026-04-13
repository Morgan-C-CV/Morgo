use std::io::{self, BufRead};
use std::sync::Arc;
use tokio::sync::RwLock;

use clap::Parser;

use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
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
use crate::hook::registry::{HookEvent, load_hook_registry};
use crate::interaction::cli::renderer::{render_output, render_turn_output};
use crate::interaction::cli::repl::handle_cli_input;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::remote::{RemoteRequest, handle_remote_request, render_remote_response_debug};
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::plan::manager::PlanManager;
use crate::plugins::loader::load_plugins;
use crate::plugins::runtime::{augment_hook_registry_with_plugins, augment_tool_registry_with_plugins};
use crate::plugins::runtime_state::{
    RuntimePluginState, build_runtime_plugin_snapshot, build_turn_engine, build_turn_router,
    hydrate_app_state_from_snapshot,
};
use crate::plugins::types::{PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity, PluginLifecycleState};
use crate::service::api::client::{
    ModelProviderClient, ModelProviderConfig, ModelPricing, ProviderTimeout,
};
use crate::service::api::retry::RetryPolicy;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::service::mcp::config::load_server_configs_with_diagnostics;
use crate::service::mcp::runtime::McpRuntime;
use crate::skills::bundled::bundled_skills;
use crate::skills::loader::SkillLoaderCache;
use crate::skills::registry::SkillRegistry;
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

        state.record_phase(BootstrapPhase::BuildToolContext);

        state.record_phase(BootstrapPhase::AssembleTools);
        let setup = SetupContext::detect();
        let base_hook_registry = load_hook_registry(&setup.working_directory);
        let plugin_load_result = Arc::new(load_plugins(&setup.working_directory));
        let hook_registry = augment_hook_registry_with_plugins(base_hook_registry, plugin_load_result.as_ref());
        let _ = run_hook(&hook_registry, HookEvent::SessionStart);
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
                .with_persistence(self.session_store.clone(), task_list_session_id.clone()),
        );
        let plan_state = self.session_store.load_plan_state(&task_list_session_id);
        let plan_manager = Arc::new(
            plan_state
                .map(PlanManager::from_state)
                .unwrap_or_default()
                .with_persistence(self.session_store.clone(), task_list_session_id),
        );
        let mut discovered_skills = bundled_skills();
        let mut skill_loader_cache = SkillLoaderCache::default();
        let (loaded_skills, _) = skill_loader_cache
            .load_or_reload(&state.current_cwd)
            .unwrap_or_default();
        discovered_skills.extend(loaded_skills.skills);
        let skill_registry = Arc::new(SkillRegistry::new(discovered_skills));
        let mcp_config_result = load_server_configs_with_diagnostics(&state.current_cwd);
        let mcp_runtime = Arc::new(McpRuntime::new_with_config_result(
            Arc::new(crate::service::mcp::client::RoutingMcpClient::default()),
            mcp_config_result,
        ));
        let tool_inventory = self.build_tool_registry();
        let (tool_inventory, plugin_tool_diagnostics) =
            augment_tool_registry_with_plugins(tool_inventory, plugin_load_result.as_ref());
        let plugin_load_result = Arc::new(crate::plugins::types::PluginLoadResult {
            root: plugin_load_result.root.clone(),
            source: plugin_load_result.source,
            plugins: plugin_load_result
                .plugins
                .iter()
                .cloned()
                .map(|mut plugin| {
                    if plugin_tool_diagnostics.iter().any(|diagnostic| {
                        diagnostic.plugin_name.as_deref() == Some(plugin.name.as_str())
                            && diagnostic.severity == PluginDiagnosticSeverity::Error
                    }) {
                        plugin.lifecycle_state = PluginLifecycleState::Error;
                        plugin.apply_status = crate::plugins::types::PluginApplyStatus::ApplyFailed;
                        plugin.activation.commands = 0;
                        plugin.activation.tools = 0;
                        plugin.activation.hooks = 0;
                    }
                    plugin
                })
                .collect::<Vec<PluginDefinition>>(),
            diagnostics: plugin_load_result
                .diagnostics
                .iter()
                .cloned()
                .chain(plugin_tool_diagnostics)
                .collect::<Vec<PluginDiagnostic>>(),
            orphaned_governance_entries: plugin_load_result.orphaned_governance_entries.clone(),
        });
        let coordinator_tools = tool_inventory.assemble_for_role(RuntimeRole::Coordinator);
        let permission_context = ToolPermissionContext::new(if self.cli.init_only {
            PermissionMode::Plan
        } else {
            PermissionMode::Default
        })
        .with_task_manager(task_manager.clone())
        .with_task_list_manager(task_list_manager.clone())
        .with_plan_manager(plan_manager.clone())
        .with_skill_registry(skill_registry.clone())
        .with_mcp_runtime(mcp_runtime.clone())
        .with_active_session_id(active_session_id.clone())
        .with_active_surface(state.surface)
        .with_notification_dispatcher(
            NotificationDispatcher::new(self.build_telegram_gateway())
                .with_hook_registry(hook_registry.clone()),
        )
        .with_deferred_tools(true)
        .with_interactive_tools(true)
        .with_inherited_tool_registry(coordinator_tools.clone())
        .with_inherited_hook_registry(hook_registry.clone());

        state.record_phase(BootstrapPhase::InitializeRuntime);
        state.record_phase(BootstrapPhase::AugmentPrompt);
        state.record_phase(BootstrapPhase::GateUserAccess);
        let state = state.finalize();

        let provider_config = self.build_model_provider_config();
        let mut app_state = AppState {
            surface: state.surface,
            session_mode: state.session_mode,
            client_type: state.client_type,
            session_source: state.session_source,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context: permission_context.clone(),
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(coordinator_tools.clone()))),
            skill_registry: Some(skill_registry.clone()),
            mcp_runtime: Some(mcp_runtime.clone()),
            plugin_load_result: Some(plugin_load_result.clone()),
            cost_tracker: CostTracker::with_default_pricing(
                provider_config.model_id.clone(),
                provider_config.pricing.clone(),
            ),
            notification_dispatcher: permission_context
                .notification_dispatcher
                .clone()
                .unwrap_or_else(|| {
                    NotificationDispatcher::new(self.build_telegram_gateway())
                        .with_hook_registry(hook_registry.clone())
                }),
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
        let initial_snapshot = build_runtime_plugin_snapshot(&app_state);
        let runtime_plugin_state = RuntimePluginState::new(initial_snapshot.clone());
        app_state.permission_context = app_state
            .permission_context
            .clone()
            .with_runtime_plugin_state(runtime_plugin_state);
        hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);

        if self.cli.show_tools {
            for tool in initial_snapshot.tool_registry.visible_tools(&app_state.permission_context) {
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

        let router = build_turn_router(&initial_snapshot);
        let base_query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: ModelProviderClient::from_config(provider_config),
            compactor: ReactiveCompactor,
            hook_registry: initial_snapshot.hook_registry.clone(),
            agent_id: None,
            system_prompt: crate::prompt::system::build_system_prompt(&app_state),
            tools_prompt: crate::prompt::tools::build_tools_prompt(
                &initial_snapshot.tool_registry,
                &app_state.permission_context,
            ),
            context_prompt: crate::prompt::context::build_context_prompt(&app_state),
        };
        let engine = build_turn_engine(&app_state, &initial_snapshot, &QueryEngine::new(base_query_context));

        if let Some(prompt) = &self.cli.print {
            if matches!(app_state.surface, InteractionSurface::Remote) {
                let response = handle_remote_request(
                    &router,
                    &engine,
                    &app_state,
                    RemoteRequest {
                        session_id: app_state.active_session_id.clone(),
                        actor_id: "remote-user".into(),
                        is_authenticated: true,
                        from_trusted_surface: true,
                        raw: prompt.clone(),
                    },
                )
                .await?;
                println!("{}", render_output(&render_remote_response_debug(&response)));
            } else {
                let output = handle_cli_input(&router, &engine, &app_state, prompt.clone()).await?;
                println!("{}", render_turn_output(&output));
            }
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
        let provider_id = std::env::var("RUST_AGENT_PROVIDER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "anthropic".into());
        let base_url = std::env::var("RUST_AGENT_PROVIDER_BASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "http://localhost".into());
        let api_key = std::env::var("RUST_AGENT_PROVIDER_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let model_id = std::env::var("RUST_AGENT_PROVIDER_MODEL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "default-model".into());
        let request_timeout_ms = std::env::var("RUST_AGENT_PROVIDER_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(30_000);
        let max_attempts = std::env::var("RUST_AGENT_PROVIDER_RETRY_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(3);
        let initial_backoff_ms = std::env::var("RUST_AGENT_PROVIDER_RETRY_INITIAL_BACKOFF_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(200);
        let max_backoff_ms = std::env::var("RUST_AGENT_PROVIDER_RETRY_MAX_BACKOFF_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_000);
        ModelProviderConfig {
            provider_id,
            base_url,
            api_key,
            model_id,
            timeout: ProviderTimeout {
                request_timeout_ms,
            },
            retry_policy: RetryPolicy {
                max_attempts,
                initial_backoff_ms,
                max_backoff_ms,
            },
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

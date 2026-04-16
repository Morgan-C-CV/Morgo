use std::io::{self, BufRead};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

use clap::Parser;

use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
use crate::command::registry::CommandRegistry;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::cost::tracker::CostTracker;
use crate::history::resume::{
    ResolvedSessionState, RestoreRequest, RestoreSource, resolve_session_state,
};
use crate::history::session::{FileBackedSessionStore, SessionId, SessionStore};
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry, load_hook_registry};
use crate::interaction::cli::renderer::{
    build_tui_screen, render_document_output, render_document_tui_output, render_output,
    render_tui_screen_output, render_turn_document,
};
use crate::interaction::cli::repl::{CliTurnOutput, handle_cli_input};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::remote::{
    RemoteRequest, handle_remote_request, render_remote_response_debug,
};
use crate::interaction::router::CommandRouter;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::plan::manager::PlanManager;
use crate::plugins::loader::load_plugins;
use crate::plugins::runtime::{
    augment_hook_registry_with_plugins, augment_tool_registry_with_plugins,
};
use crate::plugins::runtime_state::RuntimePluginSnapshot;
use crate::plugins::runtime_state::{
    RuntimePluginState, build_runtime_plugin_snapshot, build_turn_engine, build_turn_router,
    hydrate_app_state_from_snapshot,
};
use crate::plugins::types::{
    PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity, PluginLifecycleState,
};
use crate::security::audit::AuditLog;
use crate::security::authorizer::{AuthDecision, DefaultSurfaceAuthorizer, SurfaceAuthorizer};
use crate::service::api::client::{
    ModelPricing, ModelProviderClient, ModelProviderConfig, ProviderTimeout,
};
use crate::service::api::retry::RetryPolicy;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::service::mcp::config::load_server_configs_with_diagnostics;
use crate::service::mcp::runtime::McpRuntime;
use crate::service::observability::ServiceObservabilityTracker;
use crate::skills::bundled::bundled_skills;
use crate::skills::loader::SkillLoaderCache;
use crate::skills::registry::SkillRegistry;
use crate::state::app_state::{AppState, RuntimeRole};
use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
use crate::state::store::AppStateStore;
use crate::task::list_manager::TaskListManager;
use crate::task::manager::TaskManager;
use crate::tool::builtin::{
    agent::AgentTool, ask_user::AskUserQuestionTool, bash::BashTool,
    enter_plan_mode::EnterPlanModeTool, exit_plan_mode::ExitPlanModeTool, file_edit::FileEditTool,
    file_read::FileReadTool, file_write::FileWriteTool, glob::GlobTool, grep::GrepTool,
    mcp::McpTool, notebook_edit::NotebookEditTool, send_message::SendMessageTool, skill::SkillTool,
    task_create::TaskCreateTool, task_get::TaskGetTool, task_list::TaskListTool,
    task_output::TaskOutputTool, task_stop::TaskStopTool, task_update::TaskUpdateTool,
    todo_write::TodoWriteTool, tool_search::ToolSearchTool, web_fetch::WebFetchTool,
    web_search::WebSearchTool,
};
use crate::tool::registry::{ToolAssemblyContext, ToolRegistry};

pub fn is_tui_exit_input(input: &str) -> bool {
    matches!(input.trim(), "/exit" | "exit" | "quit")
}

pub fn tui_clear_screen_prefix() -> &'static str {
    "\x1B[2J\x1B[H"
}

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
    #[arg(long, default_value_t = false)]
    pub tui: bool,
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

pub struct RuntimeInitializeBundle {
    pub hook_registry: HookRegistry,
    pub notification_dispatcher: NotificationDispatcher,
    pub skill_registry: Arc<SkillRegistry>,
    pub mcp_runtime: Arc<McpRuntime>,
    pub plugin_load_result: Arc<crate::plugins::types::PluginLoadResult>,
    pub coordinator_tools: ToolRegistry,
    pub runtime_tool_registry: Arc<RwLock<ToolRegistry>>,
    pub command_registry: Arc<CommandRegistry>,
    pub provider_config: ModelProviderConfig,
    pub api_client: ModelProviderClient,
    pub compactor: ReactiveCompactor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAugmentation {
    pub system_prompt: String,
    pub tools_prompt: String,
    pub context_prompt: String,
    pub metadata: PromptAugmentationMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAugmentationMetadata {
    pub active_session_id: String,
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub visible_tool_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAccessDecision {
    pub allowed: bool,
    pub reason: Option<String>,
}

pub struct FinalizedRuntime {
    pub app_state: AppState,
    #[allow(dead_code)]
    pub store: AppStateStore<AppState>,
    pub snapshot: RuntimePluginSnapshot,
    pub router: CommandRouter,
    pub engine: QueryEngine,
    #[allow(dead_code)]
    pub prompts: PromptAugmentation,
}

impl std::fmt::Debug for RuntimeInitializeBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeInitializeBundle")
            .field("skill_registry", &self.skill_registry)
            .field("mcp_runtime", &self.mcp_runtime)
            .field("plugin_load_result", &self.plugin_load_result)
            .field(
                "coordinator_tool_count",
                &self.coordinator_tools.all_metadata().len(),
            )
            .field("command_count", &self.command_registry.names().len())
            .field("provider_config", &self.provider_config)
            .finish_non_exhaustive()
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
        state.record_phase(BootstrapPhase::Setup);
        state.current_cwd = setup.working_directory.clone();

        let restore_request = self.restore_request();
        let resolved_session =
            self.resolve_bootstrap_session_state(&state, restore_request.as_ref());
        self.session_store.save(
            resolved_session.snapshot.clone(),
            resolved_session.history.clone(),
        );
        state.surface = resolved_session.snapshot.surface;
        state.session_mode = resolved_session.snapshot.session_mode;
        state.client_type = resolved_session.client_type;
        state.session_source = resolved_session.session_source;
        let active_session_id = resolved_session.active_session_id();
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

        state.record_phase(BootstrapPhase::InitializeRuntime);
        let initialize_bundle = self.initialize_runtime(
            &state,
            active_session_id.clone(),
            task_manager.clone(),
            task_list_manager.clone(),
            plan_manager.clone(),
        );

        state.record_phase(BootstrapPhase::InitializeSettings);
        // Phase 7: settings/model/agent initialization
        // Currently model config is static from env/CLI, but this phase reserves
        // the seam for dynamic model switching and agent definition loading

        state.record_phase(BootstrapPhase::AugmentPrompt);
        let prompt_seed_state = self.build_runtime_seed_state(
            &state,
            &resolved_session,
            &initialize_bundle,
            active_session_id.clone(),
            initialize_bundle.notification_dispatcher.clone(),
        );
        let prompts = self.augment_prompts(&prompt_seed_state, &initialize_bundle);

        state.record_phase(BootstrapPhase::GateUserAccess);
        let access_decision = self.gate_user_access(&state, None);
        if !access_decision.allowed {
            anyhow::bail!(
                access_decision
                    .reason
                    .unwrap_or_else(|| "access denied during bootstrap".into())
            );
        }

        state.record_phase(BootstrapPhase::WarmupAndConvergence);
        // Phase 10: warmup & MCP convergence
        // MCP runtime is already initialized in initialize_bundle
        // Plugin sync happens via RuntimePluginState in finalize_runtime_state
        // This phase marks the boundary before final state assembly

        state.record_phase(BootstrapPhase::AssembleAppState);
        // Phase 11: AppState/Store assembly
        let state = state.finalize();
        let finalized = self.finalize_runtime_state(
            &state,
            resolved_session,
            initialize_bundle,
            prompts,
            active_session_id,
        );
        let app_state = finalized.app_state.clone();
        let initial_snapshot = finalized.snapshot.clone();
        let router = finalized.router;
        let engine = finalized.engine;

        if self.cli.trace_startup {
            println!("startup: {}", state.startup_trace());
        }

        if self.cli.show_tools {
            for tool in initial_snapshot
                .tool_registry
                .visible_tools(&app_state.permission_context)
            {
                println!("{} - {}", tool.name, tool.description);
            }
            return Ok(());
        }

        if self.cli.init_only {
            println!(
                "initialized {} runtime in {:?} mode",
                self.cli.surface, state.session_mode
            );
            return Ok(());
        }

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
                println!(
                    "{}",
                    render_output(&render_remote_response_debug(&response))
                );
            } else {
                let output = handle_cli_input(&router, &engine, &app_state, prompt.clone()).await?;
                self.print_cli_turn_output(&output);
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
            if self.cli.tui {
                self.print_tui_welcome();
            }
            for line in io::stdin().lock().lines() {
                let line = line?;
                if self.should_exit_tui_input(&line) {
                    if self.cli.tui {
                        self.print_tui_message("Exiting TUI session.");
                    }
                    break;
                }
                let output = handle_cli_input(&router, &engine, &app_state, line).await?;
                self.print_cli_turn_output(&output);
            }
            return Ok(());
        }

        let output = handle_cli_input(&router, &engine, &app_state, "/help").await?;
        self.print_cli_turn_output(&output);
        Ok(())
    }

    fn print_cli_turn_output(&self, output: &CliTurnOutput) {
        let document = render_turn_document(output);
        let rendered = if self.cli.tui {
            format!(
                "{}{}",
                tui_clear_screen_prefix(),
                render_document_tui_output(&document)
            )
        } else {
            render_document_output(&document)
        };
        println!("{rendered}");
    }

    fn print_tui_welcome(&self) {
        let document = render_turn_document(&CliTurnOutput {
            primary_text: String::new(),
            events: vec![],
        });
        let rendered = format!(
            "{}{}",
            tui_clear_screen_prefix(),
            render_document_tui_output(&document)
        );
        println!("{rendered}");
    }

    fn print_tui_message(&self, message: &str) {
        let mut screen = build_tui_screen(&render_turn_document(&CliTurnOutput {
            primary_text: String::new(),
            events: vec![],
        }));
        screen.footer = vec![message.to_string()];
        let rendered = format!(
            "{}{}",
            tui_clear_screen_prefix(),
            render_tui_screen_output(&screen)
        );
        println!("{rendered}");
    }

    fn should_exit_tui_input(&self, input: &str) -> bool {
        self.cli.tui && is_tui_exit_input(input)
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

    pub fn initialize_runtime(
        &self,
        state: &BootstrapState,
        active_session_id: String,
        task_manager: Arc<TaskManager>,
        task_list_manager: Arc<TaskListManager>,
        plan_manager: Arc<PlanManager>,
    ) -> RuntimeInitializeBundle {
        let base_hook_registry = load_hook_registry(&state.current_cwd);
        let plugin_load_result = Arc::new(load_plugins(&state.current_cwd));
        let hook_registry =
            augment_hook_registry_with_plugins(base_hook_registry, plugin_load_result.as_ref());
        let _ = run_hook(&hook_registry, HookEvent::SessionStart);
        let _ = run_hook(&hook_registry, HookEvent::Setup);

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
        let coordinator_tools = tool_inventory.assemble(ToolAssemblyContext::coordinator(
            state.surface,
            state.session_mode,
        ));
        let runtime_tool_registry = Arc::new(RwLock::new(coordinator_tools.clone()));
        let notification_dispatcher = NotificationDispatcher::new(self.build_telegram_gateway())
            .with_hook_registry(hook_registry.clone());
        let permission_context =
            ToolAssemblyContext::coordinator(state.surface, state.session_mode)
                .permission_context(if self.cli.init_only {
                    PermissionMode::Plan
                } else {
                    PermissionMode::Default
                })
                .with_task_manager(task_manager)
                .with_task_list_manager(task_list_manager)
                .with_plan_manager(plan_manager)
                .with_skill_registry(skill_registry.clone())
                .with_mcp_runtime(mcp_runtime.clone())
                .with_active_session_id(active_session_id)
                .with_active_surface(state.surface)
                .with_notification_dispatcher(notification_dispatcher.clone())
                .with_inherited_tool_registry(coordinator_tools.clone())
                .with_inherited_hook_registry(hook_registry.clone());
        let app_state = AppState {
            surface: state.surface,
            session_mode: state.session_mode,
            client_type: state.client_type,
            session_source: state.session_source,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(runtime_tool_registry.clone()),
            skill_registry: Some(skill_registry.clone()),
            mcp_runtime: Some(mcp_runtime.clone()),
            plugin_load_result: Some(plugin_load_result.clone()),
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: notification_dispatcher.clone(),
            audit_log: Arc::new(Mutex::new(AuditLog::file_backed(
                AuditLog::default_root_from(&state.current_cwd),
            ))),
            startup_trace: state
                .phases
                .iter()
                .map(|phase| format!("{phase:?}"))
                .collect(),
            active_session_id: String::new(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        };
        let snapshot = build_runtime_plugin_snapshot(&app_state);
        let command_registry = snapshot.command_registry.clone();
        let provider_config = self.build_model_provider_config();
        let api_client = ModelProviderClient::from_config(provider_config.clone());

        RuntimeInitializeBundle {
            hook_registry,
            notification_dispatcher,
            skill_registry,
            mcp_runtime,
            plugin_load_result,
            coordinator_tools,
            runtime_tool_registry,
            command_registry,
            provider_config,
            api_client,
            compactor: ReactiveCompactor,
        }
    }

    fn build_runtime_seed_state(
        &self,
        state: &BootstrapState,
        resolved_session: &ResolvedSessionState,
        initialize_bundle: &RuntimeInitializeBundle,
        active_session_id: String,
        notification_dispatcher: NotificationDispatcher,
    ) -> AppState {
        let permission_context = ToolPermissionContext::new(if self.cli.init_only {
            PermissionMode::Plan
        } else {
            PermissionMode::Default
        })
        .with_skill_registry(initialize_bundle.skill_registry.clone())
        .with_mcp_runtime(initialize_bundle.mcp_runtime.clone())
        .with_active_session_id(active_session_id.clone())
        .with_active_surface(state.surface)
        .with_notification_dispatcher(notification_dispatcher)
        .with_deferred_tools(true)
        .with_interactive_tools(true)
        .with_inherited_tool_registry(initialize_bundle.coordinator_tools.clone())
        .with_inherited_hook_registry(initialize_bundle.hook_registry.clone());
        let mut app_state = AppState {
            surface: state.surface,
            session_mode: state.session_mode,
            client_type: state.client_type,
            session_source: state.session_source,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: Some(initialize_bundle.command_registry.clone()),
            runtime_tool_registry: Some(initialize_bundle.runtime_tool_registry.clone()),
            skill_registry: Some(initialize_bundle.skill_registry.clone()),
            mcp_runtime: Some(initialize_bundle.mcp_runtime.clone()),
            plugin_load_result: Some(initialize_bundle.plugin_load_result.clone()),
            cost_tracker: CostTracker::with_default_pricing(
                initialize_bundle.provider_config.model_id.clone(),
                initialize_bundle.provider_config.pricing.clone(),
            ),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: initialize_bundle.notification_dispatcher.clone(),
            audit_log: Arc::new(Mutex::new(AuditLog::file_backed(
                AuditLog::default_root_from(&std::path::PathBuf::from(
                    resolved_session.snapshot.cwd.clone(),
                )),
            ))),
            startup_trace: state
                .phases
                .iter()
                .map(|phase| format!("{phase:?}"))
                .collect(),
            active_session_id,
            session_store: Some(self.session_store.clone()),
            session: None,
            history: None,
            restored_session: None,
        };
        app_state.apply_resolved_session_state(resolved_session);
        app_state
    }

    pub fn augment_prompts(
        &self,
        app_state: &AppState,
        initialize_bundle: &RuntimeInitializeBundle,
    ) -> PromptAugmentation {
        PromptAugmentation {
            system_prompt: crate::prompt::system::build_system_prompt(app_state),
            tools_prompt: crate::prompt::tools::build_tools_prompt(
                &initialize_bundle.coordinator_tools,
                &app_state.permission_context,
            ),
            context_prompt: crate::prompt::context::build_context_prompt(app_state),
            metadata: PromptAugmentationMetadata {
                active_session_id: app_state.active_session_id.clone(),
                surface: app_state.surface,
                session_mode: app_state.session_mode,
                visible_tool_count: initialize_bundle
                    .coordinator_tools
                    .visible_tools(&app_state.permission_context)
                    .len(),
            },
        }
    }

    pub fn gate_user_access(
        &self,
        _state: &BootstrapState,
        input: Option<&NormalizedInput>,
    ) -> UserAccessDecision {
        let authorizer = DefaultSurfaceAuthorizer::default();
        let Some(input) = input else {
            return UserAccessDecision {
                allowed: true,
                reason: None,
            };
        };
        match authorizer.authorize(input) {
            AuthDecision::Allow => UserAccessDecision {
                allowed: true,
                reason: None,
            },
            AuthDecision::Deny { reason, .. } => UserAccessDecision {
                allowed: false,
                reason: Some(reason),
            },
        }
    }

    pub fn finalize_runtime_state(
        &self,
        state: &BootstrapState,
        resolved_session: ResolvedSessionState,
        initialize_bundle: RuntimeInitializeBundle,
        prompts: PromptAugmentation,
        active_session_id: String,
    ) -> FinalizedRuntime {
        let mut app_state = self.build_runtime_seed_state(
            state,
            &resolved_session,
            &initialize_bundle,
            active_session_id,
            initialize_bundle.notification_dispatcher.clone(),
        );
        let initial_snapshot = build_runtime_plugin_snapshot(&app_state);
        let runtime_plugin_state = RuntimePluginState::new(initial_snapshot.clone());
        app_state.permission_context = app_state
            .permission_context
            .clone()
            .with_runtime_plugin_state(runtime_plugin_state);
        hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);
        let store = AppStateStore::new(app_state.clone());
        let router = build_turn_router(&initial_snapshot);
        let base_query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: initialize_bundle.api_client.clone(),
            compactor: initialize_bundle.compactor.clone(),
            hook_registry: initial_snapshot.hook_registry.clone(),
            agent_id: None,
            system_prompt: prompts.system_prompt.clone(),
            tools_prompt: prompts.tools_prompt.clone(),
            context_prompt: prompts.context_prompt.clone(),
        };
        let engine = build_turn_engine(
            &app_state,
            &initial_snapshot,
            &QueryEngine::new(base_query_context),
        );
        FinalizedRuntime {
            app_state,
            store,
            snapshot: initial_snapshot,
            router,
            engine,
            prompts,
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
            timeout: ProviderTimeout { request_timeout_ms },
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

    fn resolve_bootstrap_session_state(
        &self,
        state: &BootstrapState,
        request: Option<&RestoreRequest>,
    ) -> ResolvedSessionState {
        resolve_session_state(
            self.session_store.as_ref(),
            request,
            state.surface,
            state.session_mode,
            &state.current_cwd,
        )
    }
}

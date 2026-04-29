use std::io::{self, BufRead};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use clap::Parser;

use crate::bootstrap::config_root::resolve_config_root;
use crate::bootstrap::model_profiles::load_active_model_profile_from_root;
use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::proxy_env::resolve_proxy_env_contract;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
use crate::command::registry::CommandRegistry;
use crate::core::boss::BossCoordinator;
use crate::core::boss_runtime::BossRuntimeHost;
use crate::core::boss_state::BossLisMPolicy;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::core::lism_ab_sample::LisMAbSampleSink;
use crate::core::lism_ab_sample::LisMRolloutConclusion;
use crate::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus};
use crate::core::boss::save_plan;
use crate::cost::tracker::CostTracker;
use crate::history::resume::{
    ResolvedSessionState, RestoreRequest, RestoreSource, resolve_session_state,
};
use crate::history::session::{FileBackedSessionStore, SessionId, SessionStore};
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry, load_hook_registry_from_root};
use crate::interaction::cli::renderer::{
    build_tui_screen, render_document_output, render_document_tui_output, render_output,
    render_tui_screen_output, render_turn_document,
};
use crate::interaction::cli::repl::{CliTurnOutput, handle_cli_input, handle_normalized_input};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::remote::{
    RemoteRequest, handle_remote_request, render_remote_response_debug,
};
use crate::interaction::router::CommandRouter;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::plan::manager::PlanManager;
use crate::plugins::loader::load_plugins_from_root;
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
use crate::security::filesystem_policy::FilesystemPolicy;
use crate::security::workspace_capability::WorkspaceCapabilityConfig;
use crate::service::api::client::{
    ModelPricing, ModelProviderClient, ModelProviderConfig, ProviderAuthStrategy,
    ProviderCompatibilityProfileKind, ProviderProtocol, ProviderTimeout, validate_provider_config,
};

fn infer_provider_contract(
    provider_id: &str,
) -> Option<(ProviderProtocol, ProviderCompatibilityProfileKind)> {
    match provider_id.trim() {
        "anthropic" | "default-provider" => Some((
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::Anthropic,
        )),
        "text-only-provider" => Some((
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::TextOnly,
        )),
        "batch-provider" => Some((
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::Batch,
        )),
        "openai" | "openai-compatible" | "openai_compatible" | "kimi" | "glm" | "minimax" => {
            Some((
                ProviderProtocol::OpenAICompatible,
                ProviderCompatibilityProfileKind::OpenAICompatible,
            ))
        }
        "gemini" | "gemini-native" | "gemini_native" => Some((
            ProviderProtocol::GeminiNative,
            ProviderCompatibilityProfileKind::GeminiNativeUnsupported,
        )),
        _ => None,
    }
}

fn parse_provider_protocol(value: &str) -> anyhow::Result<ProviderProtocol> {
    match value.trim() {
        "anthropic" => Ok(ProviderProtocol::Anthropic),
        "openai" | "openai-compatible" | "openai_compatible" => {
            Ok(ProviderProtocol::OpenAICompatible)
        }
        "gemini" | "gemini-native" | "gemini_native" => Ok(ProviderProtocol::GeminiNative),
        other => anyhow::bail!("invalid_configuration: unsupported provider protocol {other}"),
    }
}

fn parse_provider_compatibility_profile(
    value: &str,
) -> anyhow::Result<ProviderCompatibilityProfileKind> {
    match value.trim() {
        "anthropic" => Ok(ProviderCompatibilityProfileKind::Anthropic),
        "text-only" | "text_only" | "textonly" => Ok(ProviderCompatibilityProfileKind::TextOnly),
        "batch" => Ok(ProviderCompatibilityProfileKind::Batch),
        "openai" | "openai-compatible" | "openai_compatible" => {
            Ok(ProviderCompatibilityProfileKind::OpenAICompatible)
        }
        "gemini" | "gemini-native-unsupported" | "gemini_native_unsupported" => {
            Ok(ProviderCompatibilityProfileKind::GeminiNativeUnsupported)
        }
        other => anyhow::bail!(
            "invalid_configuration: unsupported provider compatibility profile {other}"
        ),
    }
}

fn parse_provider_auth_strategy(value: &str) -> anyhow::Result<ProviderAuthStrategy> {
    match value.trim() {
        "bearer" | "bearer_api_key" | "bearer-api-key" => Ok(ProviderAuthStrategy::BearerApiKey),
        "none" | "no_auth" | "no-auth" => Ok(ProviderAuthStrategy::NoAuth),
        other => anyhow::bail!("invalid_configuration: unsupported auth strategy {other}"),
    }
}

pub fn summarize_active_model_provider(config: &ModelProviderConfig) -> ActiveModelProviderSummary {
    let auth_status = match (config.api_key.is_some(), config.api_key_env.as_deref()) {
        (true, Some(env_name)) => format!("env:{}(set)", env_name),
        (false, Some(env_name)) => format!("env:{}(unset)", env_name),
        (true, None) => "key:set".into(),
        (false, None) => "none".into(),
    };
    ActiveModelProviderSummary {
        provider_id: config.provider_id.clone(),
        protocol: format!("{:?}", config.protocol),
        compatibility_profile: format!("{:?}", config.compatibility_profile),
        base_url_host: extract_base_url_host(&config.base_url),
        model: config.model_id.clone(),
        auth_status,
    }
}

pub fn has_explicit_provider_env_override() -> bool {
    [
        "RUST_AGENT_PROVIDER_ID",
        "RUST_AGENT_PROVIDER_BASE_URL",
        "RUST_AGENT_PROVIDER_API_KEY",
        "RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH",
        "RUST_AGENT_PROVIDER_DEFAULT_MODEL",
        "RUST_AGENT_PROVIDER_MODEL",
        "RUST_AGENT_PROVIDER_TIMEOUT_MS",
        "RUST_AGENT_PROVIDER_STREAM_TIMEOUT_MS",
        "RUST_AGENT_PROVIDER_RETRY_MAX_ATTEMPTS",
        "RUST_AGENT_PROVIDER_RETRY_INITIAL_BACKOFF_MS",
        "RUST_AGENT_PROVIDER_RETRY_MAX_BACKOFF_MS",
        "RUST_AGENT_PROVIDER_PROTOCOL",
        "RUST_AGENT_PROVIDER_COMPATIBILITY_PROFILE",
        "RUST_AGENT_PROVIDER_AUTH_STRATEGY",
        "RUST_AGENT_PROVIDER_PROMPT_CACHE_KEY",
        "RUST_AGENT_PROVIDER_PROMPT_CACHE_RETENTION",
    ]
    .iter()
    .any(|key| {
        std::env::var(key)
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    })
}

fn extract_base_url_host(base_url: &str) -> String {
    reqwest::Url::parse(base_url)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_string()))
        .unwrap_or_else(|| base_url.trim().to_string())
}

use crate::core::concurrency::SubagentLimiter;
use crate::service::api::retry::RetryPolicy;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::service::mcp::config::load_server_configs_from_root;
use crate::service::mcp::runtime::McpRuntime;
use crate::service::mcp::state::load_mcp_governance_state_from_root;
use crate::service::observability::ServiceObservabilityTracker;
use crate::skills::bundled::bundled_skills;
use crate::skills::loader::SkillLoaderCache;
use crate::skills::registry::SkillRegistry;
use crate::state::active_model_runtime::{ActiveModelRuntime, ActiveModelRuntimeSnapshot};
use crate::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole,
    SessionPersistFailure,
};
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

const DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_MS: u64 = 1_500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownFailure {
    ForceDrainTimedOut,
    PersistBeforeShutdown(SessionPersistFailure),
    PersistAfterShutdown(SessionPersistFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Completed,
    Forced {
        hibernated_task_ids: Vec<String>,
    },
    Failed {
        failure: ShutdownFailure,
        hibernated_task_ids: Vec<String>,
    },
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
    #[arg(long = "attach", value_name = "PATH")]
    pub attachments: Vec<String>,
    /// Path to JSONL file for LisM A/B sample collection. When set, boss runs
    /// automatically append a sample record on completion/abortion.
    #[arg(long, value_name = "PATH")]
    pub lism_ab_sample: Option<String>,
    /// Read a LisM A/B JSONL sample file and print an A/B summary, then exit.
    #[arg(long, value_name = "PATH")]
    pub lism_ab_summarize: Option<String>,
    /// Like --lism-ab-summarize but also prints the rollout policy conclusion.
    #[arg(long, value_name = "PATH")]
    pub lism_ab_conclude: Option<String>,
    /// Override the boss LisM policy for this run. One of: inherit, force-on, force-off.
    #[arg(long, value_name = "POLICY")]
    pub lism_policy: Option<String>,
    /// Run a single boss task non-interactively. Creates a one-step plan, executes it
    /// to completion, records the LisM A/B sample if --lism-ab-sample is set, then exits.
    #[arg(long, value_name = "TASK")]
    pub boss_task: Option<String>,
}

impl Default for BootstrapCli {
    fn default() -> Self {
        Self {
            print: None,
            interactive: false,
            init_only: false,
            continue_session: false,
            resume: None,
            trace_startup: false,
            show_tools: false,
            tui: false,
            surface: "cli".into(),
            attachments: Vec::new(),
            lism_ab_sample: None,
            lism_ab_summarize: None,
            lism_ab_conclude: None,
            lism_policy: None,
            boss_task: None,
        }
    }
}

#[derive(Clone)]
pub struct RuntimeBootstrap {
    cli: BootstrapCli,
    session_store: Arc<dyn SessionStore>,
    provider_config_override: Option<ModelProviderConfig>,
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
    pub filesystem_policy: Option<Arc<FilesystemPolicy>>,
    pub plugin_load_result: Arc<crate::plugins::types::PluginLoadResult>,
    pub coordinator_tools: ToolRegistry,
    pub runtime_tool_registry: Arc<RwLock<ToolRegistry>>,
    pub command_registry: Arc<CommandRegistry>,
    pub provider_config: ModelProviderConfig,
    pub active_model_runtime: ActiveModelRuntime,
    pub active_model_profile_name: Option<String>,
    pub active_model_profile_source: ActiveModelProfileSource,
    pub api_client: ModelProviderClient,
    pub compactor: ReactiveCompactor,
    pub subagent_limiter: Arc<SubagentLimiter>,
    pub boss_runtime_host: Option<BossRuntimeHost>,
    pub boss_coordinator: Option<Arc<BossCoordinator>>,
    pub startup_warnings: crate::bootstrap::warnings::StartupWarnings,
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
    pub boss_runtime_host: Option<BossRuntimeHost>,
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
            provider_config_override: None,
        }
    }

    pub fn with_session_store(mut self, session_store: Arc<dyn SessionStore>) -> Self {
        self.session_store = session_store;
        self
    }

    pub fn with_provider_config(mut self, provider_config: ModelProviderConfig) -> Self {
        self.provider_config_override = Some(provider_config);
        self
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // Early-exit: print LisM A/B summary and return without bootstrapping the runtime.
        if let Some(path) = &self.cli.lism_ab_summarize {
            let records = LisMAbSampleSink::load_records(path);
            if records.is_empty() {
                println!("No LisM A/B sample records found at: {path}");
                return Ok(());
            }
            let sink = LisMAbSampleSink::in_memory();
            for rec in &records {
                sink.push_record(rec.clone());
            }
            let summary = sink.summarize();
            print_lism_ab_summary(&summary, records.len());
            return Ok(());
        }

        // Early-exit: print A/B summary + rollout conclusion.
        if let Some(path) = &self.cli.lism_ab_conclude {
            let records = LisMAbSampleSink::load_records(path);
            if records.is_empty() {
                println!("No LisM A/B sample records found at: {path}");
                return Ok(());
            }
            let sink = LisMAbSampleSink::in_memory();
            for rec in &records {
                sink.push_record(rec.clone());
            }
            let summary = sink.summarize();
            print_lism_ab_summary(&summary, records.len());
            println!();
            let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
            print!("{conclusion}");
            return Ok(());
        }

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
        let _ = self.session_store.save(
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
        )?;

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
        // build_runtime_seed_state doesn't carry task_manager/task_list_manager/plan_manager
        // through RuntimeInitializeBundle — patch them into the finalized permission_context here
        // so boss dispatch (and any other tool that requires task_manager) works.
        let app_state = {
            let mut s = app_state;
            s.permission_context = s
                .permission_context
                .with_task_manager(task_manager.clone())
                .with_task_list_manager(task_list_manager.clone())
                .with_plan_manager(plan_manager.clone());
            s
        };
        let router = finalized.router;
        let engine = finalized.engine;

        // Bootstrap actor runtimes with full A+B callbacks now that AppState is available.
        // BossCoordinator must be constructed before AppState (it is a field of AppState),
        // so new_with_app_state() cannot be used here. Route through host.bootstrap_coordinator
        // to keep the factory contract in one place.
        if let (Some(host), Some(boss)) = (
            finalized.boss_runtime_host.as_ref(),
            app_state.boss_coordinator.as_ref(),
        ) {
            let app_arc = Arc::new(app_state.clone());
            host.bootstrap_coordinator(boss, &app_arc).await;
        }

        if let Some(task_manager) = app_state.permission_context.task_manager.as_ref() {
            task_manager.set_activity_tracker(app_state.last_activity_ts.clone());
        }
        spawn_runtime_signal_shutdown(app_state.clone());

        // Initialize and spawn background housekeeping daemon
        let session_root = crate::history::session::FileBackedSessionStore::default_root();
        let task_output_root = std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join(".rust-agent")
            .join("task-outputs");

        let housekeeping_daemon = crate::core::housekeeping::HousekeepingDaemon::new(
            crate::core::housekeeping::HousekeepingConfig::default(),
            app_state.cancellation_token.clone(),
            app_state.last_activity_ts.clone(),
        )
        .with_app_state(app_state.clone())
        .with_roots(session_root, task_output_root);
        tokio::spawn(housekeeping_daemon.run());

        if self.cli.trace_startup {
            println!("startup: {}", state.startup_trace());
        }

        if self.cli.show_tools {
            for tool in finalized
                .snapshot
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

        if let Some(task_desc) = self.cli.boss_task.clone() {
            let app_arc = Arc::new(app_state.clone());
            if let Some(boss) = app_arc.boss_coordinator.as_ref() {
                boss.seed_plan_for_task(&task_desc).await;
                let advance_msg = boss.advance_plan(&app_arc).await;
                println!("[boss-task] advance_plan result: {:?}", advance_msg);
                // Poll until completion or terminal failure, 5-minute timeout.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
                let mut tick = 0u32;
                loop {
                    let stage = boss.get_stage().await;
                    if matches!(stage, crate::core::boss_state::BossStage::Completed) {
                        break;
                    }
                    // Also exit on terminal failure so we don't waste the timeout.
                    let step_failed = if let Some(task_manager) = app_arc.permission_context.task_manager.as_ref() {
                        let b_task_id = boss.b_task_id().await;
                        if let Some(tid) = b_task_id {
                            matches!(task_manager.status(&tid), Some(crate::task::types::TaskStatus::Failed | crate::task::types::TaskStatus::Killed))
                        } else { false }
                    } else { false };
                    if step_failed {
                        println!("[boss-task] step task failed/killed — stopping poll");
                        break;
                    }
                    if std::time::Instant::now() >= deadline {
                        println!("[boss-task] timed out after 5 minutes");
                        break;
                    }
                    tick += 1;
                    if tick % 20 == 0 {
                        let b_task_id = boss.b_task_id().await;
                        if let Some(tid) = b_task_id {
                            if let Some(task_manager) = app_arc.permission_context.task_manager.as_ref() {
                                println!("[boss-task] b_task {} status: {:?}", tid, task_manager.status(&tid));
                            }
                        }
                        println!("[boss-task] still waiting, stage: {:?}", stage);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                println!("[boss-task] final stage: {:?}", boss.get_stage().await);
                if let Some(task_manager) = app_arc.permission_context.task_manager.as_ref() {
                    // Print B task output to diagnose failures.
                    if let Some(b_id) = boss.b_task_id().await {
                        println!("[boss-task] b_task {} status: {:?}", b_id, task_manager.status(&b_id));
                        if let Some(slice) = task_manager.get_output(&b_id, 0) {
                            println!("[boss-task] b_task output (first 500 chars): {:?}", &slice.content[..slice.content.len().min(500)]);
                        }
                    }
                    if let Ok(report) = boss.report_progress(task_manager).await {
                        if let Some(obs) = &report.observability_summary {
                            if let Some(ratio) = obs.cache_hit_ratio() {
                                println!("[boss-task] cache_hit_ratio: {:.3}", ratio);
                            }
                            println!(
                                "[boss-task] cost_micros_usd: {}",
                                obs.estimated_cost_micros_usd
                            );
                        }
                    }
                }
            } else {
                println!("[boss-task] no BossCoordinator available");
            }
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
                        correlation_id: None,
                    },
                )
                .await?;
                println!(
                    "{}",
                    render_output(&render_remote_response_debug(&response))
                );
            } else {
                let input = NormalizedInput::from_session_raw(
                    app_state.surface,
                    app_state.active_session_id.clone(),
                    prompt.clone(),
                )
                .with_attachments(self.cli.attachments.clone());
                let output = handle_normalized_input(&router, &engine, &app_state, input).await?;
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
                    execute_runtime_shutdown(app_state.clone(), "interactive_exit").await;
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
    ) -> anyhow::Result<RuntimeInitializeBundle> {
        let config_root = resolve_config_root(&state.current_cwd)?;
        let base_hook_registry = load_hook_registry_from_root(&config_root);
        let plugin_load_result = Arc::new(load_plugins_from_root(&config_root, &state.current_cwd));
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
        let service_observability_tracker = ServiceObservabilityTracker::default();
        let mcp_config_result = load_server_configs_from_root(&config_root);
        let mcp_governance_result = load_mcp_governance_state_from_root(&config_root);
        let mcp_config_diagnostics = mcp_config_result.diagnostics.clone();
        let mcp_runtime = Arc::new(
            McpRuntime::new_with_config_and_governance_result_and_observability(
                Arc::new(crate::service::mcp::client::RoutingMcpClient::default()),
                mcp_config_result,
                mcp_governance_result,
                service_observability_tracker.clone(),
            ),
        );
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

        let boss_runtime_host = BossRuntimeHost::new();
        let mut boss_coordinator = BossCoordinator::new_with_runtime_owner(boss_runtime_host.owner());

        // Wire LisM A/B sample sink if requested via CLI.
        if let Some(path) = &self.cli.lism_ab_sample {
            match LisMAbSampleSink::with_jsonl_path(path) {
                Ok(sink) => boss_coordinator.set_lism_ab_sink(Arc::new(sink)),
                Err(e) => tracing::warn!("Failed to open LisM A/B sample path {path}: {e}"),
            }
        }

        // Apply LisM policy override if requested via CLI.
        if let Some(policy_str) = &self.cli.lism_policy {
            boss_coordinator.init_lism_policy(parse_lism_policy(policy_str));
        }

        let boss_coordinator = Arc::new(boss_coordinator);

        let notification_dispatcher = NotificationDispatcher::new(self.build_telegram_gateway())
            .with_hook_registry(hook_registry.clone())
            .with_boss_coordinator(boss_coordinator.clone());
        let filesystem_policy = self
            .load_filesystem_policy()
            .unwrap_or_else(|error| {
                panic!("failed to load filesystem policy during bootstrap: {error}")
            })
            .map(Arc::new);

        // Initialize the global subagent concurrency limiter
        let subagent_limiter = SubagentLimiter::new();

        let mut permission_context =
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
                .with_inherited_hook_registry(hook_registry.clone())
                .with_subagent_limiter(subagent_limiter.clone())
                .with_boss_coordinator(boss_coordinator.clone());
        if let Some(policy) = filesystem_policy.clone() {
            permission_context = permission_context.with_filesystem_policy(policy);
        }
        if let Some(cap_config) = self
            .load_workspace_capability_config()
            .unwrap_or_else(|e| {
                tracing::warn!("failed to load workspace capability config: {e}");
                None
            })
        {
            permission_context =
                permission_context.with_workspace_capability(Arc::new(cap_config));
        }
        let last_activity_ts = Arc::new(std::sync::atomic::AtomicU64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ));
        let cancellation_token = CancellationToken::new();
        permission_context = permission_context
            .with_last_activity_ts(last_activity_ts.clone())
            .with_cancellation_token(cancellation_token.clone());
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
            service_observability_tracker: service_observability_tracker.clone(),
            notification_dispatcher: notification_dispatcher.clone(),
            audit_log: Arc::new(Mutex::new(AuditLog::file_backed(
                AuditLog::default_root_from(&state.current_cwd),
            ))),
            startup_trace: state
                .phases
                .iter()
                .map(|phase| format!("{phase:?}"))
                .collect(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "default-provider".into(),
                protocol: "Anthropic".into(),
                compatibility_profile: "Anthropic".into(),
                base_url_host: "localhost".into(),
                model: "default-model".into(),
                auth_status: "env:OPENAI_API_KEY(unset)".into(),
            },
            active_session_id: String::new(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts,
            cancellation_token,
            subagent_limiter: Some(subagent_limiter.clone()),
            boss_coordinator: Some(boss_coordinator.clone()),
            remote_actor_store: None,
        };
        let snapshot = build_runtime_plugin_snapshot(&app_state);
        let command_registry = snapshot.command_registry.clone();
        let (provider_config, active_model_profile_name, active_model_profile_source) =
            self.build_model_provider_config(&config_root)?;
        validate_provider_config(&provider_config)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let api_client = ModelProviderClient::from_config_with_observability(
            provider_config.clone(),
            service_observability_tracker.clone(),
        );
        let active_model_snapshot = ActiveModelRuntimeSnapshot {
            config: provider_config.clone(),
            client: api_client.clone(),
            active_profile_name: active_model_profile_name.clone(),
            source: active_model_profile_source.clone(),
            summary: summarize_active_model_provider(&provider_config),
        };
        let active_model_runtime = ActiveModelRuntime::new(active_model_snapshot.clone());

        let startup_warnings = crate::bootstrap::warnings::collect_startup_warnings(
            &provider_config.base_url,
            &mcp_config_diagnostics,
            &config_root,
            filesystem_policy.is_none(),
            &provider_config.provider_id,
            false,
        );
        startup_warnings.emit_tracing();

        Ok(RuntimeInitializeBundle {
            hook_registry,
            notification_dispatcher,
            skill_registry,
            mcp_runtime,
            filesystem_policy,
            plugin_load_result,
            coordinator_tools,
            runtime_tool_registry,
            command_registry,
            provider_config,
            active_model_runtime,
            active_model_profile_name,
            active_model_profile_source,
            api_client,
            compactor: ReactiveCompactor,
            subagent_limiter,
            boss_runtime_host: Some(boss_runtime_host),
            boss_coordinator: Some(boss_coordinator),
            startup_warnings,
        })
    }

    fn build_runtime_seed_state(
        &self,
        state: &BootstrapState,
        resolved_session: &ResolvedSessionState,
        initialize_bundle: &RuntimeInitializeBundle,
        active_session_id: String,
        notification_dispatcher: NotificationDispatcher,
    ) -> AppState {
        let mut permission_context = ToolPermissionContext::new(if self.cli.init_only {
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
        .with_inherited_hook_registry(initialize_bundle.hook_registry.clone())
        .with_subagent_limiter(initialize_bundle.subagent_limiter.clone());

        if let Some(boss) = initialize_bundle.boss_coordinator.clone() {
            permission_context = permission_context.with_boss_coordinator(boss);
        }
        if let Some(policy) = initialize_bundle.filesystem_policy.clone() {
            permission_context = permission_context.with_filesystem_policy(policy);
        }
        if let Some(cap_config) = self
            .load_workspace_capability_config()
            .unwrap_or_else(|e| {
                tracing::warn!("failed to load workspace capability config: {e}");
                None
            })
        {
            permission_context =
                permission_context.with_workspace_capability(Arc::new(cap_config));
        }
        let last_activity_ts = Arc::new(std::sync::atomic::AtomicU64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ));
        let cancellation_token = CancellationToken::new();
        let active_model_snapshot = initialize_bundle.active_model_runtime.snapshot_blocking();
        permission_context = permission_context
            .with_last_activity_ts(last_activity_ts.clone())
            .with_cancellation_token(cancellation_token.clone())
            .with_inherited_active_model_snapshot(active_model_snapshot.clone());
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
            service_observability_tracker: initialize_bundle.api_client.observability_tracker(),
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
            active_model_runtime: Some(initialize_bundle.active_model_runtime.clone()),
            active_model_profile_name: active_model_snapshot.active_profile_name,
            active_model_profile_source: active_model_snapshot.source,
            active_model_provider_summary: active_model_snapshot.summary,
            active_session_id,
            session_store: Some(self.session_store.clone()),
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts,
            cancellation_token,
            subagent_limiter: Some(initialize_bundle.subagent_limiter.clone()),
            boss_coordinator: initialize_bundle.boss_coordinator.clone(),
            remote_actor_store: None,
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
            boss_runtime_host: initialize_bundle.boss_runtime_host,
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

    fn load_filesystem_policy(&self) -> anyhow::Result<Option<FilesystemPolicy>> {
        if let Ok(raw_path) = std::env::var("RUST_AGENT_FILESYSTEM_POLICY") {
            let trimmed = raw_path.trim();
            if trimmed.is_empty() {
                anyhow::bail!("RUST_AGENT_FILESYSTEM_POLICY is set but empty")
            }
            let path = std::path::PathBuf::from(trimmed);
            if !path.is_absolute() {
                anyhow::bail!(
                    "RUST_AGENT_FILESYSTEM_POLICY must be an absolute path: {}",
                    path.display()
                )
            }
            return FilesystemPolicy::load_from_path(&path).map(Some);
        }

        // If RUST_AGENT_CONFIG_ROOT is set, look for filesystem-policy.json there.
        // Otherwise fall back to $HOME/.claude/ (the historical default).
        let policy_dir = if let Ok(raw) = std::env::var("RUST_AGENT_CONFIG_ROOT") {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                anyhow::bail!("RUST_AGENT_CONFIG_ROOT is set but empty");
            }
            let p = std::path::PathBuf::from(trimmed);
            if !p.is_absolute() {
                anyhow::bail!(
                    "RUST_AGENT_CONFIG_ROOT must be an absolute path, got: {}",
                    p.display()
                );
            }
            p
        } else {
            let Some(home) = std::env::var_os("HOME") else {
                return Ok(None);
            };
            std::path::PathBuf::from(home).join(".claude")
        };

        let path = policy_dir.join("filesystem-policy.json");
        if !path.exists() {
            return Ok(None);
        }
        FilesystemPolicy::load_from_path(&path).map(Some)
    }

    fn load_workspace_capability_config(
        &self,
    ) -> anyhow::Result<Option<WorkspaceCapabilityConfig>> {
        // Explicit path override via env var.
        if let Ok(raw_path) = std::env::var("RUST_AGENT_WORKSPACE_CAPABILITY_CONFIG") {
            let trimmed = raw_path.trim();
            if trimmed.is_empty() {
                anyhow::bail!("RUST_AGENT_WORKSPACE_CAPABILITY_CONFIG is set but empty");
            }
            let path = std::path::PathBuf::from(trimmed);
            if !path.is_absolute() {
                anyhow::bail!(
                    "RUST_AGENT_WORKSPACE_CAPABILITY_CONFIG must be an absolute path: {}",
                    path.display()
                );
            }
            let json = std::fs::read_to_string(&path).map_err(|e| {
                anyhow::anyhow!(
                    "failed to read workspace capability config at {}: {e}",
                    path.display()
                )
            })?;
            return WorkspaceCapabilityConfig::load_from_json(&json).map(Some);
        }

        // Beta deny-by-default preset when env flag is set.
        if std::env::var("RUST_AGENT_BETA_DENY_BY_DEFAULT")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            return Ok(Some(WorkspaceCapabilityConfig::beta_deny_by_default()));
        }

        // Look for workspace-capability.json in config root or ~/.claude/.
        let config_dir = if let Ok(raw) = std::env::var("RUST_AGENT_CONFIG_ROOT") {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                anyhow::bail!("RUST_AGENT_CONFIG_ROOT is set but empty");
            }
            let p = std::path::PathBuf::from(trimmed);
            if !p.is_absolute() {
                anyhow::bail!(
                    "RUST_AGENT_CONFIG_ROOT must be an absolute path, got: {}",
                    p.display()
                );
            }
            p
        } else {
            let Some(home) = std::env::var_os("HOME") else {
                return Ok(None);
            };
            std::path::PathBuf::from(home).join(".claude")
        };

        let path = config_dir.join("workspace-capability.json");
        if !path.exists() {
            return Ok(None);
        }
        let json = std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!(
                "failed to read workspace capability config at {}: {e}",
                path.display()
            )
        })?;
        WorkspaceCapabilityConfig::load_from_json(&json).map(Some)
    }

    fn build_model_provider_config(
        &self,
        config_root: &std::path::Path,
    ) -> anyhow::Result<(
        ModelProviderConfig,
        Option<String>,
        ActiveModelProfileSource,
    )> {
        if let Some(provider_config) = &self.provider_config_override {
            return Ok((
                provider_config.clone(),
                None,
                ActiveModelProfileSource::BootstrapDefault,
            ));
        }

        if has_explicit_provider_env_override() {
            let provider_config = self.build_model_provider_config_from_env()?;
            return Ok((provider_config, None, ActiveModelProfileSource::EnvOverride));
        }

        if let Some(resolved) = load_active_model_profile_from_root(config_root)? {
            return Ok((
                resolved.config,
                Some(resolved.name),
                ActiveModelProfileSource::ModelsToml,
            ));
        }

        let provider_config = self.build_model_provider_config_from_env()?;
        Ok((
            provider_config,
            None,
            ActiveModelProfileSource::BootstrapDefault,
        ))
    }

    fn build_model_provider_config_from_env(&self) -> anyhow::Result<ModelProviderConfig> {
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
        let chat_completions_path = std::env::var("RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "/v1/chat/completions".into());
        if chat_completions_path.contains("://") {
            anyhow::bail!(
                "invalid_configuration: RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH must not be a full URL"
            );
        }
        if !chat_completions_path.trim().starts_with('/') {
            anyhow::bail!(
                "invalid_configuration: RUST_AGENT_PROVIDER_CHAT_COMPLETIONS_PATH must start with '/'"
            );
        }
        let model_id = std::env::var("RUST_AGENT_PROVIDER_DEFAULT_MODEL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("RUST_AGENT_PROVIDER_MODEL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
            .unwrap_or_else(|| "default-model".into());
        let request_timeout_ms = std::env::var("RUST_AGENT_PROVIDER_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(30_000);
        let stream_timeout_ms = std::env::var("RUST_AGENT_PROVIDER_STREAM_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(120_000);
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
        let inferred = infer_provider_contract(&provider_id);
        let explicit_protocol = std::env::var("RUST_AGENT_PROVIDER_PROTOCOL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_provider_protocol(&value))
            .transpose()?;
        let explicit_profile = std::env::var("RUST_AGENT_PROVIDER_COMPATIBILITY_PROFILE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_provider_compatibility_profile(&value))
            .transpose()?;
        let (protocol, compatibility_profile) = match (
            explicit_protocol,
            explicit_profile,
            inferred,
        ) {
            (Some(protocol), Some(profile), _) => (protocol, profile),
            (None, None, Some(contract)) => contract,
            (None, None, None) => anyhow::bail!(
                "invalid_configuration: unknown provider id {provider_id} requires explicit protocol and compatibility_profile"
            ),
            _ => anyhow::bail!(
                "invalid_configuration: provider protocol and compatibility_profile must be configured together"
            ),
        };
        let auth_strategy = std::env::var("RUST_AGENT_PROVIDER_AUTH_STRATEGY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_provider_auth_strategy(&value))
            .transpose()?
            .unwrap_or_else(|| {
                if api_key.is_some() {
                    ProviderAuthStrategy::BearerApiKey
                } else {
                    ProviderAuthStrategy::NoAuth
                }
            });
        let prompt_cache_key = std::env::var("RUST_AGENT_PROVIDER_PROMPT_CACHE_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let prompt_cache_retention = std::env::var("RUST_AGENT_PROVIDER_PROMPT_CACHE_RETENTION")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let proxy_resolution = resolve_proxy_env_contract();
        Ok(ModelProviderConfig {
            provider_id,
            protocol,
            compatibility_profile,
            base_url,
            chat_completions_path,
            auth_strategy,
            api_key,
            api_key_env: Some("RUST_AGENT_PROVIDER_API_KEY".into()),
            model_id,
            timeout: ProviderTimeout {
                request_timeout_ms,
                stream_timeout_ms,
            },
            retry_policy: RetryPolicy {
                max_attempts,
                initial_backoff_ms,
                max_backoff_ms,
            },
            pricing: ModelPricing::default(),
            proxy_url: proxy_resolution.proxy_url,
            no_proxy: proxy_resolution.no_proxy,
            ca_bundle_path: std::env::var("RUST_AGENT_CA_BUNDLE")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            max_tokens_param: None,
            prompt_cache_key,
            prompt_cache_retention,
        })
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

    pub fn build_model_provider_config_from_env_for_test(
        &self,
    ) -> anyhow::Result<ModelProviderConfig> {
        self.build_model_provider_config_from_env()
    }
}
fn spawn_runtime_signal_shutdown(app_state: AppState) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal;
            use tokio::signal::unix::{SignalKind, signal as unix_signal};

            let mut terminate = unix_signal(SignalKind::terminate()).ok();
            tokio::select! {
                result = signal::ctrl_c() => {
                    if result.is_ok() {
                        execute_runtime_shutdown(app_state.clone(), "signal.ctrl_c").await;
                    }
                }
                _ = async {
                    match terminate.as_mut() {
                        Some(stream) => {
                            stream.recv().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    execute_runtime_shutdown(app_state.clone(), "signal.sigterm").await;
                }
            }
        }

        #[cfg(not(unix))]
        {
            use tokio::signal;
            if signal::ctrl_c().await.is_ok() {
                execute_runtime_shutdown(app_state.clone(), "signal.ctrl_c").await;
            }
        }
    });
}

pub fn runtime_shutdown_timeout() -> Duration {
    let timeout_ms = std::env::var("RUST_AGENT_RUNTIME_SHUTDOWN_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_MS);
    Duration::from_millis(timeout_ms)
}

pub async fn execute_runtime_shutdown(
    app_state: AppState,
    reason: &'static str,
) -> ShutdownOutcome {
    execute_runtime_shutdown_with_deadline(app_state, reason, runtime_shutdown_timeout()).await
}

pub async fn execute_runtime_shutdown_with_deadline(
    app_state: AppState,
    reason: &'static str,
    deadline: Duration,
) -> ShutdownOutcome {
    tracing::info!(
        "runtime shutdown requested: reason={}, deadline_ms={}",
        reason,
        deadline.as_millis()
    );
    let persisted_before = app_state.persist_current_session_state();
    app_state.shutdown();

    let session_id = app_state.active_session_id.clone();
    let running_tasks_cleared = async {
        loop {
            let has_running = app_state
                .permission_context
                .task_manager
                .as_ref()
                .map(|manager| manager.has_running_tasks_for_session(&session_id))
                .unwrap_or(false);
            if !has_running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };

    let mut outcome = if tokio::time::timeout(deadline, running_tasks_cleared)
        .await
        .is_err()
    {
        tracing::warn!(
            "runtime shutdown deadline exceeded for session {}; forcing task hibernation",
            session_id
        );
        let hibernated_task_ids =
            if let Some(task_manager) = app_state.permission_context.task_manager.as_ref() {
                task_manager.hibernate_owned_running_tasks(
                    &app_state.active_session_id,
                    &app_state.notification_dispatcher,
                )
            } else {
                Vec::new()
            };
        if hibernated_task_ids.is_empty() {
            record_shutdown_lifecycle_failure(
                &app_state,
                "shutdown.force_drain",
                &ShutdownFailure::ForceDrainTimedOut,
                1,
            );
            ShutdownOutcome::Failed {
                failure: ShutdownFailure::ForceDrainTimedOut,
                hibernated_task_ids,
            }
        } else {
            ShutdownOutcome::Forced {
                hibernated_task_ids,
            }
        }
    } else {
        ShutdownOutcome::Completed
    };

    let persisted_after = app_state.persist_current_session_state();
    if let Err(error) = persisted_before {
        record_shutdown_lifecycle_failure(
            &app_state,
            "shutdown.persist_before",
            &ShutdownFailure::PersistBeforeShutdown(error.clone()),
            1,
        );
        outcome = ShutdownOutcome::Failed {
            failure: ShutdownFailure::PersistBeforeShutdown(error),
            hibernated_task_ids: match outcome {
                ShutdownOutcome::Forced {
                    ref hibernated_task_ids,
                }
                | ShutdownOutcome::Failed {
                    ref hibernated_task_ids,
                    ..
                } => hibernated_task_ids.clone(),
                ShutdownOutcome::Completed => Vec::new(),
            },
        };
    } else if let Err(error) = persisted_after {
        record_shutdown_lifecycle_failure(
            &app_state,
            "shutdown.persist_after",
            &ShutdownFailure::PersistAfterShutdown(error.clone()),
            1,
        );
        outcome = ShutdownOutcome::Failed {
            failure: ShutdownFailure::PersistAfterShutdown(error),
            hibernated_task_ids: match outcome {
                ShutdownOutcome::Forced {
                    ref hibernated_task_ids,
                }
                | ShutdownOutcome::Failed {
                    ref hibernated_task_ids,
                    ..
                } => hibernated_task_ids.clone(),
                ShutdownOutcome::Completed => Vec::new(),
            },
        };
    }
    outcome
}

fn record_shutdown_lifecycle_failure(
    app_state: &AppState,
    phase: &str,
    failure: &ShutdownFailure,
    attempt: usize,
) {
    let reason = shutdown_failure_reason(failure);
    app_state
        .service_observability_tracker
        .record_runtime_lifecycle_failure(phase, &reason, &app_state.active_session_id, attempt);
    tracing::warn!(
        "runtime lifecycle failure: phase={} session_id={} attempt={} reason={}",
        phase,
        app_state.active_session_id,
        attempt,
        reason
    );
}

fn shutdown_failure_reason(failure: &ShutdownFailure) -> String {
    match failure {
        ShutdownFailure::ForceDrainTimedOut => "force_drain_timed_out".into(),
        ShutdownFailure::PersistBeforeShutdown(inner) => {
            format!("persist_before_shutdown:{}", inner.reason())
        }
        ShutdownFailure::PersistAfterShutdown(inner) => {
            format!("persist_after_shutdown:{}", inner.reason())
        }
    }
}

fn parse_lism_policy(s: &str) -> BossLisMPolicy {
    match s.trim().to_lowercase().as_str() {
        "force-on" | "force_on" | "on" => BossLisMPolicy::ForceOn,
        "force-off" | "force_off" | "off" => BossLisMPolicy::ForceOff,
        _ => BossLisMPolicy::Inherit,
    }
}

fn print_lism_ab_summary(summary: &crate::core::lism_ab_sample::LisMAbSummary, total_records: usize) {
    println!("LisM A/B Sample Summary");
    println!("=======================");
    println!("Total records : {total_records}");
    println!(
        "LisM ON       : {} runs | completion {:.2} | avg cache_hit_ratio {} | avg cost {}μ | avg tokens_saved {}",
        summary.on_runs,
        summary.on_completion_rate.map_or_else(|| "n/a".into(), |r| format!("{:.2}", r)),
        summary.on_avg_cache_hit_ratio.map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.on_avg_cost_micros_usd,
        summary.on_avg_tokens_saved,
    );
    println!(
        "LisM OFF      : {} runs | completion {:.2} | avg cache_hit_ratio {} | avg cost {}μ | avg tokens_saved {}",
        summary.off_runs,
        summary.off_completion_rate.map_or_else(|| "n/a".into(), |r| format!("{:.2}", r)),
        summary.off_avg_cache_hit_ratio.map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.off_avg_cost_micros_usd,
        summary.off_avg_tokens_saved,
    );
    if summary.has_both_arms() {
        println!("---");
        if let Some(delta) = summary.cache_hit_ratio_delta() {
            let direction = if delta > 0.0 { "↑ LisM improves" } else { "↓ LisM degrades" };
            println!("Δ cache_hit_ratio  : {:+.3} ({})", delta, direction);
        }
        let cost_delta = summary.cost_delta_micros();
        let cost_direction = if cost_delta < 0 { "↓ LisM saves" } else { "↑ LisM costs more" };
        println!("Δ cost             : {:+}μ ({})", cost_delta, cost_direction);
    } else {
        println!("--- (only one arm has data; cannot compute delta)");
    }
}

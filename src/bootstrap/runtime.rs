use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use clap::Parser;
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, read};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::execute;

use crate::bootstrap::config_root::{
    is_managed_config_root, preferred_home_config_root, resolve_config_root,
};
use crate::bootstrap::model_profiles::{
    ModelLevel, load_model_profiles_registry_from_root, merge_model_profiles_registry,
    resolve_active_model_profile_from_registry, resolve_model_level_from_registry,
};
use crate::bootstrap::proxy_env::resolve_proxy_env_contract;
use crate::bootstrap::setup::SetupContext;
use crate::bootstrap::{BootstrapPhase, BootstrapState, InteractionSurface, SessionMode};
use crate::command::registry::CommandRegistry;
use crate::command::types::{CommandMetadata, CommandSource};
use crate::core::boss::BossCoordinator;
use crate::core::boss::save_plan;
use crate::core::boss_runtime::BossRuntimeHost;
use crate::core::boss_state::BossLisMPolicy;
use crate::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus};
use crate::core::context::{QueryContext, WorkerLisMPolicy};
use crate::core::engine::QueryEngine;
use crate::core::lism_ab_sample::LisMAbSampleSink;
use crate::core::lism_ab_sample::LisMRolloutConclusion;
use crate::cost::tracker::CostTracker;
use crate::history::resume::{
    ResolvedSessionState, RestoreRequest, RestoreSource, resolve_session_state,
};
use crate::history::session::{FileBackedSessionStore, SessionId, SessionStore};
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry, load_hook_registry_from_root};
use crate::interaction::cli::renderer::{
    build_tui_loading_screen, build_tui_screen, render_document_output,
    render_document_tui_output, render_output, render_tui_screen_output, render_turn_document,
};
use crate::interaction::cli::repl::{
    CliTurnOutput, handle_cli_input, handle_cli_input_streaming, handle_normalized_input,
};
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
        "morgo" | "anthropic" | "default-provider" => Some((
            ProviderProtocol::MessagesApi,
            ProviderCompatibilityProfileKind::MessagesApi,
        )),
        "text-only-provider" => Some((
            ProviderProtocol::MessagesApi,
            ProviderCompatibilityProfileKind::TextOnly,
        )),
        "batch-provider" => Some((
            ProviderProtocol::MessagesApi,
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
        "morgo" | "messages-api" | "messages_api" | "anthropic" => {
            Ok(ProviderProtocol::MessagesApi)
        }
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
        "morgo" | "messages-api" | "messages_api" | "anthropic" => {
            Ok(ProviderCompatibilityProfileKind::MessagesApi)
        }
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

struct TuiRawModeGuard;

impl TuiRawModeGuard {
    fn activate() -> anyhow::Result<Self> {
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for TuiRawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
    }
}

#[derive(Debug, Clone)]
struct TuiSuggestion {
    replacement: String,
    label: String,
    detail: String,
    accent_color: &'static str,
}

fn tui_command_suggestions(app_state: &AppState, input: &str) -> Vec<TuiSuggestion> {
    let Some(registry) = app_state.command_registry.as_deref() else {
        return Vec::new();
    };
    if !input.starts_with('/') {
        return Vec::new();
    }

    if let Some(suggestions) = heuristic_tui_suggestions(app_state, input) {
        return suggestions;
    }

    let query = input[1..]
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    let mut commands = registry.metadata();
    commands.retain(|command| !command.is_hidden);
    commands.sort_by(|left, right| {
        command_match_score(right, &query)
            .cmp(&command_match_score(left, &query))
            .then_with(|| left.name.cmp(&right.name))
    });
    commands
        .into_iter()
        .filter(|command| command_match_score(command, &query) > 0)
        .take(8)
        .map(|command| TuiSuggestion {
            replacement: format!("/{} ", command.name),
            label: format!("/{}", command.name),
            detail: command.description,
            accent_color: match command.source {
                CommandSource::Builtin => "36",
                CommandSource::Coding => "32",
                CommandSource::Skill => "35",
                CommandSource::Mcp => "34",
                CommandSource::Plugin => "33",
            },
        })
        .collect()
}

fn heuristic_tui_suggestions(app_state: &AppState, input: &str) -> Option<Vec<TuiSuggestion>> {
    let trimmed = input.trim_end();
    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let args = parts.collect::<Vec<_>>();
    let trailing_space = input.ends_with(' ');
    match command {
        "/model" => Some(model_command_suggestions(args.as_slice(), trailing_space)),
        "/permissions" | "/perms" => Some(permissions_command_suggestions(args.as_slice(), trailing_space)),
        "/plan" => Some(plan_command_suggestions(args.as_slice(), trailing_space)),
        "/plugins" => Some(plugins_command_suggestions(app_state, args.as_slice(), trailing_space)),
        "/mcp" => Some(mcp_command_suggestions(args.as_slice(), trailing_space)),
        "/swarm" => Some(swarm_command_suggestions(app_state, args.as_slice(), trailing_space)),
        "/computer" => Some(computer_command_suggestions(args.as_slice(), trailing_space)),
        "/LisM" | "/lism" => Some(lism_command_suggestions(args.as_slice(), trailing_space)),
        "/UM" | "/um" => Some(um_command_suggestions(args.as_slice(), trailing_space)),
        _ => None,
    }
}

fn model_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/model list ", "list", "Show configured model levels", "33"),
        heuristic_suggestion("/model show ", "show", "Show level -> profile mapping", "33"),
        heuristic_suggestion("/model use ", "use", "Switch session or workspace model level", "33"),
        heuristic_suggestion("/model clear ", "clear", "Clear model override", "33"),
        heuristic_suggestion("/model reload ", "reload", "Reload models.toml registry", "33"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }
    match args[0] {
        "use" => model_use_suggestions(&args[1..], trailing_space),
        "clear" => model_clear_suggestions(&args[1..], trailing_space),
        _ => base,
    }
}

fn model_use_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    if args.is_empty() || (args.len() == 1 && !trailing_space) {
        let query = args.first().copied().unwrap_or("").to_ascii_lowercase();
        let mut suggestions = model_level_suggestions();
        if !query.is_empty() {
            suggestions.retain(|suggestion| {
                suggestion.label.to_ascii_lowercase().contains(&query)
                    || suggestion.detail.to_ascii_lowercase().contains(&query)
            });
        }
        return suggestions;
    }

    let selected_level = args[0];
    let option_query = args.get(1).copied().unwrap_or("");
    if option_query.is_empty() && trailing_space {
        return Vec::new();
    }

    if option_query.starts_with('-') || "--workspace".starts_with(option_query) {
        let replacement = format!("/model use {selected_level} --workspace");
        return vec![heuristic_suggestion(
            format!("{replacement} "),
            "--workspace",
            "Persist this level as the workspace default for future sessions",
            "35",
        )];
    }

    Vec::new()
}

fn model_clear_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    if args.is_empty() && !trailing_space {
        return vec![heuristic_suggestion(
            "/model clear --workspace ",
            "--workspace",
            "Clear the workspace default and fall back to registry active_level",
            "35",
        )];
    }

    if args.len() == 1 && !trailing_space && "--workspace".starts_with(args[0]) {
        return vec![heuristic_suggestion(
            "/model clear --workspace ",
            "--workspace",
            "Clear the workspace default and fall back to registry active_level",
            "35",
        )];
    }

    Vec::new()
}

fn permissions_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/permissions show ", "show", "Show mode, rules, and pending approval", "33"),
        heuristic_suggestion("/permissions mode ", "mode", "Switch permission mode", "33"),
        heuristic_suggestion("/permissions allow ", "allow", "Add always-allow rules", "32"),
        heuristic_suggestion("/permissions deny ", "deny", "Add always-deny rules", "31"),
        heuristic_suggestion("/permissions ask ", "ask", "Add always-ask rules", "35"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }
    match args[0] {
        "mode" => filter_suggestions(
            vec![
                heuristic_suggestion("/permissions mode default ", "default", "Standard interactive approvals", "36"),
                heuristic_suggestion("/permissions mode plan ", "plan", "Plan-only mode with restricted execution", "36"),
                heuristic_suggestion("/permissions mode accept-edits ", "accept-edits", "Auto-accept file edits only", "36"),
                heuristic_suggestion("/permissions mode bypass ", "bypass", "Bypass permission prompts", "36"),
            ],
            args.get(1).copied().unwrap_or(""),
        ),
        "allow" | "deny" | "ask" => filter_suggestions(
            vec![
                heuristic_suggestion(format!("/permissions {} Bash ", args[0]), "Bash", "Terminal commands", "32"),
                heuristic_suggestion(format!("/permissions {} Edit ", args[0]), "Edit", "File edits", "32"),
                heuristic_suggestion(format!("/permissions {} Read ", args[0]), "Read", "File reads", "32"),
                heuristic_suggestion(format!("/permissions {} WebSearch ", args[0]), "WebSearch", "Web search access", "32"),
            ],
            args.get(1).copied().unwrap_or(""),
        ),
        _ => base,
    }
}

fn plan_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/plan status ", "status", "Show current plan mode status", "33"),
        heuristic_suggestion("/plan show ", "show", "Show current plan steps", "33"),
        heuristic_suggestion("/plan history ", "history", "Show plan history", "33"),
        heuristic_suggestion("/plan add ", "add", "Add a plan step", "32"),
        heuristic_suggestion("/plan update ", "update", "Update a plan step", "32"),
        heuristic_suggestion("/plan done ", "done", "Mark a step complete", "32"),
        heuristic_suggestion("/plan reorder ", "reorder", "Reorder existing steps", "32"),
        heuristic_suggestion("/plan enter ", "enter", "Enter plan mode", "35"),
        heuristic_suggestion("/plan exit ", "exit", "Exit plan mode", "35"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }
    match args[0] {
        "add" => vec![heuristic_suggestion(
            "/plan add <title> | <details> ",
            "<title> | <details>",
            "Example: /plan add Fix TUI flicker | dedupe redraw path",
            "32",
        )],
        "update" => vec![heuristic_suggestion(
            "/plan update <step-id>|<title>|<details or ->|<status> ",
            "<step-id>|<title>|<details>|<status>",
            "Use status pending, in_progress, or completed",
            "32",
        )],
        "done" => vec![heuristic_suggestion(
            "/plan done <step-id> ",
            "<step-id>",
            "Mark a single plan step completed",
            "32",
        )],
        "reorder" => vec![heuristic_suggestion(
            "/plan reorder <step-id> <step-id> ",
            "<step-id> <step-id> ...",
            "Provide the desired full ordering of step ids",
            "32",
        )],
        "enter" => vec![heuristic_suggestion(
            "/plan enter <reason> ",
            "<reason>",
            "Optional reason shown in plan-mode state",
            "35",
        )],
        "exit" => vec![heuristic_suggestion(
            "/plan exit <summary> ",
            "<summary>",
            "Optional summary when leaving plan mode",
            "35",
        )],
        _ => base,
    }
}

fn plugins_command_suggestions(app_state: &AppState, args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/plugins list ", "list", "List plugin inventory and state", "33"),
        heuristic_suggestion("/plugins show ", "show", "Show one plugin in detail", "33"),
        heuristic_suggestion("/plugins diagnostics ", "diagnostics", "Show plugin diagnostics", "33"),
        heuristic_suggestion("/plugins reload ", "reload", "Reload one plugin or all plugins", "35"),
        heuristic_suggestion("/plugins enable ", "enable", "Enable a plugin", "32"),
        heuristic_suggestion("/plugins disable ", "disable", "Disable a plugin", "31"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }

    let plugin_names = app_state
        .plugin_load_result
        .as_ref()
        .map(|result| result.plugins.iter().map(|plugin| plugin.name.clone()).collect::<Vec<_>>())
        .unwrap_or_default();

    match args[0] {
        "show" | "enable" | "disable" => named_target_suggestions(
            format!("/plugins {} ", args[0]),
            &plugin_names,
            args.get(1).copied().unwrap_or(""),
            if args[0] == "enable" { "32" } else if args[0] == "disable" { "31" } else { "33" },
            "Installed plugin",
        ),
        "diagnostics" => {
            let mut suggestions = named_target_suggestions(
                "/plugins diagnostics ".to_string(),
                &plugin_names,
                args.get(1).copied().unwrap_or(""),
                "33",
                "Show diagnostics for one plugin",
            );
            suggestions.insert(0, heuristic_suggestion("/plugins diagnostics ", "all", "Show diagnostics for all plugins", "33"));
            suggestions
        }
        "reload" => {
            let mut suggestions = named_target_suggestions(
                "/plugins reload ".to_string(),
                &plugin_names,
                args.get(1).copied().unwrap_or(""),
                "35",
                "Reload this plugin only",
            );
            suggestions.insert(0, heuristic_suggestion("/plugins reload all ", "all", "Reload all plugins", "35"));
            suggestions
        }
        _ => base,
    }
}

fn mcp_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/mcp list ", "list", "List configured MCP servers", "33"),
        heuristic_suggestion("/mcp status ", "status", "Alias of list with runtime status", "33"),
        heuristic_suggestion("/mcp connect ", "connect", "Connect one MCP server", "32"),
        heuristic_suggestion("/mcp disconnect ", "disconnect", "Disconnect one MCP server", "31"),
        heuristic_suggestion("/mcp reconnect ", "reconnect", "Reconnect one MCP server", "35"),
        heuristic_suggestion("/mcp approve ", "approve", "Approve MCP governance for a server", "32"),
        heuristic_suggestion("/mcp deny ", "deny", "Deny MCP governance for a server", "31"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }
    match args[0] {
        "connect" | "disconnect" | "reconnect" | "approve" => vec![heuristic_suggestion(
            format!("/mcp {} <server> ", args[0]),
            "<server>",
            "Enter the MCP server id or display name",
            if matches!(args[0], "disconnect") { "31" } else { "35" },
        )],
        "deny" => vec![heuristic_suggestion(
            "/mcp deny <server> <reason> ".to_string(),
            "<server> <reason>",
            "Optionally include a denial reason after the server id",
            "31",
        )],
        _ => base,
    }
}

fn swarm_command_suggestions(app_state: &AppState, args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/swarm status ", "status", "Show active swarm tasks", "33"),
        heuristic_suggestion("/swarm teammates ", "teammates", "List available teammate profiles", "33"),
        heuristic_suggestion("/swarm spawn ", "spawn", "Spawn a teammate for a task", "35"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }
    if args[0] != "spawn" {
        return base;
    }

    let teammate_ids = load_tui_teammate_ids(app_state);
    if args.len() == 1 || (args.len() == 2 && !trailing_space) {
        let query = args.get(1).copied().unwrap_or("");
        let mut suggestions = named_target_suggestions(
            "/swarm spawn ".to_string(),
            &teammate_ids,
            query,
            "35",
            "Spawn this teammate and then enter a task description",
        );
        if suggestions.is_empty() {
            suggestions.push(heuristic_suggestion(
                "/swarm spawn <teammate_id> <task description> ",
                "<teammate_id> <task description>",
                "Example: /swarm spawn reviewer audit renderer regressions",
                "35",
            ));
        }
        return suggestions;
    }
    vec![heuristic_suggestion(
        format!("/swarm spawn {} <task description> ", args[1]),
        "<task description>",
        "Describe the concrete task for the selected teammate",
        "35",
    )]
}

fn computer_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    let base = vec![
        heuristic_suggestion("/computer screenshot ", "screenshot", "Capture the current screen", "33"),
        heuristic_suggestion("/computer click ", "click", "Click at absolute screen coordinates", "31"),
        heuristic_suggestion("/computer move ", "move", "Move the pointer without clicking", "35"),
        heuristic_suggestion("/computer stop ", "stop", "Stop the active computer control session", "31"),
    ];
    if args.is_empty() {
        return base;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(base, args[0]);
    }
    match args[0] {
        "click" | "move" => vec![heuristic_suggestion(
            format!("/computer {} <x> <y> ", args[0]),
            "<x> <y>",
            "Absolute screen coordinates, for example 640 480",
            if args[0] == "click" { "31" } else { "35" },
        )],
        _ => base,
    }
}

fn lism_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    simple_subcommand_suggestions(
        args,
        trailing_space,
        vec![
            heuristic_suggestion("/LisM on ", "on", "Enable session-level Less-is-More mode", "32"),
            heuristic_suggestion("/LisM off ", "off", "Disable session-level Less-is-More mode", "31"),
            heuristic_suggestion("/LisM status ", "status", "Show LisM status and routed metadata", "33"),
            heuristic_suggestion("/LisM explain ", "explain", "Show LisM building blocks and deferred items", "35"),
        ],
    )
}

fn um_command_suggestions(args: &[&str], trailing_space: bool) -> Vec<TuiSuggestion> {
    simple_subcommand_suggestions(
        args,
        trailing_space,
        vec![
            heuristic_suggestion("/UM on ", "on", "Enable shared step memory", "32"),
            heuristic_suggestion("/UM off ", "off", "Disable shared step memory", "31"),
            heuristic_suggestion("/UM status ", "status", "Show shared step memory status", "33"),
        ],
    )
}

fn simple_subcommand_suggestions(
    args: &[&str],
    trailing_space: bool,
    suggestions: Vec<TuiSuggestion>,
) -> Vec<TuiSuggestion> {
    if args.is_empty() {
        return suggestions;
    }
    if args.len() == 1 && !trailing_space {
        return filter_suggestions(suggestions, args[0]);
    }
    suggestions
}

fn filter_suggestions(mut suggestions: Vec<TuiSuggestion>, query: &str) -> Vec<TuiSuggestion> {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return suggestions;
    }
    suggestions.retain(|suggestion| {
        suggestion.label.to_ascii_lowercase().contains(&query)
            || suggestion.detail.to_ascii_lowercase().contains(&query)
    });
    suggestions
}

fn named_target_suggestions(
    prefix: String,
    names: &[String],
    query: &str,
    accent_color: &'static str,
    detail: &str,
) -> Vec<TuiSuggestion> {
    if names.is_empty() {
        return vec![heuristic_suggestion(
            format!("{prefix}<name> "),
            "<name>",
            detail,
            accent_color,
        )];
    }
    filter_suggestions(
        names.iter()
            .map(|name| heuristic_suggestion(format!("{prefix}{name} "), name.clone(), detail, accent_color))
            .collect(),
        query,
    )
}

fn load_tui_teammate_ids(app_state: &AppState) -> Vec<String> {
    let cwd = app_state.current_working_directory();
    let Ok(config_root) = crate::bootstrap::config_root::resolve_config_root(&cwd) else {
        return Vec::new();
    };
    crate::bootstrap::teammate_registry::load_teammate_registry_from_root(&config_root)
        .ok()
        .flatten()
        .map(|registry| registry.profiles.into_iter().map(|profile| profile.id).collect())
        .unwrap_or_default()
}

fn heuristic_suggestion(
    replacement: impl Into<String>,
    label: impl Into<String>,
    detail: impl Into<String>,
    accent_color: &'static str,
) -> TuiSuggestion {
    TuiSuggestion {
        replacement: replacement.into(),
        label: label.into(),
        detail: detail.into(),
        accent_color,
    }
}

fn model_level_suggestions() -> Vec<TuiSuggestion> {
    let registry = load_tui_model_registry();
    [
        (ModelLevel::Low, "32"),
        (ModelLevel::Medium, "36"),
        (ModelLevel::High, "33"),
        (ModelLevel::Xhigh, "31"),
    ]
    .into_iter()
    .map(|(level, accent_color)| {
        let combo_summary = registry
            .as_ref()
            .and_then(|registry| registry.levels.get(&level).map(|profile| (registry, profile)))
            .and_then(|(registry, profile_name)| registry.profiles.get(profile_name).map(|spec| (profile_name, spec)))
            .and_then(|(profile_name, spec)| {
                crate::bootstrap::model_profiles::build_model_profile_display_view(profile_name, spec)
                    .ok()
                    .map(|view| format!("Mapped to {} via {}", view.model, view.provider_id))
            });
        let detail = match combo_summary {
            Some(summary) => format!("{} · {}", model_level_combo_copy(level), summary),
            None => model_level_combo_copy(level).to_string(),
        };
        heuristic_suggestion(
            format!("/model use {} ", level.as_str()),
            level.as_str(),
            detail,
            accent_color,
        )
    })
    .collect()
}

fn model_level_combo_copy(level: ModelLevel) -> &'static str {
    match level {
        ModelLevel::Low => "Fastest and most cost-efficient. Best for quick questions, small edits, and rapid iteration.",
        ModelLevel::Medium => "Balanced speed and quality. Best default for everyday coding, review, and analysis.",
        ModelLevel::High => "Stronger reasoning with more consistency. Best for larger changes, debugging, and refactors.",
        ModelLevel::Xhigh => "Maximum capability for the hardest work. Best for multi-step tasks, ambiguous problems, and deep investigations.",
    }
}

fn load_tui_model_registry() -> Option<crate::bootstrap::model_profiles::ModelProfileRegistry> {
    let workspace_root = std::env::current_dir().ok()?;
    let home_root = preferred_home_config_root();
    let home_registry = match home_root.as_ref() {
        Some(path) if path != &workspace_root => load_model_profiles_registry_from_root(path).ok().flatten(),
        _ => None,
    };
    let workspace_registry = load_model_profiles_registry_from_root(&workspace_root).ok().flatten();
    merge_model_profiles_registry(home_registry.as_ref(), workspace_registry.as_ref())
}

fn command_match_score(command: &CommandMetadata, query: &str) -> i32 {
    if query.is_empty() {
        return default_command_priority(command);
    }

    let name = command.name.to_ascii_lowercase();
    if name == query {
        return 10_000;
    }
    if command.aliases.iter().any(|alias| alias.eq_ignore_ascii_case(query)) {
        return 9_000;
    }
    if name.starts_with(query) {
        return 7_000;
    }
    if command
        .aliases
        .iter()
        .any(|alias| alias.to_ascii_lowercase().starts_with(query))
    {
        return 6_000;
    }
    if name.contains(query) {
        return 4_000;
    }
    if command.category.to_ascii_lowercase().contains(query) {
        return 2_000;
    }
    if command.description.to_ascii_lowercase().contains(query) {
        return 1_500;
    }
    if command
        .aliases
        .iter()
        .any(|alias| alias.to_ascii_lowercase().contains(query))
    {
        return 1_000;
    }
    0
}

fn default_command_priority(command: &CommandMetadata) -> i32 {
    match command.name.as_str() {
        "help" => 1000,
        "status" => 950,
        "model" => 900,
        "permissions" => 850,
        "plan" => 800,
        "resume" => 780,
        "clear" => 760,
        "compact" => 740,
        "context" => 720,
        "diff" => 700,
        "tasks" => 680,
        "config" => 660,
        _ => match command.source {
            CommandSource::Builtin => 500,
            CommandSource::Coding => 450,
            CommandSource::Skill => 350,
            CommandSource::Mcp => 300,
            CommandSource::Plugin => 250,
        },
    }
}

fn autocomplete_slash_command(
    input: &str,
    suggestions: &[TuiSuggestion],
    selected_suggestion: usize,
) -> Option<String> {
    if !input.starts_with('/') {
        return None;
    }
    let selected = suggestions.get(selected_suggestion)?;
    let current = input.trim_end();
    let replacement = selected.replacement.trim_end();
    if replacement == current || !replacement.starts_with(current) {
        None
    } else {
        Some(selected.replacement.clone())
    }
}

fn apply_selected_suggestion(
    input: &str,
    suggestions: &[TuiSuggestion],
    selected_suggestion: usize,
) -> Option<String> {
    if !input.starts_with('/') {
        return None;
    }
    suggestions
        .get(selected_suggestion)
        .map(|suggestion| suggestion.replacement.clone())
}

fn render_command_suggestion_line(suggestion: &TuiSuggestion, selected: bool) -> String {
    let label = colorize_ansi(&suggestion.label, suggestion.accent_color);
    let body = format!("{} {}", label, colorize_ansi(&suggestion.detail, "2;37"));

    if selected {
        colorize_ansi(&format!("> {body}"), "1;97;44")
    } else {
        format!("  {body}")
    }
}

fn colorize_ansi(text: &str, code: &str) -> String {
    format!("\x1b[{code}m{text}\x1b[0m")
}

fn normalize_tui_newlines(text: &str) -> String {
    text.replace('\n', "\r\n")
}

#[cfg(test)]
mod tui_output_tests {
    use super::{
        heuristic_tui_suggestions, normalize_tui_newlines, render_command_suggestion_line,
    };
    use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
    use crate::command::registry::CommandRegistry;
    use crate::cost::tracker::CostTracker;
    use crate::interaction::dispatcher::NotificationDispatcher;
    use crate::interaction::telegram::gateway::TelegramGateway;
    use crate::security::audit::AuditLog;
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::app_state::{ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole};
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn tui_newlines_are_crlf_normalized() {
        assert_eq!(normalize_tui_newlines("a\nb\nc"), "a\r\nb\r\nc");
    }

    fn test_app_state() -> AppState {
        AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context: ToolPermissionContext::new(PermissionMode::Default),
            command_registry: Some(Arc::new(CommandRegistry::new())),
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(Mutex::new(AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "default-provider".into(),
                protocol: "MessagesApi".into(),
                compatibility_profile: "MessagesApi".into(),
                base_url_host: "localhost".into(),
                model: "default-model".into(),
                auth_status: "none".into(),
            },
            active_session_id: "tui-suggestions".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(AtomicU64::new(0)),
            cancellation_token: CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
        }
    }

    #[test]
    fn tui_model_command_shows_use_suggestion_with_detail() {
        let app_state = test_app_state();
        let suggestions = heuristic_tui_suggestions(&app_state, "/model").expect("model suggestions");
        assert!(suggestions.iter().any(|item| item.label == "use" && !item.detail.is_empty()));
    }

    #[test]
    fn tui_model_enter_autocomplete_advances_to_use_stage() {
        let app_state = test_app_state();
        let suggestions = heuristic_tui_suggestions(&app_state, "/model")
            .expect("model suggestions");
        let use_index = suggestions
            .iter()
            .position(|item| item.label == "use")
            .expect("use suggestion present");
        let completed = super::autocomplete_slash_command("/model", &suggestions, use_index)
            .expect("enter should autocomplete to use stage");
        assert_eq!(completed, "/model use ");
    }

    #[test]
    fn tui_model_use_shows_combo_descriptions() {
        let app_state = test_app_state();
        let suggestions =
            heuristic_tui_suggestions(&app_state, "/model use ").expect("model use suggestions");
        let rendered = suggestions
            .iter()
            .map(|item| render_command_suggestion_line(item, false))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(suggestions.iter().any(|item| item.label == "low"));
        assert!(suggestions.iter().any(|item| item.label == "medium"));
        assert!(suggestions.iter().any(|item| item.label == "high"));
        assert!(suggestions.iter().any(|item| item.label == "xhigh"));
        assert!(rendered.contains("Fastest and most cost-efficient"));
        assert!(rendered.contains("Balanced speed and quality"));
        assert!(!rendered.contains("reasoning tier"));
        assert!(!rendered.contains("当前组合"));
    }

    #[test]
    fn tui_permissions_mode_shows_mode_choices_with_details() {
        let app_state = test_app_state();
        let suggestions = heuristic_tui_suggestions(&app_state, "/permissions mode ")
            .expect("permissions mode suggestions");
        assert!(suggestions.iter().any(|item| item.label == "default"));
        assert!(suggestions.iter().any(|item| item.label == "plan"));
        assert!(suggestions.iter().any(|item| item.label == "accept-edits"));
        assert!(suggestions.iter().any(|item| item.label == "bypass"));
        assert!(suggestions.iter().all(|item| !item.detail.is_empty()));
    }

    #[test]
    fn tui_plan_shows_structured_action_hints() {
        let app_state = test_app_state();
        let suggestions =
            heuristic_tui_suggestions(&app_state, "/plan ").expect("plan suggestions");
        assert!(suggestions.iter().any(|item| item.label == "status"));
        assert!(suggestions.iter().any(|item| item.label == "add"));
        assert!(suggestions.iter().any(|item| item.label == "update"));
        assert!(suggestions.iter().any(|item| item.label == "enter"));
    }

    #[test]
    fn tui_plugins_show_offers_named_target_placeholder_with_detail() {
        let app_state = test_app_state();
        let suggestions = heuristic_tui_suggestions(&app_state, "/plugins show ")
            .expect("plugins show suggestions");
        assert!(suggestions.iter().any(|item| item.label == "<name>"));
        assert!(suggestions.iter().all(|item| !item.detail.is_empty()));
    }

    #[test]
    fn tui_mcp_and_computer_show_parameter_hints() {
        let app_state = test_app_state();

        let mcp = heuristic_tui_suggestions(&app_state, "/mcp ")
            .expect("mcp suggestions");
        assert!(mcp.iter().any(|item| item.label == "connect"));
        assert!(mcp.iter().any(|item| item.label == "approve"));

        let computer = heuristic_tui_suggestions(&app_state, "/computer click ")
            .expect("computer click suggestions");
        assert!(computer.iter().any(|item| item.label == "<x> <y>"));
        assert!(computer.iter().all(|item| !item.detail.is_empty()));
    }

    #[test]
    fn tui_lism_um_and_swarm_show_subcommand_or_task_hints() {
        let app_state = test_app_state();

        let lism = heuristic_tui_suggestions(&app_state, "/LisM ")
            .expect("LisM suggestions");
        assert!(lism.iter().any(|item| item.label == "on"));
        assert!(lism.iter().any(|item| item.label == "status"));

        let um = heuristic_tui_suggestions(&app_state, "/UM ")
            .expect("UM suggestions");
        assert!(um.iter().any(|item| item.label == "off"));
        assert!(um.iter().any(|item| item.label == "status"));

        let swarm = heuristic_tui_suggestions(&app_state, "/swarm spawn ")
            .expect("swarm suggestions");
        assert!(swarm.iter().any(|item| {
            item.label == "<teammate_id> <task description>"
                || item.label == "<name>"
                || item.label == "<task description>"
        }));
    }
}

fn preview_chars(value: &str, max_chars: usize) -> &str {
    match value.char_indices().nth(max_chars) {
        Some((idx, _)) => &value[..idx],
        None => value,
    }
}

fn diagnostic_preview(value: &str, max_chars: usize) -> String {
    let total = value.chars().count();
    if total <= max_chars {
        return value.to_string();
    }
    let half = max_chars / 2;
    let head = preview_chars(value, half);
    let tail_start = value
        .char_indices()
        .nth(total.saturating_sub(half))
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    format!(
        "{head}\n[truncated: {} chars omitted]\n{}",
        total.saturating_sub(max_chars),
        &value[tail_start..]
    )
}

const DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_MS: u64 = 1_500;
const DEFAULT_BOSS_TASK_TIMEOUT_SECS: u64 = 900;

fn terminal_tail_stalled(
    sync_result: &anyhow::Result<bool>,
    _terminal_result: &anyhow::Result<Option<String>>,
    live_tail_task: bool,
) -> bool {
    if live_tail_task {
        return false;
    }
    sync_result.as_ref().map(|value| !*value).unwrap_or(true)
}

fn task_is_terminal(
    task_manager: &crate::task::manager::TaskManager,
    task_id: Option<&str>,
) -> bool {
    task_id.is_some_and(|tid| {
        matches!(
            task_manager.status(tid),
            Some(
                crate::task::types::TaskStatus::Completed
                    | crate::task::types::TaskStatus::Failed
                    | crate::task::types::TaskStatus::Killed
            )
        )
    })
}

fn step_terminal_from_tracked_ids(
    task_manager: &crate::task::manager::TaskManager,
    b_task_id: Option<&str>,
    current_step_task_id: Option<&str>,
) -> bool {
    task_is_terminal(task_manager, b_task_id)
        || task_is_terminal(task_manager, current_step_task_id)
}

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
    /// Override the worker LisM policy for boss-spawned execution workers.
    /// One of: inherit, force-on, force-off.
    #[arg(long, value_name = "POLICY")]
    pub worker_lism_policy: Option<String>,
    /// Enable test-first mode for development tasks.
    #[arg(long = "st", default_value_t = false)]
    pub st_mode: bool,
    /// Enable shared step memory for verification-first boss flows.
    #[arg(long, default_value_t = false)]
    pub shared_memory_enabled: bool,
    /// Experimental: disable the Boss LisM escape hatch that falls back to full worker dispatch
    /// when a step appears to require filesystem or shell side effects.
    #[arg(long, default_value_t = false)]
    pub disable_full_worker_dispatch_fallback: bool,
    /// Run a single boss task non-interactively. Creates a one-step plan, executes it
    /// to completion, records the LisM A/B sample if --lism-ab-sample is set, then exits.
    #[arg(long, value_name = "TASK")]
    pub boss_task: Option<String>,
    /// Timeout for non-interactive --boss-task polling. Default is 900 seconds.
    #[arg(long, value_name = "SECONDS", default_value_t = DEFAULT_BOSS_TASK_TIMEOUT_SECS)]
    pub boss_task_timeout_secs: u64,
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
            worker_lism_policy: None,
            st_mode: false,
            shared_memory_enabled: false,
            disable_full_worker_dispatch_fallback: false,
            boss_task: None,
            boss_task_timeout_secs: DEFAULT_BOSS_TASK_TIMEOUT_SECS,
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
                // Poll until completion or terminal failure.
                let timeout = std::time::Duration::from_secs(self.cli.boss_task_timeout_secs);
                let deadline = std::time::Instant::now() + timeout;
                let mut tick = 0u32;
                loop {
                    if let Some(task_manager) = app_arc.permission_context.task_manager.as_ref() {
                        for event in task_manager.drain_events(&app_arc.active_session_id) {
                            let _ = boss.on_task_event(&event).await;
                        }
                    }
                    let stage = boss.get_stage().await;
                    if matches!(stage, crate::core::boss_state::BossStage::Completed) {
                        break;
                    }
                    if boss.has_terminal_failure().await {
                        println!("[boss-task] boss plan reached terminal failure — stopping poll");
                        let terminal_msg = boss.advance_plan(&app_arc).await;
                        println!(
                            "[boss-task] terminal advance_plan result: {:?}",
                            terminal_msg
                        );
                        break;
                    }
                    // If the tracked B task is terminal, keep draining events until Boss catches up.
                    let step_terminal = if let Some(task_manager) =
                        app_arc.permission_context.task_manager.as_ref()
                    {
                        let b_task_id = boss.b_task_id().await;
                        let current_step_task_id = boss.current_step_worker_task_id().await;
                        step_terminal_from_tracked_ids(
                            task_manager,
                            b_task_id.as_deref(),
                            current_step_task_id.as_deref(),
                        )
                    } else {
                        false
                    };
                    if step_terminal {
                        println!(
                            "[boss-task] step task reached terminal status; syncing boss state"
                        );
                        let synced = if let Some(task_manager) =
                            app_arc.permission_context.task_manager.as_ref()
                        {
                            let synced = boss.sync_terminal_child_task_state(task_manager).await;
                            println!("[boss-task] terminal child sync result: {:?}", synced);
                            synced
                        } else {
                            Ok(false)
                        };
                        let terminal_msg = boss.advance_plan(&app_arc).await;
                        println!(
                            "[boss-task] terminal advance_plan result: {:?}",
                            terminal_msg
                        );
                        if matches!(
                            boss.get_stage().await,
                            crate::core::boss_state::BossStage::Completed
                        ) {
                            break;
                        }
                        let live_tail_task = if let Some(task_manager) =
                            app_arc.permission_context.task_manager.as_ref()
                        {
                            let b_task_live = boss
                                .b_task_id()
                                .await
                                .is_some_and(|tid| !task_is_terminal(task_manager, Some(&tid)));
                            let current_step_live = boss
                                .current_step_worker_task_id()
                                .await
                                .is_some_and(|tid| !task_is_terminal(task_manager, Some(&tid)));
                            b_task_live || current_step_live
                        } else {
                            false
                        };
                        if terminal_tail_stalled(&synced, &terminal_msg, live_tail_task) {
                            let run_id = boss.current_run_id().await;
                            let lism_enabled = crate::core::boss::effective_lism_enabled(
                                boss.lism_policy().await,
                                app_arc.permission_context.lism_enabled(),
                            );
                            boss.emit_lism_sample_once(
                                &run_id,
                                lism_enabled,
                                crate::core::boss_test_readiness::BossTestRunOutcome::Aborted,
                                0,
                            )
                            .await;
                            println!(
                                "[boss-task] terminal tail stalled after child completion; emitted terminal sample"
                            );
                            break;
                        }
                    }
                    if std::time::Instant::now() >= deadline {
                        println!(
                            "[boss-task] timed out after {} seconds",
                            self.cli.boss_task_timeout_secs
                        );
                        let run_id = boss.current_run_id().await;
                        let lism_enabled = crate::core::boss::effective_lism_enabled(
                            boss.lism_policy().await,
                            app_arc.permission_context.lism_enabled(),
                        );
                        boss.emit_lism_sample_once(
                            &run_id,
                            lism_enabled,
                            crate::core::boss_test_readiness::BossTestRunOutcome::Aborted,
                            0,
                        )
                        .await;
                        break;
                    }
                    tick += 1;
                    if tick % 20 == 0 {
                        let b_task_id = boss.b_task_id().await;
                        if let Some(tid) = b_task_id {
                            if let Some(task_manager) =
                                app_arc.permission_context.task_manager.as_ref()
                            {
                                println!(
                                    "[boss-task] b_task {} status: {:?}",
                                    tid,
                                    task_manager.status(&tid)
                                );
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
                        println!(
                            "[boss-task] b_task {} status: {:?}",
                            b_id,
                            task_manager.status(&b_id)
                        );
                        if let Some(slice) = task_manager.get_output(&b_id, 0) {
                            println!(
                                "[boss-task] b_task output (diagnostic head/tail): {:?}",
                                diagnostic_preview(&slice.content, 4_000)
                            );
                        }
                    }
                    if let Ok(report) = boss.report_progress(task_manager).await {
                        if let Some(obs) = &report.observability_summary {
                            println!(
                                "[boss-task] cache_hit_observed: {}",
                                obs.cache_hit_observed()
                            );
                            println!(
                                "[boss-task] cache_read_tokens: {}",
                                obs.total_cache_read_tokens
                            );
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
                self.run_interactive_tui(&router, &engine, &app_state).await?;
            } else {
                for line in io::stdin().lock().lines() {
                    let line = line?;
                    let output = handle_cli_input(&router, &engine, &app_state, line).await?;
                    self.print_cli_turn_output(&output);
                }
            }
            return Ok(());
        }

        let output = handle_cli_input(&router, &engine, &app_state, "/help").await?;
        self.print_cli_turn_output(&output);
        Ok(())
    }

    fn print_cli_turn_output(&self, output: &CliTurnOutput) {
        let document = render_turn_document(output);
        if self.cli.tui {
            self.write_tui_frame(format!(
                "{}{}",
                tui_clear_screen_prefix(),
                render_document_tui_output(&document)
            ));
        } else {
            println!("{}", render_document_output(&document));
        }
    }

    fn print_tui_welcome(&self) {
        let document = render_turn_document(&CliTurnOutput {
            primary_text: String::new(),
            events: vec![],
        });
        self.write_tui_frame(format!(
            "{}{}",
            tui_clear_screen_prefix(),
            render_document_tui_output(&document)
        ));
    }

    fn print_tui_message(&self, message: &str) {
        let mut screen = build_tui_screen(&render_turn_document(&CliTurnOutput {
            primary_text: String::new(),
            events: vec![],
        }));
        screen.footer = vec![message.to_string()];
        self.write_tui_frame(format!(
            "{}{}",
            tui_clear_screen_prefix(),
            render_tui_screen_output(&screen)
        ));
    }

    fn print_tui_loading_frame(&self, request: &str, frame_index: usize) {
        self.write_tui_frame(format!(
            "{}{}",
            tui_clear_screen_prefix(),
            render_tui_screen_output(&build_tui_loading_screen(request, frame_index))
        ));
    }

    async fn handle_tui_input_with_loading(
        &self,
        router: &CommandRouter,
        engine: &QueryEngine,
        app_state: &AppState,
        line: String,
        on_update: impl FnMut(&CliTurnOutput),
    ) -> anyhow::Result<CliTurnOutput> {
        self.print_tui_loading_frame(&line, 0);
        handle_cli_input_streaming(router, engine, app_state, line, on_update).await
    }

    async fn run_interactive_tui(
        &self,
        router: &CommandRouter,
        engine: &QueryEngine,
        app_state: &AppState,
    ) -> anyhow::Result<()> {
        let _raw_mode = TuiRawModeGuard::activate()?;
        let mut current_document = render_turn_document(&CliTurnOutput {
            primary_text: String::new(),
            events: vec![],
        });
        let mut input = String::new();
        let mut selected_suggestion = 0usize;

        loop {
            let suggestions = tui_command_suggestions(app_state, &input);
            if selected_suggestion >= suggestions.len() {
                selected_suggestion = 0;
            }
            self.print_tui_interactive_frame(
                &current_document,
                &input,
                &suggestions,
                selected_suggestion,
            );

            let Event::Key(key) = read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }

            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.print_tui_message("Exiting TUI session.");
                    execute_runtime_shutdown(app_state.clone(), "interactive_exit").await;
                    break;
                }
                KeyCode::Esc => {
                    input.clear();
                    selected_suggestion = 0;
                }
                KeyCode::Enter => {
                    if input.trim().is_empty() {
                        continue;
                    }
                    if let Some(completed) = autocomplete_slash_command(
                        &input,
                        &suggestions,
                        selected_suggestion,
                    ) {
                        input = completed;
                        continue;
                    }

                    let line = input.trim().to_string();
                    input.clear();
                    selected_suggestion = 0;

                    if self.should_exit_tui_input(&line) {
                        self.print_tui_message("Exiting TUI session.");
                        execute_runtime_shutdown(app_state.clone(), "interactive_exit").await;
                        break;
                    }

                    let output = self
                        .handle_tui_input_with_loading(router, engine, app_state, line, |snapshot| {
                            let next_document = render_turn_document(snapshot);
                            if next_document != current_document {
                                current_document = next_document;
                                self.print_tui_interactive_frame(&current_document, "", &[], 0);
                            }
                        })
                        .await?;
                    current_document = render_turn_document(&output);
                }
                KeyCode::Backspace => {
                    input.pop();
                    selected_suggestion = 0;
                }
                KeyCode::Tab => {
                    if let Some(completed) = apply_selected_suggestion(
                        &input,
                        &suggestions,
                        selected_suggestion,
                    ) {
                        input = completed;
                    }
                }
                KeyCode::Up => {
                    if !suggestions.is_empty() {
                        selected_suggestion =
                            (selected_suggestion + suggestions.len() - 1) % suggestions.len();
                    }
                }
                KeyCode::Down => {
                    if !suggestions.is_empty() {
                        selected_suggestion = (selected_suggestion + 1) % suggestions.len();
                    }
                }
                KeyCode::Char(ch) => {
                    if !key.modifiers.contains(KeyModifiers::CONTROL) {
                        input.push(ch);
                        selected_suggestion = 0;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn print_tui_interactive_frame(
        &self,
        document: &crate::interaction::cli::renderer::RenderDocument,
        input: &str,
        suggestions: &[TuiSuggestion],
        selected_suggestion: usize,
    ) {
        let mut screen = build_tui_screen(document);
        screen.prompt = vec![format!(
            "{} {}",
            colorize_ansi(">", "1;36"),
            if input.is_empty() { "" } else { input }
        )
        .trim_end()
        .to_string()];

        if input.starts_with('/') {
            let lines = if suggestions.is_empty() {
                vec!["No matching commands".into()]
            } else {
                suggestions
                    .iter()
                    .enumerate()
                    .map(|(index, command)| {
                        render_command_suggestion_line(command, index == selected_suggestion)
                    })
                    .collect()
            };
            screen.panels.push(crate::interaction::cli::renderer::TuiPanelSection {
                title: "Commands".into(),
                lines,
            });
            screen.footer = vec![
                "Enter sends | Tab completes | Up/Down selects | Esc clears".into(),
                "Slash commands and arguments are suggested heuristically from local runtime context.".into(),
            ];
        }

        self.write_tui_frame(format!(
            "{}{}",
            tui_clear_screen_prefix(),
            render_tui_screen_output(&screen)
        ));
    }

    fn write_tui_frame(&self, rendered: String) {
        let normalized = normalize_tui_newlines(&rendered);
        let mut stdout = io::stdout().lock();
        let _ = stdout.write_all(normalized.as_bytes());
        let _ = stdout.flush();
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

        let skill_project_root = resolve_skill_project_root(&config_root, &state.current_cwd);
        let mut discovered_skills = bundled_skills();
        let mut skill_loader_cache = SkillLoaderCache::default();
        let (loaded_skills, _) = skill_loader_cache
            .load_or_reload(&skill_project_root)
            .unwrap_or_default();
        discovered_skills.extend(loaded_skills.skills);
        let skill_registry = Arc::new(SkillRegistry::new(discovered_skills));
        let service_observability_tracker = ServiceObservabilityTracker::default();
        if let Some(path) = std::env::var("RUST_AGENT_API_CALL_LOG")
            .ok()
            .filter(|value| !value.trim().is_empty())
        {
            if let Err(error) = service_observability_tracker.configure_api_call_log_path(&path) {
                tracing::warn!("Failed to open API call log path {}: {}", path, error);
            }
        }
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
        let runtime_tool_registry = Arc::new(RwLock::new(tool_inventory.assemble(
            ToolAssemblyContext::worker(state.surface, state.session_mode),
        )));

        let boss_runtime_host = BossRuntimeHost::new();
        let mut boss_coordinator =
            BossCoordinator::new_with_runtime_owner(boss_runtime_host.owner());

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

        // Apply boss-spawned worker LisM policy override if requested via CLI.
        if let Some(policy_str) = &self.cli.worker_lism_policy {
            boss_coordinator.init_worker_lism_policy(parse_worker_lism_policy(policy_str));
        }

        if self.cli.st_mode {
            boss_coordinator.init_st_mode_enabled(true);
        }

        if self.cli.shared_memory_enabled {
            boss_coordinator.init_shared_memory_enabled(true);
        }

        if self.cli.disable_full_worker_dispatch_fallback {
            boss_coordinator.init_full_worker_dispatch_fallback_enabled(false);
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
        if let Some(cap_config) = self.load_workspace_capability_config().unwrap_or_else(|e| {
            tracing::warn!("failed to load workspace capability config: {e}");
            None
        }) {
            permission_context = permission_context.with_workspace_capability(Arc::new(cap_config));
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
        let (
            provider_config,
            active_model_profile_name,
            active_model_level,
            active_model_profile_source,
        ) =
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
            active_level: active_model_level,
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
        if let Some(cap_config) = self.load_workspace_capability_config().unwrap_or_else(|e| {
            tracing::warn!("failed to load workspace capability config: {e}");
            None
        }) {
            permission_context = permission_context.with_workspace_capability(Arc::new(cap_config));
        }
        let last_activity_ts = Arc::new(std::sync::atomic::AtomicU64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ));
        let cancellation_token = CancellationToken::new();
        let mut active_model_snapshot = initialize_bundle.active_model_runtime.snapshot_blocking();
        if let Some(level) = resolved_session.model_level_override {
            let home_root = preferred_home_config_root();
            let cwd = std::path::PathBuf::from(resolved_session.snapshot.cwd.clone());
            let config_root = resolve_config_root(&cwd).ok();
            let home_registry = match (home_root.as_ref(), config_root.as_ref()) {
                (Some(path), Some(workspace_root)) if path != workspace_root => {
                    load_model_profiles_registry_from_root(path).ok().flatten()
                }
                (Some(path), None) => load_model_profiles_registry_from_root(path).ok().flatten(),
                _ => None,
            };
            let workspace_registry = config_root
                .as_ref()
                .and_then(|path| load_model_profiles_registry_from_root(path).ok().flatten());
            let merged_registry =
                merge_model_profiles_registry(home_registry.as_ref(), workspace_registry.as_ref());
            if let Some(registry) = merged_registry {
                if let Ok(resolved) = resolve_model_level_from_registry(&registry, level) {
                    active_model_snapshot = ActiveModelRuntimeSnapshot {
                        source: ActiveModelProfileSource::SessionOverride,
                        active_level: Some(level),
                        ..ActiveModelRuntimeSnapshot::from_resolved_profile(
                            &resolved,
                            initialize_bundle.api_client.observability_tracker(),
                        )
                    };
                }
            }
        }
        let active_model_runtime = ActiveModelRuntime::new(active_model_snapshot.clone());
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
            active_model_runtime: Some(active_model_runtime),
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
        let runtime_api_client = app_state
            .active_model_runtime
            .as_ref()
            .map(|runtime| runtime.snapshot_blocking().client)
            .unwrap_or_else(|| initialize_bundle.api_client.clone());
        let base_query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry: initial_snapshot.tool_registry.clone(),
            api_client: runtime_api_client,
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
        // Otherwise fall back to the user's managed config root, preferring `.morgo`
        // while still honoring an existing legacy `.claude` directory.
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
            let Some(path) = preferred_home_config_root() else {
                return Ok(None);
            };
            path
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

        // Look for workspace-capability.json in config root or the user's managed config root.
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
            let Some(path) = preferred_home_config_root() else {
                return Ok(None);
            };
            path
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
        Option<ModelLevel>,
        ActiveModelProfileSource,
    )> {
        if let Some(provider_config) = &self.provider_config_override {
            return Ok((
                provider_config.clone(),
                None,
                None,
                ActiveModelProfileSource::BootstrapDefault,
            ));
        }

        if has_explicit_provider_env_override() {
            let provider_config = self.build_model_provider_config_from_env()?;
            return Ok((provider_config, None, None, ActiveModelProfileSource::EnvOverride));
        }

        let home_root = preferred_home_config_root();
        let home_registry = match home_root.as_ref() {
            Some(path) if path != config_root => load_model_profiles_registry_from_root(path)?,
            _ => None,
        };
        let workspace_registry = load_model_profiles_registry_from_root(config_root)?;
        let merged_registry =
            merge_model_profiles_registry(home_registry.as_ref(), workspace_registry.as_ref());

        if let Some(registry) = merged_registry {
            let resolved = resolve_active_model_profile_from_registry(&registry)?;
            let source = if workspace_registry
                .as_ref()
                .is_some_and(|registry| registry.active_level.is_some() || registry.active.is_some())
            {
                ActiveModelProfileSource::WorkspaceModelsToml
            } else if home_registry
                .as_ref()
                .is_some_and(|registry| registry.active_level.is_some() || registry.active.is_some())
            {
                ActiveModelProfileSource::HomeModelsToml
            } else {
                ActiveModelProfileSource::ModelsToml
            };
            return Ok((
                resolved.config,
                Some(resolved.name),
                resolved.level,
                source,
            ));
        }

        let provider_config = self.build_model_provider_config_from_env()?;
        Ok((
            provider_config,
            None,
            None,
            ActiveModelProfileSource::BootstrapDefault,
        ))
    }

    fn build_model_provider_config_from_env(&self) -> anyhow::Result<ModelProviderConfig> {
        let provider_id = std::env::var("RUST_AGENT_PROVIDER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "morgo".into());
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

fn parse_worker_lism_policy(s: &str) -> WorkerLisMPolicy {
    match s.trim().to_lowercase().as_str() {
        "force-on" | "force_on" | "on" => WorkerLisMPolicy::ForceOn,
        "force-off" | "force_off" | "off" => WorkerLisMPolicy::ForceOff,
        _ => WorkerLisMPolicy::Inherit,
    }
}

fn resolve_skill_project_root(
    config_root: &std::path::Path,
    current_cwd: &std::path::Path,
) -> std::path::PathBuf {
    config_root
        .parent()
        .filter(|_| is_managed_config_root(config_root))
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| current_cwd.to_path_buf())
}

fn print_lism_ab_summary(
    summary: &crate::core::lism_ab_sample::LisMAbSummary,
    total_records: usize,
) {
    println!("LisM A/B Sample Summary");
    println!("=======================");
    println!("Total records : {total_records}");
    let on_cache_dist = summary
        .on_cache_read_tokens_distribution
        .as_ref()
        .map(|d| {
            format!(
                "p50={} p90={} max={} nz={}/{}",
                d.p50, d.p90, d.max, d.nonzero_count, d.sample_count
            )
        })
        .unwrap_or_else(|| "n/a".into());
    let off_cache_dist = summary
        .off_cache_read_tokens_distribution
        .as_ref()
        .map(|d| {
            format!(
                "p50={} p90={} max={} nz={}/{}",
                d.p50, d.p90, d.max, d.nonzero_count, d.sample_count
            )
        })
        .unwrap_or_else(|| "n/a".into());
    println!(
        "LisM ON       : {} runs | completion {:.2} | avg gross_input {} | avg uncached_input {} | avg output {} | hit_run_rate {} | avg cache_read {} | cache_read_dist {} | avg cost {}μ | avg tokens_saved {} | avg sent_chars {} | avg fallback/run {} | fallback_run_rate {} | avg hydration {} | avg missing {} | avg stale {} | hydration_rate {} | context_tiers {:?} | model_tiers {:?}",
        summary.on_runs,
        summary
            .on_completion_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.2}", r)),
        summary.on_avg_input_tokens,
        summary.on_avg_uncached_input_tokens,
        summary.on_avg_output_tokens,
        summary
            .on_hit_run_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.on_avg_cache_read_tokens,
        on_cache_dist,
        summary.on_avg_cost_micros_usd,
        summary.on_avg_tokens_saved,
        summary.on_avg_sent_prompt_chars,
        summary.on_avg_fallback_count,
        summary
            .on_fallback_run_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.on_avg_hydration_count,
        summary.on_avg_hydration_ref_missing,
        summary.on_avg_stale_ref_count,
        summary
            .on_hydration_resolution_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.on_context_tier_counts,
        summary.on_model_tier_counts,
    );
    println!(
        "LisM OFF      : {} runs | completion {:.2} | avg gross_input {} | avg uncached_input {} | avg output {} | hit_run_rate {} | avg cache_read {} | cache_read_dist {} | avg cost {}μ | avg tokens_saved {} | avg sent_chars {} | avg fallback/run {} | fallback_run_rate {} | avg hydration {} | avg missing {} | avg stale {} | hydration_rate {} | context_tiers {:?} | model_tiers {:?}",
        summary.off_runs,
        summary
            .off_completion_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.2}", r)),
        summary.off_avg_input_tokens,
        summary.off_avg_uncached_input_tokens,
        summary.off_avg_output_tokens,
        summary
            .off_hit_run_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.off_avg_cache_read_tokens,
        off_cache_dist,
        summary.off_avg_cost_micros_usd,
        summary.off_avg_tokens_saved,
        summary.off_avg_sent_prompt_chars,
        summary.off_avg_fallback_count,
        summary
            .off_fallback_run_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.off_avg_hydration_count,
        summary.off_avg_hydration_ref_missing,
        summary.off_avg_stale_ref_count,
        summary
            .off_hydration_resolution_rate
            .map_or_else(|| "n/a".into(), |r| format!("{:.3}", r)),
        summary.off_context_tier_counts,
        summary.off_model_tier_counts,
    );
    if summary.has_both_arms() {
        println!("---");
        if let Some(delta) = summary.hit_run_rate_delta() {
            let direction = if delta > 0.0 {
                "↑ LisM hits cache more often"
            } else {
                "↓ LisM hits cache less often"
            };
            println!("Δ hit_run_rate     : {:+.3} ({})", delta, direction);
        }
        let cost_delta = summary.cost_delta_micros();
        let cost_direction = if cost_delta < 0 {
            "↓ LisM saves"
        } else {
            "↑ LisM costs more"
        };
        println!(
            "Δ cost             : {:+}μ ({})",
            cost_delta, cost_direction
        );
        println!("Δ gross input      : {:+}", summary.input_token_delta());
        println!(
            "Δ uncached input   : {:+}",
            summary.uncached_input_token_delta()
        );
        println!(
            "Δ sent chars       : {:+}",
            summary.sent_prompt_char_delta()
        );
    } else {
        println!("--- (only one arm has data; cannot compute delta)");
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use std::path::Path;

    use super::{
        BootstrapCli, DEFAULT_BOSS_TASK_TIMEOUT_SECS, preview_chars, resolve_skill_project_root,
        step_terminal_from_tracked_ids, terminal_tail_stalled,
    };
    use anyhow::anyhow;

    #[test]
    fn preview_chars_respects_utf8_boundaries() {
        let value = "abc中文def";
        assert_eq!(preview_chars(value, 0), "");
        assert_eq!(preview_chars(value, 3), "abc");
        assert_eq!(preview_chars(value, 5), "abc中文");
        assert_eq!(preview_chars(value, 50), value);
    }

    #[test]
    fn resolve_skill_project_root_prefers_config_root_parent_for_dot_claude() {
        let cwd = Path::new("/workspace/repo");
        let config_root = Path::new("/tmp/smoke/.claude");
        assert_eq!(
            resolve_skill_project_root(config_root, cwd),
            Path::new("/tmp/smoke")
        );
    }

    #[test]
    fn resolve_skill_project_root_falls_back_to_cwd_for_non_dot_claude_root() {
        let cwd = Path::new("/workspace/repo");
        let config_root = Path::new("/tmp/custom-config");
        assert_eq!(resolve_skill_project_root(config_root, cwd), cwd);
    }

    #[test]
    fn bootstrap_cli_default_boss_task_timeout_is_extended_for_e2e_runs() {
        let cli = BootstrapCli::default();
        assert_eq!(cli.boss_task_timeout_secs, DEFAULT_BOSS_TASK_TIMEOUT_SECS);
    }

    #[test]
    fn bootstrap_cli_default_full_worker_dispatch_fallback_remains_enabled() {
        let cli = BootstrapCli::default();
        assert!(!cli.disable_full_worker_dispatch_fallback);
    }

    #[test]
    fn bootstrap_cli_parses_shared_memory_enabled_flag() {
        let cli = BootstrapCli::parse_from(["rust-agent", "--shared-memory-enabled"]);
        assert!(cli.shared_memory_enabled);
    }

    #[test]
    fn bootstrap_cli_parses_st_flag() {
        let cli = BootstrapCli::parse_from(["rust-agent", "--st"]);
        assert!(cli.st_mode);
    }

    #[test]
    fn terminal_sample_is_emitted_when_completed_child_tail_cannot_advance() {
        let sync_result: anyhow::Result<bool> = Ok(false);
        let terminal_result: anyhow::Result<Option<String>> = Ok(None);
        assert!(terminal_tail_stalled(&sync_result, &terminal_result, false));

        let sync_result: anyhow::Result<bool> = Ok(true);
        let terminal_result: anyhow::Result<Option<String>> = Ok(Some("advance".into()));
        assert!(!terminal_tail_stalled(
            &sync_result,
            &terminal_result,
            false
        ));

        let sync_result: anyhow::Result<bool> = Ok(true);
        let terminal_result: anyhow::Result<Option<String>> = Ok(None);
        assert!(!terminal_tail_stalled(
            &sync_result,
            &terminal_result,
            false
        ));

        let sync_result: anyhow::Result<bool> = Err(anyhow!("sync failed"));
        let terminal_result: anyhow::Result<Option<String>> = Ok(None);
        assert!(terminal_tail_stalled(&sync_result, &terminal_result, false));
    }

    #[test]
    fn terminal_tail_is_not_stalled_when_live_tail_task_remains() {
        let sync_result: anyhow::Result<bool> = Ok(true);
        let terminal_result: anyhow::Result<Option<String>> = Ok(None);
        assert!(!terminal_tail_stalled(&sync_result, &terminal_result, true));
    }

    #[test]
    fn runtime_terminal_poll_does_not_depend_on_single_b_task_id() {
        let tasks = crate::task::manager::TaskManager::new_with_output_root(std::env::temp_dir());
        let stale = tasks.create_with_type(
            "stale",
            crate::task::types::TaskType::LocalAgent,
            "test-session",
            crate::bootstrap::InteractionSurface::Cli,
        );
        let current = tasks.create_with_type(
            "current",
            crate::task::types::TaskType::LocalAgent,
            "test-session",
            crate::bootstrap::InteractionSurface::Cli,
        );
        tasks.start(&stale.id);
        tasks.start(&current.id);
        tasks.complete(
            &current.id,
            &crate::interaction::dispatcher::NotificationDispatcher::new(
                crate::interaction::telegram::gateway::TelegramGateway::default(),
            ),
        );

        assert!(step_terminal_from_tracked_ids(
            &tasks,
            Some(stale.id.as_str()),
            Some(current.id.as_str())
        ));
    }
}

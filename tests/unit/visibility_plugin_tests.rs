use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::doctor::DoctorCommand;
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::builtin::lism::LisMCommand;
use rust_agent::command::builtin::permissions::PermissionsCommand;
use rust_agent::command::builtin::plugins::{PluginSlashCommand, PluginsCommand};
use rust_agent::command::builtin::skills::{SkillSlashCommand, SkillsCommand};
use rust_agent::command::builtin::status::StatusCommand;
use rust_agent::command::builtin::tasks::TasksCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{Command, CommandAvailability, CommandResult};
use rust_agent::history::session::{SessionId, SessionSnapshot};
use rust_agent::interaction::cli::renderer::render_turn_output;
use rust_agent::interaction::cli::repl::CliTurnOutput;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plugins::loader::{load_plugins, validate_runtime_artifact_canonicalized};
use rust_agent::plugins::runtime::{
    augment_hook_registry_with_plugins, augment_tool_registry_with_plugins,
};
use rust_agent::plugins::runtime_state::{
    RuntimePluginState, build_runtime_plugin_snapshot, hydrate_app_state_from_snapshot,
    rebuild_runtime_plugin_state,
};
use rust_agent::plugins::types::{
    PluginActivationSummary, PluginApplyStatus, PluginCapability, PluginCommandDefinition,
    PluginConfigSource, PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity,
    PluginDiagnosticsMetadata, PluginEnvCapability, PluginFilesystemCapability,
    PluginGovernanceSource, PluginGovernanceState, PluginHookDefinition, PluginLifecycleState,
    PluginLoadResult, PluginNetworkCapability, PluginRuntimeApplyOutcome,
    PluginRuntimeCapabilities, PluginRuntimeKind, PluginRuntimeSpec, PluginToolDefinition,
};
use rust_agent::skills::registry::SkillRegistry;
use rust_agent::skills::types::{
    SkillDefinition, SkillExecutionContext, SkillSource, SkillWorkflowExecution,
};
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::definition::{ToolCall, ToolResult};
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn write_wasm_fixture(destination: &Path, fixture_name: &str) {
    let bytes = wat::parse_file(fixture_path(fixture_name)).expect("fixture wat should parse");
    fs::write(destination, bytes).expect("fixture wasm should be written");
}

fn test_app_state(
    command_registry: Option<Arc<CommandRegistry>>,
    task_manager: Option<Arc<TaskManager>>,
    plugin_load_result: Option<Arc<PluginLoadResult>>,
    runtime_tool_registry: Option<Arc<RwLock<ToolRegistry>>>,
) -> AppState {
    let permission_context = match task_manager {
        Some(manager) => {
            ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager)
        }
        None => ToolPermissionContext::new(PermissionMode::Default),
    };
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry,
        runtime_tool_registry,
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "test-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    }
}

#[tokio::test]
async fn cli_status_surfaces_cwd_mode_and_pending_approval_before_debug_detail() {
    let root = unique_temp_path("rust-agent-status-coding-surface");
    let mut app_state = test_app_state(None, Some(Arc::new(TaskManager::default())), None, None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("status-coding-surface".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    app_state.permission_context.set_mode(PermissionMode::Plan);
    app_state
        .permission_context
        .set_pending_approval(Some(PendingApproval {
            tool_name: "Bash".into(),
            tool_input: "pytest".into(),
            message: "approval needed before running pytest".into(),
            code: Some("bash_warning".into()),
            summary: Some("Bash pending approval".into()),
            detail: Some(
                "Reason: command requires explicit approval by ask rule.\nAction: approve or deny"
                    .into(),
            ),
            approval_kind: Some("tool_permission".into()),
            escalation_reasons: vec!["privileged_system".into()],
        }));

    let result = StatusCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/status"),
            &app_state,
        )
        .await
        .expect("status command should render");

    let text = result
        .to_plain_text()
        .expect("status command should produce plain text");

    assert!(
        text.contains("Working status:")
            || text.contains("Current status:")
            || text.contains("Coding status:"),
        "/status should foreground current working state before raw runtime/debug sections; text={text}"
    );
    assert!(
        text.contains("cwd") || text.contains("working_directory"),
        "/status should surface the current working directory near the top; text={text}"
    );
    assert!(
        text.contains("mode") || text.contains("permission_mode"),
        "/status should surface the current mode or permission state near the top; text={text}"
    );
    assert!(
        text.to_ascii_lowercase().contains("pending approval"),
        "/status should surface pending approval in the main working-state block when approval is active; text={text}"
    );

    let working_anchor = text
        .find("Working status:")
        .or_else(|| text.find("Current status:"))
        .or_else(|| text.find("Coding status:"))
        .expect("working-status section should be present");
    let runtime_anchor = text.find("Runtime:").expect("runtime section present");
    let plugins_anchor = text.find("Plugins:").expect("plugins section present");

    assert!(
        working_anchor < runtime_anchor,
        "working status should appear before runtime/debug detail; text={text}"
    );
    assert!(
        working_anchor < plugins_anchor,
        "working status should appear before plugin and legacy detail; text={text}"
    );
}

#[tokio::test]
async fn cli_status_keeps_working_state_compact_and_debug_sections_secondary() {
    let root = unique_temp_path("rust-agent-status-working-vs-debug");
    let mut app_state = test_app_state(None, Some(Arc::new(TaskManager::default())), None, None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("status-working-vs-debug".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    app_state.permission_context.set_mode(PermissionMode::Plan);
    app_state
        .permission_context
        .set_pending_approval(Some(PendingApproval {
            tool_name: "Bash".into(),
            tool_input: "pytest".into(),
            message: "approval needed before running pytest".into(),
            code: Some("bash_warning".into()),
            summary: Some("Bash pending approval".into()),
            detail: Some(
                "Reason: command requires explicit approval by ask rule.\nAction: approve or deny"
                    .into(),
            ),
            approval_kind: Some("tool_permission".into()),
            escalation_reasons: vec!["privileged_system".into()],
        }));

    let result = StatusCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/status"),
            &app_state,
        )
        .await
        .expect("status command should render");

    let text = result
        .to_plain_text()
        .expect("status command should produce plain text");
    let lines = text.lines().collect::<Vec<_>>();

    let working_anchor = lines
        .iter()
        .position(|line| *line == "Working status:")
        .expect("working status section present");
    let runtime_anchor = lines
        .iter()
        .position(|line| *line == "Runtime:")
        .expect("runtime section present");
    let pending_anchor = lines
        .iter()
        .position(|line| line.contains("pending approval"))
        .expect("pending approval line present");

    assert!(
        runtime_anchor - working_anchor <= 4,
        "working status should stay compact and limited to continue-working essentials before diagnostics begin; text={text}"
    );
    assert!(
        pending_anchor < runtime_anchor,
        "pending approval must stay in the working-state block instead of sinking into diagnostics; text={text}"
    );
    assert!(
        text.contains("Debug details:")
            || text.contains("Diagnostics:")
            || text.contains("Runtime diagnostics:"),
        "/status should explicitly mark Runtime/Observability/Commands/Integrations/Plugins as secondary diagnostics instead of presenting them as the same tier as working state; text={text}"
    );
}

#[tokio::test]
async fn cli_doctor_prioritizes_coding_blockers_before_secondary_diagnostics() {
    let root = unique_temp_path("rust-agent-doctor-coding-blockers");
    let mut app_state = test_app_state(None, Some(Arc::new(TaskManager::default())), None, None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("doctor-coding-blockers".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    app_state.permission_context.set_mode(PermissionMode::Plan);
    app_state
        .permission_context
        .set_pending_approval(Some(PendingApproval {
            tool_name: "Bash".into(),
            tool_input: "pytest".into(),
            message: "approval needed before running pytest".into(),
            code: Some("bash_warning".into()),
            summary: Some("Bash pending approval".into()),
            detail: Some(
                "Reason: command requires explicit approval by ask rule.\nAction: approve or deny"
                    .into(),
            ),
            approval_kind: Some("tool_permission".into()),
            escalation_reasons: vec!["privileged_system".into()],
        }));
    app_state.active_model_provider_summary.auth_status = "env:OPENAI_API_KEY(unset)".into();

    let result = DoctorCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/doctor"),
            &app_state,
        )
        .await
        .expect("doctor command should render");

    let text = result
        .to_plain_text()
        .expect("doctor command should produce plain text");

    assert!(
        text.contains("Coding blockers:")
            || text.contains("Primary blockers:")
            || text.contains("Continue coding:"),
        "/doctor should foreground coding-path blockers before generic diagnostics; text={text}"
    );
    assert!(
        text.contains("OPENAI_API_KEY")
            || text.to_ascii_lowercase().contains("api key")
            || text.to_ascii_lowercase().contains("auth_status"),
        "/doctor should surface model/API auth blockers near the top when coding cannot proceed cleanly; text={text}"
    );
    assert!(
        text.contains("cwd") || text.to_ascii_lowercase().contains("working directory"),
        "/doctor should surface cwd/filesystem context near the top so users can tell whether the workspace is usable; text={text}"
    );
    assert!(
        text.to_ascii_lowercase().contains("pending approval")
            || text.to_ascii_lowercase().contains("permission mode")
            || text.to_ascii_lowercase().contains("mode:"),
        "/doctor should surface permission-mode or pending-approval blockers near the top; text={text}"
    );
    assert!(
        text.contains("Plugins:")
            || text.contains("Integrations:")
            || text.contains("Secondary diagnostics:"),
        "/doctor should continue to include secondary diagnostics, but only after primary coding blockers; text={text}"
    );

    let blocker_anchor = text
        .find("Coding blockers:")
        .or_else(|| text.find("Primary blockers:"))
        .or_else(|| text.find("Continue coding:"))
        .expect("coding blocker section should be present");
    let plugin_anchor = text.find("Plugins:");
    let integration_anchor = text.find("Integrations:");
    let secondary_anchor = text.find("Secondary diagnostics:");
    let later_anchor = plugin_anchor
        .or(integration_anchor)
        .or(secondary_anchor)
        .expect("secondary diagnostics anchor should be present");

    assert!(
        blocker_anchor < later_anchor,
        "/doctor should show coding blockers before plugin/secondary diagnostics; text={text}"
    );
}

fn sample_plugin_command(name: &str) -> PluginCommandDefinition {
    PluginCommandDefinition {
        plugin_name: "demo-plugin".into(),
        name: name.into(),
        description: "Plugin command description".into(),
        category: "plugin".into(),
        availability: CommandAvailability::Everywhere,
        disable_model_invocation: false,
        immediate: false,
        is_sensitive: false,
        aliases: vec![format!("{name}-alias")],
        prompt: "Follow the plugin instructions carefully.".into(),
        manifest_path: PathBuf::from("/tmp/demo/plugin.json"),
    }
}

fn sample_skill_definition(name: &str) -> SkillDefinition {
    SkillDefinition {
        name: name.into(),
        description: "Summarize repository state".into(),
        when_to_use: Some("Use when triaging repo state".into()),
        argument_hint: Some("target path".into()),
        workflow_hint: Some("inspect then summarize".into()),
        workflow_summary: Some(
            "inspect then summarize | args: target path | use: Use when triaging repo state".into(),
        ),
        allowed_tools: vec!["Read".into()],
        aliases: vec![],
        workflow_execution: SkillWorkflowExecution::PromptOnly,
        user_invocable: true,
        disable_model_invocation: true,
        hidden: false,
        paths: vec![],
        exclude_paths: vec![],
        requires_files: vec![],
        context: SkillExecutionContext::Inline,
        content: "skill body".into(),
        source: SkillSource::Filesystem,
        file_path: None,
    }
}

fn sample_plugin_tool(name: &str) -> PluginToolDefinition {
    PluginToolDefinition {
        plugin_name: "demo-plugin".into(),
        name: name.into(),
        description: "Plugin tool description".into(),
        aliases: vec![format!("{name}-alias")],
        prompt: "Inspect plugin-owned files".into(),
        search_hint: Some("plugin demo tool".into()),
        read_only: true,
        destructive: false,
        requires_auth: false,
        requires_user_interaction: false,
        manifest_path: PathBuf::from("/tmp/demo/plugin.json"),
    }
}

fn sample_plugin_hook() -> PluginHookDefinition {
    PluginHookDefinition {
        plugin_name: "demo-plugin".into(),
        event: rust_agent::hook::registry::HookEventMatcher::Stop,
        deny_match: None,
        append_message: Some("plugin stop hook fired".into()),
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
        manifest_path: PathBuf::from("/tmp/demo/plugin.json"),
    }
}

fn metadata_rich_plugin_command(name: &str) -> PluginCommandDefinition {
    PluginCommandDefinition {
        plugin_name: "demo-plugin".into(),
        name: name.into(),
        description: "Metadata-rich plugin command".into(),
        category: "plugin".into(),
        availability: CommandAvailability::CliOnly,
        disable_model_invocation: true,
        immediate: true,
        is_sensitive: true,
        aliases: vec![format!("{name}-alias")],
        prompt: "Follow the plugin instructions carefully.".into(),
        manifest_path: PathBuf::from("/tmp/demo/plugin.json"),
    }
}

fn sample_runtime_spec() -> PluginRuntimeSpec {
    sample_runtime_spec_with_limits(30_000, 65_536)
}

fn sample_runtime_spec_with_limits(timeout_ms: u64, output_cap_bytes: u64) -> PluginRuntimeSpec {
    PluginRuntimeSpec {
        kind: PluginRuntimeKind::Wasm,
        artifact: Some("dist/plugin.wasm".into()),
        entry: Some("run_tool".into()),
        timeout_ms: Some(timeout_ms),
        output_cap_bytes: Some(output_cap_bytes),
        capabilities: Some(PluginRuntimeCapabilities {
            filesystem: Some(PluginFilesystemCapability {
                read_roots: vec!["docs".into()],
                write_roots: vec![],
            }),
            network: Some(PluginNetworkCapability {
                allow_hosts: vec!["api.example.com".into()],
            }),
            env: Some(PluginEnvCapability {
                allow_names: vec!["EXAMPLE_TOKEN".into()],
            }),
        }),
    }
}

fn sample_runtime_tool(name: &str, manifest_path: PathBuf) -> PluginToolDefinition {
    PluginToolDefinition {
        plugin_name: "demo-plugin".into(),
        name: name.into(),
        description: "Runtime plugin tool description".into(),
        aliases: vec![format!("{name}-alias")],
        prompt: "Ignored by runtime placeholder".into(),
        search_hint: Some("plugin runtime tool".into()),
        read_only: true,
        destructive: false,
        requires_auth: false,
        requires_user_interaction: false,
        manifest_path,
    }
}

#[tokio::test]
async fn help_command_renders_source_counts_and_execution_kinds() {
    let registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PermissionsCommand))
            .register(Arc::new(SkillSlashCommand::from_skill(
                sample_skill_definition("summarize-skill"),
            )))
            .register(Arc::new(PluginSlashCommand::new(
                metadata_rich_plugin_command("plugin-cmd"),
            ))),
    );
    let app_state = test_app_state(Some(registry), None, None, None);

    let result = HelpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/help"),
            &app_state,
        )
        .await
        .expect("help command should render");

    let CommandResult::Message(text) = result else {
        panic!("expected help message");
    };
    assert!(text.contains("Available commands:"));
    assert!(text.contains("Legend: [type=<prompt|local>]"));
    assert!(text.contains("[sensitive]"));
    assert!(text.contains("[model_invocation=disabled]"));
    assert!(text.contains("[immediate]"));
    assert!(text.contains("Built-in (2):"));
    assert!(text.contains("Skills (1):"));
    assert!(text.contains("Plugins (1):"));
    assert!(text.contains(
        "/help — Show the available commands [type=local] [builtin:core] aliases=h [immediate]"
    ));
    assert!(text.contains("/permissions — Inspect and update permission mode and explicit tool rules [type=local] [builtin:core] aliases=perms [sensitive] [immediate]"));
    assert!(text.contains("/summarize-skill — Summarize repository state — workflow: inspect then summarize | args: target path | use: Use when triaging repo state [type=prompt] [skill:skill] [model_invocation=disabled]"));
    assert!(text.contains("/plugin-cmd — Metadata-rich plugin command [type=prompt] [plugin:plugin] aliases=plugin-cmd-alias [availability=cli-only] [sensitive] [model_invocation=disabled] [immediate]"));

    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: text.clone(),
        events: vec![],
    });
    assert!(rendered.contains("Available commands:"));
    assert!(rendered.contains("Plugins (1):"));
    assert!(!rendered.contains("[panel:"));
}

#[tokio::test]
async fn skills_command_and_slash_command_share_augmented_workflow_metadata() {
    let skill = sample_skill_definition("summarize-skill");
    let app_state = AppState {
        skill_registry: Some(Arc::new(SkillRegistry::new(vec![skill.clone()]))),
        ..test_app_state(None, None, None, None)
    };

    let list_result = SkillsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/skills"),
            &app_state,
        )
        .await
        .expect("skills command should render");
    let CommandResult::Message(list_text) = list_result else {
        panic!("expected skills message");
    };
    assert!(list_text.contains(
        "workflow: inspect then summarize | args: target path | use: Use when triaging repo state"
    ));

    let slash = SkillSlashCommand::from_skill(skill);
    let metadata = slash.metadata();
    assert_eq!(
        metadata.description,
        "Summarize repository state — workflow: inspect then summarize | args: target path | use: Use when triaging repo state"
    );
}

#[tokio::test]
async fn help_command_surfaces_plugin_diagnostics_hint() {
    let registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![],
        diagnostics: vec![PluginDiagnostic {
            plugin_name: Some("broken-plugin".into()),
            manifest_path: Some(PathBuf::from(
                "/tmp/project/.claude/plugins/broken/plugin.json",
            )),
            severity: PluginDiagnosticSeverity::Error,
            code: "plugin-manifest-load-failed".into(),
            message: "bad plugin manifest".into(),
        }],
        orphaned_governance_entries: vec![],
    });
    let app_state = test_app_state(Some(registry), None, Some(plugin_load_result), None);

    let result = HelpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/help"),
            &app_state,
        )
        .await
        .expect("help command should render");

    let CommandResult::Message(text) = result else {
        panic!("expected help message");
    };
    assert!(text.contains("Plugin diagnostics: 1 issue(s) detected (warnings=0, errors=1); run /plugins or /status for details."));
}

#[tokio::test]
async fn status_command_reports_plugin_discovery_summary() {
    let registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PluginSlashCommand::new(
                metadata_rich_plugin_command("plugin-cmd"),
            ))),
    );
    let tool_registry = ToolRegistry::new();
    let (tool_registry, _) = augment_tool_registry_with_plugins(
        tool_registry,
        &PluginLoadResult {
            root: PathBuf::from("/tmp/project/.claude/plugins"),
            source: PluginConfigSource::Directory,
            plugins: vec![PluginDefinition {
                name: "demo-plugin".into(),
                version: Some("0.1.0".into()),
                description: "demo".into(),
                manifest_path: PathBuf::from("/tmp/project/.claude/plugins/demo/plugin.json"),
                capabilities: vec![
                    PluginCapability::Commands,
                    PluginCapability::Hooks,
                    PluginCapability::Tools,
                ],
                runtime: None,
                diagnostics_metadata: Some(PluginDiagnosticsMetadata {
                    homepage: None,
                    docs: Some("https://example.com/docs".into()),
                    issues: None,
                    support_level: Some("community".into()),
                }),
                commands: vec![metadata_rich_plugin_command("plugin-cmd")],
                tools: vec![sample_plugin_tool("demo_tool")],
                hooks: vec![sample_plugin_hook()],
                governance: PluginGovernanceState::default(),
                lifecycle_state: PluginLifecycleState::Enabled,
                apply_status: PluginApplyStatus::Applied,
                activation: PluginActivationSummary {
                    commands: 1,
                    tools: 1,
                    hooks: 1,
                },
            }],
            diagnostics: vec![PluginDiagnostic {
                plugin_name: Some("broken-plugin".into()),
                manifest_path: Some(PathBuf::from(
                    "/tmp/project/.claude/plugins/broken/plugin.json",
                )),
                severity: PluginDiagnosticSeverity::Error,
                code: "plugin-manifest-load-failed".into(),
                message: "bad plugin manifest".into(),
            }],
            orphaned_governance_entries: vec![],
        },
    );
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: PathBuf::from("/tmp/project/.claude/plugins/demo/plugin.json"),
            capabilities: vec![
                PluginCapability::Commands,
                PluginCapability::Hooks,
                PluginCapability::Tools,
            ],
            runtime: None,
            diagnostics_metadata: Some(PluginDiagnosticsMetadata {
                homepage: None,
                docs: Some("https://example.com/docs".into()),
                issues: None,
                support_level: Some("community".into()),
            }),
            commands: vec![metadata_rich_plugin_command("plugin-cmd")],
            tools: vec![sample_plugin_tool("demo_tool")],
            hooks: vec![sample_plugin_hook()],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 1,
                tools: 1,
                hooks: 1,
            },
        }],
        diagnostics: vec![PluginDiagnostic {
            plugin_name: Some("broken-plugin".into()),
            manifest_path: Some(PathBuf::from(
                "/tmp/project/.claude/plugins/broken/plugin.json",
            )),
            severity: PluginDiagnosticSeverity::Error,
            code: "plugin-manifest-load-failed".into(),
            message: "bad plugin manifest".into(),
        }],
        orphaned_governance_entries: vec![],
    });
    let mut app_state = test_app_state(
        Some(registry),
        Some(Arc::new(TaskManager::default())),
        Some(plugin_load_result),
        Some(Arc::new(RwLock::new(tool_registry))),
    );
    let runtime_plugin_state = RuntimePluginState::new(build_runtime_plugin_snapshot(&app_state));
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state);

    let result = StatusCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/status"),
            &app_state,
        )
        .await
        .expect("status command should render");

    let text = result
        .to_plain_text()
        .expect("status command should produce plain text");
    assert!(text.contains("Runtime:"));
    assert!(text.contains("Commands:"));
    assert!(text.contains("Plugins:"));
    assert!(text.contains("- total: 2"));
    assert!(text.contains("- source builtin: 1"));
    assert!(text.contains("- source plugin: 1"));
    assert!(text.contains("- type local: 1"));
    assert!(text.contains("- type prompt: 1"));
    assert!(
        text.contains(
            "- contract: prompt=1, immediate=2, sensitive=1, model_invocation_disabled=1"
        )
    );
    assert!(text.contains("- plugin_discovery: directory (root=/tmp/project/.claude/plugins)"));
    assert!(text.contains("- discovered_plugins: 1"));
    assert!(text.contains("- enabled_plugins: 1"));
    assert!(text.contains("- disabled_plugins: 0"));
    assert!(text.contains("- error_plugins: 0"));
    assert!(text.contains("- discovered_plugin_commands: 1"));
    assert!(text.contains("- discovered_plugin_tools: 1"));
    assert!(text.contains("- discovered_plugin_hooks: 1"));
    assert!(text.contains("- active_plugin_commands: 1"));
    assert!(text.contains("- active_plugin_tools: 1"));
    assert!(text.contains("- active_plugin_hooks: 1"));
    assert!(text.contains("- registered_plugin_commands: 1"));
    assert!(text.contains("- registered_plugin_tools: 1"));
    assert!(text.contains("- diagnostics: total=1, info=0, warnings=0, errors=1"));
    assert!(text.contains("- runtime_apply: outcome=applied, generation=0"));
    assert!(text.contains("- runtime_apply_summary: applied runtime plugin snapshot generation 0"));
    assert!(text.contains("demo-plugin v0.1.0 — state=enabled, applied=applied, enabled=yes, active(commands=1, hooks=1, tools=1), discovered(commands=1, hooks=1, tools=1), capabilities=commands,hooks,tools, governance_source=default, disable_reason=none (manifest=/tmp/project/.claude/plugins/demo/plugin.json)"));
    assert!(text.contains("diagnostic_preview"));
    assert!(text.contains("[error:plugin-manifest-load-failed] plugin=broken-plugin; manifest=/tmp/project/.claude/plugins/broken/plugin.json; bad plugin manifest"));

    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: text.clone(),
        events: vec![],
    });
    assert!(rendered.contains("Status"));
    assert!(rendered.contains("Plugins:"));
    assert!(rendered.contains("registered_plugin_tools: 1"));
    assert!(!rendered.contains("[panel:"));
}

#[tokio::test]
async fn plugins_command_lists_show_details_and_persists_governance_state() {
    let root = unique_temp_path("rust-agent-plugins-command");
    let plugin_manifest_path = root
        .join(".claude")
        .join("plugins")
        .join("demo")
        .join("plugin.json");
    fs::create_dir_all(
        plugin_manifest_path
            .parent()
            .expect("plugin parent should exist"),
    )
    .expect("plugin dir should exist");
    fs::write(&plugin_manifest_path, "{}").expect("plugin manifest placeholder should be written");

    let plugin = PluginDefinition {
        name: "demo-plugin".into(),
        version: Some("0.1.0".into()),
        description: "demo plugin".into(),
        manifest_path: plugin_manifest_path.clone(),
        capabilities: vec![
            PluginCapability::Commands,
            PluginCapability::Tools,
            PluginCapability::Hooks,
        ],
        runtime: None,
        diagnostics_metadata: Some(PluginDiagnosticsMetadata {
            homepage: Some("https://example.com/home".into()),
            docs: Some("https://example.com/docs".into()),
            issues: Some("https://example.com/issues".into()),
            support_level: Some("community".into()),
        }),
        commands: vec![sample_plugin_command("plugin-cmd")],
        tools: vec![sample_plugin_tool("demo_tool")],
        hooks: vec![sample_plugin_hook()],
        governance: PluginGovernanceState::default(),
        lifecycle_state: PluginLifecycleState::Enabled,
        apply_status: PluginApplyStatus::Applied,
        activation: PluginActivationSummary {
            commands: 1,
            tools: 1,
            hooks: 1,
        },
    };
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: root.join(".claude").join("plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![plugin],
        diagnostics: vec![PluginDiagnostic {
            plugin_name: Some("demo-plugin".into()),
            manifest_path: Some(plugin_manifest_path.clone()),
            severity: PluginDiagnosticSeverity::Warning,
            code: "plugin-capability-tools-empty".into(),
            message: "plugin declares tools capability but no valid tools were loaded".into(),
        }],
        orphaned_governance_entries: vec![],
    });
    let registry = Arc::new(CommandRegistry::new().register(Arc::new(PluginsCommand)));
    let mut app_state = test_app_state(Some(registry), None, Some(plugin_load_result), None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("plugin-test-session".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let runtime_plugin_state = RuntimePluginState::new(build_runtime_plugin_snapshot(&app_state));
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state);

    let list_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins"),
            &app_state,
        )
        .await
        .expect("plugins list should render");
    let CommandResult::Message(list_text) = list_result else {
        panic!("expected plugins list message");
    };
    assert!(list_text.contains("Plugins:"));
    assert!(list_text.contains("- inventory: discovered=1, enabled=1, disabled=0, error=0"));
    assert!(list_text.contains("demo-plugin v0.1.0 — state=enabled, applied=applied, enabled=yes"));
    assert!(list_text.contains("runtime=prompt"));

    let show_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins show demo-plugin"),
            &app_state,
        )
        .await
        .expect("plugins show should render");
    let CommandResult::Message(show_text) = show_result else {
        panic!("expected plugins show message");
    };
    assert!(show_text.contains("Plugin: demo-plugin"));
    assert!(show_text.contains("- manifest:"));
    assert!(show_text.contains("- runtime_kind: prompt"));
    assert!(show_text.contains("- runtime_artifact: none"));
    assert!(show_text.contains("- runtime_capabilities: none"));
    assert!(show_text.contains("- diagnostics_metadata:"));
    assert!(show_text.contains("https://example.com/docs"));
    assert!(show_text.contains("- runtime_apply:"));
    assert!(show_text.contains("  - outcome: applied"));
    assert!(show_text.contains("  - generation: 0"));

    let diagnostics_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins diagnostics demo-plugin"),
            &app_state,
        )
        .await
        .expect("plugins diagnostics should render");
    let CommandResult::Message(diagnostics_text) = diagnostics_result else {
        panic!("expected plugins diagnostics message");
    };
    assert!(diagnostics_text.contains("Plugin diagnostics for demo-plugin:"));
    assert!(diagnostics_text.contains("plugin-capability-tools-empty"));

    let disable_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(
                InteractionSurface::Cli,
                "/plugins disable demo-plugin maintenance-window",
            ),
            &app_state,
        )
        .await
        .expect("plugins disable should persist state");
    let CommandResult::Message(disable_text) = disable_result else {
        panic!("expected plugins disable message");
    };
    assert!(disable_text.contains("Disabled plugin demo-plugin"));
    let persisted = fs::read_to_string(root.join(".claude").join("plugin-state.json"))
        .expect("plugin state file should exist");
    assert!(persisted.contains("\"name\": \"demo-plugin\""));
    assert!(persisted.contains("\"enabled\": false"));
    assert!(persisted.contains("\"reason\": \"maintenance-window\""));

    let enable_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins enable demo-plugin"),
            &app_state,
        )
        .await
        .expect("plugins enable should persist state");
    let CommandResult::Message(enable_text) = enable_result else {
        panic!("expected plugins enable message");
    };
    assert!(enable_text.contains("Enabled plugin demo-plugin"));
    let persisted = fs::read_to_string(root.join(".claude").join("plugin-state.json"))
        .expect("plugin state file should exist after enable");
    assert!(persisted.contains("\"enabled\": true"));

    fs::remove_dir_all(root).expect("plugins command temp dir should be cleaned up");
}

#[tokio::test]
async fn runtime_plugin_rebuild_retains_previous_snapshot_on_apply_failure() {
    let root = unique_temp_path("rust-agent-plugin-retained-snapshot");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        &manifest_path,
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands", "tools"],
  "commands": [
    {
      "name": "plugin-cmd",
      "description": "Plugin command",
      "prompt": "Do plugin command work"
    }
  ],
  "tools": [
    {
      "name": "demo_tool",
      "description": "Demo tool",
      "prompt": "Inspect plugin-owned files",
      "read_only": true
    }
  ]
}"#,
    )
    .expect("good plugin manifest should be written");

    let mut app_state = test_app_state(None, Some(Arc::new(TaskManager::default())), None, None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("plugin-test-session".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let runtime_plugin_state = RuntimePluginState::new(build_runtime_plugin_snapshot(&app_state));
    let initial_snapshot = runtime_plugin_state.snapshot().await;
    hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state.clone());

    fs::write(
        &manifest_path,
        r#"{
  "name": "demo-plugin",
  "version": "0.2.0",
  "description": "Broken demo plugin",
  "capabilities": ["commands", "tools"],
  "commands": [
    {
      "name": "broken-plugin-cmd",
      "description": "Broken plugin command"
    }
  ]
}"#,
    )
    .expect("broken plugin manifest should be written");

    let report = rebuild_runtime_plugin_state(&app_state)
        .await
        .expect("plugin rebuild should produce retained report");
    assert_eq!(report.outcome.as_str(), "retained_previous_snapshot");
    assert_eq!(report.generation, 0);
    assert!(
        report
            .message
            .contains("retained runtime plugin snapshot generation 0")
    );
    assert!(report.message.contains("demo-plugin"));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "plugin-command-prompt-invalid")
    );

    let retained_snapshot = runtime_plugin_state.snapshot().await;
    assert_eq!(
        retained_snapshot.plugin_load_result.plugins[0]
            .version
            .as_deref(),
        Some("0.1.0")
    );
    assert_eq!(
        retained_snapshot.plugin_load_result.active_command_count(),
        1
    );
    assert_eq!(retained_snapshot.plugin_load_result.active_tool_count(), 1);
    assert_eq!(runtime_plugin_state.generation().await, 0);
    assert_eq!(
        runtime_plugin_state
            .last_apply_report()
            .await
            .expect("apply report should be retained")
            .outcome
            .as_str(),
        "retained_previous_snapshot"
    );

    let status_result = StatusCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/status"),
            &app_state,
        )
        .await
        .expect("status should render retained apply state");
    let status_text = status_result
        .to_plain_text()
        .expect("status command should produce plain text");
    assert!(
        status_text.contains("- runtime_apply: outcome=retained_previous_snapshot, generation=0")
    );
    assert!(
        status_text
            .contains("- runtime_apply_summary: retained runtime plugin snapshot generation 0")
    );
    assert!(status_text.contains("demo-plugin v0.1.0 — state=enabled, applied=applied"));
    assert!(!status_text.contains("broken-plugin-cmd"));

    let show_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins show demo-plugin"),
            &app_state,
        )
        .await
        .expect("plugins show should render retained apply state");
    let CommandResult::Message(show_text) = show_result else {
        panic!("expected plugins show message");
    };
    assert!(show_text.contains("- runtime_apply:"));
    assert!(show_text.contains("  - outcome: retained_previous_snapshot"));
    assert!(show_text.contains("  - generation: 0"));
    assert!(show_text.contains("  - summary: retained runtime plugin snapshot generation 0"));

    let mut next_turn_app_state =
        test_app_state(None, Some(Arc::new(TaskManager::default())), None, None);
    hydrate_app_state_from_snapshot(&mut next_turn_app_state, &retained_snapshot);
    assert_eq!(
        next_turn_app_state
            .plugin_load_result
            .as_ref()
            .expect("next turn should have plugin result")
            .plugins[0]
            .version
            .as_deref(),
        Some("0.1.0")
    );
    assert_eq!(
        next_turn_app_state
            .plugin_load_result
            .as_ref()
            .expect("next turn should have plugin result")
            .active_command_count(),
        1
    );

    fs::remove_dir_all(root).expect("retained snapshot temp dir should be cleaned up");
}

#[test]
fn plugin_loader_accepts_legacy_prompt_plugin_without_runtime() {
    let root = unique_temp_path("rust-agent-plugin-runtime-legacy");
    let plugin_dir = root.join(".claude").join("plugins").join("legacy");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "legacy-plugin",
  "version": "0.1.0",
  "description": "Legacy prompt plugin",
  "capabilities": ["commands"],
  "commands": [
    {
      "name": "legacy-cmd",
      "description": "Legacy command",
      "prompt": "Run the legacy prompt"
    }
  ]
}"#,
    )
    .expect("legacy plugin manifest should be written");

    let result = load_plugins(&root);
    assert_eq!(result.plugins.len(), 1);
    assert_eq!(result.plugins[0].name, "legacy-plugin");
    assert_eq!(result.plugins[0].runtime, None);
    assert_eq!(
        result.plugins[0].lifecycle_state,
        PluginLifecycleState::Enabled
    );
    assert!(result.plugins[0].activation.commands == 1);
    assert!(
        !result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code.starts_with("plugin-runtime-"))
    );

    fs::remove_dir_all(root).expect("legacy runtime temp dir should be cleaned up");
}

#[tokio::test]
async fn plugins_command_renders_wasm_runtime_metadata() {
    let root = unique_temp_path("rust-agent-plugin-runtime-show");
    let plugin_manifest_path = root
        .join(".claude")
        .join("plugins")
        .join("demo")
        .join("plugin.json");
    fs::create_dir_all(
        plugin_manifest_path
            .parent()
            .expect("plugin parent should exist"),
    )
    .expect("plugin dir should exist");
    fs::write(&plugin_manifest_path, "{}").expect("plugin manifest placeholder should be written");

    let plugin = PluginDefinition {
        name: "demo-plugin".into(),
        version: Some("0.1.0".into()),
        description: "demo plugin".into(),
        manifest_path: plugin_manifest_path,
        capabilities: vec![PluginCapability::Commands, PluginCapability::Tools],
        runtime: Some(sample_runtime_spec()),
        diagnostics_metadata: None,
        commands: vec![sample_plugin_command("plugin-cmd")],
        tools: vec![sample_plugin_tool("demo_tool")],
        hooks: vec![],
        governance: PluginGovernanceState::default(),
        lifecycle_state: PluginLifecycleState::Enabled,
        apply_status: PluginApplyStatus::Applied,
        activation: PluginActivationSummary {
            commands: 1,
            tools: 1,
            hooks: 0,
        },
    };
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: root.join(".claude").join("plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![plugin],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    });
    let registry = Arc::new(CommandRegistry::new().register(Arc::new(PluginsCommand)));
    let mut app_state = test_app_state(Some(registry), None, Some(plugin_load_result), None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("plugin-runtime-session".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let runtime_plugin_state = RuntimePluginState::new(build_runtime_plugin_snapshot(&app_state));
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state);

    let list_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins"),
            &app_state,
        )
        .await
        .expect("plugins list should render");
    let CommandResult::Message(list_text) = list_result else {
        panic!("expected plugins list message");
    };
    assert!(list_text.contains("runtime=wasm"));

    let show_result = PluginsCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins show demo-plugin"),
            &app_state,
        )
        .await
        .expect("plugins show should render");
    let CommandResult::Message(show_text) = show_result else {
        panic!("expected plugins show message");
    };
    assert!(show_text.contains("- runtime_kind: wasm"));
    assert!(show_text.contains("- runtime_artifact: dist/plugin.wasm"));
    assert!(show_text.contains("- runtime_entry: run"));
    assert!(show_text.contains("- runtime_timeout_ms: 30000"));
    assert!(show_text.contains("- runtime_output_cap_bytes: 65536"));
    assert!(show_text.contains("filesystem(read_roots=[docs], write_roots=[]); network(allow_hosts=[api.example.com]); env(allow_names=[EXAMPLE_TOKEN])"));

    fs::remove_dir_all(root).expect("runtime metadata temp dir should be cleaned up");
}

#[test]
fn plugin_loader_parses_deno_runtime_without_executing_it() {
    let root = unique_temp_path("rust-agent-plugin-runtime-deno");
    let plugin_dir = root.join(".claude").join("plugins").join("deno-demo");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("dist").join("plugin.js"),
        "export default {};",
    )
    .expect("artifact should be written");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "deno-plugin",
  "version": "0.1.0",
  "description": "Deno plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "deno",
    "artifact": "dist/plugin.js",
    "entry": "main",
    "timeout_ms": 1000,
    "output_cap_bytes": 4096,
    "capabilities": {
      "network": { "allow_hosts": ["example.com"] }
    }
  },
  "commands": [
    {
      "name": "deno-cmd",
      "description": "Deno command",
      "prompt": "Still prompt-backed in T18.2.B"
    }
  ]
}"#,
    )
    .expect("deno plugin manifest should be written");

    let result = load_plugins(&root);
    assert_eq!(result.plugins.len(), 1);
    let plugin = &result.plugins[0];
    assert_eq!(
        plugin.runtime.as_ref().map(|runtime| runtime.kind),
        Some(PluginRuntimeKind::Deno)
    );
    assert_eq!(plugin.lifecycle_state, PluginLifecycleState::Enabled);
    assert_eq!(plugin.apply_status, PluginApplyStatus::Applied);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.code != "plugin-runtime-artifact-missing")
    );

    fs::remove_dir_all(root).expect("deno runtime temp dir should be cleaned up");
}

#[test]
fn plugin_loader_rejects_runtime_artifact_traversal() {
    let root = unique_temp_path("rust-agent-plugin-runtime-traversal");
    let plugin_dir = root.join(".claude").join("plugins").join("bad");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "bad-plugin",
  "version": "0.1.0",
  "description": "Bad plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "wasm",
    "artifact": "../escape.wasm"
  },
  "commands": [
    {
      "name": "bad-cmd",
      "description": "Bad command",
      "prompt": "prompt"
    }
  ]
}"#,
    )
    .expect("bad plugin manifest should be written");

    let result = load_plugins(&root);
    assert_eq!(result.plugins.len(), 1);
    assert_eq!(
        result.plugins[0].lifecycle_state,
        PluginLifecycleState::Error
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "plugin-runtime-artifact-path-traversal")
    );

    fs::remove_dir_all(root).expect("runtime traversal temp dir should be cleaned up");
}

#[test]
fn plugin_loader_marks_missing_runtime_artifact_as_error() {
    let root = unique_temp_path("rust-agent-plugin-runtime-missing-artifact");
    let plugin_dir = root.join(".claude").join("plugins").join("bad");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "bad-plugin",
  "version": "0.1.0",
  "description": "Bad plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "wasm",
    "artifact": "dist/missing.wasm"
  },
  "commands": [
    {
      "name": "bad-cmd",
      "description": "Bad command",
      "prompt": "prompt"
    }
  ]
}"#,
    )
    .expect("bad plugin manifest should be written");

    let result = load_plugins(&root);
    assert_eq!(result.plugins.len(), 1);
    assert_eq!(
        result.plugins[0].lifecycle_state,
        PluginLifecycleState::Error
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "plugin-runtime-artifact-missing")
    );

    fs::remove_dir_all(root).expect("runtime missing artifact temp dir should be cleaned up");
}

#[test]
fn plugin_loader_rejects_unknown_runtime_kind() {
    let root = unique_temp_path("rust-agent-plugin-runtime-kind");
    let plugin_dir = root.join(".claude").join("plugins").join("bad");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "bad-plugin",
  "version": "0.1.0",
  "description": "Bad plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "node"
  },
  "commands": [
    {
      "name": "bad-cmd",
      "description": "Bad command",
      "prompt": "prompt"
    }
  ]
}"#,
    )
    .expect("bad plugin manifest should be written");

    let result = load_plugins(&root);
    assert!(result.plugins.is_empty());
    assert!(result.diagnostics.iter().any(|diagnostic| diagnostic.code
        == "plugin-manifest-load-failed"
        && diagnostic.message.contains("unknown variant `node`")));

    fs::remove_dir_all(root).expect("runtime kind temp dir should be cleaned up");
}

#[test]
fn plugin_loader_rejects_unknown_runtime_capability() {
    let root = unique_temp_path("rust-agent-plugin-runtime-capability");
    let plugin_dir = root.join(".claude").join("plugins").join("bad");
    fs::create_dir_all(&plugin_dir).expect("plugin dir should exist");
    fs::write(
        plugin_dir.join("plugin.json"),
        r#"{
  "name": "bad-plugin",
  "version": "0.1.0",
  "description": "Bad plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "prompt",
    "capabilities": {
      "process": { "allow": ["sh"] }
    }
  },
  "commands": [
    {
      "name": "bad-cmd",
      "description": "Bad command",
      "prompt": "prompt"
    }
  ]
}"#,
    )
    .expect("bad plugin manifest should be written");

    let result = load_plugins(&root);
    assert!(result.plugins.is_empty());
    assert!(result.diagnostics.iter().any(|diagnostic| diagnostic.code
        == "plugin-manifest-load-failed"
        && diagnostic.message.contains("unknown field `process`")));

    fs::remove_dir_all(root).expect("runtime capability temp dir should be cleaned up");
}

#[tokio::test]
async fn runtime_plugin_rebuild_retains_previous_snapshot_on_bad_runtime_manifest_reload() {
    let root = unique_temp_path("rust-agent-plugin-runtime-retained-snapshot");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    let artifact_path = plugin_dir.join("dist").join("plugin.wasm");
    fs::create_dir_all(
        artifact_path
            .parent()
            .expect("artifact parent should exist"),
    )
    .expect("plugin dir should exist");
    fs::write(&artifact_path, "wasm-binary-placeholder").expect("artifact should be written");
    fs::write(
        &manifest_path,
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "wasm",
    "artifact": "dist/plugin.wasm"
  },
  "commands": [
    {
      "name": "plugin-cmd",
      "description": "Plugin command",
      "prompt": "Do plugin command work"
    }
  ]
}"#,
    )
    .expect("good plugin manifest should be written");

    let mut app_state = test_app_state(None, Some(Arc::new(TaskManager::default())), None, None);
    app_state.session = Some(SessionSnapshot {
        session_id: SessionId("plugin-runtime-retained-session".into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: root.display().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let runtime_plugin_state = RuntimePluginState::new(build_runtime_plugin_snapshot(&app_state));
    let initial_snapshot = runtime_plugin_state.snapshot().await;
    hydrate_app_state_from_snapshot(&mut app_state, &initial_snapshot);
    app_state.permission_context = app_state
        .permission_context
        .clone()
        .with_runtime_plugin_state(runtime_plugin_state.clone());

    fs::write(
        &manifest_path,
        r#"{
  "name": "demo-plugin",
  "version": "0.2.0",
  "description": "Broken demo plugin",
  "capabilities": ["commands"],
  "runtime": {
    "kind": "wasm",
    "artifact": "../escape.wasm"
  },
  "commands": [
    {
      "name": "plugin-cmd",
      "description": "Plugin command",
      "prompt": "Do plugin command work"
    }
  ]
}"#,
    )
    .expect("broken runtime plugin manifest should be written");

    let report = rebuild_runtime_plugin_state(&app_state)
        .await
        .expect("plugin rebuild should produce retained report");
    assert_eq!(
        report.outcome,
        PluginRuntimeApplyOutcome::RetainedPreviousSnapshot
    );
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "plugin-runtime-artifact-path-traversal")
    );

    let retained_snapshot = runtime_plugin_state.snapshot().await;
    assert_eq!(
        retained_snapshot.plugin_load_result.plugins[0]
            .version
            .as_deref(),
        Some("0.1.0")
    );
    assert_eq!(
        retained_snapshot.plugin_load_result.plugins[0]
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.artifact.as_deref()),
        Some("dist/plugin.wasm")
    );
    assert_eq!(runtime_plugin_state.generation().await, 0);

    fs::remove_dir_all(root).expect("runtime retained snapshot temp dir should be cleaned up");
}

#[tokio::test]
async fn tasks_command_groups_orchestration_tasks_and_hints() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let parent = manager.create("implement feature", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&parent.id, WorkerRole::Implement);
    manager.set_orchestration_group_id(&parent.id, Some("group-1".into()));
    manager.set_validation_state(
        &parent.id,
        Some(rust_agent::task::types::ValidationState::PendingVerification),
    );
    manager.complete(&parent.id, &dispatcher);

    let child = manager.create("verify feature", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&child.id, WorkerRole::Verify);
    manager.set_parent_task_id(&child.id, Some(parent.id.clone()));
    manager.set_orchestration_group_id(&child.id, Some("group-1".into()));
    manager.start(&child.id);

    let standalone = manager.create(
        "standalone research",
        "test-session",
        InteractionSurface::Cli,
    );
    manager.set_worker_role(&standalone.id, WorkerRole::Research);

    let app_state = test_app_state(None, Some(manager), None, None);
    let result = TasksCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/tasks"),
            &app_state,
        )
        .await
        .expect("tasks command should render");

    let CommandResult::Message(text) = result else {
        panic!("expected tasks message");
    };
    assert!(text.contains("Agent Tasks:"));
    assert!(text.contains("Summary:"));
    assert!(text.contains("- total: 3"));
    assert!(text.contains("- orchestration_groups: 1"));
    assert!(text.contains("- by_phase: implement=1, research=1, verify=1"));
    assert!(text.contains("- orchestration_contract: groups_in_progress=1, waiting_for_verification=0, ready_for_synthesis=0"));
    assert!(text.contains("Orchestration groups:"));
    assert!(text.contains("- group-1 — group group-1 still in progress"));
    assert!(text.contains("  - [task-0] implement feature (Status: Completed)"));
    assert!(text.contains("    hint: inspect task output for task-0"));
    assert!(text.contains("  - [task-1] verify feature (Status: Running)"));
    assert!(text.contains("    parent_task_id: task-0"));
    assert!(text.contains("Standalone tasks:"));
    assert!(text.contains("- [task-2] standalone research (Status: Pending)"));

    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: text.clone(),
        events: vec![],
    });
    assert!(rendered.contains("Agent Tasks:"));
    assert!(rendered.contains("Summary:"));
    assert!(rendered.contains("Standalone tasks:"));
    assert!(!rendered.contains("[panel:"));
}

#[test]
fn plugin_loader_loads_inline_and_file_prompts_and_collects_diagnostics() {
    let root = unique_temp_path("rust-agent-plugin-loader");
    let plugins_root = root.join(".claude").join("plugins");
    let good_dir = plugins_root.join("demo");
    let bad_dir = plugins_root.join("broken");
    fs::create_dir_all(&good_dir).expect("good plugin dir should exist");
    fs::create_dir_all(&bad_dir).expect("bad plugin dir should exist");
    fs::write(good_dir.join("prompt.txt"), "Prompt loaded from file")
        .expect("prompt file should be written");
    fs::write(
        good_dir.join("plugin.json"),
        r#"{
  "name": "demo-plugin",
  "version": "0.1.0",
  "description": "Demo plugin",
  "capabilities": ["commands", "hooks", "tools"],
  "diagnostics": {
    "docs": "https://example.com/docs",
    "support_level": "community"
  },
  "commands": [
    {
      "name": "inline-plugin",
      "description": "Uses inline prompt",
      "prompt": "Inline prompt body",
      "disable_model_invocation": true,
      "immediate": true,
      "is_sensitive": true
    },
    {
      "name": "file-plugin",
      "description": "Uses file prompt",
      "prompt_file": "prompt.txt",
      "availability": "cli-only"
    }
  ],
  "tools": [
    {
      "name": "demo_tool",
      "description": "Plugin tool",
      "prompt": "Inspect plugin-owned files",
      "read_only": true
    }
  ],
  "hooks": [
    {
      "event": "stop",
      "append_message": "plugin stop hook fired"
    }
  ]
}"#,
    )
    .expect("good plugin manifest should be written");
    fs::write(bad_dir.join("plugin.json"), "{ not-json }")
        .expect("bad plugin manifest should be written");

    let result = load_plugins(&root);

    assert_eq!(result.source, PluginConfigSource::Directory);
    assert_eq!(result.plugins.len(), 1);
    assert_eq!(result.plugins[0].name, "demo-plugin");
    assert_eq!(result.plugins[0].version.as_deref(), Some("0.1.0"));
    assert_eq!(
        result.plugins[0].capabilities,
        vec![
            PluginCapability::Commands,
            PluginCapability::Hooks,
            PluginCapability::Tools
        ]
    );
    assert_eq!(
        result.plugins[0].lifecycle_state,
        PluginLifecycleState::Enabled
    );
    assert_eq!(
        result.plugins[0].governance.source,
        PluginGovernanceSource::Default
    );
    assert!(result.plugins[0].governance.enabled);
    assert_eq!(result.plugins[0].activation.commands, 2);
    assert_eq!(result.plugins[0].activation.tools, 1);
    assert_eq!(result.plugins[0].activation.hooks, 1);
    assert_eq!(result.plugins[0].commands.len(), 2);
    assert_eq!(result.plugins[0].tools.len(), 1);
    assert_eq!(result.plugins[0].hooks.len(), 1);
    assert_eq!(
        result.plugins[0]
            .diagnostics_metadata
            .as_ref()
            .and_then(|meta| meta.docs.as_deref()),
        Some("https://example.com/docs")
    );
    assert_eq!(result.plugins[0].commands[0].prompt, "Inline prompt body");
    assert!(result.plugins[0].commands[0].disable_model_invocation);
    assert!(result.plugins[0].commands[0].immediate);
    assert!(result.plugins[0].commands[0].is_sensitive);
    assert_eq!(
        result.plugins[0].commands[1].prompt,
        "Prompt loaded from file"
    );
    assert_eq!(
        result.plugins[0].commands[1].availability,
        CommandAvailability::CliOnly
    );
    assert_eq!(result.diagnostics.len(), 2);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "plugin-manifest-load-failed")
    );
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "plugin-state-defaults")
    );

    fs::remove_dir_all(root).expect("plugin loader temp dir should be cleaned up");
}

#[test]
fn plugin_runtime_augments_hook_and_tool_registries() {
    let load_result = PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: PathBuf::from("/tmp/project/.claude/plugins/demo/plugin.json"),
            capabilities: vec![
                PluginCapability::Commands,
                PluginCapability::Hooks,
                PluginCapability::Tools,
            ],
            runtime: None,
            diagnostics_metadata: None,
            commands: vec![sample_plugin_command("plugin-cmd")],
            tools: vec![sample_plugin_tool("demo_tool")],
            hooks: vec![sample_plugin_hook()],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 1,
                tools: 1,
                hooks: 1,
            },
        }],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    };

    let hook_registry = augment_hook_registry_with_plugins(
        rust_agent::hook::registry::HookRegistry::default(),
        &load_result,
    );
    let (tool_registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);

    assert_eq!(hook_registry.rules().len(), 1);
    assert_eq!(tool_registry.all_metadata().len(), 1);
    assert!(tool_registry.all_metadata()[0].name.starts_with("plugin."));
    assert!(diagnostics.is_empty());
}

#[tokio::test]
async fn wasm_runtime_tool_executes_happy_path() {
    let root = unique_temp_path("rust-agent-runtime-executor");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    write_wasm_fixture(
        &plugin_dir.join("dist").join("plugin.wasm"),
        "plugin_runtime_echo.wat",
    );

    let load_result = PluginLoadResult {
        root: root.join(".claude").join("plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: manifest_path.clone(),
            capabilities: vec![PluginCapability::Tools],
            runtime: Some(sample_runtime_spec()),
            diagnostics_metadata: None,
            commands: vec![],
            tools: vec![sample_runtime_tool("runtime-tool", manifest_path.clone())],
            hooks: vec![],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 0,
                tools: 1,
                hooks: 0,
            },
        }],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    };

    let (registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);
    assert!(diagnostics.is_empty());

    let result = registry
        .invoke(
            &ToolCall::new("plugin.demo-plugin.runtime-tool", "{\"input\":\"x\"}"),
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("runtime invoke should succeed");
    let ToolResult::Text(message) = result else {
        panic!("expected text result");
    };
    assert_eq!(message, "echo:{\"input\":\"x\"}");

    fs::remove_dir_all(root).expect("runtime executor temp dir should be cleaned up");
}

#[tokio::test]
async fn wasm_runtime_tool_timeout_maps_to_interrupted() {
    timeout(Duration::from_secs(60), async {
        let root = unique_temp_path("rust-agent-runtime-timeout");
        let plugin_dir = root.join(".claude").join("plugins").join("demo");
        let manifest_path = plugin_dir.join("plugin.json");
        fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
        write_wasm_fixture(
            &plugin_dir.join("dist").join("plugin.wasm"),
            "plugin_runtime_loop.wat",
        );

        let load_result = PluginLoadResult {
            root: root.join(".claude").join("plugins"),
            source: PluginConfigSource::Directory,
            plugins: vec![PluginDefinition {
                name: "demo-plugin".into(),
                version: Some("0.1.0".into()),
                description: "demo".into(),
                manifest_path: manifest_path.clone(),
                capabilities: vec![PluginCapability::Tools],
                runtime: Some(sample_runtime_spec_with_limits(100, 65_536)),
                diagnostics_metadata: None,
                commands: vec![],
                tools: vec![sample_runtime_tool("runtime-tool", manifest_path.clone())],
                hooks: vec![],
                governance: PluginGovernanceState::default(),
                lifecycle_state: PluginLifecycleState::Enabled,
                apply_status: PluginApplyStatus::Applied,
                activation: PluginActivationSummary {
                    commands: 0,
                    tools: 1,
                    hooks: 0,
                },
            }],
            diagnostics: vec![],
            orphaned_governance_entries: vec![],
        };

        let (registry, diagnostics) =
            augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);
        assert!(diagnostics.is_empty());

        let result = registry
            .invoke(
                &ToolCall::new("plugin.demo-plugin.runtime-tool", "{\"input\":\"x\"}"),
                &ToolPermissionContext::new(PermissionMode::Default),
            )
            .await
            .expect("runtime invoke should map timeout");
        let ToolResult::Interrupted(message) = result else {
            panic!("expected interrupted result");
        };
        assert!(message.contains("plugin runtime execution interrupted"));
        assert!(message.contains("Runtime: wasm"));
        assert!(message.contains("Entry: run_tool"));
        assert!(message.contains("Timeout hit: yes"));
        assert!(message.contains("Result: interrupted"));

        fs::remove_dir_all(root).expect("runtime timeout temp dir should be cleaned up");
    })
    .await
    .expect("wasm runtime timeout test should complete within 60 seconds");
}

#[tokio::test]
async fn wasm_runtime_tool_output_cap_maps_to_result_too_large() {
    let root = unique_temp_path("rust-agent-runtime-output-cap");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    write_wasm_fixture(
        &plugin_dir.join("dist").join("plugin.wasm"),
        "plugin_runtime_large_output.wat",
    );

    let load_result = PluginLoadResult {
        root: root.join(".claude").join("plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: manifest_path.clone(),
            capabilities: vec![PluginCapability::Tools],
            runtime: Some(sample_runtime_spec_with_limits(30_000, 64)),
            diagnostics_metadata: None,
            commands: vec![],
            tools: vec![sample_runtime_tool("runtime-tool", manifest_path.clone())],
            hooks: vec![],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 0,
                tools: 1,
                hooks: 0,
            },
        }],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    };

    let (registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);
    assert!(diagnostics.is_empty());

    let result = registry
        .invoke(
            &ToolCall::new("plugin.demo-plugin.runtime-tool", "{\"input\":\"x\"}"),
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("runtime invoke should map output cap");
    let ToolResult::ResultTooLarge(message) = result else {
        panic!("expected result too large");
    };
    assert!(message.contains("plugin runtime result exceeded output cap"));
    assert!(message.contains("Output cap hit: yes"));
    assert!(message.contains("Result: result_too_large"));
    assert!(message.contains("Entry: run_tool"));

    fs::remove_dir_all(root).expect("runtime output cap temp dir should be cleaned up");
}

#[tokio::test]
async fn wasm_runtime_tool_with_static_data_segment_still_executes() {
    let root = unique_temp_path("rust-agent-runtime-static-data");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    write_wasm_fixture(
        &plugin_dir.join("dist").join("plugin.wasm"),
        "plugin_runtime_static_data.wat",
    );

    let load_result = PluginLoadResult {
        root: root.join(".claude").join("plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: manifest_path.clone(),
            capabilities: vec![PluginCapability::Tools],
            runtime: Some(sample_runtime_spec()),
            diagnostics_metadata: None,
            commands: vec![],
            tools: vec![sample_runtime_tool("runtime-tool", manifest_path.clone())],
            hooks: vec![],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 0,
                tools: 1,
                hooks: 0,
            },
        }],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    };

    let (registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);
    assert!(diagnostics.is_empty());

    let result = registry
        .invoke(
            &ToolCall::new("plugin.demo-plugin.runtime-tool", "{\"input\":\"ok\"}"),
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("runtime invoke with static data should succeed");
    let ToolResult::Text(message) = result else {
        panic!("expected text result");
    };
    assert_eq!(message, "static:{\"input\":\"ok\"}");

    fs::remove_dir_all(root).expect("runtime static data temp dir should be cleaned up");
}

#[tokio::test]
async fn wasm_runtime_tool_missing_alloc_input_export_returns_clear_error() {
    let root = unique_temp_path("rust-agent-runtime-missing-alloc");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    fs::create_dir_all(plugin_dir.join("dist")).expect("plugin dir should exist");
    write_wasm_fixture(
        &plugin_dir.join("dist").join("plugin.wasm"),
        "plugin_runtime_missing_alloc.wat",
    );

    let load_result = PluginLoadResult {
        root: root.join(".claude").join("plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: manifest_path.clone(),
            capabilities: vec![PluginCapability::Tools],
            runtime: Some(sample_runtime_spec()),
            diagnostics_metadata: None,
            commands: vec![],
            tools: vec![sample_runtime_tool("runtime-tool", manifest_path.clone())],
            hooks: vec![],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 0,
                tools: 1,
                hooks: 0,
            },
        }],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    };

    let (registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);
    assert!(diagnostics.is_empty());

    let result = registry
        .invoke(
            &ToolCall::new("plugin.demo-plugin.runtime-tool", "{\"input\":\"x\"}"),
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("runtime invoke should map missing alloc_input");
    let ToolResult::Denied(message) = result else {
        panic!("expected denied result");
    };
    assert!(message.contains("plugin runtime alloc_input export is required"));
    assert!(message.contains("failed to resolve wasm export alloc_input"));
    assert!(message.contains("Entry: run_tool"));

    fs::remove_dir_all(root).expect("runtime missing alloc temp dir should be cleaned up");
}

#[test]
fn canonical_runtime_artifact_inside_root_is_accepted() {
    let root = unique_temp_path("rust-agent-canonical-runtime-artifact");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    let artifact_path = plugin_dir.join("dist").join("plugin.wasm");
    fs::create_dir_all(
        artifact_path
            .parent()
            .expect("artifact parent should exist"),
    )
    .expect("plugin dir should exist");
    fs::write(&manifest_path, "{}").expect("manifest should be written");
    fs::write(&artifact_path, "wasm").expect("artifact should be written");

    let canonical = validate_runtime_artifact_canonicalized(
        "demo-plugin",
        &manifest_path,
        &plugin_dir,
        "dist/plugin.wasm",
    )
    .expect("artifact inside root should be accepted");
    assert!(canonical.ends_with("plugin.wasm"));

    fs::remove_dir_all(root).expect("canonical artifact temp dir should be cleaned up");
}

#[cfg(unix)]
#[test]
fn symlink_escape_runtime_artifact_is_rejected() {
    use std::os::unix::fs::symlink;

    let root = unique_temp_path("rust-agent-runtime-symlink-escape");
    let plugin_dir = root.join(".claude").join("plugins").join("demo");
    let manifest_path = plugin_dir.join("plugin.json");
    let outside_dir = root.join("outside");
    let outside_artifact = outside_dir.join("escape.wasm");
    let symlink_path = plugin_dir.join("dist").join("plugin.wasm");
    fs::create_dir_all(symlink_path.parent().expect("artifact parent should exist"))
        .expect("plugin dir should exist");
    fs::create_dir_all(&outside_dir).expect("outside dir should exist");
    fs::write(&manifest_path, "{}").expect("manifest should be written");
    fs::write(&outside_artifact, "wasm").expect("outside artifact should be written");
    symlink(&outside_artifact, &symlink_path).expect("symlink should be created");

    let error = validate_runtime_artifact_canonicalized(
        "demo-plugin",
        &manifest_path,
        &plugin_dir,
        "dist/plugin.wasm",
    )
    .expect_err("symlink escape should be rejected");
    assert_eq!(error.code, "plugin-runtime-artifact-symlink-escape");

    fs::remove_dir_all(root).expect("symlink escape temp dir should be cleaned up");
}

#[tokio::test]
async fn plugin_slash_command_returns_prompt_result() {
    let command = PluginSlashCommand::new(sample_plugin_command("plugin-cmd"));
    let app_state = test_app_state(None, None, None, None);

    let result = command
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugin-cmd --scope full"),
            &app_state,
        )
        .await
        .expect("plugin command should execute");

    let CommandResult::Prompt(prompt) = result else {
        panic!("expected prompt result");
    };
    assert!(prompt.contains("Loaded plugin command: plugin-cmd"));
    assert!(prompt.contains("Plugin: demo-plugin"));
    assert!(prompt.contains("Arguments: --scope full"));
    assert!(prompt.contains("Plugin instructions:"));
}

#[test]
fn plugin_prompt_tool_maps_read_only_into_search_or_read_metadata() {
    let load_result = PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            version: Some("0.1.0".into()),
            description: "demo".into(),
            manifest_path: PathBuf::from("/tmp/project/.claude/plugins/demo/plugin.json"),
            capabilities: vec![PluginCapability::Tools],
            runtime: None,
            diagnostics_metadata: Some(PluginDiagnosticsMetadata {
                homepage: None,
                docs: Some("https://example.com/docs".into()),
                issues: None,
                support_level: Some("community".into()),
            }),
            commands: vec![],
            tools: vec![sample_plugin_tool("plugin-tool")],
            hooks: vec![],
            governance: PluginGovernanceState::default(),
            lifecycle_state: PluginLifecycleState::Enabled,
            apply_status: PluginApplyStatus::Applied,
            activation: PluginActivationSummary {
                commands: 0,
                tools: 1,
                hooks: 0,
            },
        }],
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    };

    let (registry, diagnostics) =
        augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);
    assert!(diagnostics.is_empty());

    let metadata = registry
        .all_metadata()
        .into_iter()
        .find(|metadata| metadata.name == "plugin.demo-plugin.plugin-tool")
        .expect("plugin tool metadata should exist");

    assert_eq!(metadata.name, "plugin.demo-plugin.plugin-tool");
    assert!(metadata.read_only);
    assert!(metadata.is_search_or_read_command);
    assert_eq!(metadata.aliases, vec!["plugin-tool-alias"]);
    assert_eq!(metadata.search_hint, Some("plugin demo tool"));
}

#[test]
fn plugin_slash_command_metadata_preserves_contract_flags() {
    let command = PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd"));
    let metadata = command.metadata();

    assert_eq!(metadata.name, "plugin-cmd");
    assert_eq!(
        metadata.source,
        rust_agent::command::types::CommandSource::Plugin
    );
    assert_eq!(
        metadata.command_type,
        rust_agent::command::types::CommandType::Prompt
    );
    assert_eq!(metadata.availability, CommandAvailability::CliOnly);
    assert!(metadata.disable_model_invocation);
    assert!(metadata.immediate);
    assert!(metadata.is_sensitive);
    assert_eq!(metadata.aliases, vec!["plugin-cmd-alias".to_string()]);
}

fn _assert_path_exists(path: &Path) {
    assert!(path.exists() || !path.as_os_str().is_empty());
}

#[tokio::test]
async fn lism_command_on_sets_flag_true() {
    let app_state = test_app_state(None, None, None, None);

    let result = LisMCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/LisM on"),
            &app_state,
        )
        .await
        .expect("LisM on should succeed");

    let CommandResult::Message(text) = result else {
        panic!("expected LisM message");
    };
    assert!(app_state.permission_context.lism_enabled());
    assert!(text.contains("LisM enabled"));
    assert!(text.contains("/boss production path now switches to the StateFrame execution seam"));
}

#[tokio::test]
async fn lism_command_off_sets_flag_false() {
    let app_state = test_app_state(None, None, None, None);
    app_state.permission_context.set_lism_enabled(true);

    let result = LisMCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/LisM off"),
            &app_state,
        )
        .await
        .expect("LisM off should succeed");

    let CommandResult::Message(text) = result else {
        panic!("expected LisM message");
    };
    assert!(!app_state.permission_context.lism_enabled());
    assert!(text.contains("LisM disabled"));
}

#[tokio::test]
async fn lism_command_status_reports_current_mode() {
    let app_state = test_app_state(None, None, None, None);

    let disabled = LisMCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/LisM status"),
            &app_state,
        )
        .await
        .expect("LisM status should succeed");
    let CommandResult::Message(disabled_text) = disabled else {
        panic!("expected LisM message");
    };
    assert!(disabled_text.contains("disabled"));

    app_state.permission_context.set_lism_enabled(true);
    let enabled = LisMCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/LisM status"),
            &app_state,
        )
        .await
        .expect("LisM status should succeed");
    let CommandResult::Message(enabled_text) = enabled else {
        panic!("expected LisM message");
    };
    assert!(enabled_text.contains("enabled"));
}

#[tokio::test]
async fn lism_command_explain_lists_available_building_blocks_and_deferred_items() {
    let app_state = test_app_state(None, None, None, None);

    let result = LisMCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/LisM explain"),
            &app_state,
        )
        .await
        .expect("LisM explain should succeed");

    let CommandResult::Message(text) = result else {
        panic!("expected LisM message");
    };
    assert!(text.contains("Available building blocks"));
    assert!(text.contains("StateFrame schema and StateDecision validation"));
    assert!(text.contains("BossPlan -> StateFrame projection"));
    assert!(text.contains("Stateless JSON decision loop"));
    assert!(text.contains(
        "Toolset / skillset router is attached to the live LisM -> /boss production path"
    ));
    assert!(text.contains(
        "Model-tier router and provider_profile_id routing are connected to the production path"
    ));
    assert!(text.to_ascii_lowercase().contains("routed metadata"));
}

#[tokio::test]
async fn lism_command_unknown_subcommand_returns_usage() {
    let app_state = test_app_state(None, None, None, None);

    let result = LisMCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/LisM nope"),
            &app_state,
        )
        .await
        .expect("LisM usage should succeed");

    let CommandResult::Message(text) = result else {
        panic!("expected LisM message");
    };
    assert!(text.contains("usage: /LisM <subcommand>"));
    assert!(text.contains("status"));
    assert!(text.contains("explain"));
}

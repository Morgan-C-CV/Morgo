use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::help::HelpCommand;
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
use rust_agent::plugins::loader::load_plugins;
use rust_agent::plugins::runtime::{augment_hook_registry_with_plugins, augment_tool_registry_with_plugins};
use rust_agent::plugins::runtime_state::{RuntimePluginState, build_runtime_plugin_snapshot};
use rust_agent::plugins::types::{
    PluginActivationSummary, PluginCapability, PluginCommandDefinition, PluginConfigSource,
    PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity, PluginDiagnosticsMetadata,
    PluginGovernanceSource, PluginGovernanceState, PluginHookDefinition, PluginLifecycleState,
    PluginLoadResult, PluginToolDefinition,
};
use rust_agent::skills::registry::SkillRegistry;
use rust_agent::skills::types::{SkillDefinition, SkillExecutionContext, SkillSource};
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

fn test_app_state(
    command_registry: Option<Arc<CommandRegistry>>,
    task_manager: Option<Arc<TaskManager>>,
    plugin_load_result: Option<Arc<PluginLoadResult>>,
    runtime_tool_registry: Option<Arc<RwLock<ToolRegistry>>>,
) -> AppState {
    let permission_context = match task_manager {
        Some(manager) => ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager),
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
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: Vec::new(),
        active_session_id: "test-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    }
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
        workflow_summary: Some("inspect then summarize | args: target path | use: Use when triaging repo state".into()),
        allowed_tools: vec!["Read".into()],
        aliases: vec![],
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

#[tokio::test]
async fn help_command_renders_source_counts_and_execution_kinds() {
    let registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PermissionsCommand))
            .register(Arc::new(SkillSlashCommand::from_skill(sample_skill_definition("summarize-skill"))))
            .register(Arc::new(PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd")))),
    );
    let app_state = test_app_state(Some(registry), None, None, None);

    let result = HelpCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/help"), &app_state)
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
    assert!(text.contains("/help — Show the available commands [type=local] [builtin:core] aliases=h [immediate]"));
    assert!(text.contains("/permissions — Inspect and update permission mode and explicit tool rules [type=local] [builtin:core] aliases=perms [sensitive] [immediate]"));
    assert!(text.contains("/summarize-skill — Summarize repository state — workflow: inspect then summarize | args: target path | use: Use when triaging repo state [type=prompt] [skill:skill] [model_invocation=disabled]"));
    assert!(text.contains("/plugin-cmd — Metadata-rich plugin command [type=prompt] [plugin:plugin] aliases=plugin-cmd-alias [availability=cli-only] [sensitive] [model_invocation=disabled] [immediate]"));

    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: text.clone(),
        events: vec![],
    });
    assert!(rendered.contains("Available commands:"));
    assert!(rendered.contains("Plugins (1):"));
}

#[tokio::test]
async fn skills_command_and_slash_command_share_augmented_workflow_metadata() {
    let skill = sample_skill_definition("summarize-skill");
    let app_state = AppState {
        skill_registry: Some(Arc::new(SkillRegistry::new(vec![skill.clone()]))),
        ..test_app_state(None, None, None, None)
    };

    let list_result = SkillsCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/skills"), &app_state)
        .await
        .expect("skills command should render");
    let CommandResult::Message(list_text) = list_result else {
        panic!("expected skills message");
    };
    assert!(list_text.contains("workflow: inspect then summarize | args: target path | use: Use when triaging repo state"));

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
            manifest_path: Some(PathBuf::from("/tmp/project/.claude/plugins/broken/plugin.json")),
            severity: PluginDiagnosticSeverity::Error,
            code: "plugin-manifest-load-failed".into(),
            message: "bad plugin manifest".into(),
        }],
    });
    let app_state = test_app_state(Some(registry), None, Some(plugin_load_result), None);

    let result = HelpCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/help"), &app_state)
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
            .register(Arc::new(PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd")))),
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
                capabilities: vec![PluginCapability::Commands, PluginCapability::Hooks, PluginCapability::Tools],
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
                activation: PluginActivationSummary {
                    commands: 1,
                    tools: 1,
                    hooks: 1,
                },
            }],
            diagnostics: vec![PluginDiagnostic {
                plugin_name: Some("broken-plugin".into()),
                manifest_path: Some(PathBuf::from("/tmp/project/.claude/plugins/broken/plugin.json")),
                severity: PluginDiagnosticSeverity::Error,
                code: "plugin-manifest-load-failed".into(),
                message: "bad plugin manifest".into(),
            }],
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
            capabilities: vec![PluginCapability::Commands, PluginCapability::Hooks, PluginCapability::Tools],
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
                activation: PluginActivationSummary {
                    commands: 1,
                    tools: 1,
                    hooks: 1,
                },
        }],
        diagnostics: vec![PluginDiagnostic {
            plugin_name: Some("broken-plugin".into()),
            manifest_path: Some(PathBuf::from("/tmp/project/.claude/plugins/broken/plugin.json")),
            severity: PluginDiagnosticSeverity::Error,
            code: "plugin-manifest-load-failed".into(),
            message: "bad plugin manifest".into(),
        }],
    });
    let app_state = test_app_state(
        Some(registry),
        Some(Arc::new(TaskManager::default())),
        Some(plugin_load_result),
        Some(Arc::new(RwLock::new(tool_registry))),
    );

    let result = StatusCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/status"), &app_state)
        .await
        .expect("status command should render");

    let CommandResult::Message(text) = result else {
        panic!("expected status message");
    };
    assert!(text.contains("Runtime:"));
    assert!(text.contains("Commands:"));
    assert!(text.contains("Plugins:"));
    assert!(text.contains("- total: 2"));
    assert!(text.contains("- source builtin: 1"));
    assert!(text.contains("- source plugin: 1"));
    assert!(text.contains("- type local: 1"));
    assert!(text.contains("- type prompt: 1"));
    assert!(text.contains("- contract: prompt=1, immediate=2, sensitive=1, model_invocation_disabled=1"));
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
    assert!(text.contains("- plugin_inventory:"));
    assert!(text.contains("  - demo-plugin v0.1.0 — state=enabled, enabled=yes, active(commands=1, hooks=1, tools=1), discovered(commands=1, hooks=1, tools=1), capabilities=commands,hooks,tools, governance_source=default, disable_reason=none (manifest=/tmp/project/.claude/plugins/demo/plugin.json)"));
    assert!(text.contains("- diagnostic_preview:"));
    assert!(text.contains("[error:plugin-manifest-load-failed] plugin=broken-plugin; manifest=/tmp/project/.claude/plugins/broken/plugin.json; bad plugin manifest"));

    let rendered = render_turn_output(&CliTurnOutput {
        primary_text: text.clone(),
        events: vec![],
    });
    assert!(rendered.contains("Status"));
    assert!(rendered.contains("Plugins:"));
    assert!(rendered.contains("registered_plugin_tools: 1"));
}

#[tokio::test]
async fn plugins_command_lists_show_details_and_persists_governance_state() {
    let root = unique_temp_path("rust-agent-plugins-command");
    let plugin_manifest_path = root.join(".claude").join("plugins").join("demo").join("plugin.json");
    fs::create_dir_all(plugin_manifest_path.parent().expect("plugin parent should exist"))
        .expect("plugin dir should exist");
    fs::write(&plugin_manifest_path, "{}") .expect("plugin manifest placeholder should be written");

    let plugin = PluginDefinition {
        name: "demo-plugin".into(),
        version: Some("0.1.0".into()),
        description: "demo plugin".into(),
        manifest_path: plugin_manifest_path.clone(),
        capabilities: vec![PluginCapability::Commands, PluginCapability::Tools, PluginCapability::Hooks],
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
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/plugins"), &app_state)
        .await
        .expect("plugins list should render");
    let CommandResult::Message(list_text) = list_result else {
        panic!("expected plugins list message");
    };
    assert!(list_text.contains("Plugins:"));
    assert!(list_text.contains("- inventory: discovered=1, enabled=1, disabled=0, error=0"));
    assert!(list_text.contains("demo-plugin v0.1.0 — state=enabled, enabled=yes"));

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
    assert!(show_text.contains("- diagnostics_metadata:"));
    assert!(show_text.contains("https://example.com/docs"));

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
async fn tasks_command_groups_orchestration_tasks_and_hints() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let parent = manager.create("implement feature", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&parent.id, WorkerRole::Implement);
    manager.set_orchestration_group_id(&parent.id, Some("group-1".into()));
    manager.set_validation_state(&parent.id, Some(rust_agent::task::types::ValidationState::PendingVerification));
    manager.complete(&parent.id, &dispatcher);

    let child = manager.create("verify feature", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&child.id, WorkerRole::Verify);
    manager.set_parent_task_id(&child.id, Some(parent.id.clone()));
    manager.set_orchestration_group_id(&child.id, Some("group-1".into()));
    manager.start(&child.id);

    let standalone = manager.create("standalone research", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&standalone.id, WorkerRole::Research);

    let app_state = test_app_state(None, Some(manager), None, None);
    let result = TasksCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/tasks"), &app_state)
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
    assert!(text.contains("    hint: verification next for task-0"));
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
}

#[test]
fn plugin_loader_loads_inline_and_file_prompts_and_collects_diagnostics() {
    let root = unique_temp_path("rust-agent-plugin-loader");
    let plugins_root = root.join(".claude").join("plugins");
    let good_dir = plugins_root.join("demo");
    let bad_dir = plugins_root.join("broken");
    fs::create_dir_all(&good_dir).expect("good plugin dir should exist");
    fs::create_dir_all(&bad_dir).expect("bad plugin dir should exist");
    fs::write(good_dir.join("prompt.txt"), "Prompt loaded from file").expect("prompt file should be written");
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
    fs::write(bad_dir.join("plugin.json"), "{ not-json }").expect("bad plugin manifest should be written");

    let result = load_plugins(&root);

    assert_eq!(result.source, PluginConfigSource::Directory);
    assert_eq!(result.plugins.len(), 1);
    assert_eq!(result.plugins[0].name, "demo-plugin");
    assert_eq!(result.plugins[0].version.as_deref(), Some("0.1.0"));
    assert_eq!(
        result.plugins[0].capabilities,
        vec![PluginCapability::Commands, PluginCapability::Hooks, PluginCapability::Tools]
    );
    assert_eq!(result.plugins[0].lifecycle_state, PluginLifecycleState::Enabled);
    assert_eq!(result.plugins[0].governance.source, PluginGovernanceSource::Default);
    assert!(result.plugins[0].governance.enabled);
    assert_eq!(result.plugins[0].activation.commands, 2);
    assert_eq!(result.plugins[0].activation.tools, 1);
    assert_eq!(result.plugins[0].activation.hooks, 1);
    assert_eq!(result.plugins[0].commands.len(), 2);
    assert_eq!(result.plugins[0].tools.len(), 1);
    assert_eq!(result.plugins[0].hooks.len(), 1);
    assert_eq!(result.plugins[0].diagnostics_metadata.as_ref().and_then(|meta| meta.docs.as_deref()), Some("https://example.com/docs"));
    assert_eq!(result.plugins[0].commands[0].prompt, "Inline prompt body");
    assert!(result.plugins[0].commands[0].disable_model_invocation);
    assert!(result.plugins[0].commands[0].immediate);
    assert!(result.plugins[0].commands[0].is_sensitive);
    assert_eq!(result.plugins[0].commands[1].prompt, "Prompt loaded from file");
    assert_eq!(result.plugins[0].commands[1].availability, CommandAvailability::CliOnly);
    assert_eq!(result.diagnostics.len(), 2);
    assert!(result
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "plugin-manifest-load-failed"));
    assert!(result
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "plugin-state-defaults"));

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
            capabilities: vec![PluginCapability::Commands, PluginCapability::Hooks, PluginCapability::Tools],
            diagnostics_metadata: None,
            commands: vec![sample_plugin_command("plugin-cmd")],
            tools: vec![sample_plugin_tool("demo_tool")],
            hooks: vec![sample_plugin_hook()],
                governance: PluginGovernanceState::default(),
                lifecycle_state: PluginLifecycleState::Enabled,
                activation: PluginActivationSummary {
                    commands: 1,
                    tools: 1,
                    hooks: 1,
                },
        }],
        diagnostics: vec![],
    };

    let hook_registry = augment_hook_registry_with_plugins(rust_agent::hook::registry::HookRegistry::default(), &load_result);
    let (tool_registry, diagnostics) = augment_tool_registry_with_plugins(ToolRegistry::new(), &load_result);

    assert_eq!(hook_registry.rules().len(), 1);
    assert_eq!(tool_registry.all_metadata().len(), 1);
    assert!(tool_registry.all_metadata()[0].name.starts_with("plugin."));
    assert!(diagnostics.is_empty());
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
fn plugin_slash_command_metadata_preserves_contract_flags() {
    let command = PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd"));
    let metadata = command.metadata();

    assert_eq!(metadata.name, "plugin-cmd");
    assert_eq!(metadata.source, rust_agent::command::types::CommandSource::Plugin);
    assert_eq!(metadata.command_type, rust_agent::command::types::CommandType::Prompt);
    assert_eq!(metadata.availability, CommandAvailability::CliOnly);
    assert!(metadata.disable_model_invocation);
    assert!(metadata.immediate);
    assert!(metadata.is_sensitive);
    assert_eq!(metadata.aliases, vec!["plugin-cmd-alias".to_string()]);
}

fn _assert_path_exists(path: &Path) {
    assert!(path.exists() || !path.as_os_str().is_empty());
}

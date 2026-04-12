use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::help::HelpCommand;
use rust_agent::command::builtin::permissions::PermissionsCommand;
use rust_agent::command::builtin::plugins::PluginSlashCommand;
use rust_agent::command::builtin::skills::SkillSlashCommand;
use rust_agent::command::builtin::status::StatusCommand;
use rust_agent::command::builtin::tasks::TasksCommand;
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{Command, CommandAvailability, CommandResult};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plugins::loader::load_plugins;
use rust_agent::plugins::types::{
    PluginCommandDefinition, PluginConfigSource, PluginDefinition, PluginLoadResult,
};
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;

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
        runtime_tool_registry: None,
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
            .register(Arc::new(SkillSlashCommand::from_skill(
                "summarize-skill".into(),
                "Summarize repository state".into(),
                true,
            )))
            .register(Arc::new(PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd")))),
    );
    let app_state = test_app_state(Some(registry), None, None);

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
    assert!(text.contains("/summarize-skill — Summarize repository state [type=prompt] [skill:skill] [model_invocation=disabled]"));
    assert!(text.contains("/plugin-cmd — Metadata-rich plugin command [type=prompt] [plugin:plugin] aliases=plugin-cmd-alias [availability=cli-only] [sensitive] [model_invocation=disabled] [immediate]"));
}

#[tokio::test]
async fn help_command_surfaces_plugin_diagnostics_hint() {
    let registry = Arc::new(CommandRegistry::new().register(Arc::new(HelpCommand)));
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![],
        diagnostics: vec!["bad plugin manifest in broken/plugin.json".into()],
    });
    let app_state = test_app_state(Some(registry), None, Some(plugin_load_result));

    let result = HelpCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/help"), &app_state)
        .await
        .expect("help command should render");

    let CommandResult::Message(text) = result else {
        panic!("expected help message");
    };
    assert!(text.contains("Plugin diagnostics: 1 issue(s) detected; run /status for details."));
}

#[tokio::test]
async fn status_command_reports_plugin_discovery_summary() {
    let registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd")))),
    );
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            description: "demo".into(),
            manifest_path: PathBuf::from("/tmp/project/.claude/plugins/demo/plugin.json"),
            commands: vec![metadata_rich_plugin_command("plugin-cmd")],
        }],
        diagnostics: vec!["bad plugin manifest in broken/plugin.json".into()],
    });
    let app_state = test_app_state(Some(registry), Some(Arc::new(TaskManager::default())), Some(plugin_load_result));

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
    assert!(text.contains("- discovered_plugin_commands: 1"));
    assert!(text.contains("- registered_plugin_commands: 1"));
    assert!(text.contains("- diagnostics: 1"));
    assert!(text.contains("- plugin_inventory:"));
    assert!(text.contains("  - demo-plugin — commands=1 (manifest=/tmp/project/.claude/plugins/demo/plugin.json)"));
    assert!(text.contains("- diagnostic_preview:"));
    assert!(text.contains("  - bad plugin manifest in broken/plugin.json"));
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

    let app_state = test_app_state(None, Some(manager), None);
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
  "description": "Demo plugin",
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
  ]
}"#,
    )
    .expect("good plugin manifest should be written");
    fs::write(bad_dir.join("plugin.json"), "{ not-json }").expect("bad plugin manifest should be written");

    let result = load_plugins(&root);

    assert_eq!(result.source, PluginConfigSource::Directory);
    assert_eq!(result.plugins.len(), 1);
    assert_eq!(result.plugins[0].name, "demo-plugin");
    assert_eq!(result.plugins[0].commands.len(), 2);
    assert_eq!(result.plugins[0].commands[0].prompt, "Inline prompt body");
    assert!(result.plugins[0].commands[0].disable_model_invocation);
    assert!(result.plugins[0].commands[0].immediate);
    assert!(result.plugins[0].commands[0].is_sensitive);
    assert_eq!(result.plugins[0].commands[1].prompt, "Prompt loaded from file");
    assert_eq!(result.plugins[0].commands[1].availability, CommandAvailability::CliOnly);
    assert_eq!(result.diagnostics.len(), 1);
    assert!(result.diagnostics[0].contains("Failed to load plugin manifest"));

    fs::remove_dir_all(root).expect("plugin loader temp dir should be cleaned up");
}

#[tokio::test]
async fn plugin_slash_command_returns_prompt_result() {
    let command = PluginSlashCommand::new(sample_plugin_command("plugin-cmd"));
    let app_state = test_app_state(None, None, None);

    let result = command
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/plugin-cmd --target demo"),
            &app_state,
        )
        .await
        .expect("plugin slash command should execute");

    let CommandResult::Prompt(text) = result else {
        panic!("expected prompt result");
    };
    assert!(text.contains("Loaded plugin command: plugin-cmd"));
    assert!(text.contains("Plugin: demo-plugin"));
    assert!(text.contains("Arguments: --target demo"));
    assert!(text.contains("Plugin instructions:\nFollow the plugin instructions carefully."));
}

#[tokio::test]
async fn help_and_status_report_consistent_command_contract_counts() {
    let registry = Arc::new(
        CommandRegistry::new()
            .register(Arc::new(HelpCommand))
            .register(Arc::new(PermissionsCommand))
            .register(Arc::new(SkillSlashCommand::from_skill(
                "summarize-skill".into(),
                "Summarize repository state".into(),
                true,
            )))
            .register(Arc::new(PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd")))),
    );
    let plugin_load_result = Arc::new(PluginLoadResult {
        root: PathBuf::from("/tmp/project/.claude/plugins"),
        source: PluginConfigSource::Directory,
        plugins: vec![PluginDefinition {
            name: "demo-plugin".into(),
            description: "demo".into(),
            manifest_path: PathBuf::from("/tmp/project/.claude/plugins/demo/plugin.json"),
            commands: vec![metadata_rich_plugin_command("plugin-cmd")],
        }],
        diagnostics: vec![],
    });
    let app_state = test_app_state(Some(registry), Some(Arc::new(TaskManager::default())), Some(plugin_load_result));

    let help = HelpCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/help"), &app_state)
        .await
        .expect("help should render");
    let status = StatusCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/status"), &app_state)
        .await
        .expect("status should render");

    let CommandResult::Message(help_text) = help else {
        panic!("expected help message");
    };
    let CommandResult::Message(status_text) = status else {
        panic!("expected status message");
    };

    assert!(help_text.contains("Built-in (2):"));
    assert!(help_text.contains("Skills (1):"));
    assert!(help_text.contains("Plugins (1):"));
    assert!(status_text.contains("- total: 4"));
    assert!(status_text.contains("- source builtin: 2"));
    assert!(status_text.contains("- source skill: 1"));
    assert!(status_text.contains("- source plugin: 1"));
    assert!(status_text.contains("- contract: prompt=2, immediate=3, sensitive=2, model_invocation_disabled=2"));
    assert!(status_text.contains("- discovered_plugin_commands: 1"));
    assert!(status_text.contains("- plugin_inventory:"));
}

#[tokio::test]
async fn status_and_tasks_report_consistent_orchestration_summaries() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let implement = manager.create("implement feature", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&implement.id, WorkerRole::Implement);
    manager.set_orchestration_group_id(&implement.id, Some("group-1".into()));
    manager.set_validation_state(
        &implement.id,
        Some(rust_agent::task::types::ValidationState::PendingVerification),
    );
    manager.complete(&implement.id, &dispatcher);

    let verify = manager.create("verify feature", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&verify.id, WorkerRole::Verify);
    manager.set_parent_task_id(&verify.id, Some(implement.id.clone()));
    manager.set_orchestration_group_id(&verify.id, Some("group-1".into()));
    manager.start(&verify.id);

    let standalone = manager.create("standalone research", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&standalone.id, WorkerRole::Research);

    let app_state = test_app_state(None, Some(manager), None);

    let status = StatusCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/status"), &app_state)
        .await
        .expect("status should render");
    let tasks = TasksCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/tasks"), &app_state)
        .await
        .expect("tasks should render");

    let CommandResult::Message(status_text) = status else {
        panic!("expected status message");
    };
    let CommandResult::Message(tasks_text) = tasks else {
        panic!("expected tasks message");
    };

    assert!(status_text.contains("- pending_orchestration: yes"));
    assert!(status_text.contains("- tasks: total=3, running=1, completed=1, failed=0, killed=0"));
    assert!(status_text.contains("- pending_verification: 1"));
    assert!(status_text.contains("- orchestration_groups: 1"));

    assert!(tasks_text.contains("- total: 3"));
    assert!(tasks_text.contains("- orchestration_groups: 1"));
    assert!(tasks_text.contains("- by_validation_state: none=2, pending_verification=1"));
    assert!(tasks_text.contains("- orchestration_contract: groups_in_progress=1, waiting_for_verification=0, ready_for_synthesis=0"));
    assert!(tasks_text.contains("- group-1 — group group-1 still in progress"));
}

#[test]
fn plugin_slash_command_metadata_preserves_contract_flags() {
    let metadata = PluginSlashCommand::new(metadata_rich_plugin_command("plugin-cmd")).metadata();

    assert_eq!(metadata.availability, CommandAvailability::CliOnly);
    assert!(metadata.disable_model_invocation);
    assert!(metadata.immediate);
    assert!(metadata.is_sensitive);
}

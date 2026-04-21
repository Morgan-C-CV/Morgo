use std::sync::Arc;

use tokio::sync::RwLock;

use crate::command::registry::CommandRegistry;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::hook::registry::{HookRegistry, load_hook_registry};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::router::CommandRouter;
use crate::plugins::loader::load_plugins;
use crate::plugins::runtime::{
    augment_hook_registry_with_plugins, augment_tool_registry_with_plugins,
};
use crate::plugins::types::{
    PluginApplyStatus, PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity,
    PluginLifecycleState, PluginLoadResult, PluginRuntimeApplyOutcome, PluginRuntimeApplyReport,
};
use crate::security::authorizer::DefaultSurfaceAuthorizer;
use crate::state::app_state::AppState;
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
use crate::tool::registry::ToolRegistry;

#[derive(Clone)]
pub struct RuntimePluginState {
    inner: Arc<RwLock<RuntimePluginSnapshot>>,
    generation: Arc<RwLock<u64>>,
    last_apply_report: Arc<RwLock<Option<PluginRuntimeApplyReport>>>,
}

#[derive(Clone)]
pub struct RuntimePluginSnapshot {
    pub command_registry: Arc<CommandRegistry>,
    pub tool_registry: ToolRegistry,
    pub runtime_tool_registry: Arc<RwLock<ToolRegistry>>,
    pub hook_registry: HookRegistry,
    pub plugin_load_result: Arc<PluginLoadResult>,
    pub notification_dispatcher: NotificationDispatcher,
}

impl std::fmt::Debug for RuntimePluginState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimePluginState").finish()
    }
}

impl RuntimePluginState {
    pub fn new(snapshot: RuntimePluginSnapshot) -> Self {
        let initial_report = PluginRuntimeApplyReport {
            outcome: PluginRuntimeApplyOutcome::Applied,
            generation: 0,
            message: format!(
                "applied runtime plugin snapshot generation 0 (plugins={}, active_commands={}, active_tools={}, active_hooks={})",
                snapshot.plugin_load_result.plugins.len(),
                snapshot.plugin_load_result.active_command_count(),
                snapshot.plugin_load_result.active_tool_count(),
                snapshot.plugin_load_result.active_hook_count(),
            ),
            diagnostics: snapshot.plugin_load_result.diagnostics.clone(),
            orphaned_governance_entries: snapshot
                .plugin_load_result
                .orphaned_governance_entries
                .clone(),
        };
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
            generation: Arc::new(RwLock::new(0)),
            last_apply_report: Arc::new(RwLock::new(Some(initial_report))),
        }
    }

    pub async fn snapshot(&self) -> RuntimePluginSnapshot {
        self.inner.read().await.clone()
    }

    pub async fn replace(&self, snapshot: RuntimePluginSnapshot) -> u64 {
        *self.inner.write().await = snapshot;
        let mut generation = self.generation.write().await;
        *generation += 1;
        *generation
    }

    pub async fn generation(&self) -> u64 {
        *self.generation.read().await
    }

    pub async fn last_apply_report(&self) -> Option<PluginRuntimeApplyReport> {
        self.last_apply_report.read().await.clone()
    }

    pub async fn set_last_apply_report(&self, report: PluginRuntimeApplyReport) {
        *self.last_apply_report.write().await = Some(report);
    }
}

pub fn build_runtime_plugin_snapshot(app_state: &AppState) -> RuntimePluginSnapshot {
    let cwd = app_state.current_working_directory();
    let base_hook_registry = load_hook_registry(&cwd);
    let plugin_load_result = Arc::new(load_plugins(&cwd));
    let hook_registry =
        augment_hook_registry_with_plugins(base_hook_registry, plugin_load_result.as_ref());

    let tool_inventory = build_base_tool_registry();
    let (tool_inventory, plugin_tool_diagnostics) =
        augment_tool_registry_with_plugins(tool_inventory, plugin_load_result.as_ref());
    let plugin_load_result = Arc::new(PluginLoadResult {
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
                    plugin.apply_status = PluginApplyStatus::ApplyFailed;
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

    let coordinator_tools =
        tool_inventory.assemble(crate::tool::registry::ToolAssemblyContext::coordinator(
            app_state.surface,
            app_state.session_mode,
        ));
    let command_registry = Arc::new(build_command_registry(
        app_state,
        plugin_load_result.as_ref(),
    ));
    let runtime_tool_registry = Arc::new(RwLock::new(coordinator_tools.clone()));
    let mut notification_dispatcher = app_state.notification_dispatcher.clone();
    notification_dispatcher.set_hook_registry(hook_registry.clone());

    RuntimePluginSnapshot {
        command_registry,
        tool_registry: coordinator_tools,
        runtime_tool_registry,
        hook_registry,
        plugin_load_result,
        notification_dispatcher,
    }
}

pub async fn rebuild_runtime_plugin_state(
    app_state: &AppState,
) -> anyhow::Result<PluginRuntimeApplyReport> {
    let Some(runtime_plugin_state) = app_state.permission_context.runtime_plugin_state.as_ref()
    else {
        return Ok(PluginRuntimeApplyReport {
            outcome: PluginRuntimeApplyOutcome::Applied,
            generation: 0,
            message: "runtime plugin state is unavailable; nothing was applied".into(),
            diagnostics: Vec::new(),
            orphaned_governance_entries: Vec::new(),
        });
    };
    let snapshot = build_runtime_plugin_snapshot(app_state);
    let has_apply_failures = snapshot.plugin_load_result.plugins.iter().any(|plugin| {
        plugin.apply_status == PluginApplyStatus::ApplyFailed
            || plugin.lifecycle_state == PluginLifecycleState::Error
    });

    let report = if has_apply_failures {
        let generation = runtime_plugin_state.generation().await;
        let failing_plugins = snapshot
            .plugin_load_result
            .plugins
            .iter()
            .filter(|plugin| {
                plugin.apply_status == PluginApplyStatus::ApplyFailed
                    || plugin.lifecycle_state == PluginLifecycleState::Error
            })
            .map(|plugin| plugin.name.clone())
            .collect::<Vec<_>>();
        PluginRuntimeApplyReport {
            outcome: PluginRuntimeApplyOutcome::RetainedPreviousSnapshot,
            generation,
            message: format!(
                "retained runtime plugin snapshot generation {} after plugin apply failure(s): {}",
                generation,
                failing_plugins.join(", ")
            ),
            diagnostics: snapshot.plugin_load_result.diagnostics.clone(),
            orphaned_governance_entries: snapshot
                .plugin_load_result
                .orphaned_governance_entries
                .clone(),
        }
    } else {
        let generation = runtime_plugin_state.replace(snapshot.clone()).await;
        PluginRuntimeApplyReport {
            outcome: PluginRuntimeApplyOutcome::Applied,
            generation,
            message: format!(
                "applied runtime plugin snapshot generation {} (plugins={}, active_commands={}, active_tools={}, active_hooks={})",
                generation,
                snapshot.plugin_load_result.plugins.len(),
                snapshot.plugin_load_result.active_command_count(),
                snapshot.plugin_load_result.active_tool_count(),
                snapshot.plugin_load_result.active_hook_count(),
            ),
            diagnostics: snapshot.plugin_load_result.diagnostics.clone(),
            orphaned_governance_entries: snapshot
                .plugin_load_result
                .orphaned_governance_entries
                .clone(),
        }
    };
    runtime_plugin_state
        .set_last_apply_report(report.clone())
        .await;
    Ok(report)
}

pub fn build_turn_engine(
    app_state: &AppState,
    snapshot: &RuntimePluginSnapshot,
    base_engine: &QueryEngine,
) -> QueryEngine {
    let mut turn_app_state = app_state.clone();
    hydrate_app_state_from_snapshot(&mut turn_app_state, snapshot);
    let active_model_snapshot = turn_app_state
        .active_model_runtime
        .as_ref()
        .map(|runtime| runtime.snapshot_blocking());
    if let Some(active_model_snapshot) = active_model_snapshot.as_ref() {
        turn_app_state.active_model_profile_name =
            active_model_snapshot.active_profile_name.clone();
        turn_app_state.active_model_profile_source = active_model_snapshot.source.clone();
        turn_app_state.active_model_provider_summary = active_model_snapshot.summary.clone();
    }
    QueryEngine::new(QueryContext {
        app_state: turn_app_state.clone(),
        tool_registry: snapshot.tool_registry.clone(),
        api_client: active_model_snapshot
            .map(|snapshot| snapshot.client)
            .unwrap_or_else(|| base_engine.context.api_client.clone()),
        compactor: base_engine.context.compactor.clone(),
        hook_registry: snapshot.hook_registry.clone(),
        agent_id: base_engine.context.agent_id.clone(),
        system_prompt: crate::prompt::system::build_system_prompt(&turn_app_state),
        tools_prompt: crate::prompt::tools::build_tools_prompt(
            &snapshot.tool_registry,
            &turn_app_state.permission_context,
        ),
        context_prompt: crate::prompt::context::build_context_prompt(&turn_app_state),
    })
}

pub fn build_turn_router(snapshot: &RuntimePluginSnapshot) -> CommandRouter {
    CommandRouter::new(
        snapshot.command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    )
}

pub fn hydrate_app_state_from_snapshot(app_state: &mut AppState, snapshot: &RuntimePluginSnapshot) {
    app_state.command_registry = Some(snapshot.command_registry.clone());
    app_state.runtime_tool_registry = Some(snapshot.runtime_tool_registry.clone());
    app_state.plugin_load_result = Some(snapshot.plugin_load_result.clone());
    app_state.notification_dispatcher = snapshot.notification_dispatcher.clone();
}

fn build_command_registry(
    app_state: &AppState,
    plugin_load_result: &PluginLoadResult,
) -> CommandRegistry {
    let registry = crate::command::builtin::register_builtin_commands(CommandRegistry::new());
    let registry = crate::command::coding::register_coding_commands(registry);
    let registry = crate::command::builtin::skills::build_skill_commands(app_state)
        .into_iter()
        .fold(registry, |registry, command| {
            registry.register(Arc::new(command))
        });
    let registry = crate::command::builtin::register_mcp_commands(registry);
    plugin_load_result
        .plugins
        .iter()
        .flat_map(|plugin| plugin.active_commands().into_iter())
        .fold(registry, |registry, command| {
            registry.register(Arc::new(
                crate::command::builtin::plugins::PluginSlashCommand::new(command),
            ))
        })
}

fn build_base_tool_registry() -> ToolRegistry {
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

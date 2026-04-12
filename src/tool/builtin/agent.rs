use async_trait::async_trait;
use serde::Deserialize;

use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::core::context::{QueryContext, SubagentConfig};
use crate::core::message::Message;
use crate::core::query_loop::{QueryParams, run_query_loop_with_params};
use crate::cost::tracker::CostTracker;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::{AppState, RuntimeRole, WorkerRole};
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
enum AgentRequest {
    Spawn(SpawnAgentRequest),
    Continue { task_id: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpawnAgentRequest {
    task: String,
    role: WorkerRole,
    inherit_context: bool,
    max_turns: Option<usize>,
    allowed_tools: Option<Vec<String>>,
    reuse_strategy: ReuseStrategy,
    parent_task_id: Option<String>,
    orchestration_group_id: Option<String>,
    phase: Option<crate::task::types::WorkerPhase>,
    requires_verification: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReuseStrategy {
    RunningOnly,
    Fresh,
}

#[derive(Debug, Deserialize)]
struct AgentJsonRequest {
    task: Option<String>,
    role: Option<String>,
    inherit_context: Option<bool>,
    max_turns: Option<usize>,
    allowed_tools: Option<Vec<String>>,
    reuse_strategy: Option<String>,
    parent_task_id: Option<String>,
    orchestration_group_id: Option<String>,
    phase: Option<String>,
    requires_verification: Option<bool>,
    task_id: Option<String>,
    message: Option<String>,
}

pub struct AgentTool;

#[async_trait]
impl Tool for AgentTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Agent".into(),
            description: "Launch a subagent with isolated context".into(),
            aliases: &["TaskAgent"],
            search_hint: Some("spawn or continue subagent"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: true,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let tasks = permissions
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task manager is not configured"))?;
        let session_id = permissions
            .active_session_id
            .clone()
            .unwrap_or_else(|| "local-session".into());
        let request = parse_agent_request(&call.input)?;
        let parent_context = build_parent_query_context(permissions.clone());
        let dispatcher = parent_context.app_state.notification_dispatcher.clone();

        match request {
            AgentRequest::Spawn(request) => {
                let role_label = request.role.as_str().to_string();
                let task_label = request.task.clone();
                let action = match request.reuse_strategy {
                    ReuseStrategy::RunningOnly => maybe_reuse_running_task(
                        tasks,
                        &session_id,
                        &request.task,
                        request.role,
                        request.orchestration_group_id.as_deref(),
                    ),
                    ReuseStrategy::Fresh => None,
                };
                if let Some(task_id) = action {
                    return Ok(ToolResult::Text(format!(
                        "agent task {} reused for {} worker: {}",
                        task_id, role_label, task_label
                    )));
                }
                let owner_surface = permissions.active_surface.unwrap_or(InteractionSurface::Cli);
                let task = tasks.create(
                    format!("Spawned {} worker for {}", role_label, task_label),
                    session_id.clone(),
                    owner_surface,
                );
                tasks.set_worker_role(&task.id, request.role);
                tasks.set_parent_task_id(&task.id, request.parent_task_id.clone());
                tasks.set_orchestration_group_id(&task.id, request.orchestration_group_id.clone());
                tasks.set_phase(&task.id, request.phase);
                if request.requires_verification {
                    tasks.set_validation_state(
                        &task.id,
                        Some(crate::task::types::ValidationState::PendingVerification),
                    );
                }
                crate::coordinator::mode::set_coordinator_mode(true);
                launch_agent_task(
                    tasks.clone(),
                    &parent_context,
                    task.id.clone(),
                    request,
                    permissions,
                    dispatcher,
                );
                Ok(ToolResult::Text(format!(
                    "agent task {} respawned for {} worker: {}",
                    task.id,
                    role_label,
                    task_label
                )))
            }
            AgentRequest::Continue { task_id, message } => {
                if !tasks.send_message(&task_id, &session_id, message.clone()) {
                    anyhow::bail!("task {task_id} is not running or not owned by this session");
                }
                Ok(ToolResult::Text(format!(
                    "agent task {} accepted message {}",
                    task_id, message
                )))
            }
        }
    }
}

fn launch_agent_task(
    tasks: std::sync::Arc<crate::task::manager::TaskManager>,
    parent_context: &QueryContext,
    task_id: String,
    request: SpawnAgentRequest,
    permissions: &ToolPermissionContext,
    dispatcher: NotificationDispatcher,
) {
    let task_input = request.task.clone();
    let query_context = parent_context.create_subagent_context(
        task_id.clone(),
        permissions
            .subagent_scripted_turns
            .clone()
            .unwrap_or_default(),
        SubagentConfig {
            worker_role: request.role,
            inherit_context: request.inherit_context,
            max_turns: request.max_turns,
            allowed_tools: request.allowed_tools.clone(),
        },
    );
    let tasks_for_run = tasks.clone();
    let launched_task_id = task_id.clone();
    tasks.launch(&launched_task_id.clone(), task_input.clone(), async move {
        let mut params = QueryParams::default();
        params.max_turns = request.max_turns;
        let result = run_query_loop_with_params(&query_context, Message::user(task_input.clone()), params).await;

        if result.messages.is_empty() {
            tasks_for_run.append_output(&launched_task_id, "subagent produced no output");
        } else {
            for message in &result.messages {
                tasks_for_run.append_output(&launched_task_id, format!("{}\n", message.content));
            }
        }

        if matches!(
            result.state,
            crate::core::query_loop::QueryLoopState::Failed
        ) {
            tasks_for_run.fail(&launched_task_id, &dispatcher);
        } else {
            tasks_for_run.complete(&launched_task_id, &dispatcher);
        }
    });
}

fn parse_agent_request(input: &str) -> anyhow::Result<AgentRequest> {
    if let Ok(request) = serde_json::from_str::<AgentJsonRequest>(input) {
        if let (Some(task_id), Some(message)) = (request.task_id, request.message) {
            return Ok(AgentRequest::Continue { task_id, message });
        }
        if let Some(task) = request.task {
            let role = parse_worker_role(request.role.as_deref())?;
            return Ok(AgentRequest::Spawn(SpawnAgentRequest {
                task,
                role,
                inherit_context: request.inherit_context.unwrap_or(true),
                max_turns: request.max_turns,
                allowed_tools: request.allowed_tools,
                reuse_strategy: parse_reuse_strategy(request.reuse_strategy.as_deref(), role)?,
                parent_task_id: request.parent_task_id,
                orchestration_group_id: request.orchestration_group_id,
                phase: parse_worker_phase(request.phase.as_deref())?,
                requires_verification: request.requires_verification.unwrap_or(false),
            }));
        }
        anyhow::bail!("agent JSON input must include either task or task_id/message")
    }

    if let Some(rest) = input.strip_prefix("continue:") {
        let mut parts = rest.splitn(2, ':');
        let task_id = parts.next().unwrap_or_default().trim().to_string();
        let message = parts.next().unwrap_or_default().trim().to_string();
        return Ok(AgentRequest::Continue { task_id, message });
    }

    Ok(AgentRequest::Spawn(SpawnAgentRequest {
        task: input.to_string(),
        role: WorkerRole::Research,
        inherit_context: true,
        max_turns: None,
        allowed_tools: None,
        reuse_strategy: ReuseStrategy::RunningOnly,
        parent_task_id: None,
        orchestration_group_id: None,
        phase: None,
        requires_verification: false,
    }))
}

fn parse_worker_role(value: Option<&str>) -> anyhow::Result<WorkerRole> {
    match value.unwrap_or("research") {
        "research" => Ok(WorkerRole::Research),
        "implement" => Ok(WorkerRole::Implement),
        "verify" => Ok(WorkerRole::Verify),
        other => anyhow::bail!("unknown worker role: {other}"),
    }
}

fn parse_worker_phase(
    value: Option<&str>,
) -> anyhow::Result<Option<crate::task::types::WorkerPhase>> {
    match value {
        Some("research") => Ok(Some(crate::task::types::WorkerPhase::Research)),
        Some("implement") => Ok(Some(crate::task::types::WorkerPhase::Implement)),
        Some("verify") => Ok(Some(crate::task::types::WorkerPhase::Verify)),
        Some(other) => anyhow::bail!("unknown worker phase: {other}"),
        None => Ok(None),
    }
}

fn parse_reuse_strategy(
    value: Option<&str>,
    role: WorkerRole,
) -> anyhow::Result<ReuseStrategy> {
    match value {
        Some("running_only") => Ok(ReuseStrategy::RunningOnly),
        Some("fresh") => Ok(ReuseStrategy::Fresh),
        Some(other) => anyhow::bail!("unknown reuse strategy: {other}"),
        None => Ok(match role {
            WorkerRole::Research => ReuseStrategy::RunningOnly,
            WorkerRole::Implement | WorkerRole::Verify => ReuseStrategy::Fresh,
        }),
    }
}

fn maybe_reuse_running_task(
    tasks: &std::sync::Arc<crate::task::manager::TaskManager>,
    session_id: &str,
    task_description: &str,
    worker_role: WorkerRole,
    orchestration_group_id: Option<&str>,
) -> Option<String> {
    tasks.list().into_iter().find_map(|task| {
        let matches_owner = task.owner.session_id == session_id;
        let matches_role = task.worker_role == Some(worker_role);
        let matches_description = task.description == format!("Spawned {} worker for {}", worker_role.as_str(), task_description);
        let matches_group = task.orchestration_group_id.as_deref() == orchestration_group_id;
        if matches_owner
            && matches_role
            && matches_description
            && matches_group
            && matches!(task.status, crate::task::types::TaskStatus::Running)
        {
            Some(task.id)
        } else {
            None
        }
    })
}

fn build_parent_query_context(permissions: ToolPermissionContext) -> QueryContext {
    let mut runtime_permissions = permissions.clone();
    runtime_permissions
        .add_always_allow_rule(AgentTool.metadata().name);
    runtime_permissions.include_interactive_tools = false;
    runtime_permissions.include_deferred_tools = false;

    let hook_registry = permissions
        .inherited_hook_registry
        .clone()
        .unwrap_or_default();
    let tool_registry = permissions
        .inherited_tool_registry
        .clone()
        .unwrap_or_else(|| ToolRegistry::new().register(std::sync::Arc::new(AgentTool)))
        .assemble_for_role(RuntimeRole::Coordinator);
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: runtime_permissions,
        command_registry: None,
        runtime_tool_registry: Some(std::sync::Arc::new(tokio::sync::RwLock::new(tool_registry.clone()))),
        skill_registry: None,
        mcp_runtime: permissions.mcp_runtime.clone(),
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default())
            .with_hook_registry(hook_registry.clone()),
        startup_trace: Vec::new(),
        active_session_id: permissions
            .active_session_id
            .unwrap_or_else(|| "local-session".into()),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let system_prompt = crate::prompt::system::build_system_prompt(&app_state);
    let tools_prompt = crate::prompt::tools::build_tools_prompt(&tool_registry, &app_state.permission_context);
    let context_prompt = crate::prompt::context::build_context_prompt(&app_state);
    QueryContext {
        app_state,
        tool_registry,
        api_client: crate::service::api::client::ModelProviderClient::default(),
        compactor: ReactiveCompactor,
        hook_registry,
        agent_id: None,
        system_prompt,
        tools_prompt,
        context_prompt,
    }
}

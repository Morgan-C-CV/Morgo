use async_trait::async_trait;

use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::core::message::Message;
use crate::cost::tracker::CostTracker;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::{AppState, RuntimeRole};
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
enum AgentRequest {
    Spawn { prompt: String },
    Continue { task_id: String, message: String },
}

pub struct AgentTool;

#[async_trait]
impl Tool for AgentTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Agent",
            description: "Launch a subagent with isolated context",
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
        let request = parse_agent_request(&call.input);
        let parent_context = build_parent_query_context(permissions.clone());
        let dispatcher = parent_context.app_state.notification_dispatcher.clone();

        match request {
            AgentRequest::Spawn { prompt } => {
                let task = tasks.create(
                    format!("Spawned agent for {}", prompt),
                    session_id.clone(),
                    InteractionSurface::Cli,
                );
                crate::coordinator::mode::set_coordinator_mode(true);
                launch_agent_task(
                    tasks.clone(),
                    &parent_context,
                    task.id.clone(),
                    prompt.clone(),
                    permissions,
                    dispatcher,
                );
                Ok(ToolResult::Text(format!(
                    "agent task {} launched for {}",
                    task.id, prompt
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
    task_input: String,
    permissions: &ToolPermissionContext,
    dispatcher: NotificationDispatcher,
) {
    let query_context = parent_context.create_subagent_context(
        task_id.clone(),
        permissions
            .subagent_scripted_turns
            .clone()
            .unwrap_or_default(),
    );
    let tasks_for_run = tasks.clone();
    let launched_task_id = task_id.clone();
    tasks.launch(&launched_task_id.clone(), task_input.clone(), async move {
        let result = QueryEngine::new(query_context)
            .submit_turn(Message::user(task_input.clone()))
            .await;

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

fn parse_agent_request(input: &str) -> AgentRequest {
    if let Some(rest) = input.strip_prefix("continue:") {
        let mut parts = rest.splitn(2, ':');
        let task_id = parts.next().unwrap_or_default().trim().to_string();
        let message = parts.next().unwrap_or_default().trim().to_string();
        AgentRequest::Continue { task_id, message }
    } else {
        AgentRequest::Spawn {
            prompt: input.to_string(),
        }
    }
}

fn build_parent_query_context(permissions: ToolPermissionContext) -> QueryContext {
    let mut runtime_permissions = permissions.clone();
    runtime_permissions
        .always_allow_rules
        .push(AgentTool.metadata().name.into());
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
        .assemble_for_role(RuntimeRole::Worker);
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        permission_context: runtime_permissions,
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

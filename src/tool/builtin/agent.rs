use async_trait::async_trait;

use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::core::message::Message;
use crate::cost::tracker::CostTracker;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::app_state::AppState;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;

pub struct AgentTool;

#[async_trait]
impl Tool for AgentTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Agent",
            description: "Launch a subagent with isolated context",
            aliases: &["TaskAgent"],
            read_only: false,
            destructive: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
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
        let task = tasks.create(format!("Spawned agent for {}", call.input));

        let parent_context = build_parent_query_context(permissions.clone());
        let query_context = parent_context.create_subagent_context(
            task.id.clone(),
            permissions
                .subagent_scripted_turns
                .clone()
                .unwrap_or_default(),
        );
        let task_id = task.id.clone();
        let task_input = call.input.clone();
        let session_id = parent_context.app_state.active_session_id.clone();
        let dispatcher = parent_context.app_state.notification_dispatcher.clone();
        let tasks_for_run = tasks.clone();

        tasks.launch(&task.id, async move {
            let result = QueryEngine::new(query_context)
                .submit_turn(Message::user(task_input.clone()))
                .await;

            if result.messages.is_empty() {
                tasks_for_run.append_output(&task_id, "subagent produced no output");
            } else {
                for message in &result.messages {
                    tasks_for_run.append_output(&task_id, format!("{}\n", message.content));
                }
            }

            if matches!(
                result.state,
                crate::core::query_loop::QueryLoopState::Failed
            ) {
                tasks_for_run.fail(&task_id, &session_id, &dispatcher);
            } else {
                tasks_for_run.complete(&task_id, &session_id, &dispatcher);
            }
        });

        Ok(ToolResult::Text(format!(
            "agent task {} launched for {}",
            task.id, call.input
        )))
    }
}

fn build_parent_query_context(permissions: ToolPermissionContext) -> QueryContext {
    let mut runtime_permissions = permissions.clone();
    runtime_permissions
        .always_allow_rules
        .push(AgentTool.metadata().name.into());

    let hook_registry = permissions
        .inherited_hook_registry
        .clone()
        .unwrap_or_default();
    let tool_registry = permissions
        .inherited_tool_registry
        .clone()
        .unwrap_or_else(|| ToolRegistry::new().register(std::sync::Arc::new(AgentTool)));
    QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            permission_context: runtime_permissions,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default())
                .with_hook_registry(hook_registry.clone()),
            startup_trace: Vec::new(),
            active_session_id: permissions
                .active_session_id
                .unwrap_or_else(|| "local-session".into()),
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry,
        api_client: crate::service::api::client::AnthropicClient::default(),
        compactor: ReactiveCompactor,
        hook_registry,
        agent_id: None,
    }
}

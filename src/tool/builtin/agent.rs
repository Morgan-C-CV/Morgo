use async_trait::async_trait;

use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::core::message::Message;
use crate::cost::tracker::CostTracker;
use crate::hook::registry::HookRegistry;
use crate::service::api::client::AnthropicClient;
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
        tasks.start(&task.id);

        let mut subagent_permissions = permissions.clone();
        subagent_permissions
            .always_allow_rules
            .push(self.metadata().name.into());

        let app_state = AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            permission_context: subagent_permissions.clone(),
            cost_tracker: CostTracker::default(),
            notification_dispatcher: crate::interaction::dispatcher::NotificationDispatcher::new(
                crate::interaction::telegram::gateway::TelegramGateway::default(),
            ),
            startup_trace: Vec::new(),
            active_session_id: permissions
                .active_session_id
                .clone()
                .unwrap_or_else(|| "local-session".into()),
            session: None,
            history: None,
            restored_session: None,
        };

        let query_context = QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: AnthropicClient::with_scripted_turns(
                permissions
                    .subagent_scripted_turns
                    .clone()
                    .unwrap_or_default(),
            ),
            compactor: ReactiveCompactor,
            hook_registry: HookRegistry::default(),
            agent_id: Some(task.id.clone()),
        };

        let result = QueryEngine::new(query_context)
            .submit_turn(Message::user(call.input.clone()))
            .await;

        if result.messages.is_empty() {
            tasks.append_output(&task.id, "subagent produced no output");
        } else {
            for message in &result.messages {
                tasks.append_output(&task.id, format!("{}\n", message.content));
            }
        }

        if matches!(
            result.state,
            crate::core::query_loop::QueryLoopState::Failed
        ) {
            tasks.fail(
                &task.id,
                &app_state.active_session_id,
                &app_state.notification_dispatcher,
            );
        } else {
            tasks.complete(
                &task.id,
                &app_state.active_session_id,
                &app_state.notification_dispatcher,
            );
        }

        Ok(ToolResult::Text(format!(
            "agent task {} completed for {}",
            task.id, call.input
        )))
    }
}

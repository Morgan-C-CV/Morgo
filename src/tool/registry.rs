use std::sync::Arc;

use crate::bootstrap::{InteractionSurface, SessionMode};
use crate::core::boss_state::BossStage;
use crate::state::app_state::RuntimeRole;
use crate::state::permission_context::{BossActorPolicy, ToolPermissionContext};
use crate::tool::definition::{
    InterruptBehavior, ModelToolDefinition, ObservableInput, Tool, ToolCall, ToolMetadata,
    ToolResult,
};
use crate::tool::permission::{evaluate_tool_permission, is_tool_allowed};

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: Vec<Arc<dyn Tool>>,
}

impl std::fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolRegistry")
            .field("tool_count", &self.tools.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolAssemblyEnvironment {
    Standard,
    Restricted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolAssemblyContext {
    pub runtime_role: RuntimeRole,
    pub surface: InteractionSurface,
    pub session_mode: SessionMode,
    pub environment: ToolAssemblyEnvironment,
    pub include_deferred_tools: bool,
    pub include_interactive_tools: bool,
    pub include_open_world_tools: bool,
    /// When set, the Agent tool visibility is governed by boss spawn policy.
    pub boss_actor_policy: Option<BossActorPolicy>,
}

impl ToolAssemblyContext {
    pub fn coordinator(surface: InteractionSurface, session_mode: SessionMode) -> Self {
        let include_open_world_tools = match (surface, session_mode) {
            (InteractionSurface::Cli, SessionMode::Interactive) => true,
            (InteractionSurface::Cli, SessionMode::Print)
            | (InteractionSurface::Cli, SessionMode::InitOnly)
            | (InteractionSurface::Cli, SessionMode::Headless)
            | (InteractionSurface::Remote, _)
            | (InteractionSurface::Telegram, _) => false,
        };
        Self {
            runtime_role: RuntimeRole::Coordinator,
            surface,
            session_mode,
            environment: ToolAssemblyEnvironment::Standard,
            include_deferred_tools: true,
            include_interactive_tools: true,
            include_open_world_tools,
            boss_actor_policy: None,
        }
    }

    pub fn worker(surface: InteractionSurface, session_mode: SessionMode) -> Self {
        Self {
            runtime_role: RuntimeRole::Worker,
            surface,
            session_mode,
            environment: ToolAssemblyEnvironment::Restricted,
            include_deferred_tools: false,
            include_interactive_tools: false,
            include_open_world_tools: false,
            boss_actor_policy: None,
        }
    }

    /// Worker context for ExecutorB in Execution phase — may see a restricted Agent tool.
    pub fn executor_b(surface: InteractionSurface, session_mode: SessionMode) -> Self {
        Self {
            boss_actor_policy: Some(BossActorPolicy::executor_b(BossStage::Execution)),
            include_interactive_tools: true,
            ..Self::worker(surface, session_mode)
        }
    }

    /// Returns true when this context represents ExecutorB in Execution phase.
    pub fn is_boss_executor_b(&self) -> bool {
        self.boss_actor_policy
            .map(|p| p.may_spawn())
            .unwrap_or(false)
    }

    pub fn permission_context(
        &self,
        mode: crate::state::permission_context::PermissionMode,
    ) -> ToolPermissionContext {
        ToolPermissionContext::new(mode)
            .with_active_surface(self.surface)
            .with_deferred_tools(self.include_deferred_tools)
            .with_interactive_tools(self.include_interactive_tools)
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, tool: Arc<dyn Tool>) -> Self {
        let metadata = tool.metadata();
        assert!(
            self.tools.iter().all(|existing| {
                let existing_metadata = existing.metadata();
                existing_metadata.name != metadata.name
                    && !existing_metadata
                        .aliases
                        .iter()
                        .any(|alias| *alias == metadata.name)
                    && !metadata.aliases.iter().any(|alias| {
                        *alias == existing_metadata.name
                            || existing_metadata
                                .aliases
                                .iter()
                                .any(|existing_alias| existing_alias == alias)
                    })
            }),
            "duplicate or conflicting tool registration: {}",
            metadata.name
        );
        self.tools.push(tool);
        self.tools.sort_by_key(|tool| tool.metadata().name);
        self
    }

    pub fn visible_tools(&self, permissions: &ToolPermissionContext) -> Vec<ToolMetadata> {
        self.tools
            .iter()
            .map(|tool| tool.metadata())
            .filter(|metadata| {
                metadata.always_load
                    || (!metadata.should_defer || permissions.include_deferred_tools)
            })
            .filter(|metadata| {
                !metadata.requires_user_interaction || permissions.include_interactive_tools
            })
            .filter(|metadata| is_tool_allowed(metadata, permissions))
            .collect()
    }

    pub fn visible_model_tools(
        &self,
        permissions: &ToolPermissionContext,
    ) -> Vec<ModelToolDefinition> {
        self.tools
            .iter()
            .filter_map(|tool| {
                let metadata = tool.metadata();
                if !(metadata.always_load
                    || (!metadata.should_defer || permissions.include_deferred_tools))
                {
                    return None;
                }
                if metadata.requires_user_interaction && !permissions.include_interactive_tools {
                    return None;
                }
                if !is_tool_allowed(&metadata, permissions) {
                    return None;
                }
                let input_schema = tool.input_schema()?;
                Some(ModelToolDefinition {
                    name: metadata.name.to_string(),
                    description: metadata.description.to_string(),
                    input_schema,
                })
            })
            .collect()
    }

    pub fn all_metadata(&self) -> Vec<ToolMetadata> {
        self.tools.iter().map(|tool| tool.metadata()).collect()
    }

    pub fn assemble(&self, context: ToolAssemblyContext) -> Self {
        let permissions =
            context.permission_context(crate::state::permission_context::PermissionMode::Default);
        let tools = self
            .tools
            .iter()
            .filter(|tool| {
                let metadata = tool.metadata();
                if metadata.is_open_world && !context.include_open_world_tools {
                    return false;
                }
                match context.runtime_role {
                    RuntimeRole::Coordinator => is_tool_allowed(&metadata, &permissions),
                    RuntimeRole::Worker => {
                        if metadata.name == "Agent" || metadata.aliases.contains(&"Agent") {
                            // Only ExecutorB in Execution phase may see Agent.
                            return context.is_boss_executor_b()
                                && is_tool_allowed(&metadata, &permissions);
                        }
                        is_tool_allowed(&metadata, &permissions)
                    }
                }
            })
            .cloned()
            .collect();
        Self { tools }
    }

    pub fn assemble_for_role(&self, role: RuntimeRole) -> Self {
        let context = match role {
            RuntimeRole::Coordinator => {
                ToolAssemblyContext::coordinator(InteractionSurface::Cli, SessionMode::Interactive)
            }
            RuntimeRole::Worker => {
                ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless)
            }
        };
        self.assemble(context)
    }

    pub fn filter_for_worker(&self) -> Self {
        self.assemble_for_role(RuntimeRole::Worker)
    }

    pub fn assemble_worker_registry(&self, allowed_tools: Option<&[String]>) -> Self {
        let worker = self.assemble_for_role(RuntimeRole::Worker);
        let Some(allowed_tools) = allowed_tools else {
            return worker;
        };
        let tools = worker
            .tools
            .iter()
            .filter(|tool| {
                let metadata = tool.metadata();
                allowed_tools.iter().any(|allowed| {
                    allowed == metadata.name
                        || metadata.aliases.iter().any(|alias| allowed == alias)
                })
            })
            .cloned()
            .collect();
        Self { tools }
    }

    pub fn find(&self, call: &ToolCall) -> Option<&Arc<dyn Tool>> {
        self.tools.iter().find(|tool| {
            let metadata = tool.metadata();
            metadata.name == call.name || metadata.aliases.iter().any(|alias| *alias == call.name)
        })
    }

    pub fn is_concurrency_safe(&self, call: &ToolCall) -> Option<bool> {
        self.find(call).map(|tool| tool.is_concurrency_safe(call))
    }

    pub fn interrupt_behavior(&self, call: &ToolCall) -> Option<InterruptBehavior> {
        self.find(call).map(|tool| tool.interrupt_behavior())
    }

    pub fn observable_input(&self, call: &ToolCall) -> Option<ObservableInput> {
        self.find(call).and_then(|tool| {
            tool.backfill_observable_input(call)
                .or_else(|| tool.observable_input(call))
        })
    }

    pub async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let Some(tool) = self.find(call) else {
            return Ok(ToolResult::Interrupted(format!(
                "unknown tool {}",
                call.name
            )));
        };

        let metadata = tool.metadata();
        if tool.input_schema().is_some() && call.json_input().is_none() {
            return Ok(ToolResult::Interrupted(format!(
                "tool {} requires JSON-structured input",
                metadata.name
            )));
        }
        if let Err(error) = tool.validate_input(call).await {
            return Ok(ToolResult::Interrupted(format!(
                "invalid input for {}: {}",
                metadata.name, error
            )));
        }
        let base_decision = evaluate_tool_permission(&metadata, call, permissions);
        let tool_decision = tool.check_permissions(call, permissions).await;
        let resolved_decision = merge_permission_decisions(base_decision, tool_decision);
        match resolved_decision {
            crate::tool::definition::PermissionDecision::Allow => {
                match tool.invoke(call, permissions).await {
                    Ok(result) => Ok(result),
                    Err(error) => Ok(ToolResult::Interrupted(error.to_string())),
                }
            }
            crate::tool::definition::PermissionDecision::Ask {
                message,
                metadata: approval_metadata,
                ..
            } => {
                let approval = if let Some(approval_metadata) = approval_metadata {
                    crate::tool::result::PendingApprovalPayload {
                        code: approval_metadata.code,
                        summary: approval_metadata
                            .summary
                            .unwrap_or_else(|| format!("{} pending approval", metadata.name)),
                        detail: approval_metadata.detail.or_else(|| Some(message.clone())),
                        approval_kind: approval_metadata
                            .approval_kind
                            .or_else(|| Some("tool_permission".into())),
                        escalation_reasons: approval_metadata.escalation_reasons,
                    }
                } else {
                    crate::tool::result::PendingApprovalPayload {
                        code: None,
                        summary: format!("{} pending approval", metadata.name),
                        detail: Some(message.clone()),
                        approval_kind: Some("tool_permission".into()),
                        escalation_reasons: Vec::new(),
                    }
                };
                Ok(ToolResult::PendingApproval {
                    tool_name: metadata.name.to_string(),
                    approval,
                    message,
                })
            }
            crate::tool::definition::PermissionDecision::Deny { message, .. } => {
                Ok(ToolResult::Denied(message))
            }
        }
    }

    pub async fn invoke_with_approval(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let Some(tool) = self.find(call) else {
            return Ok(ToolResult::Interrupted(format!(
                "unknown tool {}",
                call.name
            )));
        };
        let metadata = tool.metadata();
        if tool.input_schema().is_some() && call.json_input().is_none() {
            return Ok(ToolResult::Interrupted(format!(
                "tool {} requires JSON-structured input",
                metadata.name
            )));
        }
        if let Err(error) = tool.validate_input(call).await {
            return Ok(ToolResult::Interrupted(format!(
                "invalid input for {}: {}",
                metadata.name, error
            )));
        }
        match tool.invoke(call, permissions).await {
            Ok(result) => Ok(result),
            Err(error) => Ok(ToolResult::Interrupted(error.to_string())),
        }
    }
}

fn merge_permission_decisions(
    base: crate::tool::definition::PermissionDecision,
    tool: crate::tool::definition::PermissionDecision,
) -> crate::tool::definition::PermissionDecision {
    use crate::tool::definition::PermissionDecision::{Allow, Ask, Deny};

    match (base, tool) {
        (Deny { message, reason }, _) | (_, Deny { message, reason }) => Deny { message, reason },
        (
            Ask {
                message,
                reason,
                metadata,
            },
            _,
        ) => Ask {
            message,
            reason,
            metadata,
        },
        (
            _,
            Ask {
                message,
                reason,
                metadata,
            },
        ) => Ask {
            message,
            reason,
            metadata,
        },
        (Allow, Allow) => Allow,
    }
}

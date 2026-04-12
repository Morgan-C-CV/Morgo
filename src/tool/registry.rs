use std::sync::Arc;

use crate::state::app_state::RuntimeRole;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{InterruptBehavior, Tool, ToolCall, ToolMetadata, ToolResult};
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

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, tool: Arc<dyn Tool>) -> Self {
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

    pub fn all_metadata(&self) -> Vec<ToolMetadata> {
        self.tools.iter().map(|tool| tool.metadata()).collect()
    }

    pub fn assemble_for_role(&self, role: RuntimeRole) -> Self {
        let tools = self
            .tools
            .iter()
            .filter(|tool| match role {
                RuntimeRole::Coordinator => true,
                RuntimeRole::Worker => {
                    let metadata = tool.metadata();
                    metadata.name != "Agent"
                        && !metadata.requires_user_interaction
                        && (!metadata.should_defer || metadata.always_load)
                }
            })
            .cloned()
            .collect();
        Self { tools }
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
                    allowed == metadata.name || metadata.aliases.iter().any(|alias| allowed == alias)
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

    pub async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let tool = self
            .find(call)
            .ok_or_else(|| anyhow::anyhow!("unknown tool {}", call.name))?;

        let metadata = tool.metadata();
        if tool.input_schema().is_some() && call.json_input().is_none() {
            anyhow::bail!("tool {} requires JSON-structured input", metadata.name);
        }
        tool.validate_input(call).await?;
        let base_decision = evaluate_tool_permission(&metadata, call, permissions);
        let tool_decision = tool.check_permissions(call, permissions).await;
        let resolved_decision = merge_permission_decisions(base_decision, tool_decision);
        match resolved_decision {
            crate::tool::definition::PermissionDecision::Allow => {
                tool.invoke(call, permissions).await
            }
            crate::tool::definition::PermissionDecision::Ask { message, .. } => {
                Ok(ToolResult::PendingApproval {
                    tool_name: metadata.name.to_string(),
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
        let tool = self
            .find(call)
            .ok_or_else(|| anyhow::anyhow!("unknown tool {}", call.name))?;
        let metadata = tool.metadata();
        if tool.input_schema().is_some() && call.json_input().is_none() {
            anyhow::bail!("tool {} requires JSON-structured input", metadata.name);
        }
        tool.validate_input(call).await?;
        tool.invoke(call, permissions).await
    }
}

fn merge_permission_decisions(
    base: crate::tool::definition::PermissionDecision,
    tool: crate::tool::definition::PermissionDecision,
) -> crate::tool::definition::PermissionDecision {
    use crate::tool::definition::PermissionDecision::{Allow, Ask, Deny};

    match (base, tool) {
        (Deny { message, reason }, _) | (_, Deny { message, reason }) => Deny { message, reason },
        (Ask { message, reason }, _) => Ask { message, reason },
        (_, Ask { message, reason }) => Ask { message, reason },
        (Allow, Allow) => Allow,
    }
}

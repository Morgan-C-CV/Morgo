use std::sync::Arc;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
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
            .filter(|metadata| is_tool_allowed(metadata, permissions))
            .collect()
    }

    pub fn filter_for_worker(&self) -> Self {
        let tools = self
            .tools
            .iter()
            .filter(|tool| tool.metadata().name != "Agent")
            .cloned()
            .collect();
        Self { tools }
    }

    pub async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let tool = self
            .tools
            .iter()
            .find(|tool| {
                let metadata = tool.metadata();
                metadata.name == call.name
                    || metadata.aliases.iter().any(|alias| *alias == call.name)
            })
            .ok_or_else(|| anyhow::anyhow!("unknown tool {}", call.name))?;

        let metadata = tool.metadata();
        tool.validate_input(call).await?;
        let base_decision = evaluate_tool_permission(&metadata, call, permissions);
        let tool_decision = tool.check_permissions(call, permissions).await;
        match merge_permission_decisions(base_decision, tool_decision) {
            crate::tool::definition::PermissionDecision::Allow => {
                tool.invoke(call, permissions).await
            }
            crate::tool::definition::PermissionDecision::Ask(reason)
            | crate::tool::definition::PermissionDecision::Deny(reason) => {
                Ok(ToolResult::Denied(reason))
            }
        }
    }
}

fn merge_permission_decisions(
    base: crate::tool::definition::PermissionDecision,
    tool: crate::tool::definition::PermissionDecision,
) -> crate::tool::definition::PermissionDecision {
    use crate::tool::definition::PermissionDecision::{Allow, Ask, Deny};

    match (base, tool) {
        (Deny(reason), _) | (_, Deny(reason)) => Deny(reason),
        (Ask(reason), _) => Ask(reason),
        (_, Ask(reason)) => Ask(reason),
        (Allow, Allow) => Allow,
    }
}

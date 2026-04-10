use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolMetadata {
    pub name: &'static str,
    pub description: &'static str,
    pub aliases: &'static [&'static str],
    pub read_only: bool,
    pub destructive: bool,
    pub always_load: bool,
    pub should_defer: bool,
    pub requires_auth: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub name: String,
    pub input: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny(String),
    Ask(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolResult {
    Text(String),
    Denied(String),
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn metadata(&self) -> ToolMetadata;

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        if call.input.trim().is_empty() {
            anyhow::bail!("tool input cannot be empty")
        }
        Ok(())
    }

    async fn check_permissions(
        &self,
        _call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> PermissionDecision {
        if permissions
            .always_deny_rules
            .iter()
            .any(|rule| rule == self.metadata().name)
        {
            PermissionDecision::Deny(format!("tool {} denied by policy", self.metadata().name))
        } else {
            PermissionDecision::Allow
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult>;
}

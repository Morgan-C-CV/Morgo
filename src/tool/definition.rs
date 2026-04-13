use async_trait::async_trait;
use serde_json::Value;

use crate::state::permission_context::ToolPermissionContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterruptBehavior {
    Block,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObservableInputSource {
    Raw,
    Backfilled,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObservableInput {
    pub value: String,
    pub source: ObservableInputSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolMetadata {
    pub name: &'static str,
    pub description: &'static str,
    pub aliases: &'static [&'static str],
    pub search_hint: Option<&'static str>,
    pub read_only: bool,
    pub destructive: bool,
    pub concurrency_safe: bool,
    pub always_load: bool,
    pub should_defer: bool,
    pub requires_auth: bool,
    pub requires_user_interaction: bool,
    pub is_open_world: bool,
    pub is_search_or_read_command: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub name: String,
    pub input: String,
}

impl ToolCall {
    pub fn new(name: impl Into<String>, input: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            input: input.into(),
        }
    }

    pub fn raw_input(&self) -> &str {
        &self.input
    }

    pub fn json_input(&self) -> Option<Value> {
        serde_json::from_str(&self.input).ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecisionReason {
    Rule,
    Mode,
    Tool,
    Hook,
    Safety,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny {
        message: String,
        reason: PermissionDecisionReason,
    },
    Ask {
        message: String,
        reason: PermissionDecisionReason,
    },
}

impl PermissionDecision {
    pub fn is_deny(&self) -> bool {
        matches!(self, Self::Deny { .. })
    }

    pub fn is_ask(&self) -> bool {
        matches!(self, Self::Ask { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolResult {
    Text(String),
    Denied(String),
    PendingApproval { tool_name: String, message: String },
    Interrupted(String),
    Progress(String),
    ResultTooLarge(String),
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn metadata(&self) -> ToolMetadata;

    fn input_schema(&self) -> Option<Value> {
        None
    }

    fn output_schema(&self) -> Option<Value> {
        None
    }

    fn max_result_size_chars(&self) -> usize {
        usize::MAX
    }

    fn observable_input(&self, call: &ToolCall) -> Option<ObservableInput> {
        let raw = call.raw_input().trim();
        if raw.is_empty() {
            None
        } else {
            Some(ObservableInput {
                value: raw.to_string(),
                source: ObservableInputSource::Raw,
            })
        }
    }

    fn backfill_observable_input(&self, _call: &ToolCall) -> Option<ObservableInput> {
        None
    }

    fn is_concurrency_safe(&self, _call: &ToolCall) -> bool {
        self.metadata().concurrency_safe
    }

    fn interrupt_behavior(&self) -> InterruptBehavior {
        InterruptBehavior::Block
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        if call.raw_input().trim().is_empty() && call.json_input().is_none() {
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
            .always_deny_rules()
            .iter()
            .any(|rule| rule == self.metadata().name)
        {
            PermissionDecision::Deny {
                message: format!("tool {} denied by policy", self.metadata().name),
                reason: PermissionDecisionReason::Rule,
            }
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

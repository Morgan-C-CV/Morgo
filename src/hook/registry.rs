use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookEvent {
    SessionStart,
    Setup,
    UserPromptSubmit,
    PreToolUse { tool_name: String },
    PostToolUse { tool_name: String },
    PostToolUseFailure { tool_name: String },
    Stop,
    SubagentStop,
    Notification,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRule {
    pub event: HookEventMatcher,
    pub deny_match: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookEventMatcher {
    SessionStart,
    Setup,
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    Stop,
    SubagentStop,
    Notification,
}

#[derive(Debug, Clone, Default)]
pub struct HookRegistry {
    rules: Vec<HookRule>,
    events: Arc<RwLock<Vec<HookEvent>>>,
}

impl HookRegistry {
    pub fn register_rule(mut self, rule: HookRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn rules(&self) -> &[HookRule] {
        &self.rules
    }

    pub fn record(&self, event: HookEvent) {
        self.events
            .write()
            .expect("hook events poisoned")
            .push(event);
    }

    pub fn recorded_events(&self) -> Vec<HookEvent> {
        self.events.read().expect("hook events poisoned").clone()
    }
}

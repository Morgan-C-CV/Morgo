use crate::core::message::Message;
use crate::hook::registry::{HookEvent, HookEventMatcher, HookRegistry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookDecision {
    Allow,
    Deny(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookResult {
    pub decision: HookDecision,
    pub messages: Vec<Message>,
    pub prevent_continuation: bool,
}

impl HookResult {
    pub fn allow() -> Self {
        Self {
            decision: HookDecision::Allow,
            messages: Vec::new(),
            prevent_continuation: false,
        }
    }
}

pub fn run_hook(registry: &HookRegistry, event: HookEvent) -> HookResult {
    registry.record(event.clone());

    let mut result = HookResult::allow();

    for rule in registry.rules() {
        if !matches_event(&rule.event, &event) {
            continue;
        }

        if let Some(message) = &rule.append_message {
            result.messages.push(Message::assistant(message.clone()));
        }
        if rule.prevent_continuation {
            result.prevent_continuation = true;
        }

        if let Some(deny_match) = &rule.deny_match {
            if matches_denial(&event, deny_match) {
                result.decision = HookDecision::Deny(match &event {
                    HookEvent::PreToolUse { tool_name }
                    | HookEvent::PostToolUse { tool_name }
                    | HookEvent::PostToolUseFailure { tool_name } => {
                        format!("tool {tool_name} denied by hook policy")
                    }
                    _ => format!("hook event denied by policy: {deny_match}"),
                });
                return result;
            }
        }
    }

    result
}

fn matches_event(matcher: &HookEventMatcher, event: &HookEvent) -> bool {
    matches!(
        (matcher, event),
        (HookEventMatcher::SessionStart, HookEvent::SessionStart)
            | (HookEventMatcher::Setup, HookEvent::Setup)
            | (
                HookEventMatcher::UserPromptSubmit,
                HookEvent::UserPromptSubmit
            )
            | (HookEventMatcher::PreToolUse, HookEvent::PreToolUse { .. })
            | (HookEventMatcher::PostToolUse, HookEvent::PostToolUse { .. })
            | (
                HookEventMatcher::PostToolUseFailure,
                HookEvent::PostToolUseFailure { .. }
            )
            | (HookEventMatcher::Stop, HookEvent::Stop)
            | (HookEventMatcher::SubagentStop, HookEvent::SubagentStop)
            | (HookEventMatcher::Notification, HookEvent::Notification)
    )
}

fn matches_denial(event: &HookEvent, deny_match: &str) -> bool {
    match event {
        HookEvent::PreToolUse { tool_name }
        | HookEvent::PostToolUse { tool_name }
        | HookEvent::PostToolUseFailure { tool_name } => tool_name == deny_match,
        _ => true,
    }
}

use crate::core::message::Message;
use crate::hook::output::HookPayload;
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
    pub payload: HookPayload,
}

impl HookResult {
    pub fn allow() -> Self {
        Self {
            decision: HookDecision::Allow,
            messages: Vec::new(),
            prevent_continuation: false,
            payload: HookPayload::default(),
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
        if let Some(permission_decision) = &rule.permission_decision {
            result.payload.permission_decision = Some(permission_decision.clone());
            result.payload.permission_reason = Some(format!("hook rule set permission to {permission_decision}"));
        }
        if let Some(updated_input) = &rule.updated_input {
            result.payload.updated_input = Some(updated_input.clone());
        }
        if let Some(additional_context) = &rule.additional_context {
            result.payload.additional_context = Some(additional_context.clone());
            result.messages.push(Message::assistant(additional_context.clone()));
        }

        if let Some(deny_match) = &rule.deny_match {
            if matches_denial(&event, deny_match) {
                result.decision = HookDecision::Deny(match &event {
                    HookEvent::PreToolUse { tool_name }
                    | HookEvent::PostToolUse { tool_name }
                    | HookEvent::PostToolUseFailure { tool_name } => {
                        format!("tool {tool_name} denied by hook policy")
                    }
                    HookEvent::Notification {
                        notification_type, ..
                    } => format!(
                        "notification {notification_type} denied by hook policy: {deny_match}"
                    ),
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
            | (
                HookEventMatcher::Notification,
                HookEvent::Notification { .. }
            )
    )
}

fn matches_denial(event: &HookEvent, deny_match: &str) -> bool {
    match event {
        HookEvent::PreToolUse { tool_name }
        | HookEvent::PostToolUse { tool_name }
        | HookEvent::PostToolUseFailure { tool_name } => tool_name == deny_match,
        HookEvent::Notification {
            title,
            body,
            notification_type,
            task_id,
            status,
            output_file,
        } => {
            notification_type == deny_match
                || title.contains(deny_match)
                || body.contains(deny_match)
                || task_id.as_deref() == Some(deny_match)
                || status.as_deref() == Some(deny_match)
                || output_file
                    .as_deref()
                    .is_some_and(|path| path.contains(deny_match))
        }
        _ => true,
    }
}

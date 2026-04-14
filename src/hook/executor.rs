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
    pub block_continuation: bool,
    pub payload: HookPayload,
}

impl HookResult {
    pub fn allow() -> Self {
        Self {
            decision: HookDecision::Allow,
            messages: Vec::new(),
            prevent_continuation: false,
            block_continuation: false,
            payload: HookPayload::default(),
        }
    }
}

pub fn run_hook(registry: &HookRegistry, event: HookEvent) -> HookResult {
    registry.record(event.clone());

    let mut result = HookResult::allow();
    let mut matched_rules = registry
        .rules()
        .iter()
        .filter(|rule| matches_event(&rule.event, &event))
        .collect::<Vec<_>>();
    matched_rules.sort_by_key(|rule| rule.layer.precedence());

    for rule in matched_rules {

        if let Some(message) = &rule.append_message {
            result.messages.push(Message::assistant(message.clone()));
        }
        if rule.prevent_continuation {
            result.prevent_continuation = true;
        }
        if rule.block_continuation {
            result.block_continuation = true;
        }
        if let Some(permission_decision) = &rule.permission_decision {
            let updated_input = rule.updated_input.clone();
            let reason = Some(format!("hook rule set permission to {permission_decision}"));
            result.payload.permission_result = match permission_decision.as_str() {
                "allow" => crate::hook::output::HookPermissionResult::Allow {
                    updated_input,
                    reason,
                },
                "ask" => crate::hook::output::HookPermissionResult::Ask {
                    updated_input,
                    reason,
                },
                "deny" => crate::hook::output::HookPermissionResult::Deny {
                    updated_input,
                    reason,
                },
                _ => crate::hook::output::HookPermissionResult::Passthrough,
            };
        } else if let Some(updated_input) = &rule.updated_input {
            result.payload.updated_input = Some(updated_input.clone());
        }
        if let Some(additional_context) = &rule.additional_context {
            result.payload.additional_context = Some(additional_context.clone());
            result
                .messages
                .push(Message::assistant(additional_context.clone()));
        }

        if let Some(deny_match) = &rule.deny_match {
            if matches_denial(&event, deny_match) {
                result.decision = HookDecision::Deny(match &event {
                    HookEvent::PreToolUse { tool_name }
                    | HookEvent::PostToolUse { tool_name }
                    | HookEvent::PostToolUseFailure { tool_name }
                    | HookEvent::PermissionRequest { tool_name } => {
                        format!("tool {tool_name} denied by hook policy")
                    }
                    HookEvent::PermissionDenied { tool_name, reason } => {
                        format!("tool {tool_name} denied after permission rejection: {reason}")
                    }
                    HookEvent::Notification {
                        notification_type, ..
                    } => format!(
                        "notification {notification_type} denied by hook policy: {deny_match}"
                    ),
                    _ => format!("hook event denied by policy: {deny_match}"),
                });
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
            | (
                HookEventMatcher::PermissionRequest,
                HookEvent::PermissionRequest { .. }
            )
            | (
                HookEventMatcher::PermissionDenied,
                HookEvent::PermissionDenied { .. }
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
        | HookEvent::PostToolUseFailure { tool_name }
        | HookEvent::PermissionRequest { tool_name } => tool_name == deny_match,
        HookEvent::PermissionDenied { tool_name, reason } => {
            tool_name == deny_match || reason.contains(deny_match)
        }
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

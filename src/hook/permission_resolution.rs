use crate::hook::output::HookPermissionResult;
use crate::tool::definition::{PermissionDecision, PermissionDecisionReason};

pub fn resolve_hook_permission_decision(
    hook_result: &HookPermissionResult,
    base_decision: PermissionDecision,
) -> PermissionDecision {
    match hook_result {
        HookPermissionResult::Passthrough => base_decision,
        HookPermissionResult::Deny { reason, .. } => PermissionDecision::Deny {
            message: reason.clone().unwrap_or_else(|| "Denied by hook".into()),
            reason: PermissionDecisionReason::Hook,
        },
        HookPermissionResult::Ask { reason, .. } => match base_decision {
            PermissionDecision::Deny { .. } => base_decision,
            _ => PermissionDecision::Ask {
                message: reason
                    .clone()
                    .unwrap_or_else(|| "Hook requires explicit approval".into()),
                reason: PermissionDecisionReason::Hook,
                metadata: None,
            },
        },
        HookPermissionResult::Allow { .. } => match base_decision {
            PermissionDecision::Deny { .. } | PermissionDecision::Ask { .. } => base_decision,
            PermissionDecision::Allow => PermissionDecision::Allow,
        },
    }
}

pub fn updated_input_from_hook(hook_result: &HookPermissionResult) -> Option<String> {
    match hook_result {
        HookPermissionResult::Allow { updated_input, .. }
        | HookPermissionResult::Ask { updated_input, .. }
        | HookPermissionResult::Deny { updated_input, .. } => updated_input.clone(),
        HookPermissionResult::Passthrough => None,
    }
}

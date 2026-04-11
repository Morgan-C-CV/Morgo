use rust_agent::hook::output::HookPermissionResult;
use rust_agent::hook::permission_resolution::{resolve_hook_permission_decision, updated_input_from_hook};
use rust_agent::tool::definition::{PermissionDecision, PermissionDecisionReason};

#[test]
fn hook_allow_cannot_override_base_deny() {
    let resolved = resolve_hook_permission_decision(
        &HookPermissionResult::Allow {
            updated_input: Some("patched".into()),
            reason: Some("hook says allow".into()),
        },
        PermissionDecision::Deny {
            message: "base deny".into(),
            reason: PermissionDecisionReason::Rule,
        },
    );

    assert!(matches!(resolved, PermissionDecision::Deny { .. }));
}

#[test]
fn hook_ask_upgrades_allow_to_ask() {
    let resolved = resolve_hook_permission_decision(
        &HookPermissionResult::Ask {
            updated_input: None,
            reason: Some("confirm first".into()),
        },
        PermissionDecision::Allow,
    );

    assert!(matches!(resolved, PermissionDecision::Ask { .. }));
}

#[test]
fn hook_updated_input_is_extracted_from_permission_result() {
    let updated = updated_input_from_hook(&HookPermissionResult::Deny {
        updated_input: Some("rewritten-input".into()),
        reason: Some("blocked".into()),
    });

    assert_eq!(updated.as_deref(), Some("rewritten-input"));
}

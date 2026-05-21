use std::path::Path;

use rust_agent::security::workspace_capability::{
    CapabilityCheckOutcome, CapabilityRequirementReason, CapabilityTier,
    CommandCapabilityRequirement, WorkspaceCapabilityConfig, WorkspaceCapabilityScope,
    WorkspacePermissionCheck, WorkspacePermissionConfig, WorkspacePermissionEntry,
    WorkspacePermissionLevel, check_bash_capability, requirement_from_policy,
};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::bash::permissions::evaluate_bash_policy_with_context;
use rust_agent::tool::builtin::{
    bash::BashTool, file_edit::FileEditTool, file_read::FileReadTool, file_write::FileWriteTool,
};
use rust_agent::tool::definition::{PermissionDecision, Tool, ToolCall};

fn trusted_permissions(
    path: &std::path::Path,
    permission: WorkspacePermissionLevel,
) -> std::sync::Arc<WorkspacePermissionConfig> {
    let mut config = WorkspacePermissionConfig::default();
    config.trust_workspace(path, permission);
    std::sync::Arc::new(config)
}

// ── CapabilityTier ordering ───────────────────────────────────────────────────

#[test]
fn r0_1_tier_ordering_read_lt_write_lt_admin() {
    assert!(CapabilityTier::Read < CapabilityTier::Write);
    assert!(CapabilityTier::Write < CapabilityTier::AdminBash);
    assert!(CapabilityTier::Read < CapabilityTier::AdminBash);
}

#[test]
fn r0_1_tier_as_str_values() {
    assert_eq!(CapabilityTier::Read.as_str(), "read");
    assert_eq!(CapabilityTier::Write.as_str(), "write");
    assert_eq!(CapabilityTier::AdminBash.as_str(), "admin_bash");
}

// ── WorkspaceCapabilityConfig defaults ───────────────────────────────────────

#[test]
fn r0_1_default_config_global_tier_is_write() {
    let config = WorkspaceCapabilityConfig::default();
    assert_eq!(config.global_max_tier, CapabilityTier::Write);
    assert!(config.escalate_to_pending_approval);
    assert!(!config.audit_capability_decisions);
}

#[test]
fn r0_1_beta_deny_by_default_tier_is_read() {
    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    assert_eq!(config.global_max_tier, CapabilityTier::Read);
    assert!(config.escalate_to_pending_approval);
    assert!(config.audit_capability_decisions);
}

// ── effective_max_tier ────────────────────────────────────────────────────────

#[test]
fn r0_1_effective_tier_falls_back_to_global_when_no_scopes() {
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::Write,
        scopes: vec![],
        escalate_to_pending_approval: true,
        audit_capability_decisions: false,
    };
    assert_eq!(
        config.effective_max_tier(Path::new("/any/path")),
        CapabilityTier::Write
    );
}

#[test]
fn r0_1_effective_tier_scope_overrides_global() {
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::Write,
        scopes: vec![WorkspaceCapabilityScope {
            directory: "/project/scripts".into(),
            max_tier: CapabilityTier::AdminBash,
        }],
        escalate_to_pending_approval: true,
        audit_capability_decisions: false,
    };
    assert_eq!(
        config.effective_max_tier(Path::new("/project/scripts")),
        CapabilityTier::AdminBash
    );
    assert_eq!(
        config.effective_max_tier(Path::new("/project/scripts/deploy")),
        CapabilityTier::AdminBash
    );
    assert_eq!(
        config.effective_max_tier(Path::new("/project/src")),
        CapabilityTier::Write
    );
}

#[test]
fn r0_1_effective_tier_longest_prefix_wins() {
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::AdminBash,
        scopes: vec![
            WorkspaceCapabilityScope {
                directory: "/project".into(),
                max_tier: CapabilityTier::Write,
            },
            WorkspaceCapabilityScope {
                directory: "/project/readonly".into(),
                max_tier: CapabilityTier::Read,
            },
        ],
        escalate_to_pending_approval: true,
        audit_capability_decisions: false,
    };
    // Longer prefix wins
    assert_eq!(
        config.effective_max_tier(Path::new("/project/readonly/data")),
        CapabilityTier::Read
    );
    // Shorter prefix applies when longer doesn't match
    assert_eq!(
        config.effective_max_tier(Path::new("/project/src")),
        CapabilityTier::Write
    );
    // No scope matches — global
    assert_eq!(
        config.effective_max_tier(Path::new("/other")),
        CapabilityTier::AdminBash
    );
}

// ── check_bash_capability ─────────────────────────────────────────────────────

#[test]
fn r0_1_check_allowed_when_required_tier_lte_allowed() {
    let config = WorkspaceCapabilityConfig::default(); // Write
    let req = CommandCapabilityRequirement::read();
    let outcome = check_bash_capability(&req, &config, Path::new("/project"));
    assert!(outcome.is_allowed());
    assert_eq!(outcome.as_str(), "allowed");
}

#[test]
fn r0_1_check_allowed_when_required_equals_allowed() {
    let config = WorkspaceCapabilityConfig::default(); // Write
    let req = CommandCapabilityRequirement::write();
    let outcome = check_bash_capability(&req, &config, Path::new("/project"));
    assert!(outcome.is_allowed());
}

#[test]
fn r0_1_check_requires_approval_when_tier_exceeded_and_escalation_on() {
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::Write,
        scopes: vec![],
        escalate_to_pending_approval: true,
        audit_capability_decisions: false,
    };
    let req =
        CommandCapabilityRequirement::admin_bash(CapabilityRequirementReason::DestructivePattern);
    let outcome = check_bash_capability(&req, &config, Path::new("/project"));
    assert!(outcome.requires_approval());
    assert_eq!(outcome.as_str(), "requires_approval");
    if let CapabilityCheckOutcome::RequiresApproval {
        required_tier,
        allowed_tier,
        reason,
    } = outcome
    {
        assert_eq!(required_tier, CapabilityTier::AdminBash);
        assert_eq!(allowed_tier, CapabilityTier::Write);
        assert_eq!(reason, CapabilityRequirementReason::DestructivePattern);
    }
}

#[test]
fn r0_1_check_denied_when_tier_exceeded_and_escalation_off() {
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::Write,
        scopes: vec![],
        escalate_to_pending_approval: false,
        audit_capability_decisions: false,
    };
    let req = CommandCapabilityRequirement::admin_bash(CapabilityRequirementReason::ShellOperator);
    let outcome = check_bash_capability(&req, &config, Path::new("/project"));
    assert!(outcome.is_denied());
    assert_eq!(outcome.as_str(), "denied");
}

#[test]
fn r0_1_check_beta_preset_denies_write_with_approval() {
    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    let req = CommandCapabilityRequirement::write();
    let outcome = check_bash_capability(&req, &config, Path::new("/project"));
    assert!(outcome.requires_approval());
}

#[test]
fn r0_1_check_beta_preset_allows_read() {
    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    let req = CommandCapabilityRequirement::read();
    let outcome = check_bash_capability(&req, &config, Path::new("/project"));
    assert!(outcome.is_allowed());
}

// ── CapabilityCheckOutcome::render_line ───────────────────────────────────────

#[test]
fn r0_1_outcome_render_line_allowed() {
    let line = CapabilityCheckOutcome::Allowed.render_line();
    assert!(line.contains("allowed"), "line: {line}");
}

#[test]
fn r0_1_outcome_render_line_requires_approval_contains_tiers() {
    let outcome = CapabilityCheckOutcome::RequiresApproval {
        required_tier: CapabilityTier::AdminBash,
        allowed_tier: CapabilityTier::Write,
        reason: CapabilityRequirementReason::DestructivePattern,
    };
    let line = outcome.render_line();
    assert!(line.contains("admin_bash"), "line: {line}");
    assert!(line.contains("write"), "line: {line}");
    assert!(line.contains("destructive_pattern"), "line: {line}");
}

#[test]
fn r0_1_outcome_render_line_denied_contains_tiers() {
    let outcome = CapabilityCheckOutcome::Denied {
        required_tier: CapabilityTier::AdminBash,
        allowed_tier: CapabilityTier::Read,
        reason: CapabilityRequirementReason::ShellOperator,
    };
    let line = outcome.render_line();
    assert!(line.contains("denied"), "line: {line}");
    assert!(line.contains("admin_bash"), "line: {line}");
    assert!(line.contains("read"), "line: {line}");
}

// ── requirement_from_policy ───────────────────────────────────────────────────

#[test]
fn r0_1_requirement_from_policy_read_only_command() {
    let policy = evaluate_bash_policy_with_context("ls -la", std::path::Path::new("/tmp"), None);
    let req = requirement_from_policy(&policy);
    assert_eq!(req.required_tier, CapabilityTier::Read);
}

#[test]
fn r0_1_requirement_from_policy_destructive_is_admin_bash() {
    let policy =
        evaluate_bash_policy_with_context("rm -rf /tmp/test", std::path::Path::new("/tmp"), None);
    let req = requirement_from_policy(&policy);
    assert_eq!(req.required_tier, CapabilityTier::AdminBash);
    assert_eq!(req.reason, CapabilityRequirementReason::DestructivePattern);
}

// ── serde round-trip ──────────────────────────────────────────────────────────

#[test]
fn r0_1_config_serde_round_trip() {
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::Read,
        scopes: vec![WorkspaceCapabilityScope {
            directory: "/project/scripts".into(),
            max_tier: CapabilityTier::AdminBash,
        }],
        escalate_to_pending_approval: true,
        audit_capability_decisions: true,
    };
    let json = serde_json::to_string(&config).unwrap();
    let restored: WorkspaceCapabilityConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.global_max_tier, CapabilityTier::Read);
    assert_eq!(restored.scopes.len(), 1);
    assert_eq!(restored.scopes[0].max_tier, CapabilityTier::AdminBash);
    assert!(restored.audit_capability_decisions);
}

#[test]
fn r0_1_config_load_from_json() {
    let json = r#"{
        "global_max_tier": "write",
        "scopes": [],
        "escalate_to_pending_approval": false,
        "audit_capability_decisions": false
    }"#;
    let config = WorkspaceCapabilityConfig::load_from_json(json).unwrap();
    assert_eq!(config.global_max_tier, CapabilityTier::Write);
    assert!(!config.escalate_to_pending_approval);
}

#[test]
fn workspace_permissions_parse_and_longest_prefix_wins() {
    let config = WorkspacePermissionConfig {
        version: 1,
        workspaces: vec![
            WorkspacePermissionEntry {
                path: "/project".into(),
                permission: WorkspacePermissionLevel::Worker,
                trusted_at: "2026-05-19T00:00:00Z".into(),
            },
            WorkspacePermissionEntry {
                path: "/project/readonly".into(),
                permission: WorkspacePermissionLevel::View,
                trusted_at: "2026-05-19T00:00:00Z".into(),
            },
        ],
    };

    assert_eq!(
        config
            .effective_permission(Path::new("/project/src/lib.rs"))
            .unwrap()
            .permission,
        WorkspacePermissionLevel::Worker
    );
    assert_eq!(
        config
            .effective_permission(Path::new("/project/readonly/data.txt"))
            .unwrap()
            .permission,
        WorkspacePermissionLevel::View
    );
    assert!(config.effective_permission(Path::new("/other")).is_none());
}

#[test]
fn workspace_permission_check_unmatched_is_approval() {
    let config = WorkspacePermissionConfig::default();
    let outcome = config.check_path(
        Path::new("/untrusted/file.txt"),
        WorkspacePermissionLevel::View,
    );
    assert!(matches!(
        outcome,
        WorkspacePermissionCheck::RequiresApproval {
            reason,
            current: None,
            ..
        } if reason == "workspace_untrusted"
    ));
}

#[tokio::test]
async fn file_tools_respect_view_and_edit_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let read_target = dir.path().join("read.txt");
    let write_target = dir.path().join("write.txt");

    let view_permissions =
        ToolPermissionContext::new(PermissionMode::Default).with_workspace_permissions(
            trusted_permissions(dir.path(), WorkspacePermissionLevel::View),
        );
    let edit_permissions =
        ToolPermissionContext::new(PermissionMode::Default).with_workspace_permissions(
            trusted_permissions(dir.path(), WorkspacePermissionLevel::Edit),
        );

    let read_call = ToolCall::new(
        "Read",
        serde_json::json!({ "file_path": read_target }).to_string(),
    );
    assert!(matches!(
        FileReadTool
            .check_permissions(&read_call, &view_permissions)
            .await,
        PermissionDecision::Allow
    ));

    let write_call = ToolCall::new(
        "Write",
        serde_json::json!({ "file_path": write_target, "content": "x" }).to_string(),
    );
    assert!(matches!(
        FileWriteTool
            .check_permissions(&write_call, &view_permissions)
            .await,
        PermissionDecision::Ask { .. }
    ));
    assert!(matches!(
        FileWriteTool
            .check_permissions(&write_call, &edit_permissions)
            .await,
        PermissionDecision::Allow
    ));

    let edit_call = ToolCall::new(
        "Edit",
        serde_json::json!({
            "file_path": dir.path().join("edit.txt"),
            "old_string": "a",
            "new_string": "b"
        })
        .to_string(),
    );
    assert!(matches!(
        FileEditTool
            .check_permissions(&edit_call, &view_permissions)
            .await,
        PermissionDecision::Ask { .. }
    ));
}

#[tokio::test]
async fn bash_worker_and_admin_permissions_gate_commands() {
    let cwd = std::env::current_dir().unwrap();
    let worker_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_workspace_permissions(trusted_permissions(&cwd, WorkspacePermissionLevel::Worker));
    let admin_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_workspace_permissions(trusted_permissions(&cwd, WorkspacePermissionLevel::Admin));
    let untrusted_permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_workspace_permissions(std::sync::Arc::new(WorkspacePermissionConfig::default()));

    let read_call = ToolCall::new(
        "Bash",
        serde_json::json!({ "command": "git status" }).to_string(),
    );
    assert!(matches!(
        BashTool
            .check_permissions(&read_call, &worker_permissions)
            .await,
        PermissionDecision::Allow
    ));
    assert!(matches!(
        BashTool
            .check_permissions(&read_call, &untrusted_permissions)
            .await,
        PermissionDecision::Ask { .. }
    ));

    let pipe_call = ToolCall::new(
        "Bash",
        serde_json::json!({ "command": "printf hi | cat" }).to_string(),
    );
    assert!(matches!(
        BashTool
            .check_permissions(&pipe_call, &worker_permissions)
            .await,
        PermissionDecision::Ask { .. }
    ));
    assert!(matches!(
        BashTool
            .check_permissions(&pipe_call, &admin_permissions)
            .await,
        PermissionDecision::Allow
    ));
}

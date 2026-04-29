use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::security::workspace_capability::{
    CapabilityTier, WorkspaceCapabilityConfig, WorkspaceCapabilityScope,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &PathBuf, filename: &str, json: &str) -> PathBuf {
    let path = dir.join(filename);
    std::fs::write(&path, json).unwrap();
    path
}

// ── WorkspaceCapabilityConfig::load_from_json ─────────────────────────────────

#[test]
fn r0_4_load_from_json_default_tier_write() {
    let json = r#"{"global_max_tier":"write","scopes":[],"escalate_to_pending_approval":true,"audit_capability_decisions":false}"#;
    let config = WorkspaceCapabilityConfig::load_from_json(json).unwrap();
    assert_eq!(config.global_max_tier, CapabilityTier::Write);
    assert!(config.escalate_to_pending_approval);
}

#[test]
fn r0_4_load_from_json_beta_deny_by_default_tier_read() {
    let json = r#"{"global_max_tier":"read","scopes":[],"escalate_to_pending_approval":true,"audit_capability_decisions":true}"#;
    let config = WorkspaceCapabilityConfig::load_from_json(json).unwrap();
    assert_eq!(config.global_max_tier, CapabilityTier::Read);
    assert!(config.audit_capability_decisions);
}

#[test]
fn r0_4_load_from_json_with_scopes() {
    let json = r#"{
        "global_max_tier": "write",
        "scopes": [
            {"directory": "/project/scripts", "max_tier": "admin_bash"},
            {"directory": "/project/readonly", "max_tier": "read"}
        ],
        "escalate_to_pending_approval": true,
        "audit_capability_decisions": false
    }"#;
    let config = WorkspaceCapabilityConfig::load_from_json(json).unwrap();
    assert_eq!(config.scopes.len(), 2);
    assert_eq!(config.scopes[0].max_tier, CapabilityTier::AdminBash);
    assert_eq!(config.scopes[1].max_tier, CapabilityTier::Read);
}

#[test]
fn r0_4_load_from_json_invalid_returns_error() {
    let result = WorkspaceCapabilityConfig::load_from_json("not json");
    assert!(result.is_err());
}

#[test]
fn r0_4_load_from_json_defaults_for_missing_optional_fields() {
    // Only required field is global_max_tier; scopes and flags have defaults.
    let json = r#"{"global_max_tier":"write"}"#;
    let config = WorkspaceCapabilityConfig::load_from_json(json).unwrap();
    assert_eq!(config.global_max_tier, CapabilityTier::Write);
    assert!(config.scopes.is_empty());
    assert!(config.escalate_to_pending_approval); // default_true
    assert!(!config.audit_capability_decisions);  // default false
}

// ── beta_deny_by_default preset ───────────────────────────────────────────────

#[test]
fn r0_4_beta_deny_by_default_preset_values() {
    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    assert_eq!(config.global_max_tier, CapabilityTier::Read);
    assert!(config.escalate_to_pending_approval);
    assert!(config.audit_capability_decisions);
    assert!(config.scopes.is_empty());
}

// ── file-based loading (simulating bootstrap load_workspace_capability_config) ─

#[test]
fn r0_4_config_file_round_trip() {
    let dir = unique_temp_dir("r0-4-cap-config");
    let config = WorkspaceCapabilityConfig {
        global_max_tier: CapabilityTier::Read,
        scopes: vec![WorkspaceCapabilityScope {
            directory: "/project/scripts".into(),
            max_tier: CapabilityTier::AdminBash,
        }],
        escalate_to_pending_approval: true,
        audit_capability_decisions: true,
    };
    let json = serde_json::to_string_pretty(&config).unwrap();
    let path = write_config(&dir, "workspace-capability.json", &json);

    let loaded_json = std::fs::read_to_string(&path).unwrap();
    let loaded = WorkspaceCapabilityConfig::load_from_json(&loaded_json).unwrap();
    assert_eq!(loaded.global_max_tier, CapabilityTier::Read);
    assert_eq!(loaded.scopes.len(), 1);
    assert_eq!(loaded.scopes[0].max_tier, CapabilityTier::AdminBash);
    assert!(loaded.audit_capability_decisions);

    let _ = std::fs::remove_dir_all(dir);
}

// ── effective_max_tier with loaded config ─────────────────────────────────────

#[test]
fn r0_4_effective_tier_from_loaded_config_with_scopes() {
    let json = r#"{
        "global_max_tier": "write",
        "scopes": [
            {"directory": "/project/scripts", "max_tier": "admin_bash"},
            {"directory": "/project/readonly", "max_tier": "read"}
        ],
        "escalate_to_pending_approval": true,
        "audit_capability_decisions": false
    }"#;
    let config = WorkspaceCapabilityConfig::load_from_json(json).unwrap();

    assert_eq!(
        config.effective_max_tier(std::path::Path::new("/project/scripts/deploy")),
        CapabilityTier::AdminBash
    );
    assert_eq!(
        config.effective_max_tier(std::path::Path::new("/project/readonly/data")),
        CapabilityTier::Read
    );
    assert_eq!(
        config.effective_max_tier(std::path::Path::new("/project/src")),
        CapabilityTier::Write
    );
    assert_eq!(
        config.effective_max_tier(std::path::Path::new("/other")),
        CapabilityTier::Write
    );
}

// ── deny-by-default env flag simulation ──────────────────────────────────────

#[test]
fn r0_4_beta_deny_by_default_config_blocks_write_with_approval() {
    use rust_agent::security::workspace_capability::{
        CommandCapabilityRequirement, check_bash_capability,
    };

    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    let req = CommandCapabilityRequirement::write();
    let outcome = check_bash_capability(&req, &config, std::path::Path::new("/project"));
    assert!(outcome.requires_approval(), "write should require approval in beta preset");
}

#[test]
fn r0_4_beta_deny_by_default_config_allows_read() {
    use rust_agent::security::workspace_capability::{
        CommandCapabilityRequirement, check_bash_capability,
    };

    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    let req = CommandCapabilityRequirement::read();
    let outcome = check_bash_capability(&req, &config, std::path::Path::new("/project"));
    assert!(outcome.is_allowed(), "read should be allowed in beta preset");
}

#[test]
fn r0_4_beta_deny_by_default_config_admin_bash_requires_approval() {
    use rust_agent::security::workspace_capability::{
        CapabilityRequirementReason, CommandCapabilityRequirement, check_bash_capability,
    };

    let config = WorkspaceCapabilityConfig::beta_deny_by_default();
    let req = CommandCapabilityRequirement::admin_bash(CapabilityRequirementReason::DestructivePattern);
    let outcome = check_bash_capability(&req, &config, std::path::Path::new("/project"));
    assert!(outcome.requires_approval(), "admin_bash should require approval in beta preset");
}

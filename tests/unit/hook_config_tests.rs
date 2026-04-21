use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::hook::executor::{HookDecision, run_hook};
use rust_agent::hook::registry::{
    HookConfigSource, HookEvent, HookRegistry, HookRuleLayer, load_hook_registry,
    load_hook_rules_with_diagnostics,
};

fn unique_temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn hook_loader_reads_external_rules_from_project_config() {
    let root = unique_temp_path("rust-agent-hooks-config");
    let claude_dir = root.join(".claude");
    fs::create_dir_all(&claude_dir).expect("create .claude dir");
    fs::write(
        claude_dir.join("hooks.json"),
        r#"[
  {
    "event": "pre_tool_use",
    "deny_match": "Bash",
    "append_message": "external policy active",
    "prevent_continuation": true,
    "permission_decision": "deny",
    "updated_input": "safe-input",
    "additional_context": "loaded from disk"
  }
]"#,
    )
    .expect("write hooks config");

    let registry = load_hook_registry(&root);
    let load_result = registry
        .config_load_result()
        .expect("config load result should be captured");
    assert_eq!(load_result.source, HookConfigSource::File);
    assert!(
        load_result.diagnostics.iter().any(
            |line| line.contains("Loaded 1 hook rule(s) from .claude/hooks.json (layer=file).")
        )
    );
    assert_eq!(registry.rules().len(), 1);
    assert_eq!(registry.rules()[0].layer, HookRuleLayer::File);

    let result = run_hook(
        &registry,
        HookEvent::PreToolUse {
            tool_name: "Bash".into(),
        },
    );
    assert_eq!(
        result.decision,
        HookDecision::Deny("tool Bash denied by hook policy".into())
    );
    assert!(result.prevent_continuation);
    assert_eq!(
        result.payload.additional_context.as_slice(),
        &["loaded from disk"]
    );

    fs::remove_dir_all(root).expect("cleanup hooks config root");
}

#[test]
fn hook_loader_reports_parse_failures_and_uses_empty_defaults() {
    let root = unique_temp_path("rust-agent-hooks-invalid");
    let claude_dir = root.join(".claude");
    fs::create_dir_all(&claude_dir).expect("create .claude dir");
    fs::write(claude_dir.join("hooks.json"), "{not valid json")
        .expect("write invalid hooks config");

    let load_result = load_hook_rules_with_diagnostics(&root);
    assert_eq!(load_result.source, HookConfigSource::Defaults);
    assert!(load_result.rules.is_empty());
    assert_eq!(load_result.path, claude_dir.join("hooks.json"));
    assert!(
        load_result
            .diagnostics
            .iter()
            .any(|line| line.contains("Failed to parse .claude/hooks.json"))
    );

    fs::remove_dir_all(root).expect("cleanup invalid hooks root");
}

#[test]
fn hook_registry_without_external_file_uses_empty_defaults() {
    let root = unique_temp_path("rust-agent-hooks-missing");
    fs::create_dir_all(&root).expect("create root dir");

    let registry = load_hook_registry(&root);
    let load_result = registry
        .config_load_result()
        .expect("config load result should be captured");
    assert_eq!(load_result.source, HookConfigSource::Defaults);
    assert!(registry.rules().is_empty());
    assert!(
        load_result
            .diagnostics
            .iter()
            .any(|line| line.contains("No .claude/hooks.json found"))
    );
    assert_eq!(
        run_hook(&registry, HookEvent::Setup).decision,
        HookDecision::Allow
    );

    fs::remove_dir_all(root).expect("cleanup missing hooks root");
}

#[test]
fn hook_loader_ignores_unknown_events_and_keeps_valid_rules() {
    let root = unique_temp_path("rust-agent-hooks-unknown-event");
    let claude_dir = root.join(".claude");
    fs::create_dir_all(&claude_dir).expect("create .claude dir");
    fs::write(
        claude_dir.join("hooks.json"),
        r#"[
  {
    "event": "not_a_real_event",
    "deny_match": "Setup"
  },
  {
    "event": "pre_tool_use",
    "deny_match": "Read"
  }
]"#,
    )
    .expect("write hooks config with unknown event");

    let load_result = load_hook_rules_with_diagnostics(&root);
    assert_eq!(load_result.source, HookConfigSource::File);
    assert_eq!(load_result.rules.len(), 1);
    assert_eq!(load_result.rules[0].layer, HookRuleLayer::File);
    assert!(
        load_result
            .diagnostics
            .iter()
            .any(|line| line.contains("Ignored hook rule with unknown event 'not_a_real_event'"))
    );
    assert!(
        load_result.diagnostics.iter().any(
            |line| line.contains("Loaded 1 hook rule(s) from .claude/hooks.json (layer=file).")
        )
    );

    let registry = load_hook_registry(&root);
    assert_eq!(
        run_hook(&registry, HookEvent::Setup).decision,
        HookDecision::Allow
    );

    fs::remove_dir_all(root).expect("cleanup unknown event hooks root");
}

#[test]
fn hook_registry_can_still_be_built_programmatically() {
    let registry = HookRegistry::from_rules(Vec::new());
    assert_eq!(
        run_hook(&registry, HookEvent::Stop).decision,
        HookDecision::Allow
    );
    assert!(registry.config_load_result().is_none());
}

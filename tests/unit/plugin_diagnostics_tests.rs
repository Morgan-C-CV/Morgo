use std::path::PathBuf;

use rust_agent::plugins::diagnostics::{
    CapabilityBlockReason, PluginCapabilityStatus, PluginDiagnosticReport, build_capability_record,
    build_diagnostic_report,
};
use rust_agent::plugins::types::{
    PluginActivationSummary, PluginApplyStatus, PluginCapability, PluginConfigSource,
    PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity, PluginGovernanceSource,
    PluginGovernanceState, PluginLifecycleState, PluginLoadResult,
};
use rust_agent::skills::types::{
    SkillDefinition, SkillExecutionContext, SkillSource, SkillWorkflowExecution,
};
use rust_agent::skills::visibility::resolve_skill_visibility;

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_plugin(
    name: &str,
    lifecycle: PluginLifecycleState,
    governance_enabled: bool,
    disable_reason: Option<&str>,
    capabilities: Vec<PluginCapability>,
    activation: PluginActivationSummary,
) -> PluginDefinition {
    PluginDefinition {
        name: name.to_string(),
        version: None,
        description: format!("{name} plugin"),
        manifest_path: PathBuf::from(format!("/plugins/{name}/plugin.toml")),
        capabilities,
        runtime: None,
        diagnostics_metadata: None,
        commands: vec![],
        tools: vec![],
        hooks: vec![],
        governance: PluginGovernanceState {
            enabled: governance_enabled,
            disable_reason: disable_reason.map(|s| s.to_string()),
            source: PluginGovernanceSource::Default,
        },
        lifecycle_state: lifecycle,
        apply_status: PluginApplyStatus::Applied,
        activation,
    }
}

fn active_plugin(name: &str, commands: usize, tools: usize, hooks: usize) -> PluginDefinition {
    make_plugin(
        name,
        PluginLifecycleState::Enabled,
        true,
        None,
        vec![
            PluginCapability::Commands,
            PluginCapability::Tools,
            PluginCapability::Hooks,
        ],
        PluginActivationSummary {
            commands,
            tools,
            hooks,
        },
    )
}

fn disabled_plugin(name: &str, reason: &str) -> PluginDefinition {
    make_plugin(
        name,
        PluginLifecycleState::Disabled,
        false,
        Some(reason),
        vec![PluginCapability::Commands, PluginCapability::Tools],
        PluginActivationSummary::default(),
    )
}

fn error_plugin(name: &str) -> PluginDefinition {
    make_plugin(
        name,
        PluginLifecycleState::Error,
        true,
        None,
        vec![PluginCapability::Tools],
        PluginActivationSummary::default(),
    )
}

fn make_load_result(plugins: Vec<PluginDefinition>) -> PluginLoadResult {
    PluginLoadResult {
        root: PathBuf::from("/plugins"),
        source: PluginConfigSource::Directory,
        plugins,
        diagnostics: vec![],
        orphaned_governance_entries: vec![],
    }
}

fn make_skill(name: &str, source: SkillSource, hidden: bool) -> SkillDefinition {
    SkillDefinition {
        name: name.to_string(),
        description: format!("{name} skill"),
        when_to_use: None,
        argument_hint: None,
        workflow_hint: None,
        workflow_summary: None,
        allowed_tools: vec![],
        aliases: vec![],
        workflow_execution: SkillWorkflowExecution::PromptOnly,
        user_invocable: true,
        disable_model_invocation: false,
        hidden,
        paths: vec![],
        exclude_paths: vec![],
        requires_files: vec![],
        context: SkillExecutionContext::Inline,
        content: String::new(),
        source,
        file_path: None,
    }
}

// ── CapabilityBlockReason ─────────────────────────────────────────────────────

#[test]
fn r4_4_block_reason_as_str_values() {
    assert_eq!(
        CapabilityBlockReason::GovernanceDisabled { reason: None }.as_str(),
        "governance_disabled"
    );
    assert_eq!(
        CapabilityBlockReason::LifecycleError.as_str(),
        "lifecycle_error"
    );
    assert_eq!(
        CapabilityBlockReason::CapabilityNotDeclared.as_str(),
        "capability_not_declared"
    );
    assert_eq!(
        CapabilityBlockReason::NoActiveItems.as_str(),
        "no_active_items"
    );
}

#[test]
fn r4_4_block_reason_render_line_includes_reason_text() {
    let line = CapabilityBlockReason::GovernanceDisabled {
        reason: Some("security policy".to_string()),
    }
    .render_line();
    assert!(
        line.contains("security policy"),
        "render_line should include reason text"
    );
}

// ── build_capability_record ───────────────────────────────────────────────────

#[test]
fn r4_4_active_plugin_all_capabilities_active() {
    let plugin = active_plugin("deploy", 2, 3, 1);
    let record = build_capability_record(&plugin);

    assert_eq!(record.plugin_name, "deploy");
    assert!(record.commands.is_active());
    assert!(record.tools.is_active());
    assert!(record.hooks.is_active());
    assert!(record.any_active());

    if let PluginCapabilityStatus::Active { item_count } = record.commands {
        assert_eq!(item_count, 2);
    } else {
        panic!("expected Active for commands");
    }
}

#[test]
fn r4_4_governance_disabled_blocks_all_capabilities() {
    let plugin = disabled_plugin("audit", "admin disabled");
    let record = build_capability_record(&plugin);

    assert!(!record.commands.is_active());
    assert!(!record.tools.is_active());
    assert!(!record.any_active());

    if let PluginCapabilityStatus::Blocked(CapabilityBlockReason::GovernanceDisabled { reason }) =
        &record.commands
    {
        assert_eq!(reason.as_deref(), Some("admin disabled"));
    } else {
        panic!("expected GovernanceDisabled block reason");
    }
}

#[test]
fn r4_4_lifecycle_error_blocks_all_capabilities() {
    let plugin = error_plugin("broken");
    let record = build_capability_record(&plugin);

    assert!(!record.tools.is_active());
    assert!(matches!(
        record.tools,
        PluginCapabilityStatus::Blocked(CapabilityBlockReason::LifecycleError)
    ));
}

#[test]
fn r4_4_undeclared_capability_is_blocked_not_declared() {
    // Plugin only declares Tools, not Commands
    let plugin = make_plugin(
        "tool-only",
        PluginLifecycleState::Enabled,
        true,
        None,
        vec![PluginCapability::Tools],
        PluginActivationSummary {
            commands: 0,
            tools: 2,
            hooks: 0,
        },
    );
    let record = build_capability_record(&plugin);

    assert!(matches!(
        record.commands,
        PluginCapabilityStatus::Blocked(CapabilityBlockReason::CapabilityNotDeclared)
    ));
    assert!(record.tools.is_active());
}

#[test]
fn r4_4_declared_but_zero_active_items_is_no_active_items() {
    // Plugin declares Commands but activation.commands == 0
    let plugin = make_plugin(
        "empty-cmds",
        PluginLifecycleState::Enabled,
        true,
        None,
        vec![PluginCapability::Commands],
        PluginActivationSummary {
            commands: 0,
            tools: 0,
            hooks: 0,
        },
    );
    let record = build_capability_record(&plugin);

    assert!(matches!(
        record.commands,
        PluginCapabilityStatus::Blocked(CapabilityBlockReason::NoActiveItems)
    ));
}

// ── render_summary_line ───────────────────────────────────────────────────────

#[test]
fn r4_4_render_summary_line_active_plugin() {
    let plugin = active_plugin("ci", 1, 2, 0);
    let record = build_capability_record(&plugin);
    let line = record.render_summary_line();

    assert!(line.contains("ci"), "should contain plugin name");
    assert!(line.contains("active(1)"), "commands active(1)");
    assert!(line.contains("active(2)"), "tools active(2)");
}

#[test]
fn r4_4_render_summary_line_blocked_plugin() {
    let plugin = disabled_plugin("legacy", "deprecated");
    let record = build_capability_record(&plugin);
    let line = record.render_summary_line();

    assert!(line.contains("blocked(governance_disabled)"));
}

// ── build_diagnostic_report ───────────────────────────────────────────────────

#[test]
fn r4_4_report_counts_active_and_blocked_plugins() {
    let load_result = make_load_result(vec![
        active_plugin("a", 1, 0, 0),
        disabled_plugin("b", "off"),
        error_plugin("c"),
    ]);
    let report = build_diagnostic_report(&load_result, None);

    // "a" has commands active → any_active = true
    // "b" governance disabled → any_active = false
    // "c" lifecycle error → any_active = false
    assert_eq!(report.active_plugin_count(), 1);
    assert_eq!(report.blocked_plugin_count(), 2);
}

#[test]
fn r4_4_report_error_and_warning_diagnostic_counts() {
    let mut load_result = make_load_result(vec![]);
    load_result.diagnostics = vec![
        PluginDiagnostic {
            plugin_name: Some("x".into()),
            manifest_path: None,
            severity: PluginDiagnosticSeverity::Error,
            code: "e1".into(),
            message: "error one".into(),
        },
        PluginDiagnostic {
            plugin_name: Some("x".into()),
            manifest_path: None,
            severity: PluginDiagnosticSeverity::Warning,
            code: "w1".into(),
            message: "warning one".into(),
        },
        PluginDiagnostic {
            plugin_name: Some("x".into()),
            manifest_path: None,
            severity: PluginDiagnosticSeverity::Warning,
            code: "w2".into(),
            message: "warning two".into(),
        },
    ];
    let report = build_diagnostic_report(&load_result, None);

    assert_eq!(report.error_diagnostic_count(), 1);
    assert_eq!(report.warning_diagnostic_count(), 2);
}

#[test]
fn r4_4_report_has_issues_true_when_errors_present() {
    let mut load_result = make_load_result(vec![]);
    load_result.diagnostics = vec![PluginDiagnostic {
        plugin_name: None,
        manifest_path: None,
        severity: PluginDiagnosticSeverity::Error,
        code: "e".into(),
        message: "bad".into(),
    }];
    let report = build_diagnostic_report(&load_result, None);
    assert!(report.has_issues());
}

#[test]
fn r4_4_report_has_issues_false_when_all_clean() {
    let load_result = make_load_result(vec![active_plugin("ok", 1, 1, 1)]);
    let report = build_diagnostic_report(&load_result, None);
    assert!(!report.has_issues());
}

// ── skill visibility integration ─────────────────────────────────────────────

#[test]
fn r4_4_report_captures_skill_conflicts_from_visibility() {
    use std::path::Path;

    let cwd = Path::new("/project");
    let bundled = make_skill("deploy", SkillSource::Bundled, false);
    let user = make_skill("deploy", SkillSource::User, false);
    let visibility = resolve_skill_visibility(vec![bundled, user], cwd);

    let load_result = make_load_result(vec![]);
    let report = build_diagnostic_report(&load_result, Some(&visibility));

    assert_eq!(report.skill_conflicts.len(), 1);
    let conflict = &report.skill_conflicts[0];
    assert_eq!(conflict.skill_name, "deploy");
    assert_eq!(conflict.winner_source, "user");
    assert!(conflict.shadowed_sources.contains(&"bundled".to_string()));
}

#[test]
fn r4_4_report_captures_disabled_skills_from_visibility() {
    use std::path::Path;

    let cwd = Path::new("/project");
    let hidden = make_skill("secret", SkillSource::Bundled, true);
    let active = make_skill("public", SkillSource::Bundled, false);
    let visibility = resolve_skill_visibility(vec![hidden, active], cwd);

    let load_result = make_load_result(vec![]);
    let report = build_diagnostic_report(&load_result, Some(&visibility));

    assert!(report.disabled_skill_names.contains(&"secret".to_string()));
    assert!(!report.disabled_skill_names.contains(&"public".to_string()));
}

#[test]
fn r4_4_report_no_skill_data_when_visibility_none() {
    let load_result = make_load_result(vec![]);
    let report = build_diagnostic_report(&load_result, None);

    assert!(report.skill_conflicts.is_empty());
    assert!(report.disabled_skill_names.is_empty());
}

// ── render_summary / render_lines ─────────────────────────────────────────────

#[test]
fn r4_4_render_summary_contains_key_counts() {
    let load_result = make_load_result(vec![
        active_plugin("a", 1, 0, 0),
        disabled_plugin("b", "off"),
    ]);
    let report = build_diagnostic_report(&load_result, None);
    let summary = report.render_summary();

    assert!(summary.contains("active=1"), "summary: {summary}");
    assert!(summary.contains("blocked=1"), "summary: {summary}");
    assert!(summary.contains("errors=0"), "summary: {summary}");
}

#[test]
fn r4_4_render_lines_includes_per_plugin_lines() {
    let load_result = make_load_result(vec![active_plugin("ci", 2, 1, 0)]);
    let report = build_diagnostic_report(&load_result, None);
    let lines = report.render_lines();

    assert!(
        lines.iter().any(|l| l.contains("ci")),
        "should include plugin line"
    );
}

#[test]
fn r4_4_render_lines_includes_conflict_lines() {
    use std::path::Path;

    let cwd = Path::new("/project");
    let bundled = make_skill("build", SkillSource::Bundled, false);
    let user = make_skill("build", SkillSource::User, false);
    let visibility = resolve_skill_visibility(vec![bundled, user], cwd);

    let load_result = make_load_result(vec![]);
    let report = build_diagnostic_report(&load_result, Some(&visibility));
    let lines = report.render_lines();

    assert!(
        lines.iter().any(|l| l.starts_with("conflict:")),
        "should include conflict line"
    );
}

#[test]
fn r4_4_render_lines_includes_disabled_skill_lines() {
    use std::path::Path;

    let cwd = Path::new("/project");
    let hidden = make_skill("internal", SkillSource::Bundled, true);
    let visibility = resolve_skill_visibility(vec![hidden], cwd);

    let load_result = make_load_result(vec![]);
    let report = build_diagnostic_report(&load_result, Some(&visibility));
    let lines = report.render_lines();

    assert!(
        lines.iter().any(|l| l.starts_with("disabled_skill:")),
        "should include disabled_skill line"
    );
}

#[test]
fn r4_4_render_lines_includes_orphaned_governance_lines() {
    let mut load_result = make_load_result(vec![]);
    load_result.orphaned_governance_entries = vec!["old-plugin".to_string()];
    let report = build_diagnostic_report(&load_result, None);
    let lines = report.render_lines();

    assert!(
        lines.iter().any(|l| l.starts_with("orphaned_governance:")),
        "should include orphaned_governance line"
    );
}

// ── SkillConflictSummary::render_line ─────────────────────────────────────────

#[test]
fn r4_4_skill_conflict_summary_render_line() {
    use rust_agent::plugins::diagnostics::SkillConflictSummary;

    let summary = SkillConflictSummary {
        skill_name: "deploy".to_string(),
        winner_source: "project".to_string(),
        shadowed_sources: vec!["bundled".to_string(), "user".to_string()],
    };
    let line = summary.render_line();
    assert!(line.contains("deploy"));
    assert!(line.contains("winner=project"));
    assert!(line.contains("bundled"));
    assert!(line.contains("user"));
}

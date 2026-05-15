use rust_agent::core::boss_test_readiness::{
    BossAdmissionDenyReason, BossRollbackPolicy, BossRollbackTrigger, BossTestAdmissionPolicy,
    BossTestAllowlist, BossTestReadinessGate, BossTestRunOutcome, BossTestSampleRecord,
    evaluate_rollback_triggers,
};

// ── BossTestAllowlist ─────────────────────────────────────────────────────────

#[test]
fn ptr_empty_allowlist_permits_everything() {
    let allowlist = BossTestAllowlist::default();
    assert!(allowlist.allows_provider("any-profile"));
    assert!(allowlist.allows_skill("any-skill"));
    assert!(allowlist.allows_mcp_server("any-server"));
}

#[test]
fn ptr_populated_allowlist_permits_listed_items() {
    let mut allowlist = BossTestAllowlist::default();
    allowlist
        .provider_profiles
        .insert("claude-sonnet".to_string());
    allowlist.skill_names.insert("deploy".to_string());
    allowlist.mcp_server_names.insert("github".to_string());

    assert!(allowlist.allows_provider("claude-sonnet"));
    assert!(!allowlist.allows_provider("gpt-4"));
    assert!(allowlist.allows_skill("deploy"));
    assert!(!allowlist.allows_skill("review"));
    assert!(allowlist.allows_mcp_server("github"));
    assert!(!allowlist.allows_mcp_server("jira"));
}

// ── BossTestReadinessGate::check ──────────────────────────────────────────────

#[test]
fn ptr_gate_admits_when_all_allowlists_empty() {
    let policy = BossTestAdmissionPolicy::default();
    let gate = BossTestReadinessGate::new(&policy);

    let result = gate.check(Some("any-profile"), &["skill-a"], &["mcp-x"]);
    assert!(result.is_admitted());
}

#[test]
fn ptr_gate_admits_when_all_items_in_allowlist() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy
        .allowlist
        .provider_profiles
        .insert("claude-sonnet".to_string());
    policy.allowlist.skill_names.insert("deploy".to_string());
    policy
        .allowlist
        .mcp_server_names
        .insert("github".to_string());

    let gate = BossTestReadinessGate::new(&policy);
    let result = gate.check(Some("claude-sonnet"), &["deploy"], &["github"]);
    assert!(result.is_admitted());
}

#[test]
fn ptr_gate_denies_unlisted_provider() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy
        .allowlist
        .provider_profiles
        .insert("claude-sonnet".to_string());

    let gate = BossTestReadinessGate::new(&policy);
    let result = gate.check(Some("gpt-4"), &[], &[]);

    assert!(!result.is_admitted());
    let reasons = result.deny_reasons();
    assert_eq!(reasons.len(), 1);
    assert!(matches!(
        &reasons[0],
        BossAdmissionDenyReason::ProviderNotAllowlisted { profile_id }
        if profile_id == "gpt-4"
    ));
}

#[test]
fn ptr_gate_denies_unlisted_skill() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy.allowlist.skill_names.insert("deploy".to_string());

    let gate = BossTestReadinessGate::new(&policy);
    let result = gate.check(None, &["review"], &[]);

    assert!(!result.is_admitted());
    assert!(matches!(
        &result.deny_reasons()[0],
        BossAdmissionDenyReason::SkillNotAllowlisted { skill_name }
        if skill_name == "review"
    ));
}

#[test]
fn ptr_gate_denies_unlisted_mcp_server() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy
        .allowlist
        .mcp_server_names
        .insert("github".to_string());

    let gate = BossTestReadinessGate::new(&policy);
    let result = gate.check(None, &[], &["jira"]);

    assert!(!result.is_admitted());
    assert!(matches!(
        &result.deny_reasons()[0],
        BossAdmissionDenyReason::McpServerNotAllowlisted { server_name }
        if server_name == "jira"
    ));
}

#[test]
fn ptr_gate_collects_multiple_deny_reasons() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy
        .allowlist
        .provider_profiles
        .insert("claude-sonnet".to_string());
    policy.allowlist.skill_names.insert("deploy".to_string());

    let gate = BossTestReadinessGate::new(&policy);
    let result = gate.check(Some("gpt-4"), &["review", "test"], &[]);

    assert!(!result.is_admitted());
    // 1 provider + 2 skills = 3 deny reasons
    assert_eq!(result.deny_reasons().len(), 3);
}

#[test]
fn ptr_gate_no_provider_check_when_none() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy
        .allowlist
        .provider_profiles
        .insert("claude-sonnet".to_string());

    let gate = BossTestReadinessGate::new(&policy);
    // provider = None means "no provider specified" — skip provider check
    let result = gate.check(None, &[], &[]);
    assert!(result.is_admitted());
}

// ── BossAdmissionDenyReason::as_str / render_line ────────────────────────────

#[test]
fn ptr_deny_reason_as_str_values() {
    assert_eq!(
        BossAdmissionDenyReason::ProviderNotAllowlisted {
            profile_id: "x".into()
        }
        .as_str(),
        "provider_not_allowlisted"
    );
    assert_eq!(
        BossAdmissionDenyReason::SkillNotAllowlisted {
            skill_name: "x".into()
        }
        .as_str(),
        "skill_not_allowlisted"
    );
    assert_eq!(
        BossAdmissionDenyReason::McpServerNotAllowlisted {
            server_name: "x".into()
        }
        .as_str(),
        "mcp_server_not_allowlisted"
    );
}

#[test]
fn ptr_deny_reason_render_line_includes_value() {
    let line = BossAdmissionDenyReason::ProviderNotAllowlisted {
        profile_id: "gpt-4".into(),
    }
    .render_line();
    assert!(line.contains("gpt-4"));
}

// ── evaluate_rollback_triggers ────────────────────────────────────────────────

#[test]
fn ptr_no_triggers_when_policy_all_disabled() {
    let policy = BossRollbackPolicy::default();
    let triggers = evaluate_rollback_triggers(&policy, true, 999_999, Some(0.1), true);
    assert!(triggers.is_empty());
}

#[test]
fn ptr_mcp_failure_trigger_when_enabled() {
    let policy = BossRollbackPolicy {
        abort_on_mcp_failure: true,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, true, 0, None, false);
    assert_eq!(triggers.len(), 1);
    assert!(matches!(
        triggers[0],
        BossRollbackTrigger::McpFailureOccurred
    ));
}

#[test]
fn ptr_no_mcp_trigger_when_no_failure() {
    let policy = BossRollbackPolicy {
        abort_on_mcp_failure: true,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, false, 0, None, false);
    assert!(triggers.is_empty());
}

#[test]
fn ptr_cost_limit_trigger_when_exceeded() {
    let policy = BossRollbackPolicy {
        max_cost_micros_usd: 1_000,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, false, 1_500, None, false);
    assert_eq!(triggers.len(), 1);
    assert!(matches!(
        triggers[0],
        BossRollbackTrigger::CostLimitExceeded {
            actual_micros_usd: 1_500,
            limit_micros_usd: 1_000
        }
    ));
}

#[test]
fn ptr_no_cost_trigger_when_within_limit() {
    let policy = BossRollbackPolicy {
        max_cost_micros_usd: 1_000,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, false, 999, None, false);
    assert!(triggers.is_empty());
}

#[test]
fn ptr_cache_ratio_trigger_when_below_threshold() {
    let policy = BossRollbackPolicy {
        min_cache_hit_ratio: 0.5,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, false, 0, Some(0.3), false);
    assert_eq!(triggers.len(), 1);
    assert!(matches!(
        triggers[0],
        BossRollbackTrigger::CacheHitRatioBelowThreshold { .. }
    ));
}

#[test]
fn ptr_no_cache_trigger_when_ratio_meets_threshold() {
    let policy = BossRollbackPolicy {
        min_cache_hit_ratio: 0.5,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, false, 0, Some(0.6), false);
    assert!(triggers.is_empty());
}

#[test]
fn ptr_no_cache_trigger_when_no_ratio_data() {
    let policy = BossRollbackPolicy {
        min_cache_hit_ratio: 0.5,
        ..Default::default()
    };
    // cache_hit_ratio = None means no data yet — don't trigger
    let triggers = evaluate_rollback_triggers(&policy, false, 0, None, false);
    assert!(triggers.is_empty());
}

#[test]
fn ptr_pending_approval_trigger_when_enabled() {
    let policy = BossRollbackPolicy {
        abort_on_pending_approval: true,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, false, 0, None, true);
    assert_eq!(triggers.len(), 1);
    assert!(matches!(
        triggers[0],
        BossRollbackTrigger::PendingApprovalRequired
    ));
}

#[test]
fn ptr_multiple_triggers_collected() {
    let policy = BossRollbackPolicy {
        abort_on_mcp_failure: true,
        max_cost_micros_usd: 100,
        abort_on_pending_approval: true,
        ..Default::default()
    };
    let triggers = evaluate_rollback_triggers(&policy, true, 200, None, true);
    assert_eq!(triggers.len(), 3);
}

// ── BossRollbackTrigger::as_str / render_line ─────────────────────────────────

#[test]
fn ptr_rollback_trigger_as_str_values() {
    assert_eq!(
        BossRollbackTrigger::McpFailureOccurred.as_str(),
        "mcp_failure_occurred"
    );
    assert_eq!(
        BossRollbackTrigger::CostLimitExceeded {
            actual_micros_usd: 0,
            limit_micros_usd: 0
        }
        .as_str(),
        "cost_limit_exceeded"
    );
    assert_eq!(
        BossRollbackTrigger::CacheHitRatioBelowThreshold {
            actual: 0.0,
            threshold: 0.0
        }
        .as_str(),
        "cache_hit_ratio_below_threshold"
    );
    assert_eq!(
        BossRollbackTrigger::PendingApprovalRequired.as_str(),
        "pending_approval_required"
    );
}

#[test]
fn ptr_rollback_trigger_render_line_includes_values() {
    let line = BossRollbackTrigger::CostLimitExceeded {
        actual_micros_usd: 1500,
        limit_micros_usd: 1000,
    }
    .render_line();
    assert!(line.contains("1500"));
    assert!(line.contains("1000"));
}

// ── BossTestSampleRecord::render_summary ─────────────────────────────────────

#[test]
fn ptr_sample_record_render_summary_contains_key_fields() {
    let record = BossTestSampleRecord {
        run_id: "run-001".to_string(),
        provider_profile: Some("claude-sonnet".to_string()),
        skill_names: vec!["deploy".to_string()],
        mcp_server_names: vec!["github".to_string()],
        total_steps: 5,
        completed_steps: 4,
        cost_micros_usd: 250,
        cache_hit_ratio: Some(0.72),
        estimated_tokens_saved: 1200,
        fallback_count: 1,
        fallback_tier: Some("full_context".into()),
        fallback_reason: Some("request_context_exhausted:symbol:MissingSymbol".into()),
        mcp_failure_count: 0,
        pending_approval_count: 1,
        rollback_triggers: vec![],
        outcome: BossTestRunOutcome::Completed,
    };

    let summary = record.render_summary();
    assert!(summary.contains("run-001"), "summary: {summary}");
    assert!(summary.contains("completed"), "summary: {summary}");
    assert!(summary.contains("4/5"), "summary: {summary}");
    assert!(summary.contains("72.0%"), "summary: {summary}");
}

#[test]
fn ptr_sample_record_render_summary_no_cache_ratio() {
    let record = BossTestSampleRecord {
        run_id: "run-002".to_string(),
        provider_profile: None,
        skill_names: vec![],
        mcp_server_names: vec![],
        total_steps: 1,
        completed_steps: 0,
        cost_micros_usd: 0,
        cache_hit_ratio: None,
        estimated_tokens_saved: 0,
        fallback_count: 0,
        fallback_tier: None,
        fallback_reason: None,
        mcp_failure_count: 0,
        pending_approval_count: 0,
        rollback_triggers: vec![],
        outcome: BossTestRunOutcome::Aborted,
    };

    let summary = record.render_summary();
    assert!(summary.contains("aborted"), "summary: {summary}");
    assert!(summary.contains("cache_hit=-"), "summary: {summary}");
}

// ── BossTestRunOutcome::as_str ────────────────────────────────────────────────

#[test]
fn ptr_run_outcome_as_str_values() {
    assert_eq!(BossTestRunOutcome::Completed.as_str(), "completed");
    assert_eq!(BossTestRunOutcome::RolledBack.as_str(), "rolled_back");
    assert_eq!(BossTestRunOutcome::Aborted.as_str(), "aborted");
}

// ── serde round-trip ──────────────────────────────────────────────────────────

#[test]
fn ptr_admission_policy_serde_round_trip() {
    let mut policy = BossTestAdmissionPolicy::default();
    policy
        .allowlist
        .provider_profiles
        .insert("claude-sonnet".to_string());
    policy.rollback.abort_on_mcp_failure = true;
    policy.rollback.max_cost_micros_usd = 5_000;

    let json = serde_json::to_string(&policy).unwrap();
    let restored: BossTestAdmissionPolicy = serde_json::from_str(&json).unwrap();

    assert_eq!(
        restored.allowlist.provider_profiles,
        policy.allowlist.provider_profiles
    );
    assert_eq!(restored.rollback.abort_on_mcp_failure, true);
    assert_eq!(restored.rollback.max_cost_micros_usd, 5_000);
}

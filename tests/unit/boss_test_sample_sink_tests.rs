use std::fs;

use rust_agent::core::boss_state::{
    BossActorHandle, BossActorRole, BossActorStatus, BossLisMPolicy, BossObservabilitySummary,
    BossPlanStepStatus, BossReportPayload, BossStage, BossStepReport, BossStepRoutedMetadata,
};
use rust_agent::core::boss_test_readiness::{BossRollbackPolicy, BossTestRunOutcome};
use rust_agent::core::boss_test_sample_sink::{BossTestSampleSink, new_shared_sink};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_actor(id: &str, role: BossActorRole) -> BossActorHandle {
    BossActorHandle::new(id, id, role)
}

fn make_report(
    total_steps: usize,
    completed_steps: usize,
    cache_read: usize,
    cache_write: usize,
    cost_micros: u64,
    provider_profile: Option<&str>,
) -> BossReportPayload {
    let steps: Vec<BossStepReport> = (0..total_steps)
        .map(|i| BossStepReport {
            id: i,
            status: if i < completed_steps {
                BossPlanStepStatus::Completed
            } else {
                BossPlanStepStatus::Pending
            },
            worker_task_id: None,
            attempt_count: 1,
            last_review_summary: None,
            action_required: None,
            blocker_reason: None,
            routed_metadata: if i == 0 {
                Some(BossStepRoutedMetadata {
                    toolset_id: None,
                    skillset_id: None,
                    model_tier: Some("standard".to_string()),
                    provider_profile_id: provider_profile.map(|s| s.to_string()),
                    state_frame_size: None,
                    cache_read_tokens: Some(cache_read),
                    cache_write_tokens: Some(cache_write),
                    fallback_count: None,
                    fallback_tier: None,
                    fallback_reason: None,
                    projection_mismatch_count: None,
                    hydration_count: None,
                    stale_ref_count: None,
                    hydration_ref_missing: None,
                    tool_dispatch_count: None,
                    tool_dispatch_success_count: None,
                    tool_dispatch_failure_count: None,
                    tool_dispatch_ref_write_count: None,
                    tool_dispatch_failure_taxonomy: Default::default(),
                    input_tokens: None,
                    uncached_input_tokens: None,
                    output_tokens: None,
                    original_prompt_chars: None,
                    sent_prompt_chars: None,
                    estimated_cost_micros_usd: None,
                })
            } else {
                None
            },
        })
        .collect();

    let observability_summary = if cache_read > 0 || cache_write > 0 || cost_micros > 0 {
        Some(BossObservabilitySummary {
            total_steps_routed: total_steps,
            total_cache_read_tokens: cache_read,
            total_cache_write_tokens: cache_write,
            total_fallback_count: 0,
            fallback_tier_counts: Default::default(),
            fallback_reason_counts: Default::default(),
            total_projection_mismatch_count: 0,
            total_hydration_count: 0,
            total_stale_ref_count: 0,
            total_hydration_ref_missing: 0,
            total_tool_dispatch_count: 0,
            total_tool_dispatch_success_count: 0,
            total_tool_dispatch_failure_count: 0,
            total_tool_dispatch_ref_write_count: 0,
            tool_dispatch_failure_taxonomy: Default::default(),
            override_hit_count: 0,
            model_tier_counts: Default::default(),
            total_input_tokens: 0,
            total_uncached_input_tokens: 0,
            total_output_tokens: 0,
            estimated_cost_micros_usd: cost_micros,
            total_original_chars: 0,
            total_sent_chars: 0,
        })
    } else {
        None
    };

    BossReportPayload {
        stage: BossStage::Execution,
        current_step: Some(completed_steps),
        total_steps: Some(total_steps),
        designer_a: make_actor("boss-test-a", BossActorRole::DesignerA),
        executor_b: make_actor("boss-test-b", BossActorRole::ExecutorB),
        active_children: vec![],
        steps,
        history_summary: vec![],
        observability_summary,
        lism_policy: BossLisMPolicy::Inherit,
    }
}

// ── in-memory sink ────────────────────────────────────────────────────────────

#[test]
fn ptr2_in_memory_sink_starts_empty() {
    let sink = BossTestSampleSink::in_memory();
    assert_eq!(sink.record_count(), 0);
    assert!(sink.records().is_empty());
    assert!(sink.path().is_none());
}

#[test]
fn ptr2_record_run_complete_adds_record() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(5, 5, 1000, 200, 500, Some("claude-sonnet"));
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete(
        "run-001",
        &report,
        &policy,
        vec!["deploy".to_string()],
        vec!["github".to_string()],
        0,
        0,
    );

    assert_eq!(sink.record_count(), 1);
    let records = sink.records();
    let r = &records[0];
    assert_eq!(r.run_id, "run-001");
    assert_eq!(r.outcome, BossTestRunOutcome::Completed);
    assert_eq!(r.total_steps, 5);
    assert_eq!(r.completed_steps, 5);
    assert_eq!(r.cost_micros_usd, 500);
    assert_eq!(r.provider_profile.as_deref(), Some("claude-sonnet"));
    assert_eq!(r.skill_names, vec!["deploy"]);
    assert_eq!(r.mcp_server_names, vec!["github"]);
}

#[test]
fn ptr2_record_run_aborted_sets_aborted_outcome() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(5, 2, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_aborted("run-002", &report, &policy, vec![], vec![], 1, 0);

    let records = sink.records();
    assert_eq!(records[0].outcome, BossTestRunOutcome::Aborted);
    assert_eq!(records[0].mcp_failure_count, 1);
}

#[test]
fn ptr2_record_run_rolled_back_sets_rolled_back_outcome() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(3, 1, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_rolled_back("run-003", &report, &policy, vec![], vec![], 0, 1);

    let records = sink.records();
    assert_eq!(records[0].outcome, BossTestRunOutcome::RolledBack);
    assert_eq!(records[0].pending_approval_count, 1);
}

#[test]
fn ptr2_multiple_records_accumulate() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(2, 2, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete("r1", &report, &policy, vec![], vec![], 0, 0);
    sink.record_run_complete("r2", &report, &policy, vec![], vec![], 0, 0);
    sink.record_run_aborted("r3", &report, &policy, vec![], vec![], 0, 0);

    assert_eq!(sink.record_count(), 3);
}

// ── cache hit ratio extraction ────────────────────────────────────────────────

#[test]
fn ptr2_cache_hit_ratio_extracted_from_observability_summary() {
    let sink = BossTestSampleSink::in_memory();
    // cache_read=800, cache_write=200 → ratio = 0.8
    let report = make_report(1, 1, 800, 200, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete("r", &report, &policy, vec![], vec![], 0, 0);

    let r = &sink.records()[0];
    let ratio = r.cache_hit_ratio.expect("should have cache hit ratio");
    assert!((ratio - 0.8).abs() < 1e-9, "expected 0.8, got {ratio}");
}

#[test]
fn ptr2_cache_hit_ratio_none_when_no_observability_data() {
    let sink = BossTestSampleSink::in_memory();
    // no cache data → observability_summary = None
    let report = make_report(1, 1, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete("r", &report, &policy, vec![], vec![], 0, 0);

    assert!(sink.records()[0].cache_hit_ratio.is_none());
}

// ── rollback trigger evaluation ───────────────────────────────────────────────

#[test]
fn ptr2_rollback_triggers_populated_when_policy_fires() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(2, 1, 0, 0, 2_000, None);
    let policy = BossRollbackPolicy {
        max_cost_micros_usd: 1_000,
        abort_on_mcp_failure: true,
        ..Default::default()
    };

    sink.record_run_rolled_back("r", &report, &policy, vec![], vec![], 1, 0);

    let r = &sink.records()[0];
    assert!(
        r.rollback_triggers
            .contains(&"cost_limit_exceeded".to_string())
    );
    assert!(
        r.rollback_triggers
            .contains(&"mcp_failure_occurred".to_string())
    );
}

#[test]
fn ptr2_rollback_triggers_empty_when_policy_not_triggered() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(2, 2, 0, 0, 100, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete("r", &report, &policy, vec![], vec![], 0, 0);

    assert!(sink.records()[0].rollback_triggers.is_empty());
}

// ── completed_steps count ─────────────────────────────────────────────────────

#[test]
fn ptr2_completed_steps_counted_from_step_statuses() {
    let sink = BossTestSampleSink::in_memory();
    // 5 total, 3 completed
    let report = make_report(5, 3, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete("r", &report, &policy, vec![], vec![], 0, 0);

    let r = &sink.records()[0];
    assert_eq!(r.total_steps, 5);
    assert_eq!(r.completed_steps, 3);
}

// ── JSONL persistence ─────────────────────────────────────────────────────────

#[test]
fn ptr2_jsonl_sink_writes_records_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("samples.jsonl");

    {
        let sink = BossTestSampleSink::with_jsonl_path(&path).unwrap();
        let report = make_report(3, 3, 500, 100, 250, Some("claude-sonnet"));
        let policy = BossRollbackPolicy::default();

        sink.record_run_complete(
            "run-jsonl-001",
            &report,
            &policy,
            vec!["deploy".to_string()],
            vec![],
            0,
            0,
        );
        sink.record_run_aborted("run-jsonl-002", &report, &policy, vec![], vec![], 0, 0);
    }

    let content = fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2, "should have 2 JSONL lines");

    // Each line should be valid JSON containing the run_id
    assert!(lines[0].contains("run-jsonl-001"), "line 0: {}", lines[0]);
    assert!(lines[1].contains("run-jsonl-002"), "line 1: {}", lines[1]);

    // Verify round-trip deserialization
    let r0: rust_agent::core::boss_test_readiness::BossTestSampleRecord =
        serde_json::from_str(lines[0]).unwrap();
    assert_eq!(r0.run_id, "run-jsonl-001");
    assert_eq!(r0.outcome, BossTestRunOutcome::Completed);
}

#[test]
fn ptr2_jsonl_sink_appends_across_instances() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("append.jsonl");

    let report = make_report(1, 1, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    // First instance writes one record
    {
        let sink = BossTestSampleSink::with_jsonl_path(&path).unwrap();
        sink.record_run_complete("r1", &report, &policy, vec![], vec![], 0, 0);
    }

    // Second instance appends another
    {
        let sink = BossTestSampleSink::with_jsonl_path(&path).unwrap();
        sink.record_run_complete("r2", &report, &policy, vec![], vec![], 0, 0);
    }

    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(
        content.lines().count(),
        2,
        "should have 2 lines after two instances"
    );
}

#[test]
fn ptr2_jsonl_sink_path_reported_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("check.jsonl");

    let sink = BossTestSampleSink::with_jsonl_path(&path).unwrap();
    assert_eq!(sink.path().unwrap(), path);
}

// ── shared sink ───────────────────────────────────────────────────────────────

#[test]
fn ptr2_shared_sink_accessible_from_multiple_owners() {
    let sink = new_shared_sink();
    let sink2 = sink.clone();

    let report = make_report(1, 1, 0, 0, 0, None);
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete("r1", &report, &policy, vec![], vec![], 0, 0);
    sink2.record_run_complete("r2", &report, &policy, vec![], vec![], 0, 0);

    assert_eq!(sink.record_count(), 2);
}

// ── render_summary integration ────────────────────────────────────────────────

#[test]
fn ptr2_sample_record_render_summary_from_real_report() {
    let sink = BossTestSampleSink::in_memory();
    let report = make_report(4, 4, 600, 400, 1200, Some("claude-sonnet"));
    let policy = BossRollbackPolicy::default();

    sink.record_run_complete(
        "run-render",
        &report,
        &policy,
        vec!["deploy".to_string(), "review".to_string()],
        vec!["github".to_string()],
        0,
        0,
    );

    let summary = sink.records()[0].render_summary();
    assert!(summary.contains("run-render"), "summary: {summary}");
    assert!(summary.contains("completed"), "summary: {summary}");
    assert!(summary.contains("4/4"), "summary: {summary}");
    // cache_read=600, cache_write=400 → ratio=0.6 → 60.0%
    assert!(summary.contains("60.0%"), "summary: {summary}");
}

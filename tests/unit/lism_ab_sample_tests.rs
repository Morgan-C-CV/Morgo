use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::core::boss_state::{
    BossActorHandle, BossActorRole, BossLisMPolicy, BossObservabilitySummary, BossPlanStepStatus,
    BossReportPayload, BossStage, BossStepReport,
};
use rust_agent::core::boss_test_readiness::BossTestRunOutcome;
use rust_agent::core::lism_ab_sample::{
    LisMAbSampleSink, LisMPolicyRecommendation, LisMRolloutConclusion, new_shared_ab_sink,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.jsonl"))
}

fn make_report(
    total_steps: usize,
    completed_steps: usize,
    cache_read: usize,
    cache_write: usize,
    cost_micros: u64,
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
            routed_metadata: None,
        })
        .collect();

    let observability_summary = if cache_read > 0 || cache_write > 0 || cost_micros > 0 {
        Some(BossObservabilitySummary {
            total_steps_routed: total_steps,
            total_cache_read_tokens: cache_read,
            total_cache_write_tokens: cache_write,
            total_fallback_count: 0,
            total_projection_mismatch_count: 0,
            override_hit_count: 0,
            model_tier_counts: Default::default(),
            total_input_tokens: cache_read + cache_write,
            total_uncached_input_tokens: cache_write,
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
        designer_a: BossActorHandle::new("boss-a", "boss-a", BossActorRole::DesignerA),
        executor_b: BossActorHandle::new("boss-b", "boss-b", BossActorRole::ExecutorB),
        active_children: vec![],
        steps,
        history_summary: vec![],
        observability_summary,
        lism_policy: BossLisMPolicy::Inherit,
    }
}

fn make_report_with_usage(
    total_steps: usize,
    completed_steps: usize,
    input_tokens: usize,
    output_tokens: usize,
    sent_chars: usize,
) -> BossReportPayload {
    let mut report = make_report(total_steps, completed_steps, 0, 0, 0);
    report.observability_summary = Some(BossObservabilitySummary {
        total_steps_routed: total_steps,
        total_cache_read_tokens: 0,
        total_cache_write_tokens: 0,
        total_fallback_count: 0,
        total_projection_mismatch_count: 0,
        override_hit_count: 0,
        model_tier_counts: Default::default(),
        total_input_tokens: input_tokens,
        total_uncached_input_tokens: input_tokens,
        total_output_tokens: output_tokens,
        estimated_cost_micros_usd: 0,
        total_original_chars: sent_chars,
        total_sent_chars: sent_chars,
    });
    report
}

// ── basic record collection ───────────────────────────────────────────────────

#[test]
fn r1_1_in_memory_sink_starts_empty() {
    let sink = LisMAbSampleSink::in_memory();
    assert_eq!(sink.record_count(), 0);
    assert!(sink.records().is_empty());
    assert!(sink.path().is_none());
}

#[test]
fn r1_1_record_run_adds_record_with_correct_lism_flag() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(3, 3, 600, 400, 1000);

    sink.record_run("run-on-1", true, &report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "run-off-1",
        false,
        &report,
        BossTestRunOutcome::Completed,
        0,
    );

    assert_eq!(sink.record_count(), 2);
    let records = sink.records();
    assert!(records[0].lism_enabled);
    assert!(!records[1].lism_enabled);
}

#[test]
fn r1_1_record_extracts_cache_hit_ratio_correctly() {
    let sink = LisMAbSampleSink::in_memory();
    // cache_read=600, cache_write=400 → ratio = 600/1000 = 0.6
    let report = make_report(1, 1, 600, 400, 0);
    sink.record_run("run-1", true, &report, BossTestRunOutcome::Completed, 0);

    let records = sink.records();
    let ratio = records[0].cache_hit_ratio.expect("ratio should be Some");
    assert!((ratio - 0.6).abs() < 1e-9);
}

#[test]
fn r1_1_record_cache_hit_ratio_none_when_no_cache_data() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(1, 1, 0, 0, 0);
    sink.record_run("run-1", true, &report, BossTestRunOutcome::Completed, 0);

    let records = sink.records();
    assert!(records[0].cache_hit_ratio.is_none());
}

#[test]
fn r1_1_record_extracts_cost_and_tokens_saved() {
    let sink = LisMAbSampleSink::in_memory();
    // cache_read=800, cache_write=200 → tokens_saved = 800
    let report = make_report(1, 1, 800, 200, 2500);
    sink.record_run("run-1", true, &report, BossTestRunOutcome::Completed, 0);

    let records = sink.records();
    assert_eq!(records[0].cost_micros_usd, 2500);
    assert_eq!(records[0].estimated_tokens_saved, 800);
}

#[test]
fn r1_1_record_extracts_usage_and_prompt_size_fields() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report_with_usage(2, 2, 1234, 56, 7890);

    sink.record_run("usage-run", true, &report, BossTestRunOutcome::Completed, 0);

    let record = &sink.records()[0];
    assert_eq!(record.total_input_tokens, 1234);
    assert_eq!(record.total_output_tokens, 56);
    assert_eq!(record.sent_prompt_chars, 7890);
    assert_eq!(record.original_prompt_chars, 7890);
}

#[test]
fn r1_1_record_captures_pending_approval_count() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(2, 1, 0, 0, 0);
    sink.record_run("run-1", true, &report, BossTestRunOutcome::Aborted, 3);

    let records = sink.records();
    assert_eq!(records[0].pending_approval_count, 3);
    assert_eq!(records[0].outcome, BossTestRunOutcome::Aborted);
}

#[test]
fn r1_1_record_counts_completed_steps_correctly() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(5, 3, 0, 0, 0);
    sink.record_run("run-1", true, &report, BossTestRunOutcome::Aborted, 0);

    let records = sink.records();
    assert_eq!(records[0].total_steps, 5);
    assert_eq!(records[0].completed_steps, 3);
}

// ── summarize ─────────────────────────────────────────────────────────────────

#[test]
fn r1_1_summarize_empty_sink_returns_zero_runs() {
    let sink = LisMAbSampleSink::in_memory();
    let summary = sink.summarize();
    assert_eq!(summary.on_runs, 0);
    assert_eq!(summary.off_runs, 0);
    assert!(!summary.has_both_arms());
    assert!(summary.on_avg_cache_hit_ratio.is_none());
    assert!(summary.off_avg_cache_hit_ratio.is_none());
    assert!(summary.on_completion_rate.is_none());
    assert!(summary.off_completion_rate.is_none());
}

#[test]
fn r1_1_summarize_single_arm_has_both_arms_false() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(1, 1, 600, 400, 1000);
    sink.record_run("run-on-1", true, &report, BossTestRunOutcome::Completed, 0);

    let summary = sink.summarize();
    assert_eq!(summary.on_runs, 1);
    assert_eq!(summary.off_runs, 0);
    assert!(!summary.has_both_arms());
}

#[test]
fn r1_1_summarize_both_arms_has_both_arms_true() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(1, 1, 600, 400, 1000);
    sink.record_run("run-on-1", true, &report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "run-off-1",
        false,
        &report,
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    assert!(summary.has_both_arms());
}

#[test]
fn r1_1_summarize_avg_cache_hit_ratio_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    // on arm: 600/1000 = 0.6
    let on_report = make_report(1, 1, 600, 400, 0);
    // off arm: 200/1000 = 0.2
    let off_report = make_report(1, 1, 200, 800, 0);

    sink.record_run("on-1", true, &on_report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "off-1",
        false,
        &off_report,
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    let on_ratio = summary
        .on_avg_cache_hit_ratio
        .expect("on ratio should be Some");
    let off_ratio = summary
        .off_avg_cache_hit_ratio
        .expect("off ratio should be Some");
    assert!((on_ratio - 0.6).abs() < 1e-9);
    assert!((off_ratio - 0.2).abs() < 1e-9);
}

#[test]
fn r1_1_summarize_cache_hit_ratio_delta_positive_means_lism_helps() {
    let sink = LisMAbSampleSink::in_memory();
    let on_report = make_report(1, 1, 800, 200, 0); // ratio = 0.8
    let off_report = make_report(1, 1, 300, 700, 0); // ratio = 0.3

    sink.record_run("on-1", true, &on_report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "off-1",
        false,
        &off_report,
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    let delta = summary
        .cache_hit_ratio_delta()
        .expect("delta should be Some");
    assert!(
        delta > 0.0,
        "positive delta means LisM improves cache hit ratio"
    );
    assert!((delta - 0.5).abs() < 1e-9);
}

#[test]
fn r1_1_summarize_hit_run_rate_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    sink.record_run(
        "on-hit",
        true,
        &make_report(1, 1, 800, 200, 0),
        BossTestRunOutcome::Completed,
        0,
    );
    sink.record_run(
        "on-miss",
        true,
        &make_report_with_usage(1, 1, 1000, 50, 0),
        BossTestRunOutcome::Completed,
        0,
    );
    sink.record_run(
        "off-miss",
        false,
        &make_report_with_usage(1, 1, 1000, 50, 0),
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    assert_eq!(summary.on_hit_run_rate, Some(0.5));
    assert_eq!(summary.off_hit_run_rate, Some(0.0));
    assert_eq!(summary.hit_run_rate_delta(), Some(0.5));
}

#[test]
fn r1_1_summarize_cache_read_distribution_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    for (run_id, cache_read) in [("on-1", 0usize), ("on-2", 200), ("on-3", 800)] {
        sink.record_run(
            run_id,
            true,
            &make_report(1, 1, cache_read, 0, 0),
            BossTestRunOutcome::Completed,
            0,
        );
    }

    let summary = sink.summarize();
    let dist = summary
        .on_cache_read_tokens_distribution
        .expect("distribution should be Some");
    assert_eq!(dist.sample_count, 3);
    assert_eq!(dist.nonzero_count, 2);
    assert_eq!(dist.p50, 200);
    assert_eq!(dist.p90, 800);
    assert_eq!(dist.max, 800);
}

#[test]
fn r1_1_summarize_avg_cost_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    let on_report = make_report(1, 1, 0, 0, 1000);
    let off_report = make_report(1, 1, 0, 0, 3000);

    sink.record_run("on-1", true, &on_report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "off-1",
        false,
        &off_report,
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    assert_eq!(summary.on_avg_cost_micros_usd, 1000);
    assert_eq!(summary.off_avg_cost_micros_usd, 3000);
    assert_eq!(summary.cost_delta_micros(), -2000); // LisM saves 2000 micros
}

#[test]
fn r1_1_summarize_completion_rate_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(1, 1, 0, 0, 0);

    // on arm: 2 completed, 1 aborted → 2/3
    sink.record_run("on-1", true, &report, BossTestRunOutcome::Completed, 0);
    sink.record_run("on-2", true, &report, BossTestRunOutcome::Completed, 0);
    sink.record_run("on-3", true, &report, BossTestRunOutcome::Aborted, 0);
    // off arm: 1 completed, 1 aborted → 0.5
    sink.record_run("off-1", false, &report, BossTestRunOutcome::Completed, 0);
    sink.record_run("off-2", false, &report, BossTestRunOutcome::Aborted, 0);

    let summary = sink.summarize();
    let on_rate = summary
        .on_completion_rate
        .expect("on completion rate should be Some");
    let off_rate = summary
        .off_completion_rate
        .expect("off completion rate should be Some");
    assert!((on_rate - 2.0 / 3.0).abs() < 1e-9);
    assert!((off_rate - 0.5).abs() < 1e-9);
}

#[test]
fn r1_1_summarize_avg_tokens_saved_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    // on: cache_read=800, cache_write=200 → tokens_saved=800
    let on_report = make_report(1, 1, 800, 200, 0);
    // off: cache_read=100, cache_write=900 → tokens_saved=100
    let off_report = make_report(1, 1, 100, 900, 0);

    sink.record_run("on-1", true, &on_report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "off-1",
        false,
        &off_report,
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    assert_eq!(summary.on_avg_tokens_saved, 800);
    assert_eq!(summary.off_avg_tokens_saved, 100);
}

#[test]
fn r1_1_summarize_multi_run_averages_correctly() {
    let sink = LisMAbSampleSink::in_memory();
    // on arm: two runs with ratios 0.6 and 0.8 → avg 0.7
    let on_a = make_report(1, 1, 600, 400, 1000);
    let on_b = make_report(1, 1, 800, 200, 3000);
    sink.record_run("on-1", true, &on_a, BossTestRunOutcome::Completed, 0);
    sink.record_run("on-2", true, &on_b, BossTestRunOutcome::Completed, 0);

    let off_report = make_report(1, 1, 200, 800, 2000);
    sink.record_run(
        "off-1",
        false,
        &off_report,
        BossTestRunOutcome::Completed,
        0,
    );

    let summary = sink.summarize();
    assert_eq!(summary.on_runs, 2);
    assert_eq!(summary.off_runs, 1);
    let on_ratio = summary
        .on_avg_cache_hit_ratio
        .expect("on ratio should be Some");
    assert!((on_ratio - 0.7).abs() < 1e-9);
    assert_eq!(summary.on_avg_cost_micros_usd, 2000); // (1000+3000)/2
}

// ── JSONL persistence ─────────────────────────────────────────────────────────

#[test]
fn r1_1_jsonl_sink_persists_records_to_disk() {
    let path = unique_temp_path("r1-1-lism-ab");
    let sink = LisMAbSampleSink::with_jsonl_path(&path).unwrap();
    let report = make_report(2, 2, 600, 400, 1500);

    sink.record_run("run-on-1", true, &report, BossTestRunOutcome::Completed, 0);
    sink.record_run(
        "run-off-1",
        false,
        &report,
        BossTestRunOutcome::Completed,
        1,
    );

    let loaded = LisMAbSampleSink::load_records(&path);
    assert_eq!(loaded.len(), 2);
    assert!(loaded[0].lism_enabled);
    assert!(!loaded[1].lism_enabled);
    assert_eq!(loaded[1].pending_approval_count, 1);

    let _ = std::fs::remove_file(path);
}

#[test]
fn r1_1_jsonl_sink_appends_across_instances() {
    let path = unique_temp_path("r1-1-lism-ab-append");
    let report = make_report(1, 1, 600, 400, 1000);

    {
        let sink = LisMAbSampleSink::with_jsonl_path(&path).unwrap();
        sink.record_run("run-1", true, &report, BossTestRunOutcome::Completed, 0);
    }
    {
        let sink = LisMAbSampleSink::with_jsonl_path(&path).unwrap();
        sink.record_run("run-2", false, &report, BossTestRunOutcome::Completed, 0);
    }

    let loaded = LisMAbSampleSink::load_records(&path);
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].run_id, "run-1");
    assert_eq!(loaded[1].run_id, "run-2");

    let _ = std::fs::remove_file(path);
}

#[test]
fn r1_1_load_records_returns_empty_for_missing_file() {
    let records = LisMAbSampleSink::load_records("/tmp/nonexistent-r1-1-lism-ab.jsonl");
    assert!(records.is_empty());
}

#[test]
fn r1_1_jsonl_round_trip_preserves_all_fields() {
    let path = unique_temp_path("r1-1-lism-ab-rt");
    let sink = LisMAbSampleSink::with_jsonl_path(&path).unwrap();
    let report = make_report(3, 2, 700, 300, 4200);

    sink.record_run("rt-run", true, &report, BossTestRunOutcome::RolledBack, 2);

    let loaded = LisMAbSampleSink::load_records(&path);
    assert_eq!(loaded.len(), 1);
    let r = &loaded[0];
    assert_eq!(r.run_id, "rt-run");
    assert!(r.lism_enabled);
    assert_eq!(r.total_steps, 3);
    assert_eq!(r.completed_steps, 2);
    assert_eq!(r.cost_micros_usd, 4200);
    assert_eq!(r.pending_approval_count, 2);
    assert_eq!(r.outcome, BossTestRunOutcome::RolledBack);
    let ratio = r.cache_hit_ratio.expect("ratio should be Some");
    assert!((ratio - 0.7).abs() < 1e-9);

    let _ = std::fs::remove_file(path);
}

// ── shared sink ───────────────────────────────────────────────────────────────

#[test]
fn r1_1_new_shared_ab_sink_starts_empty() {
    let sink = new_shared_ab_sink();
    assert_eq!(sink.record_count(), 0);
}

#[test]
fn r1_1_shared_sink_can_be_cloned_and_records_are_shared() {
    let sink = new_shared_ab_sink();
    let sink2 = sink.clone();
    let report = make_report(1, 1, 600, 400, 0);

    sink.record_run("run-1", true, &report, BossTestRunOutcome::Completed, 0);

    assert_eq!(sink2.record_count(), 1);
}

// ── push_record (R1 slice 3 — in-memory import for summarize path) ────────────

#[test]
fn r1_3_push_record_adds_to_memory_without_file_write() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(2, 2, 600, 400, 1000);

    // Build a record via record_run first to get a real LisMAbSampleRecord shape.
    sink.record_run("run-1", true, &report, BossTestRunOutcome::Completed, 0);
    let original = sink.records().into_iter().next().unwrap();

    // Push a cloned record directly via push_record (no JSONL writer → no I/O).
    let sink2 = LisMAbSampleSink::in_memory();
    assert!(sink2.path().is_none(), "in_memory sink should have no path");
    sink2.push_record(original.clone());
    assert_eq!(sink2.record_count(), 1);
    assert_eq!(sink2.records()[0].run_id, "run-1");
}

#[test]
fn r1_3_push_record_multiple_records_preserved_in_order() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(1, 1, 600, 400, 0);
    sink.record_run("first", true, &report, BossTestRunOutcome::Completed, 0);
    let r1 = sink.records()[0].clone();
    sink.record_run("second", false, &report, BossTestRunOutcome::Aborted, 0);
    let r2 = sink.records()[1].clone();

    let sink2 = LisMAbSampleSink::in_memory();
    sink2.push_record(r1);
    sink2.push_record(r2);

    assert_eq!(sink2.record_count(), 2);
    assert_eq!(sink2.records()[0].run_id, "first");
    assert_eq!(sink2.records()[1].run_id, "second");
}

#[test]
fn r1_3_push_record_then_summarize_matches_direct_record_run() {
    let report_on = make_report(1, 1, 800, 200, 1000);
    let report_off = make_report(1, 1, 300, 700, 3000);

    // Direct recording path.
    let sink_direct = LisMAbSampleSink::in_memory();
    sink_direct.record_run("on-1", true, &report_on, BossTestRunOutcome::Completed, 0);
    sink_direct.record_run(
        "off-1",
        false,
        &report_off,
        BossTestRunOutcome::Completed,
        0,
    );

    // Import path (simulates --lism-ab-summarize loading JSONL records).
    let sink_import = LisMAbSampleSink::in_memory();
    for rec in sink_direct.records() {
        sink_import.push_record(rec);
    }

    let s_direct = sink_direct.summarize();
    let s_import = sink_import.summarize();
    assert_eq!(s_direct.on_runs, s_import.on_runs);
    assert_eq!(s_direct.off_runs, s_import.off_runs);
    assert_eq!(
        s_direct.on_avg_cost_micros_usd,
        s_import.on_avg_cost_micros_usd
    );
    assert_eq!(
        s_direct.off_avg_cost_micros_usd,
        s_import.off_avg_cost_micros_usd
    );
    assert_eq!(
        s_direct.on_avg_cache_hit_ratio.map(|v| (v * 1000.0) as u64),
        s_import.on_avg_cache_hit_ratio.map(|v| (v * 1000.0) as u64),
    );
}

#[test]
fn r1_3_load_records_then_push_into_sink_matches_summarize() {
    let path = unique_temp_path("r1-3-load-push");
    let sink_writer = LisMAbSampleSink::with_jsonl_path(&path).unwrap();
    let report = make_report(2, 2, 700, 300, 2000);
    sink_writer.record_run("run-on", true, &report, BossTestRunOutcome::Completed, 0);
    sink_writer.record_run("run-off", false, &report, BossTestRunOutcome::Aborted, 0);

    // Simulate --lism-ab-summarize: load from file, push into in-memory sink, summarize.
    let loaded = LisMAbSampleSink::load_records(&path);
    assert_eq!(loaded.len(), 2);

    let sink_summary = LisMAbSampleSink::in_memory();
    for rec in loaded {
        sink_summary.push_record(rec);
    }
    let summary = sink_summary.summarize();
    assert_eq!(summary.on_runs, 1);
    assert_eq!(summary.off_runs, 1);
    assert!(summary.has_both_arms());
    assert_eq!(summary.on_avg_cost_micros_usd, 2000);

    let _ = std::fs::remove_file(path);
}

// ── Rollout conclusion (R1 slice 4) ───────────────────────────────────────────

fn make_two_arm_sink(
    on_cache_ratio_pct: u64, // e.g. 80 → 0.8
    off_cache_ratio_pct: u64,
    on_cost: u64,
    off_cost: u64,
    runs_per_arm: usize,
) -> LisMAbSampleSink {
    let sink = LisMAbSampleSink::in_memory();
    let total = 100usize;
    let on_read = on_cache_ratio_pct as usize;
    let on_write = total - on_read;
    let off_read = off_cache_ratio_pct as usize;
    let off_write = total - off_read;

    for i in 0..runs_per_arm {
        let on_report = make_report(2, 2, on_read, on_write, on_cost);
        sink.record_run(
            format!("on-{i}"),
            true,
            &on_report,
            BossTestRunOutcome::Completed,
            0,
        );
        let off_report = make_report(2, 2, off_read, off_write, off_cost);
        sink.record_run(
            format!("off-{i}"),
            false,
            &off_report,
            BossTestRunOutcome::Completed,
            0,
        );
    }
    sink
}

#[test]
fn r1_4_conclude_force_on_when_lism_clearly_helps() {
    // LisM ON: 80% cache hit, cost 1000μ
    // LisM OFF: 40% cache hit, cost 3000μ
    // Δcache = +0.4 > 0.05 threshold; Δcost = -2000μ < 0 → ForceOn
    let sink = make_two_arm_sink(80, 40, 1000, 3000, 3);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(conclusion.recommendation, LisMPolicyRecommendation::ForceOn);
    let delta = conclusion
        .cache_hit_ratio_delta
        .expect("delta should be Some");
    assert!(delta > 0.0, "cache delta should be positive");
    assert!(
        conclusion.cost_delta_micros < 0,
        "cost delta should be negative (LisM saves)"
    );
}

#[test]
fn r1_4_conclude_force_off_when_lism_hurts_cache() {
    // LisM ON: 30% cache hit (much worse); cost neutral
    // Δcache = -0.4 < -0.05 → ForceOff
    let sink = make_two_arm_sink(30, 70, 2000, 2000, 3);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(
        conclusion.recommendation,
        LisMPolicyRecommendation::ForceOff
    );
    let delta = conclusion
        .cache_hit_ratio_delta
        .expect("delta should be Some");
    assert!(delta < 0.0, "cache delta should be negative");
}

#[test]
fn r1_4_conclude_force_off_when_cost_penalty_exceeded() {
    // Cache neutral (both 60%), but LisM costs 600_000μ more (> 500_000μ threshold)
    let sink = make_two_arm_sink(60, 60, 700_000, 100_000, 3);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(
        conclusion.recommendation,
        LisMPolicyRecommendation::ForceOff
    );
    assert!(conclusion.cost_delta_micros > 500_000);
}

#[test]
fn r1_4_conclude_inherit_when_signal_mixed() {
    // Cache improves slightly (within noise), cost is neutral → Inherit
    let sink = make_two_arm_sink(62, 60, 1000, 1000, 3);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    // Δcache = 0.02 < 0.05 threshold, Δcost = 0 → neither ForceOn nor ForceOff
    assert_eq!(conclusion.recommendation, LisMPolicyRecommendation::Inherit);
}

#[test]
fn r1_4_conclude_inconclusive_with_insufficient_data() {
    // Only 2 runs per arm, min_runs_per_arm = 3 → Inconclusive
    let sink = make_two_arm_sink(80, 40, 1000, 3000, 2);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(
        conclusion.recommendation,
        LisMPolicyRecommendation::Inconclusive
    );
    assert!(conclusion.reason.contains("Insufficient data"));
}

#[test]
fn r1_4_conclude_inconclusive_when_only_one_arm() {
    let sink = LisMAbSampleSink::in_memory();
    let report = make_report(1, 1, 600, 400, 1000);
    // Only on-arm records
    for i in 0..5 {
        sink.record_run(
            format!("on-{i}"),
            true,
            &report,
            BossTestRunOutcome::Completed,
            0,
        );
    }
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(
        conclusion.recommendation,
        LisMPolicyRecommendation::Inconclusive
    );
}

#[test]
fn r1_4_conclude_inconclusive_when_usage_signal_is_missing() {
    let sink = LisMAbSampleSink::in_memory();
    let report_without_usage = make_report(2, 2, 0, 0, 0);

    for i in 0..3 {
        sink.record_run(
            format!("on-{i}"),
            true,
            &report_without_usage,
            BossTestRunOutcome::Completed,
            0,
        );
        sink.record_run(
            format!("off-{i}"),
            false,
            &report_without_usage,
            BossTestRunOutcome::Completed,
            0,
        );
    }

    let summary = sink.summarize();
    assert_eq!(summary.cache_hit_ratio_delta(), None);
    assert_eq!(summary.cost_delta_micros(), 0);

    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(
        conclusion.recommendation,
        LisMPolicyRecommendation::Inconclusive
    );
    assert!(
        conclusion
            .reason
            .contains("No measurable cache, cost, token, or prompt-size signal")
    );
}

#[test]
fn r1_4_conclude_force_on_when_input_tokens_drop_without_cache_data() {
    let sink = LisMAbSampleSink::in_memory();

    for i in 0..3 {
        let on_report = make_report_with_usage(1, 1, 600, 20, 2400);
        sink.record_run(
            format!("on-{i}"),
            true,
            &on_report,
            BossTestRunOutcome::Completed,
            0,
        );
        let off_report = make_report_with_usage(1, 1, 1400, 20, 5600);
        sink.record_run(
            format!("off-{i}"),
            false,
            &off_report,
            BossTestRunOutcome::Completed,
            0,
        );
    }

    let summary = sink.summarize();
    assert_eq!(summary.input_token_delta(), -800);
    assert_eq!(summary.sent_prompt_char_delta(), -3200);

    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(conclusion.recommendation, LisMPolicyRecommendation::ForceOn);
    assert!(conclusion.reason.contains("reduces input tokens"));
}

#[test]
fn r1_4_conclude_force_on_when_input_tokens_drop_and_cost_delta_within_threshold() {
    let sink = LisMAbSampleSink::in_memory();

    for i in 0..3 {
        let mut on_report = make_report_with_usage(1, 1, 363, 210, 0);
        on_report
            .observability_summary
            .as_mut()
            .expect("usage summary")
            .estimated_cost_micros_usd = 7_489;
        sink.record_run(
            format!("on-{i}"),
            true,
            &on_report,
            BossTestRunOutcome::Completed,
            0,
        );
        let mut off_report = make_report_with_usage(1, 1, 2443, 0, 0);
        off_report
            .observability_summary
            .as_mut()
            .expect("usage summary")
            .estimated_cost_micros_usd = 7_329;
        sink.record_run(
            format!("off-{i}"),
            false,
            &off_report,
            BossTestRunOutcome::Completed,
            0,
        );
    }

    let summary = sink.summarize();
    assert_eq!(summary.input_token_delta(), -2080);
    assert_eq!(summary.cost_delta_micros(), 160);

    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    assert_eq!(conclusion.recommendation, LisMPolicyRecommendation::ForceOn);
    assert!(conclusion.reason.contains("within threshold"));
}

#[test]
fn r1_4_conclude_serde_round_trip() {
    let sink = make_two_arm_sink(80, 40, 1000, 3000, 3);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);

    let json = serde_json::to_string(&conclusion).expect("serialize should succeed");
    let loaded: LisMRolloutConclusion =
        serde_json::from_str(&json).expect("deserialize should succeed");
    assert_eq!(loaded.recommendation, conclusion.recommendation);
    assert_eq!(loaded.cost_delta_micros, conclusion.cost_delta_micros);
}

#[test]
fn r1_4_conclude_display_contains_recommendation_and_reason() {
    let sink = make_two_arm_sink(80, 40, 1000, 3000, 3);
    let summary = sink.summarize();
    let conclusion = LisMRolloutConclusion::from_summary_defaults(&summary);
    let output = format!("{conclusion}");
    assert!(output.contains("ForceOn"), "display should mention ForceOn");
    assert!(output.contains("cache"), "display should mention cache");
}

#[test]
fn r1_4_conclude_custom_thresholds_force_on_at_low_threshold() {
    // With threshold = 0.01, even a tiny 0.02 delta should trigger ForceOn
    let sink = make_two_arm_sink(62, 60, 1000, 1000, 3);
    let summary = sink.summarize();
    let conclusion = summary.derive_rollout_conclusion(3, 0.01, 500_000);
    assert_eq!(conclusion.recommendation, LisMPolicyRecommendation::ForceOn);
}

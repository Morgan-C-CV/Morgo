use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::core::boss_state::{
    BossActorHandle, BossActorRole, BossLisMPolicy, BossObservabilitySummary,
    BossPlanStepStatus, BossReportPayload, BossStage, BossStepReport,
};
use rust_agent::core::boss_test_readiness::BossTestRunOutcome;
use rust_agent::core::lism_ab_sample::{LisMAbSampleSink, new_shared_ab_sink};

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
            total_output_tokens: 0,
            estimated_cost_micros_usd: cost_micros,
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
    sink.record_run("run-off-1", false, &report, BossTestRunOutcome::Completed, 0);

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
    sink.record_run("run-off-1", false, &report, BossTestRunOutcome::Completed, 0);

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
    sink.record_run("off-1", false, &off_report, BossTestRunOutcome::Completed, 0);

    let summary = sink.summarize();
    let on_ratio = summary.on_avg_cache_hit_ratio.expect("on ratio should be Some");
    let off_ratio = summary.off_avg_cache_hit_ratio.expect("off ratio should be Some");
    assert!((on_ratio - 0.6).abs() < 1e-9);
    assert!((off_ratio - 0.2).abs() < 1e-9);
}

#[test]
fn r1_1_summarize_cache_hit_ratio_delta_positive_means_lism_helps() {
    let sink = LisMAbSampleSink::in_memory();
    let on_report = make_report(1, 1, 800, 200, 0); // ratio = 0.8
    let off_report = make_report(1, 1, 300, 700, 0); // ratio = 0.3

    sink.record_run("on-1", true, &on_report, BossTestRunOutcome::Completed, 0);
    sink.record_run("off-1", false, &off_report, BossTestRunOutcome::Completed, 0);

    let summary = sink.summarize();
    let delta = summary.cache_hit_ratio_delta().expect("delta should be Some");
    assert!(delta > 0.0, "positive delta means LisM improves cache hit ratio");
    assert!((delta - 0.5).abs() < 1e-9);
}

#[test]
fn r1_1_summarize_avg_cost_computed_per_arm() {
    let sink = LisMAbSampleSink::in_memory();
    let on_report = make_report(1, 1, 0, 0, 1000);
    let off_report = make_report(1, 1, 0, 0, 3000);

    sink.record_run("on-1", true, &on_report, BossTestRunOutcome::Completed, 0);
    sink.record_run("off-1", false, &off_report, BossTestRunOutcome::Completed, 0);

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
    let on_rate = summary.on_completion_rate.expect("on completion rate should be Some");
    let off_rate = summary.off_completion_rate.expect("off completion rate should be Some");
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
    sink.record_run("off-1", false, &off_report, BossTestRunOutcome::Completed, 0);

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
    sink.record_run("off-1", false, &off_report, BossTestRunOutcome::Completed, 0);

    let summary = sink.summarize();
    assert_eq!(summary.on_runs, 2);
    assert_eq!(summary.off_runs, 1);
    let on_ratio = summary.on_avg_cache_hit_ratio.expect("on ratio should be Some");
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
    sink.record_run("run-off-1", false, &report, BossTestRunOutcome::Completed, 1);

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

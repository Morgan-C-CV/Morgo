use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::core::boss_state::BossReportPayload;
use crate::core::boss_test_readiness::BossTestRunOutcome;

// ── Record ────────────────────────────────────────────────────────────────────

/// One boss run's contribution to the LisM A/B comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LisMAbSampleRecord {
    pub run_id: String,
    /// Whether LisM was active for this run.
    pub lism_enabled: bool,
    pub total_steps: usize,
    pub completed_steps: usize,
    pub cache_hit_ratio: Option<f64>,
    pub estimated_tokens_saved: usize,
    pub cost_micros_usd: u64,
    pub pending_approval_count: usize,
    pub outcome: BossTestRunOutcome,
}

// ── Summary ───────────────────────────────────────────────────────────────────

/// Aggregated comparison between LisM-on and LisM-off runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LisMAbSummary {
    pub on_runs: usize,
    pub off_runs: usize,
    /// Mean cache hit ratio across LisM-on runs; None if no on-runs have cache data.
    pub on_avg_cache_hit_ratio: Option<f64>,
    /// Mean cache hit ratio across LisM-off runs; None if no off-runs have cache data.
    pub off_avg_cache_hit_ratio: Option<f64>,
    pub on_avg_cost_micros_usd: u64,
    pub off_avg_cost_micros_usd: u64,
    pub on_avg_tokens_saved: usize,
    pub off_avg_tokens_saved: usize,
    /// Fraction of on-runs that completed (vs aborted/rolled-back).
    pub on_completion_rate: Option<f64>,
    /// Fraction of off-runs that completed.
    pub off_completion_rate: Option<f64>,
}

impl LisMAbSummary {
    /// True when there is enough data in both arms to draw a comparison.
    pub fn has_both_arms(&self) -> bool {
        self.on_runs > 0 && self.off_runs > 0
    }

    /// Difference in average cache hit ratio (on − off). Positive means LisM helps.
    pub fn cache_hit_ratio_delta(&self) -> Option<f64> {
        Some(self.on_avg_cache_hit_ratio? - self.off_avg_cache_hit_ratio?)
    }

    /// Difference in average cost (on − off). Negative means LisM saves money.
    pub fn cost_delta_micros(&self) -> i64 {
        self.on_avg_cost_micros_usd as i64 - self.off_avg_cost_micros_usd as i64
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────

/// Collects `LisMAbSampleRecord`s and optionally persists them to JSONL.
///
/// Thread-safe via internal `Mutex`; share with `Arc<LisMAbSampleSink>`.
pub struct LisMAbSampleSink {
    inner: Mutex<AbSinkInner>,
}

struct AbSinkInner {
    records: Vec<LisMAbSampleRecord>,
    writer: Option<BufWriter<File>>,
    path: Option<PathBuf>,
}

impl std::fmt::Debug for LisMAbSampleSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("LisMAbSampleSink")
            .field("record_count", &inner.records.len())
            .field("path", &inner.path)
            .finish()
    }
}

impl LisMAbSampleSink {
    pub fn in_memory() -> Self {
        Self {
            inner: Mutex::new(AbSinkInner {
                records: Vec::new(),
                writer: None,
                path: None,
            }),
        }
    }

    pub fn with_jsonl_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            inner: Mutex::new(AbSinkInner {
                records: Vec::new(),
                writer: Some(BufWriter::new(file)),
                path: Some(path),
            }),
        })
    }

    pub fn records(&self) -> Vec<LisMAbSampleRecord> {
        self.inner.lock().unwrap().records.clone()
    }

    pub fn record_count(&self) -> usize {
        self.inner.lock().unwrap().records.len()
    }

    pub fn path(&self) -> Option<PathBuf> {
        self.inner.lock().unwrap().path.clone()
    }

    /// Record one boss run. `lism_enabled` is the effective value for that run.
    pub fn record_run(
        &self,
        run_id: impl Into<String>,
        lism_enabled: bool,
        report: &BossReportPayload,
        outcome: BossTestRunOutcome,
        pending_approval_count: usize,
    ) {
        let record = build_ab_record(run_id.into(), lism_enabled, report, outcome, pending_approval_count);
        self.push(record);
    }

    /// Compute the A/B summary across all collected records.
    pub fn summarize(&self) -> LisMAbSummary {
        let records = self.records();
        summarize_records(&records)
    }

    /// Load records from the JSONL file at `path` (for post-run analysis).
    pub fn load_records(path: impl AsRef<Path>) -> Vec<LisMAbSampleRecord> {
        let Ok(file) = File::open(path) else { return vec![] };
        BufReader::new(file)
            .lines()
            .filter_map(|line| line.ok())
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str(&line).ok())
            .collect()
    }

    fn push(&self, record: LisMAbSampleRecord) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(writer) = &mut inner.writer {
            if let Ok(json) = serde_json::to_string(&record) {
                let _ = writeln!(writer, "{}", json);
                let _ = writer.flush();
            }
        }
        inner.records.push(record);
    }

    /// Push a pre-built record (e.g., loaded from JSONL) into the in-memory list only.
    pub fn push_record(&self, record: LisMAbSampleRecord) {
        self.inner.lock().unwrap().records.push(record);
    }
}

pub type SharedLisMAbSampleSink = Arc<LisMAbSampleSink>;

pub fn new_shared_ab_sink() -> SharedLisMAbSampleSink {
    Arc::new(LisMAbSampleSink::in_memory())
}

pub fn new_shared_ab_sink_with_path(path: impl AsRef<Path>) -> anyhow::Result<SharedLisMAbSampleSink> {
    Ok(Arc::new(LisMAbSampleSink::with_jsonl_path(path)?))
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn build_ab_record(
    run_id: String,
    lism_enabled: bool,
    report: &BossReportPayload,
    outcome: BossTestRunOutcome,
    pending_approval_count: usize,
) -> LisMAbSampleRecord {
    let obs = report.observability_summary.as_ref();
    let total_steps = report.total_steps.unwrap_or(0);
    let completed_steps = report.steps.iter().filter(|s| {
        matches!(s.status, crate::core::boss_state::BossPlanStepStatus::Completed)
    }).count();

    LisMAbSampleRecord {
        run_id,
        lism_enabled,
        total_steps,
        completed_steps,
        cache_hit_ratio: obs.and_then(|o| o.cache_hit_ratio()),
        estimated_tokens_saved: obs.map(|o| o.estimated_tokens_saved()).unwrap_or(0),
        cost_micros_usd: obs.map(|o| o.estimated_cost_micros_usd).unwrap_or(0),
        pending_approval_count,
        outcome,
    }
}

fn summarize_records(records: &[LisMAbSampleRecord]) -> LisMAbSummary {
    let on: Vec<_> = records.iter().filter(|r| r.lism_enabled).collect();
    let off: Vec<_> = records.iter().filter(|r| !r.lism_enabled).collect();

    LisMAbSummary {
        on_runs: on.len(),
        off_runs: off.len(),
        on_avg_cache_hit_ratio: avg_cache_hit_ratio(&on),
        off_avg_cache_hit_ratio: avg_cache_hit_ratio(&off),
        on_avg_cost_micros_usd: avg_cost(&on),
        off_avg_cost_micros_usd: avg_cost(&off),
        on_avg_tokens_saved: avg_tokens_saved(&on),
        off_avg_tokens_saved: avg_tokens_saved(&off),
        on_completion_rate: completion_rate(&on),
        off_completion_rate: completion_rate(&off),
    }
}

fn avg_cache_hit_ratio(records: &[&LisMAbSampleRecord]) -> Option<f64> {
    let with_data: Vec<f64> = records.iter().filter_map(|r| r.cache_hit_ratio).collect();
    if with_data.is_empty() {
        None
    } else {
        Some(with_data.iter().sum::<f64>() / with_data.len() as f64)
    }
}

fn avg_cost(records: &[&LisMAbSampleRecord]) -> u64 {
    if records.is_empty() {
        return 0;
    }
    let total: u64 = records.iter().map(|r| r.cost_micros_usd).sum();
    total / records.len() as u64
}

fn avg_tokens_saved(records: &[&LisMAbSampleRecord]) -> usize {
    if records.is_empty() {
        return 0;
    }
    let total: usize = records.iter().map(|r| r.estimated_tokens_saved).sum();
    total / records.len()
}

fn completion_rate(records: &[&LisMAbSampleRecord]) -> Option<f64> {
    if records.is_empty() {
        return None;
    }
    let completed = records.iter().filter(|r| r.outcome == BossTestRunOutcome::Completed).count();
    Some(completed as f64 / records.len() as f64)
}

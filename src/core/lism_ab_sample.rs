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

    /// Derive a rollout conclusion from the summary data.
    ///
    /// Requires at least `min_runs_per_arm` in each arm to produce a non-Inconclusive result.
    /// Thresholds:
    /// - `cache_delta_threshold`: minimum positive delta to count as "LisM helps cache"
    /// - `cost_penalty_threshold_micros`: maximum cost increase before recommending ForceOff
    /// If enough runs exist but neither arm reports cache/cost/tokens-saved data,
    /// the result is still inconclusive: completion parity alone is not a rollout signal.
    pub fn derive_rollout_conclusion(
        &self,
        min_runs_per_arm: usize,
        cache_delta_threshold: f64,
        cost_penalty_threshold_micros: u64,
    ) -> LisMRolloutConclusion {
        if !self.has_both_arms()
            || self.on_runs < min_runs_per_arm
            || self.off_runs < min_runs_per_arm
        {
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::Inconclusive,
                reason: format!(
                    "Insufficient data: {} on-runs and {} off-runs (min {} per arm required)",
                    self.on_runs, self.off_runs, min_runs_per_arm
                ),
                cache_hit_ratio_delta: self.cache_hit_ratio_delta(),
                cost_delta_micros: self.cost_delta_micros(),
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        let cache_delta = self.cache_hit_ratio_delta();
        let cost_delta = self.cost_delta_micros();

        let no_cache_signal = cache_delta.is_none();
        let no_cost_signal = cost_delta == 0
            && self.on_avg_cost_micros_usd == 0
            && self.off_avg_cost_micros_usd == 0;
        let no_saved_token_signal = self.on_avg_tokens_saved == 0 && self.off_avg_tokens_saved == 0;

        if no_cache_signal && no_cost_signal && no_saved_token_signal {
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::Inconclusive,
                reason: "No measurable cache or cost signal: both arms have enough runs, but cache_hit_ratio is unavailable and cost/tokens_saved are zero; collect usage metadata before changing rollout policy".into(),
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        // Hard penalty: LisM significantly degrades cache OR costs much more → ForceOff
        let cache_hurts = cache_delta.map_or(false, |d| d < -cache_delta_threshold);
        let cost_penalty_exceeded = cost_delta > cost_penalty_threshold_micros as i64;

        if cache_hurts || cost_penalty_exceeded {
            let reason = if cache_hurts && cost_penalty_exceeded {
                format!(
                    "LisM degrades cache hit ratio ({:+.3}) and increases cost ({:+}μ); recommend ForceOff",
                    cache_delta.unwrap_or(0.0),
                    cost_delta
                )
            } else if cache_hurts {
                format!(
                    "LisM degrades cache hit ratio ({:+.3}); recommend ForceOff",
                    cache_delta.unwrap_or(0.0)
                )
            } else {
                format!(
                    "LisM increases cost beyond threshold ({:+}μ > {}μ); recommend ForceOff",
                    cost_delta, cost_penalty_threshold_micros
                )
            };
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::ForceOff,
                reason,
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        // Clear benefit: LisM improves cache AND does not increase cost → ForceOn
        let cache_helps = cache_delta.map_or(false, |d| d > cache_delta_threshold);
        let cost_neutral_or_better = cost_delta <= 0;

        if cache_helps && cost_neutral_or_better {
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::ForceOn,
                reason: format!(
                    "LisM improves cache hit ratio ({:+.3}) and reduces cost ({:+}μ); recommend ForceOn",
                    cache_delta.unwrap_or(0.0),
                    cost_delta
                ),
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        // Mixed or noisy signal → keep session-level Inherit
        LisMRolloutConclusion {
            recommendation: LisMPolicyRecommendation::Inherit,
            reason: format!(
                "Mixed signal: cache delta {}, cost delta {}μ — keep per-session Inherit policy",
                cache_delta.map_or("n/a".into(), |d| format!("{:+.3}", d)),
                cost_delta
            ),
            cache_hit_ratio_delta: cache_delta,
            cost_delta_micros: cost_delta,
            on_completion_rate: self.on_completion_rate,
            off_completion_rate: self.off_completion_rate,
        }
    }
}

// ── Rollout conclusion ────────────────────────────────────────────────────────

/// Policy recommendation derived from A/B sample data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LisMPolicyRecommendation {
    /// Not enough data in both arms to make a call.
    Inconclusive,
    /// LisM clearly helps — recommend setting `BossLisMPolicy::ForceOn` globally.
    ForceOn,
    /// LisM clearly hurts — recommend setting `BossLisMPolicy::ForceOff` globally.
    ForceOff,
    /// Signal is mixed or within noise — keep `BossLisMPolicy::Inherit` (per-session decision).
    Inherit,
}

/// Structured output of the A/B rollout analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LisMRolloutConclusion {
    pub recommendation: LisMPolicyRecommendation,
    pub reason: String,
    pub cache_hit_ratio_delta: Option<f64>,
    pub cost_delta_micros: i64,
    pub on_completion_rate: Option<f64>,
    pub off_completion_rate: Option<f64>,
}

impl LisMRolloutConclusion {
    /// Default thresholds for a standard evaluation.
    /// - min 3 runs per arm
    /// - cache delta threshold: 0.05 (5 pp)
    /// - cost penalty threshold: 500_000μ (0.50 USD)
    pub fn from_summary_defaults(summary: &LisMAbSummary) -> Self {
        summary.derive_rollout_conclusion(3, 0.05, 500_000)
    }
}

impl std::fmt::Display for LisMRolloutConclusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rec = match &self.recommendation {
            LisMPolicyRecommendation::Inconclusive => "INCONCLUSIVE",
            LisMPolicyRecommendation::ForceOn => "RECOMMEND: ForceOn",
            LisMPolicyRecommendation::ForceOff => "RECOMMEND: ForceOff",
            LisMPolicyRecommendation::Inherit => "RECOMMEND: Inherit (no change)",
        };
        writeln!(f, "Rollout Conclusion: {rec}")?;
        writeln!(f, "  Reason           : {}", self.reason)?;
        if let Some(d) = self.cache_hit_ratio_delta {
            writeln!(f, "  Δ cache_hit_ratio: {:+.3}", d)?;
        }
        writeln!(f, "  Δ cost           : {:+}μ", self.cost_delta_micros)?;
        if let (Some(on), Some(off)) = (self.on_completion_rate, self.off_completion_rate) {
            writeln!(f, "  completion on/off: {:.2} / {:.2}", on, off)?;
        }
        Ok(())
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
        let record = build_ab_record(
            run_id.into(),
            lism_enabled,
            report,
            outcome,
            pending_approval_count,
        );
        self.push(record);
    }

    /// Compute the A/B summary across all collected records.
    pub fn summarize(&self) -> LisMAbSummary {
        let records = self.records();
        summarize_records(&records)
    }

    /// Load records from the JSONL file at `path` (for post-run analysis).
    pub fn load_records(path: impl AsRef<Path>) -> Vec<LisMAbSampleRecord> {
        let Ok(file) = File::open(path) else {
            return vec![];
        };
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

pub fn new_shared_ab_sink_with_path(
    path: impl AsRef<Path>,
) -> anyhow::Result<SharedLisMAbSampleSink> {
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
    let completed_steps = report
        .steps
        .iter()
        .filter(|s| {
            matches!(
                s.status,
                crate::core::boss_state::BossPlanStepStatus::Completed
            )
        })
        .count();

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
    let completed = records
        .iter()
        .filter(|r| r.outcome == BossTestRunOutcome::Completed)
        .count();
    Some(completed as f64 / records.len() as f64)
}

use std::collections::BTreeMap;
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
    /// Provider-reported input tokens. Zero means no usage metadata was reported.
    #[serde(default)]
    pub total_input_tokens: usize,
    /// Provider-reported uncached input tokens billed at full input price.
    #[serde(default)]
    pub total_uncached_input_tokens: usize,
    /// Provider-reported output tokens. Zero means no usage metadata was reported.
    #[serde(default)]
    pub total_output_tokens: usize,
    /// Provider-reported cache read tokens.
    #[serde(default)]
    pub cache_read_tokens: usize,
    /// Provider-reported cache write/creation tokens.
    #[serde(default)]
    pub cache_write_tokens: usize,
    /// Original outbound chars before compression/context assembly, when known.
    #[serde(default)]
    pub original_prompt_chars: usize,
    /// Actual outbound chars after compression/context assembly, when known.
    #[serde(default)]
    pub sent_prompt_chars: usize,
    /// True when this run observed any provider-reported cache read tokens.
    #[serde(default)]
    pub cache_hit_observed: bool,
    /// Whether this run had any cache-related observability payload at all.
    #[serde(default)]
    pub cache_observability_present: bool,
    /// Legacy cache ratio field kept for backward compatibility with old JSONL.
    #[serde(default)]
    pub cache_hit_ratio: Option<f64>,
    pub estimated_tokens_saved: usize,
    pub cost_micros_usd: u64,
    #[serde(default)]
    pub fallback_count: usize,
    #[serde(default)]
    pub fallback_tier: Option<String>,
    #[serde(default)]
    pub fallback_reason: Option<String>,
    #[serde(default)]
    pub context_tier: String,
    #[serde(default)]
    pub model_tier_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub hydration_count: usize,
    #[serde(default)]
    pub stale_ref_count: usize,
    #[serde(default)]
    pub hydration_ref_missing: usize,
    #[serde(default)]
    pub tool_dispatch_count: usize,
    #[serde(default)]
    pub tool_dispatch_failure_count: usize,
    #[serde(default)]
    pub tool_dispatch_ref_write_count: usize,
    #[serde(default)]
    pub tool_dispatch_failure_taxonomy: BTreeMap<String, usize>,
    #[serde(default)]
    pub toolset_ids: Vec<String>,
    #[serde(default)]
    pub visible_tools: Vec<String>,
    #[serde(default)]
    pub schema_hashes: Vec<String>,
    #[serde(default)]
    pub permission_hashes: Vec<String>,
    #[serde(default)]
    pub actor_roles: Vec<String>,
    #[serde(default)]
    pub cwd_values: Vec<String>,
    #[serde(default)]
    pub config_roots: Vec<String>,
    #[serde(default)]
    pub workspace_capabilities: Vec<String>,
    #[serde(default)]
    pub tool_contract_mismatch_count: usize,
    #[serde(default)]
    pub last_failure_kinds: Vec<String>,
    #[serde(default)]
    pub last_recommended_repairs: Vec<String>,
    #[serde(default)]
    pub recovery_attempted: bool,
    #[serde(default)]
    pub recovery_tiers: Vec<String>,
    #[serde(default)]
    pub recovery_outcomes: Vec<String>,
    #[serde(default)]
    pub terminal_blocker_kinds: Vec<String>,
    #[serde(default)]
    pub missing_artifact_evidence_targets: Vec<String>,
    #[serde(default)]
    pub missing_verification_evidence_targets: Vec<String>,
    pub pending_approval_count: usize,
    pub outcome: BossTestRunOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenDistributionStats {
    pub sample_count: usize,
    pub nonzero_count: usize,
    pub p50: usize,
    pub p90: usize,
    pub max: usize,
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
    /// Fraction of runs that observed any cache-read tokens.
    pub on_hit_run_rate: Option<f64>,
    /// Fraction of runs that observed any cache-read tokens.
    pub off_hit_run_rate: Option<f64>,
    pub on_avg_cost_micros_usd: u64,
    pub off_avg_cost_micros_usd: u64,
    pub on_avg_input_tokens: usize,
    pub off_avg_input_tokens: usize,
    pub on_avg_uncached_input_tokens: usize,
    pub off_avg_uncached_input_tokens: usize,
    pub on_avg_output_tokens: usize,
    pub off_avg_output_tokens: usize,
    pub on_avg_cache_read_tokens: usize,
    pub off_avg_cache_read_tokens: usize,
    pub on_cache_read_tokens_distribution: Option<TokenDistributionStats>,
    pub off_cache_read_tokens_distribution: Option<TokenDistributionStats>,
    pub on_avg_cache_write_tokens: usize,
    pub off_avg_cache_write_tokens: usize,
    pub on_avg_sent_prompt_chars: usize,
    pub off_avg_sent_prompt_chars: usize,
    pub on_avg_tokens_saved: usize,
    pub off_avg_tokens_saved: usize,
    #[serde(default)]
    pub on_avg_fallback_count: usize,
    #[serde(default)]
    pub off_avg_fallback_count: usize,
    #[serde(default)]
    pub on_fallback_run_rate: Option<f64>,
    #[serde(default)]
    pub off_fallback_run_rate: Option<f64>,
    #[serde(default)]
    pub on_avg_hydration_count: usize,
    #[serde(default)]
    pub off_avg_hydration_count: usize,
    #[serde(default)]
    pub on_avg_stale_ref_count: usize,
    #[serde(default)]
    pub off_avg_stale_ref_count: usize,
    #[serde(default)]
    pub on_avg_hydration_ref_missing: usize,
    #[serde(default)]
    pub off_avg_hydration_ref_missing: usize,
    #[serde(default)]
    pub on_avg_tool_dispatch_count: usize,
    #[serde(default)]
    pub off_avg_tool_dispatch_count: usize,
    #[serde(default)]
    pub on_avg_tool_dispatch_failure_count: usize,
    #[serde(default)]
    pub off_avg_tool_dispatch_failure_count: usize,
    #[serde(default)]
    pub on_avg_tool_dispatch_ref_write_count: usize,
    #[serde(default)]
    pub off_avg_tool_dispatch_ref_write_count: usize,
    #[serde(default)]
    pub on_hydration_resolution_rate: Option<f64>,
    #[serde(default)]
    pub off_hydration_resolution_rate: Option<f64>,
    #[serde(default)]
    pub on_context_tier_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub off_context_tier_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub on_model_tier_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub off_model_tier_counts: BTreeMap<String, usize>,
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

    /// Difference in hit-run rate (on − off). Positive means LisM sees cache hits more often.
    pub fn hit_run_rate_delta(&self) -> Option<f64> {
        Some(self.on_hit_run_rate? - self.off_hit_run_rate?)
    }

    /// Difference in average cost (on − off). Negative means LisM saves money.
    pub fn cost_delta_micros(&self) -> i64 {
        self.on_avg_cost_micros_usd as i64 - self.off_avg_cost_micros_usd as i64
    }

    /// Difference in average input tokens (on - off). Negative means LisM uses fewer tokens.
    pub fn input_token_delta(&self) -> i64 {
        self.on_avg_input_tokens as i64 - self.off_avg_input_tokens as i64
    }

    /// Difference in average uncached input tokens (on - off). Negative means LisM uses less billable input.
    pub fn uncached_input_token_delta(&self) -> i64 {
        self.on_avg_uncached_input_tokens as i64 - self.off_avg_uncached_input_tokens as i64
    }

    /// Difference in average sent prompt chars (on - off). Negative means LisM sends less context.
    pub fn sent_prompt_char_delta(&self) -> i64 {
        self.on_avg_sent_prompt_chars as i64 - self.off_avg_sent_prompt_chars as i64
    }

    /// Difference in average fallback count (on - off). Positive means LisM falls back more often.
    pub fn fallback_count_delta(&self) -> i64 {
        self.on_avg_fallback_count as i64 - self.off_avg_fallback_count as i64
    }

    /// Difference in fallback run rate (on - off). Positive means LisM needs fallback on more runs.
    pub fn fallback_run_rate_delta(&self) -> Option<f64> {
        Some(self.on_fallback_run_rate? - self.off_fallback_run_rate?)
    }

    /// Difference in average hydration hits (on - off). Positive means LisM resolves more typed context.
    pub fn hydration_count_delta(&self) -> i64 {
        self.on_avg_hydration_count as i64 - self.off_avg_hydration_count as i64
    }

    /// Difference in average stale refs (on - off). Positive means LisM carries more stale evidence.
    pub fn stale_ref_count_delta(&self) -> i64 {
        self.on_avg_stale_ref_count as i64 - self.off_avg_stale_ref_count as i64
    }

    /// Difference in average hydration misses (on - off). Positive means LisM leaves more refs unresolved.
    pub fn hydration_ref_missing_delta(&self) -> i64 {
        self.on_avg_hydration_ref_missing as i64 - self.off_avg_hydration_ref_missing as i64
    }

    /// Difference in hydration resolution rate (on - off). Positive means LisM resolves more selectors.
    pub fn hydration_resolution_rate_delta(&self) -> Option<f64> {
        Some(self.on_hydration_resolution_rate? - self.off_hydration_resolution_rate?)
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
                cache_hit_ratio_delta: self.cache_hit_ratio_delta().or(self.hit_run_rate_delta()),
                cost_delta_micros: self.cost_delta_micros(),
                input_token_delta: self.input_token_delta(),
                sent_prompt_char_delta: self.sent_prompt_char_delta(),
                fallback_count_delta: self.fallback_count_delta(),
                fallback_run_rate_delta: self.fallback_run_rate_delta(),
                hydration_count_delta: self.hydration_count_delta(),
                stale_ref_count_delta: self.stale_ref_count_delta(),
                hydration_ref_missing_delta: self.hydration_ref_missing_delta(),
                hydration_resolution_rate_delta: self.hydration_resolution_rate_delta(),
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        let cache_delta = self.cache_hit_ratio_delta().or(self.hit_run_rate_delta());
        let cost_delta = self.cost_delta_micros();

        let no_cache_signal = cache_delta.is_none();
        let no_cost_signal = cost_delta == 0
            && self.on_avg_cost_micros_usd == 0
            && self.off_avg_cost_micros_usd == 0;
        let no_saved_token_signal = self.on_avg_tokens_saved == 0 && self.off_avg_tokens_saved == 0;
        let no_input_signal = self.on_avg_input_tokens == 0 && self.off_avg_input_tokens == 0;
        let no_uncached_input_signal =
            self.on_avg_uncached_input_tokens == 0 && self.off_avg_uncached_input_tokens == 0;
        let no_char_signal =
            self.on_avg_sent_prompt_chars == 0 && self.off_avg_sent_prompt_chars == 0;

        if no_cache_signal
            && no_cost_signal
            && no_saved_token_signal
            && no_input_signal
            && no_uncached_input_signal
            && no_char_signal
        {
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::Inconclusive,
                reason: "No measurable cache, cost, token, or prompt-size signal: both arms have enough runs, but usage metadata is unavailable; collect observability before changing rollout policy".into(),
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                input_token_delta: self.input_token_delta(),
                sent_prompt_char_delta: self.sent_prompt_char_delta(),
                fallback_count_delta: self.fallback_count_delta(),
                fallback_run_rate_delta: self.fallback_run_rate_delta(),
                hydration_count_delta: self.hydration_count_delta(),
                stale_ref_count_delta: self.stale_ref_count_delta(),
                hydration_ref_missing_delta: self.hydration_ref_missing_delta(),
                hydration_resolution_rate_delta: self.hydration_resolution_rate_delta(),
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
                    "LisM degrades cache hit signal ({:+.3}) and increases cost ({:+}μ); recommend ForceOff",
                    cache_delta.unwrap_or(0.0),
                    cost_delta
                )
            } else if cache_hurts {
                format!(
                    "LisM degrades cache hit signal ({:+.3}); recommend ForceOff",
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
                reason: append_quality_signal(reason, self),
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                input_token_delta: self.input_token_delta(),
                sent_prompt_char_delta: self.sent_prompt_char_delta(),
                fallback_count_delta: self.fallback_count_delta(),
                fallback_run_rate_delta: self.fallback_run_rate_delta(),
                hydration_count_delta: self.hydration_count_delta(),
                stale_ref_count_delta: self.stale_ref_count_delta(),
                hydration_ref_missing_delta: self.hydration_ref_missing_delta(),
                hydration_resolution_rate_delta: self.hydration_resolution_rate_delta(),
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        // Clear benefit: LisM improves cache or input volume, while any cost increase stays
        // below the configured hard penalty threshold.
        let cache_helps = cache_delta.map_or(false, |d| d > cache_delta_threshold);
        let cost_within_penalty = cost_delta <= cost_penalty_threshold_micros as i64;
        let uncached_input_delta = self.uncached_input_token_delta();
        let input_delta = self.input_token_delta();
        let input_token_saves = if !no_uncached_input_signal {
            uncached_input_delta < -128
        } else {
            !no_input_signal && input_delta < -128
        };
        let cache_not_hurt = !cache_hurts;

        if cache_helps && cost_within_penalty {
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::ForceOn,
                reason: append_quality_signal(
                    format!(
                        "LisM improves cache hit signal ({:+.3}) and keeps cost delta within threshold ({:+}μ <= {}μ); recommend ForceOn",
                        cache_delta.unwrap_or(0.0),
                        cost_delta,
                        cost_penalty_threshold_micros
                    ),
                    self,
                ),
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                input_token_delta: input_delta,
                sent_prompt_char_delta: self.sent_prompt_char_delta(),
                fallback_count_delta: self.fallback_count_delta(),
                fallback_run_rate_delta: self.fallback_run_rate_delta(),
                hydration_count_delta: self.hydration_count_delta(),
                stale_ref_count_delta: self.stale_ref_count_delta(),
                hydration_ref_missing_delta: self.hydration_ref_missing_delta(),
                hydration_resolution_rate_delta: self.hydration_resolution_rate_delta(),
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        if input_token_saves && cache_not_hurt && cost_within_penalty {
            return LisMRolloutConclusion {
                recommendation: LisMPolicyRecommendation::ForceOn,
                reason: append_quality_signal(
                    format!(
                        "LisM reduces input tokens via lower uncached input ({:+}) and keeps cost delta within threshold ({:+}μ <= {}μ); recommend ForceOn",
                        uncached_input_delta, cost_delta, cost_penalty_threshold_micros
                    ),
                    self,
                ),
                cache_hit_ratio_delta: cache_delta,
                cost_delta_micros: cost_delta,
                input_token_delta: if !no_uncached_input_signal {
                    uncached_input_delta
                } else {
                    input_delta
                },
                sent_prompt_char_delta: self.sent_prompt_char_delta(),
                fallback_count_delta: self.fallback_count_delta(),
                fallback_run_rate_delta: self.fallback_run_rate_delta(),
                hydration_count_delta: self.hydration_count_delta(),
                stale_ref_count_delta: self.stale_ref_count_delta(),
                hydration_ref_missing_delta: self.hydration_ref_missing_delta(),
                hydration_resolution_rate_delta: self.hydration_resolution_rate_delta(),
                on_completion_rate: self.on_completion_rate,
                off_completion_rate: self.off_completion_rate,
            };
        }

        // Mixed or noisy signal → keep session-level Inherit
        LisMRolloutConclusion {
            recommendation: LisMPolicyRecommendation::Inherit,
            reason: append_quality_signal(
                format!(
                    "Mixed signal: cache delta {}, cost delta {}μ — keep per-session Inherit policy",
                    cache_delta.map_or("n/a".into(), |d| format!("{:+.3}", d)),
                    cost_delta
                ),
                self,
            ),
            cache_hit_ratio_delta: cache_delta,
            cost_delta_micros: cost_delta,
            input_token_delta: self.input_token_delta(),
            sent_prompt_char_delta: self.sent_prompt_char_delta(),
            fallback_count_delta: self.fallback_count_delta(),
            fallback_run_rate_delta: self.fallback_run_rate_delta(),
            hydration_count_delta: self.hydration_count_delta(),
            stale_ref_count_delta: self.stale_ref_count_delta(),
            hydration_ref_missing_delta: self.hydration_ref_missing_delta(),
            hydration_resolution_rate_delta: self.hydration_resolution_rate_delta(),
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
    pub input_token_delta: i64,
    pub sent_prompt_char_delta: i64,
    pub fallback_count_delta: i64,
    pub fallback_run_rate_delta: Option<f64>,
    pub hydration_count_delta: i64,
    pub stale_ref_count_delta: i64,
    pub hydration_ref_missing_delta: i64,
    pub hydration_resolution_rate_delta: Option<f64>,
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
            writeln!(f, "  Δ cache_hit_sig  : {:+.3}", d)?;
        }
        writeln!(f, "  Δ cost           : {:+}μ", self.cost_delta_micros)?;
        writeln!(f, "  Δ input tokens   : {:+}", self.input_token_delta)?;
        writeln!(f, "  Δ sent chars     : {:+}", self.sent_prompt_char_delta)?;
        writeln!(f, "  Δ fallback/run   : {:+}", self.fallback_count_delta)?;
        if let Some(d) = self.fallback_run_rate_delta {
            writeln!(f, "  Δ fallback rate  : {:+.3}", d)?;
        }
        writeln!(f, "  Δ hydration hits : {:+}", self.hydration_count_delta)?;
        writeln!(f, "  Δ stale refs     : {:+}", self.stale_ref_count_delta)?;
        writeln!(
            f,
            "  Δ missing refs   : {:+}",
            self.hydration_ref_missing_delta
        )?;
        if let Some(d) = self.hydration_resolution_rate_delta {
            writeln!(f, "  Δ hydration rate : {:+.3}", d)?;
        }
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
    let last_fallback = report.steps.iter().rev().find_map(|step| {
        step.routed_metadata.as_ref().and_then(|meta| {
            if meta.fallback_count.unwrap_or(0) > 0
                || meta.fallback_tier.is_some()
                || meta.fallback_reason.is_some()
            {
                Some((meta.fallback_tier.clone(), meta.fallback_reason.clone()))
            } else {
                None
            }
        })
    });
    let mut toolset_ids = BTreeMap::new();
    let mut visible_tools = BTreeMap::new();
    let mut schema_hashes = BTreeMap::new();
    let mut permission_hashes = BTreeMap::new();
    let mut actor_roles = BTreeMap::new();
    let mut cwd_values = BTreeMap::new();
    let mut config_roots = BTreeMap::new();
    let mut workspace_capabilities = BTreeMap::new();
    let mut tool_contract_mismatch_count = 0usize;
    let mut last_failure_kinds = BTreeMap::new();
    let mut last_recommended_repairs = BTreeMap::new();
    let mut recovery_attempted = false;
    let mut recovery_tiers = BTreeMap::new();
    let mut recovery_outcomes = BTreeMap::new();
    let mut terminal_blocker_kinds = BTreeMap::new();
    let mut missing_artifact_evidence_targets = BTreeMap::new();
    let mut missing_verification_evidence_targets = BTreeMap::new();
    for step in &report.steps {
        let Some(meta) = step.routed_metadata.as_ref() else {
            continue;
        };
        if let Some(toolset_id) = meta.toolset_id.as_ref() {
            toolset_ids.insert(toolset_id.clone(), ());
        }
        for tool in &meta.visible_tools {
            visible_tools.insert(tool.clone(), ());
        }
        if let Some(schema_hash) = meta.schema_hash.as_ref() {
            schema_hashes.insert(schema_hash.clone(), ());
        }
        if let Some(permission_hash) = meta.permission_hash.as_ref() {
            permission_hashes.insert(permission_hash.clone(), ());
        }
        if let Some(actor_role) = meta.actor_role.as_ref() {
            actor_roles.insert(actor_role.clone(), ());
        }
        if let Some(cwd) = meta.cwd.as_ref() {
            cwd_values.insert(cwd.clone(), ());
        }
        if let Some(config_root) = meta.config_root.as_ref() {
            config_roots.insert(config_root.clone(), ());
        }
        for capability in &meta.workspace_capabilities {
            workspace_capabilities.insert(capability.clone(), ());
        }
        tool_contract_mismatch_count += meta.tool_contract_mismatch_count.unwrap_or(0);
        if let Some(kind) = meta.last_failure_kind.as_ref() {
            last_failure_kinds.insert(kind.clone(), ());
        }
        if let Some(repair) = meta.last_recommended_repair.as_ref() {
            last_recommended_repairs.insert(repair.clone(), ());
        }
        recovery_attempted |= meta.recovery_attempted.unwrap_or(false);
        if let Some(tier) = meta.recovery_tier.as_ref() {
            recovery_tiers.insert(tier.clone(), ());
        }
        if let Some(outcome) = meta.recovery_outcome.as_ref() {
            recovery_outcomes.insert(outcome.clone(), ());
        }
        if let Some(kind) = meta.terminal_blocker_kind.as_ref() {
            terminal_blocker_kinds.insert(kind.clone(), ());
        }
        for gap in &meta.completion_evidence_gaps {
            let target = match gap.target_path.as_deref() {
                Some(path) => format!("{}:{path}", gap.target_ref),
                None => gap.target_ref.clone(),
            };
            if gap.missing_artifact_evidence {
                missing_artifact_evidence_targets.insert(target.clone(), ());
            }
            if gap.missing_verification_evidence {
                missing_verification_evidence_targets.insert(target, ());
            }
        }
    }

    LisMAbSampleRecord {
        run_id,
        lism_enabled,
        total_steps,
        completed_steps,
        total_input_tokens: obs.map(|o| o.total_input_tokens).unwrap_or(0),
        total_uncached_input_tokens: obs.map(|o| o.total_uncached_input_tokens).unwrap_or(0),
        total_output_tokens: obs.map(|o| o.total_output_tokens).unwrap_or(0),
        cache_read_tokens: obs.map(|o| o.total_cache_read_tokens).unwrap_or(0),
        cache_write_tokens: obs.map(|o| o.total_cache_write_tokens).unwrap_or(0),
        original_prompt_chars: obs.map(|o| o.total_original_chars).unwrap_or(0),
        sent_prompt_chars: obs.map(|o| o.total_sent_chars).unwrap_or(0),
        cache_hit_observed: obs.map(|o| o.cache_hit_observed()).unwrap_or(false),
        cache_observability_present: obs.is_some(),
        cache_hit_ratio: obs.and_then(|o| o.cache_hit_ratio()),
        estimated_tokens_saved: obs.map(|o| o.estimated_tokens_saved()).unwrap_or(0),
        cost_micros_usd: obs.map(|o| o.estimated_cost_micros_usd).unwrap_or(0),
        fallback_count: obs.map(|o| o.total_fallback_count).unwrap_or(0),
        fallback_tier: last_fallback.as_ref().and_then(|(tier, _)| tier.clone()),
        fallback_reason: last_fallback
            .as_ref()
            .and_then(|(_, reason)| reason.clone()),
        context_tier: derive_context_tier(
            obs,
            last_fallback.as_ref().and_then(|(tier, _)| tier.as_deref()),
        ),
        model_tier_counts: obs
            .map(|o| {
                o.model_tier_counts
                    .iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .collect()
            })
            .unwrap_or_default(),
        hydration_count: obs.map(|o| o.total_hydration_count).unwrap_or(0),
        stale_ref_count: obs.map(|o| o.total_stale_ref_count).unwrap_or(0),
        hydration_ref_missing: obs.map(|o| o.total_hydration_ref_missing).unwrap_or(0),
        tool_dispatch_count: obs.map(|o| o.total_tool_dispatch_count).unwrap_or(0),
        tool_dispatch_failure_count: obs
            .map(|o| o.total_tool_dispatch_failure_count)
            .unwrap_or(0),
        tool_dispatch_ref_write_count: obs
            .map(|o| o.total_tool_dispatch_ref_write_count)
            .unwrap_or(0),
        tool_dispatch_failure_taxonomy: obs
            .map(|o| o.tool_dispatch_failure_taxonomy.clone())
            .unwrap_or_default(),
        toolset_ids: toolset_ids.into_keys().collect(),
        visible_tools: visible_tools.into_keys().collect(),
        schema_hashes: schema_hashes.into_keys().collect(),
        permission_hashes: permission_hashes.into_keys().collect(),
        actor_roles: actor_roles.into_keys().collect(),
        cwd_values: cwd_values.into_keys().collect(),
        config_roots: config_roots.into_keys().collect(),
        workspace_capabilities: workspace_capabilities.into_keys().collect(),
        tool_contract_mismatch_count,
        last_failure_kinds: last_failure_kinds.into_keys().collect(),
        last_recommended_repairs: last_recommended_repairs.into_keys().collect(),
        recovery_attempted,
        recovery_tiers: recovery_tiers.into_keys().collect(),
        recovery_outcomes: recovery_outcomes.into_keys().collect(),
        terminal_blocker_kinds: terminal_blocker_kinds.into_keys().collect(),
        missing_artifact_evidence_targets: missing_artifact_evidence_targets.into_keys().collect(),
        missing_verification_evidence_targets: missing_verification_evidence_targets
            .into_keys()
            .collect(),
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
        on_hit_run_rate: hit_run_rate(&on),
        off_hit_run_rate: hit_run_rate(&off),
        on_avg_cost_micros_usd: avg_cost(&on),
        off_avg_cost_micros_usd: avg_cost(&off),
        on_avg_input_tokens: avg_input_tokens(&on),
        off_avg_input_tokens: avg_input_tokens(&off),
        on_avg_uncached_input_tokens: avg_uncached_input_tokens(&on),
        off_avg_uncached_input_tokens: avg_uncached_input_tokens(&off),
        on_avg_output_tokens: avg_output_tokens(&on),
        off_avg_output_tokens: avg_output_tokens(&off),
        on_avg_cache_read_tokens: avg_cache_read_tokens(&on),
        off_avg_cache_read_tokens: avg_cache_read_tokens(&off),
        on_cache_read_tokens_distribution: cache_read_distribution(&on),
        off_cache_read_tokens_distribution: cache_read_distribution(&off),
        on_avg_cache_write_tokens: avg_cache_write_tokens(&on),
        off_avg_cache_write_tokens: avg_cache_write_tokens(&off),
        on_avg_sent_prompt_chars: avg_sent_prompt_chars(&on),
        off_avg_sent_prompt_chars: avg_sent_prompt_chars(&off),
        on_avg_tokens_saved: avg_tokens_saved(&on),
        off_avg_tokens_saved: avg_tokens_saved(&off),
        on_avg_fallback_count: avg_fallback_count(&on),
        off_avg_fallback_count: avg_fallback_count(&off),
        on_fallback_run_rate: fallback_run_rate(&on),
        off_fallback_run_rate: fallback_run_rate(&off),
        on_avg_hydration_count: avg_hydration_count(&on),
        off_avg_hydration_count: avg_hydration_count(&off),
        on_avg_stale_ref_count: avg_stale_ref_count(&on),
        off_avg_stale_ref_count: avg_stale_ref_count(&off),
        on_avg_hydration_ref_missing: avg_hydration_ref_missing(&on),
        off_avg_hydration_ref_missing: avg_hydration_ref_missing(&off),
        on_avg_tool_dispatch_count: avg_tool_dispatch_count(&on),
        off_avg_tool_dispatch_count: avg_tool_dispatch_count(&off),
        on_avg_tool_dispatch_failure_count: avg_tool_dispatch_failure_count(&on),
        off_avg_tool_dispatch_failure_count: avg_tool_dispatch_failure_count(&off),
        on_avg_tool_dispatch_ref_write_count: avg_tool_dispatch_ref_write_count(&on),
        off_avg_tool_dispatch_ref_write_count: avg_tool_dispatch_ref_write_count(&off),
        on_hydration_resolution_rate: hydration_resolution_rate(&on),
        off_hydration_resolution_rate: hydration_resolution_rate(&off),
        on_context_tier_counts: context_tier_counts(&on),
        off_context_tier_counts: context_tier_counts(&off),
        on_model_tier_counts: aggregate_model_tier_counts(&on),
        off_model_tier_counts: aggregate_model_tier_counts(&off),
        on_completion_rate: completion_rate(&on),
        off_completion_rate: completion_rate(&off),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::boss_state::{
        BossActorHandle, BossActorRole, BossPlanStepStatus, BossReportPayload, BossStage,
        BossStepReport, BossStepRoutedMetadata,
    };
    use crate::core::boss_test_readiness::BossTestRunOutcome;
    use crate::core::state_frame::CompletionEvidenceGap;

    fn empty_actor() -> BossActorHandle {
        BossActorHandle::new("actor", "session", BossActorRole::DesignerA)
    }

    #[test]
    fn ab_sample_records_exact_verification_gap_target() {
        let report = BossReportPayload {
            stage: BossStage::Execution,
            current_step: Some(1),
            total_steps: Some(1),
            designer_a: empty_actor(),
            executor_b: empty_actor(),
            active_children: Vec::new(),
            steps: vec![BossStepReport {
                id: 1,
                status: BossPlanStepStatus::Rejected,
                worker_task_id: None,
                attempt_count: 1,
                last_review_summary: None,
                action_required: None,
                blocker_reason: None,
                routed_metadata: Some(BossStepRoutedMetadata {
                    completion_evidence_gaps: vec![CompletionEvidenceGap {
                        target_ref: "artifact:contract:1".into(),
                        target_path: Some("/tmp/report.md".into()),
                        missing_artifact_evidence: false,
                        missing_test_evidence: false,
                        missing_verification_evidence: true,
                        recommended_action: "verify_artifact".into(),
                    }],
                    ..BossStepRoutedMetadata::default()
                }),
            }],
            history_summary: Vec::new(),
            observability_summary: None,
            lism_policy: Default::default(),
        };

        let record = build_ab_record(
            "run-1".into(),
            true,
            &report,
            BossTestRunOutcome::Completed,
            0,
        );

        assert_eq!(
            record.missing_verification_evidence_targets,
            vec!["artifact:contract:1:/tmp/report.md".to_string()]
        );
        assert!(record.missing_artifact_evidence_targets.is_empty());
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

fn avg_input_tokens(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.total_input_tokens)
}

fn avg_uncached_input_tokens(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.total_uncached_input_tokens)
}

fn avg_output_tokens(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.total_output_tokens)
}

fn avg_cache_read_tokens(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.cache_read_tokens)
}

fn avg_cache_write_tokens(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.cache_write_tokens)
}

fn avg_sent_prompt_chars(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.sent_prompt_chars)
}

fn avg_tokens_saved(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.estimated_tokens_saved)
}

fn avg_fallback_count(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.fallback_count)
}

fn avg_hydration_count(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.hydration_count)
}

fn avg_stale_ref_count(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.stale_ref_count)
}

fn avg_hydration_ref_missing(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.hydration_ref_missing)
}

fn avg_tool_dispatch_count(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.tool_dispatch_count)
}

fn avg_tool_dispatch_failure_count(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.tool_dispatch_failure_count)
}

fn avg_tool_dispatch_ref_write_count(records: &[&LisMAbSampleRecord]) -> usize {
    avg_usize(records, |r| r.tool_dispatch_ref_write_count)
}

fn hit_run_rate(records: &[&LisMAbSampleRecord]) -> Option<f64> {
    let with_cache_obs: Vec<&LisMAbSampleRecord> = records
        .iter()
        .copied()
        .filter(|r| r.cache_observability_present)
        .collect();
    if with_cache_obs.is_empty() {
        return None;
    }
    let hits = with_cache_obs
        .iter()
        .filter(|r| r.cache_hit_observed)
        .count();
    Some(hits as f64 / with_cache_obs.len() as f64)
}

fn fallback_run_rate(records: &[&LisMAbSampleRecord]) -> Option<f64> {
    if records.is_empty() {
        return None;
    }
    let runs_with_fallback = records
        .iter()
        .filter(|r| r.fallback_count > 0 || r.fallback_tier.is_some())
        .count();
    Some(runs_with_fallback as f64 / records.len() as f64)
}

fn hydration_resolution_rate(records: &[&LisMAbSampleRecord]) -> Option<f64> {
    let hydrated: usize = records.iter().map(|r| r.hydration_count).sum();
    let missing: usize = records.iter().map(|r| r.hydration_ref_missing).sum();
    let total = hydrated + missing;
    if total == 0 {
        None
    } else {
        Some(hydrated as f64 / total as f64)
    }
}

fn cache_read_distribution(records: &[&LisMAbSampleRecord]) -> Option<TokenDistributionStats> {
    if records.is_empty() {
        return None;
    }
    let mut values: Vec<usize> = records.iter().map(|r| r.cache_read_tokens).collect();
    values.sort_unstable();
    let len = values.len();
    let p50_idx = percentile_index(len, 0.50);
    let p90_idx = percentile_index(len, 0.90);
    Some(TokenDistributionStats {
        sample_count: len,
        nonzero_count: values.iter().filter(|&&v| v > 0).count(),
        p50: values[p50_idx],
        p90: values[p90_idx],
        max: *values.last().unwrap_or(&0),
    })
}

fn avg_usize(
    records: &[&LisMAbSampleRecord],
    field: impl Fn(&LisMAbSampleRecord) -> usize,
) -> usize {
    if records.is_empty() {
        return 0;
    }
    let total: usize = records.iter().map(|r| field(r)).sum();
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

fn context_tier_counts(records: &[&LisMAbSampleRecord]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record.context_tier.clone()).or_insert(0) += 1;
    }
    counts
}

fn aggregate_model_tier_counts(records: &[&LisMAbSampleRecord]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for record in records {
        for (tier, count) in &record.model_tier_counts {
            *counts.entry(tier.clone()).or_insert(0) += *count;
        }
    }
    counts
}

fn derive_context_tier(
    obs: Option<&crate::core::boss_state::BossObservabilitySummary>,
    fallback_tier: Option<&str>,
) -> String {
    if let Some(tier) = fallback_tier {
        return format!("fallback:{tier}");
    }
    match obs {
        Some(summary) if summary.total_hydration_count > 0 => "typed_hydration".into(),
        Some(_) => "state_frame_only".into(),
        None => "no_observability".into(),
    }
}

fn append_quality_signal(reason: String, summary: &LisMAbSummary) -> String {
    let fallback_delta = summary.fallback_count_delta();
    let missing_delta = summary.hydration_ref_missing_delta();
    let stale_delta = summary.stale_ref_count_delta();
    let resolution_delta = summary.hydration_resolution_rate_delta();
    if fallback_delta == 0 && missing_delta == 0 && stale_delta == 0 && resolution_delta.is_none() {
        return reason;
    }
    let resolution_note = resolution_delta
        .map(|d| format!("{:+.3}", d))
        .unwrap_or_else(|| "n/a".into());
    format!(
        "{reason}; context quality deltas fallback/run {:+}, missing_refs {:+}, stale_refs {:+}, hydration_rate {}",
        fallback_delta, missing_delta, stale_delta, resolution_note
    )
}

fn percentile_index(len: usize, percentile: f64) -> usize {
    if len <= 1 {
        return 0;
    }
    let rank = (percentile * len as f64).ceil() as usize;
    rank.saturating_sub(1).min(len - 1)
}

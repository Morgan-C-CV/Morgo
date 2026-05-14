use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::core::boss_state::BossReportPayload;
use crate::core::boss_test_readiness::{
    BossRollbackPolicy, BossTestRunOutcome, BossTestSampleRecord, evaluate_rollback_triggers,
};

// ── Sample sink ───────────────────────────────────────────────────────────────

/// Collects `BossTestSampleRecord`s from live boss runs and optionally persists
/// them to a JSONL file for post-run analysis.
///
/// Thread-safe: the inner state is behind a `Mutex` so the sink can be shared
/// across async tasks via `Arc<BossTestSampleSink>`.
pub struct BossTestSampleSink {
    inner: Mutex<SinkInner>,
}

struct SinkInner {
    records: Vec<BossTestSampleRecord>,
    writer: Option<BufWriter<File>>,
    path: Option<PathBuf>,
}

impl std::fmt::Debug for BossTestSampleSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("BossTestSampleSink")
            .field("record_count", &inner.records.len())
            .field("path", &inner.path)
            .finish()
    }
}

impl BossTestSampleSink {
    /// In-memory only — no file persistence.
    pub fn in_memory() -> Self {
        Self {
            inner: Mutex::new(SinkInner {
                records: Vec::new(),
                writer: None,
                path: None,
            }),
        }
    }

    /// Persist each record to `path` as newline-delimited JSON (JSONL).
    /// Creates the file if it does not exist; appends if it does.
    pub fn with_jsonl_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            inner: Mutex::new(SinkInner {
                records: Vec::new(),
                writer: Some(BufWriter::new(file)),
                path: Some(path),
            }),
        })
    }

    /// All records collected so far (in-memory copy).
    pub fn records(&self) -> Vec<BossTestSampleRecord> {
        self.inner.lock().unwrap().records.clone()
    }

    /// Number of records collected.
    pub fn record_count(&self) -> usize {
        self.inner.lock().unwrap().records.len()
    }

    /// Path to the JSONL file, if configured.
    pub fn path(&self) -> Option<PathBuf> {
        self.inner.lock().unwrap().path.clone()
    }

    // ── record helpers ────────────────────────────────────────────────────────

    /// Record a successfully completed boss run.
    pub fn record_run_complete(
        &self,
        run_id: impl Into<String>,
        report: &BossReportPayload,
        policy: &BossRollbackPolicy,
        skill_names: Vec<String>,
        mcp_server_names: Vec<String>,
        mcp_failure_count: usize,
        pending_approval_count: usize,
    ) {
        let record = build_sample_record(
            run_id.into(),
            report,
            policy,
            skill_names,
            mcp_server_names,
            mcp_failure_count,
            pending_approval_count,
            BossTestRunOutcome::Completed,
        );
        self.push(record);
    }

    /// Record a boss run that was aborted (user-initiated stop or fatal error).
    pub fn record_run_aborted(
        &self,
        run_id: impl Into<String>,
        report: &BossReportPayload,
        policy: &BossRollbackPolicy,
        skill_names: Vec<String>,
        mcp_server_names: Vec<String>,
        mcp_failure_count: usize,
        pending_approval_count: usize,
    ) {
        let record = build_sample_record(
            run_id.into(),
            report,
            policy,
            skill_names,
            mcp_server_names,
            mcp_failure_count,
            pending_approval_count,
            BossTestRunOutcome::Aborted,
        );
        self.push(record);
    }

    /// Record a boss run that was rolled back by policy triggers.
    pub fn record_run_rolled_back(
        &self,
        run_id: impl Into<String>,
        report: &BossReportPayload,
        policy: &BossRollbackPolicy,
        skill_names: Vec<String>,
        mcp_server_names: Vec<String>,
        mcp_failure_count: usize,
        pending_approval_count: usize,
    ) {
        let record = build_sample_record(
            run_id.into(),
            report,
            policy,
            skill_names,
            mcp_server_names,
            mcp_failure_count,
            pending_approval_count,
            BossTestRunOutcome::RolledBack,
        );
        self.push(record);
    }

    fn push(&self, record: BossTestSampleRecord) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(writer) = &mut inner.writer {
            if let Ok(line) = serde_json::to_string(&record) {
                let _ = writeln!(writer, "{line}");
                let _ = writer.flush();
            }
        }
        inner.records.push(record);
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

fn build_sample_record(
    run_id: String,
    report: &BossReportPayload,
    policy: &BossRollbackPolicy,
    skill_names: Vec<String>,
    mcp_server_names: Vec<String>,
    mcp_failure_count: usize,
    pending_approval_count: usize,
    outcome: BossTestRunOutcome,
) -> BossTestSampleRecord {
    let obs = report.observability_summary.as_ref();

    let cost_micros_usd = obs.map(|o| o.estimated_cost_micros_usd).unwrap_or(0);
    let cache_hit_ratio = obs.and_then(|o| o.cache_hit_ratio());
    let estimated_tokens_saved = obs.map(|o| o.estimated_tokens_saved()).unwrap_or(0);

    let total_steps = report.total_steps.unwrap_or(report.steps.len());
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

    let provider_profile = report
        .steps
        .iter()
        .find_map(|s| s.routed_metadata.as_ref()?.provider_profile_id.clone());

    let triggers = evaluate_rollback_triggers(
        policy,
        mcp_failure_count > 0,
        cost_micros_usd,
        cache_hit_ratio,
        pending_approval_count > 0,
    );
    let rollback_triggers: Vec<String> = triggers.iter().map(|t| t.as_str().to_string()).collect();

    BossTestSampleRecord {
        run_id,
        provider_profile,
        skill_names,
        mcp_server_names,
        total_steps,
        completed_steps,
        cost_micros_usd,
        cache_hit_ratio,
        estimated_tokens_saved,
        fallback_count: obs.map(|o| o.total_fallback_count).unwrap_or(0),
        fallback_tier: last_fallback.as_ref().and_then(|(tier, _)| tier.clone()),
        fallback_reason: last_fallback
            .as_ref()
            .and_then(|(_, reason)| reason.clone()),
        mcp_failure_count,
        pending_approval_count,
        rollback_triggers,
        outcome,
    }
}

// ── Shared sink type alias ────────────────────────────────────────────────────

pub type SharedBossTestSampleSink = Arc<BossTestSampleSink>;

pub fn new_shared_sink() -> SharedBossTestSampleSink {
    Arc::new(BossTestSampleSink::in_memory())
}

pub fn new_shared_sink_with_path(
    path: impl AsRef<Path>,
) -> anyhow::Result<SharedBossTestSampleSink> {
    Ok(Arc::new(BossTestSampleSink::with_jsonl_path(path)?))
}

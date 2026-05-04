use crate::core::state_frame::{
    CompletionEvidenceGap, StageContinuationContext, StageExecutionContract, WorkerStructuredReport,
};
use crate::core::state_frame_orchestrator::StepFailureClassification;
use crate::tool::registry::ToolContractMismatch;
use crate::tool::result::ToolExecutionRecord;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// How the outbound B context was compressed before dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionStrategy {
    /// No compression — message was within budget.
    #[default]
    None,
    /// LLM summarize path (stateless provider call).
    Summarized,
    /// Tail-trim path (pure char truncation).
    Trimmed,
}

/// Which context assembly mode was used for the B dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ContextMode {
    /// Full conversation history inherited (legacy / escape hatch).
    FullInherit,
    /// BossContextBrief + BossStateFrame (default).
    #[default]
    Brief,
    /// StateFrame only (minimal context).
    StateFrame,
}

/// Per-dispatch observability record written by ask_b_session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BossStepMetrics {
    pub compression_strategy: CompressionStrategy,
    pub context_mode: ContextMode,
    /// Char length of the message before any compression.
    pub original_chars: usize,
    /// Char length of the message actually sent to B.
    pub sent_chars: usize,
    /// Cache creation tokens from provider usage (0 if not yet reported by B actor).
    #[serde(default)]
    pub cache_creation_tokens: usize,
    /// Cache read tokens from provider usage (0 if not yet reported by B actor).
    #[serde(default)]
    pub cache_read_tokens: usize,
    /// True if the cacheable prefix fingerprint changed unexpectedly during this dispatch.
    #[serde(default)]
    pub cache_prefix_instability: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BossStage {
    #[default]
    /// Planning and discussion stage (Agent A & B in a documentation loop)
    Documentation,
    /// Waiting for user confirmation to proceed from Planning to Execution
    WaitingForApproval,
    /// Implementation stage (Agent B executing tasks, Agent A reviewing)
    Execution,
    /// Final review or completion
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BossStatus {
    pub stage: BossStage,
    pub current_step: Option<usize>,
    pub total_steps: Option<usize>,
    /// Path to the immutable planning file
    pub planning_file: Option<String>,
    /// Last payload dispatched to B's execution callback — observable for tests.
    #[serde(default)]
    pub last_b_dispatch_payload: Option<String>,
    /// Last message sent to A's LLM session via Continue — observable for tests.
    #[serde(default)]
    pub last_a_dispatch_message: Option<String>,
    /// Last outbound message sent to B via ask_b_session (after trim/summarize) — observable for tests.
    #[serde(default)]
    pub last_b_ask_message: Option<String>,
    /// Per-dispatch observability record — compression strategy + context mode + char counts.
    #[serde(default)]
    pub last_step_metrics: Option<BossStepMetrics>,
    /// Ensures the LisM A/B sample is emitted at most once per boss run.
    #[serde(default)]
    pub lism_sample_emitted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BossPlan {
    #[serde(default)]
    pub plan_id: String,
    pub task_description: String,
    pub document_spec: String,
    pub pseudo_code: String,
    #[serde(default)]
    pub draft_spec: Option<String>,
    #[serde(default)]
    pub review_feedback: Option<String>,
    #[serde(default)]
    pub revision_notes: Option<String>,
    #[serde(default)]
    pub finalized: bool,
    #[serde(default)]
    pub documentation_feedback: Vec<String>,
    pub steps: Vec<BossPlanStep>,
    pub accepted_by_user: bool,
    pub auto_sequence: bool,
    /// Persisted A/B session identity snapshot.
    /// Restored on `/boss` re-entry so A/B task_id / session_id survive restart.
    /// Liveness (whether the task is still running) is NOT guaranteed — callers
    /// must perform a live-task check before reusing the stored task_id.
    #[serde(default)]
    pub session_snapshot: Option<BossSession>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BossPlanStepStatus {
    #[default]
    Pending,
    Running,
    WaitingForApproval,
    /// B's fan-in completed; waiting for A's review verdict.
    Reviewing,
    /// A rejected the step output; B will retry with a correction.
    Rejected,
    /// A determined the step needs replanning before any further dispatch.
    ReplanRequired,
    Completed,
    Failed,
}

impl BossPlanStepStatus {
    pub fn is_terminal_failure(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BossPlanStep {
    pub id: usize,
    pub description: String,
    #[serde(default)]
    pub objective: Option<String>,
    #[serde(default)]
    pub acceptance: Vec<String>,
    #[serde(default)]
    pub requires_approval: bool,
    #[serde(default)]
    pub status: BossPlanStepStatus,
    pub completed: bool,
    pub result_diff: Option<String>,
    pub worker_task_id: Option<String>,
    /// How many times B has attempted this step (incremented on each dispatch).
    #[serde(default)]
    pub attempt_count: u32,
    /// Maximum number of B attempts before the step is marked Failed.
    #[serde(default = "default_retry_budget")]
    pub retry_budget: u32,
    /// Summary from A's last review (populated on accept or reject).
    #[serde(default)]
    pub last_review_summary: Option<String>,
    /// Correction message from A sent back to B on rejection.
    #[serde(default)]
    pub last_correction: Option<String>,
    /// Typed continuation source for reject / repair / continue flows.
    #[serde(default)]
    pub stage_continuation_context: Option<StageContinuationContext>,
    /// Typed step-local execution memory for the current ExecutorB stage.
    #[serde(default)]
    pub executor_b_stage_memory: Option<ExecutorBStageMemory>,
    /// Task id of the A review agent currently reviewing this step.
    #[serde(default)]
    pub review_task_id: Option<String>,
    /// Real runtime tool records captured from the latest worker attempt.
    #[serde(default)]
    pub tool_execution_records: Vec<ToolExecutionRecord>,
}

fn default_retry_budget() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorBStageMemoryContinuity {
    ReuseWithinStep,
    FreshStep,
    VerificationFirstIsolated,
    FullWorkerDispatchReuse,
    FullWorkerDispatchFresh,
    FullContextReuse,
    FullContextFresh,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ExecutorBStageMemory {
    #[serde(default)]
    pub recent_reads: Vec<String>,
    #[serde(default)]
    pub recent_edits: Vec<String>,
    #[serde(default)]
    pub recent_test_refs: Vec<String>,
    #[serde(default)]
    pub recent_verification_refs: Vec<String>,
    #[serde(default)]
    pub failed_targets: Vec<String>,
    #[serde(default)]
    pub verified_targets: Vec<String>,
    #[serde(default)]
    pub continuity: Option<ExecutorBStageMemoryContinuity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BossStepRoutedMetadata {
    #[serde(default)]
    pub toolset_id: Option<String>,
    #[serde(default)]
    pub skillset_id: Option<String>,
    #[serde(default)]
    pub model_tier: Option<String>,
    #[serde(default)]
    pub provider_profile_id: Option<String>,
    #[serde(default)]
    pub state_frame_size: Option<usize>,
    #[serde(default)]
    pub cache_read_tokens: Option<usize>,
    #[serde(default)]
    pub cache_write_tokens: Option<usize>,
    #[serde(default)]
    pub fallback_count: Option<usize>,
    #[serde(default)]
    pub fallback_tier: Option<String>,
    #[serde(default)]
    pub fallback_reason: Option<String>,
    #[serde(default)]
    pub projection_mismatch_count: Option<usize>,
    #[serde(default)]
    pub hydration_count: Option<usize>,
    #[serde(default)]
    pub hydration_from_contract_count: Option<usize>,
    #[serde(default)]
    pub hydration_from_ledger_count: Option<usize>,
    #[serde(default)]
    pub stale_ref_count: Option<usize>,
    #[serde(default)]
    pub hydration_ref_missing: Option<usize>,
    #[serde(default)]
    pub hydration_miss_unsupported_count: Option<usize>,
    #[serde(default)]
    pub hydration_miss_stale_count: Option<usize>,
    #[serde(default)]
    pub hydration_miss_no_match_count: Option<usize>,
    #[serde(default)]
    pub tool_dispatch_count: Option<usize>,
    #[serde(default)]
    pub tool_dispatch_success_count: Option<usize>,
    #[serde(default)]
    pub tool_dispatch_failure_count: Option<usize>,
    #[serde(default)]
    pub tool_dispatch_ref_write_count: Option<usize>,
    #[serde(default)]
    pub tool_dispatch_failure_taxonomy: std::collections::BTreeMap<String, usize>,
    /// Total input tokens billed for this step (v1 stub: always 0).
    #[serde(default)]
    pub input_tokens: Option<usize>,
    /// Total uncached input tokens billed at full price for this step.
    #[serde(default)]
    pub uncached_input_tokens: Option<usize>,
    /// Total output tokens billed for this step (v1 stub: always 0).
    #[serde(default)]
    pub output_tokens: Option<usize>,
    /// Original prompt chars before compression/context assembly, when known.
    #[serde(default)]
    pub original_prompt_chars: Option<usize>,
    /// Actual prompt chars sent to the provider, when known.
    #[serde(default)]
    pub sent_prompt_chars: Option<usize>,
    /// Estimated cost in micros USD for this routed step.
    #[serde(default)]
    pub estimated_cost_micros_usd: Option<u64>,
    #[serde(default)]
    pub visible_tools: Vec<String>,
    #[serde(default)]
    pub allowed_actions: Vec<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub permission_hash: Option<String>,
    #[serde(default)]
    pub actor_role: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub config_root: Option<String>,
    #[serde(default)]
    pub workspace_capabilities: Vec<String>,
    #[serde(default)]
    pub tool_contract_mismatch_count: Option<usize>,
    #[serde(default)]
    pub tool_contract_mismatch: Option<ToolContractMismatch>,
    #[serde(default)]
    pub last_effective_tool_action: Option<String>,
    #[serde(default)]
    pub last_failure_kind: Option<String>,
    #[serde(default)]
    pub last_failure_recoverable: Option<bool>,
    #[serde(default)]
    pub last_recommended_repair: Option<String>,
    #[serde(default)]
    pub last_failure_evidence_ref: Option<String>,
    #[serde(default)]
    pub last_failure_bounded_excerpt: Option<String>,
    #[serde(default)]
    pub last_failure_truncated: Option<bool>,
    #[serde(default)]
    pub recovery_attempted: Option<bool>,
    #[serde(default)]
    pub recovery_tier: Option<String>,
    #[serde(default)]
    pub recovery_outcome: Option<String>,
    #[serde(default)]
    pub terminal_blocker_kind: Option<String>,
    #[serde(default)]
    pub step_failure_classification: Option<StepFailureClassification>,
    #[serde(default)]
    pub completion_evidence_status: Option<String>,
    #[serde(default)]
    pub completion_evidence_gaps: Vec<CompletionEvidenceGap>,
    #[serde(default)]
    pub worker_report: Option<WorkerStructuredReport>,
    #[serde(default)]
    pub success_classification: Option<BossSuccessClassification>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BossSuccessClassification {
    DirectSuccess,
    RecoveredSuccess,
    FallbackSuccess,
    FullWorkerDispatchSuccess,
    TrueExternalBlocker,
}

impl BossSuccessClassification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DirectSuccess => "direct_success",
            Self::RecoveredSuccess => "recovered_success",
            Self::FallbackSuccess => "fallback_success",
            Self::FullWorkerDispatchSuccess => "full_worker_dispatch_success",
            Self::TrueExternalBlocker => "true_external_blocker",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BossRolloutTargetDecision {
    pub target_ref: String,
    #[serde(default)]
    pub target_path: Option<String>,
    #[serde(default)]
    pub missing_evidence_kinds: Vec<String>,
    pub recommended_policy: String,
    pub recommended_fallback: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BossRolloutPolicyDecision {
    #[serde(default)]
    pub denylist_targets: Vec<BossRolloutTargetDecision>,
    #[serde(default)]
    pub fallback_targets: Vec<BossRolloutTargetDecision>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BossStepReport {
    pub id: usize,
    pub status: BossPlanStepStatus,
    pub worker_task_id: Option<String>,
    pub attempt_count: u32,
    pub last_review_summary: Option<String>,
    #[serde(default)]
    pub action_required: Option<String>,
    #[serde(default)]
    pub blocker_reason: Option<String>,
    #[serde(default)]
    pub routed_metadata: Option<BossStepRoutedMetadata>,
    #[serde(default)]
    pub stage_execution_contract: StageExecutionContract,
    #[serde(default)]
    pub stage_continuation_context: Option<StageContinuationContext>,
    #[serde(default)]
    pub executor_b_stage_memory: Option<ExecutorBStageMemory>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BossObservabilitySummary {
    pub total_steps_routed: usize,
    pub total_cache_read_tokens: usize,
    pub total_cache_write_tokens: usize,
    pub total_fallback_count: usize,
    #[serde(default)]
    pub fallback_tier_counts: std::collections::HashMap<String, usize>,
    #[serde(default)]
    pub fallback_reason_counts: std::collections::HashMap<String, usize>,
    pub total_projection_mismatch_count: usize,
    #[serde(default)]
    pub total_hydration_count: usize,
    #[serde(default)]
    pub total_hydration_from_contract_count: usize,
    #[serde(default)]
    pub total_hydration_from_ledger_count: usize,
    #[serde(default)]
    pub total_stale_ref_count: usize,
    #[serde(default)]
    pub total_hydration_ref_missing: usize,
    #[serde(default)]
    pub total_hydration_miss_unsupported_count: usize,
    #[serde(default)]
    pub total_hydration_miss_stale_count: usize,
    #[serde(default)]
    pub total_hydration_miss_no_match_count: usize,
    #[serde(default)]
    pub total_tool_dispatch_count: usize,
    #[serde(default)]
    pub total_tool_dispatch_success_count: usize,
    #[serde(default)]
    pub total_tool_dispatch_failure_count: usize,
    #[serde(default)]
    pub total_tool_dispatch_ref_write_count: usize,
    #[serde(default)]
    pub tool_dispatch_failure_taxonomy: std::collections::BTreeMap<String, usize>,
    /// Steps where provider_profile_id is Some (i.e. a non-inherited model profile was used).
    pub override_hit_count: usize,
    pub model_tier_counts: std::collections::HashMap<String, usize>,
    /// Total input tokens across all routed steps (v1 stub: always 0).
    #[serde(default)]
    pub total_input_tokens: usize,
    /// Total uncached input tokens across all routed steps.
    #[serde(default)]
    pub total_uncached_input_tokens: usize,
    /// Total output tokens across all routed steps (v1 stub: always 0).
    #[serde(default)]
    pub total_output_tokens: usize,
    /// Estimated cost in micros USD across all routed steps (v1 stub: always 0).
    #[serde(default)]
    pub estimated_cost_micros_usd: u64,
    /// Original outbound prompt/message chars before compression, when known.
    #[serde(default)]
    pub total_original_chars: usize,
    /// Actual outbound prompt/message chars after compression/context assembly, when known.
    #[serde(default)]
    pub total_sent_chars: usize,
}

impl BossObservabilitySummary {
    /// Whether any cache-read tokens were observed during the run.
    pub fn cache_hit_observed(&self) -> bool {
        self.total_cache_read_tokens > 0
    }

    /// Cache hit ratio: cache_read / (cache_read + cache_write).
    /// Returns None when both are 0 (no cache data available yet).
    pub fn cache_hit_ratio(&self) -> Option<f64> {
        let total = self.total_cache_read_tokens + self.total_cache_write_tokens;
        if total == 0 {
            None
        } else {
            Some(self.total_cache_read_tokens as f64 / total as f64)
        }
    }

    /// Tokens served from cache instead of being re-processed.
    /// Each cache-read token represents one full input token of compute saved.
    pub fn estimated_tokens_saved(&self) -> usize {
        self.total_cache_read_tokens
    }

    /// Fraction of routed steps that escalated beyond typed-only context.
    pub fn fallback_step_rate(&self) -> Option<f64> {
        if self.total_steps_routed == 0 {
            None
        } else {
            let fallback_steps: usize = self.fallback_tier_counts.values().sum();
            Some(fallback_steps as f64 / self.total_steps_routed as f64)
        }
    }

    /// Fraction of typed hydration selectors that resolved to concrete evidence.
    pub fn hydration_resolution_rate(&self) -> Option<f64> {
        let total = self.total_hydration_count + self.total_hydration_ref_missing;
        if total == 0 {
            None
        } else {
            Some(self.total_hydration_count as f64 / total as f64)
        }
    }

    /// Fraction of hydrated refs that were stale after resolution.
    pub fn stale_ref_rate(&self) -> Option<f64> {
        let total = self.total_hydration_count + self.total_stale_ref_count;
        if total == 0 {
            None
        } else {
            Some(self.total_stale_ref_count as f64 / total as f64)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BossReportPayload {
    pub stage: BossStage,
    pub current_step: Option<usize>,
    pub total_steps: Option<usize>,
    pub designer_a: BossActorHandle,
    pub executor_b: BossActorHandle,
    pub active_children: Vec<BossActorHandle>,
    pub steps: Vec<BossStepReport>,
    #[serde(default)]
    pub history_summary: Vec<String>,
    #[serde(default)]
    pub observability_summary: Option<BossObservabilitySummary>,
    #[serde(default)]
    pub rollout_policy_decision: Option<BossRolloutPolicyDecision>,
    #[serde(default)]
    pub success_classification: Option<BossSuccessClassification>,
    #[serde(default)]
    pub lism_policy: BossLisMPolicy,
    #[serde(default)]
    pub stage_execution_contract: StageExecutionContract,
    #[serde(default)]
    pub stage_continuation_context: Option<StageContinuationContext>,
    #[serde(default)]
    pub executor_b_stage_memory: Option<ExecutorBStageMemory>,
}

impl BossReportPayload {
    pub fn derive_success_classification_from_steps(
        steps: &[BossStepReport],
    ) -> Option<BossSuccessClassification> {
        let last_metadata = steps
            .iter()
            .rev()
            .find_map(|step| step.routed_metadata.as_ref())?;
        last_metadata.success_classification
    }

    pub fn format_report(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "stage={:?} step={}/{} success={} ",
            self.stage,
            self.current_step
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into()),
            self.total_steps
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into()),
            self.success_classification
                .as_ref()
                .map(|classification| classification.as_str())
                .unwrap_or("-"),
        ));
        for step in &self.steps {
            let m = step.routed_metadata.as_ref();
            let worker_report = m.and_then(|m| m.worker_report.as_ref());
            lines.push(format!(
                "  step {:>3}: status={:?} failure_class={} tier={} profile={} frame={}B cache_r={} cache_w={} input={} uncached_input={} output={} sent_chars={} original_chars={} fb={} fb_tier={} fb_reason={} mm={} hydr={} stale={} miss={} worker_state={} artifact={} test={} verify={} gaps={}",
                step.id,
                step.status,
                m.and_then(|m| m.step_failure_classification.as_ref())
                    .map(|classification| classification.as_str())
                    .unwrap_or("-"),
                m.and_then(|m| m.model_tier.as_deref()).unwrap_or("-"),
                m.and_then(|m| m.provider_profile_id.as_deref()).unwrap_or("-"),
                m.and_then(|m| m.state_frame_size).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.cache_read_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.cache_write_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.input_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.uncached_input_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.output_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.sent_prompt_chars).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.original_prompt_chars).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.fallback_count).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.fallback_tier.as_deref()).unwrap_or("-"),
                m.and_then(|m| m.fallback_reason.as_deref()).unwrap_or("-"),
                m.and_then(|m| m.projection_mismatch_count).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.hydration_count).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.stale_ref_count).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.hydration_ref_missing).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                worker_report
                    .map(|report| format!("{:?}", report.worker_state).to_ascii_lowercase())
                    .unwrap_or_else(|| "-".into()),
                worker_report
                    .map(|report| report.artifact_status.clone())
                    .unwrap_or_else(|| "-".into()),
                worker_report
                    .map(|report| report.test_status.clone())
                    .unwrap_or_else(|| "-".into()),
                worker_report
                    .map(|report| report.verification_status.clone())
                    .unwrap_or_else(|| "-".into()),
                m.map(|meta| meta.completion_evidence_gaps.len().to_string())
                    .unwrap_or_else(|| "-".into()),
            ));
            if !step.stage_execution_contract.declared_artifacts.is_empty()
                || !step.stage_execution_contract.verifications.is_empty()
                || !step.stage_execution_contract.tests.is_empty()
            {
                lines.push(format!(
                    "    contract: declared_artifacts={} verifications={} tests={} required_actions={} required_evidence={}",
                    step.stage_execution_contract.declared_artifacts.len(),
                    step.stage_execution_contract.verifications.len(),
                    step.stage_execution_contract.tests.len(),
                    step.stage_execution_contract.required_actions.join("|"),
                    step.stage_execution_contract.required_evidence.join("|"),
                ));
            }
        }
        if let Some(policy) = &self.rollout_policy_decision {
            lines.push(format!(
                "rollout_policy denylist_targets={} fallback_targets={} summary={}",
                policy.denylist_targets.len(),
                policy.fallback_targets.len(),
                policy.summary
            ));
        }
        if let Some(s) = &self.observability_summary {
            let hit_ratio = s
                .cache_hit_ratio()
                .map(|r| format!("{:.1}%", r * 100.0))
                .unwrap_or_else(|| "-".into());
            let fallback_step_rate = s
                .fallback_step_rate()
                .map(|r| format!("{:.1}%", r * 100.0))
                .unwrap_or_else(|| "-".into());
            let hydration_resolution_rate = s
                .hydration_resolution_rate()
                .map(|r| format!("{:.1}%", r * 100.0))
                .unwrap_or_else(|| "-".into());
            let stale_ref_rate = s
                .stale_ref_rate()
                .map(|r| format!("{:.1}%", r * 100.0))
                .unwrap_or_else(|| "-".into());
            let dominant_fallback_tier = dominant_count_key(&s.fallback_tier_counts).unwrap_or("-");
            let dominant_model_tier = dominant_count_key(&s.model_tier_counts).unwrap_or("-");
            lines.push(format!(
                "  summary: routed={} override_hits={} cache_r={} cache_w={} cache_hit_observed={} hit_ratio={} tokens_saved={} input={} uncached_input={} output={} chars={}/{} cost_micros_usd={} fallback={} fallback_step_rate={} dominant_fallback_tier={} fallback_tiers={:?} fallback_reasons={:?} mismatch={} hydration={} hydration_rate={} stale_refs={} stale_rate={} missing_refs={} dominant_model_tier={} tiers={:?}",
                s.total_steps_routed,
                s.override_hit_count,
                s.total_cache_read_tokens,
                s.total_cache_write_tokens,
                s.cache_hit_observed(),
                hit_ratio,
                s.estimated_tokens_saved(),
                s.total_input_tokens,
                s.total_uncached_input_tokens,
                s.total_output_tokens,
                s.total_sent_chars,
                s.total_original_chars,
                s.estimated_cost_micros_usd,
                s.total_fallback_count,
                fallback_step_rate,
                dominant_fallback_tier,
                s.fallback_tier_counts,
                s.fallback_reason_counts,
                s.total_projection_mismatch_count,
                s.total_hydration_count,
                hydration_resolution_rate,
                s.total_stale_ref_count,
                stale_ref_rate,
                s.total_hydration_ref_missing,
                dominant_model_tier,
                s.model_tier_counts,
            ));
        }
        if !self.stage_execution_contract.declared_artifacts.is_empty()
            || !self.stage_execution_contract.verifications.is_empty()
            || !self.stage_execution_contract.tests.is_empty()
        {
            lines.push(format!(
                "contract summary: declared_artifacts={} verifications={} tests={} required_actions={} required_evidence={}",
                self.stage_execution_contract.declared_artifacts.len(),
                self.stage_execution_contract.verifications.len(),
                self.stage_execution_contract.tests.len(),
                self.stage_execution_contract.required_actions.join("|"),
                self.stage_execution_contract.required_evidence.join("|"),
            ));
        }
        lines.join("\n")
    }
}

fn dominant_count_key(counts: &std::collections::HashMap<String, usize>) -> Option<&str> {
    counts
        .iter()
        .max_by(|(left_key, left_count), (right_key, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_key.cmp(left_key))
        })
        .map(|(key, _)| key.as_str())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BossControlRequest {
    Report,
    Stop {
        requester_session_id: String,
        deadline_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BossStopStage {
    CancelIssued,
    DeadlineExpired,
    ForceDrain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BossStopOutcome {
    pub killed_task_ids: Vec<String>,
    pub stages: Vec<BossStopStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BossControlResponse {
    Report(BossReportPayload),
    Stop(BossStopOutcome),
}

/// Boss-level LisM execution policy.
///
/// Precedence (high → low):
///   1. User explicit `/LisM on|off` (session toggle — always wins)
///   2. This policy field on BossCoordinator
///   3. Global default: Inherit
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BossLisMPolicy {
    /// Follow the session-level `lism_enabled()` toggle (current behaviour).
    #[default]
    Inherit,
    /// Force LisM on for this Boss session regardless of the session toggle.
    ForceOn,
    /// Force LisM off for this Boss session regardless of the session toggle.
    ForceOff,
}

impl BossPlanStep {
    pub fn objective(&self) -> &str {
        self.objective.as_deref().unwrap_or(&self.description)
    }
}

impl Default for BossStatus {
    fn default() -> Self {
        Self {
            stage: BossStage::Documentation,
            current_step: None,
            total_steps: None,
            planning_file: None,
            last_b_dispatch_payload: None,
            last_a_dispatch_message: None,
            last_b_ask_message: None,
            last_step_metrics: None,
            lism_sample_emitted: false,
        }
    }
}

/// Which long-lived actor role this handle represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BossActorRole {
    DesignerA,
    ExecutorB,
    /// Child spawned to review a completed step.
    ReviewChild,
    /// Child spawned to implement a plan step.
    ImplementChild,
    /// Child spawned to verify acceptance criteria.
    VerifyChild,
}

impl BossActorRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            BossActorRole::DesignerA => "designer_a",
            BossActorRole::ExecutorB => "executor_b",
            BossActorRole::ReviewChild => "review_child",
            BossActorRole::ImplementChild => "implement_child",
            BossActorRole::VerifyChild => "verify_child",
        }
    }

    pub fn is_child(&self) -> bool {
        matches!(
            self,
            BossActorRole::ReviewChild | BossActorRole::ImplementChild | BossActorRole::VerifyChild
        )
    }
}

/// Lifecycle status of a tracked actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BossActorStatus {
    #[default]
    Pending,
    Active,
    Suspended,
    Completed,
    Failed,
}

impl BossActorStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BossActorStatus::Pending => "pending",
            BossActorStatus::Active => "active",
            BossActorStatus::Suspended => "suspended",
            BossActorStatus::Completed => "completed",
            BossActorStatus::Failed => "failed",
        }
    }
}

/// Stable, observable handle for a single long-lived actor in the boss topology.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BossActorHandle {
    /// Stable session id for this actor (e.g. "boss-{plan_id}-a").
    pub actor_id: String,
    /// Session id used when the actor was last active.
    pub session_id: String,
    pub role: BossActorRole,
    pub status: BossActorStatus,
    /// Step id this actor is currently working on, if any.
    pub task_id: Option<String>,
    /// Wall-clock time of the last status update.
    pub last_snapshot: Option<SystemTime>,
    /// How many levels deep from the root boss session (0 = direct child).
    pub lineage_depth: u32,
    /// Logical mailbox address for future message-passing (not a live channel).
    pub mailbox_id: Option<String>,
    /// Opaque token id used to cancel this actor's work; resolved at runtime.
    pub cancel_id: Option<String>,
    /// Fingerprint of the last assignment contract issued to this actor.
    #[serde(default)]
    pub last_assignment_fingerprint: Option<String>,
    /// Last plan version issued to this actor for stale-brief detection.
    #[serde(default)]
    pub last_assignment_plan_version: Option<String>,
    /// Last step revision issued to this actor for stale-brief detection.
    #[serde(default)]
    pub last_assignment_step_revision: Option<String>,
}

impl BossActorHandle {
    pub fn new(
        actor_id: impl Into<String>,
        session_id: impl Into<String>,
        role: BossActorRole,
    ) -> Self {
        Self {
            actor_id: actor_id.into(),
            session_id: session_id.into(),
            role,
            status: BossActorStatus::Pending,
            task_id: None,
            last_snapshot: None,
            lineage_depth: 0,
            mailbox_id: None,
            cancel_id: None,
            last_assignment_fingerprint: None,
            last_assignment_plan_version: None,
            last_assignment_step_revision: None,
        }
    }
}

/// Runtime topology snapshot for one boss session.
/// Persisted alongside the plan so it can be restored on `/boss` re-entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BossSession {
    pub plan_id: String,
    pub stage: BossStage,
    pub designer_a: BossActorHandle,
    pub executor_b: BossActorHandle,
    /// Transient child actors spawned during execution (cleared on restore).
    #[serde(default)]
    pub active_children: Vec<BossActorHandle>,
    /// Token budget snapshot at the time this session was last saved.
    pub budget_snapshot: Option<u64>,
}

impl BossSession {
    /// Derive a stable BossSession from a plan id.
    /// A/B session ids are deterministic: "boss-{plan_id}-a" / "boss-{plan_id}-b".
    pub fn from_plan_id(plan_id: &str, stage: BossStage) -> Self {
        let a_id = format!("boss-{plan_id}-a");
        let b_id = format!("boss-{plan_id}-b");
        Self {
            plan_id: plan_id.to_string(),
            stage,
            designer_a: BossActorHandle::new(a_id.clone(), a_id, BossActorRole::DesignerA),
            executor_b: BossActorHandle::new(b_id.clone(), b_id, BossActorRole::ExecutorB),
            active_children: Vec::new(),
            budget_snapshot: None,
        }
    }
}

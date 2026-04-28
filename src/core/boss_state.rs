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
    /// Task id of the A review agent currently reviewing this step.
    #[serde(default)]
    pub review_task_id: Option<String>,
}

fn default_retry_budget() -> u32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub projection_mismatch_count: Option<usize>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BossObservabilitySummary {
    pub total_steps_routed: usize,
    pub total_cache_read_tokens: usize,
    pub total_cache_write_tokens: usize,
    pub total_fallback_count: usize,
    pub total_projection_mismatch_count: usize,
    /// Steps where provider_profile_id is Some (i.e. a non-inherited model profile was used).
    pub override_hit_count: usize,
    pub model_tier_counts: std::collections::HashMap<String, usize>,
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
    pub lism_policy: BossLisMPolicy,
}

impl BossReportPayload {
    pub fn format_report(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "stage={:?} step={}/{} ",
            self.stage,
            self.current_step.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
            self.total_steps.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
        ));
        for step in &self.steps {
            let m = step.routed_metadata.as_ref();
            lines.push(format!(
                "  step {:>3}: status={:?} tier={} profile={} frame={}B cache_r={} cache_w={} fb={} mm={}",
                step.id,
                step.status,
                m.and_then(|m| m.model_tier.as_deref()).unwrap_or("-"),
                m.and_then(|m| m.provider_profile_id.as_deref()).unwrap_or("-"),
                m.and_then(|m| m.state_frame_size).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.cache_read_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.cache_write_tokens).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.fallback_count).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                m.and_then(|m| m.projection_mismatch_count).map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
            ));
        }
        if let Some(s) = &self.observability_summary {
            lines.push(format!(
                "  summary: routed={} override_hits={} cache_r={} cache_w={} fallback={} mismatch={} tiers={:?}",
                s.total_steps_routed,
                s.override_hit_count,
                s.total_cache_read_tokens,
                s.total_cache_write_tokens,
                s.total_fallback_count,
                s.total_projection_mismatch_count,
                s.model_tier_counts,
            ));
        }
        lines.join("\n")
    }
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
}

impl BossActorHandle {
    pub fn new(actor_id: impl Into<String>, session_id: impl Into<String>, role: BossActorRole) -> Self {
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

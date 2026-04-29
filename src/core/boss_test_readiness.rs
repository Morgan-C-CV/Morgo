use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

// ── Provider / skill / MCP allowlists ────────────────────────────────────────

/// Allowlist configuration for a `/boss` product test session.
///
/// An empty allowlist means "allow all" for that dimension — callers that want
/// to restrict must populate the set explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BossTestAllowlist {
    /// Provider profile IDs permitted in this test session.
    /// Empty = allow any provider.
    #[serde(default)]
    pub provider_profiles: BTreeSet<String>,
    /// Skill names permitted in this test session.
    /// Empty = allow any skill.
    #[serde(default)]
    pub skill_names: BTreeSet<String>,
    /// MCP server names permitted in this test session.
    /// Empty = allow any MCP server.
    #[serde(default)]
    pub mcp_server_names: BTreeSet<String>,
}

impl BossTestAllowlist {
    pub fn allows_provider(&self, profile_id: &str) -> bool {
        self.provider_profiles.is_empty() || self.provider_profiles.contains(profile_id)
    }

    pub fn allows_skill(&self, skill_name: &str) -> bool {
        self.skill_names.is_empty() || self.skill_names.contains(skill_name)
    }

    pub fn allows_mcp_server(&self, server_name: &str) -> bool {
        self.mcp_server_names.is_empty() || self.mcp_server_names.contains(server_name)
    }
}

// ── Rollback trigger policy ───────────────────────────────────────────────────

/// Conditions under which a boss test run should trigger rollback / abort.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BossRollbackPolicy {
    /// Abort if any MCP failure occurs during the run.
    #[serde(default)]
    pub abort_on_mcp_failure: bool,
    /// Abort if cost exceeds this threshold in micros USD (0 = no limit).
    #[serde(default)]
    pub max_cost_micros_usd: u64,
    /// Abort if cache hit ratio drops below this threshold (0.0 = no limit).
    #[serde(default)]
    pub min_cache_hit_ratio: f64,
    /// Abort if any step requires user approval (pending approval gate).
    #[serde(default)]
    pub abort_on_pending_approval: bool,
}

impl Default for BossRollbackPolicy {
    fn default() -> Self {
        Self {
            abort_on_mcp_failure: false,
            max_cost_micros_usd: 0,
            min_cache_hit_ratio: 0.0,
            abort_on_pending_approval: false,
        }
    }
}

// ── Admission policy ──────────────────────────────────────────────────────────

/// Full admission policy for a `/boss` product test session.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BossTestAdmissionPolicy {
    pub allowlist: BossTestAllowlist,
    pub rollback: BossRollbackPolicy,
}

// ── Admission gate ────────────────────────────────────────────────────────────

/// Why a `/boss` invocation was denied by the admission gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BossAdmissionDenyReason {
    ProviderNotAllowlisted { profile_id: String },
    SkillNotAllowlisted { skill_name: String },
    McpServerNotAllowlisted { server_name: String },
}

impl BossAdmissionDenyReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ProviderNotAllowlisted { .. } => "provider_not_allowlisted",
            Self::SkillNotAllowlisted { .. } => "skill_not_allowlisted",
            Self::McpServerNotAllowlisted { .. } => "mcp_server_not_allowlisted",
        }
    }

    pub fn render_line(&self) -> String {
        match self {
            Self::ProviderNotAllowlisted { profile_id } => {
                format!("provider_not_allowlisted: {profile_id}")
            }
            Self::SkillNotAllowlisted { skill_name } => {
                format!("skill_not_allowlisted: {skill_name}")
            }
            Self::McpServerNotAllowlisted { server_name } => {
                format!("mcp_server_not_allowlisted: {server_name}")
            }
        }
    }
}

/// Result of evaluating the admission gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BossAdmissionResult {
    Admitted,
    Denied(Vec<BossAdmissionDenyReason>),
}

impl BossAdmissionResult {
    pub fn is_admitted(&self) -> bool {
        matches!(self, Self::Admitted)
    }

    pub fn deny_reasons(&self) -> &[BossAdmissionDenyReason] {
        match self {
            Self::Admitted => &[],
            Self::Denied(reasons) => reasons,
        }
    }
}

/// Evaluates whether a proposed `/boss` invocation satisfies the admission policy.
pub struct BossTestReadinessGate<'a> {
    policy: &'a BossTestAdmissionPolicy,
}

impl<'a> BossTestReadinessGate<'a> {
    pub fn new(policy: &'a BossTestAdmissionPolicy) -> Self {
        Self { policy }
    }

    /// Check a proposed invocation context against the allowlists.
    pub fn check(
        &self,
        provider_profile: Option<&str>,
        skill_names: &[&str],
        mcp_server_names: &[&str],
    ) -> BossAdmissionResult {
        let mut reasons = Vec::new();

        if let Some(profile) = provider_profile {
            if !self.policy.allowlist.allows_provider(profile) {
                reasons.push(BossAdmissionDenyReason::ProviderNotAllowlisted {
                    profile_id: profile.to_string(),
                });
            }
        }

        for skill in skill_names {
            if !self.policy.allowlist.allows_skill(skill) {
                reasons.push(BossAdmissionDenyReason::SkillNotAllowlisted {
                    skill_name: skill.to_string(),
                });
            }
        }

        for server in mcp_server_names {
            if !self.policy.allowlist.allows_mcp_server(server) {
                reasons.push(BossAdmissionDenyReason::McpServerNotAllowlisted {
                    server_name: server.to_string(),
                });
            }
        }

        if reasons.is_empty() {
            BossAdmissionResult::Admitted
        } else {
            BossAdmissionResult::Denied(reasons)
        }
    }
}

// ── Rollback evaluator ────────────────────────────────────────────────────────

/// Why a running boss session should be rolled back / aborted.
#[derive(Debug, Clone, PartialEq)]
pub enum BossRollbackTrigger {
    McpFailureOccurred,
    CostLimitExceeded {
        actual_micros_usd: u64,
        limit_micros_usd: u64,
    },
    CacheHitRatioBelowThreshold {
        actual: f64,
        threshold: f64,
    },
    PendingApprovalRequired,
}

impl BossRollbackTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::McpFailureOccurred => "mcp_failure_occurred",
            Self::CostLimitExceeded { .. } => "cost_limit_exceeded",
            Self::CacheHitRatioBelowThreshold { .. } => "cache_hit_ratio_below_threshold",
            Self::PendingApprovalRequired => "pending_approval_required",
        }
    }

    pub fn render_line(&self) -> String {
        match self {
            Self::McpFailureOccurred => "mcp_failure_occurred".to_string(),
            Self::CostLimitExceeded {
                actual_micros_usd,
                limit_micros_usd,
            } => {
                format!("cost_limit_exceeded: actual={actual_micros_usd} limit={limit_micros_usd}")
            }
            Self::CacheHitRatioBelowThreshold { actual, threshold } => {
                format!(
                    "cache_hit_ratio_below_threshold: actual={actual:.3} threshold={threshold:.3}"
                )
            }
            Self::PendingApprovalRequired => "pending_approval_required".to_string(),
        }
    }
}

/// Evaluates whether a running boss session should be rolled back.
pub fn evaluate_rollback_triggers(
    policy: &BossRollbackPolicy,
    mcp_failure_occurred: bool,
    cost_micros_usd: u64,
    cache_hit_ratio: Option<f64>,
    has_pending_approval: bool,
) -> Vec<BossRollbackTrigger> {
    let mut triggers = Vec::new();

    if policy.abort_on_mcp_failure && mcp_failure_occurred {
        triggers.push(BossRollbackTrigger::McpFailureOccurred);
    }

    if policy.max_cost_micros_usd > 0 && cost_micros_usd > policy.max_cost_micros_usd {
        triggers.push(BossRollbackTrigger::CostLimitExceeded {
            actual_micros_usd: cost_micros_usd,
            limit_micros_usd: policy.max_cost_micros_usd,
        });
    }

    if policy.min_cache_hit_ratio > 0.0 {
        if let Some(ratio) = cache_hit_ratio {
            if ratio < policy.min_cache_hit_ratio {
                triggers.push(BossRollbackTrigger::CacheHitRatioBelowThreshold {
                    actual: ratio,
                    threshold: policy.min_cache_hit_ratio,
                });
            }
        }
    }

    if policy.abort_on_pending_approval && has_pending_approval {
        triggers.push(BossRollbackTrigger::PendingApprovalRequired);
    }

    triggers
}

// ── Test sample record ────────────────────────────────────────────────────────

/// Structured sample captured from a single `/boss` product test run.
///
/// Accumulated across runs to calibrate R1 (LisM cost/cache) and R4 (skill/MCP stability).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BossTestSampleRecord {
    pub run_id: String,
    pub provider_profile: Option<String>,
    pub skill_names: Vec<String>,
    pub mcp_server_names: Vec<String>,
    pub total_steps: usize,
    pub completed_steps: usize,
    pub cost_micros_usd: u64,
    pub cache_hit_ratio: Option<f64>,
    pub estimated_tokens_saved: usize,
    pub mcp_failure_count: usize,
    pub pending_approval_count: usize,
    pub rollback_triggers: Vec<String>,
    pub outcome: BossTestRunOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BossTestRunOutcome {
    Completed,
    RolledBack,
    Aborted,
}

impl BossTestRunOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::RolledBack => "rolled_back",
            Self::Aborted => "aborted",
        }
    }
}

impl BossTestSampleRecord {
    pub fn render_summary(&self) -> String {
        format!(
            "run={} outcome={} steps={}/{} cost_micros={} cache_hit={} tokens_saved={} mcp_failures={} pending_approvals={} rollback_triggers={}",
            self.run_id,
            self.outcome.as_str(),
            self.completed_steps,
            self.total_steps,
            self.cost_micros_usd,
            self.cache_hit_ratio
                .map(|r| format!("{:.1}%", r * 100.0))
                .unwrap_or_else(|| "-".into()),
            self.estimated_tokens_saved,
            self.mcp_failure_count,
            self.pending_approval_count,
            self.rollback_triggers.len(),
        )
    }
}

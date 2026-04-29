use serde::{Deserialize, Serialize};

use crate::bootstrap::InteractionSurface;
use crate::state::permission_context::PendingApproval;

// ── Approval decision ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approved,
    Denied,
}

impl ApprovalDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }

    pub fn from_bool(approved: bool) -> Self {
        if approved { Self::Approved } else { Self::Denied }
    }
}

// ── Approval resolution record ────────────────────────────────────────────────

/// Structured record of a completed approval resolution.
/// Written after `resolve_pending_approval()` completes on any surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalResolutionRecord {
    pub tool_name: String,
    pub decision: ApprovalDecision,
    pub surface: ApprovalSurface,
    pub code: Option<String>,
    pub approval_kind: Option<String>,
    pub escalation_reasons: Vec<String>,
}

impl ApprovalResolutionRecord {
    pub fn new(
        pending: &PendingApproval,
        decision: ApprovalDecision,
        surface: ApprovalSurface,
    ) -> Self {
        Self {
            tool_name: pending.tool_name.clone(),
            decision,
            surface,
            code: pending.code.clone(),
            approval_kind: pending.approval_kind.clone(),
            escalation_reasons: pending.escalation_reasons.clone(),
        }
    }

    pub fn render_line(&self) -> String {
        format!(
            "approval_resolution: tool={} decision={} surface={} code={} kind={}",
            self.tool_name,
            self.decision.as_str(),
            self.surface.as_str(),
            self.code.as_deref().unwrap_or("none"),
            self.approval_kind.as_deref().unwrap_or("none"),
        )
    }
}

// ── Approval surface ──────────────────────────────────────────────────────────

/// Which surface resolved the approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalSurface {
    Cli,
    Telegram,
    Remote,
    Unknown,
}

impl ApprovalSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Telegram => "telegram",
            Self::Remote => "remote",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_interaction_surface(surface: InteractionSurface) -> Self {
        match surface {
            InteractionSurface::Cli => Self::Cli,
            InteractionSurface::Telegram => Self::Telegram,
            InteractionSurface::Remote => Self::Remote,
        }
    }
}

// ── Pending approval status ───────────────────────────────────────────────────

/// Structured view of the current pending approval state.
/// Used by TUI and Telegram renderers to display the approval prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingApprovalStatus {
    pub tool_name: String,
    pub message: String,
    pub code: Option<String>,
    pub summary: Option<String>,
    pub detail: Option<String>,
    pub approval_kind: Option<String>,
    pub escalation_reasons: Vec<String>,
}

impl PendingApprovalStatus {
    pub fn from_pending(pending: &PendingApproval) -> Self {
        Self {
            tool_name: pending.tool_name.clone(),
            message: pending.message.clone(),
            code: pending.code.clone(),
            summary: pending.summary.clone(),
            detail: pending.detail.clone(),
            approval_kind: pending.approval_kind.clone(),
            escalation_reasons: pending.escalation_reasons.clone(),
        }
    }

    /// One-line prompt suitable for TUI display.
    pub fn render_prompt_line(&self) -> String {
        let code_tag = self.code.as_deref().unwrap_or("pending_approval");
        format!(
            "[{}] {} — type 'yes' to approve or 'no' to deny",
            code_tag, self.message
        )
    }

    /// Multi-line prompt suitable for Telegram delivery.
    pub fn render_telegram_prompt(&self) -> String {
        let mut lines = vec![
            format!("Approval required for: {}", self.tool_name),
            self.message.clone(),
        ];
        if let Some(detail) = &self.detail {
            lines.push(format!("Detail: {detail}"));
        }
        if !self.escalation_reasons.is_empty() {
            lines.push(format!("Reasons: {}", self.escalation_reasons.join(", ")));
        }
        lines.push("Reply 'yes' to approve or 'no' to deny.".into());
        lines.join("\n")
    }
}

// ── Approval input normalizer ─────────────────────────────────────────────────

/// Normalize a raw user input string to an approval decision.
/// Returns `None` if the input is not a recognized approval response.
pub fn parse_approval_input(raw: &str) -> Option<ApprovalDecision> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "yes" | "y" | "approve" => Some(ApprovalDecision::Approved),
        "no" | "n" | "deny" => Some(ApprovalDecision::Denied),
        _ => None,
    }
}

// ── Boss-path approval state ──────────────────────────────────────────────────

/// Typed state for a boss step that is waiting for approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BossStepApprovalGate {
    pub step_id: String,
    pub tool_name: String,
    pub message: String,
    pub code: Option<String>,
    pub escalation_reasons: Vec<String>,
}

impl BossStepApprovalGate {
    pub fn new(
        step_id: impl Into<String>,
        pending: &PendingApproval,
    ) -> Self {
        Self {
            step_id: step_id.into(),
            tool_name: pending.tool_name.clone(),
            message: pending.message.clone(),
            code: pending.code.clone(),
            escalation_reasons: pending.escalation_reasons.clone(),
        }
    }

    pub fn render_line(&self) -> String {
        format!(
            "boss_approval_gate: step={} tool={} code={}",
            self.step_id,
            self.tool_name,
            self.code.as_deref().unwrap_or("none"),
        )
    }
}

/// Outcome after a boss step's approval gate is resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BossStepApprovalOutcome {
    /// Step was approved — resume execution.
    Approved { step_id: String },
    /// Step was denied — record and skip.
    Denied { step_id: String, reason: String },
}

impl BossStepApprovalOutcome {
    pub fn approved(step_id: impl Into<String>) -> Self {
        Self::Approved { step_id: step_id.into() }
    }

    pub fn denied(step_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Denied { step_id: step_id.into(), reason: reason.into() }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approved { .. } => "approved",
            Self::Denied { .. } => "denied",
        }
    }

    pub fn step_id(&self) -> &str {
        match self {
            Self::Approved { step_id } | Self::Denied { step_id, .. } => step_id.as_str(),
        }
    }

    pub fn render_line(&self) -> String {
        match self {
            Self::Approved { step_id } => {
                format!("boss_step_approval: step={step_id} outcome=approved")
            }
            Self::Denied { step_id, reason } => {
                format!("boss_step_approval: step={step_id} outcome=denied reason={reason}")
            }
        }
    }
}

/// Resolve a boss step approval gate given a user decision.
pub fn resolve_boss_step_approval(
    gate: &BossStepApprovalGate,
    decision: ApprovalDecision,
) -> BossStepApprovalOutcome {
    match decision {
        ApprovalDecision::Approved => BossStepApprovalOutcome::approved(&gate.step_id),
        ApprovalDecision::Denied => BossStepApprovalOutcome::denied(
            &gate.step_id,
            format!("user denied approval for {}", gate.tool_name),
        ),
    }
}

use crate::core::boss_state::BossPlanStepStatus;
use crate::core::prompt_segment::{PromptAssembly, PromptSegment, PromptSegmentKind};

/// Which context strategy was used to build the B/child initial prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BossContextStrategy {
    /// Default: structured brief + state frame. Cacheable prefix + non-cacheable suffix.
    Brief,
    /// Explicit escape hatch: inherit full parent session context. Debug / complex tasks only.
    FullInherit,
}

/// Stable, cacheable fields for a B/child actor's initial context.
/// Maps to `PromptSegmentKind::ActorBrief` (cacheable).
/// `recent_decisions` and `relevant_files` are v1 empty — T27 will populate them.
#[derive(Debug, Clone)]
pub struct BossContextBrief {
    pub plan_id: String,
    pub step_id: usize,
    pub objective: String,
    pub acceptance: Vec<String>,
    pub last_correction: Option<String>,
    /// Key decisions from prior steps (v1: empty list).
    pub recent_decisions: Vec<String>,
    /// Files known to be relevant (v1: empty list).
    pub relevant_files: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub parent_session_id: String,
    pub context_strategy: BossContextStrategy,
}

/// Dynamic, non-cacheable state for the current turn.
/// Maps to `PromptSegmentKind::StateFrame` (non-cacheable).
#[derive(Debug, Clone)]
pub struct BossStateFrame {
    pub step_id: usize,
    pub status: BossPlanStepStatus,
    pub open_items: Vec<String>,
    pub blocked_items: Vec<String>,
    pub allowed_actions: Vec<String>,
    pub required_output_hint: Option<String>,
}

impl BossContextBrief {
    /// Render the stable fields as a `PromptSegment` (cacheable).
    pub fn to_prompt_segment(&self) -> PromptSegment {
        let mut lines = vec![
            format!("plan_id: {}", self.plan_id),
            format!("step_id: {}", self.step_id),
            format!("objective: {}", self.objective),
        ];
        if !self.acceptance.is_empty() {
            lines.push("acceptance:".into());
            for a in &self.acceptance {
                lines.push(format!("  - {a}"));
            }
        }
        if let Some(c) = &self.last_correction {
            lines.push(format!("correction from review:\n{c}"));
        }
        if !self.recent_decisions.is_empty() {
            lines.push("recent_decisions:".into());
            for d in &self.recent_decisions {
                lines.push(format!("  - {d}"));
            }
        }
        if !self.relevant_files.is_empty() {
            lines.push("relevant_files:".into());
            for f in &self.relevant_files {
                lines.push(format!("  - {f}"));
            }
        }
        if !self.allowed_tools.is_empty() {
            lines.push(format!("allowed_tools: {}", self.allowed_tools.join(", ")));
        }
        lines.push(format!("parent_session_id: {}", self.parent_session_id));
        PromptSegment::new("actor_brief", PromptSegmentKind::ActorBrief, lines.join("\n"))
    }
}

impl BossStateFrame {
    /// Render the dynamic fields as a `PromptSegment` (non-cacheable).
    pub fn to_prompt_segment(&self) -> PromptSegment {
        let mut lines = vec![
            format!("step_id: {}", self.step_id),
            format!("status: {:?}", self.status),
        ];
        if !self.open_items.is_empty() {
            lines.push("open_items:".into());
            for item in &self.open_items {
                lines.push(format!("  - {item}"));
            }
        }
        if !self.blocked_items.is_empty() {
            lines.push("blocked_items:".into());
            for item in &self.blocked_items {
                lines.push(format!("  - {item}"));
            }
        }
        if !self.allowed_actions.is_empty() {
            lines.push(format!("allowed_actions: {}", self.allowed_actions.join(", ")));
        }
        if let Some(hint) = &self.required_output_hint {
            lines.push(format!("required_output: {hint}"));
        }
        PromptSegment::new("state_frame", PromptSegmentKind::StateFrame, lines.join("\n"))
    }
}

/// Assemble a B/child initial prompt from a brief and state frame.
pub fn assemble_brief_prompt(brief: &BossContextBrief, frame: &BossStateFrame) -> String {
    let mut assembly = PromptAssembly::new();
    assembly.push(brief.to_prompt_segment());
    assembly.push(frame.to_prompt_segment());
    assembly.assemble()
}

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
/// `recent_decisions` and `relevant_file_handles` carry compact assignment memory.
///
/// Important for provider-side prompt cache efficiency: volatile identifiers like
/// `plan_id` must render late in the segment so stable task semantics can occupy
/// the earliest prefix tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelevantFileHandle {
    pub path: String,
    pub kind: String,
    pub source: String,
    pub freshness: String,
    pub why_relevant: String,
    pub step_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetArtifact {
    pub path: String,
    pub kind: String,
    pub required_state: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionScopeView {
    pub lism_policy: String,
    pub inherit_context: bool,
    pub workspace_capability: String,
    pub boss_actor_role: String,
}

#[derive(Debug, Clone)]
pub struct BossContextBrief {
    pub plan_id: String,
    pub step_id: usize,
    pub plan_version: String,
    pub step_revision: String,
    pub generated_at: String,
    pub objective: String,
    pub acceptance: Vec<String>,
    pub last_correction: Option<String>,
    /// Key decisions from prior steps, compact enough to stay cacheable.
    pub recent_decisions: Vec<String>,
    /// Typed file handles for the worker assignment memory.
    pub relevant_file_handles: Vec<RelevantFileHandle>,
    pub target_files: Vec<String>,
    pub target_artifacts: Vec<TargetArtifact>,
    pub allowed_tools: Vec<String>,
    pub permission_scope: PermissionScopeView,
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
            format!("objective: {}", self.objective),
            format!("step_id: {}", self.step_id),
            format!("plan_version: {}", self.plan_version),
            format!("step_revision: {}", self.step_revision),
            format!("generated_at: {}", self.generated_at),
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
        if !self.relevant_file_handles.is_empty() {
            lines.push("relevant_file_handles:".into());
            for handle in &self.relevant_file_handles {
                lines.push(format!(
                    "  - path={} kind={} source={} freshness={} step_revision={} why_relevant={}",
                    handle.path,
                    handle.kind,
                    handle.source,
                    handle.freshness,
                    handle.step_revision,
                    handle.why_relevant
                ));
            }
        }
        if !self.target_files.is_empty() {
            lines.push("target_files:".into());
            for path in &self.target_files {
                lines.push(format!("  - {path}"));
            }
        }
        if !self.target_artifacts.is_empty() {
            lines.push("target_artifacts:".into());
            for artifact in &self.target_artifacts {
                lines.push(format!(
                    "  - path={} kind={} required_state={} source={}",
                    artifact.path, artifact.kind, artifact.required_state, artifact.source
                ));
            }
        }
        if !self.allowed_tools.is_empty() {
            lines.push(format!("allowed_tools: {}", self.allowed_tools.join(", ")));
        }
        lines.push(format!(
            "permission_scope: lism_policy={} inherit_context={} workspace_capability={} boss_actor_role={}",
            self.permission_scope.lism_policy,
            self.permission_scope.inherit_context,
            self.permission_scope.workspace_capability,
            self.permission_scope.boss_actor_role
        ));
        lines.push(format!("parent_session_id: {}", self.parent_session_id));
        lines.push(format!("plan_id: {}", self.plan_id));
        PromptSegment::new(
            "actor_brief",
            PromptSegmentKind::ActorBrief,
            lines.join("\n"),
        )
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
            lines.push(format!(
                "allowed_actions: {}",
                self.allowed_actions.join(", ")
            ));
        }
        if let Some(hint) = &self.required_output_hint {
            lines.push(format!("required_output: {hint}"));
        }
        PromptSegment::new(
            "state_frame",
            PromptSegmentKind::StateFrame,
            lines.join("\n"),
        )
    }
}

/// Assemble a B/child initial prompt from a brief and state frame.
pub fn assemble_brief_prompt(brief: &BossContextBrief, frame: &BossStateFrame) -> String {
    let mut assembly = PromptAssembly::new();
    assembly.push(brief.to_prompt_segment());
    assembly.push(frame.to_prompt_segment());
    assembly.assemble()
}

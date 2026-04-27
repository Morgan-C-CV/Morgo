use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::prompt_segment::{PromptSegment, PromptSegmentKind};

/// Which actor role is receiving this StateFrame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ActorRole {
    DesignerA,
    ExecutorB,
    #[default]
    Worker,
    Verifier,
    Summarizer,
}

/// Current abstract state of the actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    #[default]
    Planning,
    Executing,
    Reviewing,
    Correcting,
    Verifying,
    Blocked,
    Done,
}

/// Cost/performance effort tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffortLevel {
    L,
    M,
    H,
}

impl Default for EffortLevel {
    fn default() -> Self {
        Self::M
    }
}

/// Token / time budget for this StateFrame dispatch.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateBudget {
    #[serde(default)]
    pub effort: EffortLevel,
    /// 0 = no limit.
    #[serde(default)]
    pub max_input_tokens: u64,
    /// 0 = no limit.
    #[serde(default)]
    pub max_tool_calls: u32,
    /// 0 = no limit.
    #[serde(default)]
    pub max_wall_time_ms: u64,
}

/// Minimal input contract sent to the LLM in StateFrame-first mode.
/// Rendered as a non-cacheable `PromptSegmentKind::StateFrame` suffix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateFrame {
    pub role: ActorRole,
    pub state: AgentState,
    pub objective: String,
    #[serde(default)]
    pub open_items: Vec<String>,
    #[serde(default)]
    pub blocked_items: Vec<String>,
    #[serde(default)]
    pub accepted_summary: Vec<String>,
    #[serde(default)]
    pub recent_evidence: Vec<String>,
    #[serde(default)]
    pub allowed_actions: Vec<String>,
    #[serde(default)]
    pub toolset_id: Option<String>,
    #[serde(default)]
    pub skillset_id: Option<String>,
    #[serde(default)]
    pub required_output_schema: Option<String>,
    #[serde(default)]
    pub budget: StateBudget,
}

impl StateFrame {
    /// Render as a non-cacheable `PromptSegment`.
    pub fn to_prompt_segment(&self) -> PromptSegment {
        let json = serde_json::to_string_pretty(self).unwrap_or_default();
        PromptSegment::new("state_frame_v1", PromptSegmentKind::StateFrame, json)
    }
}

/// Which high-level decision the LLM is returning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    Continue,
    RequestContext,
    CallTool,
    Handoff,
    Accept,
    Reject,
    Done,
}

/// A single tool/action the LLM wants to invoke.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextAction {
    pub action_type: String,
    #[serde(default)]
    pub args: Value,
}

/// Incremental patch the LLM proposes to apply to the orchestrator's state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatePatch {
    #[serde(default)]
    pub open_items_add: Vec<String>,
    #[serde(default)]
    pub open_items_remove: Vec<String>,
    #[serde(default)]
    pub accepted_summary_add: Vec<String>,
}

/// Structured output contract from the LLM in StateFrame-first mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDecision {
    pub state: AgentState,
    pub decision: DecisionKind,
    #[serde(default)]
    pub next_action: Option<NextAction>,
    /// Structured context requests — each entry is a key like "file:path" or "symbol:name".
    #[serde(default)]
    pub needed_context: Vec<String>,
    #[serde(default)]
    pub state_patch: StatePatch,
    /// LLM self-reported confidence in [0.0, 1.0]. 0.0 = not reported.
    #[serde(default)]
    pub confidence: f32,
    /// True if the LLM requests escalation to a stronger model or full-context fallback.
    #[serde(default)]
    pub escalate: bool,
}

/// Returned when `validate_state_decision` cannot parse or validate the LLM output.
#[derive(Debug, Clone)]
pub struct RepairNeeded {
    /// Human-readable reason for the repair request.
    pub reason: String,
    /// The raw JSON string that failed validation (for repair prompt construction).
    pub raw_json: String,
}

/// Parse and validate a JSON string as a `StateDecision`.
/// Pure function — no LLM calls, no side effects.
/// Returns `Err(RepairNeeded)` if the JSON is invalid or missing required fields.
pub fn validate_state_decision(json: &str) -> Result<StateDecision, RepairNeeded> {
    let decision: StateDecision = serde_json::from_str(json).map_err(|e| RepairNeeded {
        reason: format!("JSON parse error: {e}"),
        raw_json: json.to_string(),
    })?;
    Ok(decision)
}

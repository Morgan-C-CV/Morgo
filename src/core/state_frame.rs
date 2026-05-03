use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
    pub allowed_tools: Vec<String>,
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
    #[serde(default, alias = "open_items")]
    pub open_items_add: Vec<String>,
    #[serde(default)]
    pub open_items_remove: Vec<String>,
    #[serde(default, alias = "accepted_summary")]
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

fn normalized_agent_state(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "planning" | "plan" => Some("planning"),
        "executing" | "execute" | "execution" | "running" | "in_progress" => Some("executing"),
        "reviewing" | "review" => Some("reviewing"),
        "correcting" | "correct" => Some("correcting"),
        "verifying" | "verify" => Some("verifying"),
        "blocked" => Some("blocked"),
        "done" | "completed" | "complete" | "success" | "succeeded" | "idle" => Some("done"),
        _ => None,
    }
}

fn normalized_decision_kind(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "continue" => Some("continue"),
        "request_context" | "requestcontext" => Some("request_context"),
        "call_tool" | "calltool" | "tool" => Some("call_tool"),
        "handoff" => Some("handoff"),
        "accept" => Some("accept"),
        "reject" => Some("reject"),
        "done" | "complete" | "completed" | "finish" | "finished" => Some("done"),
        _ => None,
    }
}

fn infer_decision_kind(
    explicit: Option<&str>,
    state: Option<&str>,
    needed_context: Option<&Vec<Value>>,
    actions: Option<&Vec<Value>>,
) -> &'static str {
    if let Some(kind) = explicit.and_then(normalized_decision_kind) {
        return kind;
    }
    if needed_context
        .map(|items| !items.is_empty())
        .unwrap_or(false)
    {
        return "request_context";
    }
    if actions.map(|items| !items.is_empty()).unwrap_or(false) {
        return "call_tool";
    }
    if matches!(state.and_then(normalized_agent_state), Some("done")) {
        return "done";
    }
    "continue"
}

fn normalize_state_patch(
    patch: Option<&Map<String, Value>>,
    root: &Map<String, Value>,
) -> Option<Value> {
    let mut normalized = Map::new();
    let accepted_summary = patch
        .and_then(|m| {
            m.get("accepted_summary_add")
                .or_else(|| m.get("accepted_summary"))
        })
        .or_else(|| {
            root.get("accepted_summary_add")
                .or_else(|| root.get("accepted_summary"))
        });
    let open_items_add = patch
        .and_then(|m| m.get("open_items_add").or_else(|| m.get("open_items")))
        .or_else(|| {
            root.get("open_items_add")
                .or_else(|| root.get("open_items"))
        });
    let open_items_remove = patch.and_then(|m| m.get("open_items_remove"));

    if let Some(value) = accepted_summary {
        normalized.insert("accepted_summary_add".into(), value.clone());
    }
    if let Some(value) = open_items_add {
        normalized.insert("open_items_add".into(), value.clone());
    }
    if let Some(value) = open_items_remove {
        normalized.insert("open_items_remove".into(), value.clone());
    }

    if normalized.is_empty() {
        None
    } else {
        Some(Value::Object(normalized))
    }
}

fn normalize_state_decision_value(value: Value) -> Result<Value, String> {
    let root = value
        .as_object()
        .ok_or_else(|| "StateDecision must be a JSON object".to_string())?;
    let nested = root.get("decision").and_then(Value::as_object);

    let state_raw = root
        .get("state")
        .and_then(Value::as_str)
        .or_else(|| nested.and_then(|m| m.get("state").and_then(Value::as_str)))
        .or_else(|| nested.and_then(|m| m.get("next_state").and_then(Value::as_str)));
    let state = state_raw
        .and_then(normalized_agent_state)
        .ok_or_else(|| "missing or unsupported state/next_state".to_string())?;

    let needed_context = root
        .get("needed_context")
        .and_then(Value::as_array)
        .or_else(|| nested.and_then(|m| m.get("needed_context").and_then(Value::as_array)));
    let actions = root
        .get("actions")
        .and_then(Value::as_array)
        .or_else(|| nested.and_then(|m| m.get("actions").and_then(Value::as_array)));
    let decision_raw = root
        .get("decision")
        .and_then(Value::as_str)
        .or_else(|| nested.and_then(|m| m.get("decision").and_then(Value::as_str)));
    let decision = infer_decision_kind(decision_raw, Some(state), needed_context, actions);

    let mut normalized = Map::new();
    normalized.insert("state".into(), Value::String(state.to_string()));
    normalized.insert("decision".into(), Value::String(decision.to_string()));

    if let Some(next_action) = root
        .get("next_action")
        .cloned()
        .or_else(|| nested.and_then(|m| m.get("next_action").cloned()))
    {
        normalized.insert("next_action".into(), next_action);
    }
    if let Some(items) = needed_context {
        normalized.insert("needed_context".into(), Value::Array(items.clone()));
    }
    if let Some(patch) =
        normalize_state_patch(root.get("state_patch").and_then(Value::as_object), root)
    {
        normalized.insert("state_patch".into(), patch);
    }
    if let Some(confidence) = root
        .get("confidence")
        .cloned()
        .or_else(|| nested.and_then(|m| m.get("confidence").cloned()))
    {
        normalized.insert("confidence".into(), confidence);
    }
    if let Some(escalate) = root
        .get("escalate")
        .cloned()
        .or_else(|| nested.and_then(|m| m.get("escalate").cloned()))
    {
        normalized.insert("escalate".into(), escalate);
    }

    Ok(Value::Object(normalized))
}

/// Parse and validate a JSON string as a `StateDecision`.
/// Pure function — no LLM calls, no side effects.
/// Returns `Err(RepairNeeded)` if the JSON is invalid or missing required fields.
pub fn validate_state_decision(json: &str) -> Result<StateDecision, RepairNeeded> {
    let parsed: Value = serde_json::from_str(json).map_err(|e| RepairNeeded {
        reason: format!("JSON parse error: {e}"),
        raw_json: json.to_string(),
    })?;
    let normalized = normalize_state_decision_value(parsed).map_err(|reason| RepairNeeded {
        reason,
        raw_json: json.to_string(),
    })?;
    let decision: StateDecision = serde_json::from_value(normalized).map_err(|e| RepairNeeded {
        reason: format!("JSON parse error: {e}"),
        raw_json: json.to_string(),
    })?;
    Ok(decision)
}

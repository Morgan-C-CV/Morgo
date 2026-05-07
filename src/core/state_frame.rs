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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionEvidenceStatus {
    Sufficient,
    MissingArtifactEvidence,
    MissingTestEvidence,
    MissingVerificationEvidence,
}

impl CompletionEvidenceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sufficient => "sufficient",
            Self::MissingArtifactEvidence => "missing_artifact_evidence",
            Self::MissingTestEvidence => "missing_test_evidence",
            Self::MissingVerificationEvidence => "missing_verification_evidence",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionEvidenceGap {
    pub target_ref: String,
    #[serde(default)]
    pub target_path: Option<String>,
    #[serde(default)]
    pub missing_artifact_evidence: bool,
    #[serde(default)]
    pub missing_test_evidence: bool,
    #[serde(default)]
    pub missing_verification_evidence: bool,
    pub recommended_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DeclaredArtifactContract {
    pub ref_id: String,
    pub path: String,
    pub kind: String,
    #[serde(default)]
    pub required_actions: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct VerificationContract {
    pub target_ref: String,
    #[serde(default)]
    pub target_path: Option<String>,
    #[serde(default)]
    pub required_actions: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TestContract {
    pub name: String,
    #[serde(default)]
    pub required_actions: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StageExecutionContract {
    #[serde(default)]
    pub declared_artifacts: Vec<DeclaredArtifactContract>,
    #[serde(default)]
    pub verifications: Vec<VerificationContract>,
    #[serde(default)]
    pub tests: Vec<TestContract>,
    #[serde(default)]
    pub content_evidence_targets: Vec<String>,
    #[serde(default)]
    pub required_actions: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
}

impl StageExecutionContract {
    pub fn declared_artifact_by_ref(&self, ref_id: &str) -> Option<&DeclaredArtifactContract> {
        self.declared_artifacts
            .iter()
            .find(|item| item.ref_id == ref_id)
    }

    pub fn declared_artifact_by_path(&self, path: &str) -> Option<&DeclaredArtifactContract> {
        self.declared_artifacts
            .iter()
            .find(|item| item.path == path)
    }

    pub fn verification_by_target_ref(&self, target_ref: &str) -> Option<&VerificationContract> {
        self.verifications
            .iter()
            .find(|item| item.target_ref == target_ref)
    }

    pub fn verification_by_target_path(&self, target_path: &str) -> Option<&VerificationContract> {
        self.verifications.iter().find(|item| {
            item.target_path
                .as_deref()
                .is_some_and(|path| path == target_path)
        })
    }

    pub fn test_by_name(&self, name: &str) -> Option<&TestContract> {
        self.tests.iter().find(|item| item.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityMode {
    Continue,
    Repair,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepairIntent {
    #[serde(default)]
    pub failed_target: Option<String>,
    #[serde(default)]
    pub verified_facts: Vec<String>,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub continuity_mode: Option<ContinuityMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StageContinuationContext {
    #[serde(default)]
    pub repair_intent: Option<RepairIntent>,
    #[serde(default)]
    pub failed_target: Option<String>,
    #[serde(default)]
    pub verified_facts: Vec<String>,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub continuity_mode: Option<ContinuityMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerStructuredReport {
    pub worker_state: AgentState,
    #[serde(default)]
    pub last_tool_action: Option<String>,
    #[serde(default)]
    pub files_changed: Vec<String>,
    #[serde(default)]
    pub tests_run: Vec<String>,
    pub artifact_status: String,
    pub test_status: String,
    pub verification_status: String,
    #[serde(default)]
    pub stage_execution_contract: StageExecutionContract,
    #[serde(default)]
    pub stage_continuation_context: Option<StageContinuationContext>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub completion_evidence_gaps: Vec<CompletionEvidenceGap>,
    #[serde(default)]
    pub remaining_risks: Vec<String>,
    pub completion_evidence_status: CompletionEvidenceStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionGateBlock {
    pub status: CompletionEvidenceStatus,
    pub required_action: String,
    pub reason: String,
    #[serde(default)]
    pub missing_evidence_refs: Vec<String>,
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
    pub stage_execution_contract: StageExecutionContract,
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
        "executing" | "execute" | "executed" | "execution" | "running" | "in_progress" => {
            Some("executing")
        }
        "reviewing" | "review" => Some("reviewing"),
        "correcting" | "correct" | "repairing" | "repair" => Some("correcting"),
        "verifying" | "verify" | "re_verify" | "reverify" => Some("verifying"),
        "awaiting_user_input" | "awaiting_input" | "needs_user_input" | "user_input" => {
            Some("correcting")
        }
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
        "done" | "complete" | "completed" | "finish" | "finished" | "success" | "succeeded" => {
            Some("done")
        }
        _ => None,
    }
}

fn infer_decision_kind(
    explicit: Option<&str>,
    state: Option<&str>,
    needed_context: Option<&Vec<Value>>,
    actions: Option<&Vec<Value>>,
    next_action_present: bool,
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
    if next_action_present {
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
    let next_action_present =
        root.get("next_action").is_some() || nested.and_then(|m| m.get("next_action")).is_some();
    let decision = infer_decision_kind(
        decision_raw,
        Some(state),
        needed_context,
        actions,
        next_action_present,
    );

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

#[cfg(test)]
mod tests {
    use super::{AgentState, DecisionKind, validate_state_decision};

    #[test]
    fn invalid_repair_json_is_normalized_into_headless_safe_continuation() {
        let decision = validate_state_decision(
            r#"{
                "type":"repair_response",
                "decision":{
                    "next_state":"awaiting_user_input",
                    "actions":[{"action_type":"Write","args":{"file_path":"/tmp/report.md","content":"done"}}],
                    "next_action":{"action_type":"Write","args":{"file_path":"/tmp/report.md","content":"done"}}
                }
            }"#,
        )
        .expect("repair wrapper should normalize");

        assert_eq!(decision.state, AgentState::Correcting);
        assert_eq!(decision.decision, DecisionKind::CallTool);
        let next_action = decision.next_action.expect("next action");
        assert_eq!(next_action.action_type, "Write");
    }

    #[test]
    fn executed_and_success_aliases_are_accepted_by_state_decision_validation() {
        let decision = validate_state_decision(
            r#"{
                "state":"executed",
                "decision":"success"
            }"#,
        )
        .expect("executed/success aliases should normalize");

        assert_eq!(decision.state, AgentState::Executing);
    }

    #[test]
    fn executed_with_completed_next_state_is_accepted_by_state_decision_validation() {
        let decision = validate_state_decision(
            r#"{
                "state":"executed",
                "next_state":"completed",
                "decision":"success"
            }"#,
        )
        .expect("completed alias should normalize");

        assert_eq!(decision.state, AgentState::Executing);
    }
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

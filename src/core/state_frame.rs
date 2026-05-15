use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::core::prompt_segment::{PromptAssembly, PromptSegment, PromptSegmentKind};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReviewMode {
    #[default]
    TargetVerification,
    IndependentReview,
}

impl ReviewMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TargetVerification => "target_verification",
            Self::IndependentReview => "independent_review",
        }
    }

    pub fn is_independent_review(self) -> bool {
        matches!(self, Self::IndependentReview)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskProfile {
    ReadOnlyAnalysis,
    #[default]
    IndependentReview,
    TargetVerification,
    CodeChange,
}

impl TaskProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnlyAnalysis => "read_only_analysis",
            Self::IndependentReview => "independent_review",
            Self::TargetVerification => "target_verification",
            Self::CodeChange => "code_change",
        }
    }
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
    pub review_mode: Option<ReviewMode>,
    #[serde(default)]
    pub task_profile: Option<TaskProfile>,
    #[serde(default)]
    pub requires_source_evidence: Option<bool>,
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

    pub fn task_profile(&self) -> Option<TaskProfile> {
        self.task_profile
    }

    pub fn requires_source_evidence(&self) -> Option<bool> {
        self.requires_source_evidence
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
    #[serde(default, skip_serializing, skip_deserializing)]
    pub runtime_open_items: Vec<String>,
}

impl StateFrame {
    /// Render as a non-cacheable `PromptSegment`.
    pub fn to_prompt_segment(&self) -> PromptSegment {
        let json = serde_json::to_string_pretty(self).unwrap_or_default();
        PromptSegment::new("state_frame_v1", PromptSegmentKind::StateFrame, json)
    }

    /// Render the StateFrame decision prompt as a cache-aware assembly.
    ///
    /// Stable contract fields are separated from high-churn runtime evidence so
    /// providers with prefix/block caching can reuse the immutable prefix across
    /// decision turns without hiding any dynamic feedback from the model.
    pub fn to_prompt_assembly(&self, instruction: impl Into<String>) -> PromptAssembly {
        let mut assembly = PromptAssembly::new();
        assembly.push(PromptSegment::new(
            "state_decision_instruction_v1",
            PromptSegmentKind::StaticSystem,
            instruction.into(),
        ));

        let stable = json!({
            "state_frame_static_v1": {
                "role": self.role,
                "objective": &self.objective,
                "stage_execution_contract": &self.stage_execution_contract,
                "allowed_actions": &self.allowed_actions,
                "allowed_tools": &self.allowed_tools,
                "toolset_id": &self.toolset_id,
                "skillset_id": &self.skillset_id,
                "required_output_schema": &self.required_output_schema,
                "budget": &self.budget,
            }
        });
        assembly.push(PromptSegment::new(
            "state_frame_static_v1",
            PromptSegmentKind::ActorBrief,
            serde_json::to_string_pretty(&stable).unwrap_or_default(),
        ));

        let (path_aliases, recent_evidence) = compact_prompt_recent_evidence(self);
        let dynamic = json!({
            "state_frame_dynamic_v1": {
                "state": self.state,
                "open_items": &self.open_items,
                "blocked_items": &self.blocked_items,
                "accepted_summary": &self.accepted_summary,
                "path_aliases": path_aliases,
                "recent_evidence": recent_evidence,
                "runtime_open_items": &self.runtime_open_items,
            }
        });
        assembly.push(PromptSegment::new(
            "state_frame_dynamic_v1",
            PromptSegmentKind::StateFrame,
            serde_json::to_string_pretty(&dynamic).unwrap_or_default(),
        ));
        assembly
    }
}

fn compact_prompt_recent_evidence(frame: &StateFrame) -> (BTreeMap<String, String>, Vec<String>) {
    let path_aliases = prompt_path_aliases(frame);
    let mut compacted = Vec::new();
    let mut seen = HashSet::new();
    let mut empty_ledgers = BTreeSet::new();
    let mut repeated_reads: BTreeMap<String, (usize, String)> = BTreeMap::new();
    let mut latest_read_hydration: BTreeMap<String, (usize, String)> = BTreeMap::new();

    for line in &frame.recent_evidence {
        if prompt_contract_fact_is_redundant(line) {
            continue;
        }
        if let Some(name) = empty_ledger_name(line) {
            empty_ledgers.insert(name.to_string());
            continue;
        }
        if prompt_write_read_hydration_is_redundant(line) {
            continue;
        }
        if let Some(path) = prompt_read_hydration_path(line) {
            let entry = latest_read_hydration
                .entry(path)
                .or_insert((0, String::new()));
            entry.0 += 1;
            entry.1 = line.clone();
            continue;
        }
        if let Some((path, ref_id)) = prompt_read_success_path_and_ref(line) {
            let entry = repeated_reads.entry(path).or_insert((0, String::new()));
            entry.0 += 1;
            entry.1 = ref_id;
            continue;
        }
        push_prompt_line(&mut compacted, &mut seen, line, &path_aliases);
    }

    if !empty_ledgers.is_empty() {
        let line = format!(
            "fact: empty_ledgers {}",
            empty_ledgers.into_iter().collect::<Vec<_>>().join(",")
        );
        push_prompt_line(&mut compacted, &mut seen, &line, &path_aliases);
    }

    for (path, (count, latest_ref)) in repeated_reads {
        let line = format!("fact: repeated_read path={path} count={count} latest_ref={latest_ref}");
        push_prompt_line(&mut compacted, &mut seen, &line, &path_aliases);
    }
    for (_path, (count, latest_line)) in latest_read_hydration {
        push_prompt_line(&mut compacted, &mut seen, &latest_line, &path_aliases);
        if count > 1 {
            let line = format!("fact: repeated_read_hydration count={count}");
            push_prompt_line(&mut compacted, &mut seen, &line, &path_aliases);
        }
    }

    (path_aliases, compacted)
}

fn push_prompt_line(
    items: &mut Vec<String>,
    seen: &mut HashSet<String>,
    line: &str,
    aliases: &BTreeMap<String, String>,
) {
    let line = apply_prompt_path_aliases(line, aliases);
    if seen.insert(line.clone()) {
        items.push(line);
    }
}

fn prompt_contract_fact_is_redundant(line: &str) -> bool {
    line.starts_with("fact: review_mode ")
        || line.starts_with("fact: task_profile ")
        || line.starts_with("fact: requires_source_evidence ")
        || line.starts_with("fact: declared_artifact_contract ")
        || line.starts_with("fact: required_actions ")
        || line.starts_with("fact: required_evidence ")
}

fn empty_ledger_name(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("fact: ")?;
    let (name, value) = rest.split_once(' ')?;
    let value = value.trim();
    if value == "none" || value == "none recorded" {
        Some(name)
    } else {
        None
    }
}

fn prompt_write_read_hydration_is_redundant(line: &str) -> bool {
    line.starts_with("hydrated_context: ")
        && line.contains(" source=tool:Write ")
        && line.contains(" match_reason=call_tool_read ")
}

fn prompt_read_hydration_path(line: &str) -> Option<String> {
    if !(line.starts_with("hydrated_context: ")
        && line.contains(" source=tool:Read ")
        && (line.contains(" match_reason=call_tool_read ")
            || line.contains(" match_reason=call_tool_edit ")))
    {
        return None;
    }
    line.strip_prefix("hydrated_context: file_snippet:")
        .and_then(|rest| rest.split_whitespace().next())
        .map(str::to_string)
        .filter(|path| !path.is_empty())
}

fn prompt_read_success_path_and_ref(line: &str) -> Option<(String, String)> {
    if !(line.starts_with("fact: file_facts ") && line.contains(" kind=read_observation ")) {
        return None;
    }
    let path = prompt_field_value(line, "path")?;
    let ref_id = prompt_field_value(line, "ref").unwrap_or_else(|| "unknown".into());
    Some((path, ref_id))
}

fn prompt_field_value(line: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
        .map(|value| value.trim().trim_matches(',').to_string())
        .filter(|value| !value.is_empty() && value != "none" && value != "none recorded")
}

fn prompt_path_aliases(frame: &StateFrame) -> BTreeMap<String, String> {
    let mut aliases = BTreeMap::new();
    if let Some(target) = frame.stage_execution_contract.declared_artifacts.first() {
        if !target.path.trim().is_empty() {
            aliases.insert("$TARGET_DIR".into(), target.path.clone());
        }
    }

    let mut paths = BTreeSet::new();
    collect_prompt_paths(&frame.objective, &mut paths);
    for item in frame
        .recent_evidence
        .iter()
        .chain(frame.open_items.iter())
        .chain(frame.accepted_summary.iter())
    {
        collect_prompt_paths(item, &mut paths);
    }
    for artifact in &frame.stage_execution_contract.declared_artifacts {
        collect_prompt_paths(&artifact.path, &mut paths);
        for evidence in &artifact.required_evidence {
            collect_prompt_paths(evidence, &mut paths);
        }
    }
    for verification in &frame.stage_execution_contract.verifications {
        if let Some(path) = verification.target_path.as_deref() {
            collect_prompt_paths(path, &mut paths);
        }
        for evidence in &verification.required_evidence {
            collect_prompt_paths(evidence, &mut paths);
        }
    }

    insert_first_matching_alias(&mut aliases, &paths, "$LOG_OFF", |path| {
        path.ends_with(".jsonl") && path.contains("-off-")
    });
    insert_first_matching_alias(&mut aliases, &paths, "$LOG_ON", |path| {
        path.ends_with(".jsonl") && path.contains("-on-")
    });
    insert_first_matching_alias(&mut aliases, &paths, "$VALIDATOR", |path| {
        path.ends_with("/validator.py")
    });
    insert_first_matching_alias(&mut aliases, &paths, "$SUMMARY", |path| {
        path.ends_with("/summary.txt")
    });
    insert_first_matching_alias(&mut aliases, &paths, "$CONCLUSION", |path| {
        path.ends_with("/conclusion.md")
    });
    insert_first_matching_alias(&mut aliases, &paths, "$RESULTS", |path| {
        path.ends_with("/validator_results.md")
    });

    aliases
}

fn insert_first_matching_alias<F>(
    aliases: &mut BTreeMap<String, String>,
    paths: &BTreeSet<String>,
    alias: &str,
    predicate: F,
) where
    F: Fn(&str) -> bool,
{
    if aliases.contains_key(alias) {
        return;
    }
    if let Some(path) = paths.iter().find(|path| predicate(path)) {
        aliases.insert(alias.into(), path.clone());
    }
}

fn collect_prompt_paths(text: &str, paths: &mut BTreeSet<String>) {
    for token in text.split(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | ',' | '|')) {
        let Some(start) = token.find('/') else {
            continue;
        };
        let candidate = token[start..]
            .trim_matches(|ch: char| {
                matches!(
                    ch,
                    '.' | ':' | ';' | ')' | '(' | ']' | '[' | '}' | '{' | '<' | '>' | '`'
                )
            })
            .to_string();
        if candidate.starts_with('/') && candidate.contains('/') && candidate.len() > 4 {
            paths.insert(candidate);
        }
    }
}

fn apply_prompt_path_aliases(line: &str, aliases: &BTreeMap<String, String>) -> String {
    let mut pairs = aliases
        .iter()
        .map(|(alias, path)| (alias.as_str(), path.as_str()))
        .collect::<Vec<_>>();
    pairs.sort_by(|(_, a), (_, b)| b.len().cmp(&a.len()));

    let mut out = line.to_string();
    for (alias, path) in pairs {
        if !path.is_empty() {
            out = out.replace(path, alias);
        }
    }
    out
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
    #[serde(default)]
    pub review_mode: Option<ReviewMode>,
    #[serde(default)]
    pub task_profile: Option<TaskProfile>,
    #[serde(default)]
    pub requires_source_evidence: Option<bool>,
    #[serde(default)]
    pub tests_add: Vec<TestContract>,
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

fn normalize_task_profile(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "readonly" | "read_only" | "read_only_analysis" | "read-only" | "read only analysis" => {
            Some("read_only_analysis")
        }
        "independent_review" | "independent review" | "review" => Some("independent_review"),
        "target_verification" | "target verification" | "verification" => {
            Some("target_verification")
        }
        "code_change" | "code change" | "implementation" | "implement" | "development" | "dev" => {
            Some("code_change")
        }
        _ => None,
    }
}

fn normalize_boolish(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" | "required" | "need" | "needed" => Some(true),
            "false" | "0" | "no" | "n" | "not_required" | "not required" | "optional" => {
                Some(false)
            }
            _ => None,
        },
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
    let review_mode = patch
        .and_then(|m| m.get("review_mode"))
        .or_else(|| root.get("review_mode"));
    let task_profile = patch
        .and_then(|m| m.get("task_profile").or_else(|| m.get("task_type")))
        .or_else(|| root.get("task_profile").or_else(|| root.get("task_type")));
    let requires_source_evidence = patch
        .and_then(|m| {
            m.get("requires_source_evidence")
                .or_else(|| m.get("source_evidence_required"))
                .or_else(|| m.get("requires_source_evidence_read"))
        })
        .or_else(|| {
            root.get("requires_source_evidence")
                .or_else(|| root.get("source_evidence_required"))
                .or_else(|| root.get("requires_source_evidence_read"))
        });
    let tests_add = patch
        .and_then(|m| m.get("tests_add"))
        .or_else(|| root.get("tests_add"));

    if let Some(value) = accepted_summary {
        normalized.insert("accepted_summary_add".into(), value.clone());
    }
    if let Some(value) = open_items_add {
        normalized.insert("open_items_add".into(), value.clone());
    }
    if let Some(value) = open_items_remove {
        normalized.insert("open_items_remove".into(), value.clone());
    }
    if let Some(value) = review_mode {
        let is_blank = value
            .as_str()
            .map(|mode| mode.trim().is_empty())
            .unwrap_or(false);
        if !is_blank {
            normalized.insert("review_mode".into(), value.clone());
        }
    }
    if let Some(value) = tests_add {
        normalized.insert("tests_add".into(), value.clone());
    }
    if let Some(value) = task_profile {
        if let Some(profile) = value.as_str().and_then(normalize_task_profile) {
            normalized.insert("task_profile".into(), Value::String(profile.into()));
        }
    }
    if let Some(value) = requires_source_evidence {
        if let Some(flag) = normalize_boolish(value) {
            normalized.insert("requires_source_evidence".into(), Value::Bool(flag));
        }
    }

    if normalized.is_empty() {
        None
    } else {
        Some(Value::Object(normalized))
    }
}

fn normalize_next_action_value(value: Value) -> Value {
    match value {
        Value::String(action_type) => Value::Object(
            [
                (
                    "action_type".into(),
                    Value::String(action_type.trim().to_string()),
                ),
                ("args".into(), Value::Object(Map::new())),
            ]
            .into_iter()
            .collect(),
        ),
        other => other,
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
        normalized.insert(
            "next_action".into(),
            normalize_next_action_value(next_action),
        );
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
    use super::{
        ActorRole, AgentState, DecisionKind, DeclaredArtifactContract, ReviewMode,
        StageExecutionContract, StateBudget, StateFrame, TaskProfile, validate_state_decision,
    };
    use crate::core::prompt_segment::PromptSegmentKind;
    use serde_json::{Map, Value};

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

    #[test]
    fn string_next_action_is_normalized_into_object_shape() {
        let decision = validate_state_decision(
            r#"{
                "state":"executing",
                "decision":"call_tool",
                "next_action":"invoke_parsing_tool"
            }"#,
        )
        .expect("string next_action should normalize");

        assert_eq!(decision.state, AgentState::Executing);
        assert_eq!(decision.decision, DecisionKind::CallTool);
        let next_action = decision.next_action.expect("next action");
        assert_eq!(next_action.action_type, "invoke_parsing_tool");
        assert_eq!(next_action.args, Value::Object(Map::new()));
    }

    #[test]
    fn done_with_extra_next_action_preserves_summary_without_repair() {
        let decision = validate_state_decision(
            r#"{
                "state":"done",
                "decision":"done",
                "next_action":{"action_type":"SendMessage","args":{}},
                "state_patch":{"accepted_summary_add":["现状：保留有效审计结论。","主要风险：不要因多余 next_action 触发重写。","证据来源：StateDecision normalization。","下一步建议：直接完成。"]}
            }"#,
        )
        .expect("done next_action should be ignored");

        assert_eq!(decision.state, AgentState::Done);
        assert_eq!(decision.decision, DecisionKind::Done);
        assert!(decision.next_action.is_none());
        assert_eq!(decision.state_patch.accepted_summary_add.len(), 4);
        assert_eq!(
            decision.state_patch.accepted_summary_add[0],
            "现状：保留有效审计结论。"
        );
    }

    #[test]
    fn state_decision_accepts_review_mode_patch() {
        let decision = validate_state_decision(
            r#"{
                "state":"executing",
                "decision":"call_tool",
                "next_action":{"action_type":"Read","args":{"file_path":"/tmp/report.md"}},
                "state_patch":{"review_mode":"independent_review"}
            }"#,
        )
        .expect("review_mode patch should normalize");

        assert_eq!(
            decision.state_patch.review_mode,
            Some(ReviewMode::IndependentReview)
        );
    }

    #[test]
    fn state_decision_ignores_blank_review_mode_patch() {
        let decision = validate_state_decision(
            r#"{
                "state":"executing",
                "decision":"call_tool",
                "next_action":{"action_type":"Read","args":{"file_path":"/tmp/report.md"}},
                "state_patch":{"review_mode":""}
            }"#,
        )
        .expect("blank review_mode should be ignored");

        assert_eq!(decision.state_patch.review_mode, None);
    }

    #[test]
    fn state_decision_accepts_explicit_test_contract_patch() {
        let decision = validate_state_decision(
            r#"{
                "state":"executing",
                "decision":"call_tool",
                "next_action":{"action_type":"Bash","args":{"command":"python validator.py sample.jsonl"}},
                "state_patch":{"tests_add":[{"name":"runtime_validation","required_actions":["run_test"],"required_evidence":["runtime_test_passed"]}]}
            }"#,
        )
        .expect("explicit test contract patch should normalize");

        assert_eq!(decision.state_patch.tests_add.len(), 1);
        assert_eq!(decision.state_patch.tests_add[0].name, "runtime_validation");
        assert_eq!(
            decision.state_patch.tests_add[0].required_evidence,
            vec!["runtime_test_passed".to_string()]
        );
    }

    #[test]
    fn state_decision_accepts_typed_task_profile_patch() {
        let decision = validate_state_decision(
            r#"{
                "state":"executing",
                "decision":"continue",
                "state_patch":{
                    "task_profile":"code_change",
                    "requires_source_evidence":false
                }
            }"#,
        )
        .expect("typed task profile patch should normalize");

        assert_eq!(
            decision.state_patch.task_profile,
            Some(TaskProfile::CodeChange)
        );
        assert_eq!(decision.state_patch.requires_source_evidence, Some(false));
    }

    #[test]
    fn state_frame_prompt_assembly_keeps_recent_evidence_dynamic() {
        let mut frame = StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "write report".into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: vec!["open".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec!["first dynamic fact".into()],
            allowed_actions: vec!["write_file".into()],
            allowed_tools: vec!["Write".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: None,
            budget: StateBudget::default(),
            runtime_open_items: Vec::new(),
        };
        let first = frame.to_prompt_assembly("stable instruction");
        frame.recent_evidence.push("second dynamic fact".into());
        let second = frame.to_prompt_assembly("stable instruction");

        assert_eq!(
            first.stable_prefix_fingerprint(),
            second.stable_prefix_fingerprint()
        );
        assert!(
            first
                .segments()
                .iter()
                .filter(|segment| segment.is_cacheable())
                .all(|segment| !segment.content.contains("first dynamic fact"))
        );
        assert!(
            second
                .segments()
                .iter()
                .any(|segment| segment.kind == PromptSegmentKind::StateFrame
                    && segment.content.contains("second dynamic fact"))
        );
    }

    #[test]
    fn prompt_assembly_compacts_redundant_contract_facts_without_mutating_frame() {
        let mut frame = StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "write report".into(),
            stage_execution_contract: StageExecutionContract {
                review_mode: Some(ReviewMode::IndependentReview),
                task_profile: Some(TaskProfile::IndependentReview),
                requires_source_evidence: Some(false),
                declared_artifacts: vec![DeclaredArtifactContract {
                    ref_id: "artifact:step0:0".into(),
                    path: "/private/tmp/example-target".into(),
                    kind: "directory".into(),
                    required_actions: vec!["create".into(), "write".into()],
                    required_evidence: vec!["artifact:step0:0".into()],
                }],
                ..StageExecutionContract::default()
            },
            open_items: Vec::new(),
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: review_mode independent_review".into(),
                "fact: task_profile independent_review".into(),
                "fact: requires_source_evidence not_required".into(),
                "fact: declared_artifact_contract ref=artifact:step0:0 path=/private/tmp/example-target kind=directory required_actions=create | write required_evidence=artifact:step0:0".into(),
                "fact: test_failures none recorded".into(),
                "fact: review_verdicts none recorded".into(),
                "tool_outcome: ref=tool_outcome:1 tool=Write kind=success recoverable=false path=/private/tmp/example-target/validator.py".into(),
            ],
            allowed_actions: vec!["write_file".into()],
            allowed_tools: vec!["Write".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: None,
            budget: StateBudget::default(),
            runtime_open_items: Vec::new(),
        };
        let original = frame.recent_evidence.clone();
        let assembly = frame.to_prompt_assembly("stable instruction");
        frame.recent_evidence.push("later mutation".into());
        let prompt = assembly.assemble();

        assert_eq!(&original[..], &frame.recent_evidence[..original.len()]);
        assert!(prompt.contains("\"path_aliases\""));
        assert!(prompt.contains("$TARGET_DIR"));
        assert!(prompt.contains("$VALIDATOR"));
        assert!(prompt.contains("fact: empty_ledgers review_verdicts,test_failures"));
        assert!(!prompt.contains("fact: review_mode independent_review"));
        assert!(!prompt.contains("fact: declared_artifact_contract ref=artifact:step0:0"));
        assert!(prompt.contains("tool_outcome: ref=tool_outcome:1"));
    }

    #[test]
    fn prompt_assembly_removes_redundant_write_read_hydration_and_compacts_repeated_reads() {
        let frame = StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "write report".into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: Vec::new(),
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "hydrated_context: file_snippet:/private/tmp/demo/validator.py source=tool:Write match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:1:read source=tool:Write source_event_id=tool-read:runtime:1 freshness=after-runtime-read excerpt=wrote /private/tmp/demo/validator.py".into(),
                "hydrated_context: file_snippet:/private/tmp/demo/validator.py source=tool:Write match_reason=call_tool_edit trace=fact_name=file_facts ref=filefact:runtime:1:edit source=tool:Write source_event_id=tool-edit:runtime:1 freshness=after-runtime-edit excerpt=wrote /private/tmp/demo/validator.py".into(),
                "fact: file_facts ref=filefact:runtime:7:read:0 path=/private/tmp/demo/summary.txt kind=read_observation source=tool:Read source_event_id=tool-read:runtime:7 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /private/tmp/demo/summary.txt".into(),
                "fact: file_facts ref=filefact:runtime:8:read:0 path=/private/tmp/demo/summary.txt kind=read_observation source=tool:Read source_event_id=tool-read:runtime:8 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /private/tmp/demo/summary.txt".into(),
                "hydrated_context: file_snippet:/private/tmp/demo/summary.txt source=tool:Read match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:7:read source=tool:Read source_event_id=tool-read:runtime:7 freshness=after-runtime-read excerpt=old".into(),
                "hydrated_context: file_snippet:/private/tmp/demo/summary.txt source=tool:Read match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:8:read source=tool:Read source_event_id=tool-read:runtime:8 freshness=after-runtime-read excerpt=new".into(),
            ],
            allowed_actions: Vec::new(),
            allowed_tools: Vec::new(),
            toolset_id: None,
            skillset_id: None,
            required_output_schema: None,
            budget: StateBudget::default(),
            runtime_open_items: Vec::new(),
        };
        let prompt = frame.to_prompt_assembly("stable instruction").assemble();

        assert!(!prompt.contains("source=tool:Write match_reason=call_tool_read"));
        assert!(prompt.contains("match_reason=call_tool_edit"));
        assert!(prompt.contains(
            "fact: repeated_read path=$SUMMARY count=2 latest_ref=filefact:runtime:8:read:0"
        ));
        assert!(!prompt.contains("excerpt=old"));
        assert!(prompt.contains("excerpt=new"));
        assert!(prompt.contains("fact: repeated_read_hydration count=2"));
        assert!(!prompt.contains("\"$PATH1\""));
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

use crate::core::message::Message;
use crate::core::state_fact_ledger::{StepFactLedgers, append_runtime_tool_record, fact_lines_from_ledgers};
use crate::core::state_frame::{
    AgentState, DecisionKind, RepairNeeded, StateFrame, StatePatch, validate_state_decision,
};
use crate::core::state_frame_hydration::{
    NeededContextSelector, hydrate_needed_context, parse_needed_context_selector,
};
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::ObservableInput;
use crate::tool::definition::{ToolCall, ToolResult};
use crate::tool::orchestrator::build_execution_record;
use crate::tool::registry::ToolRegistry;
use crate::tool::result::ToolExecutionRecord;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct DecisionLoopConfig {
    /// Maximum number of decision iterations before giving up.
    pub max_iterations: usize,
    /// Maximum repair attempts per iteration when JSON parse fails.
    pub repair_budget: usize,
}

impl Default for DecisionLoopConfig {
    fn default() -> Self {
        Self {
            max_iterations: 5,
            repair_budget: 2,
        }
    }
}

/// Token usage accumulated across all LLM calls in a decision loop run.
#[derive(Debug, Clone, Default)]
pub struct LoopUsage {
    pub input_tokens: usize,
    pub uncached_input_tokens: usize,
    pub output_tokens: usize,
    pub cache_read_tokens: usize,
    pub cache_write_tokens: usize,
    pub original_prompt_chars: usize,
    pub sent_prompt_chars: usize,
    pub estimated_cost_micros_usd: u64,
    pub fallback_count: usize,
    pub fallback_tier: Option<String>,
    pub fallback_reason: Option<String>,
    pub hydration_count: usize,
    pub stale_ref_count: usize,
    pub hydration_ref_missing: usize,
    pub tool_dispatch_count: usize,
    pub tool_dispatch_success_count: usize,
    pub tool_dispatch_failure_count: usize,
    pub tool_dispatch_ref_write_count: usize,
    pub tool_dispatch_failure_taxonomy: BTreeMap<String, usize>,
    pub tool_execution_records: Vec<ToolExecutionRecord>,
}

#[derive(Debug, Clone)]
pub struct StateFrameToolRuntime {
    pub registry: ToolRegistry,
    pub permissions: ToolPermissionContext,
}

#[derive(Debug, Clone)]
pub enum LoopOutcome {
    Done {
        final_state: AgentState,
        usage: LoopUsage,
    },
    Rejected {
        reason: String,
        usage: LoopUsage,
    },
    MaxIterationsReached {
        last_state: AgentState,
        usage: LoopUsage,
    },
    NoProgress {
        last_state: AgentState,
        reason: String,
        usage: LoopUsage,
    },
    ToolDispatchFailed {
        last_state: AgentState,
        reason: String,
        usage: LoopUsage,
    },
    RepairExhausted {
        raw_json: String,
        reason: String,
        usage: LoopUsage,
    },
}

#[derive(Debug, Clone)]
struct CallToolDispatchError {
    reason: String,
    record: ToolExecutionRecord,
}

/// Collect text and token usage from a stream of events.
fn collect_text_and_usage(events: Vec<StreamEvent>) -> (String, LoopUsage, Option<String>) {
    let mut text = String::new();
    let mut usage = LoopUsage::default();
    let mut error_reason = None;
    for event in events {
        match event {
            StreamEvent::TextDelta(t) => text.push_str(&t),
            StreamEvent::Usage(u) => {
                usage.input_tokens += u.input_tokens;
                usage.uncached_input_tokens +=
                    u.input_tokens.saturating_sub(u.cache_read_input_tokens);
                usage.output_tokens += u.output_tokens;
                usage.cache_read_tokens += u.cache_read_input_tokens;
                usage.cache_write_tokens += u.cache_creation_input_tokens;
            }
            StreamEvent::Error(error) => {
                if error_reason.is_none() {
                    error_reason = Some(format!(
                        "provider_error provider={} kind={} message={} disposition={:?}",
                        error.provider_id, error.kind, error.message, error.disposition
                    ));
                }
            }
            _ => {}
        }
    }
    (text, usage, error_reason)
}

const STATE_DECISION_INSTRUCTION: &str = "\
You are an AI agent operating in StateFrame mode. \
Read the StateFrame JSON below and respond ONLY with valid StateDecision JSON.\n\
\n\
StateDecision schema:\n\
{\n\
  \"state\": \"<one of: planning, executing, reviewing, correcting, verifying, blocked, done>\",\n\
  \"decision\": \"<one of: continue, request_context, call_tool, handoff, accept, reject, done>\",\n\
  \"next_action\": {\"action_type\": \"Read\", \"args\": {\"file_path\": \"path/to/file.rs\"}},\n\
  \"needed_context\": [],\n\
  \"state_patch\": {\n\
    \"open_items_add\": [],\n\
    \"open_items_remove\": [],\n\
    \"accepted_summary_add\": []\n\
  },\n\
  \"confidence\": 0.9,\n\
  \"escalate\": false\n\
}\n\
\n\
Rules:\n\
- Use \"decision\": \"done\" when the objective is complete\n\
- Use \"decision\": \"continue\" only when you also change state or provide a non-empty state_patch that advances the frame\n\
- When adding accepted summary lines, use `state_patch.accepted_summary_add`; do not emit `accepted_summary` as a replacement field\n\
- When adding open items, use `state_patch.open_items_add`; do not emit `open_items` as a replacement field\n\
- Do NOT return wrapper payloads like `{ \"type\": ..., \"valid\": ..., \"decision\": {...} }`; return the canonical StateDecision object itself\n\
- If `recent_evidence` contains `fact: execution_mode read_only_analysis`, prefer a single-turn `done`; do not use `continue` just to outline or narrate your plan\n\
- If `required_output_schema` is `readonly_audit_4_paragraphs_v1`, return `decision=\"done\"` with exactly 4 `state_patch.accepted_summary_add` items, one each for `现状`、`主要风险`、`证据来源`、`下一步建议`\n\
- Treat `recent_evidence` entries prefixed with `fact:` as the authoritative Fact Ledger for this turn\n\
- If a fact entry already says `none`, `none recorded`, `absent`, or equivalent, do NOT request that same context again\n\
- Only use \"decision\": \"request_context\" when the missing fact is not already present in objective/open_items/blocked_items/accepted_summary/recent_evidence\n\
- Use \"decision\": \"call_tool\" when you need a concrete runtime action before you can continue; always include `next_action.action_type` and structured `next_action.args`\n\
- In the current runtime, `call_tool` is expected to use real worker tools. Prefer narrow `Read` calls with exact `file_path`, use `Bash` only for concrete commands, and use `Edit` with exact `file_path` / `old_string` / `new_string`\n\
- Never call `Edit` unless you already know the exact replacement span. If `old_string` is missing, empty, or uncertain, first `Read` the target file and then issue `Edit` with the exact `old_string`\n\
- If a prior `call_tool` failed, read the `tool_feedback:` / `recent_output_ref:` lines in `recent_evidence`, diagnose the reason, and choose the next action accordingly\n\
- If `tool_feedback` says `category=schema_invalid`, rewrite the tool call using canonical argument names before retrying: `Bash.command`, `Read.file_path`, `Edit.file_path/old_string/new_string`\n\
- If `tool_feedback` says `category=missing_path`, do not repeat the same failing `Read`; first inspect `parent_path` or create the missing directory/file scaffold, then continue\n\
- You may retry a tool call when the failure looks transient or fixable, but do not blindly repeat the exact same failing action without changing args, path, command, or strategy\n\
- If a `Read` says a target path does not exist yet, inspect the parent path or create the needed directory/file before retrying the same `Read`\n\
- When using `needed_context`, prefer typed selectors like `file_snippet:path`, `test_failure`, `change_ref:path`, `review_ref:ref_id`, `artifact_ref:ref_id`, `open_item_ref:ref_id`, `blocker_ref:ref_id`, `rejected_approach:ref_id`, `artifact:path`, or `fact:name`\n\
- For implement tasks, do not use broad `Glob`/`Grep` exploration when a target path is already named; prefer `request_context:file_snippet:<path>` or a direct narrow `Read`\n\
- When `recent_evidence` contains `fallback_context:` or `fallback_context_item:` lines, consume that fallback evidence before requesting the same context again\n\
- The \"decision\" field MUST be one of the exact string values above — never use free text\n\
- Respond with JSON only, no prose or explanation\n\
\n\
StateFrame:";

fn push_unique(items: &mut Vec<String>, value: String) -> bool {
    if items.iter().any(|item| item == &value) {
        return false;
    }
    items.push(value);
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FallbackTier {
    RecentLocalHistory,
    FullContext,
}

impl FallbackTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::RecentLocalHistory => "recent_local_history",
            Self::FullContext => "full_context",
        }
    }
}

#[derive(Debug, Default)]
struct FallbackLadderState {
    recent_local_history_activated: bool,
    full_context_activated: bool,
}

fn compact_excerpt(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut iter = compact.chars();
    let excerpt = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{excerpt}...")
    } else {
        excerpt
    }
}

fn local_history_candidates(frame: &StateFrame, limit: usize) -> Vec<String> {
    let mut items = Vec::new();
    for line in frame.recent_evidence.iter().rev() {
        let looks_local = line.starts_with("hydrated_context:")
            || line.contains("source_event_id=tool-")
            || line.contains("freshness=after-runtime")
            || line.contains("freshness=after-worker-output")
            || line.contains("recent_output_ref");
        if !looks_local {
            continue;
        }
        let excerpt = compact_excerpt(line, 180);
        if !items.iter().any(|existing| existing == &excerpt) {
            items.push(excerpt);
        }
        if items.len() >= limit {
            break;
        }
    }
    items.reverse();
    items
}

fn activate_recent_local_history_fallback(frame: &mut StateFrame, requested: &[String]) -> bool {
    let requested_summary = fallback_requested_summary(requested);
    let mut changed = push_unique(
        &mut frame.recent_evidence,
        format!(
            "fallback_context: tier=recent_local_history reason=request_context_unresolved requested={requested_summary}"
        ),
    );
    for item in local_history_candidates(frame, 3) {
        changed |= push_unique(
            &mut frame.recent_evidence,
            format!(
                "fallback_context_item: tier=recent_local_history source=recent_evidence excerpt={item}"
            ),
        );
    }
    changed
}

fn activate_full_context_fallback(frame: &mut StateFrame, requested: &[String]) -> bool {
    let requested_summary = fallback_requested_summary(requested);
    let mut changed = push_unique(
        &mut frame.recent_evidence,
        format!(
            "fallback_context: tier=full_context reason=request_context_exhausted requested={requested_summary}"
        ),
    );
    changed |= push_unique(
        &mut frame.recent_evidence,
        format!(
            "fallback_context_item: tier=full_context source=objective excerpt={}",
            compact_excerpt(&frame.objective, 180)
        ),
    );
    if !frame.open_items.is_empty() {
        changed |= push_unique(
            &mut frame.recent_evidence,
            format!(
                "fallback_context_item: tier=full_context source=open_items excerpt={}",
                compact_excerpt(&frame.open_items.join(" | "), 180)
            ),
        );
    }
    if !frame.blocked_items.is_empty() {
        changed |= push_unique(
            &mut frame.recent_evidence,
            format!(
                "fallback_context_item: tier=full_context source=blocked_items excerpt={}",
                compact_excerpt(&frame.blocked_items.join(" | "), 180)
            ),
        );
    }
    if !frame.accepted_summary.is_empty() {
        changed |= push_unique(
            &mut frame.recent_evidence,
            format!(
                "fallback_context_item: tier=full_context source=accepted_summary excerpt={}",
                compact_excerpt(&frame.accepted_summary.join(" | "), 180)
            ),
        );
    }
    changed
}

fn activate_fallback_tier(
    frame: &mut StateFrame,
    requested: &[String],
    ladder: &mut FallbackLadderState,
    escalate: bool,
) -> Option<FallbackTier> {
    if escalate && !ladder.full_context_activated {
        if activate_full_context_fallback(frame, requested) {
            ladder.full_context_activated = true;
            return Some(FallbackTier::FullContext);
        }
        ladder.full_context_activated = true;
    }
    if !ladder.recent_local_history_activated {
        if activate_recent_local_history_fallback(frame, requested) {
            ladder.recent_local_history_activated = true;
            return Some(FallbackTier::RecentLocalHistory);
        }
        ladder.recent_local_history_activated = true;
    }
    if !ladder.full_context_activated {
        if activate_full_context_fallback(frame, requested) {
            ladder.full_context_activated = true;
            return Some(FallbackTier::FullContext);
        }
        ladder.full_context_activated = true;
    }
    None
}

fn fallback_requested_summary(requested: &[String]) -> String {
    if requested.is_empty() {
        "none".to_string()
    } else {
        requested.join("|")
    }
}

fn fallback_reason_label(tier: FallbackTier, requested: &[String], escalate: bool) -> String {
    let base = match tier {
        FallbackTier::RecentLocalHistory => "request_context_unresolved",
        FallbackTier::FullContext if escalate => "request_context_escalated",
        FallbackTier::FullContext => "request_context_exhausted",
    };
    format!("{base}:{}", fallback_requested_summary(requested))
}

fn apply_state_patch(frame: &mut StateFrame, patch: &StatePatch) -> bool {
    let mut changed = false;
    for item in &patch.open_items_add {
        changed |= push_unique(&mut frame.open_items, item.clone());
    }
    for item in &patch.open_items_remove {
        let before = frame.open_items.len();
        frame.open_items.retain(|existing| existing != item);
        changed |= frame.open_items.len() != before;
    }
    for item in &patch.accepted_summary_add {
        changed |= push_unique(&mut frame.accepted_summary, item.clone());
    }
    changed
}

fn requires_readonly_audit_contract(frame: &StateFrame) -> bool {
    frame.required_output_schema.as_deref() == Some("readonly_audit_4_paragraphs_v1")
}

fn validate_decision_for_frame(
    frame: &StateFrame,
    decision: &crate::core::state_frame::StateDecision,
) -> Result<(), RepairNeeded> {
    if !requires_readonly_audit_contract(frame) {
        if decision.decision == DecisionKind::CallTool {
            let Some(next_action) = decision.next_action.as_ref() else {
                return Err(RepairNeeded {
                    reason: "call_tool requires next_action".into(),
                    raw_json: String::new(),
                });
            };
            if next_action.action_type.trim().is_empty() {
                return Err(RepairNeeded {
                    reason: "call_tool requires non-empty next_action.action_type".into(),
                    raw_json: String::new(),
                });
            }
            if next_action.action_type.eq_ignore_ascii_case("Edit")
                && parse_edit_old_string(decision).is_none()
            {
                return Err(RepairNeeded {
                    reason: "Edit requires exact non-empty old_string; if you do not yet know the replacement span, request Read first".into(),
                    raw_json: String::new(),
                });
            }
        }
        return Ok(());
    }

    if decision.decision != DecisionKind::Done {
        return Err(RepairNeeded {
            reason: "readonly_audit_4_paragraphs_v1 requires decision=done".into(),
            raw_json: String::new(),
        });
    }

    let sections = &decision.state_patch.accepted_summary_add;
    if sections.len() != 4 {
        return Err(RepairNeeded {
            reason: format!(
                "readonly_audit_4_paragraphs_v1 requires exactly 4 accepted_summary_add items; got {}",
                sections.len()
            ),
            raw_json: String::new(),
        });
    }

    for item in sections {
        if item.trim().is_empty() {
            return Err(RepairNeeded {
                reason: "readonly_audit_4_paragraphs_v1 does not allow empty paragraph items"
                    .into(),
                raw_json: String::new(),
            });
        }
    }

    Ok(())
}

fn parse_and_validate_decision(
    frame: &StateFrame,
    text: &str,
) -> Result<crate::core::state_frame::StateDecision, RepairNeeded> {
    let decision = validate_state_decision(text)?;
    validate_decision_for_frame(frame, &decision).map_err(|mut err| {
        err.raw_json = text.to_string();
        err
    })?;
    Ok(decision)
}

fn compact_tool_excerpt(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut iter = compact.chars();
    let excerpt = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{excerpt}...")
    } else {
        excerpt
    }
}

fn parse_read_path(decision: &crate::core::state_frame::StateDecision) -> Option<String> {
    let next_action = decision.next_action.as_ref()?;
    if !next_action.action_type.eq_ignore_ascii_case("Read") {
        return None;
    }
    if let Some(path) = next_action.args.get("file_path").and_then(|v| v.as_str()) {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(path) = next_action.args.get("path").and_then(|v| v.as_str()) {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let raw = next_action.args.as_str()?.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn parse_edit_path(decision: &crate::core::state_frame::StateDecision) -> Option<String> {
    let next_action = decision.next_action.as_ref()?;
    if !next_action.action_type.eq_ignore_ascii_case("Edit") {
        return None;
    }
    let path = next_action
        .args
        .get("file_path")
        .and_then(|v| v.as_str())
        .or_else(|| next_action.args.get("path").and_then(|v| v.as_str()))?;
    let trimmed = path.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_edit_old_string(decision: &crate::core::state_frame::StateDecision) -> Option<String> {
    let next_action = decision.next_action.as_ref()?;
    if !next_action.action_type.eq_ignore_ascii_case("Edit") {
        return None;
    }
    let old_string = next_action
        .args
        .get("old_string")
        .and_then(|v| v.as_str())
        .or_else(|| next_action.args.get("old").and_then(|v| v.as_str()))?;
    let trimmed = old_string.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_bash_command(decision: &crate::core::state_frame::StateDecision) -> Option<String> {
    let next_action = decision.next_action.as_ref()?;
    if !next_action.action_type.eq_ignore_ascii_case("Bash") {
        return None;
    }
    if let Some(command) = next_action.args.get("command").and_then(|v| v.as_str()) {
        let trimmed = command.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(command) = next_action
        .args
        .get("Bash.command")
        .and_then(|v| v.as_str())
    {
        let trimmed = command.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(command) = next_action.args.get("cmd").and_then(|v| v.as_str()) {
        let trimmed = command.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let raw = next_action.args.as_str()?.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn tool_backed_hydration_path(requested: &[String]) -> Option<String> {
    requested.iter().find_map(|raw| match parse_needed_context_selector(raw) {
        NeededContextSelector::FileSnippet { path } => {
            let trimmed = path.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        NeededContextSelector::Artifact { path: Some(path) } => {
            let trimmed = path.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        _ => None,
    })
}

fn build_tool_backed_hydration_decision(
    state: AgentState,
    file_path: String,
) -> crate::core::state_frame::StateDecision {
    crate::core::state_frame::StateDecision {
        state,
        decision: DecisionKind::CallTool,
        next_action: Some(crate::core::state_frame::NextAction {
            action_type: "Read".into(),
            args: serde_json::json!({ "file_path": file_path }),
        }),
        needed_context: Vec::new(),
        state_patch: StatePatch::default(),
        confidence: 1.0,
        escalate: false,
    }
}

fn fact_field_value(line: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let start = line.find(&needle)? + needle.len();
    let tail = &line[start..];
    let end = tail.find(' ').unwrap_or(tail.len());
    let value = tail[..end].trim();
    (!value.is_empty() && value != "none").then(|| value.to_string())
}

fn initial_target_hydration_requests(frame: &StateFrame) -> Vec<String> {
    let mut requests = Vec::new();
    for line in &frame.recent_evidence {
        if !line.starts_with("fact: artifact_status ") {
            continue;
        }
        if !line.contains("source=artifact_expectation") {
            continue;
        }
        let Some(path) = fact_field_value(line, "path") else {
            continue;
        };
        if path.starts_with("command:") {
            continue;
        }
        let request = format!("artifact:{path}");
        if !requests.iter().any(|existing| existing == &request) {
            requests.push(request);
        }
    }
    requests
}

fn canonicalize_next_action_args(
    decision: &crate::core::state_frame::StateDecision,
) -> serde_json::Value {
    let Some(next_action) = decision.next_action.as_ref() else {
        return serde_json::Value::Null;
    };
    let mut args = next_action.args.clone();
    let Some(obj) = args.as_object_mut() else {
        return args;
    };
    if next_action.action_type.eq_ignore_ascii_case("Bash") {
        if !obj.contains_key("command") {
            if let Some(value) = obj
                .get("command")
                .or_else(|| obj.get("Bash.command"))
                .or_else(|| obj.get("cmd"))
                .cloned()
            {
                obj.insert("command".into(), value);
            }
        }
    }
    if next_action.action_type.eq_ignore_ascii_case("Read")
        || next_action.action_type.eq_ignore_ascii_case("Edit")
        || next_action.action_type.eq_ignore_ascii_case("Write")
    {
        if !obj.contains_key("file_path") {
            let dotted_key = format!("{}.file_path", next_action.action_type);
            if let Some(value) = obj
                .get("file_path")
                .or_else(|| obj.get(dotted_key.as_str()))
                .or_else(|| obj.get("path"))
                .cloned()
            {
                obj.insert("file_path".into(), value);
            }
        }
    }
    if next_action.action_type.eq_ignore_ascii_case("Edit") {
        if !obj.contains_key("old_string") {
            if let Some(value) = obj
                .get("old_string")
                .or_else(|| obj.get("Edit.old_string"))
                .or_else(|| obj.get("old"))
                .or_else(|| obj.get("Edit.old"))
                .cloned()
            {
                obj.insert("old_string".into(), value);
            }
        }
        if !obj.contains_key("new_string") {
            if let Some(value) = obj
                .get("new_string")
                .or_else(|| obj.get("Edit.new_string"))
                .or_else(|| obj.get("new"))
                .or_else(|| obj.get("Edit.new"))
                .cloned()
            {
                obj.insert("new_string".into(), value);
            }
        }
    }
    args
}

async fn execute_call_tool(
    frame: &mut StateFrame,
    decision: &crate::core::state_frame::StateDecision,
    tool_runtime: Option<&StateFrameToolRuntime>,
    dispatch_seq: &mut usize,
) -> Result<(bool, ToolExecutionRecord, usize), CallToolDispatchError> {
    let tool_runtime = tool_runtime.ok_or_else(|| {
        CallToolDispatchError {
            reason: "call_tool requested but StateFrame tool runtime is unavailable".to_string(),
            record: build_execution_record(
                decision
                    .next_action
                    .as_ref()
                    .map(|action| action.action_type.clone())
                    .unwrap_or_else(|| "unknown".into()),
                &ToolResult::Interrupted(
                    "call_tool requested but StateFrame tool runtime is unavailable".into(),
                ),
                None,
            ),
        }
    })?;
    let next_action = decision
        .next_action
        .as_ref()
        .ok_or_else(|| CallToolDispatchError {
            reason: "call_tool requested without next_action".to_string(),
            record: build_execution_record(
                "unknown",
                &ToolResult::Interrupted("call_tool requested without next_action".into()),
                None,
            ),
        })?;
    let canonical_args = canonicalize_next_action_args(decision);
    let input = if canonical_args.is_string() {
        canonical_args.as_str().unwrap_or_default().to_string()
    } else {
        serde_json::to_string(&canonical_args)
            .map_err(|error| CallToolDispatchError {
                reason: format!("failed to serialize tool args: {error}"),
                record: build_execution_record(
                    next_action.action_type.clone(),
                    &ToolResult::Interrupted(format!("failed to serialize tool args: {error}")),
                    None,
                ),
            })?
    };
    let call = ToolCall::new(next_action.action_type.clone(), input);
    let observable_input = tool_runtime.registry.observable_input(&call);
    let result = tool_runtime
        .registry
        .invoke(&call, &tool_runtime.permissions)
        .await
        .map_err(|error| CallToolDispatchError {
            reason: format!("tool dispatch failed: {error}"),
            record: build_execution_record(
                next_action.action_type.clone(),
                &ToolResult::Interrupted(format!("tool dispatch failed: {error}")),
                observable_input.clone(),
            ),
        })?;
    let record = build_execution_record(
        next_action.action_type.clone(),
        &result,
        observable_input.clone(),
    );
    *dispatch_seq += 1;
    let source_event_id = format!(
        "tool-{}:{}",
        next_action.action_type.to_ascii_lowercase(),
        *dispatch_seq
    );
    match result {
        ToolResult::Text(text) => {
            let mut changed = false;
            let mut ref_write_count = 0usize;
            let excerpt = compact_tool_excerpt(&text, 220);
            changed |= push_unique(
                &mut frame.recent_evidence,
                format!(
                    "recent_output_ref: ref=tool_output:{} tool={} source_event_id={} excerpt={}",
                    dispatch_seq, next_action.action_type, source_event_id, excerpt
                ),
            );
            let mut ledgers = StepFactLedgers::default();
            append_runtime_tool_record(&mut ledgers, &record, &format!("runtime:{}", dispatch_seq));
            let fact_lines = fact_lines_from_ledgers(&ledgers);
            ref_write_count += fact_lines.len();
            for line in fact_lines {
                changed |= push_unique(&mut frame.recent_evidence, line);
            }
            if let Some(path) = parse_read_path(decision).or_else(|| observable_path_from_input(observable_input.as_ref())) {
                changed |= push_unique(
                    &mut frame.recent_evidence,
                    format!(
                        "hydrated_context: file_snippet:{} source=tool:{} match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:{}:read source=tool:{} source_event_id=tool-read:runtime:{} freshness=after-runtime-read excerpt={}",
                        path,
                        next_action.action_type,
                        dispatch_seq,
                        next_action.action_type,
                        dispatch_seq,
                        excerpt
                    ),
                );
            }
            if let Some(path) = parse_edit_path(decision).or_else(|| observable_path_from_input(observable_input.as_ref())) {
                changed |= push_unique(
                    &mut frame.recent_evidence,
                    format!(
                        "hydrated_context: file_snippet:{} source=tool:{} match_reason=call_tool_edit trace=fact_name=file_facts ref=filefact:runtime:{}:edit source=tool:{} source_event_id=tool-edit:runtime:{} freshness=after-runtime-edit excerpt={}",
                        path,
                        next_action.action_type,
                        dispatch_seq,
                        next_action.action_type,
                        dispatch_seq,
                        excerpt
                    ),
                );
            }
            if let Some(command) = parse_bash_command(decision) {
                changed |= push_unique(
                    &mut frame.recent_evidence,
                    format!(
                        "recent_output_ref: ref=artifact:runtime:{}:bash tool={} source_event_id={} command_excerpt={}",
                        dispatch_seq,
                        next_action.action_type,
                        source_event_id,
                        compact_tool_excerpt(&command, 80)
                    ),
                );
            }
            Ok((changed, record, ref_write_count))
        }
        ToolResult::ResultTooLarge(message)
        | ToolResult::Interrupted(message)
        | ToolResult::Denied(message)
        | ToolResult::Progress(message) => Err(CallToolDispatchError {
            reason: format!(
                "call_tool {} did not produce usable text: {}",
                next_action.action_type, message
            ),
            record,
        }),
        ToolResult::PendingApproval { message, .. } => Err(CallToolDispatchError {
            reason: format!(
                "call_tool {} requires approval: {}",
                next_action.action_type, message
            ),
            record,
        }),
    }
}

fn observable_path_from_input(input: Option<&ObservableInput>) -> Option<String> {
    let raw = input?.value.as_str();
    let json: serde_json::Value = serde_json::from_str(raw).ok()?;
    json.get("file_path")
        .and_then(|value| value.as_str())
        .or_else(|| json.get("path").and_then(|value| value.as_str()))
        .map(str::to_string)
}

fn outcome_kind_label(kind: &crate::tool::result::ToolExecutionOutcomeKind) -> &'static str {
    match kind {
        crate::tool::result::ToolExecutionOutcomeKind::Success => "success",
        crate::tool::result::ToolExecutionOutcomeKind::Denied => "denied",
        crate::tool::result::ToolExecutionOutcomeKind::PendingApproval => "pending_approval",
        crate::tool::result::ToolExecutionOutcomeKind::Interrupted => "interrupted",
        crate::tool::result::ToolExecutionOutcomeKind::Progress => "progress",
        crate::tool::result::ToolExecutionOutcomeKind::ResultTooLarge => "result_too_large",
    }
}

fn push_tool_failure_feedback(
    frame: &mut StateFrame,
    decision: &crate::core::state_frame::StateDecision,
    record: &ToolExecutionRecord,
    dispatch_seq: usize,
    reason: &str,
) -> (bool, usize) {
    let mut changed = false;
    let category = classify_dispatch_failure(reason);
    let detail = compact_tool_excerpt(
        record
            .detail
            .as_deref()
            .unwrap_or_else(|| record.summary.as_str()),
        220,
    );
    let source_event_id = format!(
        "tool-{}:{}",
        record.tool_name.to_ascii_lowercase(),
        dispatch_seq
    );
    changed |= push_unique(
        &mut frame.recent_evidence,
        format!(
            "recent_output_ref: ref=tool_output:{} tool={} outcome={} category={} source_event_id={} excerpt={}",
            dispatch_seq,
            record.tool_name,
            outcome_kind_label(&record.kind),
            category,
            source_event_id,
            detail
        ),
    );

    let mut feedback_tail = String::new();
    if let Some(path) = parse_read_path(decision)
        .or_else(|| parse_edit_path(decision))
        .or_else(|| observable_path_from_input(record.observable_input.as_ref()))
    {
        feedback_tail.push_str(&format!(" path={path}"));
        if category == "missing_path" {
            if let Some(parent) = std::path::Path::new(&path).parent() {
                let parent = parent.to_string_lossy();
                if !parent.trim().is_empty() {
                    feedback_tail.push_str(&format!(" parent_path={parent}"));
                }
            }
        }
    }
    if let Some(command) = parse_bash_command(decision) {
        feedback_tail.push_str(&format!(
            " command_excerpt={}",
            compact_tool_excerpt(&command, 120)
        ));
    }
    if let Some(approval) = record.pending_approval.as_ref() {
        if let Some(code) = approval.code.as_deref() {
            feedback_tail.push_str(&format!(" approval_code={code}"));
        }
        if !approval.escalation_reasons.is_empty() {
            feedback_tail.push_str(&format!(
                " escalation_reasons={}",
                approval.escalation_reasons.join("|")
            ));
        }
    }
    changed |= push_unique(
        &mut frame.recent_evidence,
        format!(
            "tool_feedback: ref=tool_feedback:{} tool={} outcome={} category={} source_event_id={}{} summary={}",
            dispatch_seq,
            record.tool_name,
            outcome_kind_label(&record.kind),
            category,
            source_event_id,
            feedback_tail,
            detail
        ),
    );

    let mut ledgers = StepFactLedgers::default();
    append_runtime_tool_record(
        &mut ledgers,
        record,
        &format!("runtime:{}", dispatch_seq),
    );
    let fact_lines = fact_lines_from_ledgers(&ledgers);
    let ref_write_count = fact_lines.len();
    for line in fact_lines {
        changed |= push_unique(&mut frame.recent_evidence, line);
    }
    (changed, ref_write_count)
}

fn classify_dispatch_failure(reason: &str) -> String {
    let lowered = reason.to_ascii_lowercase();
    if lowered.contains("runtime is unavailable") {
        "tool_runtime_unavailable".into()
    } else if lowered.contains("unknown tool") {
        "tool_unavailable".into()
    } else if lowered.contains("no such file or directory")
        || lowered.contains("not found in ")
        || lowered.contains("not available: no such file")
    {
        "missing_path".into()
    } else if lowered.contains("requires approval") || lowered.contains(" denied") {
        "permission_denied".into()
    } else if lowered.contains("invalid input")
        || lowered.contains("serialize tool args")
        || lowered.contains("json-structured input")
        || lowered.contains("without next_action")
    {
        "schema_invalid".into()
    } else if lowered.contains("sandbox")
        || lowered.contains("capability")
        || lowered.contains("not allowed in plan mode")
    {
        "sandbox_blocked".into()
    } else if lowered.contains("did not produce usable text")
        || lowered.contains("no output")
        || lowered.contains("result too large")
    {
        "tool_result_empty".into()
    } else {
        "tool_interrupted".into()
    }
}

/// Run a stateless JSON decision loop.
///
/// Each iteration:
///   1. Renders `frame` as a prompt and calls the provider once (stateless).
///   2. Validates the response as `StateDecision` JSON.
///   3. Dispatches on `DecisionKind`: Continue / RequestContext / Done / Reject.
///   4. On parse failure, attempts repair up to `config.repair_budget` times.
///
/// Pure function — no AppState, no session actors, no side effects beyond the provider calls.
pub async fn run_decision_loop(
    client: &ModelProviderClient,
    frame: StateFrame,
    config: DecisionLoopConfig,
) -> anyhow::Result<LoopOutcome> {
    run_decision_loop_with_tools(client, frame, config, None).await
}

pub async fn run_decision_loop_with_tools(
    client: &ModelProviderClient,
    mut frame: StateFrame,
    config: DecisionLoopConfig,
    tool_runtime: Option<StateFrameToolRuntime>,
) -> anyhow::Result<LoopOutcome> {
    let mut total_usage = LoopUsage::default();
    let mut fallback_ladder = FallbackLadderState::default();
    let mut tool_dispatch_seq = 0usize;
    let initial_requests = initial_target_hydration_requests(&frame);
    let initial_hydration = hydrate_needed_context(&mut frame, &initial_requests);
    total_usage.hydration_count += initial_hydration.hydrated.len();
    total_usage.stale_ref_count += initial_hydration.stale.len();
    total_usage.hydration_ref_missing += initial_hydration.unavailable.len();

    for _iter in 0..config.max_iterations {
        let prompt = format!(
            "{}\n{}",
            STATE_DECISION_INSTRUCTION,
            frame.to_prompt_segment().content
        );
        let prompt_chars = prompt.chars().count();
        total_usage.original_prompt_chars += prompt_chars;
        total_usage.sent_prompt_chars += prompt_chars;
        let events = client.stream_message(&Message::user(prompt)).await;
        let (text, iter_usage, stream_error) = collect_text_and_usage(events);
        total_usage.input_tokens += iter_usage.input_tokens;
        total_usage.uncached_input_tokens += iter_usage.uncached_input_tokens;
        total_usage.output_tokens += iter_usage.output_tokens;
        total_usage.cache_read_tokens += iter_usage.cache_read_tokens;
        total_usage.cache_write_tokens += iter_usage.cache_write_tokens;
        if let Some(reason) = stream_error {
            if text.trim().is_empty() {
                return Ok(LoopOutcome::ToolDispatchFailed {
                    last_state: frame.state,
                    reason,
                    usage: total_usage,
                });
            }
        }

        // Repair loop: retry on JSON parse failure.
        let decision = match parse_and_validate_decision(&frame, &text) {
            Ok(d) => d,
            Err(first_repair) => {
                let mut last_repair = first_repair;
                let mut resolved = None;
                for _attempt in 0..config.repair_budget {
                    let repair_prompt = format!(
                        "Your previous response could not be parsed as StateDecision JSON.\n\
                         Error: {}\n\
                         Raw output: {}\n\
                         Please respond with valid StateDecision JSON only.",
                        last_repair.reason, last_repair.raw_json
                    );
                    let repair_prompt_chars = repair_prompt.chars().count();
                    total_usage.original_prompt_chars += repair_prompt_chars;
                    total_usage.sent_prompt_chars += repair_prompt_chars;
                    let repair_events = client.stream_message(&Message::user(repair_prompt)).await;
                    let (repaired_text, repair_usage, repair_error) =
                        collect_text_and_usage(repair_events);
                    total_usage.input_tokens += repair_usage.input_tokens;
                    total_usage.uncached_input_tokens += repair_usage.uncached_input_tokens;
                    total_usage.output_tokens += repair_usage.output_tokens;
                    total_usage.cache_read_tokens += repair_usage.cache_read_tokens;
                    total_usage.cache_write_tokens += repair_usage.cache_write_tokens;
                    if let Some(reason) = repair_error {
                        if repaired_text.trim().is_empty() {
                            return Ok(LoopOutcome::ToolDispatchFailed {
                                last_state: frame.state,
                                reason,
                                usage: total_usage,
                            });
                        }
                    }
                    match parse_and_validate_decision(&frame, &repaired_text) {
                        Ok(d) => {
                            resolved = Some(d);
                            break;
                        }
                        Err(r) => last_repair = r,
                    }
                }
                match resolved {
                    Some(d) => d,
                    None => {
                        return Ok(LoopOutcome::RepairExhausted {
                            raw_json: last_repair.raw_json,
                            reason: last_repair.reason,
                            usage: total_usage,
                        });
                    }
                }
            }
        };

        match decision.decision {
            DecisionKind::Done => {
                return Ok(LoopOutcome::Done {
                    final_state: decision.state,
                    usage: total_usage,
                });
            }
            DecisionKind::Reject => {
                let reason = decision
                    .next_action
                    .as_ref()
                    .and_then(|a| a.args.get("reason"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("rejected by model")
                    .to_string();
                return Ok(LoopOutcome::Rejected {
                    reason,
                    usage: total_usage,
                });
            }
            DecisionKind::Continue => {
                let before = frame.to_prompt_segment().content;
                let open_items_before = frame.open_items.len();
                let _patch_changed = apply_state_patch(&mut frame, &decision.state_patch);
                frame.state = decision.state;
                if open_items_before > 0
                    && frame.open_items.is_empty()
                    && frame.blocked_items.is_empty()
                {
                    return Ok(LoopOutcome::Done {
                        final_state: AgentState::Done,
                        usage: total_usage,
                    });
                }
                let after = frame.to_prompt_segment().content;
                if before == after {
                    return Ok(LoopOutcome::NoProgress {
                        last_state: frame.state,
                        reason: "continue decision made no StateFrame progress".into(),
                        usage: total_usage,
                    });
                }
            }
            DecisionKind::RequestContext => {
                let mut summary = hydrate_needed_context(&mut frame, &decision.needed_context);
                total_usage.hydration_count += summary.hydrated.len();
                total_usage.stale_ref_count += summary.stale.len();
                total_usage.hydration_ref_missing += summary.unavailable.len();
                frame.state = decision.state;
                if summary.hydrated.is_empty() {
                    if let Some(file_path) = tool_backed_hydration_path(&decision.needed_context) {
                        let synthetic_read =
                            build_tool_backed_hydration_decision(decision.state, file_path);
                        total_usage.tool_dispatch_count += 1;
                        match execute_call_tool(
                            &mut frame,
                            &synthetic_read,
                            tool_runtime.as_ref(),
                            &mut tool_dispatch_seq,
                        )
                        .await
                        {
                            Ok((changed, record, ref_write_count)) => {
                                total_usage.tool_dispatch_success_count += 1;
                                total_usage.tool_dispatch_ref_write_count += ref_write_count;
                                total_usage.tool_execution_records.push(record);
                                if changed {
                                    summary =
                                        hydrate_needed_context(&mut frame, &decision.needed_context);
                                    total_usage.hydration_count += summary.hydrated.len();
                                    total_usage.stale_ref_count += summary.stale.len();
                                    total_usage.hydration_ref_missing += summary.unavailable.len();
                                }
                            }
                            Err(error) => {
                                total_usage.tool_dispatch_failure_count += 1;
                                let category = classify_dispatch_failure(&error.reason);
                                *total_usage
                                    .tool_dispatch_failure_taxonomy
                                    .entry(category)
                                    .or_insert(0) += 1;
                                let (changed, ref_write_count) = push_tool_failure_feedback(
                                    &mut frame,
                                    &synthetic_read,
                                    &error.record,
                                    tool_dispatch_seq,
                                    &error.reason,
                                );
                                total_usage.tool_dispatch_ref_write_count += ref_write_count;
                                total_usage.tool_execution_records.push(error.record);
                                if changed {
                                    summary =
                                        hydrate_needed_context(&mut frame, &decision.needed_context);
                                    total_usage.hydration_count += summary.hydrated.len();
                                    total_usage.stale_ref_count += summary.stale.len();
                                    total_usage.hydration_ref_missing += summary.unavailable.len();
                                }
                            }
                        }
                    }
                    if summary.hydrated.is_empty() {
                        if let Some(fallback_tier) = activate_fallback_tier(
                            &mut frame,
                            &decision.needed_context,
                            &mut fallback_ladder,
                            decision.escalate,
                        ) {
                            total_usage.fallback_count += 1;
                            total_usage.fallback_tier = Some(fallback_tier.as_str().to_string());
                            total_usage.fallback_reason = Some(fallback_reason_label(
                                fallback_tier,
                                &decision.needed_context,
                                decision.escalate,
                            ));
                            continue;
                        }
                    }
                }
                if !summary.changed {
                    return Ok(LoopOutcome::NoProgress {
                        last_state: frame.state,
                        reason: "request_context decision produced no hydration progress".into(),
                        usage: total_usage,
                    });
                }
            }
            DecisionKind::CallTool => {
                frame.state = decision.state;
                total_usage.tool_dispatch_count += 1;
                match execute_call_tool(
                    &mut frame,
                    &decision,
                    tool_runtime.as_ref(),
                    &mut tool_dispatch_seq,
                )
                .await
                {
                    Ok((changed, record, ref_write_count)) => {
                        total_usage.tool_dispatch_success_count += 1;
                        total_usage.tool_dispatch_ref_write_count += ref_write_count;
                        total_usage.tool_execution_records.push(record);
                        if !changed {
                            return Ok(LoopOutcome::NoProgress {
                                last_state: frame.state,
                                reason: "call_tool decision produced no StateFrame progress".into(),
                                usage: total_usage,
                            });
                        }
                    }
                    Err(error) => {
                        total_usage.tool_dispatch_failure_count += 1;
                        let category = classify_dispatch_failure(&error.reason);
                        *total_usage
                            .tool_dispatch_failure_taxonomy
                            .entry(category)
                            .or_insert(0) += 1;
                        let (changed, ref_write_count) = push_tool_failure_feedback(
                            &mut frame,
                            &decision,
                            &error.record,
                            tool_dispatch_seq,
                            &error.reason,
                        );
                        total_usage.tool_dispatch_ref_write_count += ref_write_count;
                        total_usage.tool_execution_records.push(error.record);
                        if !changed {
                            return Ok(LoopOutcome::NoProgress {
                                last_state: frame.state,
                                reason: format!(
                                    "call_tool failure feedback produced no StateFrame progress: {}",
                                    error.reason
                                ),
                                usage: total_usage,
                            });
                        }
                    }
                }
            }
            _ => {
                frame.state = decision.state;
            }
        }
    }

    Ok(LoopOutcome::MaxIterationsReached {
        last_state: frame.state,
        usage: total_usage,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DecisionLoopConfig, LoopOutcome, StateFrameToolRuntime, execute_call_tool,
        parse_and_validate_decision, run_decision_loop, run_decision_loop_with_tools,
    };
    use crate::core::state_frame::validate_state_decision;
    use crate::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use crate::core::state_frame_hydration::hydrate_needed_context;
    use crate::service::api::client::ModelProviderClient;
    use crate::service::api::streaming::{ProviderFailureDisposition, StreamError, StreamEvent};
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use crate::tool::builtin::bash::BashTool;
    use crate::tool::builtin::file_edit::FileEditTool;
    use crate::tool::builtin::file_read::FileReadTool;
    use crate::tool::registry::ToolRegistry;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "update src/core/state_frame_projection.rs and get tests passing".into(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: vec!["worker must preserve prior review signal".into()],
            recent_evidence: vec![
                "fact: recent_changes_in_files ref=change:1 path=src/core/state_frame_projection.rs source=worker_result source_event_id=worker-result:1 freshness=after-worker-output confidence=0.90 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated src/core/state_frame_projection.rs".into(),
                "fact: test_failures ref=test:1 name=worker_reported_tests status=failed source=worker_result source_event_id=worker-result:2 freshness=after-worker-output confidence=0.85 status=active invalidated_by=none supersedes=none conflicts_with=none summary=tests failed in boss_flow".into(),
            ],
            allowed_actions: vec!["read_file".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
        }
    }

    fn unique_temp_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "stateframe_{}_{}_{}.txt",
            label,
            std::process::id(),
            nonce
        ))
    }

    #[test]
    fn request_context_unresolved_activates_recent_local_history_fallback() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["symbol:MissingSymbol"]}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.fallback_count, 1);
                assert_eq!(usage.hydration_ref_missing, 1);
                assert_eq!(usage.fallback_tier.as_deref(), Some("recent_local_history"));
                assert_eq!(
                    usage.fallback_reason.as_deref(),
                    Some("request_context_unresolved:symbol:MissingSymbol")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn repeated_request_context_or_escalate_reaches_full_context_fallback() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["symbol:MissingSymbol"]}"#;
        let escalate_json = r#"{"state":"executing","decision":"request_context","needed_context":["symbol:MissingSymbol"],"escalate":true}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(escalate_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                make_frame(),
                DecisionLoopConfig {
                    max_iterations: 6,
                    ..DecisionLoopConfig::default()
                },
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.fallback_count, 2);
                assert_eq!(usage.hydration_ref_missing, 2);
                assert_eq!(usage.fallback_tier.as_deref(), Some("full_context"));
                assert_eq!(
                    usage.fallback_reason.as_deref(),
                    Some("request_context_escalated:symbol:MissingSymbol")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn target_artifact_fact_is_hydrated_before_first_decision() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step0:0 path=/tmp/example-site kind=directory status=missing_or_invalid source=artifact_expectation source_event_id=artifact-expectation:0:0 freshness=current confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=target directory missing".into(),
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client =
            ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
                done_json.into(),
            )]]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.hydration_count, 1);
                assert_eq!(usage.hydration_ref_missing, 0);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn request_context_file_snippet_can_trigger_tool_backed_hydration() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = unique_temp_path("request_context_read");
        std::fs::write(&temp_path, "alpha\nbeta\n").expect("temp file should be written");
        let request_json = format!(
            r#"{{"state":"executing","decision":"request_context","needed_context":["file_snippet:{}"]}}"#,
            temp_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(FileReadTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let _ = std::fs::remove_file(&temp_path);
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert!(usage.hydration_count >= 1);
                assert_eq!(usage.fallback_count, 0);
                assert_eq!(usage.tool_dispatch_count, 1);
                assert_eq!(usage.tool_dispatch_success_count, 1);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn call_tool_read_writes_typed_recent_evidence_and_completes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = std::env::temp_dir().join(format!(
            "stateframe_call_tool_read_{}.txt",
            std::process::id()
        ));
        std::fs::write(
            &temp_path,
            "fn important_symbol() {\n    println!(\"hello\");\n}\n",
        )
        .expect("temp file should be written");
        let request_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            temp_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
        let tool_runtime = StateFrameToolRuntime {
            registry,
            permissions: ToolPermissionContext::new(PermissionMode::Default),
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let _ = std::fs::remove_file(&temp_path);
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.hydration_count, 0);
                assert_eq!(usage.fallback_count, 0);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn call_tool_bash_can_create_file_then_read_confirms_side_effect() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = unique_temp_path("call_tool_bash");
        let bash_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Bash","args":{{"command":"printf 'from bash\n' > \"{}\"","description":"write temp file"}}}}}}"#,
            temp_path.display()
        );
        let read_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            temp_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(bash_json.into())],
            vec![StreamEvent::TextDelta(read_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let registry = ToolRegistry::new()
            .register(Arc::new(BashTool))
            .register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        permissions.add_always_allow_rule("Bash");
        let tool_runtime = StateFrameToolRuntime {
            registry,
            permissions,
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig {
                    max_iterations: 4,
                    ..DecisionLoopConfig::default()
                },
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let content = std::fs::read_to_string(&temp_path).expect("bash should create temp file");
        let _ = std::fs::remove_file(&temp_path);
        assert_eq!(content, "from bash\n");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.fallback_count, 0);
                assert_eq!(usage.tool_dispatch_count, 2);
                assert_eq!(usage.tool_dispatch_success_count, 2);
                assert_eq!(usage.tool_dispatch_failure_count, 0);
                assert!(usage.tool_dispatch_ref_write_count >= 2);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn call_tool_edit_writes_change_fact_and_hydrates_change_ref() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = unique_temp_path("call_tool_edit");
        std::fs::write(&temp_path, "alpha\nbeta\n").expect("temp file should be written");
        let decision_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Edit","args":{{"file_path":"{}","old_string":"alpha","new_string":"omega"}}}}}}"#,
            temp_path.display()
        );
        let decision = validate_state_decision(&decision_json).expect("decision json should parse");
        let registry = ToolRegistry::new()
            .register(Arc::new(FileEditTool))
            .register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        permissions.add_always_allow_rule("Edit");
        let tool_runtime = StateFrameToolRuntime {
            registry,
            permissions,
        };
        let mut frame = make_frame();
        let (changed, record, ref_write_count) = rt
            .block_on(execute_call_tool(
                &mut frame,
                &decision,
                Some(&tool_runtime),
                &mut 0usize,
            ))
            .expect("edit tool dispatch should succeed");
        assert!(changed, "edit dispatch should append typed evidence");
        assert_eq!(record.tool_name, "Edit");
        assert!(ref_write_count >= 2);
        let content = std::fs::read_to_string(&temp_path).expect("edit should update temp file");
        assert_eq!(content, "omega\nbeta\n");
        let hydration = hydrate_needed_context(
            &mut frame,
            &[
                format!("change_ref:{}", temp_path.display()),
                format!("file_snippet:{}", temp_path.display()),
            ],
        );
        let _ = std::fs::remove_file(&temp_path);
        assert!(hydration.changed, "hydration should record typed matches");
        assert_eq!(hydration.unavailable.len(), 0);
        assert!(
            hydration
                .hydrated
                .iter()
                .any(|item| item.contains("change_ref:") && item.contains("match_reason=path")),
            "expected change_ref hydration from recent edit fact"
        );
        assert!(
            hydration
                .hydrated
                .iter()
                .any(|item| item.contains("file_snippet:") && item.contains("match_reason=path")),
            "expected file_snippet hydration from recent edit file fact"
        );
    }

    #[test]
    fn call_tool_edit_without_old_string_requires_read_first() {
        let frame = make_frame();
        let decision_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Edit","args":{"file_path":"src/lib.rs","new_string":"patched"}}}"#;
        let err = parse_and_validate_decision(&frame, decision_json)
            .expect_err("edit without old_string should be rejected for repair");
        assert!(
            err.reason.contains("request Read first"),
            "expected read-first repair guidance, got {}",
            err.reason
        );
    }

    #[test]
    fn call_tool_unknown_tool_records_failure_taxonomy_and_allows_recovery() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Nope","args":{"value":"x"}}}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(FileReadTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.tool_dispatch_count, 1);
                assert_eq!(usage.tool_dispatch_success_count, 0);
                assert_eq!(usage.tool_dispatch_failure_count, 1);
                assert_eq!(
                    usage.tool_dispatch_failure_taxonomy.get("tool_unavailable"),
                    Some(&1usize)
                );
                assert_eq!(usage.tool_execution_records.len(), 1);
                assert_eq!(usage.fallback_count, 0);
            }
            other => panic!("expected Done after recovery, got {other:?}"),
        }
    }

    #[test]
    fn call_tool_read_failure_is_returned_to_agent_for_next_turn() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let missing_path = unique_temp_path("call_tool_missing_read");
        let read_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            missing_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(read_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(FileReadTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig {
                    max_iterations: 3,
                    ..DecisionLoopConfig::default()
                },
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.tool_dispatch_count, 1);
                assert_eq!(usage.tool_dispatch_success_count, 0);
                assert_eq!(usage.tool_dispatch_failure_count, 1);
                assert_eq!(
                    usage.tool_dispatch_failure_taxonomy.get("missing_path"),
                    Some(&1usize)
                );
                assert_eq!(usage.tool_execution_records.len(), 1);
            }
            other => panic!("expected Done after read failure feedback, got {other:?}"),
        }
    }

    #[test]
    fn call_tool_bash_cmd_alias_is_canonicalized() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = unique_temp_path("call_tool_bash_cmd_alias");
        let bash_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Bash","args":{{"cmd":"printf alias-ok > \"{}\""}}}}}}"#,
            temp_path.display()
        );
        let read_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"path":"{}"}}}}}}"#,
            temp_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(bash_json.into())],
            vec![StreamEvent::TextDelta(read_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let registry = ToolRegistry::new()
            .register(Arc::new(BashTool))
            .register(Arc::new(FileReadTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        permissions.add_always_allow_rule("Bash");
        let tool_runtime = StateFrameToolRuntime {
            registry,
            permissions,
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig {
                    max_iterations: 4,
                    ..DecisionLoopConfig::default()
                },
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let content = std::fs::read_to_string(&temp_path).expect("bash alias should create file");
        let _ = std::fs::remove_file(&temp_path);
        assert_eq!(content, "alias-ok");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.tool_dispatch_count, 2);
                assert_eq!(usage.tool_dispatch_success_count, 2);
                assert_eq!(usage.tool_dispatch_failure_count, 0);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn call_tool_bash_dotted_command_alias_is_canonicalized() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = unique_temp_path("call_tool_bash_dotted_alias");
        let bash_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Bash","args":{{"Bash.command":"printf dotted-ok > \"{}\""}}}}}}"#,
            temp_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(bash_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let registry = ToolRegistry::new().register(Arc::new(BashTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        permissions.add_always_allow_rule("Bash");
        let tool_runtime = StateFrameToolRuntime {
            registry,
            permissions,
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let content =
            std::fs::read_to_string(&temp_path).expect("bash dotted alias should create file");
        let _ = std::fs::remove_file(&temp_path);
        assert_eq!(content, "dotted-ok");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.tool_dispatch_count, 1);
                assert_eq!(usage.tool_dispatch_success_count, 1);
                assert_eq!(usage.tool_dispatch_failure_count, 0);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn provider_stream_error_is_reported_instead_of_json_eof() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::Error(
            StreamError {
                provider_id: "openai".into(),
                kind: "empty_response_body".into(),
                message: "provider returned empty response body".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::PreStreamTerminal,
                status_code: None,
            },
        )]]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::ToolDispatchFailed { reason, .. } => {
                assert!(reason.contains("provider_error"));
                assert!(reason.contains("empty response body"));
            }
            other => panic!("expected ToolDispatchFailed, got {other:?}"),
        }
    }
}

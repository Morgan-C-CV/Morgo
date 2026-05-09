use crate::core::evidence_scope::{
    evidence_path_scope_matches, evidence_refs_have_anchor_scope, matching_target_scope,
};
use crate::core::message::Message;
use crate::core::state_fact_ledger::{
    StepFactLedgers, append_runtime_tool_record, fact_lines_from_ledgers,
};
use crate::core::state_frame::{
    AgentState, CompletionEvidenceGap, CompletionEvidenceStatus, CompletionGateBlock, DecisionKind,
    RepairNeeded, StageExecutionContract, StateFrame, StatePatch, WorkerStructuredReport,
    validate_state_decision,
};
use crate::core::state_frame_hydration::{
    HydrationSummary, NeededContextSelector, hydrate_needed_context, parse_needed_context_selector,
};
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::ObservableInput;
use crate::tool::definition::{ToolCall, ToolResult};
use crate::tool::orchestrator::build_execution_record;
use crate::tool::registry::ToolRegistry;
use crate::tool::result::{
    ToolExecutionOutcomeKind, ToolExecutionRecord, ToolOutcome, ToolOutcomeKind,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

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
            // Direct StateFrame workers can spend a few turns on recoverable tool
            // failures before they produce the artifact and still need one turn
            // to verify/finish.
            max_iterations: 8,
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
    pub hydration_from_contract_count: usize,
    pub hydration_from_ledger_count: usize,
    pub stale_ref_count: usize,
    pub hydration_ref_missing: usize,
    pub hydration_miss_unsupported_count: usize,
    pub hydration_miss_stale_count: usize,
    pub hydration_miss_no_match_count: usize,
    pub tool_dispatch_count: usize,
    pub tool_dispatch_success_count: usize,
    pub tool_dispatch_failure_count: usize,
    pub tool_dispatch_ref_write_count: usize,
    pub tool_dispatch_failure_taxonomy: BTreeMap<String, usize>,
    pub tool_execution_records: Vec<ToolExecutionRecord>,
    pub last_effective_tool_action: Option<String>,
    pub last_failure_outcome: Option<ToolOutcome>,
    pub recovery_attempted: bool,
    pub recovery_tier: Option<String>,
    pub recovery_outcome: Option<String>,
    pub terminal_blocker_kind: Option<String>,
    pub last_recovery_attempt: Option<RecoveryAttempt>,
    pub worker_report: Option<WorkerStructuredReport>,
    pub completion_evidence_status: Option<CompletionEvidenceStatus>,
}

#[derive(Debug, Clone)]
pub struct StateFrameToolRuntime {
    pub registry: ToolRegistry,
    pub permissions: ToolPermissionContext,
    pub cwd: PathBuf,
    pub config_root: Option<PathBuf>,
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
    outcome: ToolOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryAttempt {
    pub failure_kind: String,
    pub recommended_next_action: String,
    pub target_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArtifactRepairTurn {
    target_path: String,
    parent_dir: String,
    permission_ref: String,
    missing_reason: String,
    recommended_write_strategy: String,
    create_parent_directory: bool,
    write_target_file: bool,
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
- Treat `budget.max_tool_calls=0` as unlimited tool calls, not as tool calls being disabled\n\
- If a fact entry already says `none`, `none recorded`, `absent`, or equivalent, do NOT request that same context again\n\
- Only use \"decision\": \"request_context\" when the missing fact is not already present in objective/open_items/blocked_items/accepted_summary/recent_evidence\n\
- Use \"decision\": \"call_tool\" when you need a concrete runtime action before you can continue; always include `next_action.action_type` and structured `next_action.args`\n\
- Only call tools listed in `allowed_tools`, and treat `allowed_actions` as the invokable runtime capability contract for this turn\n\
- When `allowed_actions` is non-empty, do NOT request extra context like `fact:allow_worker_tool_calls`, `fact:increase_max_tool_calls`, or `fact:budget.max_tool_calls` to confirm permission; use the listed runtime actions directly\n\
- In the current runtime, `call_tool` is expected to use real worker tools. Prefer narrow `Read` calls with exact `file_path`, use `Bash` only for concrete commands, and use `Edit` with exact `file_path` / `old_string` / `new_string`\n\
- Never call `Edit` unless you already know the exact replacement span. If `old_string` is missing, empty, or uncertain, first `Read` the target file and then issue `Edit` with the exact `old_string`\n\
- If a prior `call_tool` failed, read the `tool_feedback:` / `recent_output_ref:` lines in `recent_evidence`, diagnose the reason, and choose the next action accordingly\n\
- Prefer `tool_outcome:` lines for typed recovery hints such as recoverable, recommended_next_action, and evidence_ref\n\
- If `tool_feedback` says `category=schema_invalid`, rewrite the tool call using canonical argument names before retrying: `Bash.command`, `Read.file_path`, `Edit.file_path/old_string/new_string`\n\
- If `tool_feedback` says `category=missing_path`, do not repeat the same failing `Read`; first inspect `parent_path` or create the missing directory/file scaffold, then continue\n\
- If `hydrated_context` says `selector_note=existence_confirmation_not_readable_path`, do not call `Read` on that selector; if the artifact is a writable target directory, create the directory and write the required files\n\
- You may retry a tool call when the failure looks transient or fixable, but do not blindly repeat the exact same failing action without changing args, path, command, or strategy\n\
- If a `Read` says a target path does not exist yet, inspect the parent path or create the needed directory/file before retrying the same `Read`\n\
- When using `needed_context`, prefer typed selectors like `file_snippet:path`, `test_failure`, `change_ref:path`, `review_ref:ref_id`, `artifact_ref:ref_id`, `open_item_ref:ref_id`, `blocker_ref:ref_id`, `rejected_approach:ref_id`, `artifact:path`, or `fact:name`\n\
- For implement tasks, do not use broad `Glob`/`Grep` exploration when a target path is already named; prefer `request_context:file_snippet:<path>` or a direct narrow `Read`\n\
- When `recent_evidence` contains `fallback_context:` or `fallback_context_item:` lines, consume that fallback evidence before requesting the same context again\n\
- The \"decision\" field MUST be one of the exact string values above — never use free text\n\
- Respond with JSON only, no prose or explanation\n\
\n\
StateFrame:";

fn build_state_decision_repair_prompt(reason: &str, raw_json: &str) -> String {
    format!(
        "Your previous response could not be parsed as canonical StateDecision JSON.\n\
         Error: {reason}\n\
         Raw output: {raw_json}\n\
         Return ONLY one JSON object matching the canonical schema.\n\
         Canonical state values: planning, executing, reviewing, correcting, verifying, blocked, done.\n\
         Canonical decision values: continue, request_context, call_tool, handoff, accept, reject, done.\n\
         Alias mapping you must normalize before responding: executed -> executing, completed -> done, success -> done.\n\
         Do not return explanations, wrappers, validation prose, or keys outside the canonical schema.\n\
         Please respond with canonical StateDecision JSON only."
    )
}

fn append_runtime_contract_facts(frame: &mut StateFrame) {
    let max_tool_calls_value = if frame.budget.max_tool_calls == 0 {
        "unlimited".to_string()
    } else {
        frame.budget.max_tool_calls.to_string()
    };
    push_unique(
        &mut frame.recent_evidence,
        format!(
            "fact: budget.max_tool_calls value={} summary=max_tool_calls={} ({})",
            max_tool_calls_value,
            frame.budget.max_tool_calls,
            if frame.budget.max_tool_calls == 0 {
                "0 means unlimited"
            } else {
                "hard cap"
            }
        ),
    );
    push_unique(
        &mut frame.recent_evidence,
        format!(
            "fact: allow_worker_tool_calls status={} allowed_actions={} allowed_tools={} summary=use allowed_actions as the runtime invocation contract",
            if frame.allowed_actions.is_empty() {
                "not_allowed"
            } else {
                "allowed"
            },
            if frame.allowed_actions.is_empty() {
                "none".to_string()
            } else {
                frame.allowed_actions.join("|")
            },
            if frame.allowed_tools.is_empty() {
                "none".to_string()
            } else {
                frame.allowed_tools.join("|")
            }
        ),
    );
    push_unique(
        &mut frame.recent_evidence,
        format!(
            "fact: increase_max_tool_calls status={} summary={}",
            if frame.budget.max_tool_calls == 0 {
                "not_needed"
            } else {
                "available_if_cap_exhausted"
            },
            if frame.budget.max_tool_calls == 0 {
                "max_tool_calls is already unlimited in this runtime"
            } else {
                "request only after consuming the current capped budget"
            }
        ),
    );
}

fn push_unique(items: &mut Vec<String>, value: String) -> bool {
    if items.iter().any(|item| item == &value) {
        return false;
    }
    items.push(value);
    true
}

fn evidence_field_value(line: &str, field_name: &str) -> Option<String> {
    let prefix = format!("{field_name}=");
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
        .map(|value| value.trim().trim_matches(',').to_string())
        .filter(|value| !value.is_empty() && value != "none" && value != "none recorded")
}

fn collect_fact_field_values(frame: &StateFrame, fact_name: &str, field_name: &str) -> Vec<String> {
    frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with(&format!("fact: {fact_name} ")))
        .filter_map(|line| evidence_field_value(line, field_name))
        .collect()
}

fn split_contract_refs(value: &str) -> Vec<String> {
    value
        .split('|')
        .map(str::trim)
        .filter(|item| !item.is_empty() && *item != "none" && *item != "none recorded")
        .map(str::to_string)
        .collect()
}

fn completion_contract_requirement(frame: &StateFrame, field_name: &str) -> bool {
    match field_name {
        "artifact_evidence" => !frame.stage_execution_contract.declared_artifacts.is_empty(),
        "test_evidence" => !frame.stage_execution_contract.tests.is_empty(),
        "verification_evidence" => !frame.stage_execution_contract.verifications.is_empty(),
        _ => false,
    }
}

fn completion_contract_refs(frame: &StateFrame, field_name: &str) -> Vec<String> {
    match field_name {
        "artifact_refs" => {
            return frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .map(|item| item.ref_id.clone())
                .collect();
        }
        "test_refs" => {
            return frame
                .stage_execution_contract
                .tests
                .iter()
                .map(|item| item.name.clone())
                .collect();
        }
        "verification_refs" => {
            return frame
                .stage_execution_contract
                .verifications
                .iter()
                .map(|item| item.target_ref.clone())
                .collect();
        }
        _ => Vec::new(),
    }
}

fn fact_field_value_by_ref(
    frame: &StateFrame,
    fact_name: &str,
    ref_id: &str,
    field_name: &str,
) -> Option<String> {
    frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with(&format!("fact: {fact_name} ")))
        .find(|line| evidence_field_value(line, "ref").as_deref() == Some(ref_id))
        .and_then(|line| evidence_field_value(line, field_name))
}

fn artifact_contract_target(frame: &StateFrame, ref_id: &str) -> Option<(String, String)> {
    if let Some(artifact) = frame
        .stage_execution_contract
        .declared_artifacts
        .iter()
        .find(|item| item.ref_id == ref_id)
    {
        if !artifact.path.trim().is_empty() {
            let kind = if artifact.kind.trim().is_empty() {
                "file".to_string()
            } else {
                artifact.kind.clone()
            };
            return Some((artifact.path.clone(), kind));
        }
    }
    let path = fact_field_value_by_ref(frame, "artifact_status", ref_id, "path")?;
    let kind = fact_field_value_by_ref(frame, "artifact_status", ref_id, "kind")
        .unwrap_or_else(|| "file".into());
    Some((path, kind))
}

fn permission_target_path(line: &str) -> Option<String> {
    let rest = line.strip_prefix("fact: permission_to_create_and_write:")?;
    let path = rest
        .split_once(' ')
        .map(|(path, _)| path)
        .unwrap_or(rest)
        .trim();
    (!path.is_empty()).then(|| path.to_string())
}

fn declared_target_paths(frame: &StateFrame) -> Vec<String> {
    let mut paths = Vec::new();
    for artifact in &frame.stage_execution_contract.declared_artifacts {
        if !artifact.path.trim().is_empty()
            && !paths.iter().any(|existing| existing == &artifact.path)
        {
            paths.push(artifact.path.clone());
        }
    }
    for verification in &frame.stage_execution_contract.verifications {
        if let Some(path) = verification.target_path.as_ref() {
            if !path.trim().is_empty() && !paths.iter().any(|existing| existing == path) {
                paths.push(path.clone());
            }
        }
    }
    for target in &frame.stage_execution_contract.content_evidence_targets {
        if !target.trim().is_empty() && !paths.iter().any(|existing| existing == target) {
            paths.push(target.clone());
        }
    }
    paths
}

fn prose_read_heading(line: &str) -> bool {
    let lowered = line.trim().to_ascii_lowercase();
    lowered.contains("evidence files read")
        || lowered.contains("read source evidence files")
        || lowered.contains("source evidence files read")
        || lowered.contains("read operations completed")
        || lowered.contains("reads performed")
}

fn prose_read_path_candidate(line: &str, target_paths: &[String]) -> Option<String> {
    let mut candidates = Vec::new();
    let trimmed = line.trim().trim_start_matches('-').trim();
    if let Some((before, _)) = trimmed.split_once(" - ") {
        candidates.push(before.trim());
    }
    if let Some((before, _)) = trimmed.split_once('—') {
        candidates.push(before.trim());
    }
    if let Some((before, _)) = trimmed.split_once(':') {
        candidates.push(before.trim());
    }
    candidates.push(trimmed);

    candidates.into_iter().find_map(|candidate| {
        let candidate =
            candidate.trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | ',' | ';' | '.'));
        if candidate.is_empty() {
            return None;
        }
        matching_target_scope(candidate, target_paths).map(str::to_string)
    })
}

fn parse_prefixed_path(text: &str, prefixes: &[&str]) -> Option<String> {
    let trimmed = text.trim();
    let lowered = trimmed.to_ascii_lowercase();
    for prefix in prefixes {
        if lowered.starts_with(prefix) {
            let value = trimmed[prefix.len()..]
                .trim()
                .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | ',' | ';'));
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn collect_target_scoped_evidence_refs_from_text(
    text: &str,
    target_paths: &[String],
) -> Vec<String> {
    let mut refs = Vec::new();
    let mut prose_read_block_remaining = 0usize;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            prose_read_block_remaining = 0;
            continue;
        }
        let lowered = trimmed.to_ascii_lowercase();
        if prose_read_heading(trimmed) {
            prose_read_block_remaining = 12;
        } else if prose_read_block_remaining > 0 {
            prose_read_block_remaining -= 1;
        }
        if prose_read_block_remaining > 0
            || lowered.contains("read succeeded")
            || lowered.contains("read completed")
        {
            if let Some(target) = prose_read_path_candidate(trimmed, target_paths) {
                let anchor = format!("claimed_read:{target}");
                if !refs.iter().any(|existing| existing == &anchor) {
                    refs.push(anchor);
                }
            }
        }
        if let Some(reference) = evidence_field_value(trimmed, "ref") {
            if !reference.starts_with("artifact:")
                && !refs.iter().any(|existing| existing == &reference)
            {
                refs.push(reference);
            }
        }
        if let Some(reference) = evidence_field_value(trimmed, "evidence_ref") {
            if !reference.starts_with("artifact:")
                && !refs.iter().any(|existing| existing == &reference)
            {
                refs.push(reference);
            }
        }
        if let Some(reference) = evidence_field_value(trimmed, "evidence_refs") {
            for item in reference.split(|ch| matches!(ch, ';' | '|' | ',')) {
                let item = item.trim();
                if item.is_empty() || item.eq_ignore_ascii_case("none") {
                    continue;
                }
                if !item.starts_with("artifact:") && !refs.iter().any(|existing| existing == item) {
                    refs.push(item.to_string());
                }
            }
        }

        if let Some(path) = parse_prefixed_path(
            trimmed,
            &[
                "verified_target:",
                "verified target:",
                "target_path:",
                "target path:",
            ],
        ) {
            let anchor = format!("verification:{path}");
            if !refs.iter().any(|existing| existing == &anchor) {
                refs.push(anchor);
            }
        }

        if let Some(path) = evidence_field_value(trimmed, "path")
            .or_else(|| evidence_field_value(trimmed, "target_path"))
            .filter(|_| trimmed.starts_with("fact: verification_status "))
        {
            if evidence_field_value(trimmed, "status").as_deref() == Some("verified") {
                let anchor = format!("verification:{path}");
                if !refs.iter().any(|existing| existing == &anchor) {
                    refs.push(anchor);
                }
            }
        }

        if let Some(path) = evidence_field_value(trimmed, "path")
            .filter(|_| trimmed.starts_with("fact: artifact_status "))
        {
            if evidence_field_value(trimmed, "status").as_deref() == Some("verified")
                && (evidence_field_value(trimmed, "source").as_deref()
                    == Some("tool:ArtifactVerify")
                    || lowered.contains("artifact verification passed"))
            {
                let anchor = format!("verification:{path}");
                if !refs.iter().any(|existing| existing == &anchor) {
                    refs.push(anchor);
                }
            }
        }

        if let Some(path) = evidence_field_value(trimmed, "path")
            .filter(|_| trimmed.starts_with("fact: file_facts "))
        {
            if evidence_field_value(trimmed, "kind").as_deref() == Some("read_observation")
                && path_matches_target_scope(&path, target_paths)
            {
                let anchor = format!(
                    "read:{}",
                    matching_target_scope(&path, target_paths).unwrap_or(&path)
                );
                if !refs.iter().any(|existing| existing == &anchor) {
                    refs.push(anchor);
                }
            }
        }

        if let Some(rest) = trimmed.strip_prefix("hydrated_context: file_snippet:") {
            if lowered.contains("source=tool:read") {
                let path = rest
                    .split_once(' ')
                    .map(|(path, _)| path)
                    .unwrap_or(rest)
                    .trim();
                if !path.is_empty() && path_matches_target_scope(path, target_paths) {
                    let anchor = format!(
                        "read:{}",
                        matching_target_scope(path, target_paths).unwrap_or(path)
                    );
                    if !refs.iter().any(|existing| existing == &anchor) {
                        refs.push(anchor);
                    }
                }
            }
        }

        if let Some(path) = parse_prefixed_path(trimmed, &["file_snippet:"]) {
            if let Some(target) = matching_target_scope(&path, target_paths) {
                let anchor = format!("read:{target}");
                if !refs.iter().any(|existing| existing == &anchor) {
                    refs.push(anchor);
                }
            }
        }

        if let Some(path) = parse_prefixed_path(trimmed, &["read:", "write:"]) {
            let prefix = trimmed.split(':').next().unwrap_or("verification");
            let anchor = if prefix == "read" {
                matching_target_scope(&path, target_paths)
                    .map(|target| format!("{prefix}:{target}"))
                    .unwrap_or_else(|| format!("{prefix}:{path}"))
            } else {
                format!("{prefix}:{path}")
            };
            if !refs.iter().any(|existing| existing == &anchor) {
                refs.push(anchor);
            }
        }
    }
    refs
}

fn path_matches_target_scope(path: &str, target_paths: &[String]) -> bool {
    matching_target_scope(path, target_paths).is_some()
}

fn append_target_scoped_runtime_anchor(
    refs: &mut Vec<String>,
    prefix: &str,
    path: &str,
    target_paths: &[String],
) {
    let Some(target) = matching_target_scope(path, target_paths) else {
        return;
    };
    let anchor = format!("{prefix}:{target}");
    if !refs.iter().any(|existing| existing == &anchor) {
        refs.push(anchor);
    }
}

fn collect_evidence_refs(frame: &StateFrame, usage: Option<&LoopUsage>) -> Vec<String> {
    let mut refs = Vec::new();
    let target_paths = declared_target_paths(frame);
    let ingest_text = |text: &str, refs: &mut Vec<String>| {
        for reference in collect_target_scoped_evidence_refs_from_text(text, &target_paths) {
            if !refs.iter().any(|existing| existing == &reference) {
                refs.push(reference);
            }
        }
    };

    for line in &frame.recent_evidence {
        ingest_text(line, &mut refs);
    }
    for item in &frame.accepted_summary {
        ingest_text(item, &mut refs);
    }
    for item in &frame.open_items {
        ingest_text(item, &mut refs);
    }
    for item in &frame.blocked_items {
        ingest_text(item, &mut refs);
    }

    if let Some(usage) = usage {
        for record in &usage.tool_execution_records {
            ingest_text(&record.summary, &mut refs);
            if let Some(detail) = record.detail.as_deref() {
                ingest_text(detail, &mut refs);
            }
            if let Some(observable_input) = record.observable_input.as_ref() {
                ingest_text(&observable_input.value, &mut refs);
            }
            if record.kind == ToolExecutionOutcomeKind::Success {
                if let Some(path) = observable_path_from_input(record.observable_input.as_ref()) {
                    match record.tool_name.as_str() {
                        "Read" => append_target_scoped_runtime_anchor(
                            &mut refs,
                            "read",
                            &path,
                            &target_paths,
                        ),
                        "ArtifactVerify" => append_target_scoped_runtime_anchor(
                            &mut refs,
                            "verification",
                            &path,
                            &target_paths,
                        ),
                        _ => {}
                    }
                }
            }
        }
        if let Some(outcome) = usage.last_failure_outcome.as_ref() {
            if let Some(evidence_ref) = outcome.evidence_ref.as_ref() {
                if !refs.iter().any(|existing| existing == evidence_ref) {
                    refs.push(evidence_ref.clone());
                }
            }
            if let Some(excerpt) = outcome.bounded_excerpt.as_ref() {
                ingest_text(excerpt, &mut refs);
            }
        }
    }

    append_required_source_read_anchors(frame, usage, &mut refs);
    append_completion_gate_read_anchors(frame, usage, &mut refs);

    refs
}

fn append_required_source_read_anchors(
    frame: &StateFrame,
    usage: Option<&LoopUsage>,
    refs: &mut Vec<String>,
) {
    let source_targets = &frame.stage_execution_contract.content_evidence_targets;
    if source_targets.is_empty() {
        return;
    }
    let mut append_anchor = |path: &str| {
        let Some(target) = matching_target_scope(path, source_targets) else {
            return;
        };
        let anchor = format!("read:{target}");
        if !refs.iter().any(|existing| existing == &anchor) {
            refs.push(anchor);
        }
    };

    for line in frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with("fact: file_facts "))
    {
        if evidence_field_value(line, "kind").as_deref() == Some("read_observation")
            && evidence_field_value(line, "source").as_deref() == Some("tool:Read")
        {
            if let Some(path) = evidence_field_value(line, "path") {
                append_anchor(&path);
            }
        }
    }

    if let Some(usage) = usage {
        for record in &usage.tool_execution_records {
            if record.kind != ToolExecutionOutcomeKind::Success || record.tool_name != "Read" {
                continue;
            }
            if let Some(path) = observable_path_from_input(record.observable_input.as_ref()) {
                append_anchor(&path);
            }
        }
    }
}

fn runtime_read_observation_paths(frame: &StateFrame, usage: Option<&LoopUsage>) -> Vec<String> {
    let mut paths = Vec::new();

    for line in frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with("fact: file_facts "))
    {
        if evidence_field_value(line, "kind").as_deref() == Some("read_observation")
            && evidence_field_value(line, "source").as_deref() == Some("tool:Read")
        {
            if let Some(path) = evidence_field_value(line, "path") {
                push_unique(&mut paths, path);
            }
        }
    }

    if let Some(usage) = usage {
        for record in &usage.tool_execution_records {
            if record.kind != ToolExecutionOutcomeKind::Success || record.tool_name != "Read" {
                continue;
            }
            if let Some(path) = observable_path_from_input(record.observable_input.as_ref()) {
                push_unique(&mut paths, path);
            }
        }
    }

    paths
}

fn completion_gate_required_read_targets(frame: &StateFrame) -> Vec<String> {
    let mut targets = Vec::new();
    for line in frame.open_items.iter().chain(frame.recent_evidence.iter()) {
        let Some(required_refs) = evidence_field_value(line, "required_evidence_refs") else {
            continue;
        };
        for item in split_contract_refs(&required_refs) {
            push_completion_gate_required_read_target(frame, &mut targets, &item);
        }
    }
    for line in frame.open_items.iter().chain(frame.recent_evidence.iter()) {
        for item in stage_continuation_required_evidence_targets(line) {
            push_completion_gate_required_read_target(frame, &mut targets, &item);
        }
    }
    targets
}

fn push_completion_gate_required_read_target(
    frame: &StateFrame,
    targets: &mut Vec<String>,
    raw_target: &str,
) {
    let target = raw_target
        .trim()
        .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | ',' | ';'));
    if target.is_empty() || target == "none" || target == "none recorded" {
        return;
    }
    let target = target.strip_prefix("read:").unwrap_or(target).trim();
    if target.is_empty()
        || target.starts_with("command:")
        || target.starts_with('/')
            && target
                .split('/')
                .next_back()
                .is_some_and(|name| name.starts_with(':'))
    {
        return;
    }
    if let Some((path, kind)) = artifact_contract_target(frame, target) {
        if kind == "directory" {
            let prefix = format!("{}/", path.trim_end_matches('/'));
            let child_paths = frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .filter(|artifact| artifact.kind == "file" && artifact.path.starts_with(&prefix))
                .map(|artifact| artifact.path.clone())
                .collect::<Vec<_>>();
            if child_paths.is_empty() {
                push_unique(targets, path);
            } else {
                for child_path in child_paths {
                    push_unique(targets, child_path);
                }
            }
        } else {
            push_unique(targets, path);
        }
        return;
    }
    if frame
        .stage_execution_contract
        .declared_artifacts
        .iter()
        .any(|artifact| {
            artifact.kind == "directory" && evidence_path_scope_matches(&artifact.path, target)
        })
    {
        let child_paths = frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .filter(|artifact| {
                artifact.kind == "file" && evidence_path_scope_matches(&artifact.path, target)
            })
            .map(|artifact| artifact.path.clone())
            .collect::<Vec<_>>();
        if child_paths.is_empty() {
            push_unique(targets, target.to_string());
        } else {
            for child_path in child_paths {
                push_unique(targets, child_path);
            }
        }
        return;
    }
    if target.contains('/') {
        push_unique(targets, target.to_string());
    }
}

fn stage_continuation_required_evidence_targets(line: &str) -> Vec<String> {
    let Some((_, tail)) = line.split_once("required_evidence_targets:") else {
        return Vec::new();
    };
    let mut targets = Vec::new();
    for item in tail.split('|') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let lowered = item.to_ascii_lowercase();
        if lowered.starts_with("failure_reason:")
            || lowered.starts_with("modification_direction:")
            || lowered.starts_with("remaining_blocker:")
            || lowered.starts_with("next_action:")
            || lowered.starts_with("forbidden_evidence:")
        {
            break;
        }
        targets.push(item.to_string());
    }
    targets
}

fn append_completion_gate_read_anchors(
    frame: &StateFrame,
    usage: Option<&LoopUsage>,
    refs: &mut Vec<String>,
) {
    let required_targets = completion_gate_required_read_targets(frame);
    if required_targets.is_empty() {
        return;
    }
    let runtime_reads = runtime_read_observation_paths(frame, usage);
    for target in required_targets {
        if runtime_reads
            .iter()
            .any(|path| evidence_path_scope_matches(path, &target))
        {
            let anchor = format!("read:{target}");
            push_unique(refs, anchor);
        }
    }
}

fn completion_gate_required_reads_closed(frame: &StateFrame, usage: &LoopUsage) -> bool {
    let required_targets = completion_gate_required_read_targets(frame);
    if required_targets.is_empty() {
        return false;
    }
    let runtime_reads = runtime_read_observation_paths(frame, Some(usage));
    required_targets.iter().all(|target| {
        runtime_reads
            .iter()
            .any(|path| evidence_path_scope_matches(path, target))
    })
}

fn worker_has_target_scoped_verification_anchor(frame: &StateFrame, refs: &[String]) -> bool {
    let verification_refs = completion_contract_refs(frame, "verification_refs");
    if verification_refs.is_empty() {
        return false;
    }
    verification_refs.into_iter().all(|verification_ref| {
        artifact_contract_target(frame, &verification_ref)
            .map(|(path, _)| evidence_refs_have_anchor_scope(refs, "verification", &path))
            .unwrap_or_else(|| {
                refs.iter()
                    .any(|evidence_ref| evidence_ref.contains(&verification_ref))
            })
    })
}

fn worker_has_target_scoped_read_anchor(frame: &StateFrame, refs: &[String]) -> bool {
    let verification_refs = completion_contract_refs(frame, "verification_refs");
    if verification_refs.is_empty() {
        return false;
    }
    verification_refs.into_iter().all(|verification_ref| {
        artifact_contract_target(frame, &verification_ref)
            .map(|(path, kind)| {
                if kind == "directory" {
                    return directory_child_file_reads_closed(frame, &path, refs);
                }
                evidence_refs_have_anchor_scope(refs, "read", &path)
            })
            .unwrap_or(false)
    })
}

fn directory_child_file_reads_closed(
    frame: &StateFrame,
    directory_path: &str,
    refs: &[String],
) -> bool {
    let prefix = format!("{}/", directory_path.trim_end_matches('/'));
    let child_file_paths = frame
        .stage_execution_contract
        .declared_artifacts
        .iter()
        .filter(|artifact| artifact.kind == "file" && artifact.path.starts_with(&prefix))
        .map(|artifact| artifact.path.as_str())
        .collect::<Vec<_>>();
    if child_file_paths.is_empty() {
        return evidence_refs_have_anchor_scope(refs, "read", directory_path);
    }
    child_file_paths
        .iter()
        .all(|path| evidence_refs_have_anchor_scope(refs, "read", path))
}

fn verification_read_anchor_closed(frame: &StateFrame, refs: &[String]) -> bool {
    completion_contract_requirement(frame, "verification_evidence")
        && worker_has_target_scoped_read_anchor(frame, refs)
}

fn verification_runtime_read_anchor_closed(frame: &StateFrame, usage: &LoopUsage) -> bool {
    if !completion_contract_requirement(frame, "verification_evidence") {
        return false;
    }
    let target_paths = declared_target_paths(frame);
    if target_paths.is_empty() {
        return false;
    }

    let mut refs = Vec::new();
    for line in frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with("fact: file_facts "))
    {
        if evidence_field_value(line, "kind").as_deref() != Some("read_observation") {
            continue;
        }
        if evidence_field_value(line, "source").as_deref() != Some("tool:Read") {
            continue;
        }
        if let Some(path) = evidence_field_value(line, "path") {
            let anchor = format!(
                "read:{}",
                matching_target_scope(&path, &target_paths).unwrap_or(&path)
            );
            if !refs.iter().any(|existing| existing == &anchor)
                && path_matches_target_scope(&path, &target_paths)
            {
                refs.push(anchor);
            }
        }
    }

    for record in &usage.tool_execution_records {
        if record.kind != ToolExecutionOutcomeKind::Success || record.tool_name != "Read" {
            continue;
        }
        if let Some(path) = observable_path_from_input(record.observable_input.as_ref()) {
            let anchor = format!(
                "read:{}",
                matching_target_scope(&path, &target_paths).unwrap_or(&path)
            );
            if !refs.iter().any(|existing| existing == &anchor)
                && path_matches_target_scope(&path, &target_paths)
            {
                refs.push(anchor);
            }
        }
    }

    worker_has_target_scoped_read_anchor(frame, &refs)
}

fn infer_artifact_repair_turn(
    frame: &StateFrame,
    missing_artifact_ref: &str,
    missing_reason: &str,
) -> Option<ArtifactRepairTurn> {
    let (target_path, kind) = artifact_contract_target(frame, missing_artifact_ref)?;
    let permission_ref = frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with("fact: permission_to_create_and_write:"))
        .find(|line| permission_target_path(line).as_deref() == Some(target_path.as_str()))
        .and_then(|line| evidence_field_value(line, "ref"))
        .unwrap_or_else(|| "none".into());
    let parent_dir = std::path::Path::new(&target_path)
        .parent()
        .map(|path| path.display().to_string())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| ".".into());
    let is_directory = kind == "directory";
    let is_file_target = !is_directory && std::path::Path::new(&target_path).extension().is_some();
    let recommended_write_strategy = if is_file_target {
        "create_parent_directory_and_write_target_file".to_string()
    } else {
        "create_directory_then_write_files".to_string()
    };
    Some(ArtifactRepairTurn {
        target_path,
        parent_dir,
        permission_ref,
        missing_reason: missing_reason.to_string(),
        recommended_write_strategy,
        create_parent_directory: true,
        write_target_file: is_file_target,
    })
}

fn repair_turn_open_item(repair_turn: &ArtifactRepairTurn) -> String {
    format!(
        "repair_turn:artifact_missing target_path={} parent_dir={} permission_ref={} missing_reason={} recommended_write_strategy={} create_parent_directory={} write_target_file={}",
        repair_turn.target_path,
        repair_turn.parent_dir,
        repair_turn.permission_ref,
        repair_turn.missing_reason,
        repair_turn.recommended_write_strategy,
        repair_turn.create_parent_directory,
        repair_turn.write_target_file
    )
}

fn repair_turn_fact_line(
    repair_turn: &ArtifactRepairTurn,
    reference: &str,
    summary: String,
) -> String {
    format!(
        "fact: repair_turn ref={} target_path={} parent_dir={} permission_ref={} missing_reason={} recommended_write_strategy={} create_parent_directory={} write_target_file={} summary={}",
        reference,
        repair_turn.target_path,
        repair_turn.parent_dir,
        repair_turn.permission_ref,
        repair_turn.missing_reason,
        repair_turn.recommended_write_strategy,
        repair_turn.create_parent_directory,
        repair_turn.write_target_file,
        summary
    )
}

fn has_verified_artifact_for_path(frame: &StateFrame, path: &str) -> bool {
    let path_matches = |candidate: &str| {
        if evidence_path_scope_matches(candidate, path) {
            return true;
        }
        frame
            .stage_execution_contract
            .verification_by_target_path(path)
            .and_then(|verification| {
                frame
                    .stage_execution_contract
                    .declared_artifact_by_ref(&verification.target_ref)
            })
            .is_some_and(|artifact| {
                artifact.kind == "directory" && evidence_path_scope_matches(candidate, path)
            })
    };
    frame.recent_evidence.iter().any(|line| {
        (line.starts_with("fact: artifact_status ")
            && evidence_field_value(line, "path")
                .as_deref()
                .is_some_and(path_matches)
            && evidence_field_value(line, "status").as_deref() == Some("verified")
            && (evidence_field_value(line, "source").as_deref() == Some("tool:ArtifactVerify")
                || line.contains("artifact verification passed")))
            || (line.starts_with("fact: verification_status ")
                && evidence_field_value(line, "path")
                    .as_deref()
                    .is_some_and(path_matches)
                && evidence_field_value(line, "status").as_deref() == Some("verified"))
    })
}

fn has_explicit_verification_fact(frame: &StateFrame, target_ref: &str) -> bool {
    frame.recent_evidence.iter().any(|line| {
        (line.starts_with("fact: verification_status ")
            && evidence_field_value(line, "target_ref").as_deref() == Some(target_ref)
            && evidence_field_value(line, "status").as_deref() == Some("verified"))
            || (line.starts_with("fact: verification_status ")
                && evidence_field_value(line, "ref").as_deref() == Some(target_ref)
                && evidence_field_value(line, "status").as_deref() == Some("verified"))
    })
}

fn has_completion_verification_signal(frame: &StateFrame) -> bool {
    let verification_refs = completion_contract_refs(frame, "verification_refs");
    !verification_refs.is_empty()
        && verification_refs.into_iter().all(|verification_ref| {
            artifact_contract_target(frame, &verification_ref)
                .map(|(path, _)| has_verified_artifact_for_path(frame, &path))
                .unwrap_or_else(|| has_explicit_verification_fact(frame, &verification_ref))
        })
}

fn artifact_path_has_material_evidence(frame: &StateFrame, path: &str, _kind: &str) -> bool {
    let path_matches =
        |candidate: &str| candidate == path || evidence_path_scope_matches(candidate, path);
    let acceptable_status =
        |status: &str| matches!(status, "created" | "touched" | "verified" | "observed");

    frame.recent_evidence.iter().any(|line| {
        if line.starts_with("fact: recent_changes_in_files ") {
            return evidence_field_value(line, "path")
                .as_deref()
                .is_some_and(path_matches);
        }
        if line.starts_with("fact: artifact_status ") {
            return evidence_field_value(line, "path")
                .as_deref()
                .is_some_and(path_matches)
                && evidence_field_value(line, "status")
                    .as_deref()
                    .is_some_and(acceptable_status);
        }
        if line.starts_with("fact: file_facts ") {
            return evidence_field_value(line, "path")
                .as_deref()
                .is_some_and(path_matches);
        }
        false
    })
}

fn summarize_artifact_status(frame: &StateFrame) -> String {
    let statuses = collect_fact_field_values(frame, "artifact_status", "status");
    if statuses.iter().any(|status| status == "verified") {
        "verified".into()
    } else if let Some(status) = statuses
        .iter()
        .find(|status| status.as_str() != "none recorded")
    {
        status.clone()
    } else if statuses.is_empty() {
        "missing".into()
    } else {
        statuses.last().cloned().unwrap_or_else(|| "missing".into())
    }
}

fn summarize_test_status(frame: &StateFrame) -> String {
    let statuses = collect_fact_field_values(frame, "test_failures", "status");
    if statuses.is_empty() {
        "not_run".into()
    } else {
        statuses.last().cloned().unwrap_or_else(|| "not_run".into())
    }
}

fn summarize_verification_status(frame: &StateFrame) -> String {
    if has_completion_verification_signal(frame) {
        "verified".into()
    } else if completion_contract_requirement(frame, "verification_evidence") {
        "unverified".into()
    } else {
        "not_required".into()
    }
}

fn collect_tests_run(frame: &StateFrame) -> Vec<String> {
    let mut items = Vec::new();
    for line in frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with("fact: test_failures "))
    {
        let name = evidence_field_value(line, "name").unwrap_or_else(|| "unknown_test".into());
        let status = evidence_field_value(line, "status").unwrap_or_else(|| "unknown".into());
        let entry = format!("{name}:{status}");
        if !items.iter().any(|existing| existing == &entry) {
            items.push(entry);
        }
    }
    items
}

fn collect_files_changed(frame: &StateFrame) -> Vec<String> {
    let mut items = Vec::new();
    for line in frame
        .recent_evidence
        .iter()
        .filter(|line| line.starts_with("fact: recent_changes_in_files "))
    {
        if let Some(path) = evidence_field_value(line, "path") {
            if !items.iter().any(|existing| existing == &path) {
                items.push(path);
            }
        }
    }
    items
}

fn collect_remaining_risks(
    frame: &StateFrame,
    completion: &CompletionEvidenceStatus,
) -> Vec<String> {
    let mut items = Vec::new();
    for item in frame.open_items.iter().chain(frame.blocked_items.iter()) {
        if !items.iter().any(|existing| existing == item) {
            items.push(item.clone());
        }
    }
    if !matches!(completion, CompletionEvidenceStatus::Sufficient) {
        items.push(format!(
            "completion_evidence_status={}",
            completion.as_str()
        ));
    }
    items
}

fn missing_artifact_evidence_refs(frame: &StateFrame) -> Vec<String> {
    completion_contract_refs(frame, "artifact_refs")
        .into_iter()
        .filter(|artifact_ref| {
            artifact_contract_target(frame, artifact_ref)
                .map(|(path, kind)| !artifact_path_has_material_evidence(frame, &path, &kind))
                .unwrap_or(true)
        })
        .collect()
}

fn missing_test_evidence_refs(frame: &StateFrame) -> Vec<String> {
    let contract_refs = completion_contract_refs(frame, "test_refs");
    if contract_refs.is_empty()
        || frame.recent_evidence.iter().any(|line| {
            line.starts_with("fact: test_failures ")
                && evidence_field_value(line, "ref").is_some()
                && evidence_field_value(line, "status").is_some()
        })
    {
        Vec::new()
    } else {
        contract_refs
    }
}

fn missing_verification_evidence_refs(frame: &StateFrame) -> Vec<String> {
    completion_contract_refs(frame, "verification_refs")
        .into_iter()
        .filter(|verification_ref| {
            artifact_contract_target(frame, verification_ref)
                .map(|(path, _)| !has_verified_artifact_for_path(frame, &path))
                .unwrap_or_else(|| !has_explicit_verification_fact(frame, verification_ref))
        })
        .collect()
}

fn missing_source_evidence_targets(frame: &StateFrame, evidence_refs: &[String]) -> Vec<String> {
    if !source_evidence_gate_enabled(frame) {
        return Vec::new();
    }
    frame
        .stage_execution_contract
        .content_evidence_targets
        .iter()
        .filter(|target| !evidence_refs_have_anchor_scope(evidence_refs, "read", target))
        .cloned()
        .collect()
}

fn source_evidence_gate_enabled(frame: &StateFrame) -> bool {
    !frame
        .stage_execution_contract
        .content_evidence_targets
        .is_empty()
        && (!frame.stage_execution_contract.declared_artifacts.is_empty()
            || !frame.stage_execution_contract.verifications.is_empty()
            || completion_contract_requirement(frame, "artifact_evidence")
            || completion_contract_requirement(frame, "verification_evidence"))
}

fn recommended_action_for_gap(
    missing_artifact_evidence: bool,
    missing_test_evidence: bool,
    missing_verification_evidence: bool,
) -> String {
    if missing_artifact_evidence {
        "write_artifact".into()
    } else if missing_verification_evidence {
        "verify_artifact".into()
    } else if missing_test_evidence {
        "run_verification".into()
    } else {
        "none".into()
    }
}

fn completion_gate_repair_targets(
    frame: &StateFrame,
    block: &CompletionGateBlock,
) -> (Vec<String>, Vec<String>) {
    let mut read_targets = Vec::new();
    let mut verify_targets = Vec::new();

    let push_unique_target = |targets: &mut Vec<String>, target: String| {
        if !targets.iter().any(|existing| existing == &target) {
            targets.push(target);
        }
    };

    for missing_ref in &block.missing_evidence_refs {
        if block.required_action == "read_source_evidence" && !missing_ref.starts_with("artifact:")
        {
            push_unique_target(&mut read_targets, missing_ref.clone());
            continue;
        }

        if let Some((path, kind)) = artifact_contract_target(frame, missing_ref) {
            if kind == "directory" {
                let prefix = format!("{}/", path.trim_end_matches('/'));
                let mut found_child = false;
                for child_path in frame
                    .stage_execution_contract
                    .declared_artifacts
                    .iter()
                    .filter(|artifact| {
                        artifact.kind == "file" && artifact.path.starts_with(&prefix)
                    })
                    .map(|artifact| artifact.path.clone())
                {
                    found_child = true;
                    push_unique_target(&mut read_targets, child_path);
                }
                if !found_child {
                    push_unique_target(&mut verify_targets, path);
                }
            } else {
                push_unique_target(&mut read_targets, path);
            }
        } else if missing_ref.starts_with("artifact:") {
            push_unique_target(&mut verify_targets, missing_ref.clone());
        } else {
            push_unique_target(&mut read_targets, missing_ref.clone());
        }
    }

    (read_targets, verify_targets)
}

fn completion_gate_required_evidence_refs(
    read_targets: &[String],
    verify_targets: &[String],
) -> Vec<String> {
    let mut refs = Vec::new();
    for target in read_targets {
        let anchor = format!("read:{target}");
        if !refs.iter().any(|existing| existing == &anchor) {
            refs.push(anchor);
        }
    }
    for target in verify_targets {
        let anchor = format!("verification:{target}");
        if !refs.iter().any(|existing| existing == &anchor) {
            refs.push(anchor);
        }
    }
    refs
}

fn completion_gate_forbidden_evidence() -> &'static str {
    "Bash|Glob|cat|sed|ls|self_claims|report_prose"
}

fn collect_completion_evidence_gaps(frame: &StateFrame) -> Vec<CompletionEvidenceGap> {
    let evidence_refs = collect_evidence_refs(frame, None);
    collect_completion_evidence_gaps_with_refs(frame, &evidence_refs)
}

fn collect_completion_evidence_gaps_with_refs(
    frame: &StateFrame,
    evidence_refs: &[String],
) -> Vec<CompletionEvidenceGap> {
    let missing_artifact_refs = missing_artifact_evidence_refs(frame);
    let missing_test_refs = missing_test_evidence_refs(frame);
    let missing_verification_refs = missing_verification_evidence_refs(frame);
    let missing_source_targets = missing_source_evidence_targets(frame, evidence_refs);
    let mut ordered_refs: Vec<String> = Vec::new();
    for ref_id in missing_artifact_refs
        .iter()
        .chain(missing_test_refs.iter())
        .chain(missing_verification_refs.iter())
    {
        if !ordered_refs.iter().any(|existing| existing == ref_id) {
            ordered_refs.push(ref_id.clone());
        }
    }

    ordered_refs
        .into_iter()
        .map(|target_ref| {
            let target_path = artifact_contract_target(frame, &target_ref).map(|(path, _)| path);
            let missing_artifact_evidence =
                missing_artifact_refs.iter().any(|item| item == &target_ref);
            let missing_test_evidence = missing_test_refs.iter().any(|item| item == &target_ref);
            let missing_verification_evidence = missing_verification_refs
                .iter()
                .any(|item| item == &target_ref);
            CompletionEvidenceGap {
                target_ref,
                target_path,
                missing_artifact_evidence,
                missing_test_evidence,
                missing_verification_evidence,
                recommended_action: recommended_action_for_gap(
                    missing_artifact_evidence,
                    missing_test_evidence,
                    missing_verification_evidence,
                ),
            }
        })
        .chain(
            missing_source_targets
                .into_iter()
                .map(|target_path| CompletionEvidenceGap {
                    target_ref: format!("content_evidence:{target_path}"),
                    target_path: Some(target_path),
                    missing_artifact_evidence: false,
                    missing_test_evidence: false,
                    missing_verification_evidence: true,
                    recommended_action: "read_source_evidence".into(),
                }),
        )
        .collect()
}

fn evaluate_completion_evidence(frame: &StateFrame, usage: &LoopUsage) -> CompletionEvidenceStatus {
    let evidence_refs = collect_evidence_refs(frame, Some(usage));
    let runtime_read_anchor_closed = verification_runtime_read_anchor_closed(frame, usage);
    let completion_gate_reads_closed = completion_gate_required_reads_closed(frame, usage);
    if completion_contract_requirement(frame, "artifact_evidence")
        && !missing_artifact_evidence_refs(frame).is_empty()
    {
        return CompletionEvidenceStatus::MissingArtifactEvidence;
    }
    if completion_contract_requirement(frame, "test_evidence")
        && !missing_test_evidence_refs(frame).is_empty()
    {
        return CompletionEvidenceStatus::MissingTestEvidence;
    }
    if !missing_source_evidence_targets(frame, &evidence_refs).is_empty() {
        return CompletionEvidenceStatus::MissingVerificationEvidence;
    }
    if completion_contract_requirement(frame, "verification_evidence")
        && !missing_verification_evidence_refs(frame).is_empty()
    {
        if worker_has_target_scoped_verification_anchor(frame, &evidence_refs)
            || verification_read_anchor_closed(frame, &evidence_refs)
            || runtime_read_anchor_closed
            || completion_gate_reads_closed
        {
            return CompletionEvidenceStatus::Sufficient;
        }
        return CompletionEvidenceStatus::MissingVerificationEvidence;
    }
    CompletionEvidenceStatus::Sufficient
}

fn inject_completion_gate_block(frame: &mut StateFrame, block: &CompletionGateBlock) {
    let missing_refs = if block.missing_evidence_refs.is_empty() {
        "none".to_string()
    } else {
        block.missing_evidence_refs.join("|")
    };
    let (read_targets, verify_targets) = completion_gate_repair_targets(frame, block);
    let required_evidence_refs =
        completion_gate_required_evidence_refs(&read_targets, &verify_targets);
    let required_read_targets = if read_targets.is_empty() {
        "none".to_string()
    } else {
        read_targets.join("|")
    };
    let required_verify_targets = if verify_targets.is_empty() {
        "none".to_string()
    } else {
        verify_targets.join("|")
    };
    let required_evidence_refs_text = if required_evidence_refs.is_empty() {
        "none".to_string()
    } else {
        required_evidence_refs.join("|")
    };
    let sanitized_reason = block
        .reason
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("_");
    let repair_direction = match block.required_action.as_str() {
        "read_source_evidence" => "read_required_source_targets_then_return_read_anchors",
        "verify_artifact" => "read_required_artifact_targets_then_return_read_anchors",
        "run_verification" => "run_verification_then_return_verification_anchors",
        "write_artifact" => "write_missing_artifacts_then_return_write_anchors",
        _ => "follow_required_action_and_return_runtime_anchors",
    };
    push_unique(
        &mut frame.open_items,
        format!(
            "required_action:{} reason={} missing_refs={}",
            block.required_action, block.reason, missing_refs
        ),
    );
    push_unique(
        &mut frame.open_items,
        format!(
            "completion_gate_repair: failure_reason={} modification_direction={} required_read_targets={} required_verification_targets={} required_evidence_refs={} forbidden_evidence={}",
            sanitized_reason,
            repair_direction,
            required_read_targets,
            required_verify_targets,
            required_evidence_refs_text,
            completion_gate_forbidden_evidence()
        ),
    );
    push_unique(
        &mut frame.recent_evidence,
        format!(
            "completion_gate: status={} required_action={} reason={} missing_evidence_refs={} required_read_targets={} required_verification_targets={} required_evidence_refs={} forbidden_evidence={}",
            block.status.as_str(),
            block.required_action,
            block.reason,
            missing_refs,
            required_read_targets,
            required_verify_targets,
            required_evidence_refs_text,
            completion_gate_forbidden_evidence()
        ),
    );
    if block.required_action == "write_artifact" {
        if let Some(repair_turn) = block
            .missing_evidence_refs
            .iter()
            .find_map(|missing_ref| infer_artifact_repair_turn(frame, missing_ref, &block.reason))
        {
            push_unique(&mut frame.open_items, repair_turn_open_item(&repair_turn));
            push_unique(
                &mut frame.recent_evidence,
                repair_turn_fact_line(
                    &repair_turn,
                    "repair:artifact_missing",
                    format!("artifact repair required for {}", repair_turn.target_path),
                ),
            );
        }
    }
    frame.state = match block.required_action.as_str() {
        "write_artifact" => AgentState::Executing,
        "run_verification" | "verify_artifact" | "read_source_evidence" => AgentState::Verifying,
        _ => AgentState::Correcting,
    };
}

fn record_completion_gate_recovery(
    frame: &StateFrame,
    usage: &mut LoopUsage,
    block: &CompletionGateBlock,
) {
    usage.recovery_attempted = true;
    usage.recovery_tier = Some(
        match block.required_action.as_str() {
            "write_artifact" => "artifact_repair_turn",
            "read_source_evidence" | "run_verification" | "verify_artifact" => {
                "verification_repair_continuation"
            }
            _ => "artifact_repair_turn",
        }
        .into(),
    );
    usage.recovery_outcome = Some("repair_turn_injected".into());
    usage.terminal_blocker_kind = None;
    usage.last_recovery_attempt = Some(RecoveryAttempt {
        failure_kind: block.status.as_str().to_string(),
        recommended_next_action: block.required_action.clone(),
        target_path: completion_gate_repair_targets(frame, block)
            .0
            .into_iter()
            .next()
            .or_else(|| {
                block
                    .missing_evidence_refs
                    .iter()
                    .find_map(|missing_ref| {
                        infer_artifact_repair_turn(frame, missing_ref, &block.reason)
                    })
                    .map(|repair_turn| repair_turn.target_path)
            }),
    });
}

fn enforce_completion_gate(
    frame: &mut StateFrame,
    usage: &mut LoopUsage,
) -> Result<(), CompletionGateBlock> {
    let status = evaluate_completion_evidence(frame, usage);
    if matches!(status, CompletionEvidenceStatus::Sufficient) {
        return Ok(());
    }
    usage.completion_evidence_status = Some(status.clone());
    let (required_action, reason, missing_evidence_refs) = match status {
        CompletionEvidenceStatus::MissingArtifactEvidence => (
            "write_artifact".to_string(),
            "completion gate blocked done because required artifact evidence is missing"
                .to_string(),
            missing_artifact_evidence_refs(frame),
        ),
        CompletionEvidenceStatus::MissingTestEvidence => (
            "run_verification".to_string(),
            "completion gate blocked done because required test evidence is missing".to_string(),
            missing_test_evidence_refs(frame),
        ),
        CompletionEvidenceStatus::MissingVerificationEvidence => (
            {
                let evidence_refs = collect_evidence_refs(frame, Some(usage));
                if missing_source_evidence_targets(frame, &evidence_refs).is_empty() {
                    "verify_artifact".to_string()
                } else {
                    "read_source_evidence".to_string()
                }
            },
            {
                let evidence_refs = collect_evidence_refs(frame, Some(usage));
                if missing_source_evidence_targets(frame, &evidence_refs).is_empty() {
                    "completion gate blocked done because required verification evidence is missing"
                        .to_string()
                } else {
                    "completion gate blocked done because required source evidence has not been read"
                        .to_string()
                }
            },
            {
                let evidence_refs = collect_evidence_refs(frame, Some(usage));
                let source_targets = missing_source_evidence_targets(frame, &evidence_refs);
                if source_targets.is_empty() {
                    missing_verification_evidence_refs(frame)
                } else {
                    source_targets
                }
            },
        ),
        CompletionEvidenceStatus::Sufficient => unreachable!(),
    };
    Err(CompletionGateBlock {
        status,
        required_action,
        reason,
        missing_evidence_refs,
    })
}

fn verification_only_completion_gate_block(frame: &StateFrame) -> Option<CompletionGateBlock> {
    if !completion_contract_requirement(frame, "verification_evidence") {
        return None;
    }
    if !missing_artifact_evidence_refs(frame).is_empty()
        || !missing_test_evidence_refs(frame).is_empty()
    {
        return None;
    }
    let missing_evidence_refs = missing_verification_evidence_refs(frame);
    if missing_evidence_refs.is_empty() {
        return None;
    }
    Some(CompletionGateBlock {
        status: CompletionEvidenceStatus::MissingVerificationEvidence,
        required_action: "verify_artifact".into(),
        reason:
            "artifact repair recovered; remaining verification evidence requires short re-verify"
                .into(),
        missing_evidence_refs,
    })
}

fn promote_recovered_verification_gap(frame: &mut StateFrame, usage: &mut LoopUsage) -> bool {
    if usage.recovery_outcome.as_deref() != Some("recovered") {
        return false;
    }
    let Some(block) = verification_only_completion_gate_block(frame) else {
        return false;
    };
    usage.completion_evidence_status = Some(block.status.clone());
    inject_completion_gate_block(frame, &block);
    record_completion_gate_recovery(frame, usage, &block);
    usage.recovery_tier = Some("verification_repair_continuation".into());
    true
}

fn terminalize_verification_only_gap(frame: &mut StateFrame, usage: &mut LoopUsage) -> bool {
    let Some(block) = verification_only_completion_gate_block(frame) else {
        return false;
    };
    usage.completion_evidence_status = Some(block.status.clone());
    inject_completion_gate_block(frame, &block);
    record_completion_gate_recovery(frame, usage, &block);
    usage.recovery_tier = Some("verification_repair_continuation".into());
    true
}

fn verification_gap_still_actionable(frame: &StateFrame, usage: &LoopUsage) -> bool {
    if !matches!(
        usage.completion_evidence_status,
        Some(CompletionEvidenceStatus::MissingVerificationEvidence)
    ) {
        return false;
    }
    if usage.recovery_tier.as_deref() != Some("verification_repair_continuation")
        && usage.recovery_outcome.as_deref() != Some("recovered")
    {
        return false;
    }

    let missing_refs = missing_verification_evidence_refs(frame);
    if missing_refs.is_empty() {
        return false;
    }

    missing_refs.iter().any(|verification_ref| {
        artifact_contract_target(frame, verification_ref)
            .map(|(_path, kind)| kind == "directory" || kind == "file")
            .unwrap_or(false)
    })
}

fn build_worker_structured_report(
    frame: &StateFrame,
    usage: &LoopUsage,
    completion: CompletionEvidenceStatus,
) -> WorkerStructuredReport {
    let evidence_refs = collect_evidence_refs(frame, Some(usage));
    let read_anchor_closed = verification_read_anchor_closed(frame, &evidence_refs)
        || verification_runtime_read_anchor_closed(frame, usage);
    let completion_gate_reads_closed = completion_gate_required_reads_closed(frame, usage);
    let source_evidence_closed = missing_source_evidence_targets(frame, &evidence_refs).is_empty();
    let completion = if matches!(
        completion,
        CompletionEvidenceStatus::MissingVerificationEvidence
    ) && source_evidence_closed
        && (worker_has_target_scoped_verification_anchor(frame, &evidence_refs)
            || read_anchor_closed
            || completion_gate_reads_closed)
    {
        CompletionEvidenceStatus::Sufficient
    } else {
        completion
    };
    let mut completion_evidence_gaps =
        collect_completion_evidence_gaps_with_refs(frame, &evidence_refs);
    if matches!(completion, CompletionEvidenceStatus::Sufficient)
        && source_evidence_closed
        && (read_anchor_closed || completion_gate_reads_closed)
    {
        completion_evidence_gaps.retain(|gap| !gap.missing_verification_evidence);
    }
    let verification_status = if matches!(completion, CompletionEvidenceStatus::Sufficient)
        && (read_anchor_closed || completion_gate_reads_closed)
        && source_evidence_closed
    {
        "verified".into()
    } else {
        summarize_verification_status(frame)
    };
    WorkerStructuredReport {
        worker_state: frame.state,
        last_tool_action: usage.last_effective_tool_action.clone(),
        files_changed: collect_files_changed(frame),
        tests_run: collect_tests_run(frame),
        artifact_status: summarize_artifact_status(frame),
        test_status: summarize_test_status(frame),
        verification_status,
        stage_execution_contract: frame.stage_execution_contract.clone(),
        stage_continuation_context: None,
        evidence_refs,
        completion_evidence_gaps,
        remaining_risks: collect_remaining_risks(frame, &completion),
        completion_evidence_status: completion,
    }
}

fn finalize_worker_usage_report(frame: &StateFrame, usage: &mut LoopUsage) {
    let completion = evaluate_completion_evidence(frame, usage);
    let report = build_worker_structured_report(frame, usage, completion);
    usage.completion_evidence_status = Some(report.completion_evidence_status.clone());
    usage.worker_report = Some(report);
}

fn completion_gate_repair_active(frame: &StateFrame) -> bool {
    frame
        .open_items
        .iter()
        .chain(frame.recent_evidence.iter())
        .any(|item| {
            item.starts_with("completion_gate_repair:")
                || item.starts_with("completion_gate:")
                || item.starts_with("required_action:verify_artifact")
                || item.starts_with("required_action:read_source_evidence")
                || (item.starts_with("fact: stage_continuation ")
                    && item.contains("continuity_mode=repair")
                    && (item.contains("next_action=verify_artifact")
                        || item.contains("next_action=read_source_evidence")
                        || item.contains("required_evidence_targets:")))
        })
}

fn completion_gate_repair_terminal_outcome(
    frame: &StateFrame,
    usage: &mut LoopUsage,
) -> Option<LoopOutcome> {
    let repair_active = completion_gate_repair_active(frame);
    if !repair_active {
        return None;
    }
    let completion = evaluate_completion_evidence(frame, usage);
    let required_targets = completion_gate_required_read_targets(frame);
    let completion_gate_reads_closed = completion_gate_required_reads_closed(frame, usage);
    if verify_terminal_diagnostics_enabled() {
        eprintln!(
            "completion_gate_repair_terminal_outcome: state={:?} last_effective_tool_action={} repair_active={} completion={} completion_gate_reads_closed={} required_targets={} evidence_refs={}",
            frame.state,
            usage
                .last_effective_tool_action
                .as_deref()
                .unwrap_or("none"),
            repair_active,
            completion.as_str(),
            completion_gate_reads_closed,
            required_targets.join("|"),
            collect_evidence_refs(frame, Some(usage)).join("|")
        );
    }
    if !matches!(completion, CompletionEvidenceStatus::Sufficient) {
        return None;
    }
    let report = build_worker_structured_report(frame, usage, completion);
    if !report.completion_evidence_gaps.is_empty() {
        if verify_terminal_diagnostics_enabled() {
            eprintln!(
                "completion_gate_repair_terminal_outcome: blocked_by_gaps gaps={:?}",
                report.completion_evidence_gaps
            );
        }
        return None;
    }
    usage.completion_evidence_status = Some(report.completion_evidence_status.clone());
    usage.worker_report = Some(report);
    Some(LoopOutcome::Done {
        final_state: AgentState::Done,
        usage: usage.clone(),
    })
}

fn verify_terminal_diagnostics_enabled() -> bool {
    std::env::var("RUST_AGENT_VERIFY_TERMINAL_DIAGNOSTICS")
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

fn verification_terminal_outcome(frame: &StateFrame, usage: &mut LoopUsage) -> Option<LoopOutcome> {
    if !completion_contract_requirement(frame, "verification_evidence") {
        return None;
    }

    let verify_context_active = frame.state == AgentState::Verifying
        || matches!(
            usage.last_effective_tool_action.as_deref(),
            Some("Read" | "ArtifactVerify")
        )
        || frame
            .open_items
            .iter()
            .any(|item| item.starts_with("required_action:verify_artifact"));

    if !verify_context_active {
        return None;
    }

    let evidence_refs = collect_evidence_refs(frame, Some(usage));
    let has_target_read_anchor = worker_has_target_scoped_read_anchor(frame, &evidence_refs)
        || verification_runtime_read_anchor_closed(frame, usage);
    let completion_gate_reads_closed = completion_gate_required_reads_closed(frame, usage);
    let source_evidence_closed = missing_source_evidence_targets(frame, &evidence_refs).is_empty();
    let verification_target_read_count = verification_target_successful_read_count(frame, usage);
    let verification_status = summarize_verification_status(frame);
    let last_effective_tool_action = usage
        .last_effective_tool_action
        .as_deref()
        .unwrap_or("none");
    let read_short_circuit_candidate = has_target_read_anchor
        && source_evidence_closed
        && (verification_status == "verified"
            || last_effective_tool_action == "Read"
            || completion_gate_reads_closed);
    let read_tailspin_candidate =
        verification_target_read_count >= 2 && has_target_read_anchor && source_evidence_closed;
    if read_short_circuit_candidate {
        if verify_terminal_diagnostics_enabled() {
            eprintln!(
                "verify_terminal_outcome: done state={:?} last_effective_tool_action={} has_target_read_anchor={} source_evidence_closed={} verification_status={} evidence_refs={}",
                frame.state,
                last_effective_tool_action,
                has_target_read_anchor,
                source_evidence_closed,
                verification_status,
                evidence_refs.join("|")
            );
        }
        finalize_worker_usage_report(frame, usage);
        return Some(LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: usage.clone(),
        });
    }

    let completion = evaluate_completion_evidence(frame, usage);
    if matches!(completion, CompletionEvidenceStatus::Sufficient) {
        if verify_terminal_diagnostics_enabled() {
            eprintln!(
                "verify_terminal_outcome: done_via_completion state={:?} last_effective_tool_action={} has_target_read_anchor={} source_evidence_closed={} verification_status={} evidence_refs={}",
                frame.state,
                last_effective_tool_action,
                has_target_read_anchor,
                source_evidence_closed,
                verification_status,
                evidence_refs.join("|")
            );
        }
        finalize_worker_usage_report(frame, usage);
        return Some(LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: usage.clone(),
        });
    }
    if read_tailspin_candidate {
        if verify_terminal_diagnostics_enabled() {
            eprintln!(
                "verify_terminal_outcome: forced_done_after_repeated_read state={:?} last_effective_tool_action={} verification_target_read_count={} has_target_read_anchor={} source_evidence_closed={} evidence_refs={}",
                frame.state,
                last_effective_tool_action,
                verification_target_read_count,
                has_target_read_anchor,
                source_evidence_closed,
                evidence_refs.join("|")
            );
        }
        finalize_worker_usage_report(frame, usage);
        return Some(LoopOutcome::Done {
            final_state: AgentState::Done,
            usage: usage.clone(),
        });
    }

    let attempted_target_scoped_verification = matches!(
        usage.last_effective_tool_action.as_deref(),
        Some("ArtifactVerify")
    ) && usage.last_failure_outcome.is_none();
    if attempted_target_scoped_verification
        && matches!(
            completion,
            CompletionEvidenceStatus::MissingVerificationEvidence
        )
    {
        let missing_refs = missing_verification_evidence_refs(frame);
        finalize_worker_usage_report(frame, usage);
        return Some(LoopOutcome::ToolDispatchFailed {
            last_state: frame.state,
            reason: format!(
                "verification evidence still missing after target-scoped verification attempt: {}",
                if missing_refs.is_empty() {
                    "none".to_string()
                } else {
                    missing_refs.join("|")
                }
            ),
            usage: usage.clone(),
        });
    }

    if verify_terminal_diagnostics_enabled() {
        eprintln!(
            "verify_terminal_outcome: no_done state={:?} last_effective_tool_action={} has_target_read_anchor={} verification_status={} completion={} verify_context_active={} evidence_refs={}",
            frame.state,
            last_effective_tool_action,
            has_target_read_anchor,
            verification_status,
            completion.as_str(),
            verify_context_active,
            evidence_refs.join("|")
        );
    }

    None
}

fn verification_target_successful_read_count(frame: &StateFrame, usage: &LoopUsage) -> usize {
    let verification_targets = completion_contract_refs(frame, "verification_refs")
        .into_iter()
        .filter_map(|verification_ref| {
            artifact_contract_target(frame, &verification_ref)
                .map(|(path, _)| path)
                .or_else(|| Some(verification_ref))
        })
        .collect::<Vec<_>>();
    if verification_targets.is_empty() {
        return 0;
    }
    usage
        .tool_execution_records
        .iter()
        .filter(|record| record.kind == ToolExecutionOutcomeKind::Success)
        .filter(|record| record.tool_name == "Read")
        .filter(|record| {
            observable_path_from_input(record.observable_input.as_ref()).is_some_and(|path| {
                verification_targets
                    .iter()
                    .any(|target| evidence_path_scope_matches(&path, target))
            })
        })
        .count()
}

fn verification_report_read_tailspin_reason(
    frame: &StateFrame,
    usage: &LoopUsage,
    decision: &crate::core::state_frame::StateDecision,
) -> Option<(String, Vec<String>)> {
    let target_path = current_action_target_path(decision)?;
    let verification_targets = completion_contract_refs(frame, "verification_refs")
        .into_iter()
        .filter_map(|verification_ref| {
            artifact_contract_target(frame, &verification_ref)
                .map(|(path, _)| path)
                .or_else(|| Some(verification_ref))
        })
        .collect::<Vec<_>>();
    if !verification_targets
        .iter()
        .any(|path| evidence_path_scope_matches(path, &target_path))
    {
        return None;
    }
    let evidence_refs = collect_evidence_refs(frame, Some(usage));
    if !evidence_refs_have_anchor_scope(&evidence_refs, "read", &target_path) {
        return None;
    }
    let missing_source_targets = missing_source_evidence_targets(frame, &evidence_refs);
    if missing_source_targets.is_empty() {
        return None;
    }
    Some((
        format!(
            "repeated verification-target read of {} while source evidence remains missing",
            target_path
        ),
        missing_source_targets,
    ))
}

fn current_action_target_path(
    decision: &crate::core::state_frame::StateDecision,
) -> Option<String> {
    parse_read_path(decision).or_else(|| parse_edit_path(decision))
}

fn repeated_recovery_strategy_reason(
    usage: &LoopUsage,
    decision: &crate::core::state_frame::StateDecision,
) -> Option<String> {
    let next_action = decision.next_action.as_ref()?;
    let attempt = usage.last_recovery_attempt.as_ref()?;
    let target_path = current_action_target_path(decision);
    if attempt.failure_kind == "user_error"
        && attempt.recommended_next_action == "read_before_edit"
        && next_action.action_type.eq_ignore_ascii_case("Edit")
        && target_path == attempt.target_path
    {
        return Some(format!(
            "repeated invalid edit on {} after read_before_edit recovery hint",
            attempt.target_path.as_deref().unwrap_or("unknown_target")
        ));
    }
    if attempt.failure_kind == "missing_path"
        && next_action.action_type.eq_ignore_ascii_case("Read")
        && target_path == attempt.target_path
    {
        return Some(format!(
            "repeated missing-path read on {} without changing recovery strategy",
            attempt.target_path.as_deref().unwrap_or("unknown_target")
        ));
    }
    if attempt.failure_kind == "missing_path"
        && attempt.recommended_next_action == "create_parent_directory_and_write_target_file"
        && next_action.action_type.eq_ignore_ascii_case("Bash")
        && parse_bash_command(decision)
            .as_deref()
            .is_some_and(|command| command.contains("mkdir") && !command.contains("write"))
    {
        return Some(format!(
            "repeated create_directory without write on {} after headless repair hint",
            attempt.target_path.as_deref().unwrap_or("unknown_target")
        ));
    }
    None
}

fn record_recoverable_tool_failure(
    usage: &mut LoopUsage,
    outcome: &ToolOutcome,
    target_path: Option<String>,
) {
    if !outcome.recoverable {
        usage.terminal_blocker_kind = Some(outcome.kind.as_str().to_string());
        return;
    }
    usage.recovery_attempted = true;
    usage.recovery_tier = Some("worker_self_repair".into());
    usage.recovery_outcome = Some("pending_next_turn".into());
    usage.last_recovery_attempt = Some(RecoveryAttempt {
        failure_kind: outcome.kind.as_str().to_string(),
        recommended_next_action: outcome
            .recommended_next_action
            .clone()
            .unwrap_or_else(|| "none".into()),
        target_path,
    });
}

fn clear_recovery_after_success(usage: &mut LoopUsage) {
    if usage.last_recovery_attempt.take().is_some() {
        usage.recovery_attempted = true;
        usage.recovery_outcome = Some("recovered".into());
        usage.terminal_blocker_kind = None;
    }
}

fn inject_missing_path_recovery_gate(frame: &mut StateFrame, usage: &mut LoopUsage) -> bool {
    let Some(attempt) = usage.last_recovery_attempt.as_ref() else {
        return false;
    };
    if attempt.failure_kind != "missing_path" {
        return false;
    }
    let Some(target_path) = attempt.target_path.as_deref() else {
        return false;
    };
    if !has_create_permission_for_path(frame, target_path) {
        return false;
    }
    let repair_turn = infer_artifact_repair_turn(frame, target_path, "tool_missing_path_recovery")
        .or_else(|| {
            let parent_dir = std::path::Path::new(target_path)
                .parent()
                .map(|parent| parent.to_string_lossy().to_string())
                .filter(|parent| !parent.trim().is_empty())
                .unwrap_or_else(|| ".".into());
            let permission_marker = format!("fact: permission_to_create_and_write:{target_path} ");
            let permission_ref = frame
                .recent_evidence
                .iter()
                .find(|line| line.starts_with(&permission_marker))
                .and_then(|line| evidence_field_value(line, "ref"))
                .unwrap_or_else(|| "none".into());
            Some(ArtifactRepairTurn {
                target_path: target_path.to_string(),
                parent_dir,
                permission_ref,
                missing_reason: "tool_missing_path_recovery".into(),
                recommended_write_strategy: if std::path::Path::new(target_path)
                    .extension()
                    .is_some()
                {
                    "create_parent_directory_and_write_target_file".into()
                } else {
                    "create_directory_then_write_files".into()
                },
                create_parent_directory: true,
                write_target_file: std::path::Path::new(target_path).extension().is_some(),
            })
        });
    let Some(repair_turn) = repair_turn else {
        return false;
    };
    let mut changed = false;
    changed |= push_unique(&mut frame.open_items, repair_turn_open_item(&repair_turn));
    changed |= push_unique(
        &mut frame.recent_evidence,
        repair_turn_fact_line(
            &repair_turn,
            "repair:missing_path",
            format!(
                "missing path recovery required for {}",
                repair_turn.target_path
            ),
        ),
    );
    if changed {
        frame.state = AgentState::Executing;
        usage.recovery_attempted = true;
        usage.recovery_tier = Some("missing_path_recovery_gate".into());
        usage.recovery_outcome = Some("repair_turn_injected".into());
        usage.terminal_blocker_kind = None;
    }
    changed
}

fn inject_request_context_recovery_gate(frame: &mut StateFrame, usage: &mut LoopUsage) -> bool {
    let unsupported_only = !usage.hydration_miss_unsupported_count.eq(&0)
        && usage.hydration_miss_no_match_count == 0
        && usage.hydration_miss_stale_count == 0;
    if unsupported_only {
        usage.terminal_blocker_kind = Some("unsupported_selector".into());
        usage.recovery_attempted = false;
        usage.recovery_tier = None;
        usage.recovery_outcome = Some("unsupported_selector".into());
        return false;
    }

    let verification_gap = matches!(
        usage.completion_evidence_status,
        Some(CompletionEvidenceStatus::MissingVerificationEvidence)
    );
    if verification_gap {
        let evidence_refs = collect_evidence_refs(frame, Some(usage));
        let missing_source_targets = missing_source_evidence_targets(frame, &evidence_refs);
        if !missing_source_targets.is_empty() {
            let block = CompletionGateBlock {
                status: CompletionEvidenceStatus::MissingVerificationEvidence,
                required_action: "read_source_evidence".into(),
                reason:
                    "source evidence missing requires runtime Read on the declared input targets"
                        .into(),
                missing_evidence_refs: missing_source_targets,
            };
            inject_completion_gate_block(frame, &block);
            record_completion_gate_recovery(frame, usage, &block);
            return true;
        }
        let block = CompletionGateBlock {
            status: CompletionEvidenceStatus::MissingVerificationEvidence,
            required_action: "verify_artifact".into(),
            reason: "artifact verification failure requires repair continuation".into(),
            missing_evidence_refs: missing_verification_evidence_refs(frame),
        };
        inject_completion_gate_block(frame, &block);
        record_completion_gate_recovery(frame, usage, &block);
        return true;
    }

    if inject_missing_path_recovery_gate(frame, usage) {
        return true;
    }

    if usage.terminal_blocker_kind.as_deref() == Some("external_blocker") {
        usage.terminal_blocker_kind = Some("true_external_blocker".into());
        usage.recovery_outcome = Some("external_blocker".into());
        return false;
    }

    if usage.hydration_ref_missing > 0 || usage.hydration_miss_no_match_count > 0 {
        let changed = push_unique(
            &mut frame.recent_evidence,
            "recovery_gate: source=request_context outcome=context_unavailable classified=no_match"
                .into(),
        );
        if changed {
            usage.recovery_attempted = true;
            usage.recovery_tier = Some("request_context_recovery_gate".into());
            usage.recovery_outcome = Some("context_unavailable".into());
        }
        return changed;
    }

    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FallbackTier {
    TargetedEvidence,
    RecentLocalHistory,
    FullContext,
}

impl FallbackTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::TargetedEvidence => "targeted_evidence",
            Self::RecentLocalHistory => "recent_local_history",
            Self::FullContext => "full_context",
        }
    }
}

#[derive(Debug, Default)]
struct FallbackLadderState {
    targeted_evidence_activated: bool,
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

fn targeted_evidence_candidates(requested: &[String]) -> Vec<String> {
    let mut items = Vec::new();
    for request in requested {
        let candidate = request.trim();
        if candidate.is_empty() {
            continue;
        }
        let normalized = format!("targeted_evidence: selector={candidate}");
        if !items.iter().any(|existing| existing == &normalized) {
            items.push(normalized);
        }
    }
    items
}

fn activate_targeted_evidence_fallback(frame: &mut StateFrame, requested: &[String]) -> bool {
    let requested_summary = fallback_requested_summary(requested);
    let mut changed = push_unique(
        &mut frame.recent_evidence,
        format!(
            "fallback_context: tier=targeted_evidence reason=request_context_unresolved requested={requested_summary}"
        ),
    );
    for item in targeted_evidence_candidates(requested) {
        changed |= push_unique(
            &mut frame.recent_evidence,
            format!(
                "fallback_context_item: tier=targeted_evidence source=requested_context excerpt={item}"
            ),
        );
    }
    changed
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

fn requested_selector_has_contract_gap(frame: &StateFrame, requested: &[String]) -> bool {
    requested
        .iter()
        .any(|raw| match parse_needed_context_selector(raw) {
            NeededContextSelector::ArtifactRef { query } => query
                .as_deref()
                .map(|q| {
                    frame
                        .stage_execution_contract
                        .declared_artifact_by_ref(q)
                        .is_some()
                        || frame
                            .stage_execution_contract
                            .declared_artifact_by_path(q)
                            .is_some()
                        || frame
                            .stage_execution_contract
                            .verification_by_target_ref(q)
                            .is_some()
                        || frame
                            .stage_execution_contract
                            .verification_by_target_path(q)
                            .is_some()
                })
                .unwrap_or(!frame.stage_execution_contract.declared_artifacts.is_empty()),
            NeededContextSelector::Artifact { path } => path
                .as_deref()
                .map(|p| {
                    frame
                        .stage_execution_contract
                        .declared_artifact_by_path(
                            p.trim().trim_end_matches(":exists_confirmation"),
                        )
                        .is_some()
                        || frame
                            .stage_execution_contract
                            .verification_by_target_path(
                                p.trim().trim_end_matches(":exists_confirmation"),
                            )
                            .is_some()
                })
                .unwrap_or(!frame.stage_execution_contract.declared_artifacts.is_empty()),
            NeededContextSelector::TestFailure { query } => query
                .as_deref()
                .map(|q| frame.stage_execution_contract.test_by_name(q).is_some())
                .unwrap_or(!frame.stage_execution_contract.tests.is_empty()),
            _ => false,
        })
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
    contract_gap_present: bool,
    local_memory_hit: bool,
) -> Option<FallbackTier> {
    if local_memory_hit {
        return None;
    }
    if escalate && !ladder.full_context_activated {
        if activate_full_context_fallback(frame, requested) {
            ladder.full_context_activated = true;
            return Some(FallbackTier::FullContext);
        }
        ladder.full_context_activated = true;
    }
    if contract_gap_present && !ladder.targeted_evidence_activated {
        if activate_targeted_evidence_fallback(frame, requested) {
            ladder.targeted_evidence_activated = true;
            return Some(FallbackTier::TargetedEvidence);
        }
        ladder.targeted_evidence_activated = true;
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
        FallbackTier::TargetedEvidence => "request_context_targeted_evidence",
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

fn is_broad_discovery_tool(action_type: &str) -> bool {
    matches!(action_type, "Glob" | "Grep" | "ToolSearch")
}

fn primary_declared_target_path(frame: &StateFrame) -> Option<&str> {
    frame
        .stage_execution_contract
        .declared_artifacts
        .iter()
        .map(|artifact| artifact.path.trim())
        .find(|path| !path.is_empty())
}

fn has_explicit_implementation_target(frame: &StateFrame) -> bool {
    primary_declared_target_path(frame).is_some()
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
            if has_explicit_implementation_target(frame)
                && is_broad_discovery_tool(next_action.action_type.trim())
            {
                let target_hint = primary_declared_target_path(frame)
                    .map(|path| {
                        format!(
                            "target path `{path}` is already declared; use request_context:file_snippet:{path} or a narrow Read on that exact path instead of broad discovery"
                        )
                    })
                    .unwrap_or_else(|| {
                        "target path is already declared; use request_context:file_snippet:<target_path> or a narrow Read instead of broad discovery".into()
                    });
                return Err(RepairNeeded {
                    reason: format!(
                        "broad discovery tool {} is not allowed for this direct implement step: {}",
                        next_action.action_type.trim(),
                        target_hint
                    ),
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
    requested
        .iter()
        .find_map(|raw| match parse_needed_context_selector(raw) {
            NeededContextSelector::FileSnippet { path } => {
                let trimmed = path.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            }
            NeededContextSelector::Artifact { path: Some(path) } => {
                let trimmed = path.trim();
                (!trimmed.is_empty() && !trimmed.ends_with(":exists_confirmation"))
                    .then(|| trimmed.to_string())
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
    if let Some(path) = parse_read_path(decision) {
        if path.trim().ends_with(":exists_confirmation") {
            let record = build_execution_record(
                "Read",
                &ToolResult::Interrupted(
                    "artifact exists_confirmation selector is not a filesystem path".into(),
                ),
                None,
            );
            return Err(CallToolDispatchError {
                reason: "invalid input: artifact exists_confirmation selector is not a filesystem path; use typed artifact context, then create the writable target directory or write the required artifact files".into(),
                outcome: classify_tool_outcome(
                    frame,
                    decision,
                    &record,
                    "invalid input: artifact exists_confirmation selector is not a filesystem path",
                    dispatch_seq.saturating_add(1),
                ),
                record,
            });
        }
    }
    let tool_runtime = tool_runtime.ok_or_else(|| CallToolDispatchError {
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
        outcome: classify_tool_outcome(
            frame,
            decision,
            &build_execution_record(
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
            "call_tool requested but StateFrame tool runtime is unavailable",
            dispatch_seq.saturating_add(1),
        ),
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
            outcome: classify_tool_outcome(
                frame,
                decision,
                &build_execution_record(
                    "unknown",
                    &ToolResult::Interrupted("call_tool requested without next_action".into()),
                    None,
                ),
                "call_tool requested without next_action",
                dispatch_seq.saturating_add(1),
            ),
        })?;
    let canonical_args = canonicalize_next_action_args(decision);
    let input = if canonical_args.is_string() {
        canonical_args.as_str().unwrap_or_default().to_string()
    } else {
        serde_json::to_string(&canonical_args).map_err(|error| CallToolDispatchError {
            reason: format!("failed to serialize tool args: {error}"),
            record: build_execution_record(
                next_action.action_type.clone(),
                &ToolResult::Interrupted(format!("failed to serialize tool args: {error}")),
                None,
            ),
            outcome: classify_tool_outcome(
                frame,
                decision,
                &build_execution_record(
                    next_action.action_type.clone(),
                    &ToolResult::Interrupted(format!("failed to serialize tool args: {error}")),
                    None,
                ),
                &format!("failed to serialize tool args: {error}"),
                dispatch_seq.saturating_add(1),
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
            outcome: classify_tool_outcome(
                frame,
                decision,
                &build_execution_record(
                    next_action.action_type.clone(),
                    &ToolResult::Interrupted(format!("tool dispatch failed: {error}")),
                    observable_input.clone(),
                ),
                &format!("tool dispatch failed: {error}"),
                dispatch_seq.saturating_add(1),
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
    let tool_outcome = classify_tool_outcome(
        frame,
        decision,
        &record,
        record.summary.as_str(),
        *dispatch_seq,
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
            let mut success_outcome = tool_outcome.clone();
            success_outcome.evidence_ref = Some(format!("tool_output:{dispatch_seq}"));
            success_outcome.bounded_excerpt = Some(excerpt.clone());
            changed |= push_tool_outcome_evidence(
                frame,
                &record,
                &success_outcome,
                *dispatch_seq,
                &source_event_id,
            );
            let mut ledgers = StepFactLedgers::default();
            append_runtime_tool_record(
                &frame.stage_execution_contract,
                &mut ledgers,
                &record,
                &format!("runtime:{}", dispatch_seq),
            );
            let fact_lines = fact_lines_from_ledgers(&ledgers);
            ref_write_count += fact_lines.len();
            for line in fact_lines {
                changed |= push_unique(&mut frame.recent_evidence, line);
            }
            if let Some(path) = parse_read_path(decision)
                .or_else(|| observable_path_from_input(observable_input.as_ref()))
            {
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
            if let Some(path) = parse_edit_path(decision)
                .or_else(|| observable_path_from_input(observable_input.as_ref()))
            {
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
        ToolResult::ResultTooLarge(ref message)
        | ToolResult::Interrupted(ref message)
        | ToolResult::Denied(ref message)
        | ToolResult::Progress(ref message) => Err(CallToolDispatchError {
            reason: format!(
                "call_tool {} did not produce usable text: {}",
                next_action.action_type, message
            ),
            record: record.clone(),
            outcome: classify_tool_outcome(
                frame,
                decision,
                &record,
                &format!(
                    "call_tool {} did not produce usable text: {}",
                    next_action.action_type, message
                ),
                *dispatch_seq,
            ),
        }),
        ToolResult::PendingApproval { ref message, .. } => Err(CallToolDispatchError {
            reason: format!(
                "call_tool {} requires approval: {}",
                next_action.action_type, message
            ),
            record: record.clone(),
            outcome: classify_tool_outcome(
                frame,
                decision,
                &record,
                &format!(
                    "call_tool {} requires approval: {}",
                    next_action.action_type, message
                ),
                *dispatch_seq,
            ),
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

fn canonical_arg_shape(tool_name: &str) -> Option<&'static str> {
    match tool_name {
        "Read" => Some("Read.file_path"),
        "Edit" => Some("Edit.file_path/old_string/new_string"),
        "Write" => Some("Write.file_path/content"),
        "Bash" => Some("Bash.command"),
        _ => None,
    }
}

fn has_create_permission_for_path(frame: &StateFrame, path: &str) -> bool {
    let marker = format!("fact: permission_to_create_and_write:{path} ");
    frame
        .recent_evidence
        .iter()
        .any(|line| line.starts_with(&marker))
}

fn outcome_excerpt(text: &str) -> String {
    compact_tool_excerpt(text, 220)
}

fn classify_tool_outcome(
    frame: &StateFrame,
    decision: &crate::core::state_frame::StateDecision,
    record: &ToolExecutionRecord,
    reason: &str,
    dispatch_seq: usize,
) -> ToolOutcome {
    let tool_name = record.tool_name.as_str();
    let path = parse_read_path(decision)
        .or_else(|| parse_edit_path(decision))
        .or_else(|| observable_path_from_input(record.observable_input.as_ref()));
    let excerpt = outcome_excerpt(
        record
            .detail
            .as_deref()
            .unwrap_or_else(|| record.summary.as_str()),
    );
    let mut outcome = ToolOutcome {
        kind: ToolOutcomeKind::RuntimeError,
        recoverable: false,
        recommended_next_action: None,
        evidence_ref: Some(format!("tool_feedback:{dispatch_seq}")),
        bounded_excerpt: Some(excerpt),
        truncated: matches!(
            record.kind,
            crate::tool::result::ToolExecutionOutcomeKind::ResultTooLarge
        ),
    };
    let lowered = reason.to_ascii_lowercase();
    if matches!(
        record.kind,
        crate::tool::result::ToolExecutionOutcomeKind::Success
    ) {
        outcome.kind = ToolOutcomeKind::Success;
        outcome.recoverable = false;
        outcome.evidence_ref = Some(format!("tool_output:{dispatch_seq}"));
        return outcome;
    }
    if matches!(
        record.kind,
        crate::tool::result::ToolExecutionOutcomeKind::ResultTooLarge
    ) {
        outcome.kind = ToolOutcomeKind::ResultTooLarge;
        outcome.recoverable = true;
        outcome.recommended_next_action = Some(if tool_name == "Read" {
            "use_narrower_read_or_local_script".into()
        } else {
            "inspect_bounded_excerpt_and_follow_evidence_ref".into()
        });
        return outcome;
    }
    if matches!(
        record.kind,
        crate::tool::result::ToolExecutionOutcomeKind::Denied
            | crate::tool::result::ToolExecutionOutcomeKind::PendingApproval
    ) {
        outcome.kind = ToolOutcomeKind::PermissionDenied;
        outcome.recoverable = false;
        outcome.recommended_next_action =
            Some("request_approval_or_adjust_permission_scope".into());
        return outcome;
    }
    if lowered.contains("old_string not found") {
        outcome.kind = ToolOutcomeKind::UserError;
        outcome.recoverable = true;
        outcome.recommended_next_action = Some("read_before_edit".into());
        return outcome;
    }
    if lowered.contains("no such file or directory")
        || lowered.contains("failed to read")
        || lowered.contains("failed to access")
    {
        outcome.kind = ToolOutcomeKind::MissingPath;
        if let Some(path) = path.as_deref() {
            if has_create_permission_for_path(frame, path) {
                outcome.recoverable = true;
                let recommended = if std::path::Path::new(path).extension().is_some() {
                    "create_parent_directory_and_write_target_file"
                } else {
                    "create_directory"
                };
                outcome.recommended_next_action = Some(recommended.into());
            } else {
                outcome.recoverable = false;
                outcome.recommended_next_action = Some("context_unavailable".into());
            }
        } else {
            outcome.recommended_next_action = Some("context_unavailable".into());
        }
        return outcome;
    }
    if lowered.contains("invalid input")
        || lowered.contains("requires json-structured input")
        || lowered.contains("call_tool requested without next_action")
        || lowered.contains("artifact exists_confirmation selector")
    {
        outcome.kind = ToolOutcomeKind::SchemaInvalid;
        outcome.recoverable = true;
        outcome.recommended_next_action =
            canonical_arg_shape(tool_name).map(|shape| format!("use_canonical_args:{shape}"));
        return outcome;
    }
    if lowered.contains("unknown tool") {
        outcome.kind = ToolOutcomeKind::UserError;
        outcome.recoverable = true;
        outcome.recommended_next_action = Some("use_one_of_allowed_tools".into());
        return outcome;
    }
    if lowered.contains("timeout") {
        outcome.kind = ToolOutcomeKind::Timeout;
        outcome.recoverable = true;
        outcome.recommended_next_action = Some("retry_with_shorter_or_narrower_command".into());
        return outcome;
    }
    if lowered.contains("requires approval") || lowered.contains("permission") {
        outcome.kind = ToolOutcomeKind::PermissionDenied;
        outcome.recoverable = false;
        outcome.recommended_next_action =
            Some("request_approval_or_adjust_permission_scope".into());
        return outcome;
    }
    if lowered.contains("runtime is unavailable") {
        outcome.kind = ToolOutcomeKind::ExternalBlocker;
        outcome.recoverable = false;
        outcome.recommended_next_action = Some("runtime_unavailable".into());
        return outcome;
    }
    outcome
}

fn push_tool_outcome_evidence(
    frame: &mut StateFrame,
    record: &ToolExecutionRecord,
    outcome: &ToolOutcome,
    dispatch_seq: usize,
    source_event_id: &str,
) -> bool {
    let recommended_next_action = outcome.recommended_next_action.as_deref().unwrap_or("none");
    let evidence_ref = outcome.evidence_ref.as_deref().unwrap_or("none");
    let bounded_excerpt = outcome.bounded_excerpt.as_deref().unwrap_or("none");
    push_unique(
        &mut frame.recent_evidence,
        format!(
            "tool_outcome: ref=tool_outcome:{dispatch_seq} tool={} kind={} recoverable={} recommended_next_action={} evidence_ref={} source_event_id={} truncated={} bounded_excerpt={}",
            record.tool_name,
            outcome.kind.as_str(),
            outcome.recoverable,
            recommended_next_action,
            evidence_ref,
            source_event_id,
            outcome.truncated,
            bounded_excerpt
        ),
    )
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
    outcome: &ToolOutcome,
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
    changed |= push_tool_outcome_evidence(frame, record, outcome, dispatch_seq, &source_event_id);

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
            if has_create_permission_for_path(frame, &path) {
                if std::path::Path::new(&path).extension().is_some() {
                    feedback_tail
                        .push_str(" recovery_hint=create_parent_directory_and_write_target_file");
                } else {
                    feedback_tail.push_str(" recovery_hint=create_directory_then_write_files");
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
            "tool_feedback: ref=tool_feedback:{} tool={} outcome={} category={} recoverable={} recommended_next_action={} evidence_ref={} truncated={} source_event_id={}{} summary={}",
            dispatch_seq,
            record.tool_name,
            outcome_kind_label(&record.kind),
            category,
            outcome.recoverable,
            outcome.recommended_next_action.as_deref().unwrap_or("none"),
            outcome.evidence_ref.as_deref().unwrap_or("none"),
            outcome.truncated,
            source_event_id,
            feedback_tail,
            detail
        ),
    );

    let mut ledgers = StepFactLedgers::default();
    append_runtime_tool_record(
        &frame.stage_execution_contract,
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
        || lowered.contains("broad discovery tool")
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
    append_runtime_contract_facts(&mut frame);
    let initial_requests = initial_target_hydration_requests(&frame);
    let initial_hydration = hydrate_needed_context(&mut frame, &initial_requests);
    total_usage.hydration_count += initial_hydration.hydrated.len();
    total_usage.hydration_from_contract_count += initial_hydration.hydration_from_contract_count;
    total_usage.hydration_from_ledger_count += initial_hydration.hydration_from_ledger_count;
    total_usage.stale_ref_count += initial_hydration.stale.len();
    total_usage.hydration_ref_missing += initial_hydration.unavailable.len();
    total_usage.hydration_miss_unsupported_count +=
        initial_hydration.hydration_miss_unsupported_count;
    total_usage.hydration_miss_stale_count += initial_hydration.hydration_miss_stale_count;
    total_usage.hydration_miss_no_match_count += initial_hydration.hydration_miss_no_match_count;

    for _iter in 0..config.max_iterations {
        append_runtime_contract_facts(&mut frame);
        if let Some(outcome) = completion_gate_repair_terminal_outcome(&frame, &mut total_usage) {
            return Ok(outcome);
        }
        if let Some(outcome) = verification_terminal_outcome(&frame, &mut total_usage) {
            return Ok(outcome);
        }
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
                finalize_worker_usage_report(&frame, &mut total_usage);
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
                    let repair_prompt = build_state_decision_repair_prompt(
                        &last_repair.reason,
                        &last_repair.raw_json,
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
                            finalize_worker_usage_report(&frame, &mut total_usage);
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
                        finalize_worker_usage_report(&frame, &mut total_usage);
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
                frame.state = decision.state;
                if let Err(block) = enforce_completion_gate(&mut frame, &mut total_usage) {
                    inject_completion_gate_block(&mut frame, &block);
                    record_completion_gate_recovery(&frame, &mut total_usage, &block);
                    continue;
                }
                finalize_worker_usage_report(&frame, &mut total_usage);
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
                frame.state = decision.state;
                finalize_worker_usage_report(&frame, &mut total_usage);
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
                    if let Err(block) = enforce_completion_gate(&mut frame, &mut total_usage) {
                        inject_completion_gate_block(&mut frame, &block);
                        record_completion_gate_recovery(&frame, &mut total_usage, &block);
                        continue;
                    }
                    finalize_worker_usage_report(&frame, &mut total_usage);
                    return Ok(LoopOutcome::Done {
                        final_state: AgentState::Done,
                        usage: total_usage,
                    });
                }
                if matches!(
                    evaluate_completion_evidence(&frame, &total_usage),
                    CompletionEvidenceStatus::MissingVerificationEvidence
                ) {
                    let block = CompletionGateBlock {
                        status: CompletionEvidenceStatus::MissingVerificationEvidence,
                        required_action: "verify_artifact".into(),
                        reason: "artifact verification failure requires repair continuation".into(),
                        missing_evidence_refs: missing_verification_evidence_refs(&frame),
                    };
                    inject_completion_gate_block(&mut frame, &block);
                    record_completion_gate_recovery(&frame, &mut total_usage, &block);
                    continue;
                }
                let after = frame.to_prompt_segment().content;
                if before == after {
                    finalize_worker_usage_report(&frame, &mut total_usage);
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
                total_usage.hydration_from_contract_count += summary.hydration_from_contract_count;
                total_usage.hydration_from_ledger_count += summary.hydration_from_ledger_count;
                total_usage.stale_ref_count += summary.stale.len();
                total_usage.hydration_ref_missing += summary.unavailable.len();
                total_usage.hydration_miss_unsupported_count +=
                    summary.hydration_miss_unsupported_count;
                total_usage.hydration_miss_stale_count += summary.hydration_miss_stale_count;
                total_usage.hydration_miss_no_match_count += summary.hydration_miss_no_match_count;
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
                                total_usage.last_effective_tool_action =
                                    Some(record.tool_name.clone());
                                total_usage.last_failure_outcome = None;
                                clear_recovery_after_success(&mut total_usage);
                                total_usage.tool_execution_records.push(record);
                                if let Some(outcome) = completion_gate_repair_terminal_outcome(
                                    &frame,
                                    &mut total_usage,
                                ) {
                                    return Ok(outcome);
                                }
                                if let Some(outcome) =
                                    verification_terminal_outcome(&frame, &mut total_usage)
                                {
                                    return Ok(outcome);
                                }
                                if changed {
                                    summary = hydrate_needed_context(
                                        &mut frame,
                                        &decision.needed_context,
                                    );
                                    total_usage.hydration_count += summary.hydrated.len();
                                    total_usage.hydration_from_contract_count +=
                                        summary.hydration_from_contract_count;
                                    total_usage.hydration_from_ledger_count +=
                                        summary.hydration_from_ledger_count;
                                    total_usage.stale_ref_count += summary.stale.len();
                                    total_usage.hydration_ref_missing += summary.unavailable.len();
                                    total_usage.hydration_miss_unsupported_count +=
                                        summary.hydration_miss_unsupported_count;
                                    total_usage.hydration_miss_stale_count +=
                                        summary.hydration_miss_stale_count;
                                    total_usage.hydration_miss_no_match_count +=
                                        summary.hydration_miss_no_match_count;
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
                                    &error.outcome,
                                    tool_dispatch_seq,
                                    &error.reason,
                                );
                                total_usage.tool_dispatch_ref_write_count += ref_write_count;
                                total_usage.last_effective_tool_action =
                                    Some(error.record.tool_name.clone());
                                total_usage.last_failure_outcome = Some(error.outcome.clone());
                                record_recoverable_tool_failure(
                                    &mut total_usage,
                                    &error.outcome,
                                    current_action_target_path(&synthetic_read),
                                );
                                total_usage.tool_execution_records.push(error.record);
                                if changed {
                                    summary = hydrate_needed_context(
                                        &mut frame,
                                        &decision.needed_context,
                                    );
                                    total_usage.hydration_count += summary.hydrated.len();
                                    total_usage.hydration_from_contract_count +=
                                        summary.hydration_from_contract_count;
                                    total_usage.hydration_from_ledger_count +=
                                        summary.hydration_from_ledger_count;
                                    total_usage.stale_ref_count += summary.stale.len();
                                    total_usage.hydration_ref_missing += summary.unavailable.len();
                                    total_usage.hydration_miss_unsupported_count +=
                                        summary.hydration_miss_unsupported_count;
                                    total_usage.hydration_miss_stale_count +=
                                        summary.hydration_miss_stale_count;
                                    total_usage.hydration_miss_no_match_count +=
                                        summary.hydration_miss_no_match_count;
                                }
                            }
                        }
                    }
                    if summary.hydrated.is_empty() {
                        let contract_gap_present =
                            requested_selector_has_contract_gap(&frame, &decision.needed_context);
                        let local_memory_hit = frame.recent_evidence.iter().any(|line| {
                            line.starts_with("fallback_context_item: tier=recent_local_history")
                        }) && summary.hydration_from_ledger_count > 0;
                        let fallback_tier = if contract_gap_present || decision.escalate {
                            activate_fallback_tier(
                                &mut frame,
                                &decision.needed_context,
                                &mut fallback_ladder,
                                decision.escalate,
                                contract_gap_present,
                                local_memory_hit,
                            )
                        } else {
                            None
                        };
                        if let Some(fallback_tier) = fallback_tier {
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
                    if inject_request_context_recovery_gate(&mut frame, &mut total_usage) {
                        continue;
                    }
                    finalize_worker_usage_report(&frame, &mut total_usage);
                    return Ok(LoopOutcome::NoProgress {
                        last_state: frame.state,
                        reason: "request_context decision produced no hydration progress".into(),
                        usage: total_usage,
                    });
                }
            }
            DecisionKind::CallTool => {
                frame.state = decision.state;
                if let Some(outcome) =
                    completion_gate_repair_terminal_outcome(&frame, &mut total_usage)
                {
                    return Ok(outcome);
                }
                if let Some(outcome) = verification_terminal_outcome(&frame, &mut total_usage) {
                    return Ok(outcome);
                }
                if let Some(reason) = repeated_recovery_strategy_reason(&total_usage, &decision) {
                    push_unique(
                        &mut frame.recent_evidence,
                        format!(
                            "recovery_guard: reason={} target_path={} enforced_outcome=no_progress",
                            reason,
                            current_action_target_path(&decision).unwrap_or_else(|| "none".into())
                        ),
                    );
                    total_usage.recovery_attempted = true;
                    total_usage.recovery_tier = Some("strategy_dedupe".into());
                    total_usage.recovery_outcome = Some("no_progress_escalation".into());
                    total_usage.terminal_blocker_kind = Some("same_invalid_strategy".into());
                    finalize_worker_usage_report(&frame, &mut total_usage);
                    return Ok(LoopOutcome::NoProgress {
                        last_state: frame.state,
                        reason,
                        usage: total_usage,
                    });
                }
                if let Some((reason, missing_source_targets)) =
                    verification_report_read_tailspin_reason(&frame, &total_usage, &decision)
                {
                    let block = CompletionGateBlock {
                        status: CompletionEvidenceStatus::MissingVerificationEvidence,
                        required_action: "read_source_evidence".into(),
                        reason: reason.clone(),
                        missing_evidence_refs: missing_source_targets,
                    };
                    inject_completion_gate_block(&mut frame, &block);
                    total_usage.completion_evidence_status =
                        Some(CompletionEvidenceStatus::MissingVerificationEvidence);
                    total_usage.recovery_attempted = true;
                    total_usage.recovery_tier = Some("verification_repair_continuation".into());
                    total_usage.recovery_outcome = Some("repair_turn_injected".into());
                    total_usage.terminal_blocker_kind = None;
                    total_usage.last_recovery_attempt = Some(RecoveryAttempt {
                        failure_kind: block.status.as_str().to_string(),
                        recommended_next_action: block.required_action.clone(),
                        target_path: current_action_target_path(&decision),
                    });
                    finalize_worker_usage_report(&frame, &mut total_usage);
                    return Ok(LoopOutcome::ToolDispatchFailed {
                        last_state: frame.state,
                        reason,
                        usage: total_usage,
                    });
                }
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
                        total_usage.last_effective_tool_action = Some(record.tool_name.clone());
                        total_usage.last_failure_outcome = None;
                        clear_recovery_after_success(&mut total_usage);
                        total_usage.tool_execution_records.push(record);
                        if let Some(outcome) =
                            completion_gate_repair_terminal_outcome(&frame, &mut total_usage)
                        {
                            return Ok(outcome);
                        }
                        if let Some(outcome) =
                            verification_terminal_outcome(&frame, &mut total_usage)
                        {
                            return Ok(outcome);
                        }
                        if !changed {
                            finalize_worker_usage_report(&frame, &mut total_usage);
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
                            &error.outcome,
                            tool_dispatch_seq,
                            &error.reason,
                        );
                        total_usage.tool_dispatch_ref_write_count += ref_write_count;
                        total_usage.last_effective_tool_action =
                            Some(error.record.tool_name.clone());
                        total_usage.last_failure_outcome = Some(error.outcome.clone());
                        record_recoverable_tool_failure(
                            &mut total_usage,
                            &error.outcome,
                            current_action_target_path(&decision),
                        );
                        total_usage.tool_execution_records.push(error.record);
                        if !changed {
                            finalize_worker_usage_report(&frame, &mut total_usage);
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

        if let Some(outcome) = completion_gate_repair_terminal_outcome(&frame, &mut total_usage) {
            return Ok(outcome);
        }
        if let Some(outcome) = verification_terminal_outcome(&frame, &mut total_usage) {
            return Ok(outcome);
        }
        if promote_recovered_verification_gap(&mut frame, &mut total_usage) {
            continue;
        }
    }

    terminalize_verification_only_gap(&mut frame, &mut total_usage);
    if verification_gap_still_actionable(&frame, &total_usage) {
        frame.state = AgentState::Verifying;
        total_usage.recovery_outcome = Some("repair_turn_injected".into());
        total_usage.terminal_blocker_kind = Some("verification_repair_continuation".into());
        finalize_worker_usage_report(&frame, &mut total_usage);
        return Ok(LoopOutcome::MaxIterationsReached {
            last_state: frame.state,
            usage: total_usage,
        });
    }
    finalize_worker_usage_report(&frame, &mut total_usage);
    Ok(LoopOutcome::MaxIterationsReached {
        last_state: frame.state,
        usage: total_usage,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DecisionLoopConfig, LoopOutcome, LoopUsage, RecoveryAttempt, StateFrameToolRuntime,
        append_runtime_contract_facts, build_state_decision_repair_prompt, classify_tool_outcome,
        evaluate_completion_evidence, execute_call_tool, parse_and_validate_decision,
        push_tool_failure_feedback, push_tool_outcome_evidence, run_decision_loop,
        run_decision_loop_with_tools, tool_backed_hydration_path,
    };
    use crate::core::state_frame::validate_state_decision;
    use crate::core::state_frame::{
        ActorRole, AgentState, CompletionEvidenceStatus, DeclaredArtifactContract,
        StageExecutionContract, StateBudget, StateFrame, TestContract, VerificationContract,
    };
    use crate::core::state_frame_hydration::hydrate_needed_context;
    use crate::service::api::client::ModelProviderClient;
    use crate::service::api::streaming::{ProviderFailureDisposition, StreamError, StreamEvent};
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use crate::tool::builtin::bash::BashTool;
    use crate::tool::builtin::file_edit::FileEditTool;
    use crate::tool::builtin::file_read::FileReadTool;
    use crate::tool::definition::{ObservableInput, ObservableInputSource};
    use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
    use crate::tool::orchestrator::build_execution_record;
    use crate::tool::registry::ToolRegistry;
    use crate::tool::result::{ToolBatchContext, ToolExecutionOutcomeKind, ToolExecutionRecord};
    use crate::tool::result::{ToolOutcome, ToolOutcomeKind};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct ArtifactVerifyMockTool;

    #[test]
    fn repair_prompt_restates_canonical_enums_and_alias_mapping() {
        let prompt = build_state_decision_repair_prompt(
            "missing or unsupported state/next_state",
            r#"{"state":"executed","decision":"success"}"#,
        );

        assert!(prompt.contains("Canonical state values: planning, executing, reviewing, correcting, verifying, blocked, done."));
        assert!(prompt.contains("Canonical decision values: continue, request_context, call_tool, handoff, accept, reject, done."));
        assert!(prompt.contains("executed -> executing, completed -> done, success -> done"));
        assert!(prompt.contains("canonical StateDecision JSON only"));
    }

    #[async_trait::async_trait]
    impl Tool for ArtifactVerifyMockTool {
        fn metadata(&self) -> crate::tool::definition::ToolMetadata {
            ToolMetadata {
                name: "ArtifactVerify",
                description: "mock artifact verification tool",
                aliases: &[],
                search_hint: None,
                read_only: true,
                destructive: false,
                concurrency_safe: true,
                always_load: true,
                should_defer: false,
                requires_auth: false,
                requires_user_interaction: false,
                is_open_world: false,
                is_search_or_read_command: false,
            }
        }

        async fn invoke(
            &self,
            _call: &ToolCall,
            _permissions: &ToolPermissionContext,
        ) -> anyhow::Result<ToolResult> {
            Ok(ToolResult::Text(
                "artifact verification passed for /tmp/report.md".into(),
            ))
        }
    }

    fn make_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "update src/core/state_frame_projection.rs and get tests passing".into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: vec!["worker must preserve prior review signal".into()],
            recent_evidence: vec![
                "fact: recent_changes_in_files ref=change:1 path=src/core/state_frame_projection.rs source=worker_result source_event_id=worker-result:1 freshness=after-worker-output confidence=0.90 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated src/core/state_frame_projection.rs".into(),
                "fact: test_failures ref=test:1 name=worker_reported_tests status=failed source=worker_result source_event_id=worker-result:2 freshness=after-worker-output confidence=0.85 status=active invalidated_by=none supersedes=none conflicts_with=none summary=tests failed in boss_flow".into(),
            ],
            allowed_actions: vec!["read_file".into()],
            allowed_tools: vec!["Read".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
        }
    }

    fn make_clean_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "typed contract hydration test".into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: Vec::new(),
            allowed_actions: vec!["read_file".into()],
            allowed_tools: vec!["Read".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
        }
    }

    fn verification_repair_tool_runtime() -> StateFrameToolRuntime {
        StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(ArtifactVerifyMockTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        }
    }

    #[test]
    fn runtime_contract_facts_explain_unlimited_tool_budget() {
        let mut frame = make_frame();
        frame.allowed_actions = vec!["read_file".into(), "edit_file".into()];
        frame.allowed_tools = vec!["Read".into(), "Edit".into()];
        frame.budget.max_tool_calls = 0;

        append_runtime_contract_facts(&mut frame);

        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("fact: budget.max_tool_calls")
                && item.contains("value=unlimited")
                && item.contains("0 means unlimited")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("fact: allow_worker_tool_calls")
                && item.contains("status=allowed")
                && item.contains("allowed_actions=read_file|edit_file")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("fact: increase_max_tool_calls") && item.contains("status=not_needed")
        }));
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

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "stateframe_{}_{}_{}",
            label,
            std::process::id(),
            nonce
        ))
    }

    fn test_runtime_paths() -> (std::path::PathBuf, Option<std::path::PathBuf>) {
        let cwd = std::env::temp_dir().join("state_frame_loop_tests");
        let config_root = Some(cwd.join(".morgo"));
        (cwd, config_root)
    }

    fn push_completion_contract_with_refs(
        frame: &mut StateFrame,
        artifact_refs: &[&str],
        test_refs: &[&str],
        verification_refs: &[&str],
    ) {
        frame.recent_evidence.push(format!(
            "fact: completion_contract artifact_evidence={} artifact_refs={} test_evidence={} test_refs={} verification_evidence={} verification_refs={}",
            if artifact_refs.is_empty() {
                "not_required"
            } else {
                "required"
            },
            if artifact_refs.is_empty() {
                "none".to_string()
            } else {
                artifact_refs.join("|")
            },
            if test_refs.is_empty() {
                "not_required"
            } else {
                "required"
            },
            if test_refs.is_empty() {
                "none".to_string()
            } else {
                test_refs.join("|")
            },
            if verification_refs.is_empty() {
                "not_required"
            } else {
                "required"
            },
            if verification_refs.is_empty() {
                "none".to_string()
            } else {
                verification_refs.join("|")
            },
        ));
        frame.stage_execution_contract.required_actions.clear();
        frame.stage_execution_contract.required_evidence.clear();
        for artifact_ref in artifact_refs {
            if frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .all(|artifact| artifact.ref_id != *artifact_ref)
            {
                frame
                    .stage_execution_contract
                    .declared_artifacts
                    .push(DeclaredArtifactContract {
                        ref_id: (*artifact_ref).to_string(),
                        path: String::new(),
                        kind: "file".into(),
                        required_actions: vec!["create".into(), "write".into()],
                        required_evidence: vec![(*artifact_ref).to_string()],
                    });
            }
        }
        frame.stage_execution_contract.tests = test_refs
            .iter()
            .map(|item| TestContract {
                name: (*item).to_string(),
                required_actions: vec!["run_test".into()],
                required_evidence: vec![(*item).to_string()],
            })
            .collect();
        frame.stage_execution_contract.verifications = verification_refs
            .iter()
            .map(|item| {
                let target_path = frame
                    .stage_execution_contract
                    .declared_artifacts
                    .iter()
                    .find(|artifact| artifact.ref_id == *item)
                    .map(|artifact| artifact.path.clone());
                VerificationContract {
                    target_ref: (*item).to_string(),
                    target_path,
                    required_actions: vec!["verify".into()],
                    required_evidence: vec![(*item).to_string()],
                }
            })
            .collect();
        if !artifact_refs.is_empty() {
            frame
                .stage_execution_contract
                .required_actions
                .extend(["create".into(), "write".into()]);
        }
        if !test_refs.is_empty() {
            frame
                .stage_execution_contract
                .required_actions
                .push("run_test".into());
        }
        if !verification_refs.is_empty() {
            frame
                .stage_execution_contract
                .required_actions
                .push("verify".into());
        }
        frame.stage_execution_contract.required_evidence.extend(
            artifact_refs
                .iter()
                .chain(test_refs.iter())
                .chain(verification_refs.iter())
                .map(|item| (*item).to_string()),
        );
    }

    fn push_completion_contract(
        frame: &mut StateFrame,
        artifact_required: bool,
        test_required: bool,
        verification_required: bool,
    ) {
        push_completion_contract_with_refs(
            frame,
            if artifact_required {
                &["artifact:contract:0"]
            } else {
                &[]
            },
            if test_required {
                &["openitem:test:0"]
            } else {
                &[]
            },
            if verification_required {
                &["artifact:contract:0"]
            } else {
                &[]
            },
        );
    }

    fn push_artifact_target_fact(frame: &mut StateFrame, ref_id: &str, path: &str, kind: &str) {
        frame.recent_evidence.push(format!(
            "fact: artifact_status ref={ref_id} path={path} kind={kind} status=expected source=artifact_expectation source_event_id=artifact-expectation:test freshness=current confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=target artifact declared"
        ));
        if let Some(existing) = frame
            .stage_execution_contract
            .declared_artifacts
            .iter_mut()
            .find(|item| item.ref_id == ref_id)
        {
            existing.path = path.to_string();
            existing.kind = kind.to_string();
            if existing.required_actions.is_empty() {
                existing.required_actions = vec!["create".into(), "write".into()];
            }
            if existing.required_evidence.is_empty() {
                existing.required_evidence =
                    vec![ref_id.to_string(), path.to_string(), kind.to_string()];
            }
        } else {
            frame
                .stage_execution_contract
                .declared_artifacts
                .push(DeclaredArtifactContract {
                    ref_id: ref_id.to_string(),
                    path: path.to_string(),
                    kind: kind.to_string(),
                    required_actions: vec!["create".into(), "write".into()],
                    required_evidence: vec![ref_id.to_string(), path.to_string(), kind.to_string()],
                });
        }
        for verification in frame.stage_execution_contract.verifications.iter_mut() {
            if verification.target_ref == ref_id {
                verification.target_path = Some(path.to_string());
            }
        }
    }

    #[test]
    fn request_context_no_match_without_contract_gap_stops_without_widening() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["symbol:MissingSymbol"]}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            request_json.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::RepairExhausted { usage, .. } => {
                assert_eq!(usage.fallback_count, 0);
                assert_eq!(usage.hydration_ref_missing, 1);
                assert_eq!(usage.fallback_tier, None);
            }
            other => panic!("expected RepairExhausted, got {other:?}"),
        }
    }

    #[test]
    fn contract_hit_does_not_upgrade_widening() {
        let mut frame = make_clean_frame();
        push_completion_contract(&mut frame, true, false, true);
        if let Some(artifact) = frame
            .stage_execution_contract
            .declared_artifacts
            .iter_mut()
            .find(|artifact| artifact.ref_id == "artifact:contract:0")
        {
            artifact.path = "/tmp/contract-hit.txt".into();
            artifact.kind = "file".into();
        }
        if let Some(verification) = frame
            .stage_execution_contract
            .verifications
            .iter_mut()
            .find(|verification| verification.target_ref == "artifact:contract:0")
        {
            verification.target_path = Some("/tmp/contract-hit.txt".into());
        }
        let summary =
            hydrate_needed_context(&mut frame, &["artifact_ref:artifact:contract:0".into()]);
        assert_eq!(summary.hydration_from_contract_count, 1);
        assert_eq!(summary.hydration_from_ledger_count, 0);
        assert!(summary.unavailable.is_empty());
    }

    #[test]
    fn ledger_fallback_only_happens_after_contract_miss() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["review_ref:review:step1:runtime:0"]}"#;
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
                assert_eq!(usage.fallback_count, 0);
                assert_eq!(usage.hydration_from_contract_count, 0);
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn hydration_miss_telemetry_separates_unsupported_stale_and_no_match() {
        let mut frame = make_clean_frame();
        frame.recent_evidence.push(
            "fact: review_verdicts ref=review:step1:stale verdict=rejected source=tool:BossReview source_event_id=tool-review:1:9 freshness=after-runtime-review confidence=1.00 status=stale invalidated_by=review:step1:runtime:0 supersedes=none conflicts_with=none summary=obsolete review verdict".into(),
        );
        let hydration = hydrate_needed_context(
            &mut frame,
            &[
                "review_ref:review:step1:stale".into(),
                "symbol:MissingSymbol".into(),
                "bogus_selector".into(),
            ],
        );
        assert_eq!(hydration.hydration_miss_stale_count, 1);
        assert_eq!(hydration.hydration_miss_no_match_count, 1);
        assert_eq!(hydration.hydration_miss_unsupported_count, 1);
    }

    #[test]
    fn target_artifact_fact_is_hydrated_before_first_decision() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step0:0 path=/tmp/example-site kind=directory status=missing_or_invalid source=artifact_expectation source_event_id=artifact-expectation:0:0 freshness=current confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=target directory missing".into(),
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
    fn request_context_artifact_exists_confirmation_does_not_trigger_synthetic_read() {
        assert_eq!(
            tool_backed_hydration_path(&["artifact:/tmp/demo-site:exists_confirmation".into()]),
            None
        );
        assert_eq!(
            tool_backed_hydration_path(&["artifact:/tmp/demo-site".into()]),
            Some("/tmp/demo-site".into())
        );
    }

    #[test]
    fn call_tool_read_exists_confirmation_selector_returns_recovery_feedback() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let read_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Read","args":{"file_path":"/tmp/demo-site:exists_confirmation"}}}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(read_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(FileReadTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
                assert_eq!(usage.tool_dispatch_failure_count, 1);
                assert_eq!(
                    usage.tool_dispatch_failure_taxonomy.get("schema_invalid"),
                    Some(&1)
                );
            }
            other => panic!("expected Done after recovery feedback, got {other:?}"),
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
    fn direct_tool_recovery_has_budget_to_finish_after_artifact_write() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_dir = unique_temp_dir("direct_tool_recovery_site");
        let index_path = temp_dir.join("index.html");
        let read_dir_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            temp_dir.display()
        );
        let mkdir_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Bash","args":{{"Bash.command":"mkdir -p \"{}\""}}}}}}"#,
            temp_dir.display()
        );
        let planning_json = format!(
            r#"{{"state":"planning","decision":"continue","state_patch":{{"open_items_add":["create static site files in {}"],"accepted_summary_add":["target directory exists; create files next"]}}}}"#,
            temp_dir.display()
        );
        let write_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Bash","args":{{"command":"cat > \"{}\" <<'HTML'\n<!doctype html><title>RustAgent</title>\nHTML\n"}}}}}}"#,
            index_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(read_dir_json.clone().into())],
            vec![StreamEvent::TextDelta(mkdir_json.into())],
            vec![StreamEvent::TextDelta(read_dir_json.into())],
            vec![StreamEvent::TextDelta(planning_json.into())],
            vec![StreamEvent::TextDelta(write_json.into())],
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };

        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                make_frame(),
                DecisionLoopConfig::default(),
                Some(tool_runtime),
            ))
            .expect("loop should not error");

        let content = std::fs::read_to_string(&index_path).expect("bash should create index.html");
        let _ = std::fs::remove_dir_all(&temp_dir);
        assert!(content.contains("RustAgent"));
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.tool_dispatch_count, 4);
                assert_eq!(usage.tool_dispatch_success_count, 2);
                assert_eq!(usage.tool_dispatch_failure_count, 2);
                assert_eq!(
                    usage.tool_dispatch_failure_taxonomy.get("missing_path"),
                    Some(&1)
                );
                assert_eq!(
                    usage
                        .tool_dispatch_failure_taxonomy
                        .get("tool_result_empty"),
                    Some(&1)
                );
            }
            other => panic!("expected Done after direct tool recovery, got {other:?}"),
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
    fn broad_discovery_tool_is_rejected_when_declared_target_already_exists() {
        let mut frame = make_frame();
        frame
            .stage_execution_contract
            .declared_artifacts
            .push(DeclaredArtifactContract {
                ref_id: "artifact:demo".into(),
                path: "src/demo.rs".into(),
                kind: "file".into(),
                required_actions: vec!["write_artifact".into()],
                required_evidence: vec!["artifact_evidence".into()],
            });
        let decision_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Glob","args":{"pattern":"src/**/*.rs"}}}"#;
        let err = parse_and_validate_decision(&frame, decision_json)
            .expect_err("broad discovery should be rejected when target path is already declared");
        assert!(
            err.reason
                .contains("broad discovery tool Glob is not allowed"),
            "expected broad discovery guard, got {}",
            err.reason
        );
        assert!(
            err.reason
                .contains("request_context:file_snippet:src/demo.rs")
                || err.reason.contains("narrow Read on that exact path"),
            "expected direct-path repair hint, got {}",
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
    fn missing_path_feedback_carries_directory_recovery_hint_when_permission_exists() {
        let target_dir = unique_temp_dir("missing_path_recovery_hint");
        let read_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            target_dir.display()
        );
        let decision = validate_state_decision(&read_json).expect("decision should parse");
        let mut frame = make_frame();
        frame.recent_evidence.push(format!(
            "fact: permission_to_create_and_write:{} ref=permission:step0:0 source=permission_scope source_event_id=permission-scope:0:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=worker may create and write the declared target artifact path {}",
            target_dir.display(),
            target_dir.display()
        ));
        let record = build_execution_record(
            "Read",
            &ToolResult::Interrupted("No such file or directory (os error 2)".into()),
            None,
        );
        let outcome = ToolOutcome {
            kind: ToolOutcomeKind::MissingPath,
            recoverable: true,
            recommended_next_action: Some("create_directory".into()),
            evidence_ref: Some("tool_feedback:1".into()),
            bounded_excerpt: Some("No such file or directory (os error 2)".into()),
            truncated: false,
        };
        let (changed, _) = push_tool_failure_feedback(
            &mut frame,
            &decision,
            &record,
            &outcome,
            1,
            "tool dispatch failed: No such file or directory (os error 2)",
        );
        assert!(changed);
        assert!(frame.recent_evidence.iter().any(|line| {
            line.contains("tool_feedback:")
                && line.contains("recovery_hint=create_directory_then_write_files")
        }));
        let _ = std::fs::remove_dir_all(&target_dir);
    }

    #[test]
    fn headless_repair_does_not_fall_into_awaiting_user_input() {
        let decision = crate::core::state_frame::validate_state_decision(
            r#"{
                "decision": {
                    "next_state": "awaiting_user_input",
                    "next_action": {
                        "action_type": "Write",
                        "args": {
                            "file_path": "/tmp/headless-safe-report.md",
                            "content": "fixed"
                        }
                    }
                }
            }"#,
        )
        .expect("headless repair wrapper should normalize");
        assert_eq!(decision.state, AgentState::Correcting);
        assert_eq!(
            decision.decision,
            crate::core::state_frame::DecisionKind::CallTool
        );
    }

    #[test]
    fn tool_outcome_missing_path_on_writable_target_is_recoverable_create_hint() {
        let mut frame = make_frame();
        let path = std::env::temp_dir().join("p1_outcome_writable.md");
        frame.recent_evidence.push(format!(
            "fact: permission_to_create_and_write:{} ref=permission:step0:0 source=permission_scope source_event_id=permission-scope:0:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=worker may create and write the declared target artifact path {}",
            path.display(),
            path.display()
        ));
        let decision = validate_state_decision(&format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            path.display()
        ))
        .expect("decision");
        let record = build_execution_record(
            "Read",
            &ToolResult::Interrupted("failed to read missing path".into()),
            None,
        );
        let outcome = classify_tool_outcome(
            &frame,
            &decision,
            &record,
            "failed to read /tmp/p1_outcome_writable.md: No such file or directory (os error 2)",
            1,
        );
        assert_eq!(outcome.kind.as_str(), "missing_path");
        assert!(outcome.recoverable);
        assert_eq!(
            outcome.recommended_next_action.as_deref(),
            Some("create_parent_directory_and_write_target_file")
        );
    }

    #[test]
    fn dir_plus_file_missing_path_repair_issues_create_and_write_once() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        let target_path = "/tmp/headless-repair/report.md";
        frame.recent_evidence.push(format!(
            "fact: permission_to_create_and_write:{target_path} ref=permission:file source=permission_scope source_event_id=permission:1 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=write target"
        ));
        let mut usage = LoopUsage::default();
        usage.last_recovery_attempt = Some(RecoveryAttempt {
            failure_kind: "missing_path".into(),
            recommended_next_action: "create_parent_directory_and_write_target_file".into(),
            target_path: Some(target_path.into()),
        });
        assert!(super::inject_missing_path_recovery_gate(
            &mut frame, &mut usage
        ));
        let repair_line = frame
            .recent_evidence
            .iter()
            .find(|line| line.starts_with("fact: repair_turn "))
            .expect("repair turn");
        assert!(repair_line.contains("target_path=/tmp/headless-repair/report.md"));
        assert!(repair_line.contains("create_parent_directory=true"));
        assert!(repair_line.contains("write_target_file=true"));
        assert!(
            repair_line.contains(
                "recommended_write_strategy=create_parent_directory_and_write_target_file"
            )
        );
    }

    #[test]
    fn repeated_create_directory_without_write_is_not_accepted() {
        let decision = validate_state_decision(
            r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Bash","args":{"command":"mkdir -p /tmp/headless-repair"}}}"#,
        )
        .expect("decision");
        let mut usage = LoopUsage::default();
        usage.last_recovery_attempt = Some(RecoveryAttempt {
            failure_kind: "missing_path".into(),
            recommended_next_action: "create_parent_directory_and_write_target_file".into(),
            target_path: Some("/tmp/headless-repair/report.md".into()),
        });
        let reason =
            super::repeated_recovery_strategy_reason(&usage, &decision).expect("dedupe reason");
        assert!(reason.contains("without write"));
    }

    #[test]
    fn tool_outcome_missing_path_on_readonly_evidence_is_context_unavailable() {
        let frame = make_frame();
        let decision = validate_state_decision(
            r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Read","args":{"file_path":"/tmp/p1_readonly.log"}}}"#,
        )
        .expect("decision");
        let record = build_execution_record(
            "Read",
            &ToolResult::Interrupted("failed to read missing path".into()),
            None,
        );
        let outcome = classify_tool_outcome(
            &frame,
            &decision,
            &record,
            "failed to read /tmp/p1_readonly.log: No such file or directory (os error 2)",
            1,
        );
        assert_eq!(outcome.kind.as_str(), "missing_path");
        assert!(!outcome.recoverable);
        assert_eq!(
            outcome.recommended_next_action.as_deref(),
            Some("context_unavailable")
        );
    }

    #[test]
    fn request_context_unsupported_selector_does_not_trigger_fuzzy_widening() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["operator_action:write_artifact"]}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            request_json.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                make_clean_frame(),
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::RepairExhausted { usage, .. } => {
                assert_eq!(usage.fallback_count, 0);
                assert_eq!(usage.hydration_miss_unsupported_count, 1);
            }
            other => panic!("expected RepairExhausted, got {other:?}"),
        }
    }

    #[test]
    fn request_context_no_progress_can_inject_missing_path_recovery_gate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let target = "/tmp/recovery-gate-report.md";
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["symbol:MissingSymbol"]}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let mut frame = make_clean_frame();
        push_completion_contract_with_refs(&mut frame, &["artifact:missing"], &[], &[]);
        frame.recent_evidence.push(format!(
            "fact: artifact_status ref=artifact:missing path={target} kind=file status=missing_or_invalid source=artifact_expectation source_event_id=artifact-expectation:1 freshness=current confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=target file missing"
        ));
        frame.recent_evidence.push(format!(
            "fact: permission_to_create_and_write:{target} ref=permission:recover source=permission_scope source_event_id=permission-scope:recover freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=write target"
        ));
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::RepairExhausted { usage, .. } => {
                assert_eq!(usage.recovery_tier.as_deref(), Some("artifact_repair_turn"));
                assert_eq!(
                    usage.recovery_outcome.as_deref(),
                    Some("repair_turn_injected")
                );
                let report = usage.worker_report.expect("worker report");
                assert!(
                    report
                        .remaining_risks
                        .iter()
                        .any(|item| item.contains("repair_turn:artifact_missing"))
                );
            }
            other => panic!("expected RepairExhausted, got {other:?}"),
        }
    }

    #[test]
    fn verification_failure_enters_repair_instead_of_terminal_fail() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["symbol:MissingSymbol"]}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(request_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let mut frame = make_clean_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:verify path=/tmp/report.md source=tool:Write source_event_id=tool-write:verify freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig::default(),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::RepairExhausted { usage, .. } => {
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("missing_verification_evidence")
                );
                assert_eq!(
                    usage.recovery_tier.as_deref(),
                    Some("verification_repair_continuation")
                );
                assert_eq!(
                    usage.recovery_outcome.as_deref(),
                    Some("repair_turn_injected")
                );
            }
            other => panic!("expected RepairExhausted, got {other:?}"),
        }
    }

    #[test]
    fn tool_outcome_schema_invalid_returns_canonical_shape() {
        let frame = make_frame();
        let decision = validate_state_decision(
            r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Edit","args":{"file_path":"src/lib.rs","new_string":"patched"}}}"#,
        )
        .expect("decision");
        let record = build_execution_record(
            "Edit",
            &ToolResult::Interrupted("invalid input for Edit: old_string cannot be empty".into()),
            None,
        );
        let outcome = classify_tool_outcome(
            &frame,
            &decision,
            &record,
            "invalid input for Edit: old_string cannot be empty",
            1,
        );
        assert_eq!(outcome.kind.as_str(), "schema_invalid");
        assert_eq!(
            outcome.recommended_next_action.as_deref(),
            Some("use_canonical_args:Edit.file_path/old_string/new_string")
        );
    }

    #[test]
    fn tool_outcome_old_string_not_found_requires_read_before_edit() {
        let mut frame = make_frame();
        let path = std::env::temp_dir().join("p1_edit_drift.rs");
        frame.recent_evidence.push(format!(
            "fact: permission_to_create_and_write:{} ref=permission:step0:0 source=permission_scope source_event_id=permission-scope:0:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=worker may create and write the declared target artifact path {}",
            path.display(),
            path.display()
        ));
        let decision = validate_state_decision(&format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Edit","args":{{"file_path":"{}","old_string":"alpha","new_string":"omega"}}}}}}"#,
            path.display()
        ))
        .expect("decision");
        let record = build_execution_record(
            "Edit",
            &ToolResult::Interrupted("old_string not found".into()),
            None,
        );
        let outcome = classify_tool_outcome(
            &frame,
            &decision,
            &record,
            "old_string not found in target file",
            1,
        );
        assert_eq!(outcome.kind.as_str(), "user_error");
        assert!(outcome.recoverable);
        assert_eq!(
            outcome.recommended_next_action.as_deref(),
            Some("read_before_edit")
        );
    }

    #[test]
    fn tool_outcome_large_output_is_bounded_and_tracked_by_ref() {
        let mut frame = make_frame();
        let decision = validate_state_decision(
            r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Read","args":{"file_path":"/tmp/large.log"}}}"#,
        )
        .expect("decision");
        let record =
            build_execution_record("Read", &ToolResult::ResultTooLarge("x".repeat(1000)), None);
        let outcome = classify_tool_outcome(&frame, &decision, &record, "too large", 1);
        let changed = push_tool_outcome_evidence(&mut frame, &record, &outcome, 1, "tool-read:1");
        assert!(changed);
        assert_eq!(outcome.kind.as_str(), "result_too_large");
        assert!(outcome.truncated);
        assert!(
            outcome
                .evidence_ref
                .as_deref()
                .unwrap_or_default()
                .contains("tool_feedback")
        );
        assert!(
            frame.recent_evidence.iter().any(
                |line| line.contains("tool_outcome:") && line.contains("kind=result_too_large")
            )
        );
    }

    #[test]
    fn completion_evidence_evaluator_flags_missing_artifact_evidence() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.objective = "write output file and run tests".into();
        push_completion_contract(&mut frame, true, true, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: test_failures ref=test:2 name=cargo_test status=passed source=tool:Bash source_event_id=tool-bash:2 freshness=after-runtime-test confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=cargo test passed".into(),
        );
        let usage = LoopUsage::default();
        let status = evaluate_completion_evidence(&frame, &usage);
        assert_eq!(status.as_str(), "missing_artifact_evidence");
    }

    #[test]
    fn worker_done_report_includes_artifact_and_test_status() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        frame.objective = "write report artifact and run tests".into();
        push_completion_contract(&mut frame, true, true, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step1:runtime:0 path=/tmp/report.md kind=file status=verified source=tool:ArtifactVerify source_event_id=tool-artifact:1:0 freshness=after-runtime-artifact-verify confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact verification passed for /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:step1:0 path=/tmp/report.md source=worker_result source_event_id=worker-result:1 freshness=after-worker-output confidence=0.90 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: test_failures ref=test:step1:worker name=cargo_test status=passed source=worker_result source_event_id=worker-result:1 freshness=after-worker-output confidence=0.85 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=cargo test passed".into(),
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
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
                let report = usage.worker_report.expect("worker report");
                assert_eq!(report.worker_state, AgentState::Done);
                assert_eq!(report.artifact_status, "verified");
                assert_eq!(report.test_status, "passed");
                assert_eq!(report.verification_status, "verified");
                assert!(
                    report
                        .files_changed
                        .iter()
                        .any(|path| path == "/tmp/report.md")
                );
                assert!(
                    report
                        .tests_run
                        .iter()
                        .any(|item| item == "cargo_test:passed")
                );
                assert_eq!(report.completion_evidence_status.as_str(), "sufficient");
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn worker_report_preserves_evidence_refs_after_tool_success() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_path = unique_temp_path("worker_report_refs");
        std::fs::write(&temp_path, "alpha\n").expect("temp file should be written");
        let mut frame = make_frame();
        push_completion_contract(&mut frame, false, false, false);
        let read_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            temp_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(read_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(FileReadTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig::default(),
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let _ = std::fs::remove_file(&temp_path);
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert!(
                    report
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == "tool_output:1")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn worker_report_collects_target_scoped_anchor_from_short_verify_output() {
        let mut frame = make_frame();
        frame.accepted_summary = vec![
            "verified_target: /tmp/report.md".into(),
            "verification_result: verified".into(),
            "minimal_evidence: Read succeeded".into(),
            "remaining_blocker: none".into(),
        ];
        push_completion_contract(&mut frame, false, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");

        let usage = LoopUsage {
            last_effective_tool_action: Some("ArtifactVerify".into()),
            tool_execution_records: vec![crate::tool::result::ToolExecutionRecord {
                tool_name: "ArtifactVerify".into(),
                outcome: "Text".into(),
                kind: crate::tool::result::ToolExecutionOutcomeKind::Success,
                summary: "read-back verified /tmp/report.md".into(),
                detail: Some("verified_target: /tmp/report.md".into()),
                pending_approval: None,
                report_modifier: crate::tool::result::ToolReportModifier::None,
                observable_input: None,
                batch_context: crate::tool::result::ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
            ..LoopUsage::default()
        };

        let report = super::build_worker_structured_report(
            &frame,
            &usage,
            CompletionEvidenceStatus::MissingVerificationEvidence,
        );

        assert!(
            report
                .evidence_refs
                .iter()
                .any(|reference| reference.contains("/tmp/report.md"))
        );
        assert!(
            report
                .evidence_refs
                .iter()
                .any(|reference| reference.starts_with("verification:"))
        );
        assert_eq!(
            report.completion_evidence_status,
            CompletionEvidenceStatus::Sufficient
        );
    }

    #[test]
    fn verification_completion_status_becomes_sufficient_when_target_anchor_is_present() {
        let mut frame = make_frame();
        push_completion_contract(&mut frame, false, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/report.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        frame.accepted_summary = vec![
            "verified_target: /tmp/report.md".into(),
            "verification_result: verified".into(),
            "minimal_evidence: Read succeeded".into(),
            "remaining_blocker: none".into(),
        ];

        let usage = LoopUsage {
            last_effective_tool_action: Some("ArtifactVerify".into()),
            tool_execution_records: vec![crate::tool::result::ToolExecutionRecord {
                tool_name: "ArtifactVerify".into(),
                outcome: "Text".into(),
                kind: crate::tool::result::ToolExecutionOutcomeKind::Success,
                summary: "artifact verification passed for /tmp/report.md".into(),
                detail: Some("summary=verified_target: /tmp/report.md".into()),
                pending_approval: None,
                report_modifier: crate::tool::result::ToolReportModifier::None,
                observable_input: None,
                batch_context: crate::tool::result::ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
            ..LoopUsage::default()
        };

        assert_eq!(
            evaluate_completion_evidence(&frame, &usage),
            CompletionEvidenceStatus::Sufficient
        );
    }

    #[test]
    fn done_is_blocked_when_artifact_evidence_missing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract(&mut frame, true, false, false);
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:0",
            "/tmp/missing.txt",
            "file",
        );
        frame.recent_evidence.push(
            "fact: permission_to_create_and_write:/tmp/missing.txt ref=permission:1 source=permission_scope source_event_id=permission:1 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=write target".into(),
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            done_json.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 1,
                    ..DecisionLoopConfig::default()
                },
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::MaxIterationsReached { last_state, usage } => {
                assert_eq!(last_state, AgentState::Executing);
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("missing_artifact_evidence")
                );
                let report = usage.worker_report.expect("worker report");
                assert!(
                    report
                        .remaining_risks
                        .iter()
                        .any(|item| item.contains("required_action:write_artifact"))
                );
            }
            other => panic!("expected MaxIterationsReached, got {other:?}"),
        }
    }

    #[test]
    fn implicit_done_after_continue_is_blocked_when_verification_missing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:1 path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{"open_items_remove":["tests pass"]}}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            continue_json.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 1,
                    ..DecisionLoopConfig::default()
                },
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::MaxIterationsReached { last_state, usage } => {
                assert_eq!(last_state, AgentState::Verifying);
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("missing_verification_evidence")
                );
                let report = usage.worker_report.expect("worker report");
                assert!(
                    report
                        .remaining_risks
                        .iter()
                        .any(|item| item.contains("required_action:verify_artifact"))
                );
            }
            other => panic!("expected MaxIterationsReached, got {other:?}"),
        }
    }

    #[test]
    fn verification_repair_continuation_reverifies_before_documentation_timeout() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step1:runtime:0 path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{"open_items_remove":["tests pass"]}}"#;
        let verify_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"ArtifactVerify","args":{"path":"/tmp/report.md","kind":"file","status":"verified"}}}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(continue_json.into())],
            vec![StreamEvent::TextDelta(verify_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 3,
                    ..DecisionLoopConfig::default()
                },
                Some(verification_repair_tool_runtime()),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(report.verification_status, "verified");
                assert_eq!(report.completion_evidence_status.as_str(), "sufficient");
                assert_eq!(report.artifact_status, "verified");
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn repair_to_reverify_short_loop_completes_or_hard_fails() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step1:runtime:0 path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{"open_items_remove":["tests pass"]}}"#;
        let verify_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"ArtifactVerify","args":{"path":"/tmp/report.md","kind":"file","status":"verified"}}}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(continue_json.into())],
            vec![StreamEvent::TextDelta(verify_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 3,
                    ..DecisionLoopConfig::default()
                },
                Some(verification_repair_tool_runtime()),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("sufficient")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn verification_only_gap_does_not_expand_into_long_documentation_tail() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step1:runtime:0 path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{"open_items_remove":["tests pass"]}}"#;
        let verify_json = r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"ArtifactVerify","args":{"path":"/tmp/report.md","kind":"file","status":"verified"}}}"#;
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(continue_json.into())],
            vec![StreamEvent::TextDelta(verify_json.into())],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 3,
                    ..DecisionLoopConfig::default()
                },
                Some(verification_repair_tool_runtime()),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert!(
                    !report
                        .remaining_risks
                        .iter()
                        .any(|item| item.contains("documentation timeout"))
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn prose_evidence_files_read_block_records_claims_without_closing_targets() {
        let mut frame = make_clean_frame();
        frame.stage_execution_contract.declared_artifacts.push(
            crate::core::state_frame::DeclaredArtifactContract {
                ref_id: "artifact:report".into(),
                path: "/tmp/report.md".into(),
                kind: "file".into(),
                required_actions: vec!["write".into()],
                required_evidence: Vec::new(),
            },
        );
        frame.stage_execution_contract.content_evidence_targets = vec![
            "RustAgent/Agent/src/tool/builtin/glob.rs".into(),
            "RustAgent/Agent/src/tool/builtin/grep.rs".into(),
            "RustAgent/docs/31-token-efficiency-cost-performance.md".into(),
        ];
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:report path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=report written"
                .into(),
        );
        frame.recent_evidence.push(
            "Evidence files read:\n- RustAgent/Agent/src/tool/builtin/glob.rs\n- RustAgent/Agent/src/tool/builtin/grep.rs\n- /Users/example/repo/RustAgent/docs/31-token-efficiency-cost-performance.md"
                .into(),
        );

        let refs = super::collect_evidence_refs(&frame, None);
        assert!(
            refs.contains(&"claimed_read:RustAgent/Agent/src/tool/builtin/glob.rs".to_string())
        );
        assert!(
            refs.contains(&"claimed_read:RustAgent/Agent/src/tool/builtin/grep.rs".to_string())
        );
        assert!(refs.contains(
            &"claimed_read:RustAgent/docs/31-token-efficiency-cost-performance.md".to_string()
        ));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::MissingVerificationEvidence
        );
    }

    #[test]
    fn verification_loop_exits_when_target_scoped_verification_evidence_is_sufficient() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:created path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let verify_json = r#"{"state":"verifying","decision":"call_tool","next_action":{"action_type":"ArtifactVerify","args":{"path":"/tmp/report.md","kind":"file","status":"verified"}}}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            verify_json.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 2,
                    ..DecisionLoopConfig::default()
                },
                Some(verification_repair_tool_runtime()),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("sufficient")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn verification_loop_hard_fails_without_spinning_when_gap_is_not_repairable() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:created path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let verify_json = r#"{"state":"verifying","decision":"call_tool","next_action":{"action_type":"ArtifactVerify","args":{"path":"/tmp/other.md","kind":"file","status":"verified"}}}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            verify_json.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 3,
                    ..DecisionLoopConfig::default()
                },
                Some(verification_repair_tool_runtime()),
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::ToolDispatchFailed { reason, usage, .. } => {
                assert!(reason.contains("verification evidence still missing"));
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("missing_verification_evidence")
                );
            }
            other => panic!("expected ToolDispatchFailed, got {other:?}"),
        }
    }

    #[test]
    fn completion_gate_injects_required_action_into_stateframe() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:needs-verify path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        let mut usage = LoopUsage::default();
        let block =
            super::enforce_completion_gate(&mut frame, &mut usage).expect_err("gate should block");
        super::inject_completion_gate_block(&mut frame, &block);
        assert_eq!(block.required_action, "verify_artifact");
        assert!(
            frame
                .open_items
                .iter()
                .any(|item| item.contains("required_action:verify_artifact"))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.contains("completion_gate:")
                    && line.contains("required_action=verify_artifact"))
        );
    }

    #[test]
    fn completion_gate_injects_required_read_targets_for_source_gap() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.stage_execution_contract.content_evidence_targets = vec!["/tmp/source.md".into()];
        push_completion_contract(&mut frame, false, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:contract:0 path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );

        let mut usage = LoopUsage::default();
        let block =
            super::enforce_completion_gate(&mut frame, &mut usage).expect_err("gate should block");
        assert_eq!(block.required_action, "read_source_evidence");

        super::inject_completion_gate_block(&mut frame, &block);

        assert!(
            frame
                .open_items
                .iter()
                .any(|item| item.contains("completion_gate_repair:")
                    && item.contains("required_read_targets=/tmp/source.md")
                    && item.contains("required_evidence_refs=read:/tmp/source.md")
                    && item.contains(
                        "forbidden_evidence=Bash|Glob|cat|sed|ls|self_claims|report_prose"
                    ))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.contains("completion_gate:")
                    && line.contains("required_action=read_source_evidence")
                    && line.contains("required_read_targets=/tmp/source.md")
                    && line.contains("required_evidence_refs=read:/tmp/source.md"))
        );
    }

    #[test]
    fn completion_gate_expands_directory_verification_gap_into_child_file_reads() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:dir", "artifact:contract:file"],
            &[],
            &["artifact:contract:dir", "artifact:contract:file"],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            "/tmp/example-site",
            "directory",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:file",
            "/tmp/example-site/README.md",
            "file",
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:readme path=/tmp/example-site/README.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated README".into(),
        );

        let mut usage = LoopUsage::default();
        let block =
            super::enforce_completion_gate(&mut frame, &mut usage).expect_err("gate should block");
        assert_eq!(block.required_action, "verify_artifact");

        super::inject_completion_gate_block(&mut frame, &block);

        let repair_line = frame
            .recent_evidence
            .iter()
            .find(|line| line.contains("completion_gate:"))
            .expect("completion gate evidence");
        assert!(repair_line.contains("required_read_targets=/tmp/example-site/README.md"));
        assert!(!repair_line.contains("required_read_targets=/tmp/example-site|"));
        assert!(repair_line.contains("required_evidence_refs=read:/tmp/example-site/README.md"));
        assert!(
            repair_line
                .contains("forbidden_evidence=Bash|Glob|cat|sed|ls|self_claims|report_prose")
        );
    }

    #[test]
    fn artifact_gate_block_triggers_repair_turn_with_exact_target_path() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(&mut frame, &["artifact:missing"], &[], &[]);
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:missing path=/tmp/report.md kind=file status=missing_or_invalid source=artifact_expectation source_event_id=artifact-expectation:1 freshness=current confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=target file missing".into(),
        );
        frame.recent_evidence.push(
            "fact: permission_to_create_and_write:/tmp/report.md ref=permission:step1:0 source=permission_scope source_event_id=permission-scope:1 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=worker may create and write /tmp/report.md".into(),
        );
        let mut usage = LoopUsage::default();
        let block =
            super::enforce_completion_gate(&mut frame, &mut usage).expect_err("gate should block");
        super::inject_completion_gate_block(&mut frame, &block);
        super::record_completion_gate_recovery(&frame, &mut usage, &block);
        let repair_line = frame
            .recent_evidence
            .iter()
            .find(|line| line.starts_with("fact: repair_turn "))
            .expect("repair turn evidence");
        assert!(repair_line.contains("target_path=/tmp/report.md"));
        assert!(repair_line.contains("parent_dir=/tmp"));
        assert!(repair_line.contains("permission_ref=permission:step1:0"));
        assert!(
            repair_line.contains(
                "recommended_write_strategy=create_parent_directory_and_write_target_file"
            )
        );
        assert!(repair_line.contains("create_parent_directory=true"));
        assert!(repair_line.contains("write_target_file=true"));
        assert_eq!(usage.recovery_tier.as_deref(), Some("artifact_repair_turn"));
        assert_eq!(
            usage.recovery_outcome.as_deref(),
            Some("repair_turn_injected")
        );
    }

    #[test]
    fn completion_evidence_requires_every_declared_artifact_ref() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0", "artifact:contract:1"],
            &[],
            &[],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/one.md", "file");
        push_artifact_target_fact(&mut frame, "artifact:contract:1", "/tmp/two.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/one.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/one.md".into(),
        );

        let status = evaluate_completion_evidence(&frame, &LoopUsage::default());
        assert_eq!(status, CompletionEvidenceStatus::MissingArtifactEvidence);
        let missing_refs = super::missing_artifact_evidence_refs(&frame);
        assert_eq!(missing_refs, vec!["artifact:contract:1".to_string()]);
        let gaps = super::collect_completion_evidence_gaps(&frame);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].target_ref, "artifact:contract:1");
        assert_eq!(gaps[0].target_path.as_deref(), Some("/tmp/two.md"));
        assert!(gaps[0].missing_artifact_evidence);
        assert_eq!(gaps[0].recommended_action, "write_artifact");
    }

    #[test]
    fn review_verdict_does_not_satisfy_verification_evidence() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/report.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: review_verdicts ref=review:1 verdict=accepted source=tool:BossReview source_event_id=tool-review:1 freshness=after-runtime-review confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=LGTM".into(),
        );

        let status = evaluate_completion_evidence(&frame, &LoopUsage::default());
        assert_eq!(
            status,
            CompletionEvidenceStatus::MissingVerificationEvidence
        );
    }

    #[test]
    fn content_source_gap_requires_runtime_read_anchor_not_output_readback() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.stage_execution_contract.content_evidence_targets = vec!["/tmp/source.md".into()];
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/report.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/report.md".into(),
        );
        frame
            .accepted_summary
            .push("worker claims read:/tmp/source.md in report prose".into());

        let usage = LoopUsage::default();
        let status = evaluate_completion_evidence(&frame, &usage);
        assert_eq!(
            status,
            CompletionEvidenceStatus::MissingVerificationEvidence
        );
        let gaps = super::collect_completion_evidence_gaps(&frame);
        let source_gap = gaps
            .iter()
            .find(|gap| gap.recommended_action == "read_source_evidence")
            .expect("source evidence gap");
        assert_eq!(source_gap.target_path.as_deref(), Some("/tmp/source.md"));
        assert!(source_gap.missing_verification_evidence);
    }

    #[test]
    fn content_source_gap_ignores_report_read_detail_source_mentions() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.stage_execution_contract.content_evidence_targets = vec![
            "RustAgent/Agent/src/tool/definition.rs".into(),
            "RustAgent/Agent/src/tool/registry.rs".into(),
        ];
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:0",
            "/tmp/u8-report.md",
            "file",
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/u8-report.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/u8-report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/u8-report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/u8-report.md".into(),
        );

        let usage = LoopUsage {
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: Some(
                    "# report\nEvidence files read:\n- RustAgent/Agent/src/tool/definition.rs\n- RustAgent/Agent/src/tool/registry.rs"
                        .into(),
                ),
                pending_approval: None,
                report_modifier: crate::tool::result::ToolReportModifier::None,
                observable_input: Some(ObservableInput {
                    value: r#"{"path":"/tmp/u8-report.md"}"#.into(),
                    source: ObservableInputSource::Raw,
                }),
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
            ..LoopUsage::default()
        };

        let refs = super::collect_evidence_refs(&frame, Some(&usage));
        assert!(refs.contains(&"read:/tmp/u8-report.md".to_string()));
        assert!(refs.contains(&"claimed_read:RustAgent/Agent/src/tool/definition.rs".to_string()));
        assert!(refs.contains(&"claimed_read:RustAgent/Agent/src/tool/registry.rs".to_string()));
        assert!(!refs.contains(&"read:RustAgent/Agent/src/tool/definition.rs".to_string()));
        assert!(!refs.contains(&"read:RustAgent/Agent/src/tool/registry.rs".to_string()));
        assert_eq!(
            evaluate_completion_evidence(&frame, &usage),
            CompletionEvidenceStatus::MissingVerificationEvidence
        );
        let gaps = super::collect_completion_evidence_gaps(&frame);
        assert!(gaps.iter().any(|gap| {
            gap.recommended_action == "read_source_evidence"
                && gap.target_path.as_deref() == Some("RustAgent/Agent/src/tool/definition.rs")
                && gap.missing_verification_evidence
        }));
        assert!(gaps.iter().any(|gap| {
            gap.recommended_action == "read_source_evidence"
                && gap.target_path.as_deref() == Some("RustAgent/Agent/src/tool/registry.rs")
                && gap.missing_verification_evidence
        }));
    }

    #[test]
    fn content_source_gap_clears_after_required_source_read() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.stage_execution_contract.content_evidence_targets = vec!["/tmp/source.md".into()];
        push_completion_contract(&mut frame, true, false, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/report.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:1:read path=/tmp/source.md kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/source.md".into(),
        );

        let usage = LoopUsage::default();
        assert_eq!(
            evaluate_completion_evidence(&frame, &usage),
            CompletionEvidenceStatus::Sufficient
        );
        assert!(super::collect_completion_evidence_gaps(&frame).is_empty());
    }

    #[test]
    fn artifact_repair_turn_uses_missing_artifact_ref_not_first_permission_fact() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0", "artifact:contract:1"],
            &[],
            &[],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:0",
            "/tmp/first-output.md",
            "file",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:1",
            "/tmp/second-output.md",
            "file",
        );
        frame.recent_evidence.push(
            "fact: permission_to_create_and_write:/tmp/first-output.md ref=permission:first source=permission_scope source_event_id=permission:1 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=write first".into(),
        );
        frame.recent_evidence.push(
            "fact: permission_to_create_and_write:/tmp/second-output.md ref=permission:second source=permission_scope source_event_id=permission:2 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=write second".into(),
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:first path=/tmp/first-output.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated first".into(),
        );
        let mut usage = LoopUsage::default();
        let block =
            super::enforce_completion_gate(&mut frame, &mut usage).expect_err("gate should block");
        assert_eq!(
            block.missing_evidence_refs,
            vec!["artifact:contract:1".to_string()]
        );
        super::inject_completion_gate_block(&mut frame, &block);
        let repair_line = frame
            .recent_evidence
            .iter()
            .find(|line| line.starts_with("fact: repair_turn "))
            .expect("repair turn evidence");
        assert!(repair_line.contains("target_path=/tmp/second-output.md"));
        assert!(repair_line.contains("permission_ref=permission:second"));
    }

    #[test]
    fn completion_evidence_gaps_clear_after_later_verification() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:1 path=/tmp/report.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated /tmp/report.md".into(),
        );
        let initial_gaps = super::collect_completion_evidence_gaps(&frame);
        assert_eq!(initial_gaps.len(), 1);
        assert!(initial_gaps[0].missing_verification_evidence);

        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:verified path=/tmp/report.md kind=file status=verified source=tool:ArtifactVerify source_event_id=tool-artifact:1 freshness=after-runtime-artifact-verify confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact verification passed for /tmp/report.md".into(),
        );
        let cleared_gaps = super::collect_completion_evidence_gaps(&frame);
        assert!(cleared_gaps.is_empty());
    }

    #[test]
    fn u7_directory_and_readme_write_is_recognized_as_material_artifact_evidence() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:dir", "artifact:contract:file"],
            &[],
            &[],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            "/tmp/example-site",
            "directory",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:file",
            "/tmp/example-site/README.md",
            "file",
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:dir path=/tmp/example-site/README.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated README".into(),
        );
        assert!(super::artifact_path_has_material_evidence(
            &frame,
            "/tmp/example-site",
            "directory"
        ));
        assert!(super::artifact_path_has_material_evidence(
            &frame,
            "/tmp/example-site/README.md",
            "file"
        ));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::Sufficient
        );
    }

    #[test]
    fn read_back_or_verify_success_counts_as_verification_evidence_for_target() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:created path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/report.md".into(),
        );
        assert!(super::has_verified_artifact_for_path(
            &frame,
            "/tmp/report.md"
        ));
        assert!(super::has_explicit_verification_fact(
            &frame,
            "artifact:contract:0"
        ));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::Sufficient
        );
    }

    #[test]
    fn repair_continuation_local_reverify_closes_gap_without_full_worker_dispatch() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:created path=/tmp/report.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created".into(),
        );
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::MissingVerificationEvidence
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:2 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=local re-verify succeeded".into(),
        );
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::Sufficient
        );
        assert!(super::collect_completion_evidence_gaps(&frame).is_empty());
    }

    #[test]
    fn collect_evidence_refs_adds_read_anchor_for_target_scoped_read() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");

        let usage = LoopUsage {
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: crate::tool::result::ToolReportModifier::None,
                observable_input: Some(ObservableInput {
                    value: r#"{"file_path":"/tmp/report.md"}"#.into(),
                    source: ObservableInputSource::Raw,
                }),
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
            ..LoopUsage::default()
        };

        let evidence_refs = super::collect_evidence_refs(&frame, Some(&usage));
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == "read:/tmp/report.md")
        );
        assert!(super::worker_has_target_scoped_read_anchor(
            &frame,
            &evidence_refs
        ));
    }

    #[test]
    fn collect_evidence_refs_recovers_read_anchor_from_recent_evidence_lines() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:1:read path=/tmp/report.md kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "hydrated_context: file_snippet:/tmp/report.md source=tool:Read match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:1:read source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read excerpt=# report".into(),
        );

        let evidence_refs = super::collect_evidence_refs(&frame, None);
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == "read:/tmp/report.md")
        );
        assert!(super::worker_has_target_scoped_read_anchor(
            &frame,
            &evidence_refs
        ));
    }

    #[test]
    fn collect_evidence_refs_normalizes_absolute_file_snippet_reads_to_target_scope() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        let target = "RustAgent/Agent/src/core/state_frame_projection.rs";
        let absolute = "/Users/wangmorgan/MProject/LearnCCfromCC/RustAgent/Agent/src/core/state_frame_projection.rs";
        push_artifact_target_fact(&mut frame, "artifact:contract:0", target, "file");
        frame.recent_evidence.push(format!(
            "fact: file_facts ref=filefact:runtime:1:read path={absolute} kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for {absolute}"
        ));
        frame.recent_evidence.push(format!(
            "hydrated_context: file_snippet:{absolute} source=tool:Read match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:1:read source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read excerpt=# report"
        ));

        let evidence_refs = super::collect_evidence_refs(&frame, None);
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == &format!("read:{target}"))
        );
        assert!(super::worker_has_target_scoped_read_anchor(
            &frame,
            &evidence_refs
        ));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::Sufficient
        );
    }

    #[test]
    fn verification_terminal_outcome_closes_on_verified_status_plus_target_read_anchor() {
        let mut frame = make_frame();
        frame.state = AgentState::Verifying;
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:contract:0 path=/tmp/report.md kind=file status=verified source=tool:ArtifactVerify source_event_id=tool-artifact:1 freshness=after-runtime-artifact-verify confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact verification passed for /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/report.md".into(),
        );
        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            tool_execution_records: vec![ToolExecutionRecord {
                tool_name: "Read".into(),
                outcome: "Text".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Read succeeded".into(),
                detail: None,
                pending_approval: None,
                report_modifier: crate::tool::result::ToolReportModifier::None,
                observable_input: Some(ObservableInput {
                    value: r#"{"file_path":"/tmp/report.md"}"#.into(),
                    source: ObservableInputSource::Raw,
                }),
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            }],
            ..LoopUsage::default()
        };

        let outcome = super::verification_terminal_outcome(&frame, &mut usage)
            .expect("verified target-scoped read should close verification");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert_eq!(report.verification_status, "verified");
                assert!(
                    report
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == "read:/tmp/report.md")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn verification_terminal_outcome_closes_after_successful_target_read_even_if_state_stays_executing()
     {
        let mut frame = make_frame();
        frame.state = AgentState::Executing;
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.open_items.push(
            "required_action:verify_artifact reason=artifact verification failure requires repair continuation missing_refs=artifact:contract:0".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:1:read path=/tmp/report.md kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/report.md".into(),
        );

        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            ..LoopUsage::default()
        };

        let outcome = super::verification_terminal_outcome(&frame, &mut usage)
            .expect("successful target-scoped read should close verification even if state lags");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert_eq!(report.verification_status, "verified");
                assert!(
                    report
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == "read:/tmp/report.md")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn verification_terminal_outcome_closes_on_target_read_anchor_without_explicit_verification_fact()
     {
        let mut frame = make_frame();
        frame.state = AgentState::Executing;
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.open_items.push(
            "required_action:verify_artifact reason=completion gate blocked done because required verification evidence is missing missing_refs=artifact:contract:0".into(),
        );
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:1:read path=/tmp/report.md kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "hydrated_context: file_snippet:/tmp/report.md source=tool:Read match_reason=call_tool_read trace=fact_name=file_facts ref=filefact:runtime:1:read source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read excerpt=# report".into(),
        );

        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            ..LoopUsage::default()
        };

        let outcome = super::verification_terminal_outcome(&frame, &mut usage).expect(
            "target-scoped read anchor should close verification without extra verified fact",
        );
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert_eq!(report.verification_status, "verified");
                assert!(
                    report
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == "read:/tmp/report.md")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn verification_terminal_outcome_forces_done_after_repeated_target_read_with_closed_source_evidence()
     {
        let mut frame = make_frame();
        frame.state = AgentState::Verifying;
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:0"],
            &[],
            &["artifact:contract:0"],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:1:read path=/tmp/report.md kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/report.md".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:0 path=/tmp/report.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified /tmp/report.md".into(),
        );
        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: crate::tool::result::ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"file_path":"/tmp/report.md"}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded again".into(),
                    detail: None,
                    pending_approval: None,
                    report_modifier: crate::tool::result::ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"file_path":"/tmp/report.md"}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
            ],
            ..LoopUsage::default()
        };

        let outcome = super::verification_terminal_outcome(&frame, &mut usage)
            .expect("repeated verification read should force terminalization");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert_eq!(report.verification_status, "verified");
                assert!(
                    report
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == "read:/tmp/report.md")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn repeated_verification_target_read_without_source_evidence_is_redirected_to_source_repair() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let report_path = unique_temp_path("verification_tailspin_report");
        let source_path = unique_temp_path("verification_tailspin_source");
        std::fs::write(&report_path, "report contents").expect("report should be written");
        std::fs::write(&source_path, "source contents").expect("source should be written");

        let read_report_json = format!(
            r#"{{"state":"verifying","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
            report_path.display()
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(read_report_json.clone())],
            vec![StreamEvent::TextDelta(read_report_json)],
            vec![StreamEvent::TextDelta(done_json.into())],
        ]);

        let mut frame = make_frame();
        frame.state = AgentState::Verifying;
        frame.recent_evidence.clear();
        frame.allowed_actions.push("read_file".into());
        frame.allowed_tools.push("Read".into());
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:report",
            report_path.to_str().unwrap(),
            "file",
        );
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:report"],
            &[],
            &["artifact:contract:report"],
        );
        frame.stage_execution_contract.content_evidence_targets =
            vec![source_path.display().to_string()];

        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new().register(Arc::new(FileReadTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };

        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 4,
                    ..DecisionLoopConfig::default()
                },
                Some(tool_runtime),
            ))
            .expect("loop should not error");

        let _ = std::fs::remove_file(&report_path);
        let _ = std::fs::remove_file(&source_path);

        match outcome {
            LoopOutcome::ToolDispatchFailed {
                last_state,
                reason,
                usage,
            } => {
                assert_eq!(last_state, AgentState::Verifying);
                assert!(reason.contains("source evidence remains missing"));
                assert_eq!(
                    usage.recovery_tier.as_deref(),
                    Some("verification_repair_continuation")
                );
                assert_eq!(
                    usage.recovery_outcome.as_deref(),
                    Some("repair_turn_injected")
                );
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|status| status.as_str()),
                    Some("missing_verification_evidence")
                );
                assert_eq!(usage.tool_dispatch_count, 1);
                assert_eq!(usage.tool_dispatch_success_count, 1);
                assert!(
                    usage
                        .worker_report
                        .expect("worker report")
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == &format!("read:{}", report_path.display()))
                );
            }
            other => panic!("expected ToolDispatchFailed, got {other:?}"),
        }
    }

    #[test]
    fn verification_repair_loop_terminalizes_after_required_file_reads_before_extra_action() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let root = unique_temp_dir("required_file_read_loop");
        std::fs::create_dir_all(&root).expect("target dir should be created");
        let required_paths = [
            root.join("README.md"),
            root.join("runtime.py"),
            root.join("model.py"),
            root.join("demo.py"),
        ];
        for path in &required_paths {
            std::fs::write(path, format!("contents for {}", path.display()))
                .expect("target file should be written");
        }

        let done_json = r#"{"state":"done","decision":"done"}"#;
        let read_json = |path: &std::path::Path| {
            format!(
                r#"{{"state":"verifying","decision":"call_tool","next_action":{{"action_type":"Read","args":{{"file_path":"{}"}}}}}}"#,
                path.display()
            )
        };
        let forbidden_bash_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Bash","args":{{"command":"python3 {}"}}}}}}"#,
            required_paths[3].display()
        );
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(done_json.into())],
            vec![StreamEvent::TextDelta(read_json(&required_paths[0]))],
            vec![StreamEvent::TextDelta(read_json(&required_paths[1]))],
            vec![StreamEvent::TextDelta(read_json(&required_paths[2]))],
            vec![StreamEvent::TextDelta(read_json(&required_paths[3]))],
            vec![StreamEvent::TextDelta(forbidden_bash_json)],
        ]);

        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.state = AgentState::Executing;
        frame.allowed_actions = vec!["read_file".into(), "run_command".into()];
        frame.allowed_tools = vec!["Read".into(), "Bash".into()];
        push_completion_contract_with_refs(
            &mut frame,
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
            &[],
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            root.to_str().unwrap(),
            "directory",
        );
        for (idx, (ref_id, path)) in [
            ("artifact:contract:readme", &required_paths[0]),
            ("artifact:contract:runtime", &required_paths[1]),
            ("artifact:contract:model", &required_paths[2]),
            ("artifact:contract:demo", &required_paths[3]),
        ]
        .into_iter()
        .enumerate()
        {
            push_artifact_target_fact(&mut frame, ref_id, path.to_str().unwrap(), "file");
            frame.recent_evidence.push(format!(
                "fact: recent_changes_in_files ref=change:runtime:{idx} path={} source=tool:Bash source_event_id=tool-bash:runtime:{idx} freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=runtime write created declared artifact",
                path.display()
            ));
        }
        let tool_runtime = StateFrameToolRuntime {
            registry: ToolRegistry::new()
                .register(Arc::new(FileReadTool))
                .register(Arc::new(BashTool)),
            permissions: ToolPermissionContext::new(PermissionMode::Default),
            cwd: root.clone(),
            config_root: None,
        };

        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 8,
                    ..DecisionLoopConfig::default()
                },
                Some(tool_runtime),
            ))
            .expect("loop should not error");

        let _ = std::fs::remove_dir_all(&root);

        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(usage.tool_dispatch_count, 4);
                assert!(
                    usage
                        .tool_execution_records
                        .iter()
                        .all(|record| record.tool_name == "Read"),
                    "loop should terminalize before consuming the scripted Bash turn: {:?}",
                    usage.tool_execution_records
                );
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert!(report.completion_evidence_gaps.is_empty());
            }
            other => panic!("expected Done after required reads, got {other:?}"),
        }
    }

    #[test]
    fn u7_directory_and_readme_verification_gap_closes_after_reverify() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:dir", "artifact:contract:file"],
            &[],
            &["artifact:contract:dir", "artifact:contract:file"],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            "/tmp/example-site",
            "directory",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:file",
            "/tmp/example-site/README.md",
            "file",
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:dir path=/tmp/example-site/README.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated README".into(),
        );
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::MissingVerificationEvidence
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:file target_ref=artifact:contract:file path=/tmp/example-site/README.md status=verified source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=read-back verified README".into(),
        );
        frame.recent_evidence.push(
            "fact: verification_status ref=artifact:contract:dir target_ref=artifact:contract:dir path=/tmp/example-site/README.md status=verified source=tool:Read source_event_id=tool-read:2 freshness=after-runtime-read confidence=0.90 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=directory target verified via README".into(),
        );
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::Sufficient
        );
        assert!(super::collect_completion_evidence_gaps(&frame).is_empty());
    }

    #[test]
    fn directory_verification_closes_after_declared_child_file_reads() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.state = AgentState::Verifying;
        push_completion_contract_with_refs(
            &mut frame,
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
            ],
            &[],
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
            ],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            "/tmp/example-site",
            "directory",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:readme",
            "/tmp/example-site/README.md",
            "file",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:runtime",
            "/tmp/example-site/runtime.py",
            "file",
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:readme path=/tmp/example-site/README.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated README".into(),
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:runtime path=/tmp/example-site/runtime.py source=tool:Write source_event_id=tool-write:2 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated runtime".into(),
        );
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:1:read path=/tmp/example-site/README.md kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/example-site/README.md".into(),
        );
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:runtime:2:read path=/tmp/example-site/runtime.py kind=read_observation source=tool:Read source_event_id=tool-read:runtime:2 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/example-site/runtime.py".into(),
        );

        let refs = super::collect_evidence_refs(&frame, None);
        assert!(super::worker_has_target_scoped_read_anchor(&frame, &refs));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::Sufficient
        );

        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            ..LoopUsage::default()
        };
        match super::verification_terminal_outcome(&frame, &mut usage)
            .expect("directory child reads should close verification")
        {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn completion_gate_required_read_refs_close_only_after_runtime_read_observations() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.state = AgentState::Verifying;
        push_completion_contract_with_refs(
            &mut frame,
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
            &[],
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
        );
        let root = "/tmp/example-required-read-close";
        let required_paths = [
            format!("{root}/README.md"),
            format!("{root}/runtime.py"),
            format!("{root}/model.py"),
            format!("{root}/demo.py"),
        ];
        push_artifact_target_fact(&mut frame, "artifact:contract:dir", root, "directory");
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:readme",
            &required_paths[0],
            "file",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:runtime",
            &required_paths[1],
            "file",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:model",
            &required_paths[2],
            "file",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:demo",
            &required_paths[3],
            "file",
        );
        for (idx, path) in required_paths.iter().enumerate() {
            frame.recent_evidence.push(format!(
                "fact: recent_changes_in_files ref=change:runtime:{idx} path={path} source=tool:Bash source_event_id=tool-bash:runtime:{idx} freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=runtime write created declared artifact"
            ));
            frame.recent_evidence.push(format!(
                "fact: artifact_status ref=artifact:runtime:{idx} path={path} kind=file status=created source=tool:Bash source_event_id=tool-bash:runtime:{idx} freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact created"
            ));
        }
        frame.open_items.push(format!(
            "completion_gate_repair: failure_reason=missing_verification_reads modification_direction=read_required_artifact_targets required_read_targets={} required_verification_targets=none required_evidence_refs={} forbidden_evidence=Bash|Glob|cat|sed|ls|self_claims|report_prose",
            required_paths.join("|"),
            required_paths
                .iter()
                .map(|path| format!("read:{path}"))
                .collect::<Vec<_>>()
                .join("|")
        ));

        let refs_before = super::collect_evidence_refs(&frame, None);
        assert!(
            required_paths.iter().all(|path| !refs_before
                .iter()
                .any(|reference| reference == &format!("read:{path}"))),
            "repair instruction text must not count as runtime evidence: {refs_before:?}"
        );
        assert!(!super::completion_gate_required_reads_closed(
            &frame,
            &LoopUsage::default()
        ));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &LoopUsage::default()),
            CompletionEvidenceStatus::MissingVerificationEvidence
        );

        for (idx, path) in required_paths.iter().enumerate() {
            frame.recent_evidence.push(format!(
                "fact: file_facts ref=filefact:runtime:{idx}:read path={path} kind=read_observation source=tool:Read source_event_id=tool-read:runtime:{idx} freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for {path}"
            ));
        }

        let refs_after = super::collect_evidence_refs(&frame, None);
        assert!(
            required_paths.iter().all(|path| refs_after
                .iter()
                .any(|reference| reference == &format!("read:{path}"))),
            "runtime Read observations should materialize required anchors: {refs_after:?}"
        );

        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            ..LoopUsage::default()
        };
        assert!(super::completion_gate_required_reads_closed(&frame, &usage));
        assert_eq!(
            super::evaluate_completion_evidence(&frame, &usage),
            CompletionEvidenceStatus::Sufficient
        );
        match super::completion_gate_repair_terminal_outcome(&frame, &mut usage)
            .expect("closed repair gate should terminalize without another model turn")
        {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert_eq!(report.verification_status, "verified");
                assert!(report.completion_evidence_gaps.is_empty());
            }
            other => panic!("expected Done, got {other:?}"),
        }
        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            ..LoopUsage::default()
        };
        match super::verification_terminal_outcome(&frame, &mut usage)
            .expect("closed required runtime reads should terminalize verification")
        {
            LoopOutcome::Done { usage, .. } => {
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert_eq!(report.verification_status, "verified");
                assert!(report.completion_evidence_gaps.is_empty());
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn stage_continuation_required_targets_close_from_runtime_reads_without_extra_tool() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let root = "/tmp/example-stage-continuation-close";
        let required_paths = [
            format!("{root}/README.md"),
            format!("{root}/runtime.py"),
            format!("{root}/model.py"),
            format!("{root}/demo.py"),
        ];
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.state = AgentState::Executing;
        frame.allowed_actions = vec!["run_command".into()];
        frame.allowed_tools = vec!["Bash".into()];
        push_completion_contract_with_refs(
            &mut frame,
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
            &[],
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:dir", root, "directory");
        for (idx, (ref_id, path)) in [
            ("artifact:contract:readme", &required_paths[0]),
            ("artifact:contract:runtime", &required_paths[1]),
            ("artifact:contract:model", &required_paths[2]),
            ("artifact:contract:demo", &required_paths[3]),
        ]
        .into_iter()
        .enumerate()
        {
            push_artifact_target_fact(&mut frame, ref_id, path, "file");
            frame.recent_evidence.push(format!(
                "fact: recent_changes_in_files ref=change:runtime:{idx} path={path} source=tool:Bash source_event_id=tool-bash:runtime:{idx} freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=runtime write created declared artifact"
            ));
            frame.recent_evidence.push(format!(
                "fact: file_facts ref=filefact:runtime:{idx}:read path={path} kind=read_observation source=tool:Read source_event_id=tool-read:runtime:{idx} freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for {path}"
            ));
        }
        frame.recent_evidence.push(format!(
            "fact: stage_continuation continuity_mode=repair next_action=verify_artifact failed_target={root} verified_facts=required_evidence_targets: {root} | failure_reason: completion blocked: artifact verification runtime Read evidence is missing | modification_direction: read declared artifacts and return runtime anchors"
        ));

        let required_targets = super::completion_gate_required_read_targets(&frame);
        assert_eq!(required_targets, required_paths);
        assert!(super::completion_gate_required_reads_closed(
            &frame,
            &LoopUsage::default()
        ));

        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
            r#"{"state":"executing","decision":"call_tool","next_action":{"action_type":"Bash","args":{"command":"echo should-not-run"}}}"#.into(),
        )]]);
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 1,
                    ..DecisionLoopConfig::default()
                },
                None,
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(
                    usage.tool_dispatch_count, 0,
                    "closed stage continuation should terminalize before consuming another tool"
                );
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert!(report.completion_evidence_gaps.is_empty());
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn closed_stage_continuation_terminalizes_before_polling_model_again() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let root = "/tmp/example-stage-continuation-close";
        let required_paths = [
            format!("{root}/README.md"),
            format!("{root}/runtime.py"),
            format!("{root}/model.py"),
            format!("{root}/demo.py"),
        ];
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.state = AgentState::Executing;
        frame.allowed_actions = vec!["run_command".into()];
        frame.allowed_tools = vec!["Bash".into()];
        push_completion_contract_with_refs(
            &mut frame,
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
            &[],
            &[
                "artifact:contract:dir",
                "artifact:contract:readme",
                "artifact:contract:runtime",
                "artifact:contract:model",
                "artifact:contract:demo",
            ],
        );
        push_artifact_target_fact(&mut frame, "artifact:contract:dir", root, "directory");
        for (ref_id, path) in [
            ("artifact:contract:readme", &required_paths[0]),
            ("artifact:contract:runtime", &required_paths[1]),
            ("artifact:contract:model", &required_paths[2]),
            ("artifact:contract:demo", &required_paths[3]),
        ] {
            push_artifact_target_fact(&mut frame, ref_id, path, "file");
            frame.recent_evidence.push(format!(
                "fact: file_facts ref=filefact:runtime:1:read path={path} kind=read_observation source=tool:Read source_event_id=tool-read:runtime:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for {path}"
            ));
        }
        frame.recent_evidence.push(format!(
            "fact: stage_continuation continuity_mode=repair next_action=verify_artifact failed_target={root} verified_facts=required_evidence_targets: {root} | failure_reason: completion blocked: artifact verification runtime Read evidence is missing | modification_direction: read declared artifacts and return runtime anchors"
        ));

        let required_targets = super::completion_gate_required_read_targets(&frame);
        assert_eq!(required_targets, required_paths);
        assert!(super::completion_gate_required_reads_closed(
            &frame,
            &LoopUsage::default()
        ));

        let client = ModelProviderClient::with_scripted_turns(Vec::new());
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 1,
                    ..DecisionLoopConfig::default()
                },
                None,
            ))
            .expect("loop should not error");
        match outcome {
            LoopOutcome::Done { usage, .. } => {
                assert_eq!(
                    usage.tool_dispatch_count, 0,
                    "closed stage continuation should terminalize before polling the model again"
                );
                let report = usage.worker_report.expect("worker report");
                assert_eq!(
                    report.completion_evidence_status,
                    CompletionEvidenceStatus::Sufficient
                );
                assert!(report.completion_evidence_gaps.is_empty());
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    fn u7_recovered_artifact_frame_missing_verification() -> StateFrame {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        frame.state = AgentState::Executing;
        push_completion_contract_with_refs(
            &mut frame,
            &["artifact:contract:dir", "artifact:contract:file"],
            &[],
            &["artifact:contract:dir", "artifact:contract:file"],
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            "/tmp/example-site",
            "directory",
        );
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:file",
            "/tmp/example-site/README.md",
            "file",
        );
        frame.recent_evidence.push(
            "fact: recent_changes_in_files ref=change:readme path=/tmp/example-site/README.md source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated README".into(),
        );
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:readme-created path=/tmp/example-site/README.md kind=file status=created source=tool:Write source_event_id=tool-write:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=README created".into(),
        );
        frame
    }

    #[test]
    fn recovered_artifact_repair_with_remaining_verification_gap_enters_verify_artifact() {
        let mut frame = u7_recovered_artifact_frame_missing_verification();
        let mut usage = LoopUsage {
            recovery_attempted: true,
            recovery_tier: Some("missing_path_recovery_gate".into()),
            recovery_outcome: Some("recovered".into()),
            ..LoopUsage::default()
        };

        assert!(super::missing_artifact_evidence_refs(&frame).is_empty());
        assert!(!super::missing_verification_evidence_refs(&frame).is_empty());
        assert!(super::promote_recovered_verification_gap(
            &mut frame, &mut usage
        ));

        assert_eq!(frame.state, AgentState::Verifying);
        assert_eq!(
            usage
                .completion_evidence_status
                .as_ref()
                .map(|s| s.as_str()),
            Some("missing_verification_evidence")
        );
        assert_eq!(
            usage.recovery_tier.as_deref(),
            Some("verification_repair_continuation")
        );
        assert_eq!(
            usage.recovery_outcome.as_deref(),
            Some("repair_turn_injected")
        );
        assert!(
            frame
                .open_items
                .iter()
                .any(|item| item.contains("required_action:verify_artifact"))
        );
    }

    #[test]
    fn verification_only_gap_after_recovery_does_not_spin_in_executing_until_max_iterations() {
        let mut frame = u7_recovered_artifact_frame_missing_verification();
        let mut usage = LoopUsage {
            recovery_attempted: true,
            recovery_outcome: Some("recovered".into()),
            ..LoopUsage::default()
        };

        assert!(super::promote_recovered_verification_gap(
            &mut frame, &mut usage
        ));

        assert_ne!(frame.state, AgentState::Executing);
        assert_eq!(frame.state, AgentState::Verifying);
    }

    #[test]
    fn max_iterations_with_only_verification_gap_is_not_generic_failure() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let frame = u7_recovered_artifact_frame_missing_verification();
        let client = ModelProviderClient::with_scripted_turns(Vec::new());
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 0,
                    ..DecisionLoopConfig::default()
                },
            ))
            .expect("loop should not error");

        match outcome {
            LoopOutcome::MaxIterationsReached { last_state, usage } => {
                assert_eq!(last_state, AgentState::Verifying);
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("missing_verification_evidence")
                );
                assert_eq!(
                    usage.recovery_tier.as_deref(),
                    Some("verification_repair_continuation")
                );
                assert_eq!(
                    usage.recovery_outcome.as_deref(),
                    Some("repair_turn_injected")
                );
                assert_eq!(
                    usage.terminal_blocker_kind.as_deref(),
                    Some("verification_repair_continuation")
                );
            }
            other => panic!("expected MaxIterationsReached, got {other:?}"),
        }
    }

    #[test]
    fn u7_recovered_artifact_repair_missing_readme_verification_classifies_verification_continuation()
     {
        let mut frame = u7_recovered_artifact_frame_missing_verification();
        let mut usage = LoopUsage {
            recovery_attempted: true,
            recovery_outcome: Some("recovered".into()),
            ..LoopUsage::default()
        };

        assert!(super::promote_recovered_verification_gap(
            &mut frame, &mut usage
        ));
        super::finalize_worker_usage_report(&frame, &mut usage);
        let report = usage.worker_report.expect("worker report");

        assert_eq!(
            report.completion_evidence_status.as_str(),
            "missing_verification_evidence"
        );
        assert!(
            report
                .completion_evidence_gaps
                .iter()
                .all(|gap| { !gap.missing_artifact_evidence && gap.missing_verification_evidence })
        );
        assert!(report.completion_evidence_gaps.iter().any(|gap| {
            gap.target_path.as_deref() == Some("/tmp/example-site/README.md")
                && gap.recommended_action == "verify_artifact"
        }));
    }

    #[test]
    fn verification_first_verifying_tail_does_not_end_as_terminal_after_repair_exhausted_when_reverify_is_still_actionable()
     {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let frame = u7_recovered_artifact_frame_missing_verification();
        let client = ModelProviderClient::with_scripted_turns(Vec::new());
        let outcome = rt
            .block_on(run_decision_loop(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 0,
                    ..DecisionLoopConfig::default()
                },
            ))
            .expect("loop should not error");

        match outcome {
            LoopOutcome::MaxIterationsReached { last_state, usage } => {
                assert_eq!(last_state, AgentState::Verifying);
                assert_eq!(
                    usage.recovery_tier.as_deref(),
                    Some("verification_repair_continuation")
                );
                assert_eq!(
                    usage.recovery_outcome.as_deref(),
                    Some("repair_turn_injected")
                );
                assert_eq!(
                    usage.terminal_blocker_kind.as_deref(),
                    Some("verification_repair_continuation")
                );
            }
            other => panic!("expected MaxIterationsReached, got {other:?}"),
        }
    }

    #[test]
    fn verification_first_directory_and_readme_gap_reuses_reverify_path_before_terminalizing() {
        let mut frame = u7_recovered_artifact_frame_missing_verification();
        let mut usage = LoopUsage {
            recovery_attempted: true,
            recovery_outcome: Some("recovered".into()),
            ..LoopUsage::default()
        };

        assert!(super::promote_recovered_verification_gap(
            &mut frame, &mut usage
        ));
        assert!(super::verification_gap_still_actionable(&frame, &usage));
        assert_eq!(frame.state, AgentState::Verifying);
    }

    #[test]
    fn directory_target_evidence_is_not_evaluated_as_file_only_evidence() {
        let mut frame = make_frame();
        frame.recent_evidence.clear();
        push_completion_contract_with_refs(&mut frame, &["artifact:contract:dir"], &[], &[]);
        push_artifact_target_fact(
            &mut frame,
            "artifact:contract:dir",
            "/tmp/example-site",
            "directory",
        );
        frame.recent_evidence.push(
            "fact: file_facts ref=filefact:1 path=/tmp/example-site/README.md kind=read_observation source=tool:Read source_event_id=tool-read:1 freshness=after-runtime-read confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none fact=runtime Read succeeded for /tmp/example-site/README.md".into(),
        );
        assert!(super::artifact_path_has_material_evidence(
            &frame,
            "/tmp/example-site",
            "directory"
        ));
    }

    #[test]
    fn done_passes_when_completion_evidence_is_sufficient() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut frame = make_frame();
        push_completion_contract(&mut frame, true, true, true);
        push_artifact_target_fact(&mut frame, "artifact:contract:0", "/tmp/report.md", "file");
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:ok path=/tmp/report.md kind=file status=verified source=tool:ArtifactVerify source_event_id=tool-artifact:1 freshness=after-runtime confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact verification passed".into(),
        );
        frame.recent_evidence.push(
            "fact: test_failures ref=test:ok name=cargo_test status=passed source=tool:Bash source_event_id=tool-bash:1 freshness=after-runtime-test confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=cargo test passed".into(),
        );
        let done_json = r#"{"state":"done","decision":"done"}"#;
        let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
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
                assert_eq!(
                    usage
                        .completion_evidence_status
                        .as_ref()
                        .map(|s| s.as_str()),
                    Some("sufficient")
                );
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn recovery_does_not_repeat_same_invalid_edit_strategy() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let target_path = unique_temp_path("repeat_invalid_edit");
        std::fs::write(&target_path, "alpha\n").expect("seed file");
        let edit_json = format!(
            r#"{{"state":"executing","decision":"call_tool","next_action":{{"action_type":"Edit","args":{{"file_path":"{}","old_string":"missing-line","new_string":"beta"}}}}}}"#,
            target_path.display()
        );
        let client = ModelProviderClient::with_scripted_turns(vec![
            vec![StreamEvent::TextDelta(edit_json.clone())],
            vec![StreamEvent::TextDelta(edit_json)],
        ]);
        let mut frame = make_frame();
        frame.allowed_actions.push("edit_file".into());
        frame.allowed_tools.push("Edit".into());
        let registry = ToolRegistry::new().register(Arc::new(FileEditTool));
        let permissions = ToolPermissionContext::new(PermissionMode::Default);
        permissions.add_always_allow_rule("Edit");
        let tool_runtime = StateFrameToolRuntime {
            registry,
            permissions,
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
        };
        let outcome = rt
            .block_on(run_decision_loop_with_tools(
                &client,
                frame,
                DecisionLoopConfig {
                    max_iterations: 2,
                    ..DecisionLoopConfig::default()
                },
                Some(tool_runtime),
            ))
            .expect("loop should not error");
        let _ = std::fs::remove_file(&target_path);
        match outcome {
            LoopOutcome::NoProgress { reason, usage, .. } => {
                assert!(reason.contains("read_before_edit"));
                assert_eq!(usage.recovery_tier.as_deref(), Some("strategy_dedupe"));
                assert_eq!(
                    usage.recovery_outcome.as_deref(),
                    Some("no_progress_escalation")
                );
                assert_eq!(
                    usage.terminal_blocker_kind.as_deref(),
                    Some("same_invalid_strategy")
                );
                assert!(
                    usage
                        .worker_report
                        .expect("worker report")
                        .evidence_refs
                        .iter()
                        .any(|reference| reference == "tool_feedback:1")
                );
            }
            other => panic!("expected NoProgress, got {other:?}"),
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
            cwd: test_runtime_paths().0,
            config_root: test_runtime_paths().1,
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
        let client =
            ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::Error(StreamError {
                provider_id: "openai".into(),
                kind: "empty_response_body".into(),
                message: "provider returned empty response body".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::PreStreamTerminal,
                status_code: None,
            })]]);
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

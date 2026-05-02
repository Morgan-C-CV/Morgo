use crate::core::message::Message;
use crate::core::state_frame::{
    AgentState, DecisionKind, RepairNeeded, StateFrame, StatePatch, validate_state_decision,
};
use crate::core::state_frame_hydration::hydrate_needed_context;
use crate::service::api::client::ModelProviderClient;
use crate::service::api::streaming::StreamEvent;

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
    RepairExhausted {
        raw_json: String,
        reason: String,
        usage: LoopUsage,
    },
}

/// Collect text and token usage from a stream of events.
fn collect_text_and_usage(events: Vec<StreamEvent>) -> (String, LoopUsage) {
    let mut text = String::new();
    let mut usage = LoopUsage::default();
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
            _ => {}
        }
    }
    (text, usage)
}

const STATE_DECISION_INSTRUCTION: &str = "\
You are an AI agent operating in StateFrame mode. \
Read the StateFrame JSON below and respond ONLY with valid StateDecision JSON.\n\
\n\
StateDecision schema:\n\
{\n\
  \"state\": \"<one of: planning, executing, reviewing, correcting, verifying, blocked, done>\",\n\
  \"decision\": \"<one of: continue, request_context, call_tool, handoff, accept, reject, done>\",\n\
  \"next_action\": null,\n\
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
- When using `needed_context`, prefer typed selectors like `file_snippet:path`, `test_failure`, `change_ref:path`, `review_ref:ref_id`, `artifact_ref:ref_id`, `open_item_ref:ref_id`, `blocker_ref:ref_id`, `rejected_approach:ref_id`, `artifact:path`, or `fact:name`\n\
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
    mut frame: StateFrame,
    config: DecisionLoopConfig,
) -> anyhow::Result<LoopOutcome> {
    let mut total_usage = LoopUsage::default();
    let mut fallback_ladder = FallbackLadderState::default();

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
        let (text, iter_usage) = collect_text_and_usage(events);
        total_usage.input_tokens += iter_usage.input_tokens;
        total_usage.uncached_input_tokens += iter_usage.uncached_input_tokens;
        total_usage.output_tokens += iter_usage.output_tokens;
        total_usage.cache_read_tokens += iter_usage.cache_read_tokens;
        total_usage.cache_write_tokens += iter_usage.cache_write_tokens;

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
                    let (repaired_text, repair_usage) = collect_text_and_usage(repair_events);
                    total_usage.input_tokens += repair_usage.input_tokens;
                    total_usage.uncached_input_tokens += repair_usage.uncached_input_tokens;
                    total_usage.output_tokens += repair_usage.output_tokens;
                    total_usage.cache_read_tokens += repair_usage.cache_read_tokens;
                    total_usage.cache_write_tokens += repair_usage.cache_write_tokens;
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
                let summary = hydrate_needed_context(&mut frame, &decision.needed_context);
                total_usage.hydration_count += summary.hydrated.len();
                total_usage.stale_ref_count += summary.stale.len();
                total_usage.hydration_ref_missing += summary.unavailable.len();
                frame.state = decision.state;
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
                if !summary.changed {
                    return Ok(LoopOutcome::NoProgress {
                        last_state: frame.state,
                        reason: "request_context decision produced no hydration progress".into(),
                        usage: total_usage,
                    });
                }
            }
            // Unsupported kinds: advance state, continue loop (observable via MaxIterationsReached).
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
    use super::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use crate::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use crate::service::api::client::ModelProviderClient;
    use crate::service::api::streaming::StreamEvent;

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
}

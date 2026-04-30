use crate::core::message::Message;
use crate::core::state_frame::{
    AgentState, DecisionKind, StateFrame, StatePatch, validate_state_decision,
};
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
- If `recent_evidence` contains `fact: execution_mode read_only_analysis`, prefer a single-turn `done`; do not use `continue` just to outline or narrate your plan\n\
- Treat `recent_evidence` entries prefixed with `fact:` as the authoritative Fact Ledger for this turn\n\
- If a fact entry already says `none`, `none recorded`, `absent`, or equivalent, do NOT request that same context again\n\
- Only use \"decision\": \"request_context\" when the missing fact is not already present in objective/open_items/blocked_items/accepted_summary/recent_evidence\n\
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
        let decision = match validate_state_decision(&text) {
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
                    match validate_state_decision(&repaired_text) {
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
                // Append requested context keys with prefix to distinguish from real evidence.
                for key in &decision.needed_context {
                    frame
                        .recent_evidence
                        .push(format!("requested_context: {key}"));
                }
                frame.state = decision.state;
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

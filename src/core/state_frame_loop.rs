use crate::core::message::Message;
use crate::core::state_frame::{AgentState, DecisionKind, StateFrame, validate_state_decision};
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
        Self { max_iterations: 5, repair_budget: 2 }
    }
}

#[derive(Debug, Clone)]
pub enum LoopOutcome {
    Done { final_state: AgentState },
    Rejected { reason: String },
    MaxIterationsReached { last_state: AgentState },
    RepairExhausted { raw_json: String, reason: String },
}

/// Collect text from a stream of events.
fn collect_text(events: Vec<StreamEvent>) -> String {
    events
        .into_iter()
        .filter_map(|e| if let StreamEvent::TextDelta(t) = e { Some(t) } else { None })
        .collect()
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
    for _iter in 0..config.max_iterations {
        let prompt = frame.to_prompt_segment().content;
        let events = client.stream_message(&Message::user(prompt)).await;
        let mut text = collect_text(events);

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
                    let repair_events =
                        client.stream_message(&Message::user(repair_prompt)).await;
                    let repaired_text = collect_text(repair_events);
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
                        });
                    }
                }
            }
        };

        match decision.decision {
            DecisionKind::Done => {
                return Ok(LoopOutcome::Done { final_state: decision.state });
            }
            DecisionKind::Reject => {
                let reason = decision
                    .next_action
                    .as_ref()
                    .and_then(|a| a.args.get("reason"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("rejected by model")
                    .to_string();
                return Ok(LoopOutcome::Rejected { reason });
            }
            DecisionKind::Continue => {
                // Advance state using only decision.state — no implicit field mutation.
                frame.state = decision.state;
            }
            DecisionKind::RequestContext => {
                // Append requested context keys with prefix to distinguish from real evidence.
                for key in &decision.needed_context {
                    frame.recent_evidence.push(format!("requested_context: {key}"));
                }
                frame.state = decision.state;
            }
            // Unsupported kinds: advance state, continue loop (observable via MaxIterationsReached).
            _ => {
                frame.state = decision.state;
            }
        }
    }

    Ok(LoopOutcome::MaxIterationsReached { last_state: frame.state })
}

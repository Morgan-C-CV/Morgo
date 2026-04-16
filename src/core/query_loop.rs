use crate::core::context::QueryContext;
use crate::core::events::{EngineEvent, ServiceFailureCode, ServiceFailureNotice};
use crate::core::message::Message;
use crate::hook::executor::{HookDecision, run_hook};
use crate::hook::registry::HookEvent;
use crate::service::api::streaming::{
    ProviderFailureDisposition, StopReason, StreamError, StreamEvent,
};
use crate::service::compact::{CompactPlanKind, CompactRecoveryErrorContext};
use crate::tool::orchestrator::{ToolExecutionOutcome, aggregate_execution_records};
use crate::tool::result::{
    ToolExecutionRecord, ToolExecutionReport, ToolReportContextModifier, ToolReportModifier,
};
use tokio::time::{Duration, timeout};

const WORKER_MAILBOX_IDLE_TIMEOUT_MS: u64 = 2_000;

#[derive(Debug, Clone)]
struct PreparedTurn {
    prompt: String,
    token_estimate: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParams {
    pub messages: Vec<Message>,
    pub max_turns: Option<usize>,
    pub max_output_tokens_recovery_limit: usize,
    pub max_budget_input_tokens: Option<usize>,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            max_turns: Some(4),
            max_output_tokens_recovery_limit: 3,
            max_budget_input_tokens: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopState {
    pub messages: Vec<Message>,
    pub auto_compact_tracking: Option<String>,
    pub max_output_tokens_recovery_count: usize,
    pub has_attempted_reactive_compact: bool,
    pub max_output_tokens_override: Option<u64>,
    pub pending_tool_use_summary: Option<String>,
    pub stop_hook_active: bool,
    pub turn_count: usize,
    pub transition: Option<Continue>,
}

impl LoopState {
    fn new(params: &QueryParams) -> Self {
        Self {
            messages: params.messages.clone(),
            auto_compact_tracking: None,
            max_output_tokens_recovery_count: 0,
            has_attempted_reactive_compact: false,
            max_output_tokens_override: None,
            pending_tool_use_summary: None,
            stop_hook_active: false,
            turn_count: 0,
            transition: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminal {
    Completed,
    MaxTurns {
        count: usize,
    },
    MaxBudget {
        budget_usd_cents: u64,
    },
    StopHookPrevented,
    AbortedStreaming,
    AbortedTools,
    ModelError {
        message: String,
        code: Option<ServiceFailureCode>,
    },
}

impl Terminal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::MaxTurns { .. } => "max_turns",
            Self::MaxBudget { .. } => "max_budget",
            Self::StopHookPrevented => "stop_hook_prevented",
            Self::AbortedStreaming => "aborted_streaming",
            Self::AbortedTools => "aborted_tools",
            Self::ModelError { .. } => "model_error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Continue {
    NextTurn,
    ToolUseFollowUp,
    MaxOutputTokensEscalate,
    MaxOutputTokensRecovery,
    CollapseDrainRetry,
    ReactiveCompactRetry,
    StopHookBlocking,
    TokenBudgetContinuation,
    ModelFallbackRetry,
}

impl Continue {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NextTurn => "next_turn",
            Self::ToolUseFollowUp => "tool_use_follow_up",
            Self::MaxOutputTokensEscalate => "max_output_tokens_escalate",
            Self::MaxOutputTokensRecovery => "max_output_tokens_recovery",
            Self::CollapseDrainRetry => "collapse_drain_retry",
            Self::ReactiveCompactRetry => "reactive_compact_retry",
            Self::StopHookBlocking => "stop_hook_blocking",
            Self::TokenBudgetContinuation => "token_budget_continuation",
            Self::ModelFallbackRetry => "model_fallback_retry",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryLoopState {
    Streaming,
    AwaitingTool,
    Interrupted,
    Compacting,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLoopResult {
    pub state: QueryLoopState,
    pub terminal: Terminal,
    pub messages: Vec<Message>,
    pub transition: Option<Continue>,
    pub events: Vec<EngineEvent>,
}

pub async fn run_query_loop(context: &QueryContext, input: Message) -> QueryLoopResult {
    run_query_loop_with_params(context, input, QueryParams::default()).await
}

pub async fn run_query_loop_with_params(
    context: &QueryContext,
    input: Message,
    params: QueryParams,
) -> QueryLoopResult {
    let mut state = LoopState::new(&params);
    state.messages.extend(inbox_messages(context, None));
    let mut events = Vec::new();
    let mut current_input = input;

    loop {
        if let Some(result) = check_turn_limits(context, &state, &params, &events) {
            return result;
        }
        let prepared = match prepare_turn(context, &mut state, &params, &current_input, &mut events)
        {
            Ok(prepared) => prepared,
            Err(result) => return result,
        };
        let streamed = stream_model_turn(context, &prepared, state.transition.as_ref()).await;
        if streamed.is_empty() {
            return finalize_turn(
                context,
                state,
                QueryLoopState::Completed,
                Terminal::Completed,
                events,
            );
        }

        let turn_events = std::mem::take(&mut events);
        let turn_outcome = consume_model_stream(
            context,
            &mut state,
            turn_events,
            streamed,
            params.max_output_tokens_recovery_limit,
        )
        .await;

        match decide_next_turn(
            context,
            &mut state,
            turn_outcome,
            &mut current_input,
            &mut events,
        )
        .await
        {
            NextTurnDecision::Return(result) => return result,
            NextTurnDecision::Continue => continue,
        }
    }
}

struct TurnOutcome {
    state: LoopState,
    events: Vec<EngineEvent>,
    decision: TurnDecision,
}

enum TurnDecision {
    Return(LoopState, Terminal),
    FinalizeNormalTurn(LoopState),
    ContinueWith(Message, Continue),
    AwaitMailbox,
}

enum NextTurnDecision {
    Return(QueryLoopResult),
    Continue,
}

enum NormalTurnFinalization {
    Return(QueryLoopResult),
    Continue {
        loop_state: LoopState,
        next_input: Message,
        events: Vec<EngineEvent>,
    },
}

fn check_turn_limits(
    context: &QueryContext,
    state: &LoopState,
    params: &QueryParams,
    events: &[EngineEvent],
) -> Option<QueryLoopResult> {
    if let Some(max_turns) = params.max_turns {
        if state.turn_count >= max_turns {
            let count = state.turn_count;
            return Some(finalize_turn(
                context,
                state.clone(),
                QueryLoopState::Failed,
                Terminal::MaxTurns { count },
                events.to_vec(),
            ));
        }
    }
    None
}

fn prepare_turn(
    context: &QueryContext,
    state: &mut LoopState,
    params: &QueryParams,
    current_input: &Message,
    events: &mut Vec<EngineEvent>,
) -> Result<PreparedTurn, QueryLoopResult> {
    let prepared_prompt = format!(
        "{}\n{}\n{}\n{}",
        context.current_system_prompt(),
        context.current_tools_prompt(),
        context.current_context_prompt(),
        current_input.content
    );
    let prepared = PreparedTurn {
        token_estimate: prepared_prompt.len(),
        prompt: prepared_prompt,
    };
    context
        .app_state
        .cost_tracker
        .record_model_usage("unknown", prepared.token_estimate, 0, 0, 0);

    if let Some(max_budget_input_tokens) = params.max_budget_input_tokens {
        if prepared.token_estimate >= max_budget_input_tokens {
            events.push(EngineEvent::Notice {
                kind: "budget",
                message: format!(
                    "token budget continuation requested at input size {}",
                    prepared.token_estimate
                ),
                code: None,
                service_failure: None,
            });
            if !params.messages.is_empty()
                || state.transition == Some(Continue::TokenBudgetContinuation)
            {
                return Err(finalize_turn(
                    context,
                    state.clone(),
                    QueryLoopState::Failed,
                    Terminal::MaxBudget {
                        budget_usd_cents: prepared.token_estimate as u64,
                    },
                    events.clone(),
                ));
            }
            state.transition = Some(Continue::TokenBudgetContinuation);
            return Err(finalize_turn(
                context,
                state.clone(),
                QueryLoopState::Completed,
                Terminal::Completed,
                events.clone(),
            ));
        }
    }

    let prompt_hook = process_user_input(context, state, events);
    if let Some(result) = prompt_hook {
        return Err(result);
    }

    if let Some(plan) = context
        .compactor
        .plan_auto_compact(prepared.token_estimate, 4096)
    {
        if let Some(compact_message) = plan.assistant_message.clone() {
            let compact_message = Message::assistant(compact_message);
            events.push(EngineEvent::CompactPlanIssued {
                kind: plan.kind.clone(),
                message: plan.notice_message.clone(),
            });
            events.push(EngineEvent::Notice {
                kind: plan.notice_kind,
                message: plan.notice_message,
                code: Some(ServiceFailureCode::CompactRecoveryError),
                service_failure: Some(ServiceFailureNotice {
                    service_failure_code: ServiceFailureCode::CompactRecoveryError,
                    provider_kind: None,
                    status_code: None,
                    retryable: true,
                    surface_visible: true,
                }),
            });
            events.push(EngineEvent::Transition(Continue::ReactiveCompactRetry));
            events.push(EngineEvent::MessageCommitted(compact_message.clone()));
            state.transition = Some(Continue::ReactiveCompactRetry);
            state.has_attempted_reactive_compact = true;
            state.auto_compact_tracking = Some(match plan.kind {
                CompactPlanKind::AutoCompact => "auto_compact".into(),
                CompactPlanKind::ReactiveCompact => "reactive_compact".into(),
                CompactPlanKind::CollapseDrain => "collapse_drain".into(),
                CompactPlanKind::Exhausted => "exhausted".into(),
            });
            state.messages.push(compact_message);
            return Err(finalize_turn(
                context,
                state.clone(),
                QueryLoopState::Compacting,
                Terminal::Completed,
                events.clone(),
            ));
        }
    }

    Ok(prepared)
}

fn process_user_input(
    context: &QueryContext,
    state: &mut LoopState,
    events: &mut Vec<EngineEvent>,
) -> Option<QueryLoopResult> {
    let prompt_hook = run_hook(&context.hook_registry, HookEvent::UserPromptSubmit);
    for message in prompt_hook.messages.clone() {
        events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }
    if let HookDecision::Deny(reason) = prompt_hook.decision {
        let denial = Message::assistant(format!("hook denied prompt: {reason}"));
        events.push(EngineEvent::Terminal(Terminal::ModelError {
            message: reason,
            code: None,
        }));
        state.messages.push(denial);
        return Some(QueryLoopResult {
            state: QueryLoopState::Failed,
            terminal: Terminal::ModelError {
                message: "prompt denied by hook".into(),
                code: None,
            },
            messages: state.messages.clone(),
            transition: state.transition.clone(),
            events: events.clone(),
        });
    }
    None
}

async fn stream_model_turn(
    context: &QueryContext,
    prepared: &PreparedTurn,
    transition: Option<&Continue>,
) -> Vec<StreamEvent> {
    let mut streamed = context
        .api_client
        .stream_message(&Message::user(prepared.prompt.clone()))
        .await;
    if matches!(transition, Some(Continue::ModelFallbackRetry)) {
        if let Some(index) = streamed.iter().position(|event| {
            matches!(
                event,
                StreamEvent::Error(error) if error.disposition.is_stream_interrupted()
            ) || matches!(
                event,
                StreamEvent::MessageStop {
                    stop_reason: StopReason::Error
                }
            )
        }) {
            streamed[index] = StreamEvent::Error(StreamError {
                provider_id: context.api_client.provider_config().provider_id,
                kind: "model_fallback_failed".into(),
                message: "model fallback retry failed".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            });
        }
    }
    streamed
}

async fn decide_next_turn(
    context: &QueryContext,
    state: &mut LoopState,
    turn_outcome: TurnOutcome,
    current_input: &mut Message,
    events: &mut Vec<EngineEvent>,
) -> NextTurnDecision {
    match turn_outcome.decision {
        TurnDecision::Return(loop_state, terminal) => NextTurnDecision::Return(finalize_turn(
            context,
            loop_state,
            terminal_state(&terminal),
            terminal,
            turn_outcome.events,
        )),
        TurnDecision::FinalizeNormalTurn(loop_state) => {
            match finalize_normal_turn(context, loop_state, turn_outcome.events) {
                NormalTurnFinalization::Return(result) => NextTurnDecision::Return(result),
                NormalTurnFinalization::Continue {
                    loop_state,
                    next_input,
                    events: next_events,
                } => {
                    *state = loop_state;
                    state.turn_count += 1;
                    state.transition = Some(Continue::StopHookBlocking);
                    *events = next_events;
                    events.push(EngineEvent::Transition(Continue::StopHookBlocking));
                    *current_input = next_input;
                    state
                        .messages
                        .extend(inbox_messages(context, context.agent_id.as_deref()));
                    NextTurnDecision::Continue
                }
            }
        }
        TurnDecision::ContinueWith(next_input, continue_reason) => {
            *state = turn_outcome.state;
            state.turn_count += 1;
            state.transition = Some(continue_reason.clone());
            *events = turn_outcome.events;
            events.push(EngineEvent::Transition(continue_reason));
            *current_input = next_input;
            state
                .messages
                .extend(inbox_messages(context, context.agent_id.as_deref()));
            NextTurnDecision::Continue
        }
        TurnDecision::AwaitMailbox => {
            if let Some(next_input) = next_worker_mailbox_message(context).await {
                *state = turn_outcome.state;
                state.turn_count += 1;
                state.transition = Some(Continue::NextTurn);
                *events = turn_outcome.events;
                events.push(EngineEvent::Transition(Continue::NextTurn));
                *current_input = next_input;
                state
                    .messages
                    .extend(inbox_messages(context, context.agent_id.as_deref()));
                return NextTurnDecision::Continue;
            }
            match finalize_normal_turn(context, turn_outcome.state, turn_outcome.events) {
                NormalTurnFinalization::Return(result) => NextTurnDecision::Return(result),
                NormalTurnFinalization::Continue {
                    loop_state,
                    next_input,
                    events: next_events,
                } => {
                    *state = loop_state;
                    state.turn_count += 1;
                    state.transition = Some(Continue::StopHookBlocking);
                    *events = next_events;
                    events.push(EngineEvent::Transition(Continue::StopHookBlocking));
                    *current_input = next_input;
                    state
                        .messages
                        .extend(inbox_messages(context, context.agent_id.as_deref()));
                    NextTurnDecision::Continue
                }
            }
        }
    }
}

async fn consume_model_stream(
    context: &QueryContext,
    state: &mut LoopState,
    mut engine_events: Vec<EngineEvent>,
    stream_events: Vec<StreamEvent>,
    max_output_tokens_recovery_limit: usize,
) -> TurnOutcome {
    let mut aggregated_text = String::new();
    let mut pending_tool_use: Option<(String, String)> = None;

    for event in stream_events {
        match event {
            StreamEvent::MessageStart => {}
            StreamEvent::TextDelta(delta) => {
                aggregated_text.push_str(&delta);
                engine_events.push(EngineEvent::AssistantDelta(delta));
            }
            StreamEvent::ToolUse { tool_name, input } => {
                engine_events.push(EngineEvent::ToolCallStarted {
                    tool_name: tool_name.clone(),
                    input: input.clone(),
                });
                pending_tool_use = Some((tool_name, input));
            }
            StreamEvent::Usage(usage) => {
                context.app_state.cost_tracker.record_model_usage(
                    &usage.model,
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_creation_input_tokens,
                    usage.cache_read_input_tokens,
                );
                engine_events.push(EngineEvent::Notice {
                    kind: "usage",
                    message: format!(
                        "recorded usage for model {} (input={}, output={})",
                        usage.model, usage.input_tokens, usage.output_tokens
                    ),
                    code: None,
                    service_failure: None,
                });
            }
            StreamEvent::MessageStop { stop_reason } => match stop_reason {
                StopReason::EndTurn => {
                    if !aggregated_text.is_empty() {
                        let message = Message::assistant(aggregated_text.clone());
                        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                        state.messages.push(message);
                    }
                    if context.is_subagent() {
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::AwaitMailbox,
                        };
                    }
                    return TurnOutcome {
                        state: state.clone(),
                        events: engine_events,
                        decision: TurnDecision::FinalizeNormalTurn(state.clone()),
                    };
                }
                StopReason::ToolUse => {
                    if !aggregated_text.is_empty() {
                        let message = Message::assistant(aggregated_text.clone());
                        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                        state.messages.push(message);
                    }
                    let Some((tool_name, tool_input)) = pending_tool_use.take() else {
                        let error_message =
                            Message::assistant("stream error: tool stop without tool payload");
                        engine_events.push(EngineEvent::MessageCommitted(error_message.clone()));
                        state.messages.push(error_message);
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::Return(
                                state.clone(),
                                Terminal::ModelError {
                                    message: "tool stop without tool payload".into(),
                                    code: None,
                                },
                            ),
                        };
                    };
                    let tool_outcome =
                        execute_tool_phase(context, state, engine_events, tool_name, tool_input)
                            .await;
                    return tool_outcome;
                }
                StopReason::MaxTokens => {
                    if !aggregated_text.is_empty() {
                        let message = Message::assistant(aggregated_text.clone());
                        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                        state.messages.push(message);
                    }
                    if state.max_output_tokens_override.is_none() {
                        state.max_output_tokens_override = Some(8_192);
                        engine_events.push(EngineEvent::Notice {
                            kind: "recovery",
                            message: "escalating max output tokens after model stop".into(),
                            code: None,
                            service_failure: None,
                        });
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::ContinueWith(
                                Message::user(
                                    "Please continue and finish the response after max output token escalation.",
                                ),
                                Continue::MaxOutputTokensEscalate,
                            ),
                        };
                    }
                    if state.max_output_tokens_recovery_count < max_output_tokens_recovery_limit {
                        state.max_output_tokens_recovery_count += 1;
                        engine_events.push(EngineEvent::Notice {
                            kind: "recovery",
                            message: "continuing after max output token interruption".into(),
                            code: None,
                            service_failure: None,
                        });
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::ContinueWith(
                                Message::user(
                                    "Please continue from where you were interrupted due to max output tokens.",
                                ),
                                Continue::MaxOutputTokensRecovery,
                            ),
                        };
                    }
                    return TurnOutcome {
                        state: state.clone(),
                        events: engine_events,
                        decision: TurnDecision::Return(state.clone(), Terminal::AbortedStreaming),
                    };
                }
                StopReason::Error => {
                    if !aggregated_text.is_empty() {
                        let message = Message::assistant(aggregated_text.clone());
                        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                        state.messages.push(message);
                    }
                    let stop_error = synthetic_stop_reason_error(state.transition.as_ref());
                    if stop_error.disposition.is_stream_terminal() {
                        let code = classify_service_failure_code(&stop_error);
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::Return(
                                state.clone(),
                                Terminal::ModelError {
                                    message: stop_error.message,
                                    code: Some(code),
                                },
                            ),
                        };
                    }
                    return continue_after_stream_error(context, state, engine_events, stop_error);
                }
            },
            StreamEvent::Error(error) => {
                let error_message = Message::assistant(format!("stream error: {}", error.message));
                engine_events.push(EngineEvent::MessageCommitted(error_message.clone()));
                state.messages.push(error_message);
                if error.disposition.is_stream_terminal() {
                    let code = classify_service_failure_code(&error);
                    return TurnOutcome {
                        state: state.clone(),
                        events: engine_events,
                        decision: TurnDecision::Return(
                            state.clone(),
                            Terminal::ModelError {
                                message: error.message,
                                code: Some(code),
                            },
                        ),
                    };
                }
                return continue_after_stream_error(context, state, engine_events, error);
            }
        }
    }

    TurnOutcome {
        state: state.clone(),
        events: engine_events,
        decision: TurnDecision::AwaitMailbox,
    }
}

async fn execute_tool_phase(
    context: &QueryContext,
    state: &mut LoopState,
    mut engine_events: Vec<EngineEvent>,
    tool_name: String,
    tool_input: String,
) -> TurnOutcome {
    let pre_tool_hook = run_hook(
        &context.hook_registry,
        HookEvent::PreToolUse {
            tool_name: tool_name.clone(),
        },
    );
    let effective_tool_input = crate::hook::permission_resolution::updated_input_from_hook(
        &pre_tool_hook.payload.permission_result,
    )
    .or(pre_tool_hook.payload.updated_input.clone())
    .unwrap_or(tool_input.clone());
    for message in pre_tool_hook.messages.clone() {
        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }
    let hook_permission_decision =
        crate::hook::permission_resolution::resolve_hook_permission_decision(
            &pre_tool_hook.payload.permission_result,
            crate::tool::definition::PermissionDecision::Allow,
        );
    if let crate::tool::definition::PermissionDecision::Deny {
        message: reason, ..
    } = hook_permission_decision
    {
        let denial = Message::assistant(format!("tool {tool_name} denied by hook: {reason}"));
        let permission_denied_hook = run_hook(
            &context.hook_registry,
            HookEvent::PermissionDenied {
                tool_name: tool_name.clone(),
                reason: reason.clone(),
            },
        );
        let post_failure_hook = run_hook(
            &context.hook_registry,
            HookEvent::PostToolUseFailure {
                tool_name: tool_name.clone(),
            },
        );
        state.messages.push(denial.clone());
        engine_events.push(EngineEvent::MessageCommitted(denial));
        for message in permission_denied_hook.messages {
            engine_events.push(EngineEvent::MessageCommitted(message.clone()));
            state.messages.push(message);
        }
        for message in post_failure_hook.messages {
            engine_events.push(EngineEvent::MessageCommitted(message.clone()));
            state.messages.push(message);
        }
        return TurnOutcome {
            state: state.clone(),
            events: engine_events,
            decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
        };
    }
    if let HookDecision::Deny(reason) = pre_tool_hook.decision {
        let denial = Message::assistant(format!("tool {tool_name} denied by hook: {reason}"));
        let permission_denied_hook = run_hook(
            &context.hook_registry,
            HookEvent::PermissionDenied {
                tool_name: tool_name.clone(),
                reason: reason.clone(),
            },
        );
        let post_failure_hook = run_hook(
            &context.hook_registry,
            HookEvent::PostToolUseFailure {
                tool_name: tool_name.clone(),
            },
        );
        state.messages.push(denial.clone());
        engine_events.push(EngineEvent::MessageCommitted(denial));
        for message in permission_denied_hook.messages {
            engine_events.push(EngineEvent::MessageCommitted(message.clone()));
            state.messages.push(message);
        }
        for message in post_failure_hook.messages {
            engine_events.push(EngineEvent::MessageCommitted(message.clone()));
            state.messages.push(message);
        }
        return TurnOutcome {
            state: state.clone(),
            events: engine_events,
            decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
        };
    }

    let permission_request_hook = run_hook(
        &context.hook_registry,
        HookEvent::PermissionRequest {
            tool_name: tool_name.clone(),
        },
    );
    for message in permission_request_hook.messages.clone() {
        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }
    let requested_permission_decision =
        crate::hook::permission_resolution::resolve_hook_permission_decision(
            &permission_request_hook.payload.permission_result,
            crate::tool::definition::PermissionDecision::Allow,
        );
    if let crate::tool::definition::PermissionDecision::Deny {
        message: reason, ..
    } = requested_permission_decision
    {
        let denial = Message::assistant(format!(
            "tool {tool_name} denied before execution: {reason}"
        ));
        let permission_denied_hook = run_hook(
            &context.hook_registry,
            HookEvent::PermissionDenied {
                tool_name: tool_name.clone(),
                reason: reason.clone(),
            },
        );
        engine_events.push(EngineEvent::MessageCommitted(denial.clone()));
        state.messages.push(denial);
        for message in permission_denied_hook.messages {
            engine_events.push(EngineEvent::MessageCommitted(message.clone()));
            state.messages.push(message);
        }
        return TurnOutcome {
            state: state.clone(),
            events: engine_events,
            decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
        };
    }

    let orchestrator = crate::tool::orchestrator::ToolOrchestrator::new(&context.tool_registry);
    let tool_result = orchestrator
        .execute(
            &[crate::tool::orchestrator::ToolExecutionRequest {
                call: crate::tool::definition::ToolCall::new(
                    tool_name.clone(),
                    effective_tool_input.clone(),
                ),
            }],
            &context.app_state.permission_context,
        )
        .await;

    match tool_result {
        Ok(outcomes) => {
            let report = aggregate_execution_records(
                &outcomes
                    .iter()
                    .map(|outcome| outcome.record.clone())
                    .collect::<Vec<_>>(),
            );
            match outcomes.into_iter().next() {
                Some(ToolExecutionOutcome { result, record, .. }) => {
                    let report = report.unwrap_or_else(|| ToolExecutionReport {
                        records: vec![record.clone()],
                        summary: record.summary.clone(),
                        detail: record.detail.clone(),
                        report_modifier: record.report_modifier.clone(),
                        context_modifier: ToolReportContextModifier::None,
                    });
                    match result {
                        crate::tool::definition::ToolResult::Text(text) => {
                            let post_tool_hook = run_hook(
                                &context.hook_registry,
                                HookEvent::PostToolUse {
                                    tool_name: tool_name.clone(),
                                },
                            );
                            for message in post_tool_hook.messages.clone() {
                                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                                state.messages.push(message);
                            }
                            let tool_message = Message::assistant(format!(
                                "tool {tool_name} result: {}",
                                report_detail_or_summary(&report)
                            ));
                            engine_events
                                .push(tool_result_committed_event(&tool_name, &text, &record));
                            engine_events.push(EngineEvent::MessageCommitted(tool_message.clone()));
                            state.messages.push(tool_message);
                            if post_tool_hook.prevent_continuation {
                                state.stop_hook_active = true;
                                state.transition = Some(Continue::StopHookBlocking);
                                return TurnOutcome {
                                    state: state.clone(),
                                    events: engine_events,
                                    decision: TurnDecision::Return(
                                        state.clone(),
                                        Terminal::StopHookPrevented,
                                    ),
                                };
                            }
                            apply_tool_report_context(state, &report);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::ContinueWith(
                                    Message::user(format!(
                                        "tool result for {tool_name}: {}",
                                        report_detail_or_summary(&report)
                                    )),
                                    Continue::ToolUseFollowUp,
                                ),
                            }
                        }
                        crate::tool::definition::ToolResult::Denied(reason) => {
                            let permission_denied_hook = run_hook(
                                &context.hook_registry,
                                HookEvent::PermissionDenied {
                                    tool_name: tool_name.clone(),
                                    reason: reason.clone(),
                                },
                            );
                            let post_failure_hook = run_hook(
                                &context.hook_registry,
                                HookEvent::PostToolUseFailure {
                                    tool_name: tool_name.clone(),
                                },
                            );
                            for message in permission_denied_hook.messages {
                                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                                state.messages.push(message);
                            }
                            for message in post_failure_hook.messages {
                                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                                state.messages.push(message);
                            }
                            engine_events.push(tool_notice_event(&record));
                            apply_tool_report_context(state, &report);
                            let denial = Message::assistant(format!(
                                "tool {tool_name} denied: {}",
                                report_detail_or_summary(&report)
                            ));
                            let missing_tool_result = Message::assistant(format!(
                                "tool {tool_name} result missing; synthesized denial result preserved"
                            ));
                            engine_events.push(EngineEvent::MessageCommitted(denial.clone()));
                            engine_events
                                .push(EngineEvent::MessageCommitted(missing_tool_result.clone()));
                            state.messages.push(denial);
                            state.messages.push(missing_tool_result);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::Return(
                                    state.clone(),
                                    Terminal::AbortedTools,
                                ),
                            }
                        }
                        crate::tool::definition::ToolResult::PendingApproval {
                            tool_name,
                            message,
                            approval,
                        } => {
                            let pending_summary = record
                                .pending_approval
                                .as_ref()
                                .map(|pending| pending.summary.clone())
                                .unwrap_or_else(|| approval.summary.clone());
                            let pending_detail = record
                                .pending_approval
                                .as_ref()
                                .and_then(|pending| pending.detail.clone())
                                .or_else(|| approval.detail.clone());
                            let pending_code = record
                                .pending_approval
                                .as_ref()
                                .and_then(|pending| pending.code.clone())
                                .or_else(|| approval.code.clone());
                            let pending_kind = record
                                .pending_approval
                                .as_ref()
                                .and_then(|pending| pending.approval_kind.clone())
                                .or_else(|| approval.approval_kind.clone());
                            let pending_reasons = record
                                .pending_approval
                                .as_ref()
                                .map(|pending| pending.escalation_reasons.clone())
                                .unwrap_or_else(|| approval.escalation_reasons.clone());
                            context
                                .app_state
                                .permission_context
                                .set_pending_approval(Some(
                                    crate::state::permission_context::PendingApproval {
                                        tool_name: tool_name.clone(),
                                        tool_input: effective_tool_input.clone(),
                                        message: message.clone(),
                                        code: pending_code.clone(),
                                        summary: Some(pending_summary.clone()),
                                        detail: pending_detail.clone(),
                                        approval_kind: pending_kind.clone(),
                                        escalation_reasons: pending_reasons.clone(),
                                    },
                                ));
                            engine_events.push(EngineEvent::PendingApproval {
                                tool_name: tool_name.clone(),
                                message: message.clone(),
                                code: pending_code,
                                summary: pending_summary.clone(),
                                detail: pending_detail.clone(),
                                approval_kind: pending_kind,
                                escalation_reasons: pending_reasons,
                                report_modifier: record.report_modifier.clone(),
                            });
                            apply_tool_report_context(state, &report);
                            let approval_message = Message::assistant(format!(
                                "approval required for {tool_name}: {}",
                                pending_detail
                                    .clone()
                                    .unwrap_or_else(|| pending_summary.clone())
                            ));
                            engine_events
                                .push(EngineEvent::MessageCommitted(approval_message.clone()));
                            state.messages.push(approval_message);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::Return(
                                    state.clone(),
                                    Terminal::AbortedTools,
                                ),
                            }
                        }
                        crate::tool::definition::ToolResult::Interrupted(_reason) => {
                            engine_events.push(tool_notice_event(&record));
                            apply_tool_report_context(state, &report);
                            let interrupted = Message::assistant(format!(
                                "tool {tool_name} interrupted: {}",
                                report_detail_or_summary(&report)
                            ));
                            engine_events.push(EngineEvent::MessageCommitted(interrupted.clone()));
                            state.messages.push(interrupted);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::Return(
                                    state.clone(),
                                    Terminal::AbortedTools,
                                ),
                            }
                        }
                        crate::tool::definition::ToolResult::Progress(_progress) => {
                            engine_events.push(tool_notice_event(&record));
                            apply_tool_report_context(state, &report);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::ContinueWith(
                                    Message::user(format!(
                                        "tool progress for {tool_name}: {}",
                                        report.summary
                                    )),
                                    Continue::ToolUseFollowUp,
                                ),
                            }
                        }
                        crate::tool::definition::ToolResult::ResultTooLarge(_reason) => {
                            engine_events.push(tool_notice_event(&record));
                            apply_tool_report_context(state, &report);
                            let oversized = Message::assistant(format!(
                                "tool {tool_name} result too large: {}",
                                report_detail_or_summary(&report)
                            ));
                            engine_events.push(EngineEvent::MessageCommitted(oversized.clone()));
                            state.messages.push(oversized);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::Return(
                                    state.clone(),
                                    Terminal::AbortedTools,
                                ),
                            }
                        }
                    }
                }
                None => TurnOutcome {
                    state: state.clone(),
                    events: engine_events,
                    decision: TurnDecision::Return(
                        state.clone(),
                        Terminal::ModelError {
                            message: "tool orchestrator returned no outcome".into(),
                            code: None,
                        },
                    ),
                },
            }
        }
        Err(error) => {
            let post_failure_hook = run_hook(
                &context.hook_registry,
                HookEvent::PostToolUseFailure {
                    tool_name: tool_name.clone(),
                },
            );
            for message in post_failure_hook.messages {
                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                state.messages.push(message);
            }
            engine_events.push(EngineEvent::Notice {
                kind: "tool",
                message: format!(
                    "injecting missing tool result after tool failure for {tool_name}"
                ),
                code: None,
                service_failure: None,
            });
            let failure = Message::assistant(format!("tool {tool_name} failed: {error}"));
            let missing_tool_result = Message::assistant(format!(
                "tool {tool_name} result missing; synthesized failure result preserved"
            ));
            engine_events.push(EngineEvent::MessageCommitted(failure.clone()));
            engine_events.push(EngineEvent::MessageCommitted(missing_tool_result.clone()));
            state.messages.push(failure);
            state.messages.push(missing_tool_result);
            TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
            }
        }
    }
}

fn record_detail_or_summary(record: &ToolExecutionRecord) -> String {
    record
        .detail
        .clone()
        .unwrap_or_else(|| record.summary.clone())
}

fn report_detail_or_summary(report: &ToolExecutionReport) -> String {
    report
        .detail
        .clone()
        .unwrap_or_else(|| report.summary.clone())
}

fn apply_tool_report_context(state: &mut LoopState, report: &ToolExecutionReport) {
    match &report.context_modifier {
        ToolReportContextModifier::None => {}
        ToolReportContextModifier::SetPendingToolUseSummary(summary) => {
            state.pending_tool_use_summary = Some(summary.clone());
        }
        ToolReportContextModifier::ContinueWithUserMessage(message) => {
            state.pending_tool_use_summary = Some(message.clone());
        }
    }
}

fn tool_notice_event(record: &ToolExecutionRecord) -> EngineEvent {
    let kind = match record.report_modifier {
        ToolReportModifier::Progress => "tool-progress",
        ToolReportModifier::Pending => "tool-pending",
        ToolReportModifier::NeedsAttention => "tool-attention",
        ToolReportModifier::None => "tool",
    };
    let code = mcp_runtime_failure_code(record.detail.as_deref());
    EngineEvent::Notice {
        kind,
        message: format!("{}: {}", record.summary, record_detail_or_summary(record)),
        code,
        service_failure: None,
    }
}

fn tool_result_committed_event(
    tool_name: &str,
    content: &str,
    record: &ToolExecutionRecord,
) -> EngineEvent {
    EngineEvent::ToolResultCommitted {
        tool_name: tool_name.to_string(),
        content: content.to_string(),
        summary: record.summary.clone(),
        detail: record.detail.clone(),
        kind: record.kind.clone(),
        report_modifier: record.report_modifier.clone(),
    }
}

fn inbox_messages(context: &QueryContext, target_task_id: Option<&str>) -> Vec<Message> {
    context
        .app_state
        .permission_context
        .task_manager
        .as_ref()
        .map(|manager| {
            manager
                .drain_events_for_target(&context.app_state.active_session_id, target_task_id)
                .into_iter()
                .map(|event| {
                    let notification =
                        crate::coordinator::worker::TaskNotification::from_task_event(&event);
                    Message::user(notification.format_as_user_message())
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn next_worker_mailbox_message(context: &QueryContext) -> Option<Message> {
    let agent_id = context.agent_id.as_deref()?;
    let manager = context.app_state.permission_context.task_manager.as_ref()?;
    timeout(
        Duration::from_millis(WORKER_MAILBOX_IDLE_TIMEOUT_MS),
        manager.wait_for_mailbox_message(agent_id),
    )
    .await
    .ok()
    .flatten()
    .map(Message::user)
}

fn finalize_normal_turn(
    context: &QueryContext,
    mut state: LoopState,
    mut events: Vec<EngineEvent>,
) -> NormalTurnFinalization {
    state
        .messages
        .extend(inbox_messages(context, context.agent_id.as_deref()));
    if matches!(
        context.app_state.runtime_role,
        crate::state::app_state::RuntimeRole::Coordinator
    ) && context
        .app_state
        .permission_context
        .task_manager
        .as_ref()
        .is_some_and(|manager| {
            manager.has_pending_orchestration(&context.app_state.active_session_id)
        })
    {
        let gating_message = Message::assistant(
            "orchestration still pending: wait for grouped research fan-in or verification before final synthesis",
        );
        events.push(EngineEvent::MessageCommitted(gating_message.clone()));
        state.messages.push(gating_message);
        return NormalTurnFinalization::Return(QueryLoopResult {
            state: QueryLoopState::Completed,
            terminal: Terminal::Completed,
            messages: state.messages,
            transition: Some(Continue::NextTurn),
            events,
        });
    }

    let stop_event = if context.is_subagent() {
        HookEvent::SubagentStop
    } else {
        HookEvent::Stop
    };
    let stop_hook = run_hook(&context.hook_registry, stop_event);
    for message in stop_hook.messages.clone() {
        events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }

    if stop_hook.prevent_continuation {
        events.push(EngineEvent::Notice {
            kind: "hook",
            message: "stop hook prevented continuation".into(),
            code: None,
            service_failure: None,
        });
        let terminal = Terminal::StopHookPrevented;
        events.push(EngineEvent::Terminal(terminal.clone()));
        return NormalTurnFinalization::Return(QueryLoopResult {
            state: terminal_state(&terminal),
            terminal,
            messages: state.messages,
            transition: state.transition,
            events,
        });
    }

    if stop_hook.block_continuation {
        state.stop_hook_active = true;
        state.transition = Some(Continue::StopHookBlocking);
        events.push(EngineEvent::Notice {
            kind: "hook",
            message: "stop hook requested blocking continuation retry".into(),
            code: None,
            service_failure: None,
        });
        return NormalTurnFinalization::Continue {
            loop_state: state,
            next_input: Message::user("Address the stop-hook blocking feedback and continue."),
            events,
        };
    }

    let terminal = Terminal::Completed;
    events.push(EngineEvent::Terminal(terminal.clone()));
    NormalTurnFinalization::Return(QueryLoopResult {
        state: QueryLoopState::Completed,
        terminal,
        messages: state.messages,
        transition: state.transition,
        events,
    })
}

fn finalize_turn(
    context: &QueryContext,
    mut state: LoopState,
    default_state: QueryLoopState,
    terminal: Terminal,
    mut events: Vec<EngineEvent>,
) -> QueryLoopResult {
    state
        .messages
        .extend(inbox_messages(context, context.agent_id.as_deref()));
    events.push(EngineEvent::Terminal(terminal.clone()));

    QueryLoopResult {
        state: terminal_state(&terminal).or(default_state),
        terminal,
        messages: state.messages,
        transition: state.transition,
        events,
    }
}

fn terminal_state(terminal: &Terminal) -> QueryLoopState {
    match terminal {
        Terminal::Completed | Terminal::StopHookPrevented => QueryLoopState::Completed,
        Terminal::MaxTurns { .. } | Terminal::MaxBudget { .. } | Terminal::ModelError { .. } => {
            QueryLoopState::Failed
        }
        Terminal::AbortedStreaming | Terminal::AbortedTools => QueryLoopState::Interrupted,
    }
}

fn continue_after_stream_error(
    context: &QueryContext,
    state: &mut LoopState,
    mut engine_events: Vec<EngineEvent>,
    error: StreamError,
) -> TurnOutcome {
    if state.transition != Some(Continue::ModelFallbackRetry)
        && should_trigger_model_fallback(&error)
    {
        let code = classify_service_failure_code(&error);
        let service_failure = service_failure_notice_from_error(&error, code.clone(), true);
        engine_events.push(EngineEvent::Notice {
            kind: "recovery",
            message: format!(
                "model fallback retry triggered after stream error [{}]: {}",
                error.kind, error.message
            ),
            code: Some(code),
            service_failure: Some(service_failure),
        });
        return TurnOutcome {
            state: state.clone(),
            events: engine_events,
            decision: TurnDecision::ContinueWith(
                Message::user("Retry after model fallback recovery."),
                Continue::ModelFallbackRetry,
            ),
        };
    }
    let plan = context.compactor.plan_stream_error_recovery(
        state.has_attempted_reactive_compact,
        state.transition == Some(Continue::CollapseDrainRetry),
        Some(CompactRecoveryErrorContext {
            kind: &error.kind,
            message: &error.message,
        }),
    );
    engine_events.push(EngineEvent::CompactPlanIssued {
        kind: plan.kind.clone(),
        message: plan.notice_message.clone(),
    });
    engine_events.push(EngineEvent::Notice {
        kind: plan.notice_kind,
        message: plan.notice_message.clone(),
        code: Some(ServiceFailureCode::CompactRecoveryError),
        service_failure: Some(ServiceFailureNotice {
            service_failure_code: ServiceFailureCode::CompactRecoveryError,
            provider_kind: Some(error.provider_id.clone()),
            status_code: error.status_code,
            retryable: true,
            surface_visible: true,
        }),
    });
    match plan.kind {
        CompactPlanKind::ReactiveCompact => {
            state.has_attempted_reactive_compact = true;
            state.auto_compact_tracking = Some("reactive_compact".into());
            TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::ContinueWith(
                    Message::user(plan.retry_prompt.expect("reactive compact prompt")),
                    Continue::ReactiveCompactRetry,
                ),
            }
        }
        CompactPlanKind::CollapseDrain => {
            state.auto_compact_tracking = Some("collapse_drain".into());
            TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::ContinueWith(
                    Message::user(plan.retry_prompt.expect("collapse drain prompt")),
                    Continue::CollapseDrainRetry,
                ),
            }
        }
        CompactPlanKind::Exhausted => {
            state.auto_compact_tracking = Some("exhausted".into());
            let code = classify_service_failure_code(&error);
            TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::Return(
                    state.clone(),
                    Terminal::ModelError {
                        message: error.message,
                        code: Some(code),
                    },
                ),
            }
        }
        CompactPlanKind::AutoCompact => unreachable!("auto compact is pre-stream only"),
    }
}

fn should_trigger_model_fallback(error: &StreamError) -> bool {
    error.kind == "model_fallback" || (error.disposition.is_stream_interrupted() && error.retryable)
}

fn service_failure_notice_from_error(
    error: &StreamError,
    service_failure_code: ServiceFailureCode,
    surface_visible: bool,
) -> ServiceFailureNotice {
    ServiceFailureNotice {
        service_failure_code,
        provider_kind: Some(error.provider_id.clone()),
        status_code: error.status_code,
        retryable: error.retryable,
        surface_visible,
    }
}

fn synthetic_stop_reason_error(transition: Option<&Continue>) -> StreamError {
    let kind = if matches!(transition, Some(Continue::ModelFallbackRetry)) {
        "model_fallback_failed"
    } else {
        "stream_stop_error"
    };
    let disposition = ProviderFailureDisposition::StreamInterrupted;
    StreamError {
        provider_id: "provider".into(),
        kind: kind.into(),
        message: "stream stopped with error".into(),
        retryable: false,
        disposition,
        status_code: None,
    }
}

fn classify_service_failure_code(error: &StreamError) -> ServiceFailureCode {
    match error.disposition {
        ProviderFailureDisposition::PreStreamRetryable
        | ProviderFailureDisposition::PreStreamTerminal => {
            classify_pre_stream_failure_code(&error.kind, error.status_code)
        }
        ProviderFailureDisposition::StreamInterrupted
        | ProviderFailureDisposition::StreamTerminal => {
            classify_stream_failure_code(&error.kind, error.disposition.clone())
        }
    }
}

fn classify_pre_stream_failure_code(kind: &str, status_code: Option<u16>) -> ServiceFailureCode {
    match kind {
        "timeout" => ServiceFailureCode::ApiProviderTimeout,
        "transport" => ServiceFailureCode::ApiProviderTransport,
        "request_build" => ServiceFailureCode::ApiProviderRequestBuild,
        "invalid_response" => ServiceFailureCode::ApiProviderInvalidResponse,
        "sse_protocol" => ServiceFailureCode::ApiStreamProtocol,
        "http_status" => match status_code {
            Some(429) => ServiceFailureCode::ApiProviderHttp429,
            Some(500..=599) => ServiceFailureCode::ApiProviderHttp5xx,
            Some(400..=499) => ServiceFailureCode::ApiProviderHttp4xx,
            _ => ServiceFailureCode::ApiProviderInvalidResponse,
        },
        _ => ServiceFailureCode::ApiProviderInvalidResponse,
    }
}

fn classify_stream_failure_code(
    kind: &str,
    disposition: ProviderFailureDisposition,
) -> ServiceFailureCode {
    match kind {
        "model_fallback" | "model_fallback_failed" => ServiceFailureCode::ApiStreamModelFallback,
        "overloaded_error" => ServiceFailureCode::ApiStreamOverloaded,
        "sse_protocol" => ServiceFailureCode::ApiStreamProtocol,
        _ if disposition.is_stream_interrupted() => ServiceFailureCode::ApiStreamInterrupted,
        _ => ServiceFailureCode::ApiStreamTerminal,
    }
}

fn mcp_runtime_failure_code(detail: Option<&str>) -> Option<ServiceFailureCode> {
    detail
        .filter(|value| value.contains("mcp") || value.contains("MCP"))
        .map(|_| ServiceFailureCode::McpRuntimeError)
}

trait QueryLoopStateExt {
    fn or(self, fallback: QueryLoopState) -> QueryLoopState;
}

impl QueryLoopStateExt for QueryLoopState {
    fn or(self, _fallback: QueryLoopState) -> QueryLoopState {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LoopState, QueryParams, apply_tool_report_context, classify_pre_stream_failure_code,
        classify_stream_failure_code, report_detail_or_summary,
    };
    use crate::core::events::ServiceFailureCode;
    use crate::service::api::streaming::ProviderFailureDisposition;
    use crate::tool::result::{ToolExecutionReport, ToolReportContextModifier, ToolReportModifier};

    #[test]
    fn report_detail_or_summary_prefers_detail() {
        let report = ToolExecutionReport {
            records: Vec::new(),
            summary: "summary".into(),
            detail: Some("detail".into()),
            report_modifier: ToolReportModifier::None,
            context_modifier: ToolReportContextModifier::None,
        };

        assert_eq!(report_detail_or_summary(&report), "detail");
    }

    #[test]
    fn apply_tool_report_context_sets_pending_summary_from_continue_message() {
        let mut state = LoopState::new(&QueryParams::default());
        let report = ToolExecutionReport {
            records: Vec::new(),
            summary: "2 tool results".into(),
            detail: Some("alpha\nbeta".into()),
            report_modifier: ToolReportModifier::None,
            context_modifier: ToolReportContextModifier::ContinueWithUserMessage(
                "alpha\nbeta".into(),
            ),
        };

        apply_tool_report_context(&mut state, &report);

        assert_eq!(
            state.pending_tool_use_summary.as_deref(),
            Some("alpha\nbeta")
        );
    }

    #[test]
    fn apply_tool_report_context_sets_pending_summary_from_pending_modifier() {
        let mut state = LoopState::new(&QueryParams::default());
        let report = ToolExecutionReport {
            records: Vec::new(),
            summary: "Read succeeded; ProgressTool in progress".into(),
            detail: Some("alpha\nstill running".into()),
            report_modifier: ToolReportModifier::Progress,
            context_modifier: ToolReportContextModifier::SetPendingToolUseSummary(
                "Read succeeded; ProgressTool in progress".into(),
            ),
        };

        apply_tool_report_context(&mut state, &report);

        assert_eq!(
            state.pending_tool_use_summary.as_deref(),
            Some("Read succeeded; ProgressTool in progress")
        );
    }

    #[test]
    fn classify_pre_stream_failure_code_uses_status_and_kind_metadata() {
        assert_eq!(
            classify_pre_stream_failure_code("http_status", Some(429)),
            ServiceFailureCode::ApiProviderHttp429
        );
        assert_eq!(
            classify_pre_stream_failure_code("http_status", Some(503)),
            ServiceFailureCode::ApiProviderHttp5xx
        );
        assert_eq!(
            classify_pre_stream_failure_code("http_status", Some(400)),
            ServiceFailureCode::ApiProviderHttp4xx
        );
        assert_eq!(
            classify_pre_stream_failure_code("transport", None),
            ServiceFailureCode::ApiProviderTransport
        );
        assert_eq!(
            classify_pre_stream_failure_code("timeout", None),
            ServiceFailureCode::ApiProviderTimeout
        );
        assert_eq!(
            classify_pre_stream_failure_code("request_build", None),
            ServiceFailureCode::ApiProviderRequestBuild
        );
        assert_eq!(
            classify_pre_stream_failure_code("invalid_response", None),
            ServiceFailureCode::ApiProviderInvalidResponse
        );
    }

    #[test]
    fn classify_stream_failure_code_uses_stream_kind_and_disposition() {
        assert_eq!(
            classify_stream_failure_code(
                "model_fallback",
                ProviderFailureDisposition::StreamInterrupted,
            ),
            ServiceFailureCode::ApiStreamModelFallback
        );
        assert_eq!(
            classify_stream_failure_code(
                "overloaded_error",
                ProviderFailureDisposition::StreamInterrupted,
            ),
            ServiceFailureCode::ApiStreamOverloaded
        );
        assert_eq!(
            classify_stream_failure_code(
                "provider_stream",
                ProviderFailureDisposition::StreamInterrupted
            ),
            ServiceFailureCode::ApiStreamInterrupted
        );
        assert_eq!(
            classify_stream_failure_code(
                "provider_terminal",
                ProviderFailureDisposition::StreamTerminal
            ),
            ServiceFailureCode::ApiStreamTerminal
        );
    }
}

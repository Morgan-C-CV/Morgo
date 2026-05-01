use crate::core::context::QueryContext;
use crate::core::events::{EngineEvent, ServiceFailureCode, ServiceFailureNotice};
use crate::core::message::Message;
use crate::hook::executor::{HookDecision, run_hook};
use crate::hook::registry::HookEvent;
use crate::service::api::streaming::{
    ProviderFailureDisposition, StopReason, StreamError, StreamEvent,
};
use crate::service::compact::{
    AUTO_COMPACT_INPUT_CHAR_LIMIT, CompactRecoveryErrorContext, CompactServiceNextStep,
};
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
    pub prompt_only_output_contract_active: bool,
    pub prompt_only_discovery_locked: bool,
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
            prompt_only_output_contract_active: false,
            prompt_only_discovery_locked: false,
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
    state.messages.push(current_input.clone());

    loop {
        context.app_state.record_activity();

        // Start a keep-alive heartbeat for the duration of this turn
        let turn_token = tokio_util::sync::CancellationToken::new();
        let hb_token = turn_token.clone();
        let app_state = context.app_state.clone();
        let hb_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            // First tick is immediate, so we skip it to avoid double-reporting at start
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => app_state.record_activity(),
                    _ = hb_token.cancelled() => break,
                }
            }
        });

        if let Some(result) = check_turn_limits(context, &state, &params, &events) {
            turn_token.cancel();
            let _ = hb_handle.await;
            return result;
        }
        let prepared = match prepare_turn(context, &mut state, &params, &current_input, &mut events)
        {
            Ok(prepared) => prepared,
            Err(result) => {
                turn_token.cancel();
                let _ = hb_handle.await;
                return result;
            }
        };
        let streamed = stream_model_turn(context, &prepared, state.transition.as_ref()).await;
        if streamed.is_empty() {
            turn_token.cancel();
            let _ = hb_handle.await;
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
            prepared.prompt.chars().count(),
            params.max_output_tokens_recovery_limit,
        )
        .await;

        let decision = decide_next_turn(
            context,
            &mut state,
            turn_outcome,
            &mut current_input,
            &mut events,
        )
        .await;

        // Turn finished, stop heartbeat
        turn_token.cancel();
        let _ = hb_handle.await;

        match decision {
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
    let prepared_prompt = if state.turn_count == 0 && params.messages.is_empty() {
        context.compose_turn_prompt(&current_input.text())
    } else {
        context.compose_turn_prompt_from_messages(&state.messages)
    };
    let prepared = PreparedTurn {
        token_estimate: prepared_prompt.len(),
        prompt: prepared_prompt,
    };
    if prepared.prompt.contains("Output contract:") {
        state.prompt_only_output_contract_active = true;
    }

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

    if let Some(compact) = context
        .compactor
        .plan_auto_compact(prepared.token_estimate, AUTO_COMPACT_INPUT_CHAR_LIMIT)
    {
        if let Some(compact_message) = compact.plan.assistant_message.clone() {
            let compact_message = Message::assistant(compact_message);
            events.push(EngineEvent::CompactPlanIssued {
                kind: compact.plan.kind.clone(),
                message: compact.plan.notice_message.clone(),
            });
            events.push(EngineEvent::Notice {
                kind: compact.plan.notice_kind,
                message: compact.plan.notice_message,
                code: Some(ServiceFailureCode::CompactRecoveryError),
                service_failure: Some(ServiceFailureNotice {
                    service_failure_code: ServiceFailureCode::CompactRecoveryError,
                    provider_kind: None,
                    status_code: None,
                    retryable: compact.next_step != CompactServiceNextStep::Exhausted,
                    surface_visible: true,
                }),
            });
            let continue_reason = match compact.next_step {
                CompactServiceNextStep::RetryReactiveCompact => Continue::ReactiveCompactRetry,
                CompactServiceNextStep::RetryCollapseDrain => Continue::CollapseDrainRetry,
                CompactServiceNextStep::Exhausted => {
                    unreachable!("auto compact cannot terminate as exhausted")
                }
            };
            events.push(EngineEvent::Transition(continue_reason.clone()));
            events.push(EngineEvent::MessageCommitted(compact_message.clone()));
            state.transition = Some(continue_reason);
            state.has_attempted_reactive_compact = true;
            state.auto_compact_tracking = Some(compact.tracking_key.into());
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
    let model_tools = context
        .tool_registry
        .visible_model_tools(&context.app_state.permission_context);
    let mut streamed = context
        .api_client
        .stream_message_with_tools(&Message::user(prepared.prompt.clone()), &model_tools)
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
                    state.messages.push(next_input.clone());
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
            state.messages.push(next_input.clone());
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
                state.messages.push(next_input.clone());
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
                    state.messages.push(next_input.clone());
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

fn record_usage_notice(
    context: &QueryContext,
    engine_events: &mut Vec<EngineEvent>,
    usage: crate::service::api::streaming::UsageEvent,
    prompt_chars: usize,
    response_chars: usize,
) {
    context.app_state.cost_tracker.record_model_usage_detailed(
        &usage.model,
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
        prompt_chars,
        prompt_chars,
    );
    engine_events.push(EngineEvent::Notice {
        kind: "usage",
        message: format!(
            "recorded usage for model {} (input={}, output={}, prompt_chars={}, response_chars={})",
            usage.model, usage.input_tokens, usage.output_tokens, prompt_chars, response_chars
        ),
        code: None,
        service_failure: None,
    });
}

async fn consume_model_stream(
    context: &QueryContext,
    state: &mut LoopState,
    mut engine_events: Vec<EngineEvent>,
    stream_events: Vec<StreamEvent>,
    prompt_chars: usize,
    max_output_tokens_recovery_limit: usize,
) -> TurnOutcome {
    let mut aggregated_text = String::new();
    let mut pending_tool_uses: Vec<(String, String)> = Vec::new();
    let mut pending_usage: Option<crate::service::api::streaming::UsageEvent> = None;
    let mut terminal_stop_reason: Option<StopReason> = None;

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
                pending_tool_uses.push((tool_name, input));
            }
            StreamEvent::Usage(usage) => {
                pending_usage = Some(usage);
            }
            StreamEvent::MessageStop { stop_reason } => {
                terminal_stop_reason = Some(stop_reason);
            }
            StreamEvent::Error(error) => {
                if let Some(usage) = pending_usage.take() {
                    record_usage_notice(
                        context,
                        &mut engine_events,
                        usage,
                        prompt_chars,
                        aggregated_text.chars().count(),
                    );
                }
                let error_message = Message::assistant(format!("stream error: {}", error.message));
                engine_events.push(EngineEvent::MessageCommitted(error_message.clone()));
                state.messages.push(error_message);
                if !should_attempt_stream_recovery(&error) {
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

    if let Some(usage) = pending_usage.take() {
        record_usage_notice(
            context,
            &mut engine_events,
            usage,
            prompt_chars,
            aggregated_text.chars().count(),
        );
    }

    if !aggregated_text.is_empty() {
        let message = Message::assistant(aggregated_text.clone());
        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }

    match terminal_stop_reason {
        Some(StopReason::EndTurn) => {
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
        Some(StopReason::ToolUse) => {
            if pending_tool_uses.is_empty() {
                let message = "tool stop without tool payload";
                let error_message = Message::assistant(format!("stream error: {message}"));
                engine_events.push(EngineEvent::MessageCommitted(error_message.clone()));
                state.messages.push(error_message);
                return TurnOutcome {
                    state: state.clone(),
                    events: engine_events,
                    decision: TurnDecision::Return(
                        state.clone(),
                        Terminal::ModelError {
                            message: message.into(),
                            code: Some(ServiceFailureCode::ApiStreamProtocol),
                        },
                    ),
                };
            }
            if pending_tool_uses.len() == 1 {
                let (tool_name, tool_input) = pending_tool_uses.remove(0);
                return execute_tool_phase(context, state, engine_events, tool_name, tool_input)
                    .await;
            }
            return execute_tool_batch_phase(context, state, engine_events, pending_tool_uses)
                .await;
        }
        Some(StopReason::MaxTokens) => {
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
        Some(StopReason::Error) => {
            let stop_error = synthetic_stop_reason_error(state.transition.as_ref());
            if !should_attempt_stream_recovery(&stop_error) {
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
        None => {}
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

    if let crate::tool::definition::PermissionDecision::Ask { message, .. } =
        requested_permission_decision
    {
        let ask_message = if message.is_empty() {
            format!("tool {tool_name} requires approval before execution")
        } else {
            message
        };
        context
            .app_state
            .permission_context
            .set_pending_approval(Some(crate::state::permission_context::PendingApproval {
                tool_name: tool_name.clone(),
                tool_input: effective_tool_input.clone(),
                message: ask_message.clone(),
                code: None,
                summary: Some(ask_message.clone()),
                detail: None,
                approval_kind: Some("hook_ask".to_string()),
                escalation_reasons: Vec::new(),
            }));
        engine_events.push(EngineEvent::PendingApproval {
            tool_name: tool_name.clone(),
            message: ask_message.clone(),
            code: None,
            summary: ask_message,
            detail: None,
            approval_kind: Some("hook_ask".to_string()),
            escalation_reasons: Vec::new(),
            report_modifier: ToolReportModifier::Pending,
        });
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
                                "tool {tool_name} result: {} ({} chars)",
                                report.summary,
                                text.chars().count()
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
                            let follow_up =
                                tool_follow_up_message(&tool_name, &effective_tool_input, &report);
                            TurnOutcome {
                                state: state.clone(),
                                events: engine_events,
                                decision: TurnDecision::ContinueWith(
                                    Message::user(follow_up),
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
                                decision: TurnDecision::ContinueWith(
                                    Message::user(format!(
                                        "tool result for {tool_name}: denied: {reason}"
                                    )),
                                    Continue::ToolUseFollowUp,
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
                                decision: TurnDecision::ContinueWith(
                                    Message::user(format!(
                                        "tool result for {tool_name}: interrupted: {}",
                                        report_detail_or_summary(&report)
                                    )),
                                    Continue::ToolUseFollowUp,
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
                                decision: TurnDecision::ContinueWith(
                                    Message::user(format!(
                                        "tool result for {tool_name}: result too large: {}",
                                        report_detail_or_summary(&report)
                                    )),
                                    Continue::ToolUseFollowUp,
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
                decision: TurnDecision::ContinueWith(
                    Message::user(format!("tool result for {tool_name}: failure: {error}")),
                    Continue::ToolUseFollowUp,
                ),
            }
        }
    }
}

fn tool_follow_up_message(
    tool_name: &str,
    tool_input: &str,
    report: &ToolExecutionReport,
) -> String {
    let detail = report_detail_or_summary(report);
    let mut message = format!("tool result for {tool_name}: {detail}");
    if tool_name == "Read"
        && detail.contains("[Read truncated:")
        && (tool_input.contains(".jsonl")
            || tool_input.contains(".csv")
            || tool_input.contains(".log")
            || detail.contains(".jsonl")
            || detail.contains(".csv")
            || detail.contains(".log"))
    {
        message.push_str(
            "\nRuntime guidance: this is a truncated structured data/log file. Do not keep paging through the entire input with Read. Infer the schema from the sample, then use Bash or write a local script to aggregate the full file and produce the requested artifact.",
        );
    }
    if tool_name == "Write"
        && (tool_input.contains(".py")
            || tool_input.contains(".sh")
            || detail.contains(".py")
            || detail.contains(".sh"))
    {
        message.push_str(
            "\nRuntime guidance: you wrote an executable/script artifact. Next, run it with Bash, inspect the result, and verify the named target output exists before doing more input reading.",
        );
    }
    message
}

fn batch_follow_up_message(state: &LoopState, report: &ToolExecutionReport) -> String {
    let mut message = format!("tool batch result:\n{}", report_detail_or_summary(report));
    if state.prompt_only_discovery_locked
        && report
            .records
            .iter()
            .any(is_prompt_only_discovery_gate_record)
    {
        message.push_str(
            "\nRuntime gate: broad discovery is now locked for this prompt-only skill. Do not call Glob/Grep again. Answer from the evidence you already have, or issue one specific Read only if a concrete gap remains.",
        );
    }
    if should_discourage_repeated_discovery_search(report) {
        message.push_str(
            "\nRuntime guidance: you already have non-empty evidence from this tool batch. Do not repeat the same discovery/search patterns. Either answer directly from the evidence you have, or read one specific next file only if a concrete gap remains.",
        );
        if let Some(contract) = prompt_only_output_contract_from_messages(&state.messages) {
            message.push_str("\nContract reminder:\n");
            message.push_str(&contract);
            message.push_str("\nReturn the final answer now if the current evidence is sufficient.");
        }
    }
    message
}

fn prompt_only_output_contract_from_messages(messages: &[Message]) -> Option<String> {
    let first_user = messages.iter().find(|message| matches!(message.role, crate::core::message::Role::User))?;
    let text = first_user.text();
    let (_, tail) = text.split_once("Output contract:\n")?;
    let end = tail.find("\nArguments:").unwrap_or(tail.len());
    let contract = tail[..end].trim();
    (!contract.is_empty()).then(|| contract.to_string())
}

fn should_discourage_repeated_discovery_search(report: &ToolExecutionReport) -> bool {
    let mut has_non_empty_read_or_glob = false;
    let mut has_empty_discovery = false;
    for record in &report.records {
        let summary = record.summary.to_ascii_lowercase();
        let raw_detail = record.detail.as_deref().unwrap_or_default();
        let detail = raw_detail.to_ascii_lowercase();
        let combined = format!("{summary}\n{detail}");
        let detail_is_empty = raw_detail.trim().is_empty();
        match record.tool_name.as_str() {
            "Read" | "Glob" => {
                if !detail_is_empty
                    && !combined.contains("(0 chars)")
                    && !combined.contains("returned no matches")
                {
                    has_non_empty_read_or_glob = true;
                }
            }
            "Grep" => {
                if detail_is_empty
                    || combined.contains("(0 chars)")
                    || combined.contains("returned no matches")
                {
                    has_empty_discovery = true;
                }
            }
            _ => {}
        }
    }
    has_non_empty_read_or_glob && has_empty_discovery
}

fn should_lock_prompt_only_discovery(state: &LoopState, report: &ToolExecutionReport) -> bool {
    state.prompt_only_output_contract_active
        && should_discourage_repeated_discovery_search(report)
}

fn is_broad_discovery_tool(tool_name: &str) -> bool {
    matches!(tool_name, "Glob" | "Grep" | "ToolSearch")
}

fn should_gate_prompt_only_discovery(state: &LoopState, tool_name: &str) -> bool {
    state.prompt_only_discovery_locked && is_broad_discovery_tool(tool_name)
}

fn prompt_only_discovery_gate_outcome(
    tool_name: &str,
    tool_input: &str,
    batch_index: usize,
    batch_size: usize,
    executed_in_batch: bool,
) -> ToolExecutionOutcome {
    let summary = format!(
        "{tool_name} blocked by prompt-only output contract after sufficient evidence"
    );
    let detail = Some(
        "Broad discovery is locked for this prompt-only skill. Reuse the evidence you already have, or issue one specific Read only."
            .to_string(),
    );
    ToolExecutionOutcome {
        tool_name: tool_name.to_string(),
        result: crate::tool::definition::ToolResult::Interrupted(
            detail.clone().unwrap_or_else(|| summary.clone()),
        ),
        executed_in_batch,
        record: ToolExecutionRecord {
            tool_name: tool_name.to_string(),
            outcome: "Interrupted(\"prompt_only_discovery_locked\")".into(),
            kind: crate::tool::result::ToolExecutionOutcomeKind::Interrupted,
            summary,
            detail,
            pending_approval: None,
            report_modifier: ToolReportModifier::NeedsAttention,
            observable_input: Some(crate::tool::definition::ObservableInput {
                value: tool_input.trim().to_string(),
                source: crate::tool::definition::ObservableInputSource::Raw,
            }),
            batch_context: crate::tool::result::ToolBatchContext {
                batch_index,
                batch_size,
                executed_in_batch,
            },
        },
    }
}

fn is_prompt_only_discovery_gate_record(record: &ToolExecutionRecord) -> bool {
    record.summary.contains("blocked by prompt-only output contract")
}

async fn execute_tool_batch_phase(
    context: &QueryContext,
    state: &mut LoopState,
    mut engine_events: Vec<EngineEvent>,
    tool_uses: Vec<(String, String)>,
) -> TurnOutcome {
    let batch_size = tool_uses.len();
    let executed_in_batch = batch_size > 1;
    let mut requests = Vec::new();
    let mut blocked_outcomes = Vec::new();
    for (batch_index, (tool_name, tool_input)) in tool_uses.iter().enumerate() {
        if should_gate_prompt_only_discovery(state, tool_name) {
            blocked_outcomes.push(prompt_only_discovery_gate_outcome(
                tool_name,
                tool_input,
                batch_index,
                batch_size,
                executed_in_batch,
            ));
            continue;
        }
        requests.push(crate::tool::orchestrator::ToolExecutionRequest {
            call: crate::tool::definition::ToolCall::new(tool_name.clone(), tool_input.clone()),
        });
    }
    let orchestrator = crate::tool::orchestrator::ToolOrchestrator::new(&context.tool_registry);
    let tool_result = if requests.is_empty() {
        Ok(Vec::new())
    } else {
        orchestrator
            .execute(&requests, &context.app_state.permission_context)
            .await
    };

    let mut outcomes = match tool_result {
        Ok(outcomes) => outcomes,
        Err(error) => {
            let failure = Message::assistant(format!("tool batch failed: {error}"));
            engine_events.push(EngineEvent::MessageCommitted(failure.clone()));
            state.messages.push(failure);
            return TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::ContinueWith(
                    Message::user(format!("tool batch result: failure: {error}")),
                    Continue::ToolUseFollowUp,
                ),
            };
        }
    };
    outcomes.extend(blocked_outcomes);

    let records = outcomes
        .iter()
        .map(|outcome| outcome.record.clone())
        .collect::<Vec<_>>();
    let report = aggregate_execution_records(&records).unwrap_or_else(|| ToolExecutionReport {
        records,
        summary: "tool batch returned no outcomes".to_string(),
        detail: None,
        report_modifier: ToolReportModifier::NeedsAttention,
        context_modifier: ToolReportContextModifier::SetPendingToolUseSummary(
            "tool batch returned no outcomes".to_string(),
        ),
    });

    for outcome in outcomes {
        engine_events.push(tool_notice_event(&outcome.record));
        match outcome.result {
            crate::tool::definition::ToolResult::Text(text) => {
                engine_events.push(tool_result_committed_event(
                    &outcome.tool_name,
                    &text,
                    &outcome.record,
                ));
                let message = Message::assistant(format!(
                    "tool {} result: {} ({} chars)",
                    outcome.tool_name,
                    outcome.record.summary,
                    text.chars().count()
                ));
                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                state.messages.push(message);
            }
            crate::tool::definition::ToolResult::PendingApproval {
                tool_name,
                message,
                approval,
            } => {
                let pending_summary = outcome
                    .record
                    .pending_approval
                    .as_ref()
                    .map(|pending| pending.summary.clone())
                    .unwrap_or_else(|| approval.summary.clone());
                let pending_detail = outcome
                    .record
                    .pending_approval
                    .as_ref()
                    .and_then(|pending| pending.detail.clone())
                    .or_else(|| approval.detail.clone());
                let pending_code = outcome
                    .record
                    .pending_approval
                    .as_ref()
                    .and_then(|pending| pending.code.clone())
                    .or_else(|| approval.code.clone());
                let pending_kind = outcome
                    .record
                    .pending_approval
                    .as_ref()
                    .and_then(|pending| pending.approval_kind.clone())
                    .or_else(|| approval.approval_kind.clone());
                let pending_reasons = outcome
                    .record
                    .pending_approval
                    .as_ref()
                    .map(|pending| pending.escalation_reasons.clone())
                    .unwrap_or_else(|| approval.escalation_reasons.clone());
                let tool_input = requests
                    .iter()
                    .find(|request| request.call.name == tool_name)
                    .map(|request| request.call.input.clone())
                    .unwrap_or_default();
                context
                    .app_state
                    .permission_context
                    .set_pending_approval(Some(
                        crate::state::permission_context::PendingApproval {
                            tool_name: tool_name.clone(),
                            tool_input,
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
                    report_modifier: outcome.record.report_modifier.clone(),
                });
                apply_tool_report_context(state, &report);
                let approval_message = Message::assistant(format!(
                    "approval required for {tool_name}: {}",
                    pending_detail.unwrap_or(pending_summary)
                ));
                engine_events.push(EngineEvent::MessageCommitted(approval_message.clone()));
                state.messages.push(approval_message);
                return TurnOutcome {
                    state: state.clone(),
                    events: engine_events,
                    decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
                };
            }
            crate::tool::definition::ToolResult::Denied(reason) => {
                let message = Message::assistant(format!(
                    "tool {} denied: {}",
                    outcome.tool_name,
                    record_detail_or_summary(&outcome.record)
                ));
                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                state.messages.push(message);
                let follow_up = Message::assistant(format!(
                    "tool {} result missing; synthesized denial result preserved: {reason}",
                    outcome.tool_name
                ));
                engine_events.push(EngineEvent::MessageCommitted(follow_up.clone()));
                state.messages.push(follow_up);
            }
            crate::tool::definition::ToolResult::Interrupted(reason) => {
                let message = Message::assistant(format!(
                    "tool {} interrupted: {}",
                    outcome.tool_name,
                    record_detail_or_summary(&outcome.record)
                ));
                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                state.messages.push(message);
                let follow_up = Message::assistant(format!(
                    "tool {} structured failure preserved: {reason}",
                    outcome.tool_name
                ));
                engine_events.push(EngineEvent::MessageCommitted(follow_up.clone()));
                state.messages.push(follow_up);
            }
            crate::tool::definition::ToolResult::Progress(progress) => {
                let message =
                    Message::assistant(format!("tool {} progress: {progress}", outcome.tool_name));
                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                state.messages.push(message);
            }
            crate::tool::definition::ToolResult::ResultTooLarge(reason) => {
                let message = Message::assistant(format!(
                    "tool {} result too large: {}",
                    outcome.tool_name,
                    record_detail_or_summary(&outcome.record)
                ));
                engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                state.messages.push(message);
                let follow_up = Message::assistant(format!(
                    "tool {} oversized result preserved: {reason}",
                    outcome.tool_name
                ));
                engine_events.push(EngineEvent::MessageCommitted(follow_up.clone()));
                state.messages.push(follow_up);
            }
        }
    }

    if should_lock_prompt_only_discovery(state, &report) {
        state.prompt_only_discovery_locked = true;
    }
    apply_tool_report_context(state, &report);
    TurnOutcome {
        state: state.clone(),
        events: engine_events,
        decision: TurnDecision::ContinueWith(
            Message::user(batch_follow_up_message(state, &report)),
            Continue::ToolUseFollowUp,
        ),
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
    if should_return_terminal_after_recovery_exhausted(&error, state.transition.as_ref()) {
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
    let recovery = context.compactor.plan_stream_error_recovery(
        state.has_attempted_reactive_compact,
        state.transition == Some(Continue::CollapseDrainRetry),
        Some(CompactRecoveryErrorContext {
            kind: &error.kind,
            message: &error.message,
        }),
    );
    engine_events.push(EngineEvent::CompactPlanIssued {
        kind: recovery.plan.kind.clone(),
        message: recovery.plan.notice_message.clone(),
    });
    engine_events.push(EngineEvent::Notice {
        kind: recovery.plan.notice_kind,
        message: recovery.plan.notice_message.clone(),
        code: Some(ServiceFailureCode::CompactRecoveryError),
        service_failure: Some(ServiceFailureNotice {
            service_failure_code: ServiceFailureCode::CompactRecoveryError,
            provider_kind: Some(error.provider_id.clone()),
            status_code: error.status_code,
            retryable: recovery.next_step != CompactServiceNextStep::Exhausted,
            surface_visible: true,
        }),
    });
    if recovery.should_record_observability_hit {
        context
            .app_state
            .service_observability_tracker
            .record_compact_recovery_hit(&recovery.plan.kind);
    }
    state.auto_compact_tracking = Some(recovery.tracking_key.into());
    match recovery.next_step {
        CompactServiceNextStep::RetryReactiveCompact => {
            state.has_attempted_reactive_compact = true;
            TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::ContinueWith(
                    Message::user(recovery.plan.retry_prompt.expect("reactive compact prompt")),
                    Continue::ReactiveCompactRetry,
                ),
            }
        }
        CompactServiceNextStep::RetryCollapseDrain => TurnOutcome {
            state: state.clone(),
            events: engine_events,
            decision: TurnDecision::ContinueWith(
                Message::user(recovery.plan.retry_prompt.expect("collapse drain prompt")),
                Continue::CollapseDrainRetry,
            ),
        },
        CompactServiceNextStep::Exhausted => {
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
    }
}

fn should_attempt_stream_recovery(error: &StreamError) -> bool {
    matches!(
        error.disposition,
        ProviderFailureDisposition::StreamInterrupted
    )
}

fn should_return_terminal_after_recovery_exhausted(
    error: &StreamError,
    transition: Option<&Continue>,
) -> bool {
    matches!(transition, Some(Continue::CollapseDrainRetry))
        && matches!(
            error.kind.as_str(),
            "timeout" | "connection_reset" | "bad_content_type" | "empty_body" | "sse_protocol"
        )
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
    let disposition = if matches!(transition, Some(Continue::ModelFallbackRetry)) {
        ProviderFailureDisposition::StreamInterrupted
    } else {
        ProviderFailureDisposition::StreamTerminal
    };
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
        "transport" | "connection_reset" => ServiceFailureCode::ApiProviderTransport,
        "request_build" => ServiceFailureCode::ApiProviderRequestBuild,
        "invalid_response" | "empty_body" | "bad_content_type" => {
            ServiceFailureCode::ApiProviderInvalidResponse
        }
        "sse_protocol" | "tool_use_protocol" | "structured_output_invalid" => {
            ServiceFailureCode::ApiStreamProtocol
        }
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
        "sse_protocol"
        | "tool_use_protocol"
        | "structured_output_invalid"
        | "stream_stop_error" => ServiceFailureCode::ApiStreamProtocol,
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
        LoopState, QueryParams, apply_tool_report_context, batch_follow_up_message,
        classify_pre_stream_failure_code, classify_stream_failure_code, report_detail_or_summary,
        is_broad_discovery_tool, is_prompt_only_discovery_gate_record,
        prompt_only_discovery_gate_outcome, prompt_only_output_contract_from_messages,
        should_discourage_repeated_discovery_search,
        should_gate_prompt_only_discovery, should_lock_prompt_only_discovery,
        should_return_terminal_after_recovery_exhausted,
    };
    use crate::core::events::ServiceFailureCode;
    use crate::core::message::Message;
    use crate::service::api::streaming::ProviderFailureDisposition;
    use crate::tool::definition::{ObservableInput, ObservableInputSource};
    use crate::tool::result::{
        ToolBatchContext, ToolExecutionOutcomeKind, ToolExecutionRecord, ToolExecutionReport,
        ToolReportContextModifier, ToolReportModifier,
    };

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
    fn repeated_discovery_search_guidance_triggers_after_non_empty_batch() {
        let mut state = LoopState::new(&QueryParams::default());
        state.prompt_only_output_contract_active = true;
        state.messages.push(Message::user(
            "Loaded skill: collaboration-audit-handoff\nOutput contract:\n- final answer only\n- max_lines: 3\n- required_line_prefixes: 目标文件 | 改动点 | 验收标准\n- do not broaden scope beyond this contract\nArguments: 只给出 roadmap 下一步的 3 行 handoff：目标文件、改动点、验收标准",
        ));
        let report = ToolExecutionReport {
            records: vec![
                ToolExecutionRecord {
                    tool_name: "Glob".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Glob succeeded (43 chars)".into(),
                    detail: Some("./RustAgent/docs/14-progress-gap-roadmap.md".into()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: "{\"path\":\".\",\"pattern\":\"**/*roadmap*\"}".into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 2,
                        executed_in_batch: true,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Grep".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Grep succeeded (0 chars)".into(),
                    detail: Some(String::new()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: "{\"path\":\".\",\"pattern\":\"roadmap|next step\"}".into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 1,
                        batch_size: 2,
                        executed_in_batch: true,
                    },
                },
            ],
            summary: "Glob succeeded; Grep found no matches".into(),
            detail: Some("./RustAgent/docs/14-progress-gap-roadmap.md".into()),
            report_modifier: ToolReportModifier::None,
            context_modifier: ToolReportContextModifier::None,
        };

        assert!(should_discourage_repeated_discovery_search(&report));
        let follow_up = batch_follow_up_message(&state, &report);
        assert!(follow_up.contains("Do not repeat the same discovery/search patterns"));
        assert!(follow_up.contains("Contract reminder:"));
        assert!(follow_up.contains("- max_lines: 3"));
        assert!(should_lock_prompt_only_discovery(&state, &report));
    }

    #[test]
    fn repeated_discovery_search_guidance_does_not_trigger_without_non_empty_evidence() {
        let report = ToolExecutionReport {
            records: vec![ToolExecutionRecord {
                tool_name: "Grep".into(),
                outcome: "success".into(),
                kind: ToolExecutionOutcomeKind::Success,
                summary: "Grep succeeded (0 chars)".into(),
                detail: Some(String::new()),
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: None,
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: true,
                },
            }],
            summary: "Grep found no matches".into(),
            detail: None,
            report_modifier: ToolReportModifier::None,
            context_modifier: ToolReportContextModifier::None,
        };

        assert!(!should_discourage_repeated_discovery_search(&report));
    }

    #[test]
    fn repeated_discovery_search_guidance_treats_blank_grep_detail_as_empty() {
        let report = ToolExecutionReport {
            records: vec![
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded".into(),
                    detail: Some("useful evidence".into()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 2,
                        executed_in_batch: true,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Grep".into(),
                    outcome: "success".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Grep succeeded".into(),
                    detail: Some(String::new()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: None,
                    batch_context: ToolBatchContext {
                        batch_index: 1,
                        batch_size: 2,
                        executed_in_batch: true,
                    },
                },
            ],
            summary: "Read succeeded; Grep succeeded".into(),
            detail: Some("useful evidence".into()),
            report_modifier: ToolReportModifier::None,
            context_modifier: ToolReportContextModifier::None,
        };

        assert!(should_discourage_repeated_discovery_search(&report));
    }

    #[test]
    fn prompt_only_output_contract_is_extracted_from_loaded_skill_message() {
        let messages = vec![Message::user(
            "Loaded skill: summarize-skill\nOutput contract:\n- final answer only\n- max_lines: 3\nArguments: demo",
        )];
        assert_eq!(
            prompt_only_output_contract_from_messages(&messages).as_deref(),
            Some("- final answer only\n- max_lines: 3")
        );
    }

    #[test]
    fn prompt_only_discovery_gate_blocks_broad_search_after_lock() {
        let mut state = LoopState::new(&QueryParams::default());
        state.prompt_only_discovery_locked = true;
        assert!(is_broad_discovery_tool("Glob"));
        assert!(is_broad_discovery_tool("Grep"));
        assert!(is_broad_discovery_tool("ToolSearch"));
        assert!(should_gate_prompt_only_discovery(&state, "Glob"));
        assert!(!should_gate_prompt_only_discovery(&state, "Read"));
    }

    #[test]
    fn prompt_only_discovery_gate_outcome_is_marked_and_reported() {
        let blocked =
            prompt_only_discovery_gate_outcome("Glob", "{\"pattern\":\"**/*\"}", 0, 1, false);
        assert!(is_prompt_only_discovery_gate_record(&blocked.record));
        assert_eq!(
            blocked.record.kind,
            ToolExecutionOutcomeKind::Interrupted
        );
        assert_eq!(
            blocked.record.detail.as_deref(),
            Some(
                "Broad discovery is locked for this prompt-only skill. Reuse the evidence you already have, or issue one specific Read only."
            )
        );

        let mut state = LoopState::new(&QueryParams::default());
        state.prompt_only_discovery_locked = true;
        let report = ToolExecutionReport {
            records: vec![blocked.record],
            summary: "Glob blocked by prompt-only output contract after sufficient evidence"
                .into(),
            detail: Some(
                "Broad discovery is locked for this prompt-only skill. Reuse the evidence you already have, or issue one specific Read only."
                    .into(),
            ),
            report_modifier: ToolReportModifier::NeedsAttention,
            context_modifier: ToolReportContextModifier::SetPendingToolUseSummary(
                "Glob blocked by prompt-only output contract after sufficient evidence".into(),
            ),
        };
        let follow_up = batch_follow_up_message(&state, &report);
        assert!(follow_up.contains("Runtime gate: broad discovery is now locked"));
        assert!(follow_up.contains("Do not call Glob/Grep again"));
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
        assert_eq!(
            classify_pre_stream_failure_code("connection_reset", None),
            ServiceFailureCode::ApiProviderTransport
        );
        assert_eq!(
            classify_pre_stream_failure_code("empty_body", None),
            ServiceFailureCode::ApiProviderInvalidResponse
        );
        assert_eq!(
            classify_pre_stream_failure_code("bad_content_type", None),
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
                "sse_protocol",
                ProviderFailureDisposition::StreamTerminal
            ),
            ServiceFailureCode::ApiStreamProtocol
        );
        assert_eq!(
            classify_stream_failure_code(
                "provider_terminal",
                ProviderFailureDisposition::StreamTerminal
            ),
            ServiceFailureCode::ApiStreamTerminal
        );
    }

    #[test]
    fn should_return_terminal_after_recovery_exhausted_for_terminal_like_failures() {
        let timeout = crate::service::api::streaming::StreamError {
            provider_id: "anthropic".into(),
            kind: "timeout".into(),
            message: "provider request timed out".into(),
            retryable: true,
            disposition: ProviderFailureDisposition::PreStreamRetryable,
            status_code: None,
        };
        assert!(should_return_terminal_after_recovery_exhausted(
            &timeout,
            Some(&crate::core::query_loop::Continue::CollapseDrainRetry)
        ));

        let protocol = crate::service::api::streaming::StreamError {
            provider_id: "anthropic".into(),
            kind: "sse_protocol".into(),
            message: "provider returned truncated SSE frame".into(),
            retryable: false,
            disposition: ProviderFailureDisposition::StreamTerminal,
            status_code: None,
        };
        assert!(should_return_terminal_after_recovery_exhausted(
            &protocol,
            Some(&crate::core::query_loop::Continue::CollapseDrainRetry)
        ));

        let interrupted = crate::service::api::streaming::StreamError {
            provider_id: "anthropic".into(),
            kind: "provider_stream".into(),
            message: "provider overloaded".into(),
            retryable: true,
            disposition: ProviderFailureDisposition::StreamInterrupted,
            status_code: None,
        };
        assert!(!should_return_terminal_after_recovery_exhausted(
            &interrupted,
            Some(&crate::core::query_loop::Continue::CollapseDrainRetry)
        ));
    }
}

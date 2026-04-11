use crate::core::context::QueryContext;
use crate::core::events::EngineEvent;
use crate::core::message::Message;
use crate::hook::executor::{HookDecision, run_hook};
use crate::hook::registry::HookEvent;
use crate::service::api::streaming::{StopReason, StreamEvent};
use tokio::time::{Duration, timeout};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParams {
    pub messages: Vec<Message>,
    pub max_turns: Option<usize>,
    pub max_output_tokens_recovery_limit: usize,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            max_turns: Some(4),
            max_output_tokens_recovery_limit: 3,
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
    MaxTurns { count: usize },
    MaxBudget { budget_usd_cents: u64 },
    StopHookPrevented,
    AbortedStreaming,
    AbortedTools,
    ModelError(String),
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
        if let Some(max_turns) = params.max_turns {
            if state.turn_count >= max_turns {
                let count = state.turn_count;
                return finalize_with_stop_hook(
                    context,
                    state,
                    QueryLoopState::Failed,
                    Terminal::MaxTurns { count },
                    events,
                );
            }
        }

        let token_estimate = current_input.content.len();
        context
            .app_state
            .cost_tracker
            .record_request(token_estimate, 0);

        let prompt_hook = run_hook(&context.hook_registry, HookEvent::UserPromptSubmit);
        for message in prompt_hook.messages.clone() {
            events.push(EngineEvent::MessageCommitted(message.clone()));
            state.messages.push(message);
        }
        if let HookDecision::Deny(reason) = prompt_hook.decision {
            let denial = Message::assistant(format!("hook denied prompt: {reason}"));
            events.push(EngineEvent::Terminal(Terminal::ModelError(reason)));
            state.messages.push(denial);
            return QueryLoopResult {
                state: QueryLoopState::Failed,
                terminal: Terminal::ModelError("prompt denied by hook".into()),
                messages: state.messages,
                transition: state.transition,
                events,
            };
        }

        if context.compactor.should_compact(token_estimate, 512) {
            let compact_message = Message::assistant("compaction requested before continuing the turn");
            events.push(EngineEvent::Transition(Continue::ReactiveCompactRetry));
            events.push(EngineEvent::MessageCommitted(compact_message.clone()));
            state.transition = Some(Continue::ReactiveCompactRetry);
            state.has_attempted_reactive_compact = true;
            state.messages.push(compact_message);
            return finalize_with_stop_hook(
                context,
                state,
                QueryLoopState::Compacting,
                Terminal::Completed,
                events,
            );
        }

        let events_for_turn = context.api_client.stream_message(&current_input).await;
        if events_for_turn.is_empty() {
            return finalize_with_stop_hook(
                context,
                state,
                QueryLoopState::Completed,
                Terminal::Completed,
                events,
            );
        }

        let turn_outcome = consume_model_stream(context, &mut state, events, events_for_turn).await;
        events = turn_outcome.events;

        match turn_outcome.decision {
            TurnDecision::Return(loop_state, terminal) => {
                return finalize_with_stop_hook(context, loop_state, terminal_state(&terminal), terminal, events);
            }
            TurnDecision::ContinueWith(next_input, continue_reason) => {
                state = turn_outcome.state;
                state.turn_count += 1;
                state.transition = Some(continue_reason.clone());
                events.push(EngineEvent::Transition(continue_reason));
                current_input = next_input;
                state
                    .messages
                    .extend(inbox_messages(context, context.agent_id.as_deref()));
            }
            TurnDecision::AwaitMailbox => {
                if let Some(next_input) = next_worker_mailbox_message(context).await {
                    state.turn_count += 1;
                    state.transition = Some(Continue::NextTurn);
                    events.push(EngineEvent::Transition(Continue::NextTurn));
                    current_input = next_input;
                    state
                        .messages
                        .extend(inbox_messages(context, context.agent_id.as_deref()));
                    continue;
                }
                return finalize_with_stop_hook(
                    context,
                    state,
                    QueryLoopState::Completed,
                    Terminal::Completed,
                    events,
                );
            }
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
    ContinueWith(Message, Continue),
    AwaitMailbox,
}

async fn consume_model_stream(
    context: &QueryContext,
    state: &mut LoopState,
    mut engine_events: Vec<EngineEvent>,
    stream_events: Vec<StreamEvent>,
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
                        decision: TurnDecision::Return(state.clone(), Terminal::Completed),
                    };
                }
                StopReason::ToolUse => {
                    if !aggregated_text.is_empty() {
                        let message = Message::assistant(aggregated_text.clone());
                        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
                        state.messages.push(message);
                    }
                    let Some((tool_name, tool_input)) = pending_tool_use.take() else {
                        let error_message = Message::assistant("stream error: tool stop without tool payload");
                        engine_events.push(EngineEvent::MessageCommitted(error_message.clone()));
                        state.messages.push(error_message);
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::Return(
                                state.clone(),
                                Terminal::ModelError("tool stop without tool payload".into()),
                            ),
                        };
                    };
                    let tool_outcome = execute_tool_phase(context, state, engine_events, tool_name, tool_input).await;
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
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::ContinueWith(
                                Message::user("Please continue and finish the response after max output token escalation."),
                                Continue::MaxOutputTokensEscalate,
                            ),
                        };
                    }
                    if state.max_output_tokens_recovery_count < params_max_output_recovery_limit() {
                        state.max_output_tokens_recovery_count += 1;
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::ContinueWith(
                                Message::user("Please continue from where you were interrupted due to max output tokens."),
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
                    if !state.has_attempted_reactive_compact {
                        state.has_attempted_reactive_compact = true;
                        return TurnOutcome {
                            state: state.clone(),
                            events: engine_events,
                            decision: TurnDecision::ContinueWith(
                                Message::user("Retry after reactive compact / fallback recovery."),
                                Continue::ModelFallbackRetry,
                            ),
                        };
                    }
                    return TurnOutcome {
                        state: state.clone(),
                        events: engine_events,
                        decision: TurnDecision::Return(state.clone(), Terminal::ModelError("stream stopped with error".into())),
                    };
                }
            },
            StreamEvent::Error(error) => {
                let error_message = Message::assistant(format!("stream error: {error}"));
                engine_events.push(EngineEvent::MessageCommitted(error_message.clone()));
                state.messages.push(error_message);
                if !state.has_attempted_reactive_compact {
                    state.has_attempted_reactive_compact = true;
                    return TurnOutcome {
                        state: state.clone(),
                        events: engine_events,
                        decision: TurnDecision::ContinueWith(
                            Message::user("Retry after reactive compact / fallback recovery."),
                            Continue::ReactiveCompactRetry,
                        ),
                    };
                }
                return TurnOutcome {
                    state: state.clone(),
                    events: engine_events,
                    decision: TurnDecision::Return(state.clone(), Terminal::ModelError(error)),
                };
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
    for message in pre_tool_hook.messages.clone() {
        engine_events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }
    if let HookDecision::Deny(reason) = pre_tool_hook.decision {
        let denial = Message::assistant(format!("tool {tool_name} denied by hook: {reason}"));
        let post_failure_hook = run_hook(
            &context.hook_registry,
            HookEvent::PostToolUseFailure {
                tool_name: tool_name.clone(),
            },
        );
        state.messages.push(denial.clone());
        engine_events.push(EngineEvent::MessageCommitted(denial));
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

    let orchestrator = crate::tool::orchestrator::ToolOrchestrator::new(&context.tool_registry);
    let tool_result = orchestrator
        .execute(
            &[crate::tool::orchestrator::ToolExecutionRequest {
                call: crate::tool::definition::ToolCall::new(tool_name.clone(), tool_input.clone()),
            }],
            &context.app_state.permission_context,
        )
        .await;

    match tool_result {
        Ok(outcomes) => match outcomes.into_iter().next().map(|outcome| outcome.result) {
            Some(crate::tool::definition::ToolResult::Text(text)) => {
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
                let tool_message = Message::assistant(format!("tool {tool_name} result: {text}"));
                engine_events.push(EngineEvent::ToolResultCommitted {
                    tool_name: tool_name.clone(),
                    content: text.clone(),
                });
                engine_events.push(EngineEvent::MessageCommitted(tool_message.clone()));
                state.messages.push(tool_message);
                if post_tool_hook.prevent_continuation {
                    state.stop_hook_active = true;
                    state.transition = Some(Continue::StopHookBlocking);
                    return TurnOutcome {
                        state: state.clone(),
                        events: engine_events,
                        decision: TurnDecision::Return(state.clone(), Terminal::StopHookPrevented),
                    };
                }
                state.pending_tool_use_summary = Some(format!("tool {tool_name} result committed"));
                TurnOutcome {
                    state: state.clone(),
                    events: engine_events,
                    decision: TurnDecision::ContinueWith(
                        Message::user(format!("tool result for {tool_name}: {text}")),
                        Continue::ToolUseFollowUp,
                    ),
                }
            }
            Some(crate::tool::definition::ToolResult::Denied(reason)) => {
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
                let denial = Message::assistant(format!("tool {tool_name} denied: {reason}"));
                engine_events.push(EngineEvent::MessageCommitted(denial.clone()));
                state.messages.push(denial);
                TurnOutcome {
                    state: state.clone(),
                    events: engine_events,
                    decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
                }
            }
            None => TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::Return(
                    state.clone(),
                    Terminal::ModelError("tool orchestrator returned no outcome".into()),
                ),
            },
        },
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
            let failure = Message::assistant(format!("tool {tool_name} failed: {error}"));
            engine_events.push(EngineEvent::MessageCommitted(failure.clone()));
            state.messages.push(failure);
            TurnOutcome {
                state: state.clone(),
                events: engine_events,
                decision: TurnDecision::Return(state.clone(), Terminal::AbortedTools),
            }
        }
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
                .map(|event| crate::core::engine::QueryEngine::format_task_event_message(&event))
                .collect()
        })
        .unwrap_or_default()
}

async fn next_worker_mailbox_message(context: &QueryContext) -> Option<Message> {
    let agent_id = context.agent_id.as_deref()?;
    let manager = context.app_state.permission_context.task_manager.as_ref()?;
    timeout(
        Duration::from_millis(100),
        manager.wait_for_mailbox_message(agent_id),
    )
    .await
    .ok()
    .flatten()
    .map(Message::user)
}

fn finalize_with_stop_hook(
    context: &QueryContext,
    mut state: LoopState,
    default_state: QueryLoopState,
    default_terminal: Terminal,
    mut events: Vec<EngineEvent>,
) -> QueryLoopResult {
    state
        .messages
        .extend(inbox_messages(context, context.agent_id.as_deref()));
    let stop_event = if context.is_subagent() {
        HookEvent::SubagentStop
    } else {
        HookEvent::Stop
    };
    let stop_hook = run_hook(&context.hook_registry, stop_event);
    for message in stop_hook.messages {
        events.push(EngineEvent::MessageCommitted(message.clone()));
        state.messages.push(message);
    }

    let terminal = if stop_hook.prevent_continuation {
        Terminal::StopHookPrevented
    } else {
        default_terminal
    };
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
        Terminal::MaxTurns { .. } | Terminal::MaxBudget { .. } | Terminal::ModelError(_) => {
            QueryLoopState::Failed
        }
        Terminal::AbortedStreaming | Terminal::AbortedTools => QueryLoopState::Interrupted,
    }
}

trait QueryLoopStateExt {
    fn or(self, fallback: QueryLoopState) -> QueryLoopState;
}

impl QueryLoopStateExt for QueryLoopState {
    fn or(self, _fallback: QueryLoopState) -> QueryLoopState {
        self
    }
}

fn params_max_output_recovery_limit() -> usize {
    3
}

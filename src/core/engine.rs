use crate::core::context::QueryContext;
use crate::core::events::{
    EngineEvent, RuntimeEventEnvelope, RuntimeEventKind, ServiceFailureCode, ServiceFailureNotice,
    SessionMilestone,
};
use crate::core::message::Message;
use crate::core::query_loop::{
    QueryLoopEventSink, QueryLoopResult, QueryLoopState, QueryParams, Terminal,
    run_query_loop_with_params_and_sink,
};
use crate::history::session::SessionHistoryEntry;
use crate::state::app_state::SessionPersistFailure;
use crate::task::types::TaskEvent;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const STREAM_TURN_BUFFER: usize = 64;

fn record_runtime_observability(context: &QueryContext, runtime: &RuntimeEventEnvelope) {
    if let Some(service_failure) = runtime.service_failure.as_ref() {
        context
            .app_state
            .service_observability_tracker
            .record_service_failure(service_failure);
    }
}

fn record_persistence_failure(
    context: &QueryContext,
    phase: &str,
    error: &SessionPersistFailure,
) -> String {
    let reason = error.reason();
    context
        .app_state
        .service_observability_tracker
        .record_runtime_lifecycle_failure(
            phase,
            &reason,
            &context.app_state.active_session_id,
            1,
        );
    tracing::error!(
        "session persistence failed: phase={} session_id={} reason={}",
        phase,
        context.app_state.active_session_id,
        reason
    );
    reason
}

struct StreamingTurnState {
    context: QueryContext,
    surface_tx: mpsc::UnboundedSender<EngineEvent>,
    emitted_events: Vec<EngineEvent>,
    store_present: bool,
}

impl StreamingTurnState {
    fn new(context: QueryContext, surface_tx: mpsc::UnboundedSender<EngineEvent>) -> Self {
        Self {
            store_present: context.app_state.session_store.is_some(),
            context,
            surface_tx,
            emitted_events: Vec::new(),
        }
    }

    fn persist_user_input(&mut self, input: &Message) {
        let entry = SessionHistoryEntry {
            message: input.clone(),
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: Some(SessionMilestone::UserInputCommitted),
        };
        self.append_history_entry("engine.persist_user_input", entry, SessionMilestone::UserInputCommitted);
    }

    fn handle_query_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::MessageCommitted(message) => {
                self.emit(EngineEvent::MessageCommitted(message.clone()));
                self.append_history_entry(
                    "engine.persist_assistant_message",
                    SessionHistoryEntry {
                        message,
                        timestamp: None,
                        tool_refs: Vec::new(),
                        milestone: Some(SessionMilestone::AssistantMessageCommitted),
                    },
                    SessionMilestone::AssistantMessageCommitted,
                );
            }
            EngineEvent::ToolResultCommitted {
                tool_name,
                content,
                summary,
                detail,
                kind,
                report_modifier,
            } => {
                self.emit(EngineEvent::ToolResultCommitted {
                    tool_name: tool_name.clone(),
                    content: content.clone(),
                    summary: summary.clone(),
                    detail: detail.clone(),
                    kind: kind.clone(),
                    report_modifier: report_modifier.clone(),
                });
                self.append_history_entry(
                    "engine.persist_tool_result",
                    SessionHistoryEntry {
                        message: Message::assistant(format!(
                            "tool {tool_name} result: {}",
                            detail.clone().unwrap_or_else(|| summary.clone())
                        )),
                        timestamp: None,
                        tool_refs: vec![tool_name],
                        milestone: Some(SessionMilestone::ToolResultCommitted),
                    },
                    SessionMilestone::ToolResultCommitted,
                );
            }
            EngineEvent::CompactPlanIssued { kind, message } => {
                let runtime = runtime_event_for_compact_plan(
                    &kind,
                    &message,
                    Some(ServiceFailureCode::CompactRecoveryError),
                );
                record_runtime_observability(&self.context, &runtime);
                self.emit(EngineEvent::RuntimeEvent(runtime));
                self.emit(EngineEvent::CompactPlanIssued { kind, message });
            }
            EngineEvent::Terminal(terminal) => {
                let runtime = runtime_event_for_terminal(&terminal);
                record_runtime_observability(&self.context, &runtime);
                self.emit(EngineEvent::RuntimeEvent(runtime));
                self.emit(EngineEvent::Terminal(terminal));
                if self.store_present {
                    self.emit(EngineEvent::SessionMilestoneWritten(
                        SessionMilestone::TurnCompleted,
                    ));
                }
            }
            EngineEvent::Transition(transition) => {
                let runtime = runtime_event_for_transition(&transition);
                record_runtime_observability(&self.context, &runtime);
                self.emit(EngineEvent::RuntimeEvent(runtime));
                self.emit(EngineEvent::Transition(transition));
            }
            other => self.emit(other),
        }
    }

    fn append_history_entry(
        &mut self,
        phase: &str,
        entry: SessionHistoryEntry,
        milestone: SessionMilestone,
    ) {
        match self.context.app_state.append_current_session_history_entry(entry) {
            Ok(()) => {
                if self.store_present {
                    self.emit(EngineEvent::SessionMilestoneWritten(milestone));
                }
            }
            Err(error) => {
                let reason = record_persistence_failure(&self.context, phase, &error);
                self.emit(EngineEvent::Notice {
                    kind: "persistence",
                    message: format!("session history append failed during {phase}: {reason}"),
                    code: None,
                    service_failure: None,
                });
            }
        }
    }

    fn emit(&mut self, event: EngineEvent) {
        self.emitted_events.push(event.clone());
        let _ = self.surface_tx.send(event);
    }
}

struct StreamingTurnHandle {
    receiver: mpsc::Receiver<EngineEvent>,
    completion: oneshot::Receiver<QueryLoopResult>,
}

#[derive(Debug, Clone)]
pub struct QueryEngine {
    pub context: QueryContext,
}

impl QueryEngine {
    pub fn new(context: QueryContext) -> Self {
        Self { context }
    }

    pub async fn submit_message(&mut self, input: Message) -> Vec<Message> {
        self.submit_turn(input).await.messages
    }

    pub async fn submit_message_events(&mut self, input: Message) -> Vec<EngineEvent> {
        self.submit_turn(input).await.events
    }

    pub async fn stream_turn(&mut self, input: Message) -> mpsc::Receiver<EngineEvent> {
        self.start_turn(input).receiver
    }

    pub async fn submit_turn(&mut self, input: Message) -> QueryLoopResult {
        let fallback_input = input.clone();
        let StreamingTurnHandle {
            mut receiver,
            completion,
        } = self.start_turn(input);
        let mut events = Vec::new();
        while let Some(event) = receiver.recv().await {
            events.push(event);
        }
        match completion.await {
            Ok(mut result) => {
                result.events = events;
                result
            }
            Err(_) => QueryLoopResult {
                state: QueryLoopState::Failed,
                terminal: Terminal::ModelError {
                    message: "streaming turn task terminated unexpectedly".into(),
                    code: None,
                },
                messages: vec![fallback_input],
                transition: None,
                events,
            },
        }
    }

    pub fn drain_task_events(&self) -> Vec<TaskEvent> {
        self.context
            .app_state
            .permission_context
            .task_manager
            .as_ref()
            .map(|manager| manager.drain_events(&self.context.app_state.active_session_id))
            .unwrap_or_default()
    }

    pub fn persist_messages(
        &mut self,
        input: Message,
        messages: &[Message],
        milestone: SessionMilestone,
    ) {
        let mut entries = vec![SessionHistoryEntry {
            message: input,
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: Some(SessionMilestone::UserInputCommitted),
        }];
        entries.extend(messages.iter().cloned().map(|message| SessionHistoryEntry {
            message,
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: Some(milestone.clone()),
        }));
        if let Err(error) = self
            .context
            .app_state
            .append_current_session_history_entries(entries)
        {
            let _ = record_persistence_failure(&self.context, "engine.persist_messages", &error);
        }
    }

    pub fn format_task_event_message(event: &TaskEvent) -> Message {
        Message::assistant(event.format_notification())
    }

    fn query_params_for_input(&self, input: &Message) -> QueryParams {
        let mut params = query_params_for_input(input);
        params.messages = self
            .context
            .app_state
            .canonical_session_history_entries()
            .into_iter()
            .map(|entry| entry.message)
            .collect();
        params
    }

    fn start_turn(&mut self, input: Message) -> StreamingTurnHandle {
        let params = self.query_params_for_input(&input);
        let mut context = self.context.clone();
        let turn_cancellation = self.context.app_state.cancellation_token.child_token();
        context.app_state.cancellation_token = turn_cancellation.clone();
        context.app_state.permission_context = context
            .app_state
            .permission_context
            .clone()
            .with_cancellation_token(turn_cancellation.clone());
        let background_input = input.clone();
        let (surface_tx, surface_rx) = mpsc::channel(STREAM_TURN_BUFFER);
        let (bridge_tx, mut bridge_rx) = mpsc::unbounded_channel();
        let (completion_tx, completion_rx) = oneshot::channel();

        let bridge_cancellation = turn_cancellation.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = surface_tx.closed() => {
                        bridge_cancellation.cancel();
                        break;
                    }
                    maybe_event = bridge_rx.recv() => {
                        match maybe_event {
                            Some(event) => {
                                if surface_tx.send(event).await.is_err() {
                                    bridge_cancellation.cancel();
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            let stream_state = Arc::new(Mutex::new(StreamingTurnState::new(
                context.clone(),
                bridge_tx,
            )));
            {
                let mut state = stream_state
                    .lock()
                    .expect("streaming turn state should not be poisoned");
                state.persist_user_input(&background_input);
            }
            let sink: QueryLoopEventSink = {
                let stream_state = Arc::clone(&stream_state);
                Arc::new(move |event| {
                    let mut state = stream_state
                        .lock()
                        .expect("streaming turn state should not be poisoned");
                    state.handle_query_event(event);
                })
            };
            let mut result =
                run_query_loop_with_params_and_sink(
                    &context,
                    background_input,
                    params,
                    Some(sink),
                    Some(turn_cancellation),
                )
                .await;
            let mut state = stream_state
                .lock()
                .expect("streaming turn state should not be poisoned");
            result.events = std::mem::take(&mut state.emitted_events);
            let _ = completion_tx.send(result);
        });

        StreamingTurnHandle {
            receiver: surface_rx,
            completion: completion_rx,
        }
    }
}

fn query_params_for_input(input: &Message) -> QueryParams {
    if should_extend_turn_budget_for_skill(input) {
        QueryParams {
            max_turns: Some(8),
            ..QueryParams::default()
        }
    } else {
        QueryParams::default()
    }
}

fn should_extend_turn_budget_for_skill(input: &Message) -> bool {
    let text = input.text();
    text.starts_with("Loaded skill:") || text.starts_with("Skill workflow:")
}

fn runtime_event_for_transition(
    transition: &crate::core::query_loop::Continue,
) -> RuntimeEventEnvelope {
    let (kind, code) = match transition {
        crate::core::query_loop::Continue::ReactiveCompactRetry
        | crate::core::query_loop::Continue::CollapseDrainRetry => (
            RuntimeEventKind::RetryScheduled,
            Some(ServiceFailureCode::CompactRecoveryError),
        ),
        crate::core::query_loop::Continue::ModelFallbackRetry => (
            RuntimeEventKind::RetryScheduled,
            Some(ServiceFailureCode::ApiStreamModelFallback),
        ),
        crate::core::query_loop::Continue::MaxOutputTokensEscalate
        | crate::core::query_loop::Continue::MaxOutputTokensRecovery
        | crate::core::query_loop::Continue::TokenBudgetContinuation => {
            (RuntimeEventKind::RetryScheduled, None)
        }
        crate::core::query_loop::Continue::StopHookBlocking => {
            (RuntimeEventKind::StopHookBlocking, None)
        }
        crate::core::query_loop::Continue::NextTurn
        | crate::core::query_loop::Continue::ToolUseFollowUp => {
            (RuntimeEventKind::NormalTerminal, None)
        }
    };
    let service_failure = code
        .clone()
        .map(|service_failure_code| ServiceFailureNotice {
            service_failure_code,
            provider_kind: None,
            status_code: None,
            retryable: matches!(
                transition,
                crate::core::query_loop::Continue::ReactiveCompactRetry
                    | crate::core::query_loop::Continue::CollapseDrainRetry
                    | crate::core::query_loop::Continue::ModelFallbackRetry
            ),
            surface_visible: true,
        });
    RuntimeEventEnvelope {
        kind,
        detail: transition.as_str().into(),
        code,
        service_failure,
    }
}

fn runtime_event_for_terminal(
    terminal: &crate::core::query_loop::Terminal,
) -> RuntimeEventEnvelope {
    let (kind, code) = match terminal {
        crate::core::query_loop::Terminal::Completed => (RuntimeEventKind::NormalTerminal, None),
        crate::core::query_loop::Terminal::StopHookPrevented => {
            (RuntimeEventKind::StopHookPrevented, None)
        }
        crate::core::query_loop::Terminal::ModelError { code, .. } => {
            (RuntimeEventKind::ModelError, code.clone())
        }
        crate::core::query_loop::Terminal::MaxTurns { .. }
        | crate::core::query_loop::Terminal::MaxBudget { .. }
        | crate::core::query_loop::Terminal::AbortedStreaming
        | crate::core::query_loop::Terminal::AbortedTools => {
            (RuntimeEventKind::RetryScheduled, None)
        }
    };
    let service_failure = match terminal {
        crate::core::query_loop::Terminal::ModelError {
            code: Some(service_failure_code),
            ..
        } => Some(ServiceFailureNotice {
            service_failure_code: service_failure_code.clone(),
            provider_kind: None,
            status_code: None,
            retryable: false,
            surface_visible: true,
        }),
        _ => None,
    };
    RuntimeEventEnvelope {
        kind,
        detail: terminal.as_str().into(),
        code,
        service_failure,
    }
}

fn runtime_event_for_compact_plan(
    kind: &crate::service::compact::CompactPlanKind,
    message: &str,
    code: Option<ServiceFailureCode>,
) -> RuntimeEventEnvelope {
    let service_failure = code
        .clone()
        .map(|service_failure_code| ServiceFailureNotice {
            service_failure_code,
            provider_kind: None,
            status_code: None,
            retryable: true,
            surface_visible: true,
        });
    RuntimeEventEnvelope {
        kind: RuntimeEventKind::CompactPlan,
        detail: format!("{:?}: {}", kind, message),
        code,
        service_failure,
    }
}

#[cfg(test)]
mod tests {
    use super::{query_params_for_input, should_extend_turn_budget_for_skill};
    use crate::core::message::Message;
    use crate::core::query_loop::QueryParams;

    #[test]
    fn skill_loaded_prompt_gets_extended_turn_budget() {
        let input = Message::user("Loaded skill: summarize-skill\nArguments: src");
        let params = query_params_for_input(&input);
        assert_eq!(params.max_turns, Some(8));
        assert!(should_extend_turn_budget_for_skill(&input));
    }

    #[test]
    fn regular_user_prompt_keeps_default_turn_budget() {
        let input = Message::user("Summarize the roadmap");
        let params = query_params_for_input(&input);
        assert_eq!(params.max_turns, QueryParams::default().max_turns);
        assert!(!should_extend_turn_budget_for_skill(&input));
    }
}

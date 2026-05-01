use crate::core::context::QueryContext;
use crate::core::events::{
    EngineEvent, RuntimeEventEnvelope, RuntimeEventKind, ServiceFailureCode, ServiceFailureNotice,
    SessionMilestone,
};
use crate::core::message::Message;
use crate::core::query_loop::{QueryLoopResult, QueryParams, run_query_loop_with_params};
use crate::history::session::{SessionHistoryEntry, SessionId};
use crate::task::types::TaskEvent;
use tokio::sync::mpsc;

fn record_runtime_observability(context: &QueryContext, runtime: &RuntimeEventEnvelope) {
    if let Some(service_failure) = runtime.service_failure.as_ref() {
        context
            .app_state
            .service_observability_tracker
            .record_service_failure(service_failure);
    }
}

#[derive(Debug, Clone)]
pub struct QueryEngine {
    pub context: QueryContext,
}

impl QueryEngine {
    pub fn new(context: QueryContext) -> Self {
        Self { context }
    }

    pub async fn submit_message(&self, input: Message) -> Vec<Message> {
        self.submit_turn(input).await.messages
    }

    pub async fn submit_message_events(&self, input: Message) -> Vec<EngineEvent> {
        self.submit_turn(input).await.events
    }

    pub async fn stream_turn(&self, input: Message) -> mpsc::Receiver<EngineEvent> {
        let result = self.submit_turn(input).await;
        let (tx, rx) = mpsc::channel(result.events.len().max(1));
        for event in result.events {
            if tx.send(event).await.is_err() {
                break;
            }
        }
        rx
    }

    pub async fn submit_turn(&self, input: Message) -> QueryLoopResult {
        let user_input = input.clone();
        let mut result = run_query_loop_with_params(
            &self.context,
            input,
            query_params_for_input(&user_input),
        )
        .await;
        result.events = self.persist_turn(user_input, result.events.clone());
        result
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
        &self,
        input: Message,
        messages: &[Message],
        milestone: SessionMilestone,
    ) {
        let Some(session_store) = &self.context.app_state.session_store else {
            return;
        };
        let session_id = SessionId(self.context.app_state.active_session_id.clone());
        let _ = session_store.append_entry(
            &session_id,
            SessionHistoryEntry {
                message: input,
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: Some(SessionMilestone::UserInputCommitted),
            },
        );
        for message in messages {
            let _ = session_store.append_entry(
                &session_id,
                SessionHistoryEntry {
                    message: message.clone(),
                    timestamp: None,
                    tool_refs: Vec::new(),
                    milestone: Some(milestone.clone()),
                },
            );
        }
    }

    fn persist_turn(&self, input: Message, events: Vec<EngineEvent>) -> Vec<EngineEvent> {
        let session_store = self.context.app_state.session_store.as_ref();
        let session_id = SessionId(self.context.app_state.active_session_id.clone());
        let mut persisted_events = Vec::new();
        let compact_plan_code = compact_plan_code_from_events(&events);

        if let Some(session_store) = session_store {
            let _ = session_store.append_entry(
                &session_id,
                SessionHistoryEntry {
                    message: input,
                    timestamp: None,
                    tool_refs: Vec::new(),
                    milestone: Some(SessionMilestone::UserInputCommitted),
                },
            );
            persisted_events.push(EngineEvent::SessionMilestoneWritten(
                SessionMilestone::UserInputCommitted,
            ));
        }

        for event in events {
            match &event {
                EngineEvent::MessageCommitted(message) => {
                    if let Some(session_store) = session_store {
                        let _ = session_store.append_entry(
                            &session_id,
                            SessionHistoryEntry {
                                message: message.clone(),
                                timestamp: None,
                                tool_refs: Vec::new(),
                                milestone: Some(SessionMilestone::AssistantMessageCommitted),
                            },
                        );
                        persisted_events.push(event.clone());
                        persisted_events.push(EngineEvent::SessionMilestoneWritten(
                            SessionMilestone::AssistantMessageCommitted,
                        ));
                    } else {
                        persisted_events.push(event.clone());
                    }
                }
                EngineEvent::ToolResultCommitted {
                    tool_name,
                    content,
                    summary,
                    detail,
                    kind,
                    report_modifier,
                } => {
                    if let Some(session_store) = session_store {
                        let _ = session_store.append_entry(
                            &session_id,
                            SessionHistoryEntry {
                                message: Message::assistant(format!(
                                    "tool {tool_name} result: {}",
                                    detail.clone().unwrap_or_else(|| summary.clone())
                                )),
                                timestamp: None,
                                tool_refs: vec![tool_name.clone()],
                                milestone: Some(SessionMilestone::ToolResultCommitted),
                            },
                        );
                        persisted_events.push(EngineEvent::ToolResultCommitted {
                            tool_name: tool_name.clone(),
                            content: content.clone(),
                            summary: summary.clone(),
                            detail: detail.clone(),
                            kind: kind.clone(),
                            report_modifier: report_modifier.clone(),
                        });
                        persisted_events.push(EngineEvent::SessionMilestoneWritten(
                            SessionMilestone::ToolResultCommitted,
                        ));
                    } else {
                        persisted_events.push(event.clone());
                    }
                }
                EngineEvent::CompactPlanIssued { kind, message } => {
                    let runtime =
                        runtime_event_for_compact_plan(kind, message, compact_plan_code.clone());
                    record_runtime_observability(&self.context, &runtime);
                    persisted_events.push(EngineEvent::RuntimeEvent(runtime));
                    persisted_events.push(event.clone());
                }
                EngineEvent::Terminal(terminal) => {
                    let runtime = runtime_event_for_terminal(terminal);
                    record_runtime_observability(&self.context, &runtime);
                    persisted_events.push(EngineEvent::RuntimeEvent(runtime));
                    persisted_events.push(event.clone());
                    if session_store.is_some() {
                        persisted_events.push(EngineEvent::SessionMilestoneWritten(
                            SessionMilestone::TurnCompleted,
                        ));
                    }
                }
                EngineEvent::Transition(transition) => {
                    let runtime = runtime_event_for_transition(transition);
                    record_runtime_observability(&self.context, &runtime);
                    persisted_events.push(EngineEvent::RuntimeEvent(runtime));
                    persisted_events.push(event.clone());
                }
                _ => persisted_events.push(event.clone()),
            }
        }

        persisted_events
    }

    pub fn format_task_event_message(event: &TaskEvent) -> Message {
        Message::assistant(event.format_notification())
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

fn compact_plan_code_from_events(events: &[EngineEvent]) -> Option<ServiceFailureCode> {
    events.iter().find_map(|event| match event {
        EngineEvent::CompactPlanIssued { .. } => Some(ServiceFailureCode::CompactRecoveryError),
        _ => None,
    })
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

use crate::core::context::QueryContext;
use crate::core::events::{
    EngineEvent, RuntimeEventEnvelope, RuntimeEventKind, SessionMilestone,
};
use crate::core::message::Message;
use crate::core::query_loop::{QueryLoopResult, run_query_loop};
use crate::history::session::{SessionHistoryEntry, SessionId};
use crate::task::types::TaskEvent;
use tokio::sync::mpsc;

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
        let mut result = run_query_loop(&self.context, input).await;
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
        session_store.append_entry(
            &session_id,
            SessionHistoryEntry {
                message: input,
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: Some(SessionMilestone::UserInputCommitted),
            },
        );
        for message in messages {
            session_store.append_entry(
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
        let Some(session_store) = &self.context.app_state.session_store else {
            return events;
        };
        let session_id = SessionId(self.context.app_state.active_session_id.clone());
        let mut persisted_events = Vec::new();

        session_store.append_entry(
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

        for event in events {
            match &event {
                EngineEvent::MessageCommitted(message) => {
                    session_store.append_entry(
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
                }
                EngineEvent::ToolResultCommitted { tool_name, content } => {
                    session_store.append_entry(
                        &session_id,
                        SessionHistoryEntry {
                            message: Message::assistant(format!(
                                "tool {tool_name} result: {content}"
                            )),
                            timestamp: None,
                            tool_refs: vec![tool_name.clone()],
                            milestone: Some(SessionMilestone::ToolResultCommitted),
                        },
                    );
                    persisted_events.push(event.clone());
                    persisted_events.push(EngineEvent::SessionMilestoneWritten(
                        SessionMilestone::ToolResultCommitted,
                    ));
                }
                EngineEvent::CompactPlanIssued { .. } => {
                    persisted_events.push(event.clone());
                }
                EngineEvent::Terminal(terminal) => {
                    persisted_events.push(EngineEvent::RuntimeEvent(runtime_event_for_terminal(
                        terminal,
                    )));
                    persisted_events.push(event.clone());
                    persisted_events.push(EngineEvent::SessionMilestoneWritten(
                        SessionMilestone::TurnCompleted,
                    ));
                }
                EngineEvent::Transition(transition) => {
                    persisted_events.push(EngineEvent::RuntimeEvent(runtime_event_for_transition(
                        transition,
                    )));
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

use crate::core::context::QueryContext;
use crate::core::events::EngineEvent;
use crate::core::message::Message;
use crate::core::query_loop::{QueryLoopResult, run_query_loop};
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
        run_query_loop(&self.context, input).await
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

    pub fn format_task_event_message(event: &TaskEvent) -> Message {
        let next_action = match event.status {
            crate::task::types::TaskStatus::Running => {
                format!(
                    "use SendMessage with input '{}:<message>' to continue this task",
                    event.task_id
                )
            }
            _ => format!(
                "use TaskOutput with input '{}:0' to inspect task output",
                event.task_id
            ),
        };
        Message::assistant(format!(
            "[task] id: {}\n[task] summary: {}\n[task] status: {:?}\n[task] output: {}\n[task] next_action: {}",
            event.task_id, event.summary, event.status, event.output_file, next_action
        ))
    }
}

use crate::core::context::QueryContext;
use crate::core::message::Message;
use crate::core::query_loop::{QueryLoopResult, run_query_loop};
use crate::task::types::TaskEvent;

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
        Message::assistant(format!(
            "[task] {}\n[task] status: {:?}\n[task] output: {}",
            event.summary, event.status, event.output_file
        ))
    }
}

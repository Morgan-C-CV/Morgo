use crate::core::context::QueryContext;
use crate::core::message::Message;
use crate::core::query_loop::{QueryLoopResult, run_query_loop};

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
}

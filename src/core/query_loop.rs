use crate::core::context::QueryContext;
use crate::core::message::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryLoopAction {
    Continue,
    Completed,
}

pub async fn run_query_loop(_context: &QueryContext, input: Message) -> Vec<Message> {
    vec![Message::assistant(format!(
        "stub response: {}",
        input.content
    ))]
}

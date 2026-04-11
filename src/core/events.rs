use crate::core::message::Message;
use crate::core::query_loop::{Continue, Terminal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    AssistantDelta(String),
    MessageCommitted(Message),
    ToolCallStarted { tool_name: String, input: String },
    ToolResultCommitted { tool_name: String, content: String },
    Transition(Continue),
    Terminal(Terminal),
}

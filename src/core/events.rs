use crate::core::message::Message;
use crate::core::query_loop::{Continue, Terminal};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SessionMilestone {
    UserInputCommitted,
    AssistantMessageCommitted,
    ToolResultCommitted,
    TurnCompleted,
}

impl SessionMilestone {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UserInputCommitted => "user_input_committed",
            Self::AssistantMessageCommitted => "assistant_message_committed",
            Self::ToolResultCommitted => "tool_result_committed",
            Self::TurnCompleted => "turn_completed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    AssistantDelta(String),
    MessageCommitted(Message),
    ToolCallStarted { tool_name: String, input: String },
    ToolResultCommitted { tool_name: String, content: String },
    PendingApproval { tool_name: String, message: String },
    Notice { kind: &'static str, message: String },
    Transition(Continue),
    Terminal(Terminal),
    SessionMilestoneWritten(SessionMilestone),
}

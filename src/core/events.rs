use crate::core::message::Message;
use crate::core::query_loop::{Continue, Terminal};
use crate::service::compact::CompactPlanKind;
use crate::tool::result::{ToolExecutionOutcomeKind, ToolReportModifier};

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
pub enum RuntimeEventKind {
    NormalTerminal,
    RetryScheduled,
    ModelError,
    StopHookPrevented,
    StopHookBlocking,
    CompactPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEventEnvelope {
    pub kind: RuntimeEventKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    AssistantDelta(String),
    MessageCommitted(Message),
    ToolCallStarted { tool_name: String, input: String },
    ToolResultCommitted {
        tool_name: String,
        content: String,
        summary: String,
        detail: Option<String>,
        kind: ToolExecutionOutcomeKind,
        report_modifier: ToolReportModifier,
    },
    PendingApproval {
        tool_name: String,
        message: String,
        summary: String,
        detail: Option<String>,
        report_modifier: ToolReportModifier,
    },
    Notice { kind: &'static str, message: String },
    CompactPlanIssued {
        kind: CompactPlanKind,
        message: String,
    },
    Transition(Continue),
    RuntimeEvent(RuntimeEventEnvelope),
    Terminal(Terminal),
    SessionMilestoneWritten(SessionMilestone),
}

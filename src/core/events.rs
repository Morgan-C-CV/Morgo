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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ServiceFailureCode {
    ApiStreamError,
    ApiProviderError,
    McpRuntimeError,
    CompactRecoveryError,
}

impl ServiceFailureCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ApiStreamError => "api_stream_error",
            Self::ApiProviderError => "api_provider_error",
            Self::McpRuntimeError => "mcp_runtime_error",
            Self::CompactRecoveryError => "compact_recovery_error",
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
    pub code: Option<ServiceFailureCode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    AssistantDelta(String),
    MessageCommitted(Message),
    ToolCallStarted {
        tool_name: String,
        input: String,
    },
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
        code: Option<String>,
        summary: String,
        detail: Option<String>,
        approval_kind: Option<String>,
        escalation_reasons: Vec<String>,
        report_modifier: ToolReportModifier,
    },
    Notice {
        kind: &'static str,
        message: String,
        code: Option<ServiceFailureCode>,
    },
    CompactPlanIssued {
        kind: CompactPlanKind,
        message: String,
    },
    Transition(Continue),
    RuntimeEvent(RuntimeEventEnvelope),
    Terminal(Terminal),
    SessionMilestoneWritten(SessionMilestone),
}

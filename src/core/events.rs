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
    ApiProviderHttp4xx,
    ApiProviderHttp429,
    ApiProviderHttp5xx,
    ApiProviderTransport,
    ApiProviderTimeout,
    ApiProviderRequestBuild,
    ApiProviderInvalidResponse,
    ApiStreamModelFallback,
    ApiStreamOverloaded,
    ApiStreamInterrupted,
    ApiStreamProtocol,
    ApiStreamTerminal,
    McpRuntimeError,
    CompactRecoveryError,
}

impl ServiceFailureCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ApiProviderHttp4xx => "api_provider_http_4xx",
            Self::ApiProviderHttp429 => "api_provider_http_429",
            Self::ApiProviderHttp5xx => "api_provider_http_5xx",
            Self::ApiProviderTransport => "api_provider_transport",
            Self::ApiProviderTimeout => "api_provider_timeout",
            Self::ApiProviderRequestBuild => "api_provider_request_build",
            Self::ApiProviderInvalidResponse => "api_provider_invalid_response",
            Self::ApiStreamModelFallback => "api_stream_model_fallback",
            Self::ApiStreamOverloaded => "api_stream_overloaded",
            Self::ApiStreamInterrupted => "api_stream_interrupted",
            Self::ApiStreamProtocol => "api_stream_protocol",
            Self::ApiStreamTerminal => "api_stream_terminal",
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

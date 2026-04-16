#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEvent {
    pub model: String,
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub cache_creation_input_tokens: usize,
    pub cache_read_input_tokens: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderFailureDisposition {
    PreStreamRetryable,
    PreStreamTerminal,
    StreamInterrupted,
    StreamTerminal,
}

impl ProviderFailureDisposition {
    pub fn is_pre_stream_retryable(&self) -> bool {
        matches!(self, Self::PreStreamRetryable)
    }

    pub fn is_stream_interrupted(&self) -> bool {
        matches!(self, Self::StreamInterrupted)
    }

    pub fn is_stream_terminal(&self) -> bool {
        matches!(self, Self::StreamTerminal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamError {
    pub provider_id: String,
    pub kind: String,
    pub message: String,
    pub retryable: bool,
    pub disposition: ProviderFailureDisposition,
    pub status_code: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    MessageStart,
    TextDelta(String),
    ToolUse { tool_name: String, input: String },
    Usage(UsageEvent),
    MessageStop { stop_reason: StopReason },
    Error(StreamError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    Error,
}

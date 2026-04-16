#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEvent {
    pub model: String,
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub cache_creation_input_tokens: usize,
    pub cache_read_input_tokens: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamError {
    pub provider_id: String,
    pub kind: String,
    pub message: String,
    pub retryable: bool,
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

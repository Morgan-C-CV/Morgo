#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    MessageStart,
    TextDelta(String),
    ToolUse { tool_name: String, input: String },
    MessageStop { stop_reason: StopReason },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    Error,
}

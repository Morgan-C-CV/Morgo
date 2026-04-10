use crate::core::context::QueryContext;
use crate::core::message::Message;
use crate::service::api::streaming::{StopReason, StreamEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryLoopState {
    Streaming,
    AwaitingTool,
    Interrupted,
    Compacting,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryTerminalReason {
    Completed,
    ToolUseRequested,
    Interrupted,
    Compacted,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLoopResult {
    pub state: QueryLoopState,
    pub terminal_reason: QueryTerminalReason,
    pub messages: Vec<Message>,
}

pub async fn run_query_loop(context: &QueryContext, input: Message) -> QueryLoopResult {
    let mut messages = Vec::new();
    let mut aggregated_text = String::new();
    let token_estimate = input.content.len();

    if context.compactor.should_compact(token_estimate, 512) {
        messages.push(Message::assistant(
            "compaction requested before continuing the turn",
        ));
        return QueryLoopResult {
            state: QueryLoopState::Compacting,
            terminal_reason: QueryTerminalReason::Compacted,
            messages,
        };
    }

    let events = context.api_client.stream_message(&input).await;
    if events.is_empty() {
        messages.push(Message::assistant(format!(
            "no stream events available for: {}",
            input.content
        )));
        return QueryLoopResult {
            state: QueryLoopState::Completed,
            terminal_reason: QueryTerminalReason::Completed,
            messages,
        };
    }

    for event in events {
        match event {
            StreamEvent::MessageStart => {}
            StreamEvent::TextDelta(delta) => {
                aggregated_text.push_str(&delta);
            }
            StreamEvent::ToolUse { tool_name, input } => {
                if !aggregated_text.is_empty() {
                    messages.push(Message::assistant(aggregated_text.clone()));
                }
                messages.push(Message::assistant(format!(
                    "tool requested: {tool_name} {input}"
                )));
                return QueryLoopResult {
                    state: QueryLoopState::AwaitingTool,
                    terminal_reason: QueryTerminalReason::ToolUseRequested,
                    messages,
                };
            }
            StreamEvent::MessageStop { stop_reason } => {
                if !aggregated_text.is_empty() {
                    messages.push(Message::assistant(aggregated_text.clone()));
                }
                let terminal_reason = match stop_reason {
                    StopReason::EndTurn => QueryTerminalReason::Completed,
                    StopReason::ToolUse => QueryTerminalReason::ToolUseRequested,
                    StopReason::MaxTokens => QueryTerminalReason::Interrupted,
                    StopReason::Error => QueryTerminalReason::Failed,
                };
                let state = match stop_reason {
                    StopReason::EndTurn => QueryLoopState::Completed,
                    StopReason::ToolUse => QueryLoopState::AwaitingTool,
                    StopReason::MaxTokens => QueryLoopState::Interrupted,
                    StopReason::Error => QueryLoopState::Failed,
                };
                return QueryLoopResult {
                    state,
                    terminal_reason,
                    messages,
                };
            }
            StreamEvent::Error(error) => {
                messages.push(Message::assistant(format!("stream error: {error}")));
                return QueryLoopResult {
                    state: QueryLoopState::Failed,
                    terminal_reason: QueryTerminalReason::Failed,
                    messages,
                };
            }
        }
    }

    if !aggregated_text.is_empty() {
        messages.push(Message::assistant(aggregated_text));
    }

    QueryLoopResult {
        state: QueryLoopState::Completed,
        terminal_reason: QueryTerminalReason::Completed,
        messages,
    }
}

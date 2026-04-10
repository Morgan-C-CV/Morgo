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

    let mut current_input = input;
    for _ in 0..4 {
        let events = context.api_client.stream_message(&current_input).await;
        if events.is_empty() {
            break;
        }

        let mut aggregated_text = String::new();
        let mut pending_tool_use: Option<(String, String)> = None;

        for event in events {
            match event {
                StreamEvent::MessageStart => {}
                StreamEvent::TextDelta(delta) => {
                    aggregated_text.push_str(&delta);
                }
                StreamEvent::ToolUse { tool_name, input } => {
                    pending_tool_use = Some((tool_name, input));
                }
                StreamEvent::MessageStop { stop_reason } => match stop_reason {
                    StopReason::EndTurn => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text));
                        }
                        return QueryLoopResult {
                            state: QueryLoopState::Completed,
                            terminal_reason: QueryTerminalReason::Completed,
                            messages,
                        };
                    }
                    StopReason::ToolUse => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text.clone()));
                        }
                        let Some((tool_name, tool_input)) = pending_tool_use.take() else {
                            messages.push(Message::assistant(
                                "stream error: tool stop without tool payload",
                            ));
                            return QueryLoopResult {
                                state: QueryLoopState::Failed,
                                terminal_reason: QueryTerminalReason::Failed,
                                messages,
                            };
                        };
                        let tool_result = context
                            .tool_registry
                            .invoke(
                                &crate::tool::definition::ToolCall {
                                    name: tool_name.clone(),
                                    input: tool_input.clone(),
                                },
                                &context.app_state.permission_context,
                            )
                            .await;
                        match tool_result {
                            Ok(crate::tool::definition::ToolResult::Text(text)) => {
                                messages.push(Message::assistant(format!(
                                    "tool {tool_name} result: {text}"
                                )));
                                current_input =
                                    Message::user(format!("tool result for {tool_name}: {text}"));
                                break;
                            }
                            Ok(crate::tool::definition::ToolResult::Denied(reason)) => {
                                messages.push(Message::assistant(format!(
                                    "tool {tool_name} denied: {reason}"
                                )));
                                return QueryLoopResult {
                                    state: QueryLoopState::Failed,
                                    terminal_reason: QueryTerminalReason::Failed,
                                    messages,
                                };
                            }
                            Err(error) => {
                                messages.push(Message::assistant(format!(
                                    "tool {tool_name} failed: {error}"
                                )));
                                return QueryLoopResult {
                                    state: QueryLoopState::Failed,
                                    terminal_reason: QueryTerminalReason::Failed,
                                    messages,
                                };
                            }
                        }
                    }
                    StopReason::MaxTokens => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text));
                        }
                        return QueryLoopResult {
                            state: QueryLoopState::Interrupted,
                            terminal_reason: QueryTerminalReason::Interrupted,
                            messages,
                        };
                    }
                    StopReason::Error => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text));
                        }
                        return QueryLoopResult {
                            state: QueryLoopState::Failed,
                            terminal_reason: QueryTerminalReason::Failed,
                            messages,
                        };
                    }
                },
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
    }

    QueryLoopResult {
        state: QueryLoopState::Completed,
        terminal_reason: QueryTerminalReason::Completed,
        messages,
    }
}

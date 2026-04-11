use crate::core::context::QueryContext;
use crate::core::message::Message;
use crate::hook::executor::{HookDecision, run_hook};
use crate::hook::registry::HookEvent;
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
    StoppedByHook,
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
    context
        .app_state
        .cost_tracker
        .record_request(token_estimate, 0);

    let prompt_hook = run_hook(&context.hook_registry, HookEvent::UserPromptSubmit);
    messages.extend(prompt_hook.messages.clone());
    if let HookDecision::Deny(reason) = prompt_hook.decision {
        messages.push(Message::assistant(format!("hook denied prompt: {reason}")));
        return QueryLoopResult {
            state: QueryLoopState::Failed,
            terminal_reason: QueryTerminalReason::Failed,
            messages,
        };
    }

    if context.compactor.should_compact(token_estimate, 512) {
        messages.push(Message::assistant(
            "compaction requested before continuing the turn",
        ));
        return finalize_with_stop_hook(
            context,
            messages,
            QueryLoopState::Compacting,
            QueryTerminalReason::Compacted,
        );
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
                        return finalize_with_stop_hook(
                            context,
                            messages,
                            QueryLoopState::Completed,
                            QueryTerminalReason::Completed,
                        );
                    }
                    StopReason::ToolUse => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text.clone()));
                        }
                        let Some((tool_name, tool_input)) = pending_tool_use.take() else {
                            messages.push(Message::assistant(
                                "stream error: tool stop without tool payload",
                            ));
                            return finalize_with_stop_hook(
                                context,
                                messages,
                                QueryLoopState::Failed,
                                QueryTerminalReason::Failed,
                            );
                        };
                        let pre_tool_hook = run_hook(
                            &context.hook_registry,
                            HookEvent::PreToolUse {
                                tool_name: tool_name.clone(),
                            },
                        );
                        messages.extend(pre_tool_hook.messages.clone());
                        if let HookDecision::Deny(reason) = pre_tool_hook.decision {
                            messages.push(Message::assistant(format!(
                                "tool {tool_name} denied by hook: {reason}"
                            )));
                            let post_failure_hook = run_hook(
                                &context.hook_registry,
                                HookEvent::PostToolUseFailure {
                                    tool_name: tool_name.clone(),
                                },
                            );
                            messages.extend(post_failure_hook.messages);
                            return finalize_with_stop_hook(
                                context,
                                messages,
                                QueryLoopState::Failed,
                                QueryTerminalReason::Failed,
                            );
                        }
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
                                let post_tool_hook = run_hook(
                                    &context.hook_registry,
                                    HookEvent::PostToolUse {
                                        tool_name: tool_name.clone(),
                                    },
                                );
                                messages.extend(post_tool_hook.messages.clone());
                                if post_tool_hook.prevent_continuation {
                                    messages.push(Message::assistant(format!(
                                        "tool {tool_name} result: {text}"
                                    )));
                                    return finalize_with_stop_hook(
                                        context,
                                        messages,
                                        QueryLoopState::Completed,
                                        QueryTerminalReason::StoppedByHook,
                                    );
                                }
                                messages.push(Message::assistant(format!(
                                    "tool {tool_name} result: {text}"
                                )));
                                current_input =
                                    Message::user(format!("tool result for {tool_name}: {text}"));
                                break;
                            }
                            Ok(crate::tool::definition::ToolResult::Denied(reason)) => {
                                let post_failure_hook = run_hook(
                                    &context.hook_registry,
                                    HookEvent::PostToolUseFailure {
                                        tool_name: tool_name.clone(),
                                    },
                                );
                                messages.extend(post_failure_hook.messages);
                                messages.push(Message::assistant(format!(
                                    "tool {tool_name} denied: {reason}"
                                )));
                                return finalize_with_stop_hook(
                                    context,
                                    messages,
                                    QueryLoopState::Failed,
                                    QueryTerminalReason::Failed,
                                );
                            }
                            Err(error) => {
                                let post_failure_hook = run_hook(
                                    &context.hook_registry,
                                    HookEvent::PostToolUseFailure {
                                        tool_name: tool_name.clone(),
                                    },
                                );
                                messages.extend(post_failure_hook.messages);
                                messages.push(Message::assistant(format!(
                                    "tool {tool_name} failed: {error}"
                                )));
                                return finalize_with_stop_hook(
                                    context,
                                    messages,
                                    QueryLoopState::Failed,
                                    QueryTerminalReason::Failed,
                                );
                            }
                        }
                    }
                    StopReason::MaxTokens => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text));
                        }
                        return finalize_with_stop_hook(
                            context,
                            messages,
                            QueryLoopState::Interrupted,
                            QueryTerminalReason::Interrupted,
                        );
                    }
                    StopReason::Error => {
                        if !aggregated_text.is_empty() {
                            messages.push(Message::assistant(aggregated_text));
                        }
                        return finalize_with_stop_hook(
                            context,
                            messages,
                            QueryLoopState::Failed,
                            QueryTerminalReason::Failed,
                        );
                    }
                },
                StreamEvent::Error(error) => {
                    messages.push(Message::assistant(format!("stream error: {error}")));
                    return finalize_with_stop_hook(
                        context,
                        messages,
                        QueryLoopState::Failed,
                        QueryTerminalReason::Failed,
                    );
                }
            }
        }
    }

    finalize_with_stop_hook(
        context,
        messages,
        QueryLoopState::Completed,
        QueryTerminalReason::Completed,
    )
}

fn finalize_with_stop_hook(
    context: &QueryContext,
    mut messages: Vec<Message>,
    default_state: QueryLoopState,
    default_reason: QueryTerminalReason,
) -> QueryLoopResult {
    let stop_hook = run_hook(&context.hook_registry, HookEvent::Stop);
    messages.extend(stop_hook.messages);

    let (state, terminal_reason) = if stop_hook.prevent_continuation {
        (
            QueryLoopState::Completed,
            QueryTerminalReason::StoppedByHook,
        )
    } else {
        (default_state, default_reason)
    };

    QueryLoopResult {
        state,
        terminal_reason,
        messages,
    }
}

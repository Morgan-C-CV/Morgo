use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::message::Message;
use rust_agent::core::query_loop::{QueryLoopState, QueryTerminalReason};
use rust_agent::hook::registry::{HookEvent, HookEventMatcher, HookRegistry, HookRule};
use rust_agent::service::api::client::AnthropicClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use std::sync::Arc;

use rust_agent::state::app_state::AppState;
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::registry::ToolRegistry;

fn test_context(events: Vec<StreamEvent>) -> QueryContext {
    test_context_with_turns(vec![events], ToolRegistry::new())
}

fn test_context_with_turns(
    turns: Vec<Vec<StreamEvent>>,
    tool_registry: ToolRegistry,
) -> QueryContext {
    let mut permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.always_allow_rules.push("Agent".into());
    QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            permission_context,
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry,
        api_client: AnthropicClient::with_scripted_turns(turns),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
    }
}

#[tokio::test]
async fn query_loop_collects_text_until_end_turn() {
    let engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("hello ".into()),
        StreamEvent::TextDelta("world".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]));

    let result = engine.submit_turn(Message::user("hi")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal_reason, QueryTerminalReason::Completed);
    assert_eq!(result.messages, vec![Message::assistant("hello world")]);
}

#[tokio::test]
async fn query_loop_invokes_tool_and_continues_follow_up_turn() {
    let registry = ToolRegistry::new().register(Arc::new(AgentTool));
    let engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("planning...".into()),
                StreamEvent::ToolUse {
                    tool_name: "Agent".into(),
                    input: "inspect file".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done after tool".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));

    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal_reason, QueryTerminalReason::Completed);
    assert_eq!(result.messages.len(), 3);
    assert_eq!(result.messages[0], Message::assistant("planning..."));
    assert!(result.messages[1].content.contains("tool Agent result:"));
    assert_eq!(result.messages[2], Message::assistant("done after tool"));
}

#[tokio::test]
async fn query_loop_marks_interrupted_on_max_tokens() {
    let engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("partial".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::MaxTokens,
        },
    ]));

    let result = engine.submit_turn(Message::user("long answer")).await;

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal_reason, QueryTerminalReason::Interrupted);
    assert_eq!(result.messages, vec![Message::assistant("partial")]);
}

#[tokio::test]
async fn query_loop_requests_compaction_for_large_input() {
    let engine = QueryEngine::new(test_context(Vec::new()));
    let oversized = "x".repeat(600);

    let result = engine.submit_turn(Message::user(oversized)).await;

    assert_eq!(result.state, QueryLoopState::Compacting);
    assert_eq!(result.terminal_reason, QueryTerminalReason::Compacted);
    assert_eq!(
        result.messages,
        vec![Message::assistant(
            "compaction requested before continuing the turn"
        )]
    );
}

#[tokio::test]
async fn query_loop_surfaces_stream_errors() {
    let engine = QueryEngine::new(test_context(vec![StreamEvent::Error("boom".into())]));

    let result = engine.submit_turn(Message::user("trigger error")).await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(result.terminal_reason, QueryTerminalReason::Failed);
    assert_eq!(
        result.messages,
        vec![Message::assistant("stream error: boom")]
    );
}

#[tokio::test]
async fn query_loop_fails_when_tool_is_unknown() {
    let engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::ToolUse {
            tool_name: "MissingTool".into(),
            input: "payload".into(),
        },
        StreamEvent::MessageStop {
            stop_reason: StopReason::ToolUse,
        },
    ]));

    let result = engine
        .submit_turn(Message::user("trigger unknown tool"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(result.terminal_reason, QueryTerminalReason::Failed);
    assert!(
        result.messages[0]
            .content
            .contains("tool MissingTool failed")
    );
}

#[tokio::test]
async fn query_loop_respects_pre_tool_hook_denial() {
    let registry = ToolRegistry::new().register(Arc::new(AgentTool));
    let mut permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.always_allow_rules.push("Agent".into());

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            permission_context,
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: registry,
        api_client: AnthropicClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::ToolUse {
                tool_name: "Agent".into(),
                input: "inspect file".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default().register_rule(HookRule {
            event: HookEventMatcher::PreToolUse,
            deny_match: Some("Agent".into()),
        }),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert!(result.messages[0].content.contains("denied by hook"));
    assert!(
        engine
            .context
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::UserPromptSubmit)
    );
}

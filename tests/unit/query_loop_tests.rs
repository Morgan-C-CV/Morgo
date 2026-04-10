use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::message::Message;
use rust_agent::core::query_loop::{QueryLoopState, QueryTerminalReason};
use rust_agent::service::api::client::AnthropicClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::AppState;
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::registry::ToolRegistry;

fn test_context(events: Vec<StreamEvent>) -> QueryContext {
    QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            permission_context: ToolPermissionContext::new(PermissionMode::Default),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: AnthropicClient::with_scripted_events(events),
        compactor: ReactiveCompactor,
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
async fn query_loop_enters_tool_wait_state_on_tool_use() {
    let engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("planning...".into()),
        StreamEvent::ToolUse {
            tool_name: "Read".into(),
            input: "/tmp/demo.txt".into(),
        },
    ]));

    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::AwaitingTool);
    assert_eq!(
        result.terminal_reason,
        QueryTerminalReason::ToolUseRequested
    );
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages[0], Message::assistant("planning..."));
    assert!(
        result.messages[1]
            .content
            .contains("tool requested: Read /tmp/demo.txt")
    );
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

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::context::{QueryContext, SubagentConfig};
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::events::EngineEvent;
use rust_agent::core::message::Message;
use rust_agent::history::session::{SessionHistory, SessionHistoryEntry};
use rust_agent::core::query_loop::{
    Continue, QueryLoopState, QueryParams, Terminal, run_query_loop, run_query_loop_with_params,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::hook::registry::{
    HookEvent, HookEventMatcher, HookRegistry, HookRule, HookRuleLayer,
};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::service::api::client::{ModelProviderClient, parse_sse_response};
use rust_agent::service::api::errors::ApiError;
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::service::api::streaming::{StopReason, StreamEvent, UsageEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::app_state::WorkerRole;
use rust_agent::task::types::{TaskOwner, ValidationState, WorkerPhase};
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};

use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::definition::PermissionDecision;
use rust_agent::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use rust_agent::tool::registry::ToolRegistry;

struct ProgressFixtureTool;
struct PendingApprovalFixtureTool;

#[async_trait]
impl Tool for ProgressFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "ProgressFixture".into(),
            description: "Returns progress updates".into(),
            aliases: &[],
            search_hint: None,
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Progress("42% complete".into()))
    }
}

#[async_trait]
impl Tool for PendingApprovalFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "PendingApprovalFixture".into(),
            description: "Requests approval for query loop tests".into(),
            aliases: &[],
            search_hint: None,
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn check_permissions(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> PermissionDecision {
        PermissionDecision::Ask {
            message: "requires explicit approval".into(),
            reason: rust_agent::tool::definition::PermissionDecisionReason::Tool,
        }
    }

    async fn invoke(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Text("should not execute".into()))
    }
}

fn test_context(events: Vec<StreamEvent>) -> QueryContext {
    test_context_with_turns(vec![events], ToolRegistry::new())
}

fn test_context_with_turns(
    turns: Vec<Vec<StreamEvent>>,
    tool_registry: ToolRegistry,
) -> QueryContext {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");
    QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry,
        api_client: ModelProviderClient::with_scripted_turns(turns),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    }
}

#[tokio::test]
async fn query_loop_records_usage_events_into_cost_tracker() {
    let context = test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::Usage(UsageEvent {
            model: "default-model".into(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_input_tokens: 10,
            cache_read_input_tokens: 5,
        }),
        StreamEvent::TextDelta("usage tracked".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]);

    let result = run_query_loop(&context, Message::user("track usage")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Notice { kind, .. } if kind == &"usage"))
    );
    let report = context.app_state.cost_tracker.format_report();
    assert!(report.contains("model default-model -> requests: 1"));
    assert!(report.contains("cache_creation_input_tokens: 10"));
    assert!(report.contains("cache_read_input_tokens: 5"));
}

#[tokio::test]
async fn engine_stream_turn_yields_committed_messages() {
    let engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("hello ".into()),
        StreamEvent::TextDelta("stream".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]));

    let mut receiver = engine.stream_turn(Message::user("hi")).await;
    let mut committed = Vec::new();
    while let Some(event) = receiver.recv().await {
        if let EngineEvent::MessageCommitted(message) = event {
            committed.push(message);
        }
    }

    assert_eq!(committed, vec![Message::assistant("hello stream")]);
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
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, None);
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
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ToolUseFollowUp));
    assert_eq!(result.messages.len(), 3);
    assert_eq!(result.messages[0], Message::assistant("planning..."));
    assert!(result.messages[1].content.contains("tool Agent result:"));
    assert_eq!(result.messages[2], Message::assistant("done after tool"));
    assert!(result.messages[1].content.contains(": "));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::ToolResultCommitted {
            tool_name,
            content,
            summary,
            detail,
            ..
        } if tool_name == "Agent"
            && !content.is_empty()
            && summary == "Agent succeeded"
            && detail.as_deref().is_some_and(|detail| !detail.is_empty())
    )));
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Transition(Continue::ToolUseFollowUp)))
    );
}

#[tokio::test]
async fn query_loop_surfaces_progress_record_summary_and_detail() {
    let registry = ToolRegistry::new().register(Arc::new(ProgressFixtureTool));
    let engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "ProgressFixture".into(),
                    input: "payload".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done after progress".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));

    let result = engine.submit_turn(Message::user("show progress")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ToolUseFollowUp));
    assert_eq!(
        result.messages.last(),
        Some(&Message::assistant("done after progress"))
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice { kind, message }
            if kind == &"tool-progress"
                && message.contains("ProgressFixture in progress")
                && message.contains("42% complete")
    )));
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Transition(Continue::ToolUseFollowUp)))
    );
}

#[tokio::test]
async fn query_loop_progress_follow_up_uses_aggregated_summary_not_detail() {
    let registry = ToolRegistry::new().register(Arc::new(ProgressFixtureTool));
    let context = test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "ProgressFixture".into(),
                    input: "payload".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done after progress".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("show progress"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.transition, Some(Continue::ToolUseFollowUp));
    assert!(
        result
            .messages
            .iter()
            .all(|message| !message.content.contains("42% complete"))
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice { kind, message }
            if kind == &"tool-progress"
                && message.contains("42% complete")
    )));
}

#[tokio::test]
async fn query_loop_pending_approval_uses_aggregated_summary_for_pending_context() {
    let registry = ToolRegistry::new().register(Arc::new(PendingApprovalFixtureTool));
    let context = test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::ToolUse {
                tool_name: "PendingApprovalFixture".into(),
                input: "payload".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        registry,
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs approval"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(matches!(
        context
            .app_state
            .permission_context
            .pending_approval(),
        Some(pending)
            if pending.tool_name == "PendingApprovalFixture"
                && pending.message == "requires explicit approval"
    ));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::PendingApproval {
            tool_name,
            summary,
            detail,
            ..
        } if tool_name == "PendingApprovalFixture"
            && summary == "PendingApprovalFixture pending approval"
            && detail.as_deref() == Some("requires explicit approval")
    )));
    assert!(result.messages.iter().any(|message| {
        message.content
            == "approval required for PendingApprovalFixture: requires explicit approval"
    }));
}

#[tokio::test]
async fn query_loop_uses_max_output_escalation_then_recovery() {
    let engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("partial".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::MaxTokens,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("completed".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    ));

    let result = engine.submit_turn(Message::user("long answer")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::MaxOutputTokensEscalate));
    assert_eq!(
        result.messages,
        vec![
            Message::assistant("partial"),
            Message::assistant("completed")
        ]
    );
}

#[tokio::test]
async fn query_loop_requests_compaction_for_large_input() {
    let engine = QueryEngine::new(test_context(Vec::new()));
    let oversized = "x".repeat(5000);

    let result = engine.submit_turn(Message::user(oversized)).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ReactiveCompactRetry));
    assert_eq!(
        result.messages,
        vec![Message::assistant(
            "compaction requested before continuing the turn"
        )]
    );
}

#[tokio::test]
async fn query_loop_surfaces_stream_errors_after_recovery_attempt() {
    let engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![StreamEvent::Error("boom".into())],
            vec![StreamEvent::Error("boom again".into())],
        ],
        ToolRegistry::new(),
    ));

    let result = engine.submit_turn(Message::user("trigger error")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::CollapseDrainRetry));
    assert!(result.messages[0].content.contains("stream error: boom"));
    assert!(
        result.messages[1]
            .content
            .contains("stream error: boom again")
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice { kind: "recovery", message }
        if message.contains("collapse drain retry")
    )));
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

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(
        result.messages[0]
            .content
            .contains("tool MissingTool failed")
    );
    assert!(
        result.messages[1]
            .content
            .contains("result missing; synthesized failure result")
    );
}

#[tokio::test]
async fn query_loop_stop_hook_can_prevent_continuation() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("done".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default().register_rule(HookRule {
            event: HookEventMatcher::Stop,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("stop hook appended message".into()),
            prevent_continuation: true,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
        }),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::StopHookPrevented);
    assert_eq!(
        result.messages,
        vec![
            Message::assistant("done"),
            Message::assistant("stop hook appended message")
        ]
    );
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Notice { kind: "hook", .. }))
    );
}

#[tokio::test]
async fn query_loop_respects_pre_tool_hook_denial() {
    let registry = ToolRegistry::new().register(Arc::new(AgentTool));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: registry,
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
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
            layer: HookRuleLayer::Defaults,
            deny_match: Some("Agent".into()),
            append_message: None,
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
        }),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(result.messages[0].content.contains("denied by hook"));
    assert!(
        engine
            .context
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::UserPromptSubmit)
    );
    assert!(engine.context.hook_registry.recorded_events().contains(
        &HookEvent::PermissionDenied {
            tool_name: "Agent".into(),
            reason: "tool Agent denied by hook policy".into(),
        }
    ));
    assert!(engine.context.hook_registry.recorded_events().contains(
        &HookEvent::PreToolUse {
            tool_name: "Agent".into(),
        }
    ));
    assert!(engine.context.hook_registry.recorded_events().contains(
        &HookEvent::PostToolUseFailure {
            tool_name: "Agent".into(),
        }
    ));
}

#[tokio::test]
async fn query_loop_runs_permission_request_hook_before_tool_execution() {
    let registry = ToolRegistry::new().register(Arc::new(AgentTool));
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: registry,
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
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
            event: HookEventMatcher::PermissionRequest,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("permission request observed".into()),
            prevent_continuation: false,
            block_continuation: false,
            permission_decision: Some("deny".into()),
            updated_input: None,
            additional_context: None,
        }),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(
        result.messages[0]
            .content
            .contains("permission request observed")
    );
    assert!(
        result.messages[1]
            .content
            .contains("denied before execution")
    );
    assert!(engine.context.hook_registry.recorded_events().contains(
        &HookEvent::PermissionRequest {
            tool_name: "Agent".into(),
        }
    ));
    assert!(engine.context.hook_registry.recorded_events().contains(
        &HookEvent::PermissionDenied {
            tool_name: "Agent".into(),
            reason: "hook rule set permission to deny".into(),
        }
    ));
}

#[tokio::test]
async fn query_loop_stop_hook_blocking_continues_with_follow_up_turn() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("draft answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("revised answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default().register_rule(HookRule {
            event: HookEventMatcher::Stop,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("stop hook requires revision".into()),
            prevent_continuation: false,
            block_continuation: true,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
        }),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::StopHookBlocking));
    assert_eq!(
        result.messages,
        vec![
            Message::assistant("draft answer"),
            Message::assistant("stop hook requires revision"),
            Message::assistant("revised answer"),
            Message::assistant("stop hook requires revision")
        ]
    );
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Transition(Continue::StopHookBlocking)))
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "hook",
            message,
        } if message.contains("blocking continuation retry")
    )));
}

#[tokio::test]
async fn query_loop_uses_subagent_stop_hook_for_subagent_context() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Worker,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("subagent done".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default().register_rule(HookRule {
            event: HookEventMatcher::SubagentStop,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("subagent stop appended message".into()),
            prevent_continuation: true,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
        }),
        agent_id: Some("agent-task-1".into()),
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::StopHookPrevented);
    assert_eq!(
        result.messages,
        vec![
            Message::assistant("subagent done"),
            Message::assistant("subagent stop appended message")
        ]
    );
    assert!(
        engine
            .context
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::SubagentStop)
    );
    assert!(
        !engine
            .context
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::Stop)
    );
}

#[test]
fn provider_sse_parsing_maps_standard_events() {
    let body = concat!(
        "event: message\n",
        "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":11}}}\n\n",
        "event: message\n",
        "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello\"}}\n\n",
        "event: message\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n"
    );

    let events = parse_sse_response(body, "default-model").expect("provider SSE should parse");
    assert!(matches!(events[0], StreamEvent::MessageStart));
    assert!(matches!(events[1], StreamEvent::Usage(_)));
    assert!(matches!(events[2], StreamEvent::TextDelta(_)));
    assert!(matches!(
        events[3],
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn
        }
    ));
}

#[test]
fn retry_policy_retries_only_retryable_pre_stream_errors() {
    let policy = RetryPolicy {
        max_attempts: 3,
        initial_backoff_ms: 1,
        max_backoff_ms: 2,
    };
    let retryable = ApiError::http_status(429, "rate limited");
    let fatal = ApiError::invalid_response("bad json");

    assert!(policy.should_retry(0, &retryable, false));
    assert!(!policy.should_retry(2, &retryable, false));
    assert!(!policy.should_retry(0, &retryable, true));
    assert!(!policy.should_retry(0, &fatal, false));
}

#[tokio::test]
async fn engine_drains_internal_task_events() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let task = manager.create("worker task", "test-session", InteractionSurface::Cli);
    manager.complete(&task.id, &dispatcher);

    let permission_context =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());
    permission_context.add_always_allow_rule("Agent");

    let engine = QueryEngine::new(QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::default(),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    });

    let events = engine.drain_task_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].task_id, "task-0");
    assert_eq!(
        events[0].owner,
        TaskOwner {
            session_id: "test-session".into(),
            surface: InteractionSurface::Cli,
        }
    );
    assert_eq!(
        events[0].status,
        rust_agent::task::types::TaskStatus::Completed
    );
    let formatted = QueryEngine::format_task_event_message(&events[0]);
    assert!(formatted.content.contains("<task-notification>"));
    assert!(formatted.content.contains("<task-id>task-0</task-id>"));
    assert!(
        formatted
            .content
            .contains("<result>Task completed</result>")
    );
    assert!(
        formatted
            .content
            .contains("<next-action>inspect task output for task-0</next-action>")
    );
    assert!(engine.drain_task_events().is_empty());
}

#[tokio::test]
async fn worker_query_loop_consumes_mailbox_messages() {
    let manager = Arc::new(TaskManager::default());
    let task = manager.create("worker task", "test-session", InteractionSurface::Cli);

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(manager.clone())
        .with_active_session_id("test-session");
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Worker,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("first answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("second answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: Some(task.id.clone()),
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    manager.launch(&task.id, "initial", std::future::pending::<()>());
    let engine = QueryEngine::new(context);
    let engine_handle =
        tokio::spawn(async move { engine.submit_turn(Message::user("initial")).await });

    tokio::task::yield_now().await;
    assert!(manager.send_message(&task.id, "test-session", "follow-up"));

    let result = timeout(Duration::from_secs(4), engine_handle)
        .await
        .expect("worker should finish")
        .expect("join should succeed");

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(
        result.messages,
        vec![
            Message::assistant("first answer"),
            Message::assistant("second answer")
        ]
    );
    assert_eq!(result.transition, Some(Continue::NextTurn));
}

#[tokio::test]
async fn subagent_context_inherits_parent_tools_and_hooks() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_external_memory_entries(vec!["external note for child".into()]);
    permission_context.add_always_allow_rule("Agent");

    let parent_hook_registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::SubagentStop,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: Some("inherited stop hook".into()),
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: None,
        updated_input: None,
        additional_context: None,
    });
    let parent_tool_registry = ToolRegistry::new().register(Arc::new(AgentTool));

    let parent = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: vec!["parent-runtime".into()],
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: parent_tool_registry.clone(),
        api_client: ModelProviderClient::default(),
        compactor: ReactiveCompactor,
        hook_registry: parent_hook_registry.clone(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let child = parent.create_subagent_context(
        "agent-task-2",
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("child result".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
        },
    );

    assert_eq!(child.app_state.runtime_role, RuntimeRole::Worker);
    assert!(child.is_subagent());
    assert!(
        !child
            .tool_registry
            .visible_tools(&child.app_state.permission_context)
            .iter()
            .any(|tool| tool.name == "Agent")
    );
    assert_eq!(child.app_state.startup_trace, vec!["parent-runtime"]);
    assert_eq!(
        child.app_state.permission_context.external_memory_entries(),
        vec!["external note for child"]
    );
    assert_eq!(
        child.app_state.permission_context.nested_memory_lineage(),
        vec![
            "session:test-session".to_string(),
            "agent:agent-task-2:inherit_context=true".to_string()
        ]
    );
    assert!(
        child
            .context_prompt
            .contains("- path: session:test-session -> agent:agent-task-2:inherit_context=true")
    );

    let result = QueryEngine::new(child.clone())
        .submit_turn(Message::user("run child"))
        .await;
    assert_eq!(
        result.messages[1],
        Message::assistant("inherited stop hook")
    );
    assert!(
        child
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::SubagentStop)
    );
}

#[tokio::test]
async fn subagent_context_does_not_inherit_session_memory_when_disabled() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_external_memory_entries(vec!["external note only".into()]);

    let parent = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: vec!["parent-runtime".into()],
            active_session_id: "parent-session".into(),
            session_store: None,
            session: None,
            history: Some(SessionHistory {
                entries: vec![SessionHistoryEntry {
                    message: Message::user("parent history present"),
                    timestamp: None,
                    tool_refs: vec!["src/parent.rs".into()],
                    milestone: None,
                }],
            }),
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::default(),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let child = parent.create_subagent_context(
        "agent-task-noinherit",
        vec![],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: false,
            max_turns: None,
            allowed_tools: None,
        },
    );

    assert!(child.app_state.history.is_none());
    assert_eq!(
        child.app_state.permission_context.external_memory_entries(),
        vec!["external note only"]
    );
    assert_eq!(
        child.app_state.permission_context.nested_memory_lineage(),
        vec![
            "session:parent-session".to_string(),
            "agent:agent-task-noinherit:inherit_context=false".to_string()
        ]
    );
    assert!(
        child
            .context_prompt
            .contains("- history: unavailable"),
        "session memory should be unavailable when inherit_context=false"
    );
    assert!(
        child.context_prompt.contains("External memory:")
            && child.context_prompt.contains("external note only")
    );
}

#[tokio::test]
async fn query_loop_respects_max_turns_terminal() {
    let context = test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("partial".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::MaxTokens,
            },
        ]],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs many turns"),
        QueryParams {
            max_turns: Some(0),
            ..QueryParams::default()
        },
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(result.terminal, Terminal::MaxTurns { count: 0 });
}

#[tokio::test]
async fn query_loop_emits_token_budget_continuation_before_max_budget() {
    let context = test_context(Vec::new());

    let first = run_query_loop_with_params(
        &context,
        Message::user("budgeted"),
        QueryParams {
            max_budget_input_tokens: Some(1),
            ..QueryParams::default()
        },
    )
    .await;
    assert_eq!(first.state, QueryLoopState::Completed);
    assert_eq!(first.terminal, Terminal::Completed);
    assert_eq!(first.transition, Some(Continue::TokenBudgetContinuation));

    let second = run_query_loop_with_params(
        &context,
        Message::user("budgeted"),
        QueryParams {
            messages: vec![Message::assistant("budget continuation already attempted")],
            max_budget_input_tokens: Some(1),
            ..QueryParams::default()
        },
    )
    .await;
    assert_eq!(second.state, QueryLoopState::Failed);
    let expected_budget = format!(
        "{}\n{}\n{}\n{}",
        context.current_system_prompt(),
        context.current_tools_prompt(),
        context.current_context_prompt(),
        "budgeted"
    )
    .len() as u64;
    assert_eq!(
        second.terminal,
        Terminal::MaxBudget {
            budget_usd_cents: expected_budget
        }
    );
}

#[tokio::test]
async fn coordinator_waits_for_group_barrier_before_synthesis_follow_up() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let first = manager.create("research shard a", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&first.id, WorkerRole::Research);
    manager.set_parent_task_id(&first.id, Some("parent-1".into()));
    manager.set_orchestration_group_id(&first.id, Some("group-1".into()));
    manager.set_phase(&first.id, Some(WorkerPhase::Research));
    manager.set_validation_state(&first.id, Some(ValidationState::NotNeeded));
    manager.start(&first.id);

    let second = manager.create("research shard b", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&second.id, WorkerRole::Research);
    manager.set_parent_task_id(&second.id, Some("parent-1".into()));
    manager.set_orchestration_group_id(&second.id, Some("group-1".into()));
    manager.set_phase(&second.id, Some(WorkerPhase::Research));
    manager.set_validation_state(&second.id, Some(ValidationState::NotNeeded));
    manager.start(&second.id);

    manager.complete(&first.id, &dispatcher);

    let permission_context =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("waiting for sibling worker".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("all research shards merged".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let first_result = QueryEngine::new(context.clone())
        .submit_turn(Message::user("synthesize research"))
        .await;
    assert_eq!(first_result.state, QueryLoopState::Completed);
    assert_eq!(first_result.terminal, Terminal::Completed);
    assert_eq!(first_result.transition, Some(Continue::NextTurn));
    assert!(
        first_result
            .messages
            .iter()
            .any(|message| message.content.contains("<task-id>task-0</task-id>"))
    );
    assert!(first_result
        .messages
        .iter()
        .any(|message| message
            .content
            .contains("orchestration still pending: wait for grouped research fan-in or verification before final synthesis")));
    assert!(
        !first_result
            .messages
            .iter()
            .any(|message| message.content.contains("grouped research tasks completed"))
    );
    assert!(manager.has_pending_orchestration("test-session"));

    manager.complete(&second.id, &dispatcher);

    let second_result = QueryEngine::new(context)
        .submit_turn(Message::user("synthesize research"))
        .await;
    assert_eq!(second_result.state, QueryLoopState::Completed);
    assert_eq!(second_result.terminal, Terminal::Completed);
    assert_eq!(second_result.transition, None);
    assert!(
        second_result
            .messages
            .iter()
            .any(|message| message.content.contains("<task-id>group-group-1</task-id>"))
    );
    assert!(second_result.messages.iter().any(|message| {
        message
            .content
            .contains("synthesize grouped findings for group-1")
    }));
    assert!(
        second_result
            .messages
            .iter()
            .any(|message| message.content.contains("all research shards merged"))
    );
    assert!(!manager.has_pending_orchestration("test-session"));
}

#[tokio::test]
async fn coordinator_gates_finalization_until_verification_finishes() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let implement = manager.create("implement patch", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&implement.id, WorkerRole::Implement);
    manager.set_parent_task_id(&implement.id, Some("parent-2".into()));
    manager.set_orchestration_group_id(&implement.id, Some("group-verify-1".into()));
    manager.set_phase(&implement.id, Some(WorkerPhase::Implement));
    manager.set_validation_state(&implement.id, Some(ValidationState::PendingVerification));
    manager.start(&implement.id);
    manager.complete(&implement.id, &dispatcher);

    let permission_context =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("verification still pending".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("verified synthesis ready".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let gated = QueryEngine::new(context.clone())
        .submit_turn(Message::user("finalize implementation"))
        .await;
    assert_eq!(gated.state, QueryLoopState::Completed);
    assert_eq!(gated.terminal, Terminal::Completed);
    assert_eq!(gated.transition, Some(Continue::NextTurn));
    assert!(gated.messages.iter().any(|message| {
        message
            .content
            .contains("<worker-role>implement</worker-role>")
    }));
    assert!(
        gated
            .messages
            .iter()
            .any(|message| message.content.contains("<phase>implement</phase>"))
    );
    assert!(gated.messages.iter().any(|message| {
        message
            .content
            .contains("<validation-state>pending_verification</validation-state>")
    }));
    assert!(gated.messages.iter().any(|message| {
        message
            .content
            .contains("dispatch verify worker for task-0")
    }));
    assert!(gated
        .messages
        .iter()
        .any(|message| message
            .content
            .contains("orchestration still pending: wait for grouped research fan-in or verification before final synthesis")));

    let verify = manager.create("verify patch", "test-session", InteractionSurface::Cli);
    manager.set_worker_role(&verify.id, WorkerRole::Verify);
    manager.set_parent_task_id(&verify.id, Some(implement.id.clone()));
    manager.set_orchestration_group_id(&verify.id, Some("group-verify-1".into()));
    manager.set_phase(&verify.id, Some(WorkerPhase::Verify));
    manager.start(&verify.id);
    manager.complete(&verify.id, &dispatcher);

    let verified = QueryEngine::new(context)
        .submit_turn(Message::user("finalize implementation"))
        .await;
    assert_eq!(verified.state, QueryLoopState::Completed);
    assert_eq!(verified.terminal, Terminal::Completed);
    assert_eq!(verified.transition, None);
    assert!(verified.messages.iter().any(|message| {
        message
            .content
            .contains("<worker-role>verify</worker-role>")
    }));
    assert!(
        verified
            .messages
            .iter()
            .any(|message| message.content.contains("<phase>verify</phase>"))
    );
    assert!(verified.messages.iter().any(|message| {
        message
            .content
            .contains("<validation-state>verified</validation-state>")
    }));
    assert!(verified.messages.iter().any(|message| {
        message
            .content
            .contains("synthesize validated result for task-1")
    }));
    assert!(
        verified
            .messages
            .iter()
            .any(|message| message.content.contains("verified synthesis ready"))
    );
}

#[tokio::test]
async fn coordinator_surfaces_verification_failure_and_missing_verification_risk() {
    let manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let implement = manager.create(
        "implement risky patch",
        "test-session",
        InteractionSurface::Cli,
    );
    manager.set_worker_role(&implement.id, WorkerRole::Implement);
    manager.set_parent_task_id(&implement.id, Some("parent-3".into()));
    manager.set_orchestration_group_id(&implement.id, Some("group-risk-1".into()));
    manager.set_phase(&implement.id, Some(WorkerPhase::Implement));
    manager.set_validation_state(&implement.id, Some(ValidationState::PendingVerification));
    manager.start(&implement.id);
    manager.complete(&implement.id, &dispatcher);

    let permission_context =
        ToolPermissionContext::new(PermissionMode::Default).with_task_manager(manager.clone());
    permission_context.add_always_allow_rule("Agent");

    let context = QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta(
                    "validation status: pending_verification; unverified risk remains before final answer"
                        .into(),
                ),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta(
                    "validation status: verification_failed; unverified risk remains after verify failure"
                        .into(),
                ),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta(
                    "validation status: unverified; unverified risk remains after verify worker was killed"
                        .into(),
                ),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    };

    let missing = QueryEngine::new(context.clone())
        .submit_turn(Message::user("finalize risky implementation"))
        .await;
    assert_eq!(missing.transition, Some(Continue::NextTurn));
    assert!(missing.messages.iter().any(|message| {
        message
            .content
            .contains("validation status: pending_verification")
    }));
    assert!(
        missing
            .messages
            .iter()
            .any(|message| message.content.contains("unverified risk remains"))
    );
    assert!(missing.messages.iter().any(|message| {
        message
            .content
            .contains("dispatch verify worker for task-0")
    }));

    let failed_verify = manager.create(
        "verify risky patch",
        "test-session",
        InteractionSurface::Cli,
    );
    manager.set_worker_role(&failed_verify.id, WorkerRole::Verify);
    manager.set_parent_task_id(&failed_verify.id, Some(implement.id.clone()));
    manager.set_orchestration_group_id(&failed_verify.id, Some("group-risk-1".into()));
    manager.set_phase(&failed_verify.id, Some(WorkerPhase::Verify));
    manager.start(&failed_verify.id);
    manager.fail(&failed_verify.id, &dispatcher);

    let failed = QueryEngine::new(context.clone())
        .submit_turn(Message::user("finalize risky implementation"))
        .await;
    assert_eq!(failed.transition, None);
    assert!(failed.messages.iter().any(|message| {
        message
            .content
            .contains("<validation-state>verification_failed</validation-state>")
    }));
    assert!(failed.messages.iter().any(|message| {
        message
            .content
            .contains("inspect verification failure for task-1")
    }));
    assert!(failed.messages.iter().any(|message| {
        message
            .content
            .contains("validation status: verification_failed")
    }));
    assert!(
        failed
            .messages
            .iter()
            .any(|message| message.content.contains("unverified risk remains"))
    );

    let second_implement = manager.create(
        "implement another risky patch",
        "test-session",
        InteractionSurface::Cli,
    );
    manager.set_worker_role(&second_implement.id, WorkerRole::Implement);
    manager.set_parent_task_id(&second_implement.id, Some("parent-4".into()));
    manager.set_orchestration_group_id(&second_implement.id, Some("group-risk-2".into()));
    manager.set_phase(&second_implement.id, Some(WorkerPhase::Implement));
    manager.set_validation_state(
        &second_implement.id,
        Some(ValidationState::PendingVerification),
    );
    manager.start(&second_implement.id);
    manager.complete(&second_implement.id, &dispatcher);

    let killed_verify = manager.create(
        "verify another risky patch",
        "test-session",
        InteractionSurface::Cli,
    );
    manager.set_worker_role(&killed_verify.id, WorkerRole::Verify);
    manager.set_parent_task_id(&killed_verify.id, Some(second_implement.id.clone()));
    manager.set_orchestration_group_id(&killed_verify.id, Some("group-risk-2".into()));
    manager.set_phase(&killed_verify.id, Some(WorkerPhase::Verify));
    manager.start(&killed_verify.id);
    assert!(manager.kill(&killed_verify.id, "test-session", &dispatcher));

    let killed = QueryEngine::new(context)
        .submit_turn(Message::user("finalize risky implementation"))
        .await;
    assert_eq!(killed.transition, None);
    assert!(killed.messages.iter().any(|message| {
        message
            .content
            .contains("<validation-state>unverified</validation-state>")
    }));
    assert!(killed.messages.iter().any(|message| {
        message
            .content
            .contains("synthesize with explicit unverified risk for task-3")
    }));
    assert!(
        killed
            .messages
            .iter()
            .any(|message| message.content.contains("validation status: unverified"))
    );
    assert!(
        killed
            .messages
            .iter()
            .any(|message| message.content.contains("unverified risk remains"))
    );
}

#[tokio::test]
async fn query_loop_retries_with_model_fallback_before_other_stream_recovery() {
    let context = test_context_with_turns(
        vec![
            vec![StreamEvent::Error(
                "fallback:model_error: upstream overloaded".into(),
            )],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("fallback recovered".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs fallback"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ModelFallbackRetry));
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Transition(Continue::ModelFallbackRetry)))
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            message,
        } if message.contains("model fallback retry")
    )));
}

#[tokio::test]
async fn query_loop_escalates_fallback_failure_to_terminal_model_error() {
    let context = test_context_with_turns(
        vec![
            vec![StreamEvent::Error(
                "fallback:model_error: upstream overloaded".into(),
            )],
            vec![StreamEvent::Error(
                "fallback:model_error: still failing".into(),
            )],
            vec![StreamEvent::Error("residual collapse failure".into())],
            vec![StreamEvent::Error("fatal after retries".into())],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs fallback failure"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError("fatal after retries".into())
    );
    assert_eq!(result.transition, Some(Continue::CollapseDrainRetry));
}

#[tokio::test]
async fn query_loop_second_max_tokens_hit_uses_recovery_branch() {
    let context = test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("partial one".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::MaxTokens,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("partial two".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::MaxTokens,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("finished".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs explicit recovery branch"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::MaxOutputTokensRecovery));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Transition(Continue::MaxOutputTokensEscalate)
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Transition(Continue::MaxOutputTokensRecovery)
    )));
}

#[tokio::test]
async fn submit_turn_emits_runtime_events_for_compact_recovery_and_terminal_paths() {
    let engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![StreamEvent::Error(
                "fallback:model_error: upstream overloaded".into(),
            )],
            vec![StreamEvent::Error("still overloaded".into())],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("final answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    ));

    let result = engine
        .submit_turn(Message::user("needs layered recovery"))
        .await;

    assert_eq!(result.terminal, Terminal::Completed);
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::CompactPlan
                && runtime.detail.contains("ReactiveCompact")
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::RetryScheduled
                && runtime.detail == Continue::ModelFallbackRetry.as_str()
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::NormalTerminal
                && runtime.detail == Terminal::Completed.as_str()
    )));
}

#[tokio::test]
async fn submit_turn_distinguishes_stop_hook_prevented_and_blocking_runtime_events() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let prevented_engine = QueryEngine::new(QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context: permission_context.clone(),
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("done".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default().register_rule(HookRule {
            event: HookEventMatcher::Stop,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("stop hook appended message".into()),
            prevent_continuation: true,
            block_continuation: false,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
        }),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    });

    let blocking_engine = QueryEngine::new(QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            startup_trace: Vec::new(),
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
        },
        tool_registry: ToolRegistry::new(),
        api_client: ModelProviderClient::with_scripted_turns(vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("draft answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("revised answer".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ]),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default().register_rule(HookRule {
            event: HookEventMatcher::Stop,
            layer: HookRuleLayer::Defaults,
            deny_match: None,
            append_message: Some("stop hook requires revision".into()),
            prevent_continuation: false,
            block_continuation: true,
            permission_decision: None,
            updated_input: None,
            additional_context: None,
        }),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    });

    let prevented = prevented_engine.submit_turn(Message::user("prevent")).await;
    let blocking = blocking_engine.submit_turn(Message::user("block")).await;

    assert!(prevented.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::StopHookPrevented
    )));
    assert!(blocking.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::StopHookBlocking
    )));
}

#[tokio::test]
async fn query_loop_uses_param_max_output_recovery_limit() {
    let context = test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("partial".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::MaxTokens,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("still partial".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::MaxTokens,
                },
            ],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs recovery"),
        QueryParams {
            max_output_tokens_recovery_limit: 0,
            ..QueryParams::default()
        },
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal, Terminal::AbortedStreaming);
    assert_eq!(result.transition, Some(Continue::MaxOutputTokensEscalate));
}

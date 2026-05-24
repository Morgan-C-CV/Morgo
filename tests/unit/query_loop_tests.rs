use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::core::boss_state::BossStage;
use rust_agent::core::context::{QueryContext, SubagentConfig, WorkerLisMPolicy};
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::events::EngineEvent;
use rust_agent::core::message::Message;
use rust_agent::core::query_loop::{
    Continue, QueryLoopState, QueryParams, Terminal, run_query_loop, run_query_loop_with_params,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::resume::RestoredSession;
use rust_agent::history::session::{
    InMemorySessionStore, SessionHistory, SessionHistoryEntry, SessionId, SessionSnapshot,
    SessionStore,
};
use rust_agent::history::transcript::Transcript;
use rust_agent::hook::registry::{
    HookEvent, HookEventMatcher, HookRegistry, HookRule, HookRuleLayer,
};
use rust_agent::interaction::cli::repl::{
    CliDisplayEvent, CliRuntimeEvent, handle_cli_input_streaming,
};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::router::CommandRouter;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plugins::runtime_state::{RuntimePluginSnapshot, build_turn_engine};
use rust_agent::security::authorizer::DefaultSurfaceAuthorizer;
use rust_agent::service::api::client::{
    ModelProviderClient, ModelProviderConfig, ProviderAuthStrategy,
    ProviderCompatibilityProfileKind, ProviderProtocol, parse_anthropic_sse_response,
};
use rust_agent::service::api::errors::ApiError;
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::service::api::streaming::{
    ProviderFailureDisposition, StopReason, StreamError, StreamEvent, UsageEvent,
};
use rust_agent::service::compact::reactive_compact::{
    AUTO_COMPACT_INPUT_CHAR_LIMIT, CompactServiceNextStep, ReactiveCompactor,
};
use rust_agent::service::observability::ServiceObservabilityTracker;
use rust_agent::state::active_model_runtime::{ActiveModelRuntime, ActiveModelRuntimeSnapshot};
use rust_agent::state::app_state::WorkerRole;
use rust_agent::task::types::{TaskOwner, ValidationState, WorkerPhase};
use serde_json::json;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};

use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{
    BossActorPolicy, PermissionMode, ToolPermissionContext,
};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::definition::PermissionDecision;
use rust_agent::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use rust_agent::tool::registry::ToolRegistry;
use std::time::Instant;

struct ProgressFixtureTool;
struct PendingApprovalFixtureTool;
struct DeniedFixtureTool;
struct EchoFixtureTool;
struct SlowFixtureTool;
struct CancellableFixtureTool {
    started: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
}

struct DropSignalGuard(Option<Arc<AtomicBool>>);

impl Drop for DropSignalGuard {
    fn drop(&mut self) {
        if let Some(flag) = self.0.take() {
            flag.store(true, Ordering::SeqCst);
        }
    }
}

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
            metadata: None,
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

#[async_trait]
impl Tool for DeniedFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "DeniedFixture".into(),
            description: "Returns denied tool results".into(),
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
        Ok(ToolResult::Denied("requires policy escalation".into()))
    }
}

#[async_trait]
impl Tool for EchoFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "EchoFixture".into(),
            description: "Echoes a value for tool-calling tests".into(),
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

    fn input_schema(&self) -> Option<serde_json::Value> {
        Some(json!({
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": { "type": "string" }
            }
        }))
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let value = call
            .json_input()
            .and_then(|json| {
                json.get("value")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "missing".into());
        Ok(ToolResult::Text(format!("echoed {value}")))
    }
}

#[async_trait]
impl Tool for SlowFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "SlowFixture".into(),
            description: "Sleeps before returning a tool result".into(),
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
        tokio::time::sleep(Duration::from_millis(75)).await;
        Ok(ToolResult::Text("slow result".into()))
    }
}

#[async_trait]
impl Tool for CancellableFixtureTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "CancellableFixture".into(),
            description: "Sleeps until the turn is cancelled".into(),
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
        self.started.store(true, Ordering::SeqCst);
        let _guard = DropSignalGuard(Some(self.dropped.clone()));
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.completed.store(true, Ordering::SeqCst);
        Ok(ToolResult::Text("completed".into()))
    }
}

fn assert_ordered_sections(haystack: &str, headers: &[&str]) {
    let mut previous = None;
    for header in headers {
        let index = haystack
            .find(header)
            .unwrap_or_else(|| panic!("missing header: {header}"));
        if let Some(prev) = previous {
            assert!(prev < index, "header {header} was out of order");
        }
        previous = Some(index);
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

fn test_context_with_production_client(
    api_client: ModelProviderClient,
    tool_registry: ToolRegistry,
    observability: ServiceObservabilityTracker,
) -> QueryContext {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
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
            service_observability_tracker: observability,
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "openai-compatible".into(),
                    protocol: "OpenAICompatible".into(),
                    compatibility_profile: "OpenAICompatible".into(),
                    base_url_host: "localhost".into(),
                    model: "test-model".into(),
                    auth_status: "none".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
        },
        tool_registry,
        api_client,
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    }
}

fn last_assistant_text(messages: &[Message]) -> Option<String> {
    messages
        .iter()
        .rev()
        .find(|message| matches!(message.role, rust_agent::core::message::Role::Assistant))
        .map(Message::text)
}

async fn run_openai_tool_loop_mock_server(
    listener: TcpListener,
    request_bodies: Arc<Mutex<Vec<String>>>,
) {
    for response_body in [
        concat!(
            "data: {\"id\":\"chatcmpl-tool\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_echo\",\"type\":\"function\",\"function\":{\"name\":\"EchoFixture\",\"arguments\":\"{\\\"value\\\":\\\"123\\\"}\"}}]},\"index\":0,\"finish_reason\":\"tool_calls\"}],\"usage\":{\"model\":\"test-model\",\"prompt_tokens\":20,\"completion_tokens\":5,\"total_tokens\":25}}\n\n",
            "data: [DONE]\n\n"
        ),
        concat!(
            "data: {\"id\":\"chatcmpl-final\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"completed after tool\"},\"index\":0,\"finish_reason\":\"stop\"}],\"usage\":{\"model\":\"test-model\",\"prompt_tokens\":30,\"completion_tokens\":6,\"total_tokens\":36}}\n\n",
            "data: [DONE]\n\n"
        ),
    ] {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut buffer = vec![0_u8; 32 * 1024];
        let bytes_read = stream.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
        request_bodies
            .lock()
            .expect("request bodies poisoned")
            .push(request);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        stream.flush().await.expect("flush response");
    }
}

async fn run_openai_single_turn_mock_server(
    listener: TcpListener,
    request_bodies: Arc<Mutex<Vec<String>>>,
    response_body: &'static str,
) {
    let (mut stream, _) = listener.accept().await.expect("accept request");
    let mut buffer = vec![0_u8; 32 * 1024];
    let bytes_read = stream.read(&mut buffer).await.expect("read request");
    let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
    request_bodies
        .lock()
        .expect("request bodies poisoned")
        .push(request);
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write response");
    stream.flush().await.expect("flush response");
}

#[test]
fn query_context_composes_turn_prompt_in_system_tools_context_user_order() {
    let context =
        test_context_with_turns(vec![], ToolRegistry::new().register(Arc::new(AgentTool)));
    let prompt = context.compose_turn_prompt("USER_SENTINEL");

    assert_ordered_sections(
        &prompt,
        &[
            "You are",
            "Agent -",
            "Runtime context summary:",
            "USER_SENTINEL",
        ],
    );
}

#[test]
fn query_context_composes_transcript_prompt_with_roles_in_order() {
    let context = test_context_with_turns(vec![], ToolRegistry::new());
    let prompt = context.compose_turn_prompt_from_messages(&[
        Message::user("original objective"),
        Message::assistant("tool Read result: alpha"),
        Message::user("tool result for Read: alpha"),
    ]);

    assert_ordered_sections(
        &prompt,
        &[
            "You are",
            "Runtime context summary:",
            "Conversation transcript:",
            "<user>",
            "original objective",
            "<assistant>",
            "tool Read result: alpha",
            "tool result for Read: alpha",
        ],
    );
}

#[tokio::test]
async fn query_loop_openai_tool_calling_includes_tools_and_preserves_transcript() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("listener addr");
    let request_bodies = Arc::new(Mutex::new(Vec::new()));
    let request_bodies_for_server = request_bodies.clone();
    let server = tokio::spawn(async move {
        run_openai_tool_loop_mock_server(listener, request_bodies_for_server).await;
    });

    let observability = ServiceObservabilityTracker::default();
    let config = ModelProviderConfig {
        provider_id: "openai-compatible".into(),
        protocol: ProviderProtocol::OpenAICompatible,
        compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
        base_url: format!("http://{addr}"),
        chat_completions_path: "/v1/chat/completions".into(),
        auth_strategy: ProviderAuthStrategy::NoAuth,
        model_id: "test-model".into(),
        retry_policy: RetryPolicy::default(),
        ..ModelProviderConfig::default()
    };
    let client = ModelProviderClient::from_config_with_observability(config, observability.clone());
    let context = test_context_with_production_client(
        client,
        ToolRegistry::new().register(Arc::new(EchoFixtureTool)),
        observability,
    );

    let result = run_query_loop(&context, Message::user("perform the echo task")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.text().contains("completed after tool"))
    );

    let bodies = request_bodies.lock().expect("request bodies poisoned");
    assert_eq!(bodies.len(), 2, "expected tool turn plus follow-up turn");
    let first = &bodies[0];
    let second = &bodies[1];
    assert!(
        first.contains("\"tools\""),
        "first request must expose tools"
    );
    assert!(
        first.contains("\"tool_choice\":\"auto\""),
        "first request must allow automatic tool selection"
    );
    assert!(
        first.contains("EchoFixture"),
        "first request must include the tool schema"
    );
    assert!(
        second.contains("perform the echo task"),
        "follow-up prompt must retain the original objective"
    );
    assert!(
        second.contains("tool result for EchoFixture: echoed 123"),
        "follow-up prompt must carry the concrete tool result"
    );

    drop(bodies);
    server.await.expect("mock server finished");
}

#[tokio::test]
async fn query_engine_submit_turn_syncs_store_and_runtime_history_mirrors() {
    let session_store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("mirror-sync-session".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/mirror-sync".into(),
        last_turn_at: None,
        prompt_seed: None,
    };
    let restored_history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::user("restored objective"),
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: None,
        }],
    };
    session_store
        .save(snapshot.clone(), restored_history.clone())
        .expect("seed restored history");

    let mut context = test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("fresh assistant reply".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]);
    context.app_state.active_session_id = session_id.0.clone();
    context.app_state.session_store = Some(session_store.clone());
    context.app_state.session = Some(snapshot.clone());
    context.app_state.history = Some(restored_history.clone());
    context.app_state.restored_session = Some(RestoredSession {
        snapshot: snapshot.clone(),
        history: restored_history.clone(),
        transcript: Transcript::from(restored_history),
    });

    let mut engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("fresh task")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);

    let (_, persisted_history) = session_store
        .load(&rust_agent::history::session::SessionRestoreRequest {
            resume: Some(session_id.0.clone()),
            continue_session: false,
        })
        .expect("persisted history should exist");
    assert_eq!(persisted_history.entries.len(), 3);
    assert_eq!(
        persisted_history.entries[1].message,
        Message::user("fresh task")
    );
    assert_eq!(
        persisted_history.entries[2].message,
        Message::assistant("fresh assistant reply")
    );

    let app_history = engine
        .context
        .app_state
        .history
        .clone()
        .expect("app state history should stay populated");
    assert_eq!(app_history, persisted_history);

    let restored_runtime_history = engine
        .context
        .app_state
        .restored_session
        .as_ref()
        .expect("restored session should stay attached")
        .history
        .clone();
    assert_eq!(restored_runtime_history, persisted_history);
}

#[tokio::test]
async fn query_engine_submit_turn_restores_transcript_from_store_before_current_input() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("listener addr");
    let request_bodies = Arc::new(Mutex::new(Vec::new()));
    let request_bodies_for_server = request_bodies.clone();
    let server = tokio::spawn(async move {
        run_openai_single_turn_mock_server(
            listener,
            request_bodies_for_server,
            concat!(
                "data: {\"id\":\"chatcmpl-resume\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"continued after restore\"},\"index\":0,\"finish_reason\":\"stop\"}],\"usage\":{\"model\":\"test-model\",\"prompt_tokens\":18,\"completion_tokens\":4,\"total_tokens\":22}}\n\n",
                "data: [DONE]\n\n"
            ),
        )
        .await;
    });

    let observability = ServiceObservabilityTracker::default();
    let config = ModelProviderConfig {
        provider_id: "openai-compatible".into(),
        protocol: ProviderProtocol::OpenAICompatible,
        compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
        base_url: format!("http://{addr}"),
        chat_completions_path: "/v1/chat/completions".into(),
        auth_strategy: ProviderAuthStrategy::NoAuth,
        model_id: "test-model".into(),
        retry_policy: RetryPolicy::default(),
        ..ModelProviderConfig::default()
    };
    let client = ModelProviderClient::from_config_with_observability(config, observability.clone());
    let session_store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("resume-session".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/resume-query".into(),
        last_turn_at: None,
        prompt_seed: None,
    };
    session_store
        .save(
            snapshot.clone(),
            SessionHistory {
                entries: vec![
                    SessionHistoryEntry {
                        message: Message::user("restored objective"),
                        timestamp: None,
                        tool_refs: Vec::new(),
                        milestone: None,
                    },
                    SessionHistoryEntry {
                        message: Message::assistant("restored assistant context"),
                        timestamp: None,
                        tool_refs: Vec::new(),
                        milestone: None,
                    },
                ],
            },
        )
        .expect("seed restored session history");

    let mut context =
        test_context_with_production_client(client, ToolRegistry::new(), observability);
    context.app_state.active_session_id = session_id.0.clone();
    context.app_state.session_store = Some(session_store);
    context.app_state.session = Some(snapshot);
    context.app_state.history = Some(SessionHistory::default());

    let result = QueryEngine::new(context)
        .submit_turn(Message::user("continue from here"))
        .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);

    let bodies = request_bodies.lock().expect("request bodies poisoned");
    assert_eq!(bodies.len(), 1, "expected a single restored turn request");
    let request = &bodies[0];
    assert!(
        request.contains("Conversation transcript:"),
        "restored turn should render transcript into the query prompt; request={request}"
    );
    assert!(
        request.contains("restored objective"),
        "restored user message must be included in the first resumed request; request={request}"
    );
    assert!(
        request.contains("restored assistant context"),
        "restored assistant message must be included in the first resumed request; request={request}"
    );
    assert!(
        request.contains("continue from here"),
        "current user input must still be appended after restored history; request={request}"
    );

    drop(bodies);
    server.await.expect("mock server finished");
}

#[tokio::test]
async fn query_loop_synthesizes_terminal_user_update_when_tool_follow_up_turn_is_silent() {
    let context = test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "EchoFixture".into(),
                    input: r#"{"value":"123"}"#.into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new().register(Arc::new(EchoFixtureTool)),
    );

    let result = run_query_loop(&context, Message::user("perform the echo task")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.text() == "Final update: echoed 123"),
        "{:?}",
        result.messages
    );
    assert!(
        result.events.iter().any(|event| matches!(
            event,
            EngineEvent::MessageCommitted(message)
                if message.text() == "Final update: echoed 123"
        )),
        "{:?}",
        result.events
    );
}

#[tokio::test]
async fn query_loop_preserves_natural_language_final_report_without_extra_continuation() {
    let result = run_query_loop(
        &test_context(vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("Completed the patch and verified the output.".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]),
        Message::user("finish the patch"),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, None);
    assert_eq!(
        last_assistant_text(&result.messages).as_deref(),
        Some("Completed the patch and verified the output.")
    );
}

#[tokio::test]
async fn query_loop_requests_final_report_when_turn_ends_with_tool_status_text() {
    let result = run_query_loop(
        &test_context_with_turns(
            vec![
                vec![
                    StreamEvent::MessageStart,
                    StreamEvent::TextDelta("tool Bash result: command succeeded (12 chars)".into()),
                    StreamEvent::MessageStop {
                        stop_reason: StopReason::EndTurn,
                    },
                ],
                vec![
                    StreamEvent::MessageStart,
                    StreamEvent::TextDelta(
                        "Implemented the requested change, validation passed, and no remaining risk was found."
                            .into(),
                    ),
                    StreamEvent::MessageStop {
                        stop_reason: StopReason::EndTurn,
                    },
                ],
            ],
            ToolRegistry::new(),
        ),
        Message::user("wrap up the task"),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::FinalUserReport));
    assert_eq!(
        last_assistant_text(&result.messages).as_deref(),
        Some(
            "Implemented the requested change, validation passed, and no remaining risk was found."
        )
    );
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Transition(Continue::FinalUserReport)))
    );
}

#[tokio::test]
async fn query_loop_synthesizes_final_report_after_invalid_final_report_retry() {
    let result = run_query_loop(
        &test_context_with_turns(
            vec![
                vec![
                    StreamEvent::MessageStart,
                    StreamEvent::TextDelta("tool batch result:\nRead succeeded".into()),
                    StreamEvent::MessageStop {
                        stop_reason: StopReason::EndTurn,
                    },
                ],
                vec![
                    StreamEvent::MessageStart,
                    StreamEvent::TextDelta("tool Read result: alpha".into()),
                    StreamEvent::MessageStop {
                        stop_reason: StopReason::EndTurn,
                    },
                ],
            ],
            ToolRegistry::new(),
        ),
        Message::user("finish the response"),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::FinalUserReport));
    let final_assistant =
        last_assistant_text(&result.messages).expect("synthetic final report should be appended");
    assert_eq!(
        final_assistant,
        "Final update: completed the requested runtime work, but the runtime had to synthesize this closing report because the model did not provide one."
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| { message.text() == "tool batch result:\nRead succeeded" })
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.text() == "tool Read result: alpha")
    );
}

#[tokio::test]
async fn query_loop_skips_tool_execution_during_final_report_retry() {
    let result = run_query_loop(
        &test_context_with_turns(
            vec![
                vec![
                    StreamEvent::MessageStart,
                    StreamEvent::TextDelta("tool Bash result: command succeeded (12 chars)".into()),
                    StreamEvent::MessageStop {
                        stop_reason: StopReason::EndTurn,
                    },
                ],
                vec![
                    StreamEvent::MessageStart,
                    StreamEvent::ToolUse {
                        tool_name: "EchoFixture".into(),
                        input: r#"{"value":"123"}"#.into(),
                    },
                    StreamEvent::MessageStop {
                        stop_reason: StopReason::ToolUse,
                    },
                ],
            ],
            ToolRegistry::new().register(Arc::new(EchoFixtureTool)),
        ),
        Message::user("close out the task"),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::FinalUserReport));
    assert_eq!(
        last_assistant_text(&result.messages).as_deref(),
        Some(
            "Final update: completed the requested runtime work, but the runtime had to synthesize this closing report because the model did not provide one."
        )
    );
    assert!(
        !result
            .messages
            .iter()
            .any(|message| { message.text().contains("tool EchoFixture result:") })
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::ToolCallStarted { tool_name, .. } if tool_name == "EchoFixture"
    )));
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
    assert!(!report.contains("model unknown ->"));
    assert!(report.contains("cache_creation_input_tokens: 10"));
    assert!(report.contains("cache_read_input_tokens: 5"));
}

#[tokio::test]
async fn query_loop_uses_latest_usage_without_double_counting() {
    let context = test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::Usage(UsageEvent {
            model: "default-model".into(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        }),
        StreamEvent::TextDelta("usage tracked".into()),
        StreamEvent::Usage(UsageEvent {
            model: "default-model".into(),
            input_tokens: 101,
            output_tokens: 24,
            cache_creation_input_tokens: 2,
            cache_read_input_tokens: 1,
        }),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]);

    let result = run_query_loop(&context, Message::user("track usage")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    let snapshot = context.app_state.cost_tracker.snapshot();
    assert_eq!(snapshot.requests, 1);
    assert_eq!(snapshot.input_tokens, 101);
    assert_eq!(snapshot.output_tokens, 24);
    assert_eq!(snapshot.cache_creation_input_tokens, 2);
    assert_eq!(snapshot.cache_read_input_tokens, 1);
    let report = context.app_state.cost_tracker.format_report();
    assert!(report.contains("model default-model -> requests: 1"));
    assert!(!report.contains("model unknown ->"));
    assert!(report.contains("input_tokens: 101"));
    assert!(report.contains("output_tokens: 24"));
    assert!(report.contains("cache_creation_input_tokens: 2"));
    assert!(report.contains("cache_read_input_tokens: 1"));
    assert_eq!(
        result
            .events
            .iter()
            .filter(|event| matches!(event, EngineEvent::Notice { kind, .. } if kind == &"usage"))
            .count(),
        1
    );
}

#[tokio::test]
async fn query_loop_records_usage_emitted_after_terminal_stop() {
    let context = test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("openai usage-only tail".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
        StreamEvent::Usage(UsageEvent {
            model: "gpt-5-mini-2025-08-07".into(),
            input_tokens: 2048,
            output_tokens: 64,
            cache_creation_input_tokens: 1024,
            cache_read_input_tokens: 1536,
        }),
    ]);

    let result = run_query_loop(&context, Message::user("track post-stop usage")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    let snapshot = context.app_state.cost_tracker.snapshot();
    assert_eq!(snapshot.requests, 1);
    assert_eq!(snapshot.input_tokens, 2048);
    assert_eq!(snapshot.output_tokens, 64);
    assert_eq!(snapshot.cache_creation_input_tokens, 1024);
    assert_eq!(snapshot.cache_read_input_tokens, 1536);
    assert!(
        result
            .events
            .iter()
            .any(|event| matches!(event, EngineEvent::Notice { kind, .. } if kind == &"usage"))
    );
}

#[tokio::test]
async fn engine_stream_turn_yields_committed_messages() {
    let mut engine = QueryEngine::new(test_context(vec![
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
async fn engine_stream_turn_returns_before_slow_tool_finishes() {
    let registry = ToolRegistry::new().register(Arc::new(SlowFixtureTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("planning".into()),
                StreamEvent::ToolUse {
                    tool_name: "SlowFixture".into(),
                    input: "{}".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));

    let started = Instant::now();
    let mut receiver = timeout(
        Duration::from_millis(25),
        engine.stream_turn(Message::user("run slow tool")),
    )
    .await
    .expect("stream_turn should return before the tool finishes");
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "stream_turn blocked for {:?}",
        started.elapsed()
    );

    let first = timeout(Duration::from_millis(25), receiver.recv())
        .await
        .expect("expected an early event")
        .expect("receiver should stay open");
    assert!(matches!(first, EngineEvent::AssistantDelta(text) if text == "planning"));

    let second = timeout(Duration::from_millis(25), receiver.recv())
        .await
        .expect("expected tool start before tool completion")
        .expect("receiver should stay open");
    assert!(matches!(
        second,
        EngineEvent::ToolCallStarted { ref tool_name, .. } if tool_name == "SlowFixture"
    ));

    let tool_result = timeout(Duration::from_millis(250), async {
        while let Some(event) = receiver.recv().await {
            if matches!(
                &event,
                EngineEvent::ToolResultCommitted { tool_name, .. } if tool_name == "SlowFixture"
            ) {
                return event;
            }
        }
        panic!("stream closed before tool result event");
    })
    .await
    .expect("tool result should arrive after the slow tool finishes");
    assert!(matches!(
        tool_result,
        EngineEvent::ToolResultCommitted { ref tool_name, .. } if tool_name == "SlowFixture"
    ));
}

#[tokio::test]
async fn submit_turn_aggregates_the_same_streamed_event_sequence() {
    let turns = vec![vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("same ".into()),
        StreamEvent::TextDelta("path".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]];
    let mut stream_engine =
        QueryEngine::new(test_context_with_turns(turns.clone(), ToolRegistry::new()));
    let mut submit_engine = QueryEngine::new(test_context_with_turns(turns, ToolRegistry::new()));

    let submit_result = submit_engine
        .submit_turn(Message::user("compare paths"))
        .await;
    let mut receiver = stream_engine
        .stream_turn(Message::user("compare paths"))
        .await;
    let mut streamed_events = Vec::new();
    while let Some(event) = receiver.recv().await {
        streamed_events.push(event);
    }

    assert_eq!(streamed_events, submit_result.events);
    assert_eq!(submit_result.terminal, Terminal::Completed);
    assert_eq!(submit_result.transition, None);
    assert!(
        submit_result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("same path"))
    );
}

#[tokio::test]
async fn engine_stream_turn_receiver_drop_cancels_background_turn() {
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let registry = ToolRegistry::new().register(Arc::new(CancellableFixtureTool {
        started: started.clone(),
        dropped: dropped.clone(),
        completed: completed.clone(),
    }));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("planning".into()),
                StreamEvent::ToolUse {
                    tool_name: "CancellableFixture".into(),
                    input: "{}".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("should not finish".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));

    let mut receiver = engine
        .stream_turn(Message::user("cancel via receiver drop"))
        .await;

    let first = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected an early delta event")
        .expect("receiver should stay open");
    assert!(matches!(first, EngineEvent::AssistantDelta(text) if text == "planning"));

    let second = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected the tool start event")
        .expect("receiver should stay open");
    assert!(matches!(
        second,
        EngineEvent::ToolCallStarted { ref tool_name, .. } if tool_name == "CancellableFixture"
    ));

    timeout(Duration::from_millis(50), async {
        while !started.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("tool should have started");

    drop(receiver);

    timeout(Duration::from_millis(150), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dropping the receiver should cancel the in-flight tool");
    assert!(!completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn engine_stream_turn_parent_cancellation_emits_aborted_terminal() {
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let registry = ToolRegistry::new().register(Arc::new(CancellableFixtureTool {
        started: started.clone(),
        dropped: dropped.clone(),
        completed: completed.clone(),
    }));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("planning".into()),
            StreamEvent::ToolUse {
                tool_name: "CancellableFixture".into(),
                input: "{}".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        registry,
    ));

    let mut receiver = engine
        .stream_turn(Message::user("cancel via app token"))
        .await;

    let first = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected an early delta event")
        .expect("receiver should stay open");
    assert!(matches!(first, EngineEvent::AssistantDelta(text) if text == "planning"));

    let second = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected the tool start event")
        .expect("receiver should stay open");
    assert!(matches!(
        second,
        EngineEvent::ToolCallStarted { ref tool_name, .. } if tool_name == "CancellableFixture"
    ));

    timeout(Duration::from_millis(50), async {
        while !started.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("tool should have started");

    engine.context.app_state.cancellation_token.cancel();

    let terminal = timeout(Duration::from_millis(200), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::Terminal(terminal) = event {
                return terminal;
            }
        }
        panic!("stream ended without a terminal event");
    })
    .await
    .expect("parent cancellation should end the turn promptly");

    assert_eq!(terminal, Terminal::AbortedStreaming);
    timeout(Duration::from_millis(150), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("tool future should be dropped on cancellation");
    assert!(!completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn engine_stream_turn_parent_cancellation_aborts_worker_mailbox_wait() {
    let mut context = test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("worker done".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]],
        ToolRegistry::new(),
    );
    context.app_state.runtime_role = RuntimeRole::Worker;
    context.agent_id = Some("worker-mailbox".into());

    let mut engine = QueryEngine::new(context);
    let mut receiver = engine
        .stream_turn(Message::user("wait for mailbox follow-up"))
        .await;

    let first = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected an early delta event")
        .expect("receiver should stay open");
    assert!(matches!(first, EngineEvent::AssistantDelta(text) if text == "worker done"));

    let committed = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected the committed message before mailbox wait")
        .expect("receiver should stay open");
    assert!(matches!(
        committed,
        EngineEvent::MessageCommitted(ref message) if message == &Message::assistant("worker done")
    ));

    engine.context.app_state.cancellation_token.cancel();

    let terminal = timeout(Duration::from_millis(200), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::Terminal(terminal) = event {
                return terminal;
            }
        }
        panic!("stream ended without a terminal event");
    })
    .await
    .expect("mailbox wait should abort promptly on parent cancellation");

    assert_eq!(terminal, Terminal::AbortedStreaming);
}

#[tokio::test]
async fn engine_rejects_second_turn_while_owner_is_busy() {
    let registry = ToolRegistry::new().register(Arc::new(SlowFixtureTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("planning".into()),
                StreamEvent::ToolUse {
                    tool_name: "SlowFixture".into(),
                    input: "{}".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));

    let mut receiver = engine.stream_turn(Message::user("run slow tool")).await;
    assert!(engine.has_active_turn());

    let first = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected an early delta event")
        .expect("receiver should stay open");
    assert!(matches!(first, EngineEvent::AssistantDelta(text) if text == "planning"));

    let busy = engine.submit_turn(Message::user("second turn")).await;
    assert_eq!(busy.state, QueryLoopState::Failed);
    assert_eq!(busy.terminal, Terminal::OwnerBusy);
    assert!(matches!(
        busy.events.as_slice(),
        [EngineEvent::RuntimeEvent(runtime), EngineEvent::Terminal(Terminal::OwnerBusy)]
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::OwnerBusy
                && runtime.detail == Terminal::OwnerBusy.as_str()
    ));

    let terminal = timeout(Duration::from_millis(250), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::Terminal(terminal) = event {
                return terminal;
            }
        }
        panic!("stream ended without a terminal event");
    })
    .await
    .expect("first turn should finish normally");
    assert_eq!(terminal, Terminal::Completed);

    timeout(Duration::from_millis(100), async {
        while engine.has_active_turn() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("owner slot should be released after first turn completion");
}

#[tokio::test]
async fn owner_busy_turn_does_not_persist_history() {
    let session_store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("owner-busy-history".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/owner-busy-history".into(),
        last_turn_at: None,
        prompt_seed: None,
    };
    session_store
        .save(snapshot.clone(), SessionHistory::default())
        .expect("seed session history");

    let registry = ToolRegistry::new().register(Arc::new(SlowFixtureTool));
    let mut context = test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("planning".into()),
                StreamEvent::ToolUse {
                    tool_name: "SlowFixture".into(),
                    input: "{}".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    );
    context.app_state.active_session_id = session_id.0.clone();
    context.app_state.session_store = Some(session_store.clone());
    context.app_state.session = Some(snapshot);
    context.app_state.history = Some(SessionHistory::default());

    let mut engine = QueryEngine::new(context);
    let mut receiver = engine.stream_turn(Message::user("run slow tool")).await;

    let _ = timeout(Duration::from_millis(50), receiver.recv())
        .await
        .expect("expected an early delta event")
        .expect("receiver should stay open");

    let (_, history_before_busy) = session_store
        .load(&rust_agent::history::session::SessionRestoreRequest {
            resume: Some(session_id.0.clone()),
            continue_session: false,
        })
        .expect("persisted history should exist before busy rejection");

    let busy = engine
        .submit_turn(Message::user("should be rejected"))
        .await;
    assert_eq!(busy.terminal, Terminal::OwnerBusy);

    let (_, persisted_history) = session_store
        .load(&rust_agent::history::session::SessionRestoreRequest {
            resume: Some(session_id.0.clone()),
            continue_session: false,
        })
        .expect("persisted history should exist");
    assert_eq!(persisted_history, history_before_busy);

    let _ = timeout(Duration::from_millis(250), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::Terminal(terminal) = event {
                return terminal;
            }
        }
        panic!("stream ended without a terminal event");
    })
    .await
    .expect("first turn should finish normally");
}

#[tokio::test]
async fn interrupt_active_turn_cancels_inflight_turn_and_releases_owner_slot() {
    let registry = ToolRegistry::new().register(Arc::new(CancellableFixtureTool {
        started: Arc::new(AtomicBool::new(false)),
        dropped: Arc::new(AtomicBool::new(false)),
        completed: Arc::new(AtomicBool::new(false)),
    }));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("planning".into()),
            StreamEvent::ToolUse {
                tool_name: "CancellableFixture".into(),
                input: "{}".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        registry,
    ));

    let mut receiver = engine
        .stream_turn(Message::user("cancel current turn"))
        .await;
    assert!(engine.has_active_turn());
    assert!(engine.interrupt_active_turn());

    let terminal = timeout(Duration::from_millis(250), async {
        while let Some(event) = receiver.recv().await {
            if let EngineEvent::Terminal(terminal) = event {
                return terminal;
            }
        }
        panic!("stream ended without a terminal event");
    })
    .await
    .expect("interrupt should end the in-flight turn");
    assert_eq!(terminal, Terminal::AbortedStreaming);

    timeout(Duration::from_millis(100), async {
        while engine.has_active_turn() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("owner slot should be released after interrupt");
    assert!(!engine.interrupt_active_turn());
}

#[tokio::test]
async fn cli_streaming_callback_receives_multiple_delta_updates_before_completion() {
    let mut engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("a".into()),
        StreamEvent::TextDelta("b".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]));
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let app_state = engine.context.app_state.clone();
    let updates = Arc::new(Mutex::new(Vec::new()));
    let update_sink = Arc::clone(&updates);

    let output = handle_cli_input_streaming(&router, &mut engine, &app_state, "hi", move |turn| {
        update_sink
            .lock()
            .expect("updates mutex should not be poisoned")
            .push(turn.clone());
    })
    .await
    .expect("cli streaming should succeed");

    let updates = updates
        .lock()
        .expect("updates mutex should not be poisoned")
        .clone();
    assert!(updates.len() >= 3, "expected delta and completion updates");
    assert!(matches!(
        updates.first().and_then(|turn| turn.events.first()),
        Some(CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta { text })) if text == "a"
    ));
    assert!(matches!(
        updates.get(1).and_then(|turn| turn.events.get(1)),
        Some(CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta { text })) if text == "b"
    ));
    assert!(updates.iter().any(|turn| turn.primary_text == "ab"));
    assert_eq!(output.primary_text, "ab");
}

#[tokio::test]
async fn cli_streaming_primary_text_ends_with_final_user_report_after_status_retry() {
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("tool batch result:\nRead succeeded".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta(
                    "Implemented the requested change, validation passed, and no remaining risk was found."
                        .into(),
                ),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    ));
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let app_state = engine.context.app_state.clone();

    let output = handle_cli_input_streaming(&router, &mut engine, &app_state, "wrap up", |_| {})
        .await
        .expect("cli streaming should succeed");

    assert!(output.primary_text.ends_with(
        "Implemented the requested change, validation passed, and no remaining risk was found."
    ));
    assert!(
        !output
            .primary_text
            .ends_with("tool batch result:\nRead succeeded")
    );
}

#[tokio::test]
async fn cli_streaming_surfaces_owner_busy_as_typed_runtime_and_terminal() {
    let registry = ToolRegistry::new().register(Arc::new(SlowFixtureTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("planning".into()),
                StreamEvent::ToolUse {
                    tool_name: "SlowFixture".into(),
                    input: "{}".into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));
    let router = CommandRouter::new(
        Arc::new(CommandRegistry::new()),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let app_state = engine.context.app_state.clone();
    let mut first_receiver = engine.stream_turn(Message::user("run slow tool")).await;

    let _ = timeout(Duration::from_millis(50), first_receiver.recv())
        .await
        .expect("expected an early delta event")
        .expect("receiver should stay open");

    let output = handle_cli_input_streaming(&router, &mut engine, &app_state, "second", |_| {})
        .await
        .expect("owner-busy turn should surface as a normal CLI result");

    assert!(output.primary_text.is_empty());
    assert!(output.events.iter().any(|event| matches!(
        event,
        CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice { runtime_kind, .. })
            if runtime_kind.as_deref() == Some("OwnerBusy")
    )));
    assert!(output.events.iter().any(|event| matches!(
        event,
        CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Terminal { kind, .. })
            if kind == "owner_busy"
    )));

    let _ = timeout(Duration::from_millis(250), async {
        while let Some(event) = first_receiver.recv().await {
            if let EngineEvent::Terminal(terminal) = event {
                return terminal;
            }
        }
        panic!("stream ended without a terminal event");
    })
    .await
    .expect("first turn should finish normally");
}

#[test]
fn test_subagent_context_inherits_activity_tracking() {
    let context = test_context(vec![]);
    let original_ts = 123456789;
    context
        .app_state
        .last_activity_ts
        .store(original_ts, std::sync::atomic::Ordering::Relaxed);

    let sub_context = context.create_subagent_context(
        "sub-agent-1",
        vec![],
        SubagentConfig {
            worker_role: WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
        },
    );

    // Verify same Arc instance or at least same underlying value and atomic shared state
    assert_eq!(
        sub_context
            .app_state
            .last_activity_ts
            .load(std::sync::atomic::Ordering::Relaxed),
        original_ts
    );

    // Verify it's truly shared by updating via sub-context
    let updated_ts = 987654321;
    sub_context
        .app_state
        .last_activity_ts
        .store(updated_ts, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        context
            .app_state
            .last_activity_ts
            .load(std::sync::atomic::Ordering::Relaxed),
        updated_ts
    );

    // Child cancellation should not cancel the parent token.
    assert!(!context.app_state.cancellation_token.is_cancelled());
    sub_context.app_state.cancellation_token.cancel();
    assert!(sub_context.app_state.cancellation_token.is_cancelled());
    assert!(!context.app_state.cancellation_token.is_cancelled());
}

#[test]
fn executor_b_subagent_prompt_keeps_bash_visible() {
    let context = test_context_with_turns(
        vec![],
        // Reproduce the production headless path: the parent registry may already have filtered
        // open-world Bash out before ExecutorB is spawned.
        ToolRegistry::new().register(Arc::new(AgentTool)),
    );

    let sub_context = context.create_subagent_context(
        "executor-b",
        vec![],
        SubagentConfig {
            worker_role: WorkerRole::Implement,
            inherit_context: false,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Implement),
            boss_actor_policy: Some(BossActorPolicy::executor_b(BossStage::Execution)),
        },
    );

    assert!(
        sub_context.tools_prompt.contains("Bash -"),
        "ExecutorB prompt must expose Bash for script execution and verification; prompt: {}",
        sub_context.tools_prompt
    );
    assert!(
        sub_context
            .tool_registry
            .all_metadata()
            .iter()
            .any(|tool| tool.name == "Bash"),
        "ExecutorB registry must retain Bash"
    );
}

#[tokio::test]
async fn query_loop_collects_text_until_end_turn() {
    let mut engine = QueryEngine::new(test_context(vec![
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
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("hello world"))
    );
}

#[tokio::test]
async fn query_loop_invokes_tool_and_continues_follow_up_turn() {
    let registry = ToolRegistry::new().register(Arc::new(AgentTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
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
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("planning..."))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("tool Agent result:")
                && message.content.contains(": "))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("done after tool"))
    );
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
async fn query_loop_executes_multiple_tool_calls_from_one_turn() {
    let registry = ToolRegistry::new().register(Arc::new(EchoFixtureTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "EchoFixture".into(),
                    input: r#"{"value":"first"}"#.into(),
                },
                StreamEvent::ToolUse {
                    tool_name: "EchoFixture".into(),
                    input: r#"{"value":"second"}"#.into(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("done after batch".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        registry,
    ));

    let result = engine.submit_turn(Message::user("run both tools")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ToolUseFollowUp));
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("echoed first"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("echoed second"))
    );
    let committed_tool_results = result
        .events
        .iter()
        .filter(|event| matches!(event, EngineEvent::ToolResultCommitted { .. }))
        .count();
    assert_eq!(committed_tool_results, 2);
}

#[tokio::test]
async fn query_loop_surfaces_progress_record_summary_and_detail() {
    let registry = ToolRegistry::new().register(Arc::new(ProgressFixtureTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
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
        EngineEvent::Notice { kind, message, .. }
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
        EngineEvent::Notice { kind, message, .. }
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
                && pending.code.is_none()
                && pending.summary.as_deref() == Some("PendingApprovalFixture pending approval")
                && pending.detail.as_deref() == Some("requires explicit approval")
                && pending.approval_kind.as_deref() == Some("tool_permission")
                && pending.escalation_reasons.is_empty()
    ));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::PendingApproval {
            tool_name,
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
            ..
        } if tool_name == "PendingApprovalFixture"
            && code.is_none()
            && summary == "PendingApprovalFixture pending approval"
            && detail.as_deref() == Some("requires explicit approval")
            && approval_kind.as_deref() == Some("tool_permission")
            && escalation_reasons.is_empty()
    )));
    assert!(result.messages.iter().any(|message| {
        message.content
            == "approval required for PendingApprovalFixture: requires explicit approval"
    }));
}

#[tokio::test]
async fn query_loop_uses_max_output_escalation_then_recovery() {
    let mut engine = QueryEngine::new(test_context_with_turns(
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
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("partial"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("completed"))
    );
}

#[test]
fn compact_service_returns_typed_auto_compact_contract() {
    let compactor = ReactiveCompactor;
    let compact = compactor
        .plan_auto_compact(
            AUTO_COMPACT_INPUT_CHAR_LIMIT + 1,
            AUTO_COMPACT_INPUT_CHAR_LIMIT,
        )
        .expect("oversized input should request auto compact");

    assert_eq!(
        compact.plan.kind,
        rust_agent::service::compact::CompactPlanKind::AutoCompact
    );
    assert_eq!(
        compact.next_step,
        CompactServiceNextStep::RetryReactiveCompact
    );
    assert_eq!(compact.tracking_key, "auto_compact");
    assert!(!compact.should_record_observability_hit);
    assert_eq!(
        compact.plan.assistant_message.as_deref(),
        Some("compaction requested before continuing the turn")
    );
    assert_eq!(compact.plan.retry_prompt, None);
}

#[tokio::test]
async fn query_loop_requests_compaction_for_large_input() {
    let mut engine = QueryEngine::new(test_context(Vec::new()));
    let oversized = "x".repeat(AUTO_COMPACT_INPUT_CHAR_LIMIT + 1);

    let result = engine.submit_turn(Message::user(oversized)).await;

    assert_eq!(result.state, QueryLoopState::Compacting);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ReactiveCompactRetry));
    assert!(
        result.messages.iter().any(|message| message
            == &Message::assistant("compaction requested before continuing the turn"))
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::CompactPlanIssued { kind, message }
            if *kind == rust_agent::service::compact::CompactPlanKind::AutoCompact
                && message == "reactive compact requested before continuing the turn"
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "compaction",
            message,
            ..
        } if message == "reactive compact requested before continuing the turn"
    )));

    let snapshot = engine
        .context
        .app_state
        .service_observability_tracker
        .snapshot();
    assert_eq!(snapshot.compact_recovery_hits.get("reactive_compact"), None);
    assert_eq!(snapshot.compact_recovery_hits.get("collapse_drain"), None);
}

#[tokio::test]
async fn query_loop_surfaces_stream_errors_after_recovery_attempt() {
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "boom".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "boom again".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
        ],
        ToolRegistry::new(),
    ));

    let result = engine.submit_turn(Message::user("trigger error")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::CollapseDrainRetry));
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("stream error: boom"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("stream error: boom again"))
    );
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            message,
            ..
        }
        if message.contains("collapse drain retry")
    )));
}

#[tokio::test]
async fn query_loop_treats_pre_stream_terminal_errors_as_immediate_terminal_failures() {
    let mut engine = QueryEngine::new(test_context(vec![StreamEvent::Error(StreamError {
        provider_id: "anthropic".into(),
        kind: "http_status".into(),
        message: "provider request failed with status 400".into(),
        retryable: false,
        disposition: ProviderFailureDisposition::PreStreamTerminal,
        status_code: Some(400),
    })]));

    let result = engine
        .submit_turn(Message::user("trigger pre-stream terminal"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "provider request failed with status 400".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiProviderHttp4xx),
        }
    );
    assert_eq!(result.transition, None);
    assert!(result.messages.iter().any(|message| {
        message
            .content
            .contains("stream error: provider request failed")
    }));
    assert!(!result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            ..
        }
    )));
}

#[tokio::test]
async fn query_loop_treats_pre_stream_retryable_errors_as_terminal_after_retries_exhaust() {
    let mut engine = QueryEngine::new(test_context(vec![StreamEvent::Error(StreamError {
        provider_id: "anthropic".into(),
        kind: "timeout".into(),
        message: "provider request timed out".into(),
        retryable: true,
        disposition: ProviderFailureDisposition::PreStreamRetryable,
        status_code: None,
    })]));

    let result = engine
        .submit_turn(Message::user("trigger pre-stream retryable"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "provider request timed out".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiProviderTimeout),
        }
    );
    assert_eq!(result.transition, None);
    assert!(!result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            ..
        }
    )));
}

#[tokio::test]
async fn query_loop_treats_retry_exhausted_connection_failures_as_terminal_failures() {
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "first interrupted response".into(),
                retryable: true,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "connection_reset".into(),
                message: "connection reset by peer".into(),
                retryable: true,
                disposition: ProviderFailureDisposition::PreStreamRetryable,
                status_code: None,
            })],
        ],
        ToolRegistry::new(),
    ));

    let result = engine
        .submit_turn(Message::user("trigger exhausted connection failure"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "connection reset by peer".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiProviderTransport),
        }
    );
    assert_eq!(result.transition, Some(Continue::ModelFallbackRetry));
}

#[tokio::test]
async fn query_loop_treats_stream_terminal_protocol_errors_as_immediate_terminal_failures() {
    let mut engine = QueryEngine::new(test_context(vec![StreamEvent::Error(StreamError {
        provider_id: "anthropic".into(),
        kind: "sse_protocol".into(),
        message: "tool_use block ended without complete input payload".into(),
        retryable: false,
        disposition: ProviderFailureDisposition::StreamTerminal,
        status_code: None,
    })]));

    let result = engine
        .submit_turn(Message::user("trigger stream terminal"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "tool_use block ended without complete input payload".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamProtocol),
        }
    );
    assert_eq!(result.transition, None);
    assert!(!result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            ..
        }
    )));
}

#[tokio::test]
async fn query_loop_maps_tool_use_protocol_errors_to_stream_protocol_code() {
    let mut engine = QueryEngine::new(test_context(vec![StreamEvent::Error(StreamError {
        provider_id: "anthropic".into(),
        kind: "tool_use_protocol".into(),
        message: "tool stop without tool payload".into(),
        retryable: false,
        disposition: ProviderFailureDisposition::StreamTerminal,
        status_code: None,
    })]));

    let result = engine
        .submit_turn(Message::user("trigger tool protocol error"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "tool stop without tool payload".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamProtocol),
        }
    );
}

#[tokio::test]
async fn query_loop_maps_structured_output_invalid_errors_to_stream_protocol_code() {
    let mut engine = QueryEngine::new(test_context(vec![StreamEvent::Error(StreamError {
        provider_id: "anthropic".into(),
        kind: "structured_output_invalid".into(),
        message: "structured output block ended without complete JSON payload".into(),
        retryable: false,
        disposition: ProviderFailureDisposition::StreamTerminal,
        status_code: None,
    })]));

    let result = engine
        .submit_turn(Message::user("trigger structured output error"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "structured output block ended without complete JSON payload".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamProtocol),
        }
    );
}

#[tokio::test]
async fn query_loop_treats_stop_reason_error_as_terminal_protocol_failure() {
    let mut engine = QueryEngine::new(test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::MessageStop {
            stop_reason: StopReason::Error,
        },
    ]));

    let result = engine
        .submit_turn(Message::user("trigger stop error"))
        .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "stream stopped with error".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamProtocol),
        }
    );
    assert_eq!(result.transition, None);
}

#[tokio::test]
async fn query_loop_compensates_missing_tool_result_after_tool_failure() {
    let mut engine = QueryEngine::new(test_context(vec![
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

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ToolUseFollowUp));
    assert!(result.messages.iter().any(|message| {
        message.content.contains("tool result for MissingTool:")
            && message.content.contains("status=failed")
            && message.content.contains("reason=unknown_tool")
            && message.content.contains("unknown tool MissingTool")
    }));
    assert!(
        result.messages.iter().any(|message| message
            .content
            .contains("tool result for MissingTool: status=failed"))
            || result
                .events
                .iter()
                .any(|event| matches!(event, EngineEvent::Transition(Continue::ToolUseFollowUp)))
    );
}

#[tokio::test]
async fn query_loop_preserves_synthesized_missing_tool_result_for_denied_tool() {
    let registry = ToolRegistry::new().register(Arc::new(DeniedFixtureTool));
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::ToolUse {
                tool_name: "DeniedFixture".into(),
                input: "payload".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        registry,
    ));

    let result = engine
        .submit_turn(Message::user("trigger denied tool"))
        .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ToolUseFollowUp));
    assert!(result.messages.iter().any(|message| {
        message
            .content
            .contains("tool DeniedFixture denied: requires policy escalation")
    }));
    assert!(result.messages.iter().any(|message| {
        message
            .content
            .contains("tool DeniedFixture result missing; synthesized denial result preserved")
    }));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::MessageCommitted(message)
            if message
                .content
                .contains("tool DeniedFixture result missing; synthesized denial result preserved")
    )));
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

    let mut engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::StopHookPrevented);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("done"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("stop hook appended message"))
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

    let mut engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("denied by hook"))
    );
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
    assert!(
        engine
            .context
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::PreToolUse {
                tool_name: "Agent".into(),
            })
    );
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

    let mut engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Interrupted);
    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("permission request observed"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("denied before execution"))
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

    let mut engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::StopHookBlocking));
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("draft answer"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("revised answer"))
    );
    assert_eq!(
        result
            .messages
            .iter()
            .filter(|message| *message == &Message::assistant("stop hook requires revision"))
            .count(),
        2
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
            ..
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

    let mut engine = QueryEngine::new(context);
    let result = engine.submit_turn(Message::user("inspect file")).await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::StopHookPrevented);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("subagent done"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("subagent stop appended message"))
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

    let events = parse_anthropic_sse_response("anthropic", body, "default-model")
        .expect("provider SSE should parse");
    assert!(matches!(events[0], StreamEvent::MessageStart));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, StreamEvent::TextDelta(text) if text == "hello"))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, StreamEvent::Usage(usage)
        if usage.model == "claude-test"
            && usage.input_tokens == 11
            && usage.output_tokens == 4))
    );
    assert!(matches!(
        events.last(),
        Some(StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn
        })
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
    let provider_terminal = ApiError::http_status(503, "provider says do not retry")
        .with_disposition(ProviderFailureDisposition::PreStreamTerminal);

    assert!(policy.should_retry(0, &retryable, false));
    assert!(!policy.should_retry(2, &retryable, false));
    assert!(!policy.should_retry(0, &retryable, true));
    assert!(!policy.should_retry(0, &fatal, false));
    assert!(!policy.should_retry(0, &provider_terminal, false));
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
    let mut engine = QueryEngine::new(context);
    let engine_handle =
        tokio::spawn(async move { engine.submit_turn(Message::user("initial")).await });

    tokio::task::yield_now().await;
    assert!(manager.send_message(&task.id, "test-session", "follow-up"));

    let result = timeout(Duration::from_secs(4), engine_handle)
        .await
        .expect("worker should finish")
        .expect("join should succeed");

    assert_eq!(result.state, QueryLoopState::Completed);
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("first answer"))
    );
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("second answer"))
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: vec!["parent-runtime".into()],
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
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
    assert_ordered_sections(
        &child.context_prompt,
        &["Git context:", "Session memory:", "Runtime user context:"],
    );

    let result = QueryEngine::new(child.clone())
        .submit_turn(Message::user("run child"))
        .await;
    assert!(
        result
            .messages
            .iter()
            .any(|message| message == &Message::assistant("inherited stop hook"))
    );
    assert!(
        child
            .hook_registry
            .recorded_events()
            .contains(&HookEvent::SubagentStop)
    );
}

#[tokio::test]
async fn subagent_context_inherits_active_model_snapshot_without_sharing_handle() {
    let parent_client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("parent runtime reply".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]]);
    let parent_runtime = ActiveModelRuntime::new(ActiveModelRuntimeSnapshot {
        config: rust_agent::service::api::client::ModelProviderConfig::default(),
        client: parent_client.clone(),
        active_profile_name: Some("runtime-profile".into()),
        active_level: None,
        source: rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "runtime-provider".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "runtime.example".into(),
            model: "runtime-model".into(),
            auth_status: "env:RUNTIME_KEY(set)".into(),
        },
    });
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_external_memory_entries(vec!["external note for child".into()]);
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: vec!["parent-runtime".into()],
            active_model_runtime: Some(parent_runtime.clone()),
            active_model_profile_name: Some("runtime-profile".into()),
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "runtime-provider".into(),
                    protocol: "OpenAICompatible".into(),
                    compatibility_profile: "OpenAICompatible".into(),
                    base_url_host: "runtime.example".into(),
                    model: "runtime-model".into(),
                    auth_status: "env:RUNTIME_KEY(set)".into(),
                },
            active_session_id: "parent-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
        "agent-runtime-child",
        vec![],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
        },
    );

    assert_eq!(
        child.app_state.active_model_profile_name.as_deref(),
        Some("runtime-profile")
    );
    assert_eq!(
        child.app_state.active_model_provider_summary.model,
        "runtime-model"
    );
    assert!(child.app_state.active_model_runtime.is_some());
    assert!(!matches!(
        (&parent.app_state.active_model_runtime, &child.app_state.active_model_runtime),
        (Some(parent_runtime), Some(child_runtime)) if std::ptr::eq(parent_runtime, child_runtime)
    ));

    let result = QueryEngine::new(child)
        .submit_turn(Message::user("run child"))
        .await;
    assert!(
        result
            .messages
            .iter()
            .any(|message| message.content.contains("parent runtime reply"))
    );
}

#[tokio::test]
async fn updated_runtime_snapshot_applies_only_to_next_turn_and_new_subagents() {
    let stale_client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("stale turn reply".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]]);
    let old_runtime_client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("old runtime reply".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]]);
    let new_runtime_client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("new runtime reply".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]]);
    let old_runtime = ActiveModelRuntime::new(ActiveModelRuntimeSnapshot {
        config: rust_agent::service::api::client::ModelProviderConfig {
            model_id: "old-runtime-model".into(),
            ..rust_agent::service::api::client::ModelProviderConfig::default()
        },
        client: old_runtime_client,
        active_profile_name: Some("old-profile".into()),
        active_level: None,
        source: rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "old-provider".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "old.example".into(),
            model: "old-runtime-model".into(),
            auth_status: "env:RUNTIME_KEY(set)".into(),
        },
    });
    let new_runtime = ActiveModelRuntime::new(ActiveModelRuntimeSnapshot {
        config: rust_agent::service::api::client::ModelProviderConfig {
            model_id: "new-runtime-model".into(),
            ..rust_agent::service::api::client::ModelProviderConfig::default()
        },
        client: new_runtime_client,
        active_profile_name: Some("new-profile".into()),
        active_level: None,
        source: rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "new-provider".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "new.example".into(),
            model: "new-runtime-model".into(),
            auth_status: "env:RUNTIME_KEY(set)".into(),
        },
    });
    let mut app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default())),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker: ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_model_runtime: Some(old_runtime),
        active_model_profile_name: Some("old-profile".into()),
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "old-provider".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "old.example".into(),
            model: "old-runtime-model".into(),
            auth_status: "env:RUNTIME_KEY(set)".into(),
        },
        active_session_id: "next-turn-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let base_engine = QueryEngine::new(QueryContext {
        app_state: app_state.clone(),
        tool_registry: ToolRegistry::new(),
        api_client: stale_client,
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    });
    let snapshot = RuntimePluginSnapshot {
        command_registry: Arc::new(rust_agent::command::registry::CommandRegistry::new()),
        tool_registry: ToolRegistry::new(),
        runtime_tool_registry: Arc::new(RwLock::new(ToolRegistry::new())),
        hook_registry: HookRegistry::default(),
        plugin_load_result: Arc::new(rust_agent::plugins::types::PluginLoadResult {
            root: std::path::PathBuf::new(),
            source: rust_agent::plugins::types::PluginConfigSource::Missing,
            plugins: Vec::new(),
            diagnostics: Vec::new(),
            orphaned_governance_entries: Vec::new(),
        }),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
    };

    let mut old_turn_engine = build_turn_engine(&app_state, &snapshot, &base_engine);
    let old_turn_result = old_turn_engine.submit_turn(Message::user("old turn")).await;
    assert!(
        old_turn_result
            .messages
            .iter()
            .any(|message| message.content.contains("old runtime reply"))
    );
    assert!(
        !old_turn_result
            .messages
            .iter()
            .any(|message| message.content.contains("new runtime reply"))
    );

    let old_child = old_turn_engine.context.create_subagent_context(
        "child-before-switch",
        vec![],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
        },
    );
    assert_eq!(
        old_child.app_state.active_model_profile_name.as_deref(),
        Some("old-profile")
    );

    app_state.active_model_runtime = Some(new_runtime);
    app_state.active_model_profile_name = Some("new-profile".into());
    app_state.active_model_provider_summary.model = "new-runtime-model".into();

    let mut next_turn_engine = build_turn_engine(&app_state, &snapshot, &base_engine);
    assert_eq!(
        next_turn_engine
            .context
            .app_state
            .active_model_profile_name
            .as_deref(),
        Some("new-profile")
    );
    assert_eq!(
        next_turn_engine
            .context
            .app_state
            .active_model_provider_summary
            .model,
        "new-runtime-model"
    );
    let next_turn_result = next_turn_engine
        .submit_turn(Message::user("next turn"))
        .await;
    assert!(
        next_turn_result
            .messages
            .iter()
            .any(|message| message.content.contains("new runtime reply"))
    );

    let new_child = next_turn_engine.context.create_subagent_context(
        "child-after-switch",
        vec![],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
        },
    );
    assert_eq!(
        new_child.app_state.active_model_profile_name.as_deref(),
        Some("new-profile")
    );
    assert_eq!(
        new_child.app_state.active_model_provider_summary.model,
        "new-runtime-model"
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: vec!["parent-runtime".into()],
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
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
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
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
        child.context_prompt.contains("- history: unavailable"),
        "session memory should be unavailable when inherit_context=false"
    );
    assert!(
        child.context_prompt.contains("External memory:")
            && child.context_prompt.contains("external note only")
    );
    assert_ordered_sections(
        &child.context_prompt,
        &["Git context:", "Session memory:", "Runtime user context:"],
    );
}

#[tokio::test]
async fn subagent_context_reanchors_and_bounds_nested_memory_lineage() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_nested_memory_lineage(vec![
            "agent:orphan:inherit_context=true".into(),
            "session:test-session".into(),
            "agent:first:inherit_context=true".into(),
            "agent:second:inherit_context=false".into(),
            "agent:third:inherit_context=true".into(),
            "agent:fourth:inherit_context=false".into(),
            "agent:fifth:inherit_context=true".into(),
            "agent:sixth:inherit_context=false".into(),
            "agent:seventh:inherit_context=true".into(),
            "agent:eighth:inherit_context=false".into(),
            "bad marker".into(),
        ]);
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: vec!["parent-runtime".into()],
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
        "agent-task-bounded",
        vec![],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
        },
    );

    assert_eq!(
        child.app_state.permission_context.nested_memory_lineage(),
        vec![
            "session:test-session".to_string(),
            "agent:second:inherit_context=false".to_string(),
            "agent:third:inherit_context=true".to_string(),
            "agent:fourth:inherit_context=false".to_string(),
            "agent:fifth:inherit_context=true".to_string(),
            "agent:sixth:inherit_context=false".to_string(),
            "agent:seventh:inherit_context=true".to_string(),
            "agent:agent-task-bounded:inherit_context=true".to_string(),
        ]
    );
    assert!(
        !child
            .context_prompt
            .contains("agent:orphan:inherit_context=true")
    );
    assert!(!child.context_prompt.contains("bad marker"));
}

#[tokio::test]
async fn subagent_context_shares_activity_tracker_and_cancellation_with_parent() {
    let mut parent = test_context(vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("parent".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]);
    let shared_activity = Arc::new(std::sync::atomic::AtomicU64::new(0));
    parent.app_state.last_activity_ts = shared_activity.clone();
    parent.app_state.permission_context = parent
        .app_state
        .permission_context
        .clone()
        .with_last_activity_ts(shared_activity.clone())
        .with_cancellation_token(parent.app_state.cancellation_token.clone());
    shared_activity.store(1, std::sync::atomic::Ordering::Release);

    let child = parent.create_subagent_context(
        "agent-shared-heartbeat",
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta("child heartbeat".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]],
        SubagentConfig {
            worker_role: rust_agent::state::app_state::WorkerRole::Research,
            inherit_context: true,
            max_turns: None,
            allowed_tools: None,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
            boss_actor_policy: None,
        },
    );

    assert!(Arc::ptr_eq(
        &parent.app_state.last_activity_ts,
        &child.app_state.last_activity_ts
    ));

    let result = QueryEngine::new(child.clone())
        .submit_turn(Message::user("run child heartbeat"))
        .await;
    assert!(
        result
            .messages
            .iter()
            .any(|message| *message == Message::assistant("child heartbeat")),
        "child turn should complete with the scripted response"
    );
    assert!(
        shared_activity.load(std::sync::atomic::Ordering::Acquire) > 1,
        "child query-loop activity should refresh the parent session heartbeat"
    );

    parent.app_state.cancellation_token.cancel();
    assert!(child.app_state.cancellation_token.is_cancelled());
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
    match second.terminal {
        Terminal::MaxBudget { budget_usd_cents } => {
            assert!(budget_usd_cents > 0);
            assert!(budget_usd_cents > 1);
        }
        other => panic!("expected MaxBudget terminal, got {other:?}"),
    }
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
            .contains("inspect grouped task results for group-1")
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
    assert_eq!(gated.transition, None);
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
    assert!(
        gated
            .messages
            .iter()
            .any(|message| { message.content.contains("inspect task output for task-0") })
    );

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
    assert!(
        verified
            .messages
            .iter()
            .any(|message| { message.content.contains("inspect task output for task-1") })
    );
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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
    assert_eq!(missing.transition, None);
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
    assert!(
        missing
            .messages
            .iter()
            .any(|message| { message.content.contains("inspect task output for task-0") })
    );

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
    assert!(
        failed
            .messages
            .iter()
            .any(|message| { message.content.contains("inspect task output for task-1") })
    );
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
    manager.launch(&killed_verify.id, "verify", std::future::pending::<()>());
    assert!(manager.kill(&killed_verify.id, "test-session", &dispatcher));

    let killed = QueryEngine::new(context)
        .submit_turn(Message::user("finalize risky implementation"))
        .await;
    assert_eq!(killed.transition, None);
    assert!(
        killed
            .messages
            .iter()
            .any(|message| { message.content.contains("inspect task output for task-3") })
    );
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
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "model_fallback".into(),
                message: "fallback:model_error: upstream overloaded".into(),
                retryable: true,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: Some(503),
            })],
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
            code,
            service_failure,
        } if message.contains("model fallback retry")
            && code == &Some(rust_agent::core::events::ServiceFailureCode::ApiStreamModelFallback)
            && matches!(
                service_failure,
                Some(rust_agent::core::events::ServiceFailureNotice {
                    service_failure_code: rust_agent::core::events::ServiceFailureCode::ApiStreamModelFallback,
                    provider_kind: Some(provider_kind),
                    status_code: Some(503),
                    retryable: true,
                    surface_visible: true,
                }) if provider_kind == "anthropic"
            )
    )));
}

#[tokio::test]
async fn query_loop_escalates_fallback_failure_to_terminal_model_error() {
    let context = test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "model_fallback".into(),
                message: "fallback:model_error: upstream overloaded".into(),
                retryable: true,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: Some(503),
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "model_fallback".into(),
                message: "fallback:model_error: still failing".into(),
                retryable: true,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: Some(503),
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "residual collapse failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "fatal after retries".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
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
        Terminal::ModelError {
            message: "fatal after retries".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamInterrupted),
        }
    );
    assert_eq!(result.transition, Some(Continue::CollapseDrainRetry));
}

#[test]
fn compact_service_returns_typed_stream_error_recovery_contract() {
    let compactor = ReactiveCompactor;

    let reactive = compactor.plan_stream_error_recovery(
        false,
        false,
        Some(rust_agent::service::compact::CompactRecoveryErrorContext {
            kind: "provider_stream",
            message: "first failure",
        }),
    );
    assert_eq!(
        reactive.plan.kind,
        rust_agent::service::compact::CompactPlanKind::ReactiveCompact
    );
    assert_eq!(
        reactive.next_step,
        CompactServiceNextStep::RetryReactiveCompact
    );
    assert_eq!(reactive.tracking_key, "reactive_compact");
    assert!(reactive.should_record_observability_hit);
    assert_eq!(
        reactive.plan.retry_prompt.as_deref(),
        Some("Retry after reactive compact recovery.")
    );

    let collapse = compactor.plan_stream_error_recovery(
        true,
        false,
        Some(rust_agent::service::compact::CompactRecoveryErrorContext {
            kind: "provider_stream",
            message: "second failure",
        }),
    );
    assert_eq!(
        collapse.plan.kind,
        rust_agent::service::compact::CompactPlanKind::CollapseDrain
    );
    assert_eq!(
        collapse.next_step,
        CompactServiceNextStep::RetryCollapseDrain
    );
    assert_eq!(collapse.tracking_key, "collapse_drain");
    assert!(collapse.should_record_observability_hit);
    assert_eq!(
        collapse.plan.retry_prompt.as_deref(),
        Some("Retry after collapse drain recovery.")
    );

    let exhausted = compactor.plan_stream_error_recovery(
        true,
        true,
        Some(rust_agent::service::compact::CompactRecoveryErrorContext {
            kind: "provider_stream",
            message: "third failure",
        }),
    );
    assert_eq!(
        exhausted.plan.kind,
        rust_agent::service::compact::CompactPlanKind::Exhausted
    );
    assert_eq!(exhausted.next_step, CompactServiceNextStep::Exhausted);
    assert_eq!(exhausted.tracking_key, "exhausted");
    assert!(!exhausted.should_record_observability_hit);
    assert_eq!(exhausted.plan.retry_prompt, None);
}

#[tokio::test]
async fn query_loop_uses_reactive_compact_before_collapse_drain_on_first_stream_error() {
    let context = test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "first failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("recovered after reactive compact".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs first recovery"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::ReactiveCompactRetry));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            message,
            ..
        } if message == "reactive compact retry triggered after stream error [provider_stream]: first failure"
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::CompactPlanIssued { kind, message }
            if *kind == rust_agent::service::compact::CompactPlanKind::ReactiveCompact
                && message == "reactive compact retry triggered after stream error [provider_stream]: first failure"
    )));

    let snapshot = context.app_state.service_observability_tracker.snapshot();
    assert_eq!(
        snapshot.compact_recovery_hits.get("reactive_compact"),
        Some(&1)
    );
    assert_eq!(snapshot.compact_recovery_hits.get("collapse_drain"), None);
}

#[tokio::test]
async fn query_loop_uses_collapse_drain_after_reactive_compact_boundary() {
    let context = test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "first failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "second failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("recovered after collapse drain".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("needs collapse drain"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Completed);
    assert_eq!(result.terminal, Terminal::Completed);
    assert_eq!(result.transition, Some(Continue::CollapseDrainRetry));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            message,
            ..
        } if message == "collapse drain retry triggered after repeated stream error [provider_stream]: second failure"
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::CompactPlanIssued { kind, message }
            if *kind == rust_agent::service::compact::CompactPlanKind::CollapseDrain
                && message == "collapse drain retry triggered after repeated stream error [provider_stream]: second failure"
    )));

    let snapshot = context.app_state.service_observability_tracker.snapshot();
    assert_eq!(
        snapshot.compact_recovery_hits.get("reactive_compact"),
        Some(&1)
    );
    assert_eq!(
        snapshot.compact_recovery_hits.get("collapse_drain"),
        Some(&1)
    );
}

#[tokio::test]
async fn query_loop_exhausts_after_collapse_drain_and_surfaces_terminal_error() {
    let context = test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "first failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "second failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "third failure".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
        ],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("recovery exhausted"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "third failure".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamInterrupted),
        }
    );
    assert_eq!(result.transition, Some(Continue::CollapseDrainRetry));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::Notice {
            kind: "recovery",
            message,
            ..
        } if message == "stream recovery exhausted after error [provider_stream]: third failure"
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::CompactPlanIssued { kind, message }
            if *kind == rust_agent::service::compact::CompactPlanKind::Exhausted
                && message == "stream recovery exhausted after error [provider_stream]: third failure"
    )));

    let snapshot = context.app_state.service_observability_tracker.snapshot();
    assert_eq!(
        snapshot.compact_recovery_hits.get("reactive_compact"),
        Some(&1)
    );
    assert_eq!(
        snapshot.compact_recovery_hits.get("collapse_drain"),
        Some(&1)
    );
}

#[tokio::test]
async fn query_loop_terminal_stream_failures_do_not_enter_recovery() {
    let context = test_context_with_turns(
        vec![vec![StreamEvent::Error(StreamError {
            provider_id: "anthropic".into(),
            kind: "provider_terminal".into(),
            message: "fatal terminal failure".into(),
            retryable: false,
            disposition: ProviderFailureDisposition::StreamTerminal,
            status_code: Some(400),
        })]],
        ToolRegistry::new(),
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user("terminal failure"),
        QueryParams::default(),
    )
    .await;

    assert_eq!(result.state, QueryLoopState::Failed);
    assert_eq!(
        result.terminal,
        Terminal::ModelError {
            message: "fatal terminal failure".into(),
            code: Some(rust_agent::core::events::ServiceFailureCode::ApiStreamTerminal),
        }
    );
    assert_eq!(result.transition, None);
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
    let mut engine = QueryEngine::new(test_context_with_turns(
        vec![
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "model_fallback".into(),
                message: "fallback:model_error: upstream overloaded".into(),
                retryable: true,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: Some(503),
            })],
            vec![StreamEvent::Error(StreamError {
                provider_id: "anthropic".into(),
                kind: "provider_stream".into(),
                message: "still overloaded".into(),
                retryable: false,
                disposition: ProviderFailureDisposition::StreamInterrupted,
                status_code: None,
            })],
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
                && runtime.code
                    == Some(rust_agent::core::events::ServiceFailureCode::CompactRecoveryError)
                && matches!(
                    runtime.service_failure.as_ref(),
                    Some(rust_agent::core::events::ServiceFailureNotice {
                        service_failure_code: rust_agent::core::events::ServiceFailureCode::CompactRecoveryError,
                        provider_kind: None,
                        status_code: None,
                        retryable: true,
                        surface_visible: true,
                    })
                )
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::RetryScheduled
                && runtime.detail == Continue::ModelFallbackRetry.as_str()
                && runtime.code
                    == Some(rust_agent::core::events::ServiceFailureCode::ApiStreamModelFallback)
                && matches!(
                    runtime.service_failure.as_ref(),
                    Some(rust_agent::core::events::ServiceFailureNotice {
                        service_failure_code: rust_agent::core::events::ServiceFailureCode::ApiStreamModelFallback,
                        provider_kind: None,
                        status_code: None,
                        retryable: true,
                        surface_visible: true,
                    })
                )
    )));
    assert!(result.events.iter().any(|event| matches!(
        event,
        EngineEvent::RuntimeEvent(runtime)
            if runtime.kind == rust_agent::core::events::RuntimeEventKind::NormalTerminal
                && runtime.detail == Terminal::Completed.as_str()
    )));

    let snapshot = engine
        .context
        .app_state
        .service_observability_tracker
        .snapshot();
    assert_eq!(
        snapshot.by_failure_code.get("api_stream_model_fallback"),
        Some(&1)
    );
    assert_eq!(
        snapshot.by_failure_code.get("compact_recovery_error"),
        Some(&2)
    );
    assert_eq!(snapshot.retryable_count, 3);
    assert_eq!(snapshot.terminal_count, 0);
    assert!(snapshot.by_provider_kind.is_empty());
    assert_eq!(
        snapshot.compact_recovery_hits.get("reactive_compact"),
        Some(&1)
    );
}

#[tokio::test]
async fn submit_turn_distinguishes_stop_hook_prevented_and_blocking_runtime_events() {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()));
    permission_context.add_always_allow_rule("Agent");

    let mut prevented_engine = QueryEngine::new(QueryContext {
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

    let mut blocking_engine = QueryEngine::new(QueryContext {
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
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "default-provider".into(),
                    protocol: "Anthropic".into(),
                    compatibility_profile: "Anthropic".into(),
                    base_url_host: "localhost".into(),
                    model: "default-model".into(),
                    auth_status: "env:OPENAI_API_KEY(unset)".into(),
                },
            active_session_id: "test-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
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

struct EchoTool;

#[async_trait]
impl rust_agent::tool::definition::Tool for EchoTool {
    fn metadata(&self) -> rust_agent::tool::definition::ToolMetadata {
        rust_agent::tool::definition::ToolMetadata {
            name: "Echo".into(),
            description: "Echoes input".into(),
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
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::Text(format!("echo: {}", call.input)))
    }
}

#[tokio::test]
async fn hook_ask_does_not_execute_tool_and_produces_pending_approval_event() {
    let registry = ToolRegistry::new().register(Arc::new(EchoTool));
    let hook_registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PermissionRequest,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: Some("ask".into()),
        updated_input: None,
        additional_context: None,
    });

    let mut context = test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::ToolUse {
                tool_name: "Echo".into(),
                input: "hello".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        registry,
    );
    context.hook_registry = hook_registry;

    let result = run_query_loop(&context, Message::user("run echo")).await;

    assert_eq!(result.terminal, Terminal::AbortedTools);
    assert!(
        result.events.iter().any(
            |e| matches!(e, EngineEvent::PendingApproval { tool_name, approval_kind, .. }
                if tool_name == "Echo" && approval_kind.as_deref() == Some("hook_ask"))
        ),
        "expected PendingApproval event with hook_ask kind"
    );
    assert!(
        !result.messages.iter().any(|m| m.content.contains("echo:")),
        "tool must not have executed"
    );
}

#[tokio::test]
async fn hook_ask_sets_pending_approval_in_permission_context() {
    let registry = ToolRegistry::new().register(Arc::new(EchoTool));
    let hook_registry = HookRegistry::default().register_rule(HookRule {
        event: HookEventMatcher::PermissionRequest,
        layer: HookRuleLayer::Defaults,
        deny_match: None,
        append_message: None,
        prevent_continuation: false,
        block_continuation: false,
        permission_decision: Some("ask".into()),
        updated_input: None,
        additional_context: None,
    });

    let mut context = test_context_with_turns(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::ToolUse {
                tool_name: "Echo".into(),
                input: "world".into(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        registry,
    );
    context.hook_registry = hook_registry;

    run_query_loop(&context, Message::user("run echo")).await;

    let pending = context.app_state.permission_context.pending_approval();
    assert!(
        matches!(pending, Some(p) if p.tool_name == "Echo" && p.approval_kind.as_deref() == Some("hook_ask")),
        "permission_context must have pending approval with hook_ask kind"
    );
}

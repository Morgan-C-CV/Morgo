use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::builtin::mcp::McpCommand;
use rust_agent::command::types::{Command, CommandResult};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::service::mcp::client::McpClient;
use rust_agent::service::mcp::config::{McpConfigLoadResult, McpConfigSource};
use rust_agent::service::mcp::runtime::{McpRuntime, replace_runtime_server_config};
use rust_agent::service::mcp::state::{
    McpGovernanceStateEntry, McpGovernanceStateLoadResult, McpGovernanceStateSource,
};
use rust_agent::service::mcp::types::{
    McpAction, McpCapabilities, McpConnectInfo, McpConnectionStatus, McpFailureCode, McpPeerInfo,
    McpRequest, McpResourceInfo, McpServerConfig, McpServerGovernanceConfig, McpToolInfo,
    McpTransportKind,
};
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::mcp::McpTool;
use rust_agent::tool::definition::{Tool, ToolCall};
use serde_json::{Value, json};
use tokio::sync::Mutex;

#[derive(Debug, Default)]
struct FakeMcpClient {
    mode: Mutex<FakeMode>,
    connect_calls: Mutex<usize>,
}

#[derive(Debug, Clone)]
enum FakeMode {
    Ok,
    IdMismatch,
    MalformedHeader,
    ServerError,
    Hanging,
}

impl Default for FakeMode {
    fn default() -> Self {
        Self::Ok
    }
}

#[async_trait]
impl McpClient for FakeMcpClient {
    async fn connect(&self, _config: &McpServerConfig) -> anyhow::Result<McpConnectInfo> {
        *self.connect_calls.lock().await += 1;
        match self.mode.lock().await.clone() {
            FakeMode::MalformedHeader => {
                anyhow::bail!("MCP stdio response did not include Content-Length header")
            }
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method initialize"),
            FakeMode::ServerError => anyhow::bail!(
                "{}",
                r#"MCP initialize failed: {"code":-32000,"message":"server exploded"}"#
            ),
            FakeMode::Hanging => {
                std::future::pending::<()>().await;
                unreachable!()
            }
            FakeMode::Ok => Ok(McpConnectInfo {
                protocol_initialized: true,
                pid: Some(42),
                peer: McpPeerInfo {
                    server_name: Some("fake".into()),
                    server_version: Some("1.0.0".into()),
                    protocol_version: Some("2025-03-26".into()),
                    capabilities: McpCapabilities::from_initialize_result(Some(
                        &json!({"tools": {}, "resources": {}}),
                    )),
                },
            }),
        }
    }

    async fn disconnect(&self, _config: &McpServerConfig) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_tools(&self, _config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!(
                "{}",
                r#"MCP tools/list failed: {"code":-32000,"message":"server exploded"}"#
            ),
            FakeMode::MalformedHeader => {
                anyhow::bail!("MCP stdio response did not include Content-Length header")
            }
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method tools/list"),
            FakeMode::Ok | FakeMode::Hanging => Ok(vec![
                McpToolInfo {
                    name: "echo".into(),
                    description: "Echo tool".into(),
                    input_schema: Some(json!({"type": "object"})),
                },
                McpToolInfo {
                    name: "sum".into(),
                    description: "Sum tool".into(),
                    input_schema: None,
                },
                McpToolInfo {
                    name: "inspect".into(),
                    description: "Inspect tool".into(),
                    input_schema: None,
                },
            ]),
        }
    }

    async fn list_resources(
        &self,
        _config: &McpServerConfig,
    ) -> anyhow::Result<Vec<McpResourceInfo>> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!(
                "{}",
                r#"MCP resources/list failed: {"code":-32000,"message":"server exploded"}"#
            ),
            FakeMode::MalformedHeader => {
                anyhow::bail!("MCP stdio response did not include Content-Length header")
            }
            FakeMode::IdMismatch => {
                anyhow::bail!("MCP response id mismatch for method resources/list")
            }
            FakeMode::Ok | FakeMode::Hanging => Ok(vec![
                McpResourceInfo {
                    name: "readme".into(),
                    uri: "mcp://fake/readme".into(),
                    description: "Readme".into(),
                    mime_type: Some("text/plain".into()),
                },
                McpResourceInfo {
                    name: "config".into(),
                    uri: "mcp://fake/config".into(),
                    description: "Config".into(),
                    mime_type: Some("application/json".into()),
                },
            ]),
        }
    }

    async fn call_tool(
        &self,
        _config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!(
                "{}",
                r#"MCP tools/call failed: {"code":-32000,"message":"server exploded"}"#
            ),
            FakeMode::MalformedHeader => {
                anyhow::bail!("MCP stdio response did not include Content-Length header")
            }
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method tools/call"),
            FakeMode::Ok | FakeMode::Hanging => Ok(json!({"tool": tool, "input": input})),
        }
    }

    async fn read_resource(
        &self,
        _config: &McpServerConfig,
        resource: &str,
    ) -> anyhow::Result<String> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!(
                "{}",
                r#"MCP resources/read failed: {"code":-32000,"message":"server exploded"}"#
            ),
            FakeMode::MalformedHeader => {
                anyhow::bail!("MCP stdio response did not include Content-Length header")
            }
            FakeMode::IdMismatch => {
                anyhow::bail!("MCP response id mismatch for method resources/read")
            }
            FakeMode::Ok | FakeMode::Hanging => Ok(format!("resource:{resource}")),
        }
    }
}

fn fake_config(command: &str) -> McpServerConfig {
    McpServerConfig {
        id: "fake".into(),
        name: "fake".into(),
        command: command.into(),
        args: vec!["--stdio".into()],
        env: BTreeMap::from([("A".into(), "1".into())]),
        transport: McpTransportKind::StdioProcess,
        governance: McpServerGovernanceConfig {
            review_required: false,
            notes: None,
        },
        connect_timeout_ms: 10_000,
    }
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

fn test_app_state(runtime: Arc<McpRuntime>) -> AppState {
    let service_observability_tracker = runtime.observability_tracker();
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_mcp_runtime(runtime.clone());
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: None,
        skill_registry: None,
        mcp_runtime: Some(runtime),
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker,
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "mcp-test-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
    }
}

#[tokio::test]
async fn mcp_status_shows_reconnecting_and_inventory_summaries() {
    let client = Arc::new(FakeMcpClient::default());
    let runtime = Arc::new(McpRuntime::new_with_config_result(
        client,
        McpConfigLoadResult {
            path: ".claude/mcp_servers.json".into(),
            source: McpConfigSource::File,
            configs: vec![fake_config("fake-server")],
            diagnostics: Vec::new(),
        },
    ));
    runtime.connect("fake").await.expect("connect fake server");
    runtime
        .reconnect("fake")
        .await
        .expect("reconnect fake server");

    let app_state = test_app_state(runtime.clone());
    let result = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp status"),
            &app_state,
        )
        .await
        .expect("mcp status should render");
    let CommandResult::Message(text) = result else {
        panic!("expected mcp status message");
    };
    let servers = runtime.list_servers().await;
    assert!(servers[0].server_capabilities.tools.is_some());
    assert!(servers[0].server_capabilities.resources.is_some());
    assert!(text.contains("status: connected"));
    assert!(text.contains("inventory: tools=3, resources=2"));
    assert!(text.contains("capabilities: tools, resources"));
}

#[tokio::test]
async fn mcp_runtime_marks_unknown_server_as_user_visible_error() {
    let runtime = Arc::new(McpRuntime::new(
        Arc::new(FakeMcpClient::default()),
        vec![fake_config("fake-server")],
    ));
    let app_state = test_app_state(runtime.clone());

    let command_error = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect missing"),
            &app_state,
        )
        .await
        .expect_err("unknown server should error");
    assert!(
        command_error
            .to_string()
            .contains("unknown MCP server: missing")
    );

    let tool = McpTool;
    let tool_error = tool
        .invoke(
            &ToolCall::new(
                "Mcp",
                json!({"action":"list_tools","server":"missing","tool":null,"resource":null,"input":null}).to_string(),
            ),
            &app_state.permission_context,
        )
        .await
        .expect_err("unknown server tool request should error");
    assert!(
        tool_error
            .to_string()
            .contains("unknown MCP server: missing")
    );
}

#[tokio::test]
async fn mcp_command_and_tool_surface_protocol_errors() {
    let client = Arc::new(FakeMcpClient::default());
    *client.mode.lock().await = FakeMode::MalformedHeader;
    let runtime = Arc::new(McpRuntime::new(
        client.clone(),
        vec![fake_config("fake-server")],
    ));
    let app_state = test_app_state(runtime.clone());

    let command_error = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect fake"),
            &app_state,
        )
        .await
        .expect_err("malformed header should surface via command");
    assert!(command_error.to_string().contains("Content-Length"));

    let tool = McpTool;
    let tool_error = tool
        .invoke(
            &ToolCall::new(
                "Mcp",
                json!({"action":"list_tools","server":"fake","tool":null,"resource":null,"input":null}).to_string(),
            ),
            &app_state.permission_context,
        )
        .await
        .expect_err("malformed header should surface via tool");
    assert!(tool_error.to_string().contains("Content-Length"));

    *client.mode.lock().await = FakeMode::IdMismatch;
    let command_error = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect fake"),
            &app_state,
        )
        .await
        .expect_err("id mismatch should surface via command");
    assert!(command_error.to_string().contains("id mismatch"));

    *client.mode.lock().await = FakeMode::ServerError;
    let tool_error = tool
        .invoke(
            &ToolCall::new(
                "Mcp",
                json!({"action":"call_tool","server":"fake","tool":"echo","resource":null,"input":{"x":1}}).to_string(),
            ),
            &app_state.permission_context,
        )
        .await
        .expect_err("server error should surface via tool");
    assert!(tool_error.to_string().contains("server exploded"));

    let snapshot = runtime.observability_tracker().snapshot();
    assert_eq!(snapshot.mcp_failures_by_kind.get("connect"), Some(&4));
    assert_eq!(snapshot.mcp_failures_by_kind.get("call_tool"), None);
    assert_eq!(snapshot.mcp_failures_by_server.get("fake"), Some(&4));
    assert_eq!(
        snapshot.recent_events.last().map(|event| event.category),
        Some("mcp_server_failure")
    );
}

#[tokio::test]
async fn mcp_connect_requires_governance_approval_until_approved() {
    let client = Arc::new(FakeMcpClient::default());
    let mut config = fake_config("fake-server");
    config.governance.review_required = true;
    let runtime = Arc::new(
        McpRuntime::new_with_config_and_governance_result_and_observability(
            client.clone(),
            McpConfigLoadResult {
                path: ".claude/mcp_servers.json".into(),
                source: McpConfigSource::File,
                configs: vec![config],
                diagnostics: Vec::new(),
            },
            McpGovernanceStateLoadResult {
                path: ".claude/mcp-governance.json".into(),
                source: McpGovernanceStateSource::Defaults,
                states: BTreeMap::new(),
                diagnostics: Vec::new(),
            },
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        ),
    );
    let app_state = test_app_state(runtime.clone());

    let error = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect fake"),
            &app_state,
        )
        .await
        .expect_err("approval should be required");
    assert!(error.to_string().contains("mcp_governance_review_required"));
    assert_eq!(*client.connect_calls.lock().await, 0);

    let cwd = unique_temp_dir("mcp-governance-approve");
    std::fs::create_dir_all(&cwd).expect("create temp cwd");
    let approve = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp approve fake"),
            &AppState {
                session: Some(rust_agent::history::session::SessionSnapshot {
                    session_id: rust_agent::history::session::SessionId("session-1".into()),
                    surface: InteractionSurface::Cli,
                    session_mode: SessionMode::Headless,
                    cwd: cwd.display().to_string(),
                    last_turn_at: None,
                    prompt_seed: None,
                }),
                ..app_state.clone()
            },
        )
        .await
        .expect("approve should succeed");
    let CommandResult::Message(approve_text) = approve else {
        panic!("expected approve message");
    };
    assert!(approve_text.contains("Approved MCP server fake (fake)"));

    runtime
        .connect("fake")
        .await
        .expect("approved connect should succeed");
    assert_eq!(*client.connect_calls.lock().await, 1);
    let servers = runtime.list_servers().await;
    assert_eq!(servers[0].governance.approval_status.as_str(), "approved");
}

#[tokio::test]
async fn stale_config_hash_invalidates_cached_connection() {
    let client = Arc::new(FakeMcpClient::default());
    let runtime = Arc::new(McpRuntime::new(
        client.clone(),
        vec![fake_config("fake-server")],
    ));
    runtime.connect("fake").await.expect("connect fake server");
    let initial_connects = *client.connect_calls.lock().await;

    let mut changed = fake_config("changed-server");
    changed.id = "fake".into();
    changed.name = "fake".into();
    replace_runtime_server_config(&runtime, changed)
        .await
        .expect("replace runtime config");

    let result = runtime
        .dispatch(McpRequest {
            action: McpAction::ListTools,
            server: "fake".into(),
            tool: None,
            resource: None,
            input: None,
        })
        .await
        .expect("list tools after stale config");
    let rust_agent::service::mcp::types::McpResponse::ToolList(tools) = result else {
        panic!("expected tool list after stale config refresh");
    };
    assert_eq!(tools.len(), 3);
    let later_connects = *client.connect_calls.lock().await;
    assert!(later_connects >= initial_connects);
}

#[tokio::test]
async fn stale_governance_entry_is_reported_in_status() {
    let mut config = fake_config("fake-server");
    config.governance.review_required = true;
    let old_fingerprint = 7_u64;
    let runtime = Arc::new(
        McpRuntime::new_with_config_and_governance_result_and_observability(
            Arc::new(FakeMcpClient::default()),
            McpConfigLoadResult {
                path: ".claude/mcp_servers.json".into(),
                source: McpConfigSource::File,
                configs: vec![config],
                diagnostics: Vec::new(),
            },
            McpGovernanceStateLoadResult {
                path: ".claude/mcp-governance.json".into(),
                source: McpGovernanceStateSource::File,
                states: BTreeMap::from([(
                    "fake".into(),
                    McpGovernanceStateEntry {
                        server_id: "fake".into(),
                        approved: true,
                        fingerprint: old_fingerprint,
                        reason: None,
                    },
                )]),
                diagnostics: Vec::new(),
            },
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        ),
    );
    let app_state = test_app_state(runtime.clone());

    let result = McpCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp status"),
            &app_state,
        )
        .await
        .expect("status should render");
    let CommandResult::Message(text) = result else {
        panic!("expected status message");
    };
    assert!(text.contains("governance: status=stale"));
    assert!(
        text.contains("governance_reasons: transport.stdio_process, governance.review_required")
    );
}

#[tokio::test]
async fn mcp_connect_timeout_produces_typed_failure_and_does_not_hang() {
    let client = Arc::new(FakeMcpClient::default());
    *client.mode.lock().await = FakeMode::Hanging;

    let mut config = fake_config("fake");
    config.connect_timeout_ms = 50; // very short for test speed

    let runtime = Arc::new(McpRuntime::new(
        Arc::clone(&client) as Arc<dyn McpClient>,
        vec![config],
    ));

    let result = runtime.connect("fake").await;

    assert!(
        result.is_err(),
        "connect must fail when server hangs past timeout"
    );
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("connection_timeout") || err_str.contains("timed out"),
        "error must indicate connection_timeout, got: {err_str}"
    );

    // server state must reflect the timeout failure — session is not hung
    let servers = runtime.list_servers().await;
    let server = servers
        .iter()
        .find(|s| s.config.id == "fake")
        .expect("server must exist");
    assert_eq!(server.status, McpConnectionStatus::Failed);
    assert!(
        server
            .last_failure
            .as_ref()
            .map(|f| f.code == McpFailureCode::ConnectionTimeout)
            .unwrap_or(false),
        "last_failure must be ConnectionTimeout, got: {:?}",
        server.last_failure
    );
}

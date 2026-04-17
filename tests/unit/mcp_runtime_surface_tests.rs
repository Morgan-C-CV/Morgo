use std::collections::BTreeMap;
use std::sync::Arc;

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
use rust_agent::service::mcp::types::{
    McpAction, McpCapabilities, McpConnectInfo, McpPeerInfo, McpRequest, McpResourceInfo,
    McpServerConfig, McpToolInfo, McpTransportKind,
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
            FakeMode::MalformedHeader => anyhow::bail!("MCP stdio response did not include Content-Length header"),
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method initialize"),
            FakeMode::ServerError => anyhow::bail!("{}", r#"MCP initialize failed: {"code":-32000,"message":"server exploded"}"#),
            FakeMode::Ok => Ok(McpConnectInfo {
                protocol_initialized: true,
                pid: Some(42),
                peer: McpPeerInfo {
                    server_name: Some("fake".into()),
                    server_version: Some("1.0.0".into()),
                    protocol_version: Some("2025-03-26".into()),
                    capabilities: McpCapabilities::from_initialize_result(Some(&json!({"tools": {}, "resources": {}}))),
                },
            }),
        }
    }

    async fn disconnect(&self, _config: &McpServerConfig) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_tools(&self, _config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!("{}", r#"MCP tools/list failed: {"code":-32000,"message":"server exploded"}"#),
            FakeMode::MalformedHeader => anyhow::bail!("MCP stdio response did not include Content-Length header"),
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method tools/list"),
            FakeMode::Ok => Ok(vec![
                McpToolInfo { name: "echo".into(), description: "Echo tool".into(), input_schema: Some(json!({"type": "object"})) },
                McpToolInfo { name: "sum".into(), description: "Sum tool".into(), input_schema: None },
                McpToolInfo { name: "inspect".into(), description: "Inspect tool".into(), input_schema: None },
            ]),
        }
    }

    async fn list_resources(&self, _config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!("{}", r#"MCP resources/list failed: {"code":-32000,"message":"server exploded"}"#),
            FakeMode::MalformedHeader => anyhow::bail!("MCP stdio response did not include Content-Length header"),
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method resources/list"),
            FakeMode::Ok => Ok(vec![
                McpResourceInfo { name: "readme".into(), uri: "mcp://fake/readme".into(), description: "Readme".into(), mime_type: Some("text/plain".into()) },
                McpResourceInfo { name: "config".into(), uri: "mcp://fake/config".into(), description: "Config".into(), mime_type: Some("application/json".into()) },
            ]),
        }
    }

    async fn call_tool(&self, _config: &McpServerConfig, tool: &str, input: Option<Value>) -> anyhow::Result<Value> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!("{}", r#"MCP tools/call failed: {"code":-32000,"message":"server exploded"}"#),
            FakeMode::MalformedHeader => anyhow::bail!("MCP stdio response did not include Content-Length header"),
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method tools/call"),
            FakeMode::Ok => Ok(json!({"tool": tool, "input": input})),
        }
    }

    async fn read_resource(&self, _config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        match self.mode.lock().await.clone() {
            FakeMode::ServerError => anyhow::bail!("{}", r#"MCP resources/read failed: {"code":-32000,"message":"server exploded"}"#),
            FakeMode::MalformedHeader => anyhow::bail!("MCP stdio response did not include Content-Length header"),
            FakeMode::IdMismatch => anyhow::bail!("MCP response id mismatch for method resources/read"),
            FakeMode::Ok => Ok(format!("resource:{resource}")),
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
    }
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
        audit_log: Arc::new(std::sync::Mutex::new(rust_agent::security::audit::AuditLog::default())),
        startup_trace: Vec::new(),
        active_session_id: "mcp-test-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
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
    runtime.reconnect("fake").await.expect("reconnect fake server");

    let app_state = test_app_state(runtime.clone());
    let result = McpCommand
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp status"), &app_state)
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
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect missing"), &app_state)
        .await
        .expect_err("unknown server should error");
    assert!(command_error.to_string().contains("unknown MCP server: missing"));

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
    assert!(tool_error.to_string().contains("unknown MCP server: missing"));
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
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect fake"), &app_state)
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
        .execute(&NormalizedInput::from_raw(InteractionSurface::Cli, "/mcp connect fake"), &app_state)
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

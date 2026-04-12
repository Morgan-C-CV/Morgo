use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::service::mcp::types::{
    McpConnectInfo, McpPeerInfo, McpResourceInfo, McpServerConfig, McpToolInfo,
    McpTransportKind,
};

#[async_trait]
pub trait McpClient: Send + Sync {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<McpConnectInfo>;
    async fn disconnect(&self, config: &McpServerConfig) -> anyhow::Result<()>;
    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>>;
    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>>;
    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value>;
    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String>;
}

#[derive(Debug, Default)]
pub struct RoutingMcpClient {
    mock: MockMcpClient,
    stdio: StdioProcessMcpClient,
}

#[derive(Debug)]
struct StdioSession {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_request_id: u64,
    connect_info: McpConnectInfo,
}

impl RoutingMcpClient {
    fn client_for(&self, transport: McpTransportKind) -> &dyn McpClient {
        match transport {
            McpTransportKind::Mock => &self.mock,
            McpTransportKind::StdioProcess => &self.stdio,
        }
    }
}

#[async_trait]
impl McpClient for RoutingMcpClient {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<McpConnectInfo> {
        self.client_for(config.transport).connect(config).await
    }

    async fn disconnect(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        self.client_for(config.transport).disconnect(config).await
    }

    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        self.client_for(config.transport).list_tools(config).await
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        self.client_for(config.transport).list_resources(config).await
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        self.client_for(config.transport)
            .call_tool(config, tool, input)
            .await
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        self.client_for(config.transport)
            .read_resource(config, resource)
            .await
    }
}

#[derive(Debug, Default)]
pub struct MockMcpClient;

#[async_trait]
impl McpClient for MockMcpClient {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<McpConnectInfo> {
        Ok(McpConnectInfo {
            protocol_initialized: true,
            pid: None,
            peer: McpPeerInfo {
                server_name: Some(config.name.clone()),
                server_version: Some("mock".to_string()),
                protocol_version: Some("mock".to_string()),
            },
        })
    }

    async fn disconnect(&self, _config: &McpServerConfig) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        Ok(vec![McpToolInfo {
            name: format!("{}__echo", config.id),
            description: format!("Echo tool exposed by {}", config.name),
        }])
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        Ok(vec![McpResourceInfo {
            name: format!("{}-readme", config.id),
            uri: format!("mcp://{}/readme", config.id),
        }])
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        Ok(json!({
            "server": config.id,
            "tool": tool,
            "input": input,
            "ok": true,
            "transport": config.transport.as_str()
        }))
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        Ok(format!("resource:{}:{}", config.id, resource))
    }
}

#[derive(Debug, Default)]
pub struct StdioProcessMcpClient {
    processes: Arc<Mutex<BTreeMap<String, StdioSession>>>,
}

#[async_trait]
impl McpClient for StdioProcessMcpClient {
    async fn connect(&self, config: &McpServerConfig) -> anyhow::Result<McpConnectInfo> {
        let mut processes = self.processes.lock().await;
        if let Some(session) = processes.get(&config.id) {
            return Ok(session.connect_info.clone());
        }

        let mut session = self.spawn_session(config).await?;
        let connect_info = self.initialize_session(config, &mut session).await?;
        session.connect_info = connect_info.clone();
        processes.insert(config.id.clone(), session);
        Ok(connect_info)
    }

    async fn disconnect(&self, config: &McpServerConfig) -> anyhow::Result<()> {
        let mut session = {
            let mut processes = self.processes.lock().await;
            processes.remove(&config.id)
        };
        if let Some(session) = session.as_mut() {
            session.child.kill().await.with_context(|| {
                format!("Failed to stop MCP stdio process for server {}", config.id)
            })?;
        }
        Ok(())
    }

    async fn list_tools(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpToolInfo>> {
        self.ensure_process(config).await?;
        Ok(vec![McpToolInfo {
            name: format!("{}__process_tool", config.id),
            description: format!(
                "Process-backed MCP tool surface exposed by {}",
                config.name
            ),
        }])
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        self.ensure_process(config).await?;
        Ok(vec![McpResourceInfo {
            name: format!("{}-process-status", config.id),
            uri: format!("mcp://{}/process-status", config.id),
        }])
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        self.ensure_process(config).await?;
        Ok(json!({
            "server": config.id,
            "tool": tool,
            "input": input,
            "ok": true,
            "transport": config.transport.as_str(),
            "note": "stdio process completed the initialize handshake skeleton; full MCP method exchange is not implemented yet"
        }))
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        self.ensure_process(config).await?;
        Ok(format!(
            "resource:{}:{}:{}",
            config.id,
            config.transport.as_str(),
            resource
        ))
    }
}

impl StdioProcessMcpClient {
    async fn ensure_process(&self, config: &McpServerConfig) -> anyhow::Result<McpConnectInfo> {
        self.connect(config).await
    }

    async fn spawn_session(&self, config: &McpServerConfig) -> anyhow::Result<StdioSession> {
        let mut command = Command::new(&config.command);
        command.args(&config.args);
        command.envs(&config.env);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let mut child = command.spawn().with_context(|| {
            format!(
                "Failed to spawn MCP stdio process '{}' for server {}",
                config.command, config.id
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("MCP stdio process did not provide stdin for server {}", config.id))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("MCP stdio process did not provide stdout for server {}", config.id))?;

        Ok(StdioSession {
            child,
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            next_request_id: 1,
            connect_info: McpConnectInfo::default(),
        })
    }

    async fn initialize_session(
        &self,
        config: &McpServerConfig,
        session: &mut StdioSession,
    ) -> anyhow::Result<McpConnectInfo> {
        let response = self
            .send_jsonrpc_request(
                session,
                "initialize",
                json!({
                    "protocolVersion": "2025-03-26",
                    "clientInfo": {
                        "name": "rust-agent",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {}
                }),
            )
            .await?;

        self.send_jsonrpc_notification(session, "notifications/initialized", json!({}))
            .await?;

        let result = response
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("MCP initialize response for server {} was missing result", config.id))?;
        let server_info = result.get("serverInfo").cloned().unwrap_or(Value::Null);

        Ok(McpConnectInfo {
            protocol_initialized: true,
            pid: session.child.id(),
            peer: McpPeerInfo {
                server_name: server_info
                    .get("name")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                server_version: server_info
                    .get("version")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                protocol_version: result
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            },
        })
    }

    async fn send_jsonrpc_request(
        &self,
        session: &mut StdioSession,
        method: &str,
        params: Value,
    ) -> anyhow::Result<Value> {
        let id = session.next_request_id;
        session.next_request_id += 1;
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.write_jsonrpc_message(session, &request).await?;
        let response = self.read_jsonrpc_message(session).await?;
        if response.get("id").and_then(Value::as_u64) != Some(id) {
            anyhow::bail!("MCP response id mismatch for method {method}");
        }
        if let Some(error) = response.get("error") {
            anyhow::bail!("MCP {method} failed: {error}");
        }
        Ok(response)
    }

    async fn send_jsonrpc_notification(
        &self,
        session: &mut StdioSession,
        method: &str,
        params: Value,
    ) -> anyhow::Result<()> {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.write_jsonrpc_message(session, &notification).await
    }

    async fn write_jsonrpc_message(
        &self,
        session: &mut StdioSession,
        message: &Value,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(message)?;
        session
            .stdin
            .write_all(format!("Content-Length: {}\r\n\r\n", payload.len()).as_bytes())
            .await?;
        session.stdin.write_all(&payload).await?;
        session.stdin.flush().await?;
        Ok(())
    }

    async fn read_jsonrpc_message(&self, session: &mut StdioSession) -> anyhow::Result<Value> {
        let mut content_length = None;
        loop {
            let mut line = String::new();
            let bytes = session.stdout.read_line(&mut line).await?;
            if bytes == 0 {
                anyhow::bail!("MCP stdio process closed stdout before completing a JSON-RPC response");
            }
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.strip_prefix("Content-Length:") {
                content_length = Some(value.trim().parse::<usize>()?);
            }
        }

        let length = content_length
            .ok_or_else(|| anyhow::anyhow!("MCP stdio response did not include Content-Length header"))?;
        let mut body = vec![0_u8; length];
        session.stdout.read_exact(&mut body).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}

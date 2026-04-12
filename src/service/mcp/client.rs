use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{Value, json};
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
                capabilities: Some(json!({"tools": {}, "resources": {}})),
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
            input_schema: Some(json!({"type": "object"})),
        }])
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        Ok(vec![McpResourceInfo {
            name: format!("{}-readme", config.id),
            uri: format!("mcp://{}/readme", config.id),
            description: format!("Readme resource exposed by {}", config.name),
            mime_type: Some("text/plain".into()),
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
        let mut processes = self.processes.lock().await;
        let session = self.ensure_process(&mut processes, config).await?;
        let response = self
            .send_jsonrpc_request(session, "tools/list", json!({}))
            .await?;
        parse_tools_list_response(&response)
    }

    async fn list_resources(&self, config: &McpServerConfig) -> anyhow::Result<Vec<McpResourceInfo>> {
        let mut processes = self.processes.lock().await;
        let session = self.ensure_process(&mut processes, config).await?;
        let response = self
            .send_jsonrpc_request(session, "resources/list", json!({}))
            .await?;
        parse_resources_list_response(&response)
    }

    async fn call_tool(
        &self,
        config: &McpServerConfig,
        tool: &str,
        input: Option<Value>,
    ) -> anyhow::Result<Value> {
        let mut processes = self.processes.lock().await;
        let session = self.ensure_process(&mut processes, config).await?;
        let response = self
            .send_jsonrpc_request(
                session,
                "tools/call",
                json!({
                    "name": tool,
                    "arguments": input.unwrap_or(Value::Null),
                }),
            )
            .await?;
        parse_tool_call_response(&response)
    }

    async fn read_resource(&self, config: &McpServerConfig, resource: &str) -> anyhow::Result<String> {
        let mut processes = self.processes.lock().await;
        let session = self.ensure_process(&mut processes, config).await?;
        let response = self
            .send_jsonrpc_request(
                session,
                "resources/read",
                json!({
                    "uri": resource,
                }),
            )
            .await?;
        parse_resource_read_response(&response)
    }
}

impl StdioProcessMcpClient {
    async fn ensure_process<'a>(
        &self,
        processes: &'a mut BTreeMap<String, StdioSession>,
        config: &McpServerConfig,
    ) -> anyhow::Result<&'a mut StdioSession> {
        if !processes.contains_key(&config.id) {
            let mut session = self.spawn_session(config).await?;
            let connect_info = self.initialize_session(config, &mut session).await?;
            session.connect_info = connect_info;
            processes.insert(config.id.clone(), session);
        }
        processes
            .get_mut(&config.id)
            .ok_or_else(|| anyhow::anyhow!("missing stdio process session for server {}", config.id))
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
                capabilities: result.get("capabilities").cloned(),
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

fn parse_tools_list_response(response: &Value) -> anyhow::Result<Vec<McpToolInfo>> {
    let tools = response
        .get("result")
        .and_then(|result| result.get("tools"))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("MCP tools/list response missing result.tools"))?;
    serde_json::from_value(tools).context("failed to parse MCP tool list")
}

fn parse_resources_list_response(response: &Value) -> anyhow::Result<Vec<McpResourceInfo>> {
    let resources = response
        .get("result")
        .and_then(|result| result.get("resources"))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("MCP resources/list response missing result.resources"))?;
    serde_json::from_value(resources).context("failed to parse MCP resource list")
}

fn parse_tool_call_response(response: &Value) -> anyhow::Result<Value> {
    response
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("MCP tools/call response missing result"))
}

fn parse_resource_read_response(response: &Value) -> anyhow::Result<String> {
    let result = response
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("MCP resources/read response missing result"))?;
    if let Some(text) = result.get("contents").and_then(Value::as_array).and_then(|items| items.first()).and_then(|item| item.get("text")).and_then(Value::as_str) {
        return Ok(text.to_string());
    }
    if let Some(text) = result.get("text").and_then(Value::as_str) {
        return Ok(text.to_string());
    }
    anyhow::bail!("MCP resources/read response missing text content")
}

#[cfg(test)]
mod tests {
    use super::{
        parse_resource_read_response, parse_resources_list_response, parse_tool_call_response,
        parse_tools_list_response,
    };
    use serde_json::json;

    #[test]
    fn parses_tools_list_response() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "tools": [
                    {"name": "echo", "description": "Echo tool", "input_schema": {"type": "object"}}
                ]
            }
        });
        let tools = parse_tools_list_response(&response).expect("tool list parses");
        assert_eq!(tools[0].name, "echo");
        assert_eq!(tools[0].description, "Echo tool");
    }

    #[test]
    fn parses_resources_list_response() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "resources": [
                    {"name": "readme", "uri": "mcp://server/readme", "description": "Readme", "mime_type": "text/plain"}
                ]
            }
        });
        let resources = parse_resources_list_response(&response).expect("resource list parses");
        assert_eq!(resources[0].name, "readme");
        assert_eq!(resources[0].uri, "mcp://server/readme");
    }

    #[test]
    fn parses_tool_call_response() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"content": [{"type": "text", "text": "ok"}]}
        });
        let value = parse_tool_call_response(&response).expect("tool result parses");
        assert_eq!(value["content"][0]["text"], "ok");
    }

    #[test]
    fn parses_resource_read_response() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "contents": [
                    {"uri": "mcp://server/readme", "text": "hello"}
                ]
            }
        });
        let content = parse_resource_read_response(&response).expect("resource read parses");
        assert_eq!(content, "hello");
    }
}

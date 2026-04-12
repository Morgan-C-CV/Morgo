use std::collections::BTreeMap;
use std::sync::Arc;

use rust_agent::service::mcp::client::{McpClient, RoutingMcpClient};
use rust_agent::service::mcp::types::{McpServerConfig, McpTransportKind};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;

#[tokio::test]
async fn stdio_mcp_client_round_trips_list_call_and_read() {
    let script = r#"
import json, sys


def write(msg):
    data = json.dumps(msg).encode()
    sys.stdout.write(f'Content-Length: {len(data)}\\r\\n\\r\\n')
    sys.stdout.flush()
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

while True:
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            raise SystemExit(0)
        if line == b'\r\n':
            break
        key, value = line.decode().split(':', 1)
        headers[key.strip().lower()] = value.strip()
    length = int(headers['content-length'])
    payload = json.loads(sys.stdin.buffer.read(length))
    method = payload.get('method')
    if method == 'initialize':
        write({
            'jsonrpc': '2.0',
            'id': payload['id'],
            'result': {
                'protocolVersion': '2025-03-26',
                'serverInfo': {'name': 'fake-mcp', 'version': '1.0.0'},
                'capabilities': {'tools': {}, 'resources': {}}
            }
        })
    elif method == 'notifications/initialized':
        continue
    elif method == 'tools/list':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'tools': [{'name': 'echo', 'description': 'Echo tool', 'input_schema': {'type': 'object'}}]}})
    elif method == 'resources/list':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'resources': [{'name': 'readme', 'uri': 'mcp://fake/readme', 'description': 'Readme', 'mime_type': 'text/plain'}]}})
    elif method == 'tools/call':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'content': [{'type': 'text', 'text': payload['params']['name']}], 'structured': payload['params']['arguments']}})
    elif method == 'resources/read':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'contents': [{'uri': payload['params']['uri'], 'text': 'resource-body'}]}})
    else:
        write({'jsonrpc': '2.0', 'id': payload.get('id'), 'error': {'code': -32601, 'message': 'unknown method'}})
"#;

    let config = McpServerConfig {
        id: "fake".into(),
        name: "fake".into(),
        command: "python3".into(),
        args: vec!["-c".into(), script.into()],
        env: BTreeMap::new(),
        transport: McpTransportKind::StdioProcess,
    };

    let client = RoutingMcpClient::default();
    let info = client.connect(&config).await.expect("connect fake mcp");
    assert!(info.protocol_initialized);
    assert_eq!(info.peer.server_name.as_deref(), Some("fake-mcp"));

    let tools = client.list_tools(&config).await.expect("list tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "echo");

    let resources = client.list_resources(&config).await.expect("list resources");
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].uri, "mcp://fake/readme");

    let tool_result = client
        .call_tool(&config, "echo", Some(json!({"value": 42})))
        .await
        .expect("call tool");
    assert_eq!(tool_result["content"][0]["text"], "echo");
    assert_eq!(tool_result["structured"]["value"], 42);

    let resource = client
        .read_resource(&config, "mcp://fake/readme")
        .await
        .expect("read resource");
    assert_eq!(resource, "resource-body");

    client.disconnect(&config).await.expect("disconnect fake mcp");
}

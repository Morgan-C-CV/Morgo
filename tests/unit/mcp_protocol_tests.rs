use std::collections::BTreeMap;

use rust_agent::service::mcp::client::{McpClient, RoutingMcpClient};
use rust_agent::service::mcp::runtime::McpRuntime;
use rust_agent::service::mcp::types::{
    McpAction, McpCapabilities, McpFailureCode, McpRequest, McpServerConfig,
    McpTransportKind,
};
use serde_json::json;

#[tokio::test]
async fn stdio_mcp_client_round_trips_list_call_and_read() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let script = r#"
import json, sys


def write(msg):
    data = json.dumps(msg).encode()
    sys.stdout.write(f'Content-Length: {len(data)}\r\n\r\n')
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
            governance: rust_agent::service::mcp::types::McpServerGovernanceConfig {
                review_required: false,
                notes: None,
            },
            connect_timeout_ms: 10_000,
            proxy_url: None,
            no_proxy: None,
            ca_bundle_path: None,
        };

        let client = RoutingMcpClient::default();
        let info = client.connect(&config).await.expect("connect fake mcp");
        assert!(info.protocol_initialized);
        assert_eq!(info.peer.server_name.as_deref(), Some("fake-mcp"));
        assert_eq!(info.peer.capabilities, McpCapabilities::from_initialize_result(Some(&json!({"tools": {}, "resources": {}}))));

        let tools = client.list_tools(&config).await.expect("list tools");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "echo");

        let resources = client.list_resources(&config).await.expect("list resources");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].uri, "mcp://fake/readme");

        client.disconnect(&config).await.expect("disconnect fake mcp");
    })
    .await
    .expect("legacy MCP round-trip test should not hang");
}

#[tokio::test]
async fn stdio_mcp_client_rejects_malformed_content_length_header() {
    let script = r#"
import json, sys


def write_bad_header(msg):
    data = json.dumps(msg).encode()
    sys.stdout.write('Content-Length: nope\r\n\r\n')
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
        write_bad_header({
            'jsonrpc': '2.0',
            'id': payload['id'],
            'result': {
                'protocolVersion': '2025-03-26',
                'serverInfo': {'name': 'bad-header-mcp', 'version': '1.0.0'},
                'capabilities': {'tools': {}, 'resources': {}}
            }
        })
    else:
        write_bad_header({'jsonrpc': '2.0', 'id': payload.get('id'), 'result': {}})
"#;

    let config = McpServerConfig {
        id: "bad-header".into(),
        name: "bad-header".into(),
        command: "python3".into(),
        args: vec!["-c".into(), script.into()],
        env: BTreeMap::new(),
        transport: McpTransportKind::StdioProcess,
        governance: rust_agent::service::mcp::types::McpServerGovernanceConfig {
            review_required: false,
            notes: None,
        },
        connect_timeout_ms: 10_000,
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    };

    let client = RoutingMcpClient::default();
    let error = client.connect(&config).await.expect_err("connect should fail");
    assert!(error.to_string().contains("Content-Length") || error.to_string().contains("invalid digit"));
}

#[tokio::test]
async fn stdio_mcp_client_rejects_response_id_mismatch() {
    let script = r#"
import json, sys


def write(msg):
    data = json.dumps(msg).encode()
    sys.stdout.write(f'Content-Length: {len(data)}\r\n\r\n')
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
            'id': payload['id'] + 99,
            'result': {
                'protocolVersion': '2025-03-26',
                'serverInfo': {'name': 'id-mismatch-mcp', 'version': '1.0.0'},
                'capabilities': {'tools': {}, 'resources': {}}
            }
        })
    else:
        write({'jsonrpc': '2.0', 'id': payload.get('id'), 'result': {}})
"#;

    let config = McpServerConfig {
        id: "id-mismatch".into(),
        name: "id-mismatch".into(),
        command: "python3".into(),
        args: vec!["-c".into(), script.into()],
        env: BTreeMap::new(),
        transport: McpTransportKind::StdioProcess,
        governance: rust_agent::service::mcp::types::McpServerGovernanceConfig {
            review_required: false,
            notes: None,
        },
        connect_timeout_ms: 10_000,
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    };

    let client = RoutingMcpClient::default();
    let error = client.connect(&config).await.expect_err("connect should fail");
    assert!(error.to_string().contains("id mismatch"));
}

#[tokio::test]
async fn mcp_runtime_marks_post_initialize_inventory_failure_as_protocol_baseline() {
    let script = r#"
import json, sys


def write(msg):
    data = json.dumps(msg).encode()
    sys.stdout.write(f'Content-Length: {len(data)}\r\n\r\n')
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
                'serverInfo': {'name': 'inventory-fail-mcp', 'version': '1.0.0'},
                'capabilities': {'tools': {}, 'resources': {}}
            }
        })
    elif method == 'notifications/initialized':
        continue
    elif method == 'tools/list':
        write({'jsonrpc': '2.0', 'id': payload['id'] + 1, 'result': {'tools': []}})
    elif method == 'resources/list':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'resources': []}})
    else:
        write({'jsonrpc': '2.0', 'id': payload.get('id'), 'result': {}})
"#;

    let config = McpServerConfig {
        id: "inventory-fail".into(),
        name: "inventory-fail".into(),
        command: "python3".into(),
        args: vec!["-c".into(), script.into()],
        env: BTreeMap::new(),
        transport: McpTransportKind::StdioProcess,
        governance: rust_agent::service::mcp::types::McpServerGovernanceConfig {
            review_required: false,
            notes: None,
        },
        connect_timeout_ms: 10_000,
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    };

    let runtime = McpRuntime::new(std::sync::Arc::new(RoutingMcpClient::default()), vec![config]);
    let error = runtime
        .dispatch(McpRequest {
            action: McpAction::ListTools,
            server: "inventory-fail".into(),
            tool: None,
            resource: None,
            input: None,
        })
        .await
        .expect_err("inventory bootstrap should fail");
    assert!(error.to_string().contains("id mismatch"));

    let servers = runtime.list_servers().await;
    let failure = servers[0]
        .last_failure
        .clone()
        .expect("failure metadata should be recorded");
    assert_eq!(failure.code, McpFailureCode::Protocol);
}

#[tokio::test]
async fn stdio_mcp_client_normalizes_sparse_capability_payloads() {
    let script = r#"
import json, sys


def write(msg):
    data = json.dumps(msg).encode()
    sys.stdout.write(f'Content-Length: {len(data)}\r\n\r\n')
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
                'serverInfo': {'name': 'sparse-mcp', 'version': '1.0.0'},
                'capabilities': {'tools': None, 'resources': {}, 'vendorAuth': {'scheme': 'bearer'}}
            }
        })
    elif method == 'notifications/initialized':
        continue
    elif method == 'tools/list':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'tools': []}})
    elif method == 'resources/list':
        write({'jsonrpc': '2.0', 'id': payload['id'], 'result': {'resources': []}})
    else:
        write({'jsonrpc': '2.0', 'id': payload.get('id'), 'result': {}})
"#;

    let config = McpServerConfig {
        id: "sparse".into(),
        name: "sparse".into(),
        command: "python3".into(),
        args: vec!["-c".into(), script.into()],
        env: BTreeMap::new(),
        transport: McpTransportKind::StdioProcess,
        governance: rust_agent::service::mcp::types::McpServerGovernanceConfig {
            review_required: false,
            notes: None,
        },
        connect_timeout_ms: 10_000,
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    };

    let client = RoutingMcpClient::default();
    let info = client.connect(&config).await.expect("connect sparse mcp");
    assert!(info.peer.capabilities.tools.is_none());
    assert!(info.peer.capabilities.resources.is_some());
    assert_eq!(
        info.peer.capabilities.extensions.get("vendorAuth"),
        Some(&json!({"scheme": "bearer"}))
    );

    client
        .disconnect(&config)
        .await
        .expect("disconnect sparse mcp");
}

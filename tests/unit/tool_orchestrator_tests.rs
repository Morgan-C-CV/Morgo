use std::sync::Arc;

use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::glob::GlobTool;
use rust_agent::tool::orchestrator::{ToolExecutionRequest, ToolOrchestrator};
use rust_agent::tool::registry::ToolRegistry;

#[test]
fn orchestrator_groups_concurrency_safe_tools_into_batches() {
    let registry = ToolRegistry::new()
        .register(Arc::new(FileReadTool))
        .register(Arc::new(GlobTool))
        .register(Arc::new(AgentTool));
    let orchestrator = ToolOrchestrator::new(&registry);
    let plan = orchestrator.plan(&[
        ToolExecutionRequest {
            call: rust_agent::tool::definition::ToolCall::new("Read", "/tmp/a"),
        },
        ToolExecutionRequest {
            call: rust_agent::tool::definition::ToolCall::new("Glob", "*.rs"),
        },
        ToolExecutionRequest {
            call: rust_agent::tool::definition::ToolCall::new("Agent", "do work"),
        },
    ]);

    assert_eq!(plan.batches.len(), 2);
    assert!(plan.batches[0].concurrency_safe);
    assert_eq!(plan.batches[0].start_index, 0);
    assert_eq!(plan.batches[0].end_index, 2);
    assert!(!plan.batches[1].concurrency_safe);
    assert_eq!(plan.batches[1].start_index, 2);
    assert_eq!(plan.batches[1].end_index, 3);
}

#[tokio::test]
async fn orchestrator_executes_single_request_through_registry() {
    let dir = std::env::temp_dir().join("rust-agent-orchestrator-read.txt");
    tokio::fs::write(&dir, "hello orchestrator")
        .await
        .expect("write temp file");

    let registry = ToolRegistry::new().register(Arc::new(FileReadTool));
    let orchestrator = ToolOrchestrator::new(&registry);
    let outcomes = orchestrator
        .execute(
            &[ToolExecutionRequest {
                call: rust_agent::tool::definition::ToolCall::new(
                    "Read",
                    serde_json::json!({
                        "file_path": dir.to_string_lossy().into_owned(),
                    })
                    .to_string(),
                ),
            }],
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("execute tool request");

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].tool_name, "Read");
    assert_eq!(
        outcomes[0].result,
        rust_agent::tool::definition::ToolResult::Text("hello orchestrator".into())
    );
    assert!(!outcomes[0].executed_in_batch);

    let _ = tokio::fs::remove_file(&dir).await;
}

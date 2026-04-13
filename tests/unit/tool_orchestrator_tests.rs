use std::sync::Arc;

use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use async_trait::async_trait;
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::glob::GlobTool;
use rust_agent::tool::definition::{InterruptBehavior, PermissionDecision, Tool, ToolCall, ToolMetadata, ToolResult};
use rust_agent::tool::orchestrator::{ToolExecutionRequest, ToolOrchestrator};
use rust_agent::tool::registry::ToolRegistry;

struct CancelOnDenyTool;
struct PassiveTool;

#[async_trait]
impl Tool for CancelOnDenyTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "CancelOnDeny",
            description: "Test tool with cancel interrupt behavior",
            aliases: &[],
            search_hint: None,
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    fn interrupt_behavior(&self) -> InterruptBehavior {
        InterruptBehavior::Cancel
    }

    async fn check_permissions(
        &self,
        _call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> PermissionDecision {
        PermissionDecision::Deny {
            message: "cancelled by test policy".into(),
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

#[async_trait]
impl Tool for PassiveTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Passive",
            description: "Test tool that would run if not cancelled",
            aliases: &[],
            search_hint: None,
            read_only: true,
            destructive: false,
            concurrency_safe: false,
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
        Ok(ToolResult::Text("passive executed".into()))
    }
}

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
    assert_eq!(
        outcomes[0].record.kind,
        rust_agent::tool::result::ToolExecutionOutcomeKind::Success
    );
    assert!(!outcomes[0].executed_in_batch);

    let _ = tokio::fs::remove_file(&dir).await;
}

#[tokio::test]
async fn orchestrator_cancels_remaining_serial_requests_after_cancel_denial() {
    let registry = ToolRegistry::new()
        .register(Arc::new(CancelOnDenyTool))
        .register(Arc::new(PassiveTool));
    let orchestrator = ToolOrchestrator::new(&registry);
    let outcomes = orchestrator
        .execute(
            &[
                ToolExecutionRequest {
                    call: rust_agent::tool::definition::ToolCall::new("CancelOnDeny", "input"),
                },
                ToolExecutionRequest {
                    call: rust_agent::tool::definition::ToolCall::new("Passive", "input"),
                },
            ],
            &ToolPermissionContext::new(PermissionMode::Default),
        )
        .await
        .expect("execute tool request");

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].tool_name, "CancelOnDeny");
    assert_eq!(
        outcomes[0].result,
        ToolResult::Denied("cancelled by test policy".into())
    );
}

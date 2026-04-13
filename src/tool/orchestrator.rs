use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{InterruptBehavior, ToolCall, ToolResult};
use crate::tool::registry::ToolRegistry;
use crate::tool::result::{ToolExecutionOutcomeKind, ToolExecutionRecord};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionRequest {
    pub call: ToolCall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionOutcome {
    pub tool_name: String,
    pub result: ToolResult,
    pub executed_in_batch: bool,
    pub record: ToolExecutionRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionBatch {
    pub start_index: usize,
    pub end_index: usize,
    pub concurrency_safe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExecutionPlan {
    pub batches: Vec<ToolExecutionBatch>,
}

pub struct ToolOrchestrator {
    registry: Arc<ToolRegistry>,
}

impl ToolOrchestrator {
    pub fn new(registry: &ToolRegistry) -> Self {
        Self {
            registry: Arc::new(registry.clone()),
        }
    }

    pub fn plan(&self, requests: &[ToolExecutionRequest]) -> ToolExecutionPlan {
        let mut batches = Vec::new();
        let mut start = 0;

        while start < requests.len() {
            let concurrency_safe = self
                .registry
                .is_concurrency_safe(&requests[start].call)
                .unwrap_or(false);
            let mut end = start + 1;
            while end < requests.len()
                && self
                    .registry
                    .is_concurrency_safe(&requests[end].call)
                    .unwrap_or(false)
                    == concurrency_safe
            {
                end += 1;
            }
            batches.push(ToolExecutionBatch {
                start_index: start,
                end_index: end,
                concurrency_safe,
            });
            start = end;
        }

        ToolExecutionPlan { batches }
    }

    pub async fn execute(
        &self,
        requests: &[ToolExecutionRequest],
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<Vec<ToolExecutionOutcome>> {
        let plan = self.plan(requests);
        let mut outcomes = Vec::new();

        for batch in plan.batches {
            let executed_in_batch =
                batch.concurrency_safe && batch.end_index - batch.start_index > 1;
            if executed_in_batch {
                let mut handles = Vec::new();
                for request in &requests[batch.start_index..batch.end_index] {
                    let registry = self.registry.clone();
                    let permissions = permissions.clone();
                    let call = request.call.clone();
                    handles.push(tokio::spawn(async move {
                        let result = registry.invoke(&call, &permissions).await;
                        (call, result)
                    }));
                }
                for handle in handles {
                    let (call, result) = handle
                        .await
                        .map_err(|error| anyhow::anyhow!("tool task join failed: {error}"))?;
                    let result = result?;
                    outcomes.push(ToolExecutionOutcome {
                        tool_name: call.name.clone(),
                        record: ToolExecutionRecord {
                            tool_name: call.name.clone(),
                            outcome: format!("{:?}", result),
                            kind: classify_result(&result),
                        },
                        result,
                        executed_in_batch,
                    });
                }
                continue;
            }

            for request in &requests[batch.start_index..batch.end_index] {
                let interrupt_behavior = self
                    .registry
                    .interrupt_behavior(&request.call)
                    .unwrap_or(InterruptBehavior::Block);
                let result = self.registry.invoke(&request.call, permissions).await?;
                outcomes.push(ToolExecutionOutcome {
                    tool_name: request.call.name.clone(),
                    record: ToolExecutionRecord {
                        tool_name: request.call.name.clone(),
                        outcome: format!("{:?}", result),
                        kind: classify_result(&result),
                    },
                    result,
                    executed_in_batch,
                });
                if matches!(interrupt_behavior, InterruptBehavior::Cancel)
                    && matches!(
                        outcomes.last(),
                        Some(ToolExecutionOutcome {
                            result: ToolResult::Denied(_),
                            ..
                        })
                    )
                {
                    break;
                }
            }
        }

        Ok(outcomes)
    }
}

fn classify_result(result: &ToolResult) -> ToolExecutionOutcomeKind {
    match result {
        ToolResult::Text(_) => ToolExecutionOutcomeKind::Success,
        ToolResult::Denied(_) => ToolExecutionOutcomeKind::Denied,
        ToolResult::PendingApproval { .. } => ToolExecutionOutcomeKind::PendingApproval,
        ToolResult::Interrupted(_) => ToolExecutionOutcomeKind::Interrupted,
        ToolResult::Progress(_) => ToolExecutionOutcomeKind::Progress,
        ToolResult::ResultTooLarge(_) => ToolExecutionOutcomeKind::ResultTooLarge,
    }
}

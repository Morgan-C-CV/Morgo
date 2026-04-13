use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{InterruptBehavior, ToolCall, ToolResult};
use crate::tool::registry::ToolRegistry;
use crate::tool::result::{
    ToolExecutionOutcomeKind, ToolExecutionRecord, ToolReportModifier,
};
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
                        let observable_input = registry.observable_input(&call);
                        let result = registry.invoke(&call, &permissions).await;
                        (call, result, observable_input)
                    }));
                }
                let batch_size = batch.end_index - batch.start_index;
                for (batch_offset, handle) in handles.into_iter().enumerate() {
                    let (call, result, observable_input) = handle
                        .await
                        .map_err(|error| anyhow::anyhow!("tool task join failed: {error}"))?;
                    let result = result?;
                    outcomes.push(build_outcome(
                        call.name.clone(),
                        result,
                        observable_input,
                        batch_offset,
                        batch_size,
                        executed_in_batch,
                    ));
                }
                continue;
            }

            let batch_size = batch.end_index - batch.start_index;
            for (batch_offset, request) in requests[batch.start_index..batch.end_index]
                .iter()
                .enumerate()
            {
                let interrupt_behavior = self
                    .registry
                    .interrupt_behavior(&request.call)
                    .unwrap_or(InterruptBehavior::Block);
                let observable_input = self.registry.observable_input(&request.call);
                let result = self.registry.invoke(&request.call, permissions).await?;
                let should_break = should_stop_serial_execution(&interrupt_behavior, &result);
                outcomes.push(build_outcome(
                    request.call.name.clone(),
                    result,
                    observable_input,
                    batch_offset,
                    batch_size,
                    executed_in_batch,
                ));
                if should_break {
                    break;
                }
            }
        }

        Ok(outcomes)
    }
}

fn build_outcome(
    tool_name: String,
    result: ToolResult,
    observable_input: Option<crate::tool::definition::ObservableInput>,
    batch_index: usize,
    batch_size: usize,
    executed_in_batch: bool,
) -> ToolExecutionOutcome {
    let (kind, summary, detail, report_modifier) = summarize_result(&tool_name, &result);
    ToolExecutionOutcome {
        record: ToolExecutionRecord {
            tool_name: tool_name.clone(),
            outcome: format!("{:?}", result),
            kind,
            summary,
            detail,
            report_modifier,
            observable_input,
            batch_context: crate::tool::result::ToolBatchContext {
                batch_index,
                batch_size,
                executed_in_batch,
            },
        },
        tool_name,
        result,
        executed_in_batch,
    }
}

fn should_stop_serial_execution(
    interrupt_behavior: &InterruptBehavior,
    result: &ToolResult,
) -> bool {
    match interrupt_behavior {
        InterruptBehavior::Block => false,
        InterruptBehavior::Cancel => matches!(result, ToolResult::Denied(_)),
    }
}

fn summarize_result(
    tool_name: &str,
    result: &ToolResult,
) -> (
    ToolExecutionOutcomeKind,
    String,
    Option<String>,
    ToolReportModifier,
) {
    match result {
        ToolResult::Text(text) => (
            ToolExecutionOutcomeKind::Success,
            format!("{tool_name} succeeded"),
            Some(text.clone()),
            ToolReportModifier::None,
        ),
        ToolResult::Denied(message) => (
            ToolExecutionOutcomeKind::Denied,
            format!("{tool_name} denied"),
            Some(message.clone()),
            ToolReportModifier::NeedsAttention,
        ),
        ToolResult::PendingApproval { message, .. } => (
            ToolExecutionOutcomeKind::PendingApproval,
            format!("{tool_name} pending approval"),
            Some(message.clone()),
            ToolReportModifier::Pending,
        ),
        ToolResult::Interrupted(message) => (
            ToolExecutionOutcomeKind::Interrupted,
            format!("{tool_name} interrupted"),
            Some(message.clone()),
            ToolReportModifier::NeedsAttention,
        ),
        ToolResult::Progress(message) => (
            ToolExecutionOutcomeKind::Progress,
            format!("{tool_name} in progress"),
            Some(message.clone()),
            ToolReportModifier::Progress,
        ),
        ToolResult::ResultTooLarge(message) => (
            ToolExecutionOutcomeKind::ResultTooLarge,
            format!("{tool_name} result too large"),
            Some(message.clone()),
            ToolReportModifier::NeedsAttention,
        ),
    }
}

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{InterruptBehavior, ToolCall, ToolResult};
use crate::tool::registry::ToolRegistry;
use crate::tool::result::{
    PendingApprovalPayload, ToolExecutionOutcomeKind, ToolExecutionRecord, ToolExecutionReport,
    ToolReportContextModifier, ToolReportModifier,
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
                    let handle_call = call.clone();
                    handles.push((
                        call,
                        tokio::spawn(async move {
                            let observable_input = registry.observable_input(&handle_call);
                            let result = registry.invoke(&handle_call, &permissions).await;
                            (result, observable_input)
                        }),
                    ));
                }
                let batch_size = batch.end_index - batch.start_index;
                for (batch_offset, (call, handle)) in handles.into_iter().enumerate() {
                    let (result, observable_input) = handle.await.unwrap_or_else(|error| {
                        (
                            Ok(registry_error_result(
                                &call.name,
                                format!("tool task join failed: {error}"),
                            )),
                            None,
                        )
                    });
                    let result = result.unwrap_or_else(|error| {
                        registry_error_result(&call.name, format!("tool dispatch failed: {error}"))
                    });
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
                let result = self
                    .registry
                    .invoke(&request.call, permissions)
                    .await
                    .unwrap_or_else(|error| {
                        registry_error_result(
                            &request.call.name,
                            format!("tool dispatch failed: {error}"),
                        )
                    });
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

fn registry_error_result(tool_name: &str, message: String) -> ToolResult {
    ToolResult::Text(format!(
        "status=failed\ntool={tool_name}\nreason=tool_dispatch_error\nmessage={}\nnext_action=Read the error message, correct the tool request or environment assumptions, and retry if appropriate.\n\nTool was not executed successfully.",
        message.split_whitespace().collect::<Vec<_>>().join(" ")
    ))
}

fn build_outcome(
    tool_name: String,
    result: ToolResult,
    observable_input: Option<crate::tool::definition::ObservableInput>,
    batch_index: usize,
    batch_size: usize,
    executed_in_batch: bool,
) -> ToolExecutionOutcome {
    let (kind, summary, detail, pending_approval, report_modifier) =
        summarize_result(&tool_name, &result);
    ToolExecutionOutcome {
        record: ToolExecutionRecord {
            tool_name: tool_name.clone(),
            outcome: format!("{:?}", result),
            kind,
            summary,
            detail,
            pending_approval,
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

pub fn build_execution_record(
    tool_name: impl Into<String>,
    result: &ToolResult,
    observable_input: Option<crate::tool::definition::ObservableInput>,
) -> ToolExecutionRecord {
    let tool_name = tool_name.into();
    let (kind, summary, detail, pending_approval, report_modifier) =
        summarize_result(&tool_name, result);
    ToolExecutionRecord {
        tool_name,
        outcome: format!("{:?}", result),
        kind,
        summary,
        detail,
        pending_approval,
        report_modifier,
        observable_input,
        batch_context: crate::tool::result::ToolBatchContext {
            batch_index: 0,
            batch_size: 1,
            executed_in_batch: false,
        },
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

pub fn aggregate_execution_records(records: &[ToolExecutionRecord]) -> Option<ToolExecutionReport> {
    let first = records.first()?;
    if records.len() == 1 {
        let context_modifier = match first.kind {
            ToolExecutionOutcomeKind::Success => {
                ToolReportContextModifier::ContinueWithUserMessage(
                    first
                        .detail
                        .clone()
                        .unwrap_or_else(|| first.summary.clone()),
                )
            }
            ToolExecutionOutcomeKind::Progress
            | ToolExecutionOutcomeKind::RecoverableFailure
            | ToolExecutionOutcomeKind::PendingApproval
            | ToolExecutionOutcomeKind::Denied
            | ToolExecutionOutcomeKind::Interrupted
            | ToolExecutionOutcomeKind::ResultTooLarge => {
                ToolReportContextModifier::SetPendingToolUseSummary(first.summary.clone())
            }
        };
        return Some(ToolExecutionReport {
            records: records.to_vec(),
            summary: first.summary.clone(),
            detail: first.detail.clone(),
            report_modifier: first.report_modifier.clone(),
            context_modifier,
        });
    }

    let has_non_success = records
        .iter()
        .any(|record| record.kind != ToolExecutionOutcomeKind::Success);
    let report_modifier = records
        .iter()
        .fold(ToolReportModifier::None, |current, record| {
            aggregate_report_modifier(current, &record.report_modifier)
        });
    let summaries = records
        .iter()
        .map(|record| record.summary.clone())
        .collect::<Vec<_>>();
    let details = records
        .iter()
        .map(|record| {
            record
                .detail
                .clone()
                .unwrap_or_else(|| record.summary.clone())
        })
        .collect::<Vec<_>>();
    let summary = if summaries
        .iter()
        .all(|summary| summary.ends_with("succeeded"))
    {
        format!("{} tool results", records.len())
    } else {
        summaries.join("; ")
    };
    let detail = Some(details.join("\n"));
    let context_modifier = if !has_non_success && report_modifier == ToolReportModifier::None {
        ToolReportContextModifier::ContinueWithUserMessage(
            detail.clone().unwrap_or_else(|| summary.clone()),
        )
    } else {
        ToolReportContextModifier::SetPendingToolUseSummary(summary.clone())
    };

    Some(ToolExecutionReport {
        records: records.to_vec(),
        summary,
        detail,
        report_modifier,
        context_modifier,
    })
}

fn aggregate_report_modifier(
    current: ToolReportModifier,
    next: &ToolReportModifier,
) -> ToolReportModifier {
    use ToolReportModifier::{NeedsAttention, None, Pending, Progress};

    match (current, next) {
        (NeedsAttention, _) | (_, NeedsAttention) => NeedsAttention,
        (Pending, _) | (_, Pending) => Pending,
        (Progress, _) | (_, Progress) => Progress,
        _ => None,
    }
}

fn summarize_result(
    tool_name: &str,
    result: &ToolResult,
) -> (
    ToolExecutionOutcomeKind,
    String,
    Option<String>,
    Option<PendingApprovalPayload>,
    ToolReportModifier,
) {
    match result {
        ToolResult::Text(text) if text.to_ascii_lowercase().contains("status=failed") => (
            ToolExecutionOutcomeKind::RecoverableFailure,
            format!("{tool_name} failed"),
            Some(text.clone()),
            None,
            ToolReportModifier::NeedsAttention,
        ),
        ToolResult::Text(text) => (
            ToolExecutionOutcomeKind::Success,
            format!("{tool_name} succeeded"),
            Some(text.clone()),
            None,
            ToolReportModifier::None,
        ),
        ToolResult::Denied(message) => (
            ToolExecutionOutcomeKind::Denied,
            format!("{tool_name} denied"),
            Some(message.clone()),
            None,
            ToolReportModifier::NeedsAttention,
        ),
        ToolResult::PendingApproval {
            message, approval, ..
        } => (
            ToolExecutionOutcomeKind::PendingApproval,
            approval.summary.clone(),
            approval.detail.clone().or_else(|| Some(message.clone())),
            Some(approval.clone()),
            ToolReportModifier::Pending,
        ),
        ToolResult::Interrupted(message) => (
            ToolExecutionOutcomeKind::Interrupted,
            format!("{tool_name} interrupted"),
            Some(message.clone()),
            None,
            ToolReportModifier::NeedsAttention,
        ),
        ToolResult::Progress(message) => (
            ToolExecutionOutcomeKind::Progress,
            format!("{tool_name} in progress"),
            Some(message.clone()),
            None,
            ToolReportModifier::Progress,
        ),
        ToolResult::ResultTooLarge(message) => (
            ToolExecutionOutcomeKind::ResultTooLarge,
            format!("{tool_name} result too large"),
            Some(message.clone()),
            None,
            ToolReportModifier::NeedsAttention,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_failed_text_is_not_summarized_as_success() {
        let record = build_execution_record(
            "Example",
            &ToolResult::Text(
                "status=failed\ntool=Example\nreason=tool_error\nmessage=boom".into(),
            ),
            None,
        );

        assert_eq!(record.kind, ToolExecutionOutcomeKind::RecoverableFailure);
        assert_ne!(record.kind, ToolExecutionOutcomeKind::Success);
        assert_eq!(record.summary, "Example failed");
        assert_eq!(record.report_modifier, ToolReportModifier::NeedsAttention);
        assert!(record.detail.unwrap().contains("status=failed"));
    }

    #[test]
    fn registry_invoke_error_result_is_recoverable_failure_outcome() {
        let result = registry_error_result("Example", "tool dispatch failed: boom".into());
        let outcome = build_outcome("Example".into(), result, None, 0, 1, false);

        assert_eq!(
            outcome.record.kind,
            ToolExecutionOutcomeKind::RecoverableFailure
        );
        assert_ne!(outcome.record.kind, ToolExecutionOutcomeKind::Success);
        assert_eq!(outcome.record.summary, "Example failed");
        assert_eq!(
            outcome.record.report_modifier,
            ToolReportModifier::NeedsAttention
        );
        assert!(
            outcome
                .record
                .detail
                .as_deref()
                .unwrap_or_default()
                .contains("status=failed")
        );
    }
}

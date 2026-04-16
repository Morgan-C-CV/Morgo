use serde::{Deserialize, Serialize};

use crate::tool::definition::ObservableInput;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingApprovalPayload {
    pub code: Option<String>,
    pub summary: String,
    pub detail: Option<String>,
    pub approval_kind: Option<String>,
    pub escalation_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ToolExecutionOutcomeKind {
    Success,
    Denied,
    PendingApproval,
    Interrupted,
    Progress,
    ResultTooLarge,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolBatchContext {
    pub batch_index: usize,
    pub batch_size: usize,
    pub executed_in_batch: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ToolReportModifier {
    None,
    Pending,
    Progress,
    NeedsAttention,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolExecutionRecord {
    pub tool_name: String,
    pub outcome: String,
    pub kind: ToolExecutionOutcomeKind,
    pub summary: String,
    pub detail: Option<String>,
    pub pending_approval: Option<PendingApprovalPayload>,
    pub report_modifier: ToolReportModifier,
    pub observable_input: Option<ObservableInput>,
    pub batch_context: ToolBatchContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ToolReportContextModifier {
    None,
    SetPendingToolUseSummary(String),
    ContinueWithUserMessage(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolExecutionReport {
    pub records: Vec<ToolExecutionRecord>,
    pub summary: String,
    pub detail: Option<String>,
    pub report_modifier: ToolReportModifier,
    pub context_modifier: ToolReportContextModifier,
}

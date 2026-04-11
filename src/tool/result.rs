use serde::{Deserialize, Serialize};

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
pub struct ToolExecutionRecord {
    pub tool_name: String,
    pub outcome: String,
    pub kind: ToolExecutionOutcomeKind,
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanStatus {
    Drafting,
    Ready,
    Approved,
    Executing,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanStepStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub title: String,
    pub details: Option<String>,
    pub status: PlanStepStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDraft {
    pub summary: String,
    pub steps: Vec<PlanStep>,
    pub notes: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanState {
    pub status: PlanStatus,
    pub draft: Option<PlanDraft>,
    pub approved_at: Option<String>,
    pub approval_summary: Option<String>,
}

impl Default for PlanState {
    fn default() -> Self {
        Self {
            status: PlanStatus::Drafting,
            draft: Some(PlanDraft::default()),
            approved_at: None,
            approval_summary: None,
        }
    }
}

impl Default for PlanDraft {
    fn default() -> Self {
        Self {
            summary: String::new(),
            steps: Vec::new(),
            notes: None,
            updated_at: None,
        }
    }
}

impl PlanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Drafting => "drafting",
            Self::Ready => "ready",
            Self::Approved => "approved",
            Self::Executing => "executing",
            Self::Completed => "completed",
        }
    }
}

impl PlanStepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
        }
    }
}

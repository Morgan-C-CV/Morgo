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
pub struct PlanExecutionState {
    pub active_step_id: Option<String>,
    pub completed_steps: usize,
    pub total_steps: usize,
    pub progress_percent: u8,
    pub last_updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanHistoryEntry {
    pub timestamp: String,
    pub action: String,
    pub summary: String,
    pub status: PlanStatus,
    pub draft: Option<PlanDraft>,
    pub execution: Option<PlanExecutionState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanState {
    pub status: PlanStatus,
    pub draft: Option<PlanDraft>,
    pub approved_at: Option<String>,
    pub approval_summary: Option<String>,
    pub execution: Option<PlanExecutionState>,
    pub history: Vec<PlanHistoryEntry>,
    pub next_step_id: usize,
}

impl Default for PlanState {
    fn default() -> Self {
        Self {
            status: PlanStatus::Drafting,
            draft: Some(PlanDraft::default()),
            approved_at: None,
            approval_summary: None,
            execution: Some(PlanExecutionState::default()),
            history: Vec::new(),
            next_step_id: 1,
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

impl Default for PlanExecutionState {
    fn default() -> Self {
        Self {
            active_step_id: None,
            completed_steps: 0,
            total_steps: 0,
            progress_percent: 0,
            last_updated_at: None,
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

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pending" => Some(Self::Pending),
            "in_progress" | "inprogress" | "doing" => Some(Self::InProgress),
            "completed" | "done" => Some(Self::Completed),
            _ => None,
        }
    }
}

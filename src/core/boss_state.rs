use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BossStage {
    /// Planning and discussion stage (Agent A & B in a documentation loop)
    Documentation,
    /// Waiting for user confirmation to proceed from Planning to Execution
    WaitingForApproval,
    /// Implementation stage (Agent B executing tasks, Agent A reviewing)
    Execution,
    /// Final review or completion
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BossStatus {
    pub stage: BossStage,
    pub current_step: Option<usize>,
    pub total_steps: Option<usize>,
    /// Path to the immutable planning file
    pub planning_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BossPlan {
    pub task_description: String,
    pub document_spec: String,
    pub pseudo_code: String,
    pub steps: Vec<BossPlanStep>,
    pub accepted_by_user: bool,
    pub auto_sequence: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BossPlanStep {
    pub id: usize,
    pub description: String,
    pub completed: bool,
    pub result_diff: Option<String>,
    pub worker_task_id: Option<String>,
}

impl Default for BossStatus {
    fn default() -> Self {
        Self {
            stage: BossStage::Documentation,
            current_step: None,
            total_steps: None,
            planning_file: None,
        }
    }
}

use crate::core::boss_state::{BossPlan, BossStage, BossStatus};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct BossCoordinator {
    pub status: Arc<RwLock<BossStatus>>,
    /// Placed here so the planner can hold and modify it in memory before flushing
    pub plan: Arc<RwLock<Option<BossPlan>>>,

    // Decoupled lightweight tracking (Prevents QueryContext RwLock Deadlocks):
    pub agent_a_session_id: Arc<RwLock<Option<String>>>,
    pub agent_b_session_id: Arc<RwLock<Option<String>>>,
    pub agent_a_cancel: Arc<RwLock<Option<CancellationToken>>>,
    pub agent_b_cancel: Arc<RwLock<Option<CancellationToken>>>,
}

impl BossCoordinator {
    pub fn new() -> Self {
        Self {
            status: Arc::new(RwLock::new(BossStatus::default())),
            plan: Arc::new(RwLock::new(None)),
            agent_a_session_id: Arc::new(RwLock::new(None)),
            agent_b_session_id: Arc::new(RwLock::new(None)),
            agent_a_cancel: Arc::new(RwLock::new(None)),
            agent_b_cancel: Arc::new(RwLock::new(None)),
        }
    }

    /// Attempts to restore a BossCoordinator from an existing planning file.
    /// If the file doesn't exist, it falls back to a fresh coordinator.
    pub async fn restore_or_init(path: &std::path::Path) -> anyhow::Result<Self> {
        let coordinator = Self::new();

        if path.exists() {
            let loaded_plan = load_plan(path).await?;

            // Determine stage based on plan progress
            let mut stage = BossStage::Documentation;
            if loaded_plan.accepted_by_user {
                let all_completed =
                    !loaded_plan.steps.is_empty() && loaded_plan.steps.iter().all(|s| s.completed);
                if all_completed {
                    stage = BossStage::Completed;
                } else {
                    stage = BossStage::Execution;
                }
            }

            // Figure out the current step (first uncompleted)
            let mut current_step = None;
            let total_steps = Some(loaded_plan.steps.len());
            if loaded_plan.accepted_by_user {
                current_step = loaded_plan
                    .steps
                    .iter()
                    .find(|s| !s.completed)
                    .map(|s| s.id);
            }

            {
                let mut status = coordinator.status.write().await;
                status.stage = stage;
                status.planning_file = Some(path.to_string_lossy().into_owned());
                status.current_step = current_step;
                status.total_steps = total_steps;
            }

            {
                let mut plan_guard = coordinator.plan.write().await;
                *plan_guard = Some(loaded_plan);
            }
        } else {
            let mut status = coordinator.status.write().await;
            status.planning_file = Some(path.to_string_lossy().into_owned());
        }

        Ok(coordinator)
    }

    pub async fn get_stage(&self) -> BossStage {
        self.status.read().await.stage
    }

    /// Enforces a strict DAG state transition to prevent invalid lifecycle jumps.
    pub async fn transition_to(&self, new_stage: BossStage) -> anyhow::Result<()> {
        let mut status = self.status.write().await;
        // Verify valid transition
        let valid = match (status.stage, new_stage) {
            (BossStage::Documentation, BossStage::WaitingForApproval) => true,
            (BossStage::WaitingForApproval, BossStage::Execution) => true,
            (BossStage::WaitingForApproval, BossStage::Documentation) => true, // Rejected by user
            (BossStage::Execution, BossStage::Completed) => true,
            (BossStage::Documentation, BossStage::Documentation) => true, // Re-entering valid
            (BossStage::Execution, BossStage::Documentation) => true,     // Fallback/Fatal failure
            _ => false,
        };

        if !valid {
            anyhow::bail!(
                "Invalid BossStage transition from {:?} to {:?}",
                status.stage,
                new_stage
            );
        }

        status.stage = new_stage;
        Ok(())
    }

    /// Returns the default path for the immutable planning cache.
    pub fn default_plan_path(root: &std::path::Path) -> std::path::PathBuf {
        root.join(".claude").join("boss").join("planning.json")
    }

    /// Handles the user confirmation for transitioning from Documentation -> Execution.
    /// MUST only be called when in WaitingForApproval.
    /// Returns true if user confirmed (Y/enter), false if they provided feedback (re-enter Documentation).
    pub async fn handle_user_approval(&self, user_input: &str) -> anyhow::Result<bool> {
        let path_to_save = {
            let status = self.status.read().await;
            if status.stage != BossStage::WaitingForApproval {
                tracing::warn!(
                    "handle_user_approval called in wrong state: {:?}",
                    status.stage
                );
                return Ok(false);
            }
            status.planning_file.clone()
        };

        if user_input.trim().to_uppercase() == "Y" || user_input.trim().is_empty() {
            // Update in-memory plan flag
            {
                let mut plan_guard = self.plan.write().await;
                if let Some(plan) = plan_guard.as_mut() {
                    plan.accepted_by_user = true;
                }
            }

            // Always flush to disk if path is provided
            if let Some(path_str) = path_to_save {
                let path = std::path::PathBuf::from(path_str);
                if let Some(plan) = self.plan.read().await.as_ref() {
                    save_plan(plan, &path).await?;
                }
            }

            self.transition_to(BossStage::Execution).await?;
            Ok(true)
        } else {
            self.transition_to(BossStage::Documentation).await?;
            Ok(false)
        }
    }

    /// Processes a task completion event to update the BossPlan status.
    pub async fn on_task_event(&self, event: &crate::task::types::TaskEvent) -> anyhow::Result<()> {
        let step_id = match event.step_id {
            Some(id) => id,
            None => return Ok(()),
        };

        let mut plan_guard = self.plan.write().await;
        let plan = match plan_guard.as_mut() {
            Some(p) => p,
            None => return Ok(()),
        };

        if let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) {
            if event.status == crate::task::types::TaskStatus::Completed {
                step.completed = true;
                step.worker_task_id = Some(event.task_id.clone());
                tracing::info!("BossPlan: Step {} marked as completed", step_id);
                
                // Update status current_step
                let mut status = self.status.write().await;
                status.current_step = plan.steps.iter().find(|s| !s.completed).map(|s| s.id);
            }
        }

        Ok(())
    }

    /// Simplified entry point for dispatcher updates.
    pub async fn on_notification(&self, notification: &crate::interaction::notification::Notification) -> anyhow::Result<()> {
        if notification.notification_type != crate::interaction::notification::NotificationType::TaskUpdate {
            return Ok(());
        }

        let step_id = match notification.step_id {
            Some(id) => id,
            None => return Ok(()),
        };

        let mut plan_guard = self.plan.write().await;
        let plan = match plan_guard.as_mut() {
            Some(p) => p,
            None => return Ok(()),
        };

        if let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) {
            // We consider the step completed only if the status is "Completed" (case-insensitive check)
            let status = notification.status.as_deref().unwrap_or("");
            if status.to_lowercase() == "completed" {
                step.completed = true;
                step.worker_task_id = notification.task_id.clone();
                tracing::info!("BossPlan: Step {} marked as completed via notification", step_id);
                
                // Update status current_step
                let mut status_guard = self.status.write().await;
                status_guard.current_step = plan.steps.iter().find(|s| !s.completed).map(|s| s.id);
            }
        }

        Ok(())
    }

    /// Automatically scans for the next runnable step and returns it.
    pub async fn get_next_runnable_step(&self) -> Option<usize> {
        let plan_guard = self.plan.read().await;
        let plan = plan_guard.as_ref()?;
        
        if plan.steps.iter().all(|s| s.completed) {
            return None;
        }

        plan.steps.iter()
            .find(|s| !s.completed)
            .map(|s| s.id)
    }

    /// Advances the plan by spawning the next worker if applicable.
    /// This is the core of the Auto-Sequencing engine.
    pub async fn advance_plan(&self, app_state: &Arc<crate::state::app_state::AppState>) -> anyhow::Result<Option<String>> {
        let next_step_id = match self.get_next_runnable_step().await {
            Some(id) => id,
            None => return Ok(None),
        };

        let (task_desc, role, orchestration_group_id) = {
            let plan_guard = self.plan.read().await;
            let plan = plan_guard.as_ref().ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            
            if !plan.auto_sequence {
                return Ok(None);
            }

            let step = plan.steps.iter().find(|s| s.id == next_step_id).unwrap();
            let desc = format!("{} (Boss Mode Auto-Step {})", step.description, next_step_id);
            // Default to Implement for execution steps if not specified, 
            // but we might want to infer this from the step description in the future.
            (desc, crate::state::app_state::WorkerRole::Implement, plan.steps[0].worker_task_id.as_deref().map(|id| id.to_owned()))
        };

        // We need ToolRegistry to invoke AgentTool.
        // Or we can just call launch_agent_task directly if we can access it.
        // For now, let's just mark it as "ready to advance" in the state 
        // until we decide on the best way to trigger the actual tool call.
        
        tracing::info!("BossPlan: Auto-advancing to step {}", next_step_id);
        
        Ok(Some(format!("Next recommended action: Spawn agent for step {}: {}", next_step_id, task_desc)))
    }
}

impl Default for BossCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Saves a boss plan to a file using atomic write to prevent corruption.
pub async fn save_plan(plan: &BossPlan, path: &std::path::Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let content = serde_json::to_string_pretty(plan)?;
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, content).await?;
    tokio::fs::rename(tmp_path, path).await?;

    Ok(())
}

/// Loads a boss plan from a file (free function, no self needed).
pub async fn load_plan(path: &std::path::Path) -> anyhow::Result<BossPlan> {
    let content = tokio::fs::read_to_string(path).await?;
    let plan = serde_json::from_str(&content)?;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_boss_coordinator_initial_stage_is_documentation() {
        let coordinator = BossCoordinator::new();
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn test_state_transition_to_waiting_for_approval() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);
    }

    #[tokio::test]
    async fn test_user_approval_y_transitions_to_execution() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        // set dummy plan to avoid ignoring boolean conversion
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan::default());
        }
        let confirmed = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(confirmed);
        assert_eq!(coordinator.get_stage().await, BossStage::Execution);
        assert!(
            coordinator
                .plan
                .read()
                .await
                .as_ref()
                .unwrap()
                .accepted_by_user
        );
    }

    #[tokio::test]
    async fn test_user_approval_feedback_returns_to_documentation() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        let rejected = coordinator
            .handle_user_approval("Wait, this is wrong")
            .await
            .unwrap();
        assert!(!rejected);
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn test_handle_user_approval_rejects_call_from_wrong_state() {
        let coordinator = BossCoordinator::new();
        // Still in Documentation (not WaitingForApproval) — should be a no-op and return false
        let result = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(!result);
        // Should remain unchanged
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn test_boss_plan_persistence() {
        let plan = BossPlan {
            task_description: "Fix bugs".into(),
            document_spec: "Spec v1".into(),
            pseudo_code: "Code v1".into(),
            steps: vec![],
            accepted_by_user: true,
        };

        let temp_dir = std::env::temp_dir();
        let plan_path = temp_dir.join("boss_test_plan.json");

        save_plan(&plan, &plan_path).await.unwrap();
        let loaded = load_plan(&plan_path).await.unwrap();

        assert_eq!(loaded.task_description, "Fix bugs");
        assert_eq!(loaded.document_spec, "Spec v1");
        assert!(loaded.accepted_by_user);

        std::fs::remove_file(plan_path).unwrap();
    }

    #[test]
    fn test_default_plan_path_uses_claude_boss_dir() {
        let root = std::path::Path::new("/home/user/project");
        let path = BossCoordinator::default_plan_path(root);
        assert_eq!(
            path,
            std::path::Path::new("/home/user/project/.claude/boss/planning.json")
        );
    }

    #[tokio::test]
    async fn test_restore_or_init_handles_state_properly() {
        let temp_dir = std::env::temp_dir();
        let plan_path = temp_dir.join("boss_test_restore_plan.json");

        // 1. Init without file
        let new_coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
        assert_eq!(new_coordinator.get_stage().await, BossStage::Documentation);
        assert_eq!(
            new_coordinator
                .status
                .read()
                .await
                .planning_file
                .as_ref()
                .unwrap(),
            &plan_path.to_string_lossy().into_owned()
        );

        // 2. Save a plan that is accepted
        let plan = BossPlan {
            task_description: "task".into(),
            accepted_by_user: true,
            steps: vec![crate::core::boss_state::BossPlanStep {
                id: 0,
                description: "".into(),
                completed: false,
                result_diff: None,
            }],
            ..Default::default()
        };
        save_plan(&plan, &plan_path).await.unwrap();

        // 3. Restore and verify it skips straight to Execution
        let restored = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
        assert_eq!(restored.get_stage().await, BossStage::Execution);
        assert_eq!(restored.status.read().await.current_step, Some(0));

        std::fs::remove_file(plan_path).unwrap();
    }
}

use crate::core::boss_state::{
    BossActorHandle, BossActorStatus, BossPlan, BossPlanStep, BossPlanStepStatus, BossSession,
    BossStage, BossStatus,
};
use crate::task::types::{TaskEvent, TaskStatus};
use crate::tool::definition::{Tool, ToolCall};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct BossCoordinator {
    pub status: Arc<RwLock<BossStatus>>,
    /// Placed here so the planner can hold and modify it in memory before flushing
    pub plan: Arc<RwLock<Option<BossPlan>>>,

    /// Structured actor topology — replaces the former loose a/b session_id/cancel fields.
    pub session: Arc<RwLock<Option<BossSession>>>,

    auto_advance_app_state: Arc<RwLock<Option<Arc<crate::state::app_state::AppState>>>>,
}

impl BossCoordinator {
    pub fn new() -> Self {
        Self {
            status: Arc::new(RwLock::new(BossStatus::default())),
            plan: Arc::new(RwLock::new(None)),
            session: Arc::new(RwLock::new(None)),
            auto_advance_app_state: Arc::new(RwLock::new(None)),
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
                *plan_guard = Some(loaded_plan.clone());
            }

            // Init actor session from plan — deterministic A/B ids, no real spawn yet.
            {
                let mut session_guard = coordinator.session.write().await;
                *session_guard = Some(BossSession::from_plan_id(&loaded_plan.plan_id, stage));
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

    /// Ensures a BossSession exists for the given plan_id, creating one if absent.
    /// Idempotent: if a session already exists for the same plan_id it is returned unchanged.
    pub async fn ensure_actor_session(&self, plan_id: &str, stage: BossStage) {
        let mut guard = self.session.write().await;
        if guard.as_ref().map(|s| s.plan_id.as_str()) != Some(plan_id) {
            *guard = Some(BossSession::from_plan_id(plan_id, stage));
        }
    }

    /// Returns a point-in-time snapshot of all tracked actor handles (A, B, and children).
    pub async fn actor_registry_snapshot(&self) -> Vec<BossActorHandle> {
        let guard = self.session.read().await;
        let Some(session) = guard.as_ref() else {
            return Vec::new();
        };
        let mut handles = vec![session.designer_a.clone(), session.executor_b.clone()];
        handles.extend(session.active_children.iter().cloned());
        handles
    }

    /// Updates the status of a tracked actor by actor_id.
    pub async fn update_actor_status(&self, actor_id: &str, status: BossActorStatus) {
        let mut guard = self.session.write().await;
        let Some(session) = guard.as_mut() else {
            return;
        };
        for handle in std::iter::once(&mut session.designer_a)
            .chain(std::iter::once(&mut session.executor_b))
            .chain(session.active_children.iter_mut())
        {
            if handle.actor_id == actor_id {
                handle.status = status;
                handle.last_snapshot = Some(std::time::SystemTime::now());
                return;
            }
        }
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

    /// Records one Documentation-stage red/blue loop pass.
    ///
    /// A drafts the spec, B reviews feasibility/risk/testability, then A revises.
    /// The revised spec becomes the immutable planning content and the coordinator
    /// transitions to `WaitingForApproval`.
    pub async fn finalize_documentation_loop(
        &self,
        draft_spec: &str,
        review_feedback: &str,
        revision_notes: &str,
        final_document_spec: &str,
        final_pseudo_code: &str,
    ) -> anyhow::Result<()> {
        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            plan.draft_spec = Some(draft_spec.to_string());
            plan.review_feedback = Some(review_feedback.to_string());
            plan.revision_notes = Some(revision_notes.to_string());
            plan.document_spec = final_document_spec.to_string();
            plan.pseudo_code = final_pseudo_code.to_string();
            plan.finalized = true;
            plan.accepted_by_user = false;
        }

        let path_to_save = self.status.read().await.planning_file.clone();
        if let Some(path_str) = path_to_save {
            let path = std::path::PathBuf::from(path_str);
            if let Some(plan) = self.plan.read().await.as_ref() {
                save_plan(plan, &path).await?;
            }
        }

        self.transition_to(BossStage::WaitingForApproval).await?;
        Ok(())
    }

    /// Records user feedback while in the documentation/approval loop.
    /// Any non-confirmation input during `WaitingForApproval` reopens Documentation,
    /// preserving a feedback trail for the next A/B revision pass.
    pub async fn record_documentation_feedback(&self, feedback: &str) -> anyhow::Result<()> {
        let trimmed = feedback.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            plan.documentation_feedback.push(trimmed.to_string());
            plan.accepted_by_user = false;
            plan.finalized = false;
        }

        let path_to_save = self.status.read().await.planning_file.clone();
        if let Some(path_str) = path_to_save {
            let path = std::path::PathBuf::from(path_str);
            if let Some(plan) = self.plan.read().await.as_ref() {
                save_plan(plan, &path).await?;
            }
        }

        self.transition_to(BossStage::Documentation).await?;
        Ok(())
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
            self.record_documentation_feedback(user_input).await?;
            Ok(false)
        }
    }

    /// Processes a task event to update the BossPlan by structured step identity.
    pub async fn on_task_event(&self, event: &TaskEvent) -> anyhow::Result<()> {
        // Group fan-in: task_id starts with "group-" and orchestration_group_id is B's task id.
        // Find the step whose worker_task_id matches the group_id (B's task id).
        if event.task_id.starts_with("group-") {
            if let Some(group_id) = &event.orchestration_group_id {
                let mut plan_guard = self.plan.write().await;
                let Some(plan) = plan_guard.as_mut() else {
                    return Ok(());
                };
                let step = plan.steps.iter_mut().find(|s| {
                    s.worker_task_id.as_deref() == Some(group_id.as_str())
                });
                if let Some(step) = step {
                    let step_id = step.id;
                    match event.status {
                        TaskStatus::Completed => {
                            // Fan-in complete — enter Reviewing, not Completed.
                            // A review gate must accept before the step advances.
                            step.status = BossPlanStepStatus::Reviewing;
                            tracing::info!("BossPlan: Step {} fan-in complete, entering Reviewing", step_id);
                        }
                        TaskStatus::Failed | TaskStatus::Killed => {
                            step.status = BossPlanStepStatus::Failed;
                            tracing::warn!("BossPlan: Step {} fan-in failed via group {}", step_id, group_id);
                        }
                        _ => {}
                    }
                    drop(plan_guard);
                    // No auto-advance here — A review must call on_review_event to proceed.
                    return Ok(());
                }
            }
        }

        if event.orchestration_group_id.is_some() {
            tracing::debug!(
                "BossPlan: ignoring non-group child event {} with orchestration group {:?}",
                event.task_id,
                event.orchestration_group_id
            );
            return Ok(());
        }

        let Some(step_id) = event.step_id else {
            return Ok(());
        };

        let mut plan_guard = self.plan.write().await;
        let Some(plan) = plan_guard.as_mut() else {
            return Ok(());
        };

        let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
            return Ok(());
        };

        let should_auto_advance = match event.status {
            TaskStatus::Completed => {
                let was_completed = step.completed || step.status == BossPlanStepStatus::Completed;
                step.completed = true;
                step.status = BossPlanStepStatus::Completed;
                step.worker_task_id = Some(event.task_id.clone());
                tracing::info!("BossPlan: Step {} marked as completed", step_id);
                !was_completed
            }
            TaskStatus::Failed | TaskStatus::Killed => {
                step.completed = false;
                step.status = BossPlanStepStatus::Failed;
                step.worker_task_id = Some(event.task_id.clone());
                tracing::warn!("BossPlan: Step {} marked as failed", step_id);
                false
            }
            TaskStatus::Running => {
                step.status = BossPlanStepStatus::Running;
                step.worker_task_id = Some(event.task_id.clone());
                false
            }
            TaskStatus::Pending => false,
        };

        let next_step = next_unfinished_step_id(plan);
        drop(plan_guard);
        self.update_current_step(next_step).await;
        if should_auto_advance {
            self.maybe_auto_advance_after_completion().await?;
        }

        Ok(())
    }

    /// Called by A's review agent when it has a verdict on a step.
    ///
    /// `accepted = true` → step moves to Completed and auto-advance fires.
    /// `accepted = false` → step moves to Rejected; if under retry budget, correction is stored
    ///   and the step is reset to Pending so the next `advance_plan` re-dispatches B.
    ///   If over budget, step is marked Failed.
    pub async fn on_review_event(
        &self,
        step_id: usize,
        accepted: bool,
        review_summary: &str,
        correction: Option<&str>,
    ) -> anyhow::Result<()> {
        let should_auto_advance = {
            let mut plan_guard = self.plan.write().await;
            let Some(plan) = plan_guard.as_mut() else {
                return Ok(());
            };
            let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
                return Ok(());
            };

            step.last_review_summary = Some(review_summary.to_string());

            if accepted {
                step.completed = true;
                step.status = BossPlanStepStatus::Completed;
                step.last_correction = None;
                tracing::info!("BossPlan: Step {} accepted by A review", step_id);
                true
            } else {
                step.attempt_count += 1;
                if step.attempt_count >= step.retry_budget {
                    step.status = BossPlanStepStatus::Failed;
                    tracing::warn!(
                        "BossPlan: Step {} rejected by A review, retry budget exhausted ({}/{})",
                        step_id, step.attempt_count, step.retry_budget
                    );
                } else {
                    step.status = BossPlanStepStatus::Rejected;
                    step.last_correction = correction.map(str::to_string);
                    tracing::info!(
                        "BossPlan: Step {} rejected by A review, attempt {}/{}, queuing retry",
                        step_id, step.attempt_count, step.retry_budget
                    );
                }
                false
            }
        };

        if should_auto_advance {
            let plan_guard = self.plan.read().await;
            let next_step = plan_guard.as_ref().and_then(|p| next_unfinished_step_id(p));
            drop(plan_guard);
            self.update_current_step(next_step).await;
            self.maybe_auto_advance_after_completion().await?;
        }

        Ok(())
    }

    /// Simplified entry point for dispatcher updates.
    pub async fn on_notification(
        &self,
        notification: &crate::interaction::notification::Notification,
    ) -> anyhow::Result<()> {
        if notification.notification_type
            != crate::interaction::notification::NotificationType::TaskUpdate
        {
            return Ok(());
        }

        let step_id = match notification.step_id {
            Some(id) => id,
            None => return Ok(()),
        };

        let mut plan_guard = self.plan.write().await;
        let Some(plan) = plan_guard.as_mut() else {
            return Ok(());
        };

        let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
            return Ok(());
        };

        let should_auto_advance = match notification.status.as_deref().unwrap_or_default() {
            status if status.eq_ignore_ascii_case("completed") => {
                let was_completed = step.completed || step.status == BossPlanStepStatus::Completed;
                step.completed = true;
                step.status = BossPlanStepStatus::Completed;
                step.worker_task_id = notification.task_id.clone();
                tracing::info!(
                    "BossPlan: Step {} marked as completed via notification",
                    step_id
                );
                !was_completed
            }
            status
                if status.eq_ignore_ascii_case("failed")
                    || status.eq_ignore_ascii_case("killed") =>
            {
                step.completed = false;
                step.status = BossPlanStepStatus::Failed;
                step.worker_task_id = notification.task_id.clone();
                tracing::warn!(
                    "BossPlan: Step {} marked as failed via notification",
                    step_id
                );
                false
            }
            status if status.eq_ignore_ascii_case("running") => {
                step.status = BossPlanStepStatus::Running;
                step.worker_task_id = notification.task_id.clone();
                false
            }
            _ => false,
        };

        let next_step = next_unfinished_step_id(plan);
        drop(plan_guard);
        self.update_current_step(next_step).await;
        if should_auto_advance {
            self.maybe_auto_advance_after_completion().await?;
        }

        Ok(())
    }

    async fn maybe_auto_advance_after_completion(&self) -> anyhow::Result<()> {
        let app_state = self.auto_advance_app_state.read().await.clone();
        let Some(app_state) = app_state else {
            return Ok(());
        };
        let _ = self.advance_plan(&app_state).await?;
        Ok(())
    }

    /// Automatically scans for the next runnable step and returns it.
    pub async fn get_next_runnable_step(&self) -> Option<usize> {
        let plan_guard = self.plan.read().await;
        let plan = plan_guard.as_ref()?;
        next_runnable_step(plan).map(|step| step.id)
    }

    /// Returns the running ExecutorB task id if one exists in the task manager.
    /// Returns None if B has no live task (caller must spawn fresh).
    fn find_running_b_task_id(
        &self,
        session: &crate::core::boss_state::BossSession,
        tasks: &crate::task::manager::TaskManager,
    ) -> Option<String> {
        let task_id = session.executor_b.task_id.as_ref()?;
        let record = tasks.get(task_id)?;
        if matches!(
            record.status,
            crate::task::types::TaskStatus::Running | crate::task::types::TaskStatus::Pending
        ) {
            Some(task_id.clone())
        } else {
            None
        }
    }

    /// Builds a Continue payload that sends step context to a running ExecutorB task.
    pub async fn build_step_continue_payload(
        &self,
        step_id: usize,
        b_task_id: &str,
        parent_session_id: &str,
    ) -> anyhow::Result<String> {
        let plan_guard = self.plan.read().await;
        let plan = plan_guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter()
            .find(|s| s.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;

        let message = format!(
            "Boss step {step_id}\nplan_id: {}\nobjective: {}\nacceptance:\n{}",
            plan.plan_id,
            step.objective(),
            format_acceptance(step),
        );

        Ok(json!({
            "task_id": b_task_id,
            "message": message,
            "step_id": step_id,
            "boss_plan_id": plan.plan_id,
            "step_objective": step.objective(),
            "step_acceptance": step.acceptance,
            "parent_session_id": parent_session_id,
        })
        .to_string())
    }

    pub async fn build_step_spawn_payload(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<String> {
        let plan_guard = self.plan.read().await;
        let plan = plan_guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter()
            .find(|step| step.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;

        Ok(json!({
            "task": format!(
                "Boss mode step {}\nplan_id: {}\nobjective: {}\nacceptance:\n{}{}",
                step.id,
                plan.plan_id,
                step.objective(),
                format_acceptance(step),
                step.last_correction.as_deref()
                    .map(|c| format!("\ncorrection from review:\n{c}"))
                    .unwrap_or_default(),
            ),
            "role": "implement",
            "inherit_context": true,
            "reuse_strategy": "running_only",
            "step_id": step.id,
            "boss_plan_id": plan.plan_id,
            "step_objective": step.objective(),
            "step_acceptance": step.acceptance,
            "parent_session_id": parent_session_id,
            "parent_runtime_role": "coordinator",
            "orchestration_group_id": b_actor_id,
            "boss_actor_role": "executor_b",
            "boss_lineage_depth": 0,
        })
        .to_string())
    }

    /// Advances the plan by selecting the next deterministic action.
    pub async fn advance_plan(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Option<String>> {
        {
            let mut auto_advance_app_state = self.auto_advance_app_state.write().await;
            if auto_advance_app_state.is_none() {
                *auto_advance_app_state = Some(app_state.clone());
            }
        }
        let parent_session_id = app_state.active_session_id.clone();
        let next_action = {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;

            if !plan.auto_sequence {
                return Ok(None);
            }

            if plan.steps.iter().all(|step| step.completed) {
                Some(AdvanceOutcome::PlanComplete)
            } else if plan
                .steps
                .iter()
                .any(|step| step.status.is_terminal_failure())
            {
                Some(AdvanceOutcome::TerminalFailure)
            } else if plan
                .steps
                .iter()
                .any(|step| step.status == BossPlanStepStatus::Running)
            {
                None
            } else if let Some(step_id) = next_runnable_step(plan).map(|step| step.id) {
                let step = plan
                    .steps
                    .iter_mut()
                    .find(|step| step.id == step_id)
                    .expect("runnable step must exist");
                if step.requires_approval {
                    Some(AdvanceOutcome::ApprovalBarrier(step.id))
                } else {
                    step.status = BossPlanStepStatus::Running;
                    Some(AdvanceOutcome::Dispatch(step_id))
                }
            } else {
                Some(AdvanceOutcome::NoRunnableStep)
            }
        };

        match next_action {
            Some(AdvanceOutcome::PlanComplete) => {
                self.update_current_step(None).await;
                self.transition_to(BossStage::Completed).await?;
                Ok(Some(
                    "Boss plan complete; no further steps to dispatch.".into(),
                ))
            }
            Some(AdvanceOutcome::TerminalFailure) => Ok(Some(
                "Boss plan stopped after a terminal step failure; auto-advance halted.".into(),
            )),
            Some(AdvanceOutcome::ApprovalBarrier(step_id)) => {
                self.update_step_status(step_id, BossPlanStepStatus::WaitingForApproval)
                    .await?;
                self.update_current_step(Some(step_id)).await;
                Ok(Some(format!(
                    "Boss plan paused before step {} because it requires approval.",
                    step_id
                )))
            }
            Some(AdvanceOutcome::Dispatch(step_id)) => {
                self.update_current_step(Some(step_id)).await;

                let tasks = app_state.permission_context.task_manager.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;

                // Check if B already has a live task — if so, send step context via Continue.
                let running_b = {
                    let guard = self.session.read().await;
                    guard.as_ref().and_then(|s| self.find_running_b_task_id(s, tasks))
                };

                let payload = if let Some(b_task_id) = running_b {
                    // B is alive — deliver step context via Continue (no new task).
                    let continue_payload = self
                        .build_step_continue_payload(step_id, &b_task_id, &parent_session_id)
                        .await?;
                    self.invoke_agent_tool(app_state, &continue_payload).await?;
                    continue_payload
                } else {
                    // B is not running — spawn fresh and record the new task id.
                    let b_actor_id = {
                        let guard = self.session.read().await;
                        guard.as_ref()
                            .map(|s| s.executor_b.actor_id.clone())
                            .unwrap_or_else(|| "boss-unknown-b".into())
                    };
                    let spawn_payload = self
                        .build_step_spawn_payload(step_id, &parent_session_id, &b_actor_id)
                        .await?;
                    self.invoke_agent_tool(app_state, &spawn_payload).await?;
                    // Record the newly created task id in the B handle.
                    let records = tasks.list();
                    if let Some(task) = records.last() {
                        let mut guard = self.session.write().await;
                        if let Some(session) = guard.as_mut() {
                            session.executor_b.task_id = Some(task.id.clone());
                            session.executor_b.status = BossActorStatus::Active;
                        }
                    }
                    spawn_payload
                };

                Ok(Some(payload))
            }
            Some(AdvanceOutcome::NoRunnableStep) | None => Ok(None),
        }
    }

    async fn invoke_agent_tool(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        payload: &str,
    ) -> anyhow::Result<()> {
        let agent_tool = crate::tool::builtin::agent::AgentTool;
        let result = agent_tool
            .invoke(
                &ToolCall::new("Agent", payload),
                &app_state.permission_context,
            )
            .await?;

        match result {
            crate::tool::definition::ToolResult::Text(_) => Ok(()),
            crate::tool::definition::ToolResult::Denied(reason) => {
                anyhow::bail!("boss worker dispatch denied: {reason}")
            }
            crate::tool::definition::ToolResult::PendingApproval { message, .. } => {
                anyhow::bail!("boss worker dispatch requires approval: {message}")
            }
            crate::tool::definition::ToolResult::Interrupted(reason) => {
                anyhow::bail!("boss worker dispatch interrupted: {reason}")
            }
            crate::tool::definition::ToolResult::Progress(message) => {
                anyhow::bail!(
                    "boss worker dispatch returned progress instead of spawn result: {message}"
                )
            }
            crate::tool::definition::ToolResult::ResultTooLarge(reason) => {
                anyhow::bail!("boss worker dispatch returned oversized result: {reason}")
            }
        }
    }

    async fn update_current_step(&self, current_step: Option<usize>) {
        let mut status = self.status.write().await;
        status.current_step = current_step;
    }

    async fn update_step_status(
        &self,
        step_id: usize,
        next_status: BossPlanStepStatus,
    ) -> anyhow::Result<()> {
        let mut plan_guard = self.plan.write().await;
        let plan = plan_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter_mut()
            .find(|step| step.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;
        step.status = next_status;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvanceOutcome {
    Dispatch(usize),
    ApprovalBarrier(usize),
    TerminalFailure,
    PlanComplete,
    NoRunnableStep,
}

fn next_unfinished_step_id(plan: &BossPlan) -> Option<usize> {
    plan.steps
        .iter()
        .find(|step| !step.completed)
        .map(|step| step.id)
}

fn next_runnable_step(plan: &BossPlan) -> Option<&BossPlanStep> {
    plan.steps.iter().find(|step| {
        !step.completed
            && matches!(
                step.status,
                BossPlanStepStatus::Pending
                    | BossPlanStepStatus::WaitingForApproval
                    | BossPlanStepStatus::Rejected
            )
    })
}

fn format_acceptance(step: &BossPlanStep) -> String {
    if step.acceptance.is_empty() {
        "- Complete the step objective.".into()
    } else {
        step.acceptance
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
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
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan::default());
        }
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
            plan_id: "plan-test".into(),
            task_description: "Fix bugs".into(),
            document_spec: "Spec v1".into(),
            pseudo_code: "Code v1".into(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![],
            accepted_by_user: true,
            auto_sequence: false,
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
            plan_id: "plan-restore".into(),
            task_description: "task".into(),
            accepted_by_user: true,
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![crate::core::boss_state::BossPlanStep {
                id: 0,
                description: "".into(),
                objective: None,
                acceptance: Vec::new(),
                requires_approval: false,
                status: BossPlanStepStatus::Pending,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                review_task_id: None,
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

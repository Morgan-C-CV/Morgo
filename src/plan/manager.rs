use std::sync::{Arc, RwLock};

use crate::history::session::{SessionId, SessionStore};
use crate::plan::types::{
    PlanDraft, PlanExecutionState, PlanHistoryEntry, PlanState, PlanStatus, PlanStep,
    PlanStepStatus,
};

#[derive(Clone)]
struct PlanPersistence {
    session_store: Arc<dyn SessionStore>,
    session_id: SessionId,
}

#[derive(Clone)]
pub struct PlanManager {
    state: Arc<RwLock<Option<PlanState>>>,
    persistence: Option<PlanPersistence>,
}

impl std::fmt::Debug for PlanManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanManager")
            .field("state", &self.state())
            .field("persistent", &self.persistence.is_some())
            .finish()
    }
}

impl Default for PlanManager {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
            persistence: None,
        }
    }
}

impl PlanManager {
    pub fn from_state(state: PlanState) -> Self {
        Self {
            state: Arc::new(RwLock::new(Some(state))),
            persistence: None,
        }
    }

    pub fn with_persistence(
        mut self,
        session_store: Arc<dyn SessionStore>,
        session_id: SessionId,
    ) -> Self {
        self.persistence = Some(PlanPersistence {
            session_store,
            session_id,
        });
        self
    }

    pub fn state(&self) -> Option<PlanState> {
        self.state.read().ok().and_then(|slot| slot.clone())
    }

    pub fn ensure_draft(&self, note: Option<&str>) -> PlanState {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            if state.draft.is_none() {
                state.draft = Some(PlanDraft::default());
            }
            state.status = PlanStatus::Drafting;
            if let Some(note) = note.map(str::trim).filter(|value| !value.is_empty()) {
                if let Some(draft) = state.draft.as_mut() {
                    draft.notes = Some(note.to_string());
                    draft.updated_at = Some(timestamp_now());
                }
            }
            recalculate_execution(&mut state);
            push_history(
                &mut state,
                "ensure_draft",
                "entered or refreshed draft state",
            );
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        updated
    }

    pub fn set_summary(&self, summary: &str) -> PlanState {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            let summary_text = summary.trim().to_string();
            {
                let draft = state.draft.get_or_insert_with(PlanDraft::default);
                draft.summary = summary_text.clone();
                draft.updated_at = Some(timestamp_now());
            }
            sync_status_from_draft(&mut state);
            recalculate_execution(&mut state);
            push_history(
                &mut state,
                "set_summary",
                format!("updated summary to {}", summarize_text(&summary_text)),
            );
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        updated
    }

    pub fn add_step(&self, title: &str, details: Option<&str>) -> anyhow::Result<PlanStep> {
        let added = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            let title = title.trim();
            if title.is_empty() {
                anyhow::bail!("plan step title cannot be empty");
            }
            let step = PlanStep {
                id: format!("step-{}", state.next_step_id),
                title: title.to_string(),
                details: normalize_optional(details),
                status: PlanStepStatus::Pending,
            };
            state.next_step_id += 1;
            let draft = state.draft.get_or_insert_with(PlanDraft::default);
            draft.steps.push(step.clone());
            draft.updated_at = Some(timestamp_now());
            sync_status_from_draft(&mut state);
            recalculate_execution(&mut state);
            push_history(&mut state, "add_step", format!("added {}", step.id));
            *slot = Some(state);
            step
        };
        self.persist_state();
        Ok(added)
    }

    pub fn update_step(
        &self,
        step_id: &str,
        title: Option<&str>,
        details: Option<Option<&str>>,
        status: Option<PlanStepStatus>,
    ) -> anyhow::Result<PlanState> {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            let draft = state
                .draft
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("plan draft is missing"))?;
            let step = draft
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or_else(|| anyhow::anyhow!("unknown plan step: {step_id}"))?;
            if let Some(title) = title {
                let trimmed = title.trim();
                if trimmed.is_empty() {
                    anyhow::bail!("plan step title cannot be empty");
                }
                step.title = trimmed.to_string();
            }
            if let Some(details) = details {
                step.details = details.and_then(|value| {
                    let trimmed = value.trim();
                    (!trimmed.is_empty()).then(|| trimmed.to_string())
                });
            }
            if let Some(status) = status {
                step.status = status;
            }
            draft.updated_at = Some(timestamp_now());
            sync_status_from_draft(&mut state);
            recalculate_execution(&mut state);
            push_history(&mut state, "update_step", format!("updated {step_id}"));
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        Ok(updated)
    }

    pub fn mark_step_status(
        &self,
        step_id: &str,
        status: PlanStepStatus,
    ) -> anyhow::Result<PlanState> {
        let updated = self.update_step(step_id, None, None, Some(status))?;
        Ok(updated)
    }

    pub fn remove_step(&self, step_id: &str) -> anyhow::Result<PlanState> {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            let draft = state
                .draft
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("plan draft is missing"))?;
            let original_len = draft.steps.len();
            draft.steps.retain(|step| step.id != step_id);
            if draft.steps.len() == original_len {
                anyhow::bail!("unknown plan step: {step_id}");
            }
            draft.updated_at = Some(timestamp_now());
            sync_status_from_draft(&mut state);
            recalculate_execution(&mut state);
            push_history(&mut state, "remove_step", format!("removed {step_id}"));
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        Ok(updated)
    }

    pub fn reorder_steps(&self, ordered_ids: &[String]) -> anyhow::Result<PlanState> {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            let draft = state
                .draft
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("plan draft is missing"))?;
            if ordered_ids.len() != draft.steps.len() {
                anyhow::bail!("reorder requires exactly {} step ids", draft.steps.len());
            }
            let mut reordered = Vec::with_capacity(draft.steps.len());
            for step_id in ordered_ids {
                let index = draft
                    .steps
                    .iter()
                    .position(|step| &step.id == step_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown plan step: {step_id}"))?;
                reordered.push(draft.steps[index].clone());
            }
            draft.steps = reordered;
            draft.updated_at = Some(timestamp_now());
            sync_status_from_draft(&mut state);
            recalculate_execution(&mut state);
            push_history(
                &mut state,
                "reorder_steps",
                format!("reordered {} steps", ordered_ids.len()),
            );
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        Ok(updated)
    }

    pub fn history(&self) -> Vec<PlanHistoryEntry> {
        self.state().map(|state| state.history).unwrap_or_default()
    }

    pub fn approve(&self, summary: Option<&str>) -> anyhow::Result<PlanState> {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            let mut state = slot.clone().unwrap_or_default();
            let draft = state
                .draft
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("plan draft is missing"))?;
            if draft.summary.trim().is_empty() && draft.steps.is_empty() {
                anyhow::bail!("cannot approve an empty plan draft");
            }
            state.status = if draft
                .steps
                .iter()
                .all(|step| step.status == PlanStepStatus::Completed)
                && !draft.steps.is_empty()
            {
                PlanStatus::Completed
            } else {
                PlanStatus::Approved
            };
            state.approved_at = Some(crate::plan::manager::timestamp_now());
            state.approval_summary = summary
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            recalculate_execution(&mut state);
            push_history(&mut state, "approve", summary.unwrap_or("approved plan"));
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        Ok(updated)
    }

    pub fn replace_state_with_history(
        &self,
        mut next_state: PlanState,
        action: &str,
        summary: impl Into<String>,
    ) -> PlanState {
        let updated = {
            let mut slot = self.state.write().expect("plan state poisoned");
            recalculate_execution(&mut next_state);
            push_history(&mut next_state, action, summary);
            *slot = Some(next_state.clone());
            next_state
        };
        self.persist_state();
        updated
    }

    fn persist_state(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        if let Some(state) = self.state() {
            let _ = persistence
                .session_store
                .save_plan_state(&persistence.session_id, state);
        }
    }
}

fn sync_status_from_draft(state: &mut PlanState) {
    let steps = state
        .draft
        .as_ref()
        .map(|draft| draft.steps.as_slice())
        .unwrap_or(&[]);
    state.status = if steps.is_empty() {
        PlanStatus::Drafting
    } else if steps
        .iter()
        .all(|step| step.status == PlanStepStatus::Completed)
    {
        PlanStatus::Completed
    } else if steps
        .iter()
        .any(|step| step.status == PlanStepStatus::InProgress)
    {
        PlanStatus::Executing
    } else {
        PlanStatus::Ready
    };
}

fn recalculate_execution(state: &mut PlanState) {
    let steps = state
        .draft
        .as_ref()
        .map(|draft| draft.steps.as_slice())
        .unwrap_or(&[]);
    let total_steps = steps.len();
    let completed_steps = steps
        .iter()
        .filter(|step| step.status == PlanStepStatus::Completed)
        .count();
    let active_step_id = steps
        .iter()
        .find(|step| step.status == PlanStepStatus::InProgress)
        .map(|step| step.id.clone());
    let progress_percent = if total_steps == 0 {
        0
    } else {
        ((completed_steps * 100) / total_steps) as u8
    };
    state.execution = Some(PlanExecutionState {
        active_step_id,
        completed_steps,
        total_steps,
        progress_percent,
        last_updated_at: Some(timestamp_now()),
    });
}

fn push_history(state: &mut PlanState, action: &str, summary: impl Into<String>) {
    state.history.push(PlanHistoryEntry {
        timestamp: timestamp_now(),
        action: action.to_string(),
        summary: summary.into(),
        status: state.status,
        draft: state.draft.clone(),
        execution: state.execution.clone(),
    });
}

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn summarize_text(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "(empty summary)".into()
    } else {
        format!("\"{trimmed}\"")
    }
}

fn timestamp_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

use std::sync::{Arc, RwLock};

use crate::history::session::{SessionId, SessionStore};
use crate::plan::types::{PlanDraft, PlanState, PlanStatus};

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
                }
            }
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        updated
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
            state.status = PlanStatus::Approved;
            state.approved_at = Some(crate::plan::manager::timestamp_now());
            state.approval_summary = summary
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            *slot = Some(state.clone());
            state
        };
        self.persist_state();
        Ok(updated)
    }

    fn persist_state(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        if let Some(state) = self.state() {
            persistence
                .session_store
                .save_plan_state(&persistence.session_id, state);
        }
    }
}

fn timestamp_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

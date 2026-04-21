use std::sync::{Arc, RwLock};

use crate::service::api::client::{ModelProviderClient, ModelProviderConfig};
use crate::state::app_state::{ActiveModelProfileSource, ActiveModelProviderSummary};

#[derive(Debug, Clone)]
pub struct ActiveModelRuntimeSnapshot {
    pub config: ModelProviderConfig,
    pub client: ModelProviderClient,
    pub active_profile_name: Option<String>,
    pub source: ActiveModelProfileSource,
    pub summary: ActiveModelProviderSummary,
}

#[derive(Clone)]
pub struct ActiveModelRuntime {
    inner: Arc<RwLock<ActiveModelRuntimeSnapshot>>,
}

impl std::fmt::Debug for ActiveModelRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveModelRuntime").finish_non_exhaustive()
    }
}

impl ActiveModelRuntime {
    pub fn new(snapshot: ActiveModelRuntimeSnapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub async fn snapshot(&self) -> ActiveModelRuntimeSnapshot {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn snapshot_blocking(&self) -> ActiveModelRuntimeSnapshot {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) async fn replace(&self, snapshot: ActiveModelRuntimeSnapshot) {
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
    }
}

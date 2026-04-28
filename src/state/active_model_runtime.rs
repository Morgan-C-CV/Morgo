use std::sync::{Arc, RwLock};

use crate::bootstrap::model_profiles::ResolvedModelProfile;
use crate::bootstrap::summarize_active_model_provider;
use crate::service::api::client::{ModelProviderClient, ModelProviderConfig};
use crate::service::observability::ServiceObservabilityTracker;
use crate::state::app_state::{ActiveModelProfileSource, ActiveModelProviderSummary};

#[derive(Debug, Clone)]
pub struct ActiveModelRuntimeSnapshot {
    pub config: ModelProviderConfig,
    pub client: ModelProviderClient,
    pub active_profile_name: Option<String>,
    pub source: ActiveModelProfileSource,
    pub summary: ActiveModelProviderSummary,
}

impl ActiveModelRuntimeSnapshot {
    pub fn from_resolved_profile(
        resolved: &ResolvedModelProfile,
        observability: ServiceObservabilityTracker,
    ) -> Self {
        let client = ModelProviderClient::from_config_with_observability(
            resolved.config.clone(),
            observability,
        );
        Self {
            config: resolved.config.clone(),
            client,
            active_profile_name: Some(resolved.name.clone()),
            source: ActiveModelProfileSource::ModelsToml,
            summary: summarize_active_model_provider(&resolved.config),
        }
    }
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

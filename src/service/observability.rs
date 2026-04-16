use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::core::events::ServiceFailureNotice;
use crate::service::compact::CompactPlanKind;

#[derive(Debug, Clone, Default)]
pub struct ServiceObservabilityTracker {
    inner: Arc<RwLock<ServiceObservabilityState>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceObservabilitySnapshot {
    pub by_failure_code: BTreeMap<String, usize>,
    pub retryable_count: usize,
    pub terminal_count: usize,
    pub by_provider_kind: BTreeMap<String, usize>,
    pub compact_recovery_hits: BTreeMap<String, usize>,
}

#[derive(Debug, Default)]
struct ServiceObservabilityState {
    by_failure_code: BTreeMap<String, usize>,
    retryable_count: usize,
    terminal_count: usize,
    by_provider_kind: BTreeMap<String, usize>,
    compact_recovery_hits: BTreeMap<String, usize>,
}

impl ServiceObservabilityTracker {
    pub fn record_service_failure(&self, notice: &ServiceFailureNotice) {
        let mut state = self
            .inner
            .write()
            .expect("service observability tracker poisoned");
        *state
            .by_failure_code
            .entry(notice.service_failure_code.as_str().to_string())
            .or_default() += 1;
        if notice.retryable {
            state.retryable_count += 1;
        } else {
            state.terminal_count += 1;
        }
        if let Some(provider_kind) = notice.provider_kind.as_ref() {
            *state
                .by_provider_kind
                .entry(provider_kind.clone())
                .or_default() += 1;
        }
    }

    pub fn record_compact_recovery_hit(&self, kind: &CompactPlanKind) {
        let Some(key) = compact_recovery_key(kind) else {
            return;
        };
        let mut state = self
            .inner
            .write()
            .expect("service observability tracker poisoned");
        *state.compact_recovery_hits.entry(key.into()).or_default() += 1;
    }

    pub fn snapshot(&self) -> ServiceObservabilitySnapshot {
        let state = self
            .inner
            .read()
            .expect("service observability tracker poisoned");
        ServiceObservabilitySnapshot {
            by_failure_code: state.by_failure_code.clone(),
            retryable_count: state.retryable_count,
            terminal_count: state.terminal_count,
            by_provider_kind: state.by_provider_kind.clone(),
            compact_recovery_hits: state.compact_recovery_hits.clone(),
        }
    }
}

fn compact_recovery_key(kind: &CompactPlanKind) -> Option<&'static str> {
    match kind {
        CompactPlanKind::ReactiveCompact => Some("reactive_compact"),
        CompactPlanKind::CollapseDrain => Some("collapse_drain"),
        CompactPlanKind::Exhausted | CompactPlanKind::AutoCompact => None,
    }
}

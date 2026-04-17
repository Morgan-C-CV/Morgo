use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::core::events::ServiceFailureNotice;
use crate::service::api::errors::ApiError;
use crate::service::compact::CompactPlanKind;

const MAX_RECENT_EVENTS: usize = 16;

#[derive(Debug, Clone, Default)]
pub struct ServiceObservabilityTracker {
    inner: Arc<RwLock<ServiceObservabilityState>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceObservabilitySnapshot {
    pub service_failures_total: usize,
    pub by_failure_code: BTreeMap<String, usize>,
    pub retryable_count: usize,
    pub terminal_count: usize,
    pub by_provider_kind: BTreeMap<String, usize>,
    pub compact_recovery_hits: BTreeMap<String, usize>,
    pub api_errors_by_kind: BTreeMap<String, usize>,
    pub api_errors_by_provider: BTreeMap<String, usize>,
    pub api_errors_by_status: BTreeMap<String, usize>,
    pub mcp_failures_by_kind: BTreeMap<String, usize>,
    pub mcp_failures_by_server: BTreeMap<String, usize>,
    pub recent_events: Vec<ServiceObservabilityEventRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceObservabilityEventRecord {
    pub category: &'static str,
    pub key: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObservabilityEvent {
    ServiceFailure {
        failure_code: String,
        provider_kind: Option<String>,
        retryable: bool,
    },
    CompactRecovery {
        kind: String,
    },
    ApiClientError {
        provider_id: String,
        kind: String,
        status_code: Option<u16>,
    },
    McpServerFailure {
        server: String,
        kind: String,
    },
}

#[derive(Debug, Default)]
struct ServiceObservabilityState {
    service_failures_total: usize,
    by_failure_code: BTreeMap<String, usize>,
    retryable_count: usize,
    terminal_count: usize,
    by_provider_kind: BTreeMap<String, usize>,
    compact_recovery_hits: BTreeMap<String, usize>,
    api_errors_by_kind: BTreeMap<String, usize>,
    api_errors_by_provider: BTreeMap<String, usize>,
    api_errors_by_status: BTreeMap<String, usize>,
    mcp_failures_by_kind: BTreeMap<String, usize>,
    mcp_failures_by_server: BTreeMap<String, usize>,
    recent_events: Vec<ServiceObservabilityEventRecord>,
}

impl ServiceObservabilityTracker {
    pub fn record_service_failure(&self, notice: &ServiceFailureNotice) {
        self.record_event(ObservabilityEvent::ServiceFailure {
            failure_code: notice.service_failure_code.as_str().to_string(),
            provider_kind: notice.provider_kind.clone(),
            retryable: notice.retryable,
        });
    }

    pub fn record_compact_recovery_hit(&self, kind: &CompactPlanKind) {
        let Some(key) = compact_recovery_key(kind) else {
            return;
        };
        self.record_event(ObservabilityEvent::CompactRecovery { kind: key.into() });
    }

    pub fn record_api_client_error(&self, provider_id: &str, error: &ApiError) {
        self.record_event(ObservabilityEvent::ApiClientError {
            provider_id: provider_id.to_string(),
            kind: error.kind_label().to_string(),
            status_code: match error.kind {
                crate::service::api::errors::ApiErrorKind::HttpStatus(status) => Some(status),
                _ => None,
            },
        });
    }

    pub fn record_mcp_server_failure(&self, server: &str, kind: &str) {
        self.record_event(ObservabilityEvent::McpServerFailure {
            server: server.to_string(),
            kind: kind.to_string(),
        });
    }

    pub fn snapshot(&self) -> ServiceObservabilitySnapshot {
        let state = self
            .inner
            .read()
            .expect("service observability tracker poisoned");
        ServiceObservabilitySnapshot {
            service_failures_total: state.service_failures_total,
            by_failure_code: state.by_failure_code.clone(),
            retryable_count: state.retryable_count,
            terminal_count: state.terminal_count,
            by_provider_kind: state.by_provider_kind.clone(),
            compact_recovery_hits: state.compact_recovery_hits.clone(),
            api_errors_by_kind: state.api_errors_by_kind.clone(),
            api_errors_by_provider: state.api_errors_by_provider.clone(),
            api_errors_by_status: state.api_errors_by_status.clone(),
            mcp_failures_by_kind: state.mcp_failures_by_kind.clone(),
            mcp_failures_by_server: state.mcp_failures_by_server.clone(),
            recent_events: state.recent_events.clone(),
        }
    }

    fn record_event(&self, event: ObservabilityEvent) {
        let mut state = self
            .inner
            .write()
            .expect("service observability tracker poisoned");
        match &event {
            ObservabilityEvent::ServiceFailure {
                failure_code,
                provider_kind,
                retryable,
            } => {
                state.service_failures_total += 1;
                *state.by_failure_code.entry(failure_code.clone()).or_default() += 1;
                if *retryable {
                    state.retryable_count += 1;
                } else {
                    state.terminal_count += 1;
                }
                if let Some(provider_kind) = provider_kind {
                    *state
                        .by_provider_kind
                        .entry(provider_kind.clone())
                        .or_default() += 1;
                }
            }
            ObservabilityEvent::CompactRecovery { kind } => {
                *state.compact_recovery_hits.entry(kind.clone()).or_default() += 1;
            }
            ObservabilityEvent::ApiClientError {
                provider_id,
                kind,
                status_code,
            } => {
                *state.api_errors_by_kind.entry(kind.clone()).or_default() += 1;
                *state
                    .api_errors_by_provider
                    .entry(provider_id.clone())
                    .or_default() += 1;
                if let Some(status_code) = status_code {
                    *state
                        .api_errors_by_status
                        .entry(status_code.to_string())
                        .or_default() += 1;
                }
            }
            ObservabilityEvent::McpServerFailure { server, kind } => {
                *state.mcp_failures_by_kind.entry(kind.clone()).or_default() += 1;
                *state.mcp_failures_by_server.entry(server.clone()).or_default() += 1;
            }
        }
        push_recent_event(&mut state.recent_events, event);
    }
}

fn push_recent_event(events: &mut Vec<ServiceObservabilityEventRecord>, event: ObservabilityEvent) {
    let record = match event {
        ObservabilityEvent::ServiceFailure {
            failure_code,
            provider_kind,
            retryable,
        } => ServiceObservabilityEventRecord {
            category: "service_failure",
            key: failure_code.clone(),
            detail: format!(
                "provider={} retryable={retryable}",
                provider_kind.unwrap_or_else(|| "unknown".into())
            ),
        },
        ObservabilityEvent::CompactRecovery { kind } => ServiceObservabilityEventRecord {
            category: "compact_recovery",
            key: kind,
            detail: "query loop recovery path recorded".into(),
        },
        ObservabilityEvent::ApiClientError {
            provider_id,
            kind,
            status_code,
        } => ServiceObservabilityEventRecord {
            category: "api_client_error",
            key: kind,
            detail: format!(
                "provider={provider_id} status={}",
                status_code
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "none".into())
            ),
        },
        ObservabilityEvent::McpServerFailure { server, kind } => ServiceObservabilityEventRecord {
            category: "mcp_server_failure",
            key: kind,
            detail: format!("server={server}"),
        },
    };
    events.push(record);
    if events.len() > MAX_RECENT_EVENTS {
        let overflow = events.len() - MAX_RECENT_EVENTS;
        events.drain(0..overflow);
    }
}

fn compact_recovery_key(kind: &CompactPlanKind) -> Option<&'static str> {
    match kind {
        CompactPlanKind::ReactiveCompact => Some("reactive_compact"),
        CompactPlanKind::CollapseDrain => Some("collapse_drain"),
        CompactPlanKind::Exhausted | CompactPlanKind::AutoCompact => None,
    }
}

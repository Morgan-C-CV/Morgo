use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::events::ServiceFailureNotice;
use crate::service::api::errors::ApiError;
use crate::service::compact::CompactPlanKind;
use serde::{Deserialize, Serialize};

const MAX_RECENT_EVENTS: usize = 16;

#[derive(Debug, Clone, Default)]
pub struct ServiceObservabilityTracker {
    inner: Arc<RwLock<ServiceObservabilityState>>,
    api_call_log_sink: Arc<Mutex<Option<ApiCallLogSink>>>,
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
    pub runtime_lifecycle_failures_by_phase: BTreeMap<String, usize>,
    pub runtime_lifecycle_failures_by_reason: BTreeMap<String, usize>,
    pub recent_events: Vec<ServiceObservabilityEventRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceObservabilityEventRecord {
    pub category: &'static str,
    pub key: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiCallRecord {
    pub timestamp_ms: u64,
    pub provider_id: String,
    pub model: String,
    pub prompt_chars: usize,
    pub response_chars: usize,
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub cache_creation_input_tokens: usize,
    pub cache_read_input_tokens: usize,
    pub stop_reason: Option<String>,
    pub response_text: String,
}

#[derive(Debug)]
struct ApiCallLogSink {
    writer: BufWriter<File>,
}

pub trait ServiceObservabilityExportSink {
    fn record_scalar(&mut self, name: &'static str, value: usize);
    fn record_bucket_entry(&mut self, group: &'static str, key: &str, value: usize);
    fn record_recent_event(&mut self, event: &ServiceObservabilityEventRecord);
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
    RuntimeLifecycleFailure {
        phase: String,
        reason: String,
        session_id: String,
        attempt: usize,
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
    runtime_lifecycle_failures_by_phase: BTreeMap<String, usize>,
    runtime_lifecycle_failures_by_reason: BTreeMap<String, usize>,
    recent_events: Vec<ServiceObservabilityEventRecord>,
}

impl ServiceObservabilitySnapshot {
    pub fn export_to(&self, sink: &mut impl ServiceObservabilityExportSink) {
        sink.record_scalar("service_failures_total", self.service_failures_total);
        sink.record_scalar("retryable_count", self.retryable_count);
        sink.record_scalar("terminal_count", self.terminal_count);

        export_bucket_group(sink, "by_failure_code", &self.by_failure_code);
        export_bucket_group(sink, "by_provider_kind", &self.by_provider_kind);
        export_bucket_group(sink, "compact_recovery_hits", &self.compact_recovery_hits);
        export_bucket_group(sink, "api_errors_by_kind", &self.api_errors_by_kind);
        export_bucket_group(sink, "api_errors_by_provider", &self.api_errors_by_provider);
        export_bucket_group(sink, "api_errors_by_status", &self.api_errors_by_status);
        export_bucket_group(sink, "mcp_failures_by_kind", &self.mcp_failures_by_kind);
        export_bucket_group(sink, "mcp_failures_by_server", &self.mcp_failures_by_server);
        export_bucket_group(
            sink,
            "runtime_lifecycle_failures_by_phase",
            &self.runtime_lifecycle_failures_by_phase,
        );
        export_bucket_group(
            sink,
            "runtime_lifecycle_failures_by_reason",
            &self.runtime_lifecycle_failures_by_reason,
        );

        for event in &self.recent_events {
            sink.record_recent_event(event);
        }
    }
}

impl ServiceObservabilityTracker {
    pub fn configure_api_call_log_path(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        *self
            .api_call_log_sink
            .lock()
            .expect("service observability api call log sink poisoned") = Some(ApiCallLogSink {
            writer: BufWriter::new(file),
        });
        Ok(())
    }

    pub fn record_api_call(&self, record: ApiCallRecord) {
        let mut guard = self
            .api_call_log_sink
            .lock()
            .expect("service observability api call log sink poisoned");
        let Some(sink) = guard.as_mut() else {
            return;
        };
        if let Ok(line) = serde_json::to_string(&record) {
            let _ = writeln!(sink.writer, "{line}");
            let _ = sink.writer.flush();
        }
    }

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

    pub fn record_runtime_lifecycle_failure(
        &self,
        phase: &str,
        reason: &str,
        session_id: &str,
        attempt: usize,
    ) {
        self.record_event(ObservabilityEvent::RuntimeLifecycleFailure {
            phase: phase.to_string(),
            reason: reason.to_string(),
            session_id: session_id.to_string(),
            attempt,
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
            runtime_lifecycle_failures_by_phase: state.runtime_lifecycle_failures_by_phase.clone(),
            runtime_lifecycle_failures_by_reason: state
                .runtime_lifecycle_failures_by_reason
                .clone(),
            recent_events: state.recent_events.clone(),
        }
    }

    pub fn export_snapshot_to(&self, sink: &mut impl ServiceObservabilityExportSink) {
        self.snapshot().export_to(sink);
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
                *state
                    .by_failure_code
                    .entry(failure_code.clone())
                    .or_default() += 1;
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
                *state
                    .mcp_failures_by_server
                    .entry(server.clone())
                    .or_default() += 1;
            }
            ObservabilityEvent::RuntimeLifecycleFailure { phase, reason, .. } => {
                *state
                    .runtime_lifecycle_failures_by_phase
                    .entry(phase.clone())
                    .or_default() += 1;
                *state
                    .runtime_lifecycle_failures_by_reason
                    .entry(reason.clone())
                    .or_default() += 1;
            }
        }
        push_recent_event(&mut state.recent_events, event);
    }
}

impl ApiCallRecord {
    pub fn now(
        provider_id: impl Into<String>,
        model: impl Into<String>,
        prompt_chars: usize,
        response_chars: usize,
        input_tokens: usize,
        output_tokens: usize,
        cache_creation_input_tokens: usize,
        cache_read_input_tokens: usize,
        stop_reason: Option<String>,
        response_text: impl Into<String>,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default();
        Self {
            timestamp_ms,
            provider_id: provider_id.into(),
            model: model.into(),
            prompt_chars,
            response_chars,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            stop_reason,
            response_text: response_text.into(),
        }
    }
}

fn export_bucket_group(
    sink: &mut impl ServiceObservabilityExportSink,
    group: &'static str,
    entries: &BTreeMap<String, usize>,
) {
    for (key, value) in entries {
        sink.record_bucket_entry(group, key, *value);
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
        ObservabilityEvent::RuntimeLifecycleFailure {
            phase,
            reason,
            session_id,
            attempt,
        } => ServiceObservabilityEventRecord {
            category: "runtime_lifecycle_failure",
            key: phase,
            detail: format!("reason={reason} session={session_id} attempt={attempt}"),
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

#[cfg(test)]
mod tests {
    use super::{
        ApiCallRecord, ServiceObservabilityEventRecord, ServiceObservabilityExportSink,
        ServiceObservabilitySnapshot, ServiceObservabilityTracker,
    };
    use crate::core::events::{ServiceFailureCode, ServiceFailureNotice};
    use crate::service::api::errors::ApiError;
    use crate::service::compact::CompactPlanKind;

    #[derive(Debug, Default, PartialEq, Eq)]
    struct CapturedExport {
        scalars: Vec<(&'static str, usize)>,
        buckets: Vec<(&'static str, String, usize)>,
        recent_events: Vec<ServiceObservabilityEventRecord>,
    }

    impl ServiceObservabilityExportSink for CapturedExport {
        fn record_scalar(&mut self, name: &'static str, value: usize) {
            self.scalars.push((name, value));
        }

        fn record_bucket_entry(&mut self, group: &'static str, key: &str, value: usize) {
            self.buckets.push((group, key.to_string(), value));
        }

        fn record_recent_event(&mut self, event: &ServiceObservabilityEventRecord) {
            self.recent_events.push(event.clone());
        }
    }

    #[test]
    fn snapshot_exports_stable_shape_in_order() {
        let snapshot = ServiceObservabilitySnapshot {
            service_failures_total: 2,
            by_failure_code: [
                ("api_provider_http_5xx".to_string(), 1),
                ("api_stream_terminal".to_string(), 1),
            ]
            .into_iter()
            .collect(),
            retryable_count: 1,
            terminal_count: 1,
            by_provider_kind: [("anthropic".to_string(), 1)].into_iter().collect(),
            compact_recovery_hits: [("reactive_compact".to_string(), 1)].into_iter().collect(),
            api_errors_by_kind: [("http_status".to_string(), 1)].into_iter().collect(),
            api_errors_by_provider: [("anthropic".to_string(), 1)].into_iter().collect(),
            api_errors_by_status: [("503".to_string(), 1)].into_iter().collect(),
            mcp_failures_by_kind: [("list_tools".to_string(), 1)].into_iter().collect(),
            mcp_failures_by_server: [("filesystem".to_string(), 1)].into_iter().collect(),
            runtime_lifecycle_failures_by_phase: [("shutdown.persist_before".to_string(), 1)]
                .into_iter()
                .collect(),
            runtime_lifecycle_failures_by_reason: [(
                "persist_before_shutdown:missing_session_store".to_string(),
                1,
            )]
            .into_iter()
            .collect(),
            recent_events: vec![
                ServiceObservabilityEventRecord {
                    category: "service_failure",
                    key: "api_provider_http_5xx".into(),
                    detail: "provider=anthropic retryable=true".into(),
                },
                ServiceObservabilityEventRecord {
                    category: "mcp_server_failure",
                    key: "list_tools".into(),
                    detail: "server=filesystem".into(),
                },
            ],
        };

        let mut sink = CapturedExport::default();
        snapshot.export_to(&mut sink);

        assert_eq!(
            sink.scalars,
            vec![
                ("service_failures_total", 2),
                ("retryable_count", 1),
                ("terminal_count", 1),
            ]
        );
        assert_eq!(
            sink.buckets,
            vec![
                ("by_failure_code", "api_provider_http_5xx".into(), 1),
                ("by_failure_code", "api_stream_terminal".into(), 1),
                ("by_provider_kind", "anthropic".into(), 1),
                ("compact_recovery_hits", "reactive_compact".into(), 1),
                ("api_errors_by_kind", "http_status".into(), 1),
                ("api_errors_by_provider", "anthropic".into(), 1),
                ("api_errors_by_status", "503".into(), 1),
                ("mcp_failures_by_kind", "list_tools".into(), 1),
                ("mcp_failures_by_server", "filesystem".into(), 1),
                (
                    "runtime_lifecycle_failures_by_phase",
                    "shutdown.persist_before".into(),
                    1,
                ),
                (
                    "runtime_lifecycle_failures_by_reason",
                    "persist_before_shutdown:missing_session_store".into(),
                    1,
                ),
            ]
        );
        assert_eq!(sink.recent_events, snapshot.recent_events);
    }

    #[test]
    fn tracker_export_matches_runtime_api_and_mcp_snapshot_counters() {
        let tracker = ServiceObservabilityTracker::default();
        tracker.record_service_failure(&ServiceFailureNotice {
            service_failure_code: ServiceFailureCode::ApiProviderHttp5xx,
            provider_kind: Some("anthropic".into()),
            status_code: Some(503),
            retryable: true,
            surface_visible: true,
        });
        tracker.record_compact_recovery_hit(&CompactPlanKind::ReactiveCompact);
        tracker.record_api_client_error(
            "anthropic",
            &ApiError::http_status(503, "provider request failed with status 503"),
        );
        tracker.record_mcp_server_failure("filesystem", "list_tools");
        tracker.record_runtime_lifecycle_failure(
            "shutdown.persist_before",
            "persist_before_shutdown:missing_session_store",
            "session-123",
            1,
        );

        let snapshot = tracker.snapshot();
        let mut sink = CapturedExport::default();
        tracker.export_snapshot_to(&mut sink);

        assert_eq!(
            sink.scalars[0],
            ("service_failures_total", snapshot.service_failures_total)
        );
        assert!(
            sink.buckets.contains(&(
                "by_failure_code",
                "api_provider_http_5xx".into(),
                *snapshot
                    .by_failure_code
                    .get("api_provider_http_5xx")
                    .expect("runtime failure code should export")
            ))
        );
        assert!(
            sink.buckets.contains(&(
                "compact_recovery_hits",
                "reactive_compact".into(),
                *snapshot
                    .compact_recovery_hits
                    .get("reactive_compact")
                    .expect("compact recovery bucket should export")
            ))
        );
        assert!(
            sink.buckets.contains(&(
                "api_errors_by_kind",
                "http_status".into(),
                *snapshot
                    .api_errors_by_kind
                    .get("http_status")
                    .expect("api error kind should export")
            ))
        );
        assert!(
            sink.buckets.contains(&(
                "mcp_failures_by_server",
                "filesystem".into(),
                *snapshot
                    .mcp_failures_by_server
                    .get("filesystem")
                    .expect("mcp server failure should export")
            ))
        );
        assert!(
            sink.buckets.contains(&(
                "runtime_lifecycle_failures_by_phase",
                "shutdown.persist_before".into(),
                *snapshot
                    .runtime_lifecycle_failures_by_phase
                    .get("shutdown.persist_before")
                    .expect("runtime lifecycle phase should export")
            ))
        );
        assert!(
            sink.buckets.contains(&(
                "runtime_lifecycle_failures_by_reason",
                "persist_before_shutdown:missing_session_store".into(),
                *snapshot
                    .runtime_lifecycle_failures_by_reason
                    .get("persist_before_shutdown:missing_session_store")
                    .expect("runtime lifecycle reason should export")
            ))
        );
        assert_eq!(sink.recent_events, snapshot.recent_events);
    }

    #[test]
    fn api_call_log_sink_persists_jsonl_records() {
        let tracker = ServiceObservabilityTracker::default();
        let path = std::env::temp_dir().join(format!(
            "service-observability-api-call-{}.jsonl",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        ));
        tracker
            .configure_api_call_log_path(&path)
            .expect("log path should be configurable");
        tracker.record_api_call(ApiCallRecord::now(
            "openai",
            "gpt-test",
            1200,
            42,
            1500,
            10,
            0,
            1024,
            Some("endturn".into()),
            "hello world",
        ));

        let text = std::fs::read_to_string(&path).expect("log file should exist");
        assert!(text.contains("\"provider_id\":\"openai\""));
        assert!(text.contains("\"model\":\"gpt-test\""));
        assert!(text.contains("\"response_text\":\"hello world\""));

        let _ = std::fs::remove_file(path);
    }
}

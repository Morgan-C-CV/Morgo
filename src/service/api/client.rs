use std::sync::{Arc, RwLock};
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};

use crate::core::message::Message;
use crate::service::api::errors::ApiError;
use crate::service::api::retry::RetryPolicy;
use crate::service::api::streaming::{
    ProviderFailureDisposition, StopReason, StreamError, StreamEvent, UsageEvent,
};
use crate::service::observability::ServiceObservabilityTracker;

#[derive(Debug, Clone, PartialEq)]
pub struct ModelPricing {
    pub input_per_million_usd: f64,
    pub output_per_million_usd: f64,
    pub cache_write_per_million_usd: f64,
    pub cache_read_per_million_usd: f64,
}

impl Default for ModelPricing {
    fn default() -> Self {
        Self {
            input_per_million_usd: 3.0,
            output_per_million_usd: 15.0,
            cache_write_per_million_usd: 3.75,
            cache_read_per_million_usd: 0.3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTimeout {
    pub request_timeout_ms: u64,
}

impl Default for ProviderTimeout {
    fn default() -> Self {
        Self {
            request_timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelProviderConfig {
    pub provider_id: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub model_id: String,
    pub timeout: ProviderTimeout,
    pub retry_policy: RetryPolicy,
    pub pricing: ModelPricing,
}

impl Default for ModelProviderConfig {
    fn default() -> Self {
        Self {
            provider_id: "default-provider".into(),
            base_url: "http://localhost".into(),
            api_key: None,
            model_id: "default-model".into(),
            timeout: ProviderTimeout::default(),
            retry_policy: RetryPolicy::default(),
            pricing: ModelPricing::default(),
        }
    }
}

#[derive(Debug, Clone)]
enum ProviderTransport {
    Scripted {
        turns: Arc<RwLock<Vec<Vec<StreamEvent>>>>,
    },
    Production {
        config: ModelProviderConfig,
        client: reqwest::Client,
        observability: ServiceObservabilityTracker,
    },
}

#[derive(Debug, Clone)]
pub struct ModelProviderClient {
    transport: ProviderTransport,
}

impl Default for ModelProviderClient {
    fn default() -> Self {
        Self::from_config(ModelProviderConfig::default())
    }
}

impl ModelProviderClient {
    pub fn from_config(config: ModelProviderConfig) -> Self {
        Self::from_config_with_observability(config, ServiceObservabilityTracker::default())
    }

    pub fn from_config_with_observability(
        config: ModelProviderConfig,
        observability: ServiceObservabilityTracker,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout.request_timeout_ms))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            transport: ProviderTransport::Production {
                config,
                client,
                observability,
            },
        }
    }

    pub fn with_scripted_events(scripted_events: Vec<StreamEvent>) -> Self {
        Self::with_scripted_turns(vec![scripted_events])
    }

    pub fn with_scripted_turns(scripted_turns: Vec<Vec<StreamEvent>>) -> Self {
        Self {
            transport: ProviderTransport::Scripted {
                turns: Arc::new(RwLock::new(scripted_turns)),
            },
        }
    }

    pub fn provider_config(&self) -> ModelProviderConfig {
        match &self.transport {
            ProviderTransport::Scripted { .. } => ModelProviderConfig::default(),
            ProviderTransport::Production { config, .. } => config.clone(),
        }
    }

    pub fn observability_tracker(&self) -> ServiceObservabilityTracker {
        match &self.transport {
            ProviderTransport::Scripted { .. } => ServiceObservabilityTracker::default(),
            ProviderTransport::Production { observability, .. } => observability.clone(),
        }
    }

    pub fn is_scripted(&self) -> bool {
        matches!(self.transport, ProviderTransport::Scripted { .. })
    }

    pub async fn stream_message(&self, input: &Message) -> Vec<StreamEvent> {
        match &self.transport {
            ProviderTransport::Scripted { turns } => {
                let mut turns = turns.write().expect("scripted turns poisoned");
                if turns.is_empty() {
                    Vec::new()
                } else {
                    turns.remove(0)
                }
            }
            ProviderTransport::Production {
                config,
                client,
                observability,
            } => {
                if input.content.trim().is_empty() {
                    return Vec::new();
                }
                match self
                    .stream_message_with_retry(config, client, observability, input)
                    .await
                {
                    Ok(events) => events,
                    Err(error) => vec![StreamEvent::Error(
                        error.to_stream_error(&config.provider_id),
                    )],
                }
            }
        }
    }

    async fn stream_message_with_retry(
        &self,
        config: &ModelProviderConfig,
        client: &reqwest::Client,
        observability: &ServiceObservabilityTracker,
        input: &Message,
    ) -> Result<Vec<StreamEvent>, ApiError> {
        let mut attempt = 0;
        loop {
            match self.stream_message_once(config, client, input).await {
                Ok(events) => return Ok(events),
                Err(error) => {
                    observability.record_api_client_error(&config.provider_id, &error);
                    if config.retry_policy.should_retry(attempt, &error, false) {
                        sleep(config.retry_policy.backoff_for_attempt(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn stream_message_once(
        &self,
        config: &ModelProviderConfig,
        client: &reqwest::Client,
        input: &Message,
    ) -> Result<Vec<StreamEvent>, ApiError> {
        let url = build_messages_url_for_provider(&config.provider_id, &config.base_url)?;
        let payload = build_request_payload_for_provider(config, input)?;
        let mut request = client
            .post(url)
            .header(ACCEPT, "text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .json(&payload);
        if let Some(api_key) = config
            .api_key
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
        }

        let response = timeout(
            Duration::from_millis(config.timeout.request_timeout_ms),
            request.send(),
        )
        .await
        .map_err(|_| ApiError::timeout("provider request timed out"))?
        .map_err(classify_request_transport_error)?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(ApiError::http_status(
                status.as_u16(),
                format!(
                    "provider request failed with status {}: {body}",
                    status.as_u16()
                ),
            ));
        }
        validate_streaming_response_headers(response.headers(), status)?;

        let body = timeout(
            Duration::from_millis(config.timeout.request_timeout_ms),
            response.text(),
        )
        .await
        .map_err(|_| ApiError::timeout("provider stream timed out while reading response"))?
        .map_err(classify_response_body_error)?;

        if body.trim().is_empty() {
            return Err(ApiError::empty_body("provider returned empty response body"));
        }

        parse_stream_response_for_provider(&config.provider_id, &body, &config.model_id)
    }
}

fn classify_request_transport_error(error: reqwest::Error) -> ApiError {
    let message = format!("provider request failed: {error}");
    if error.is_timeout() {
        ApiError::timeout(message)
    } else if is_connection_reset_error(&error) {
        ApiError::connection_reset(message)
    } else {
        ApiError::transport(message)
    }
}

fn classify_response_body_error(error: reqwest::Error) -> ApiError {
    let message = format!("failed reading provider stream: {error}");
    if error.is_timeout() {
        ApiError::timeout(message)
    } else if is_connection_reset_error(&error) {
        ApiError::connection_reset(message)
    } else if error.is_body() {
        ApiError::sse_protocol_with_disposition(message, ProviderFailureDisposition::StreamTerminal)
    } else {
        ApiError::transport(message)
    }
}

fn is_connection_reset_error(error: &reqwest::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("connection reset")
        || text.contains("broken pipe")
        || text.contains("unexpected eof")
        || text.contains("early eof")
        || text.contains("connection closed before message completed")
}

fn validate_streaming_response_headers(
    headers: &reqwest::header::HeaderMap,
    status: StatusCode,
) -> Result<(), ApiError> {
    let Some(content_type) = headers.get(CONTENT_TYPE) else {
        return Err(ApiError::bad_content_type(format!(
            "provider returned response without content-type header for status {}",
            status.as_u16()
        )));
    };
    let content_type = content_type.to_str().map_err(|_| {
        ApiError::bad_content_type("provider returned non-utf8 content-type header")
    })?;
    if !content_type
        .to_ascii_lowercase()
        .starts_with("text/event-stream")
    {
        return Err(ApiError::bad_content_type(format!(
            "provider returned unsupported content-type: {content_type}"
        )));
    }
    Ok(())
}

fn build_messages_url_for_provider(provider_id: &str, base_url: &str) -> Result<String, ApiError> {
    match normalized_provider_id(provider_id) {
        "anthropic" | "default-provider" => {
            Ok(format!("{}/v1/messages", base_url.trim_end_matches('/')))
        }
        _ => Err(ApiError::invalid_response(format!(
            "unsupported provider id: {provider_id}"
        ))),
    }
}

fn build_request_payload_for_provider(
    config: &ModelProviderConfig,
    input: &Message,
) -> Result<Value, ApiError> {
    match normalized_provider_id(&config.provider_id) {
        "anthropic" | "default-provider" => Ok(json!({
            "model": config.model_id,
            "messages": [
                {
                    "role": "user",
                    "content": input.content,
                }
            ],
            "stream": true,
        })),
        _ => Err(ApiError::invalid_response(format!(
            "unsupported provider id: {}",
            config.provider_id
        ))),
    }
}

fn parse_stream_response_for_provider(
    provider_id: &str,
    body: &str,
    default_model: &str,
) -> Result<Vec<StreamEvent>, ApiError> {
    match normalized_provider_id(provider_id) {
        "anthropic" | "default-provider" => {
            parse_anthropic_sse_response(provider_id, body, default_model)
        }
        _ => Err(ApiError::invalid_response(format!(
            "unsupported provider id: {provider_id}"
        ))),
    }
}

fn normalized_provider_id(provider_id: &str) -> &str {
    let trimmed = provider_id.trim();
    if trimmed.is_empty() {
        "anthropic"
    } else {
        trimmed
    }
}

pub fn parse_anthropic_sse_response(
    provider_id: &str,
    body: &str,
    default_model: &str,
) -> Result<Vec<StreamEvent>, ApiError> {
    if body.trim().is_empty() {
        return Err(ApiError::empty_body("provider returned empty response body"));
    }

    let mut events = Vec::new();
    let mut parser = ProviderStreamParser::new(provider_id, default_model);
    let normalized = body.replace("\r\n", "\n");
    let complete_body = normalized.ends_with("\n\n");
    let frames = if complete_body {
        normalized.split("\n\n").collect::<Vec<_>>()
    } else {
        normalized
            .split_terminator("\n\n")
            .collect::<Vec<_>>()
    };

    for frame in frames.into_iter().filter(|frame| !frame.trim().is_empty()) {
        let payload = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
        if payload.is_empty() || payload == "[DONE]" {
            if !complete_body {
                return Err(ApiError::sse_protocol_with_disposition(
                    "provider returned truncated SSE frame",
                    if parser.saw_message_start {
                        ProviderFailureDisposition::StreamTerminal
                    } else {
                        ProviderFailureDisposition::PreStreamTerminal
                    },
                ));
            }
            continue;
        }
        let json: Value = serde_json::from_str(&payload).map_err(|error| {
            ApiError::sse_protocol_with_disposition(
                format!("invalid SSE JSON payload: {error}"),
                if parser.saw_message_start {
                    ProviderFailureDisposition::StreamTerminal
                } else {
                    ProviderFailureDisposition::PreStreamTerminal
                },
            )
        })?;
        parser.map_provider_event(&json, &mut events)?;
    }

    if !complete_body {
        return Err(ApiError::sse_protocol_with_disposition(
            "provider returned truncated SSE frame",
            if parser.saw_message_start {
                ProviderFailureDisposition::StreamTerminal
            } else {
                ProviderFailureDisposition::PreStreamTerminal
            },
        ));
    }

    parser.finish(&mut events)?;
    Ok(events)
}

#[derive(Debug, Default, Clone)]
struct PendingToolUseBlock {
    tool_name: Option<String>,
    input: Option<Value>,
    partial_json: String,
    emitted: bool,
}

struct ProviderStreamParser<'a> {
    provider_id: &'a str,
    default_model: &'a str,
    saw_message_start: bool,
    pending_tool_use: Option<PendingToolUseBlock>,
}

impl<'a> ProviderStreamParser<'a> {
    fn new(provider_id: &'a str, default_model: &'a str) -> Self {
        Self {
            provider_id,
            default_model,
            saw_message_start: false,
            pending_tool_use: None,
        }
    }

    fn map_provider_event(
        &mut self,
        payload: &Value,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        let event_type = payload
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| self.protocol_error("provider event missing type"))?;

        match event_type {
            "message_start" => {
                self.ensure_message_start(output);
                if let Some(usage) = payload
                    .get("message")
                    .and_then(|message| message.get("usage"))
                {
                    output.push(StreamEvent::Usage(parse_usage(
                        usage,
                        payload_model(payload).unwrap_or(self.default_model),
                    )));
                }
            }
            "content_block_start" => {
                self.ensure_message_start(output);
                let block = payload
                    .get("content_block")
                    .ok_or_else(|| self.protocol_error("content_block_start missing content_block"))?;
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            output.push(StreamEvent::TextDelta(text.to_string()));
                        }
                    }
                    Some("tool_use") => {
                        let tool_name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .ok_or_else(|| self.protocol_error("tool_use content block missing name"))?;
                        let mut pending = PendingToolUseBlock {
                            tool_name: Some(tool_name.to_string()),
                            input: block.get("input").cloned(),
                            partial_json: String::new(),
                            emitted: false,
                        };
                        Self::emit_pending_tool_use_if_ready(
                            self.saw_message_start,
                            &mut pending,
                            output,
                        )?;
                        self.pending_tool_use = Some(pending);
                    }
                    _ => {}
                }
            }
            "content_block_delta" => {
                if let Some(text) = payload
                    .get("delta")
                    .and_then(|delta| delta.get("text"))
                    .and_then(Value::as_str)
                {
                    output.push(StreamEvent::TextDelta(text.to_string()));
                }
                if let Some(pending) = self.pending_tool_use.as_mut() {
                    if let Some(partial_json) = payload
                        .get("delta")
                        .and_then(|delta| delta.get("partial_json"))
                        .and_then(Value::as_str)
                    {
                        pending.partial_json.push_str(partial_json);
                        if let Ok(value) = serde_json::from_str::<Value>(&pending.partial_json) {
                            pending.input = Some(value);
                        }
                    }
                    Self::emit_pending_tool_use_if_ready(self.saw_message_start, pending, output)?;
                }
            }
            "content_block_stop" => {
                self.finalize_pending_tool_use(output)?;
            }
            "message_delta" => {
                if let Some(usage) = payload.get("usage") {
                    output.push(StreamEvent::Usage(parse_usage(
                        usage,
                        payload_model(payload).unwrap_or(self.default_model),
                    )));
                }
                if let Some(stop_reason) = payload
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.finalize_pending_tool_use(output)?;
                    output.push(StreamEvent::MessageStop {
                        stop_reason: map_stop_reason(stop_reason),
                    });
                }
            }
            "message_stop" => {
                self.finalize_pending_tool_use(output)?;
                if !output
                    .iter()
                    .any(|event| matches!(event, StreamEvent::MessageStop { .. }))
                {
                    output.push(StreamEvent::MessageStop {
                        stop_reason: StopReason::EndTurn,
                    });
                }
            }
            "error" => {
                let message = payload
                    .get("error")
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("provider stream error")
                    .to_string();
                let raw_kind = payload
                    .get("error")
                    .and_then(|error| error.get("type"))
                    .and_then(Value::as_str);
                let (kind, disposition, retryable) =
                    classify_stream_error(self.provider_id, raw_kind, None);
                output.push(StreamEvent::Error(StreamError {
                    provider_id: normalized_provider_id(self.provider_id).to_string(),
                    kind,
                    message,
                    retryable,
                    disposition,
                    status_code: None,
                }));
            }
            _ => {}
        }

        Ok(())
    }

    fn finish(&mut self, output: &mut Vec<StreamEvent>) -> Result<(), ApiError> {
        self.finalize_pending_tool_use(output)
    }

    fn ensure_message_start(&mut self, output: &mut Vec<StreamEvent>) {
        if !self.saw_message_start {
            output.push(StreamEvent::MessageStart);
            self.saw_message_start = true;
        }
    }

    fn finalize_pending_tool_use(&mut self, output: &mut Vec<StreamEvent>) -> Result<(), ApiError> {
        if let Some(mut pending) = self.pending_tool_use.take() {
            Self::emit_pending_tool_use_if_ready(self.saw_message_start, &mut pending, output)?;
            if !pending.emitted {
                return Err(self.protocol_error("tool_use block ended without complete input payload"));
            }
        }
        Ok(())
    }

    fn emit_pending_tool_use_if_ready(
        saw_message_start: bool,
        pending: &mut PendingToolUseBlock,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        if pending.emitted {
            return Ok(());
        }
        let Some(tool_name) = pending.tool_name.clone() else {
            return Err(ApiError::sse_protocol_with_disposition(
                "tool_use content block missing name",
                if saw_message_start {
                    ProviderFailureDisposition::StreamTerminal
                } else {
                    ProviderFailureDisposition::PreStreamTerminal
                },
            ));
        };
        let Some(input) = pending.input.clone() else {
            return Ok(());
        };
        pending.emitted = true;
        output.push(StreamEvent::ToolUse {
            tool_name,
            input: input.to_string(),
        });
        Ok(())
    }

    fn protocol_error(&self, message: impl Into<String>) -> ApiError {
        ApiError::sse_protocol_with_disposition(
            message,
            if self.saw_message_start {
                ProviderFailureDisposition::StreamTerminal
            } else {
                ProviderFailureDisposition::PreStreamTerminal
            },
        )
    }
}

fn payload_model(payload: &Value) -> Option<&str> {
    payload
        .get("message")
        .and_then(|message| message.get("model"))
        .and_then(Value::as_str)
}

fn parse_usage(usage: &Value, default_model: &str) -> UsageEvent {
    UsageEvent {
        model: default_model.to_string(),
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        cache_creation_input_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        cache_read_input_tokens: usage
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
    }
}

fn map_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" => StopReason::EndTurn,
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "error" | "model_error" | "stop_sequence_error" | "pause_turn" => {
            StopReason::Error
        }
        _ => StopReason::Error,
    }
}

fn classify_stream_error(
    provider_id: &str,
    raw_kind: Option<&str>,
    status_code: Option<u16>,
) -> (String, ProviderFailureDisposition, bool) {
    let _ = normalized_provider_id(provider_id);
    let normalized_kind = raw_kind.unwrap_or("provider_stream");
    let disposition = classify_stream_error_disposition(normalized_kind, status_code);
    let retryable = disposition.is_stream_interrupted();
    (normalized_kind.to_string(), disposition, retryable)
}

fn classify_stream_error_disposition(
    raw_kind: &str,
    status_code: Option<u16>,
) -> ProviderFailureDisposition {
    match raw_kind {
        "model_fallback" | "provider_stream" | "overloaded_error" | "rate_limit_error" => {
            ProviderFailureDisposition::StreamInterrupted
        }
        "invalid_request_error" | "authentication_error" | "permission_error"
        | "not_found_error" | "malformed_payload" | "sse_protocol" => {
            ProviderFailureDisposition::StreamTerminal
        }
        _ => match status_code {
            Some(429) | Some(500..=599) => ProviderFailureDisposition::StreamInterrupted,
            _ => ProviderFailureDisposition::StreamTerminal,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ModelProviderConfig, ProviderTimeout, build_messages_url_for_provider,
        classify_stream_error, classify_stream_error_disposition, map_stop_reason,
        parse_anthropic_sse_response, parse_stream_response_for_provider,
        validate_streaming_response_headers,
    };
    use crate::service::api::retry::RetryPolicy;
    use crate::service::api::streaming::{ProviderFailureDisposition, StopReason, StreamEvent};
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
    use reqwest::StatusCode;

    #[test]
    fn stop_reason_mapping_matches_expected_values() {
        assert_eq!(map_stop_reason("end_turn"), StopReason::EndTurn);
        assert_eq!(map_stop_reason("tool_use"), StopReason::ToolUse);
        assert_eq!(map_stop_reason("max_tokens"), StopReason::MaxTokens);
        assert_eq!(map_stop_reason("error"), StopReason::Error);
        assert_eq!(map_stop_reason("model_error"), StopReason::Error);
        assert_eq!(map_stop_reason("pause_turn"), StopReason::Error);
        assert_eq!(map_stop_reason("unknown_provider_stop"), StopReason::Error);
    }

    #[test]
    fn parses_standard_sse_stream_into_stream_events() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":12}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello \"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Read\",\"input\":{\"path\":\"foo\"}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("sse should parse");
        assert!(matches!(events[0], StreamEvent::MessageStart));
        assert!(matches!(events[1], StreamEvent::Usage(_)));
        assert!(matches!(events[2], StreamEvent::TextDelta(_)));
        assert!(matches!(events[3], StreamEvent::ToolUse { .. }));
        assert!(matches!(events[4], StreamEvent::Usage(_)));
        assert!(matches!(
            events[5],
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse
            }
        ));
    }

    #[test]
    fn provider_config_defaults_include_runtime_fields() {
        let config = ModelProviderConfig::default();
        assert_eq!(config.timeout, ProviderTimeout::default());
        assert_eq!(config.retry_policy, RetryPolicy::default());
        assert_eq!(
            build_messages_url_for_provider(&config.provider_id, &config.base_url)
                .expect("default provider should resolve URL"),
            "http://localhost/v1/messages"
        );
    }

    #[test]
    fn rejects_unknown_provider_ids_for_request_and_parse_paths() {
        let config = ModelProviderConfig {
            provider_id: "custom-provider".into(),
            ..ModelProviderConfig::default()
        };

        let request_error = build_messages_url_for_provider(&config.provider_id, &config.base_url)
            .expect_err("unknown provider should be rejected");
        assert_eq!(request_error.kind_label(), "invalid_response");
        assert!(request_error.message.contains("custom-provider"));

        let parse_error = parse_stream_response_for_provider("custom-provider", "", "model")
            .expect_err("unknown provider parser should be rejected");
        assert_eq!(parse_error.kind_label(), "invalid_response");
        assert!(parse_error.message.contains("custom-provider"));
    }

    #[test]
    fn parses_error_events_into_structured_stream_errors() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"provider exploded\"}}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("sse should parse");
        assert!(matches!(
            &events[0],
            StreamEvent::Error(error)
                if error.provider_id == "anthropic"
                    && error.kind == "overloaded_error"
                    && error.message == "provider exploded"
                    && error.retryable
                    && error.disposition == ProviderFailureDisposition::StreamInterrupted
                    && error.status_code.is_none()
        ));
    }

    #[test]
    fn assembles_partial_tool_use_payloads_across_deltas() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Read\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"partial_json\":\"{\\\"path\\\":\\\"foo\\\"\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"partial_json\":\"}\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_stop\"}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("partial tool payload should parse");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolUse { tool_name, input }
                if tool_name == "Read" && input == "{\"path\":\"foo\"}"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse
            }
        )));
    }

    #[test]
    fn rejects_incomplete_tool_use_payload_at_end_of_stream() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Read\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"partial_json\":\"{\\\"path\\\":\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let error = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect_err("incomplete tool payload should fail");
        assert_eq!(error.kind_label(), "sse_protocol");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::StreamTerminal
        );
        assert!(error
            .message
            .contains("tool_use block ended without complete input payload"));
    }

    #[test]
    fn rejects_empty_response_body() {
        let error = parse_anthropic_sse_response("anthropic", "   ", "default-model")
            .expect_err("empty body should fail");
        assert_eq!(error.kind_label(), "empty_body");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::PreStreamTerminal
        );
    }

    #[test]
    fn rejects_truncated_sse_stream() {
        let pre_stream_error = parse_anthropic_sse_response(
            "anthropic",
            "event: message\ndata: {\"type\":",
            "default-model",
        )
        .expect_err("truncated pre-stream sse should fail");
        assert_eq!(pre_stream_error.kind_label(), "sse_protocol");
        assert_eq!(
            pre_stream_error.disposition,
            ProviderFailureDisposition::PreStreamTerminal
        );
        assert!(
            pre_stream_error.message.contains("truncated SSE frame")
                || pre_stream_error.message.contains("invalid SSE JSON payload")
        );

        let mid_stream_error = parse_anthropic_sse_response(
            "anthropic",
            concat!(
                "event: message\n",
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
                "event: message\n",
                "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"partial\"}}"
            ),
            "default-model",
        )
        .expect_err("truncated mid-stream sse should fail");
        assert_eq!(mid_stream_error.kind_label(), "sse_protocol");
        assert_eq!(
            mid_stream_error.disposition,
            ProviderFailureDisposition::StreamTerminal
        );
    }

    #[test]
    fn rejects_wrong_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let error = validate_streaming_response_headers(&headers, StatusCode::OK)
            .expect_err("wrong content-type should fail");
        assert_eq!(error.kind_label(), "bad_content_type");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::PreStreamTerminal
        );
    }

    #[test]
    fn accepts_event_stream_content_type_with_charset() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );

        validate_streaming_response_headers(&headers, StatusCode::OK)
            .expect("event-stream content-type should pass");
    }

    #[test]
    fn rejects_missing_content_type() {
        let headers = HeaderMap::new();

        let error = validate_streaming_response_headers(&headers, StatusCode::OK)
            .expect_err("missing content-type should fail");
        assert_eq!(error.kind_label(), "bad_content_type");
        assert!(error.message.contains("without content-type"));
    }

    #[test]
    fn distinguishes_pre_stream_and_mid_stream_sse_protocol_failures() {
        let pre_stream_error = parse_anthropic_sse_response(
            "anthropic",
            "event: message\ndata: {not-json}\n\n",
            "default-model",
        )
        .expect_err("invalid pre-stream json should fail");
        assert_eq!(pre_stream_error.kind_label(), "sse_protocol");
        assert_eq!(
            pre_stream_error.disposition,
            ProviderFailureDisposition::PreStreamTerminal
        );

        let mid_stream_error = parse_anthropic_sse_response(
            "anthropic",
            concat!(
                "event: message\n",
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
                "event: message\n",
                "data: {not-json}\n\n"
            ),
            "default-model",
        )
        .expect_err("invalid mid-stream json should fail");
        assert_eq!(mid_stream_error.kind_label(), "sse_protocol");
        assert_eq!(
            mid_stream_error.disposition,
            ProviderFailureDisposition::StreamTerminal
        );
    }

    #[test]
    fn classifies_provider_stream_dispositions() {
        assert_eq!(
            classify_stream_error_disposition("model_fallback", None),
            ProviderFailureDisposition::StreamInterrupted
        );
        assert_eq!(
            classify_stream_error_disposition("provider_stream", None),
            ProviderFailureDisposition::StreamInterrupted
        );
        assert_eq!(
            classify_stream_error_disposition("rate_limit_error", Some(429)),
            ProviderFailureDisposition::StreamInterrupted
        );
        assert_eq!(
            classify_stream_error_disposition("sse_protocol", None),
            ProviderFailureDisposition::StreamTerminal
        );
        assert_eq!(
            classify_stream_error_disposition("bad_request", None),
            ProviderFailureDisposition::StreamTerminal
        );
    }

    #[test]
    fn classifies_stream_errors_with_retryable_flag_from_disposition() {
        let (kind, disposition, retryable) =
            classify_stream_error("anthropic", Some("overloaded_error"), Some(529));
        assert_eq!(kind, "overloaded_error");
        assert_eq!(disposition, ProviderFailureDisposition::StreamInterrupted);
        assert!(retryable);

        let (kind, disposition, retryable) =
            classify_stream_error("anthropic", Some("invalid_request_error"), Some(400));
        assert_eq!(kind, "invalid_request_error");
        assert_eq!(disposition, ProviderFailureDisposition::StreamTerminal);
        assert!(!retryable);
    }
}

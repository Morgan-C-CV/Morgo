use std::sync::{Arc, RwLock};
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};

use crate::core::message::Message;
use crate::service::api::errors::ApiError;
use crate::service::api::retry::RetryPolicy;
use crate::service::api::streaming::{StopReason, StreamEvent, UsageEvent};

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

    pub fn from_config(config: ModelProviderConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout.request_timeout_ms))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            transport: ProviderTransport::Production { config, client },
        }
    }

    pub fn provider_config(&self) -> ModelProviderConfig {
        match &self.transport {
            ProviderTransport::Scripted { .. } => ModelProviderConfig::default(),
            ProviderTransport::Production { config, .. } => config.clone(),
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
            ProviderTransport::Production { config, client } => {
                if input.content.trim().is_empty() {
                    return Vec::new();
                }
                match self.stream_message_with_retry(config, client, input).await {
                    Ok(events) => events,
                    Err(error) => vec![StreamEvent::Error(error.to_string())],
                }
            }
        }
    }

    async fn stream_message_with_retry(
        &self,
        config: &ModelProviderConfig,
        client: &reqwest::Client,
        input: &Message,
    ) -> Result<Vec<StreamEvent>, ApiError> {
        let mut attempt = 0;
        loop {
            match self.stream_message_once(config, client, input).await {
                Ok(events) => return Ok(events),
                Err(error) => {
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
        let url = build_messages_url(&config.base_url);
        let mut request = client
            .post(url)
            .header(ACCEPT, "text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .json(&build_request_payload(config, input));
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
        .map_err(|error| ApiError::transport(format!("provider request failed: {error}")))?;

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

        let body = timeout(
            Duration::from_millis(config.timeout.request_timeout_ms),
            response.text(),
        )
        .await
        .map_err(|_| ApiError::timeout("provider stream timed out while reading response"))?
        .map_err(|error| ApiError::transport(format!("failed reading provider stream: {error}")))?;

        parse_sse_response(&body, &config.model_id)
    }
}

fn build_messages_url(base_url: &str) -> String {
    format!("{}/v1/messages", base_url.trim_end_matches('/'))
}

fn build_request_payload(config: &ModelProviderConfig, input: &Message) -> Value {
    json!({
        "model": config.model_id,
        "messages": [
            {
                "role": "user",
                "content": input.content,
            }
        ],
        "stream": true,
    })
}

pub fn parse_sse_response(body: &str, default_model: &str) -> Result<Vec<StreamEvent>, ApiError> {
    let mut events = Vec::new();
    let mut saw_message_start = false;
    let normalized = body.replace("\r\n", "\n");

    for frame in normalized
        .split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
    {
        let payload = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let json: Value = serde_json::from_str(&payload).map_err(|error| {
            ApiError::sse_protocol(format!("invalid SSE JSON payload: {error}"))
        })?;
        map_provider_event(&json, default_model, &mut saw_message_start, &mut events)?;
    }

    Ok(events)
}

fn map_provider_event(
    payload: &Value,
    default_model: &str,
    saw_message_start: &mut bool,
    output: &mut Vec<StreamEvent>,
) -> Result<(), ApiError> {
    let event_type = payload
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid_response("provider event missing type"))?;

    match event_type {
        "message_start" => {
            if !*saw_message_start {
                output.push(StreamEvent::MessageStart);
                *saw_message_start = true;
            }
            if let Some(usage) = payload
                .get("message")
                .and_then(|message| message.get("usage"))
            {
                output.push(StreamEvent::Usage(parse_usage(
                    usage,
                    payload_model(payload).unwrap_or(default_model),
                )));
            }
        }
        "content_block_start" => {
            if !*saw_message_start {
                output.push(StreamEvent::MessageStart);
                *saw_message_start = true;
            }
            let block = payload.get("content_block").ok_or_else(|| {
                ApiError::invalid_response("content_block_start missing content_block")
            })?;
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        output.push(StreamEvent::TextDelta(text.to_string()));
                    }
                }
                Some("tool_use") => {
                    let tool_name = block.get("name").and_then(Value::as_str).ok_or_else(|| {
                        ApiError::invalid_response("tool_use content block missing name")
                    })?;
                    let tool_input = block
                        .get("input")
                        .cloned()
                        .unwrap_or(Value::Null)
                        .to_string();
                    output.push(StreamEvent::ToolUse {
                        tool_name: tool_name.to_string(),
                        input: tool_input,
                    });
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
        }
        "message_delta" => {
            if let Some(stop_reason) = payload
                .get("delta")
                .and_then(|delta| delta.get("stop_reason"))
                .and_then(Value::as_str)
            {
                output.push(StreamEvent::MessageStop {
                    stop_reason: map_stop_reason(stop_reason),
                });
            }
            if let Some(usage) = payload.get("usage") {
                output.push(StreamEvent::Usage(parse_usage(
                    usage,
                    payload_model(payload).unwrap_or(default_model),
                )));
            }
        }
        "message_stop" => {
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
            output.push(StreamEvent::Error(message));
        }
        _ => {}
    }

    Ok(())
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
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "error" => StopReason::Error,
        _ => StopReason::EndTurn,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ModelProviderConfig, ProviderTimeout, build_messages_url, map_stop_reason,
        parse_sse_response,
    };
    use crate::service::api::retry::RetryPolicy;
    use crate::service::api::streaming::{StopReason, StreamEvent};

    #[test]
    fn stop_reason_mapping_matches_expected_values() {
        assert_eq!(map_stop_reason("end_turn"), StopReason::EndTurn);
        assert_eq!(map_stop_reason("tool_use"), StopReason::ToolUse);
        assert_eq!(map_stop_reason("max_tokens"), StopReason::MaxTokens);
        assert_eq!(map_stop_reason("error"), StopReason::Error);
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

        let events = parse_sse_response(body, "default-model").expect("sse should parse");
        assert!(matches!(events[0], StreamEvent::MessageStart));
        assert!(matches!(events[1], StreamEvent::Usage(_)));
        assert!(matches!(events[2], StreamEvent::TextDelta(_)));
        assert!(matches!(events[3], StreamEvent::ToolUse { .. }));
        assert!(matches!(
            events[4],
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
            build_messages_url(&config.base_url),
            "http://localhost/v1/messages"
        );
    }
}

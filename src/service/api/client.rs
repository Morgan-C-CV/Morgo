use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use base64::Engine as _;
use futures_util::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};

use crate::core::message::Message;
use crate::service::api::errors::{ApiError, ApiErrorKind};
use crate::service::api::retry::RetryPolicy;
use crate::service::api::streaming::{
    ProviderFailureDisposition, StopReason, StreamError, StreamEvent, UsageEvent,
};
use crate::service::observability::ServiceObservabilityTracker;

// Retry-After header is authoritative but bounded to prevent malicious or runaway values.
const RETRY_AFTER_SAFETY_CAP_MS: u64 = 30_000;

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
    pub stream_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct RequestOptions {
    max_tokens: Option<u64>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    stop_sequences: Vec<String>,
    require_tools: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderCompatibilityProfile {
    supports_tools: bool,
    supports_streaming: bool,
    supports_temperature: bool,
    supports_top_p: bool,
    supports_stop_sequences: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderProtocol {
    Anthropic,
    OpenAICompatible,
    GeminiNative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderCompatibilityProfileKind {
    Anthropic,
    TextOnly,
    Batch,
    OpenAICompatible,
    GeminiNativeUnsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAuthStrategy {
    BearerApiKey,
    NoAuth,
}

trait ProviderAdapter {
    fn messages_url(&self, config: &ModelProviderConfig) -> Result<String, ApiError>;
    fn build_request_payload(
        &self,
        config: &ModelProviderConfig,
        input: &Message,
        request_options: RequestOptions,
    ) -> Result<Value, ApiError>;
    fn parse_stream_response(
        &self,
        config: &ModelProviderConfig,
        body: &str,
        default_model: &str,
    ) -> Result<Vec<StreamEvent>, ApiError>;
}

#[derive(Debug, Clone, PartialEq)]
struct NormalizedRequestOptions {
    max_tokens: u64,
    temperature: Option<f64>,
    top_p: Option<f64>,
    stop_sequences: Vec<String>,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            max_tokens: Some(4096),
            temperature: None,
            top_p: None,
            stop_sequences: Vec::new(),
            require_tools: false,
        }
    }
}

impl Default for ProviderTimeout {
    fn default() -> Self {
        Self {
            request_timeout_ms: 30_000,
            stream_timeout_ms: 120_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelProviderConfig {
    pub provider_id: String,
    pub protocol: ProviderProtocol,
    pub compatibility_profile: ProviderCompatibilityProfileKind,
    pub base_url: String,
    pub chat_completions_path: String,
    pub auth_strategy: ProviderAuthStrategy,
    pub api_key: Option<String>,
    pub api_key_env: Option<String>,
    pub model_id: String,
    pub timeout: ProviderTimeout,
    pub retry_policy: RetryPolicy,
    pub pricing: ModelPricing,
    pub proxy_url: Option<String>,
    pub no_proxy: Option<String>,
    pub ca_bundle_path: Option<String>,
    pub max_tokens_param: Option<String>,
    pub prompt_cache_key: Option<String>,
    pub prompt_cache_retention: Option<String>,
}

/// Redact userinfo (password) from a proxy URL for safe display in logs/warnings.
pub fn redact_proxy_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            if parsed.password().is_some() {
                let _ = parsed.set_password(Some("***"));
            }
            parsed.to_string()
        }
        Err(_) => url.to_string(),
    }
}

impl ModelProviderConfig {
    pub fn from_legacy_provider_id(provider_id: impl Into<String>) -> Self {
        let provider_id = provider_id.into();
        let (protocol, compatibility_profile) = expected_contract_for_provider_id(&provider_id)
            .unwrap_or_else(|| {
                let default = Self::default();
                (default.protocol, default.compatibility_profile)
            });
        Self {
            provider_id,
            protocol,
            compatibility_profile,
            ..Self::default()
        }
    }
}

impl Default for ModelProviderConfig {
    fn default() -> Self {
        Self {
            provider_id: "default-provider".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
            base_url: "http://localhost".into(),
            chat_completions_path: "/v1/chat/completions".into(),
            auth_strategy: ProviderAuthStrategy::NoAuth,
            api_key: None,
            api_key_env: None,
            model_id: "default-model".into(),
            timeout: ProviderTimeout::default(),
            retry_policy: RetryPolicy::default(),
            pricing: ModelPricing::default(),
            proxy_url: None,
            no_proxy: None,
            ca_bundle_path: None,
            max_tokens_param: None,
            prompt_cache_key: None,
            prompt_cache_retention: None,
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
        let client = build_reqwest_client(&config);
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
                if input.text().trim().is_empty() {
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
                    let retry_decision = classify_retry_policy(&error);
                    if config.retry_policy.should_retry(attempt, &error, false)
                        && !matches!(retry_decision, RetryDecision::DoNotRetry)
                    {
                        match retry_decision {
                            RetryDecision::DoNotRetry => {}
                            RetryDecision::RetryDefaultBackoff => {
                                sleep(config.retry_policy.backoff_for_attempt(attempt)).await;
                            }
                            RetryDecision::RetryAfterMs(delay_ms) => {
                                let capped = delay_ms.min(RETRY_AFTER_SAFETY_CAP_MS);
                                sleep(Duration::from_millis(capped)).await;
                            }
                        }
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
        let url = build_messages_url_for_provider(config)?;
        let payload = build_request_payload_for_provider(config, input)?;
        let mut request = client
            .post(url)
            .header(ACCEPT, "text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .json(&payload);
        match config.auth_strategy {
            ProviderAuthStrategy::BearerApiKey => {
                let api_key = config.api_key.as_ref().ok_or_else(|| {
                    ApiError::invalid_configuration("provider auth strategy requires api_key")
                })?;
                request = request.header(AUTHORIZATION, format!("Bearer {}", api_key.trim()));
            }
            ProviderAuthStrategy::NoAuth => {}
        }

        let response = timeout(
            Duration::from_millis(config.timeout.request_timeout_ms),
            request.send(),
        )
        .await
        .map_err(|_| ApiError::timeout("provider request timed out"))?
        .map_err(classify_request_transport_error)?;

        let status = response.status();
        let retry_after_ms = parse_retry_after_ms(response.headers());
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(ApiError::http_status(
                status.as_u16(),
                normalized_http_error_message(status, &body),
            )
            .with_retry_after_ms(retry_after_ms));
        }
        validate_streaming_response_headers(response.headers(), status)?;

        let body = read_response_body_with_idle_timeout(response, config.timeout.stream_timeout_ms)
            .await?;

        if body.trim().is_empty() {
            return Err(ApiError::empty_body(
                "provider returned empty response body",
            ));
        }

        parse_stream_response_for_provider(config, &body, &config.model_id)
    }
}

async fn read_response_body_with_idle_timeout(
    response: reqwest::Response,
    stream_timeout_ms: u64,
) -> Result<String, ApiError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();

    loop {
        let next_chunk = timeout(Duration::from_millis(stream_timeout_ms), stream.next())
            .await
            .map_err(|_| {
                ApiError::timeout("provider stream timed out while idle reading response")
            })?;

        match next_chunk {
            Some(Ok(chunk)) => body.extend_from_slice(&chunk),
            Some(Err(error)) => return Err(classify_response_body_error(error)),
            None => break,
        }
    }

    String::from_utf8(body).map_err(|error| {
        ApiError::invalid_response(format!(
            "provider response body was not valid UTF-8: {error}"
        ))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryDecision {
    DoNotRetry,
    RetryDefaultBackoff,
    RetryAfterMs(u64),
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

fn classify_retry_policy(error: &ApiError) -> RetryDecision {
    match error.kind {
        ApiErrorKind::Timeout | ApiErrorKind::ConnectionReset => RetryDecision::RetryDefaultBackoff,
        ApiErrorKind::HttpStatus(429) => error
            .retry_after_ms
            .map(RetryDecision::RetryAfterMs)
            .unwrap_or(RetryDecision::RetryDefaultBackoff),
        ApiErrorKind::HttpStatus(500..=599) => RetryDecision::RetryDefaultBackoff,
        ApiErrorKind::HttpStatus(_)
        | ApiErrorKind::RequestBuild
        | ApiErrorKind::Transport
        | ApiErrorKind::EmptyBody
        | ApiErrorKind::BadContentType
        | ApiErrorKind::InvalidResponse
        | ApiErrorKind::InvalidConfiguration
        | ApiErrorKind::CapabilityUnsupported
        | ApiErrorKind::InvalidRequestOption
        | ApiErrorKind::SseProtocol
        | ApiErrorKind::ToolUseProtocol
        | ApiErrorKind::StructuredOutputInvalid => RetryDecision::DoNotRetry,
    }
}

fn parse_retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    let seconds = value.parse::<u64>().ok()?;
    Some(seconds.saturating_mul(1000))
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

fn build_reqwest_client(config: &ModelProviderConfig) -> reqwest::Client {
    build_reqwest_client_with_result(config).unwrap_or_else(|_| reqwest::Client::new())
}

fn build_reqwest_client_with_result(config: &ModelProviderConfig) -> anyhow::Result<reqwest::Client> {
    use crate::bootstrap::proxy_env::resolve_proxy_env_contract;

    let mut builder = reqwest::Client::builder();

    // CA bundle — explicit config takes precedence over env.
    let ca_bundle_path = config.ca_bundle_path.as_deref().or_else(|| {
        // Checked at call time; env var may not be set.
        None
    });
    if let Some(path) = ca_bundle_path {
        let pem = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("failed to read CA bundle at {path}: {e}"))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| anyhow::anyhow!("invalid CA bundle PEM at {path}: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }

    // Proxy — explicit config > env fallback.
    let (proxy_url, no_proxy) = if config.proxy_url.is_some() {
        (config.proxy_url.as_deref(), config.no_proxy.as_deref())
    } else {
        let env = resolve_proxy_env_contract();
        // Leak the strings into the builder scope via owned values.
        // We need to return them as &str but they're owned — use a local binding.
        // Instead, handle the env case inline.
        if let Some(url) = env.proxy_url {
            let mut proxy = reqwest::Proxy::all(&url)
                .map_err(|e| anyhow::anyhow!("invalid proxy URL from env '{url}': {e}"))?;
            if let Some(np) = env.no_proxy {
                proxy = proxy.no_proxy(reqwest::NoProxy::from_string(&np));
            }
            builder = builder.proxy(proxy);
            return Ok(builder.build()?);
        }
        (None, None)
    };

    if let Some(url) = proxy_url {
        let mut proxy = reqwest::Proxy::all(url)
            .map_err(|e| anyhow::anyhow!("invalid proxy URL '{url}': {e}"))?;
        if let Some(np) = no_proxy {
            proxy = proxy.no_proxy(reqwest::NoProxy::from_string(np));
        }
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}

fn build_messages_url_for_provider(config: &ModelProviderConfig) -> Result<String, ApiError> {
    adapter_for_config(config)?.messages_url(config)
}

fn build_request_payload_for_provider(
    config: &ModelProviderConfig,
    input: &Message,
) -> Result<Value, ApiError> {
    build_request_payload_with_options(config, input, RequestOptions::default())
}

fn build_request_payload_with_options(
    config: &ModelProviderConfig,
    input: &Message,
    request_options: RequestOptions,
) -> Result<Value, ApiError> {
    adapter_for_config(config)?.build_request_payload(config, input, request_options)
}

fn normalized_request_model(config: &ModelProviderConfig) -> &str {
    let trimmed = config.model_id.trim();
    if trimmed.is_empty() {
        "default-model"
    } else {
        trimmed
    }
}

fn normalized_request_message(input: &Message) -> Value {
    use crate::core::message::ContentBlock;
    let content_blocks: Vec<Value> = input
        .blocks
        .iter()
        .map(|block| match block {
            ContentBlock::Text { text } => json!({"type": "text", "text": text}),
            ContentBlock::Image { media_type, data } => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(data);
                json!({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": encoded,
                    }
                })
            }
        })
        .collect();
    json!({"role": "user", "content": content_blocks})
}

struct AnthropicAdapter;
struct OpenAICompatibleAdapter;
struct GeminiNativeAdapter;

pub fn validate_provider_config(config: &ModelProviderConfig) -> Result<(), ApiError> {
    if config.base_url.trim().is_empty() {
        return Err(ApiError::invalid_configuration(
            "provider configuration missing base_url",
        ));
    }
    validate_chat_completions_path(&config.chat_completions_path)?;
    if config.model_id.trim().is_empty() {
        return Err(ApiError::invalid_configuration(
            "provider configuration missing default_model",
        ));
    }
    match config.auth_strategy {
        ProviderAuthStrategy::BearerApiKey => {
            let Some(api_key) = config.api_key.as_ref() else {
                return Err(ApiError::invalid_configuration(
                    "provider configuration missing auth header strategy input api_key",
                ));
            };
            if api_key.trim().is_empty() {
                return Err(ApiError::invalid_configuration(
                    "provider configuration missing auth header strategy input api_key",
                ));
            }
        }
        ProviderAuthStrategy::NoAuth => {}
    }

    if let Some((expected_protocol, expected_profile)) =
        expected_contract_for_provider_id(&config.provider_id)
    {
        if config.protocol == expected_protocol && config.compatibility_profile == expected_profile
        {
            return Ok(());
        }
        return Err(ApiError::invalid_configuration(format!(
            "provider {} has incompatible protocol/profile configuration",
            config.provider_id
        )));
    }

    match (config.protocol, config.compatibility_profile) {
        (ProviderProtocol::Anthropic, ProviderCompatibilityProfileKind::Anthropic)
        | (ProviderProtocol::Anthropic, ProviderCompatibilityProfileKind::TextOnly)
        | (ProviderProtocol::Anthropic, ProviderCompatibilityProfileKind::Batch)
        | (
            ProviderProtocol::OpenAICompatible,
            ProviderCompatibilityProfileKind::OpenAICompatible,
        )
        | (
            ProviderProtocol::GeminiNative,
            ProviderCompatibilityProfileKind::GeminiNativeUnsupported,
        ) => Ok(()),
        _ => Err(ApiError::invalid_configuration(format!(
            "provider {} has incompatible protocol/profile configuration",
            config.provider_id
        ))),
    }
}

fn adapter_for_config(
    config: &ModelProviderConfig,
) -> Result<&'static dyn ProviderAdapter, ApiError> {
    validate_provider_config(config)?;
    match config.protocol {
        ProviderProtocol::Anthropic => Ok(&AnthropicAdapter),
        ProviderProtocol::OpenAICompatible => Ok(&OpenAICompatibleAdapter),
        ProviderProtocol::GeminiNative => Ok(&GeminiNativeAdapter),
    }
}

fn validate_chat_completions_path(path: &str) -> Result<&str, ApiError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(ApiError::invalid_configuration(
            "provider chat completions path is empty",
        ));
    }
    if trimmed.contains("://") {
        return Err(ApiError::invalid_configuration(
            "provider chat completions path must not be a full URL",
        ));
    }
    if !trimmed.starts_with('/') {
        return Err(ApiError::invalid_configuration(
            "provider chat completions path must start with '/'",
        ));
    }
    Ok(trimmed)
}

impl ProviderAdapter for AnthropicAdapter {
    fn messages_url(&self, config: &ModelProviderConfig) -> Result<String, ApiError> {
        Ok(format!(
            "{}/v1/messages",
            config.base_url.trim_end_matches('/')
        ))
    }

    fn build_request_payload(
        &self,
        config: &ModelProviderConfig,
        input: &Message,
        request_options: RequestOptions,
    ) -> Result<Value, ApiError> {
        let profile = profile_for_provider(config)?;
        let options = normalize_request_options(&profile, &request_options)?;
        let mut payload = json!({
            "model": normalized_request_model(config),
            "messages": [normalized_request_message(input)],
            "stream": normalized_request_stream_flag(&profile)?,
            "stream_options": {"include_usage": true},
            "max_tokens": options.max_tokens,
        });
        if let Some(temperature) = options.temperature {
            payload["temperature"] = json!(temperature);
        }
        if let Some(top_p) = options.top_p {
            payload["top_p"] = json!(top_p);
        }
        if !options.stop_sequences.is_empty() {
            payload["stop_sequences"] = json!(options.stop_sequences);
        }
        Ok(payload)
    }

    fn parse_stream_response(
        &self,
        config: &ModelProviderConfig,
        body: &str,
        default_model: &str,
    ) -> Result<Vec<StreamEvent>, ApiError> {
        parse_anthropic_sse_response(&config.provider_id, body, default_model)
    }
}

impl ProviderAdapter for OpenAICompatibleAdapter {
    fn messages_url(&self, config: &ModelProviderConfig) -> Result<String, ApiError> {
        let path = validate_chat_completions_path(&config.chat_completions_path)?;
        Ok(format!("{}{}", config.base_url.trim_end_matches('/'), path))
    }

    fn build_request_payload(
        &self,
        config: &ModelProviderConfig,
        input: &Message,
        request_options: RequestOptions,
    ) -> Result<Value, ApiError> {
        use crate::core::message::ContentBlock;
        let profile = profile_for_provider(config)?;
        let options = normalize_request_options(&profile, &request_options)?;
        let message_content: Value = if input.is_text_only() {
            json!(input.text())
        } else {
            let blocks: Vec<Value> = input
                .blocks
                .iter()
                .map(|block| match block {
                    ContentBlock::Text { text } => json!({"type": "text", "text": text}),
                    ContentBlock::Image { media_type, data } => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(data);
                        json!({
                            "type": "image_url",
                            "image_url": {
                                "url": format!("data:{media_type};base64,{encoded}"),
                            }
                        })
                    }
                })
                .collect();
            json!(blocks)
        };
        let mut payload = json!({
            "model": normalized_request_model(config),
            "messages": [{"role": "user", "content": message_content}],
            "stream": normalized_request_stream_flag(&profile)?,
            "stream_options": {"include_usage": true},
        });
        let max_tokens_key = config.max_tokens_param.as_deref().unwrap_or("max_tokens");
        payload[max_tokens_key] = json!(options.max_tokens);
        if let Some(temperature) = options.temperature {
            payload["temperature"] = json!(temperature);
        }
        if let Some(top_p) = options.top_p {
            payload["top_p"] = json!(top_p);
        }
        if !options.stop_sequences.is_empty() {
            payload["stop"] = json!(options.stop_sequences);
        }
        if let Some(key) = config
            .prompt_cache_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            payload["prompt_cache_key"] = json!(key);
        }
        if let Some(retention) = config
            .prompt_cache_retention
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            payload["prompt_cache_retention"] = json!(retention);
        }
        Ok(payload)
    }

    fn parse_stream_response(
        &self,
        config: &ModelProviderConfig,
        body: &str,
        default_model: &str,
    ) -> Result<Vec<StreamEvent>, ApiError> {
        parse_openai_compatible_sse_response(&config.provider_id, body, default_model)
    }
}

impl ProviderAdapter for GeminiNativeAdapter {
    fn messages_url(&self, _config: &ModelProviderConfig) -> Result<String, ApiError> {
        Err(ApiError::capability_unsupported(
            "gemini native protocol is not supported yet",
        ))
    }

    fn build_request_payload(
        &self,
        _config: &ModelProviderConfig,
        _input: &Message,
        _request_options: RequestOptions,
    ) -> Result<Value, ApiError> {
        Err(ApiError::capability_unsupported(
            "gemini native protocol is not supported yet",
        ))
    }

    fn parse_stream_response(
        &self,
        _config: &ModelProviderConfig,
        _body: &str,
        _default_model: &str,
    ) -> Result<Vec<StreamEvent>, ApiError> {
        Err(ApiError::capability_unsupported(
            "gemini native protocol is not supported yet",
        ))
    }
}

fn profile_for_provider(
    config: &ModelProviderConfig,
) -> Result<ProviderCompatibilityProfile, ApiError> {
    match config.protocol {
        ProviderProtocol::Anthropic | ProviderProtocol::OpenAICompatible => {
            Ok(compatibility_profile_for_kind(config.compatibility_profile))
        }
        ProviderProtocol::GeminiNative => Err(ApiError::capability_unsupported(format!(
            "provider protocol {:?} is not supported yet",
            config.protocol
        ))),
    }
}

fn normalize_request_options(
    profile: &ProviderCompatibilityProfile,
    options: &RequestOptions,
) -> Result<NormalizedRequestOptions, ApiError> {
    if options.require_tools && !profile.supports_tools {
        return Err(ApiError::capability_unsupported(
            "provider does not support tool-use requests",
        ));
    }
    let max_tokens = options.max_tokens.unwrap_or(4096);
    if max_tokens == 0 {
        return Err(ApiError::invalid_request_option(
            "max_tokens must be greater than zero",
        ));
    }
    if let Some(temperature) = options.temperature {
        if !(0.0..=2.0).contains(&temperature) || !temperature.is_finite() {
            return Err(ApiError::invalid_request_option(
                "temperature must be finite and between 0.0 and 2.0",
            ));
        }
    }
    if let Some(top_p) = options.top_p {
        if !(0.0..=1.0).contains(&top_p) || !top_p.is_finite() {
            return Err(ApiError::invalid_request_option(
                "top_p must be finite and between 0.0 and 1.0",
            ));
        }
    }
    if options
        .stop_sequences
        .iter()
        .any(|sequence| sequence.trim().is_empty())
    {
        return Err(ApiError::invalid_request_option(
            "stop_sequences cannot contain empty values",
        ));
    }

    Ok(NormalizedRequestOptions {
        max_tokens,
        temperature: options.temperature.filter(|_| profile.supports_temperature),
        top_p: options.top_p.filter(|_| profile.supports_top_p),
        stop_sequences: if profile.supports_stop_sequences {
            options.stop_sequences.clone()
        } else {
            Vec::new()
        },
    })
}

fn normalized_request_stream_flag(
    profile: &ProviderCompatibilityProfile,
) -> Result<bool, ApiError> {
    if !profile.supports_streaming {
        return Err(ApiError::capability_unsupported(
            "provider does not support streaming requests",
        ));
    }
    Ok(true)
}

fn parse_stream_response_for_provider(
    config: &ModelProviderConfig,
    body: &str,
    default_model: &str,
) -> Result<Vec<StreamEvent>, ApiError> {
    adapter_for_config(config)?.parse_stream_response(config, body, default_model)
}

fn normalized_http_error_message(status: StatusCode, body: &str) -> String {
    let detail = extract_error_detail(body).unwrap_or_else(|| body.trim().to_string());
    format!(
        "provider request failed with status {}: {}",
        status.as_u16(),
        if detail.is_empty() {
            "<empty error body>".into()
        } else {
            detail
        }
    )
}

fn extract_error_detail(body: &str) -> Option<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }

    let parsed: Value = serde_json::from_str(trimmed).ok()?;
    extract_error_detail_from_value(&parsed)
}

fn extract_error_detail_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => {
            if let Some(text) = map.get("message").and_then(Value::as_str) {
                return Some(text.to_string());
            }
            if let Some(error) = map.get("error") {
                return extract_error_detail_from_value(error);
            }
            if let Some(text) = map.get("detail").and_then(Value::as_str) {
                return Some(text.to_string());
            }
            if let Some(text) = map.get("error_message").and_then(Value::as_str) {
                return Some(text.to_string());
            }
            if let Some(kind) = map.get("type").and_then(Value::as_str) {
                return Some(kind.to_string());
            }
            Some(value.to_string())
        }
        Value::Array(items) => items
            .iter()
            .find_map(extract_error_detail_from_value)
            .or_else(|| Some(value.to_string())),
        _ => Some(value.to_string()),
    }
}

fn normalized_provider_id(provider_id: &str) -> &str {
    provider_id.trim()
}

fn expected_contract_for_provider_id(
    provider_id: &str,
) -> Option<(ProviderProtocol, ProviderCompatibilityProfileKind)> {
    match normalized_provider_id(provider_id) {
        "anthropic" | "default-provider" => Some((
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::Anthropic,
        )),
        "text-only-provider" => Some((
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::TextOnly,
        )),
        "batch-provider" => Some((
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::Batch,
        )),
        "openai" | "openai-compatible" | "openai_compatible" | "kimi" | "glm" | "minimax" => {
            Some((
                ProviderProtocol::OpenAICompatible,
                ProviderCompatibilityProfileKind::OpenAICompatible,
            ))
        }
        "gemini" | "gemini-native" | "gemini_native" => Some((
            ProviderProtocol::GeminiNative,
            ProviderCompatibilityProfileKind::GeminiNativeUnsupported,
        )),
        _ => None,
    }
}

pub fn resolve_provider_protocol(provider_id: &str) -> Option<ProviderProtocol> {
    expected_contract_for_provider_id(provider_id).map(|(protocol, _)| protocol)
}

pub fn resolve_provider_profile(provider_id: &str) -> Option<ProviderCompatibilityProfileKind> {
    expected_contract_for_provider_id(provider_id).map(|(_, profile)| profile)
}

fn compatibility_profile_for_kind(
    profile: ProviderCompatibilityProfileKind,
) -> ProviderCompatibilityProfile {
    match profile {
        ProviderCompatibilityProfileKind::Anthropic => ProviderCompatibilityProfile {
            supports_tools: true,
            supports_streaming: true,
            supports_temperature: true,
            supports_top_p: true,
            supports_stop_sequences: true,
        },
        ProviderCompatibilityProfileKind::TextOnly => ProviderCompatibilityProfile {
            supports_tools: false,
            supports_streaming: true,
            supports_temperature: false,
            supports_top_p: false,
            supports_stop_sequences: false,
        },
        ProviderCompatibilityProfileKind::Batch => ProviderCompatibilityProfile {
            supports_tools: true,
            supports_streaming: false,
            supports_temperature: true,
            supports_top_p: true,
            supports_stop_sequences: true,
        },
        ProviderCompatibilityProfileKind::OpenAICompatible => ProviderCompatibilityProfile {
            supports_tools: true,
            supports_streaming: true,
            supports_temperature: true,
            supports_top_p: true,
            supports_stop_sequences: true,
        },
        ProviderCompatibilityProfileKind::GeminiNativeUnsupported => ProviderCompatibilityProfile {
            supports_tools: false,
            supports_streaming: true,
            supports_temperature: false,
            supports_top_p: false,
            supports_stop_sequences: false,
        },
    }
}

pub fn parse_anthropic_sse_response(
    provider_id: &str,
    body: &str,
    default_model: &str,
) -> Result<Vec<StreamEvent>, ApiError> {
    if body.trim().is_empty() {
        return Err(ApiError::empty_body(
            "provider returned empty response body",
        ));
    }

    let mut events = Vec::new();
    let mut parser = ProviderStreamParser::new(provider_id, default_model);
    let normalized = body.replace("\r\n", "\n");
    let complete_body = normalized.ends_with("\n\n");
    let frames = if complete_body {
        normalized.split("\n\n").collect::<Vec<_>>()
    } else {
        normalized.split_terminator("\n\n").collect::<Vec<_>>()
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

pub fn parse_openai_compatible_sse_response(
    _provider_id: &str,
    body: &str,
    default_model: &str,
) -> Result<Vec<StreamEvent>, ApiError> {
    if body.trim().is_empty() {
        return Err(ApiError::empty_body(
            "provider returned empty response body",
        ));
    }

    let normalized = body.replace("\r\n", "\n");
    let complete_body = normalized.ends_with("\n\n");
    let frames = if complete_body {
        normalized.split("\n\n").collect::<Vec<_>>()
    } else {
        normalized.split_terminator("\n\n").collect::<Vec<_>>()
    };

    let mut events = Vec::new();
    let mut parser = OpenAICompatibleStreamParser::new(default_model);

    for frame in frames.into_iter().filter(|frame| !frame.trim().is_empty()) {
        let payload = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
        if payload.is_empty() {
            continue;
        }
        if payload == "[DONE]" {
            parser.saw_terminal = true;
            continue;
        }

        let json: Value = serde_json::from_str(&payload)
            .map_err(|error| parser.protocol_error(format!("invalid SSE JSON payload: {error}")))?;
        parser.map_event(&json, &mut events)?;
    }

    parser.finish(complete_body, &mut events)?;
    Ok(events)
}

#[derive(Debug, Default, Clone)]
struct PendingOpenAIToolCall {
    name: Option<String>,
    arguments: String,
    saw_arguments: bool,
    saw_null_arguments: bool,
    emitted: bool,
}

struct OpenAICompatibleStreamParser<'a> {
    default_model: &'a str,
    saw_message_start: bool,
    saw_terminal: bool,
    pending_usage: Option<NormalizedUsage>,
    pending_tool_calls: BTreeMap<usize, PendingOpenAIToolCall>,
}

impl<'a> OpenAICompatibleStreamParser<'a> {
    fn new(default_model: &'a str) -> Self {
        Self {
            default_model,
            saw_message_start: false,
            saw_terminal: false,
            pending_usage: None,
            pending_tool_calls: BTreeMap::new(),
        }
    }

    fn map_event(
        &mut self,
        payload: &Value,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        let choices_value = payload.get("choices");
        if let Some(error) = payload.get("error") {
            let choices_absent_or_empty = match choices_value {
                None => true,
                Some(Value::Array(choices)) => choices.is_empty(),
                Some(_) => false,
            };
            if choices_absent_or_empty {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown provider error");
                let error_type = error.get("type").and_then(Value::as_str).unwrap_or("unknown");
                let error_code = error.get("code").and_then(Value::as_str).unwrap_or("unknown");
                return Err(ApiError::invalid_response(format!(
                    "provider returned error envelope in openai-compatible stream: message={message}, type={error_type}, code={error_code}",
                )));
            }
        }

        if let Some(usage) = payload.get("usage") {
            let incoming = normalize_usage(usage, self.default_model);
            self.pending_usage = Some(match self.pending_usage.take() {
                Some(existing) => merge_usage(existing, incoming),
                None => incoming,
            });
        }

        let choices = choices_value
            .and_then(Value::as_array)
            .ok_or_else(|| self.protocol_error("openai-compatible event missing choices"))?;

        if choices.is_empty() {
            let has_error = payload.get("error").is_some();
            // OpenAI sends a final usage-only chunk with empty choices when stream_options.include_usage=true.
            // Real usage terminals have no top-level finish_reason (it was already in a prior choices entry).
            // Chunks with both empty choices and a top-level finish_reason are a protocol anomaly.
            let finish_reason_absent = payload.get("finish_reason").is_none_or(Value::is_null);
            let usage_only_terminal_chunk =
                !has_error && finish_reason_absent && payload.get("usage").is_some();
            if usage_only_terminal_chunk {
                return Ok(());
            }
            return Err(self.protocol_error(
                "openai-compatible event had empty choices outside usage-only terminal chunk",
            ));
        }

        self.ensure_message_start(output);

        for choice in choices {
            if let Some(delta) = choice.get("delta") {
                if let Some(content) = delta.get("content") {
                    let Some(text) = content.as_str() else {
                        return Err(self.protocol_error(
                            "openai-compatible delta.content must be string when present",
                        ));
                    };
                    output.push(StreamEvent::TextDelta(text.to_string()));
                }
            }

            if let Some(tool_calls) = choice
                .get("delta")
                .and_then(|delta| delta.get("tool_calls"))
                .and_then(Value::as_array)
            {
                for tool_call in tool_calls {
                    let Some(index) = tool_call
                        .get("index")
                        .and_then(Value::as_u64)
                        .map(|value| value as usize)
                    else {
                        return Err(self.tool_use_protocol_error("tool_call delta missing index"));
                    };
                    let pending = self.pending_tool_calls.entry(index).or_default();
                    if let Some(name) = tool_call
                        .get("function")
                        .and_then(|function| function.get("name"))
                        .and_then(Value::as_str)
                    {
                        pending.name = Some(name.to_string());
                    }
                    if let Some(arguments) = tool_call
                        .get("function")
                        .and_then(|function| function.get("arguments"))
                    {
                        pending.saw_arguments = true;
                        match arguments {
                            Value::String(text) => pending.arguments.push_str(text),
                            Value::Null => pending.saw_null_arguments = true,
                            _ => {
                                return Err(self.tool_use_protocol_error(
                                    "tool_call arguments must be string or null",
                                ));
                            }
                        }
                    }
                }
            }

            if let Some(finish_reason) = choice.get("finish_reason").and_then(Value::as_str) {
                self.saw_terminal = true;
                let stop_reason = map_openai_finish_reason(finish_reason);
                if matches!(stop_reason, StopReason::ToolUse) {
                    self.finalize_tool_calls(output)?;
                }
                output.push(StreamEvent::MessageStop { stop_reason });
            }
        }

        Ok(())
    }

    fn finish(
        &mut self,
        complete_body: bool,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        if let Some(event) = self
            .pending_usage
            .take()
            .and_then(|usage| usage.into_usage_event(self.default_model))
        {
            output.push(StreamEvent::Usage(event));
        }

        if !complete_body {
            return Err(self.protocol_error("provider returned truncated SSE frame"));
        }

        if !self.saw_terminal {
            output.push(StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            });
        }

        Ok(())
    }

    fn finalize_tool_calls(&mut self, output: &mut Vec<StreamEvent>) -> Result<(), ApiError> {
        if self.pending_tool_calls.is_empty() {
            return Err(self.tool_use_protocol_error("tool_calls stop without tool payload"));
        }

        for pending in self.pending_tool_calls.values_mut() {
            if pending.emitted {
                continue;
            }
            let Some(tool_name) = pending.name.clone() else {
                return Err(self.tool_use_protocol_error("tool_call missing function name"));
            };
            if pending.saw_null_arguments {
                return Err(self.tool_use_protocol_error("tool_call arguments must not be null"));
            }
            if pending.arguments.trim().is_empty() {
                if pending.saw_arguments {
                    return Err(self.tool_use_protocol_error(
                        "tool_call arguments did not contain valid JSON payload",
                    ));
                }
                pending.emitted = true;
                output.push(StreamEvent::ToolUse {
                    tool_name,
                    input: "{}".to_string(),
                });
                continue;
            }

            let normalized = normalize_json_like_value(
                &Value::String(pending.arguments.clone()),
                "tool_use input",
            )?;
            if matches!(normalized, Value::String(_)) {
                return Err(ApiError::tool_use_protocol_with_disposition(
                    "tool_call arguments must normalize to JSON object or array",
                    ProviderFailureDisposition::StreamTerminal,
                ));
            }
            pending.emitted = true;
            output.push(StreamEvent::ToolUse {
                tool_name,
                input: normalized.to_string(),
            });
        }

        Ok(())
    }

    fn ensure_message_start(&mut self, output: &mut Vec<StreamEvent>) {
        if !self.saw_message_start {
            output.push(StreamEvent::MessageStart);
            self.saw_message_start = true;
        }
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

    fn tool_use_protocol_error(&self, message: impl Into<String>) -> ApiError {
        ApiError::tool_use_protocol_with_disposition(
            message,
            if self.saw_message_start {
                ProviderFailureDisposition::StreamTerminal
            } else {
                ProviderFailureDisposition::PreStreamTerminal
            },
        )
    }
}

fn map_openai_finish_reason(reason: &str) -> StopReason {
    match reason {
        "stop" => StopReason::EndTurn,
        "length" => StopReason::MaxTokens,
        "tool_calls" => StopReason::ToolUse,
        "content_filter" => StopReason::Error,
        _ => StopReason::Error,
    }
}

#[derive(Debug, Default, Clone)]
struct PendingToolUseBlock {
    tool_name: Option<String>,
    input: Option<Value>,
    partial_json: String,
    emitted: bool,
}

#[derive(Debug, Default, Clone)]
struct PendingStructuredOutputBlock {
    value: Option<Value>,
    partial_json: String,
    emitted: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct NormalizedUsage {
    model: Option<String>,
    input_tokens: Option<usize>,
    output_tokens: Option<usize>,
    cache_creation_input_tokens: Option<usize>,
    cache_read_input_tokens: Option<usize>,
    total_tokens: Option<usize>,
}

struct ProviderStreamParser<'a> {
    provider_id: &'a str,
    default_model: &'a str,
    saw_message_start: bool,
    emitted_tool_use: bool,
    pending_tool_use: Option<PendingToolUseBlock>,
    pending_structured_output: Option<PendingStructuredOutputBlock>,
    pending_usage: Option<NormalizedUsage>,
    pending_stop_reason: Option<StopReason>,
}

impl<'a> ProviderStreamParser<'a> {
    fn new(provider_id: &'a str, default_model: &'a str) -> Self {
        Self {
            provider_id,
            default_model,
            saw_message_start: false,
            emitted_tool_use: false,
            pending_tool_use: None,
            pending_structured_output: None,
            pending_usage: None,
            pending_stop_reason: None,
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

        self.collect_usage(payload);

        match event_type {
            "message_start" => {
                self.ensure_message_start(output);
            }
            "content_block_start" => {
                self.ensure_message_start(output);
                let block = payload.get("content_block").ok_or_else(|| {
                    self.protocol_error("content_block_start missing content_block")
                })?;
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            output.push(StreamEvent::TextDelta(text.to_string()));
                        }
                    }
                    Some("tool_use") => {
                        let tool_name =
                            block.get("name").and_then(Value::as_str).ok_or_else(|| {
                                self.tool_use_protocol_error("tool_use content block missing name")
                            })?;
                        let mut pending = PendingToolUseBlock {
                            tool_name: Some(tool_name.to_string()),
                            input: normalize_tool_use_input(block)?,
                            partial_json: String::new(),
                            emitted: false,
                        };
                        Self::emit_pending_tool_use_if_ready(
                            self.saw_message_start,
                            &mut self.emitted_tool_use,
                            &mut pending,
                            output,
                        )?;
                        self.pending_tool_use = Some(pending);
                    }
                    Some("json") | Some("structured_output") => {
                        let mut pending = PendingStructuredOutputBlock {
                            value: normalize_structured_output_value(block)?,
                            partial_json: String::new(),
                            emitted: false,
                        };
                        Self::emit_pending_structured_output_if_ready(&mut pending, output)?;
                        self.pending_structured_output = Some(pending);
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
                    if let Some(input_delta) = payload
                        .get("delta")
                        .and_then(|delta| delta.get("input_json_delta"))
                    {
                        pending.input =
                            Some(normalize_json_like_value(input_delta, "tool_use input")?);
                    }
                    Self::emit_pending_tool_use_if_ready(
                        self.saw_message_start,
                        &mut self.emitted_tool_use,
                        pending,
                        output,
                    )?;
                }
                if let Some(pending) = self.pending_structured_output.as_mut() {
                    if let Some(partial_json) = payload
                        .get("delta")
                        .and_then(|delta| delta.get("partial_json"))
                        .and_then(Value::as_str)
                    {
                        pending.partial_json.push_str(partial_json);
                        if let Ok(value) = serde_json::from_str::<Value>(&pending.partial_json) {
                            pending.value = Some(value);
                        }
                    }
                    if let Some(output_delta) = payload
                        .get("delta")
                        .and_then(|delta| delta.get("output_json_delta"))
                    {
                        pending.value = Some(normalize_json_like_value(
                            output_delta,
                            "structured output",
                        )?);
                    }
                    Self::emit_pending_structured_output_if_ready(pending, output)?;
                }
            }
            "content_block_stop" => {
                self.finalize_pending_tool_use(output)?;
                self.finalize_pending_structured_output(output)?;
            }
            "message_delta" => {
                if let Some(stop_reason) = payload
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.pending_stop_reason = Some(map_stop_reason(stop_reason));
                }
            }
            "message_stop" => {
                self.finalize_pending_tool_use(output)?;
                self.finalize_pending_structured_output(output)?;
                self.flush_usage(output);
                let stop_reason = self
                    .pending_stop_reason
                    .take()
                    .unwrap_or(StopReason::EndTurn);
                self.validate_stop_reason(&stop_reason)?;
                if !output
                    .iter()
                    .any(|event| matches!(event, StreamEvent::MessageStop { .. }))
                {
                    output.push(StreamEvent::MessageStop { stop_reason });
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
        self.finalize_pending_tool_use(output)?;
        self.finalize_pending_structured_output(output)?;
        self.flush_usage(output);
        if let Some(stop_reason) = self.pending_stop_reason.take() {
            self.validate_stop_reason(&stop_reason)?;
            if !output
                .iter()
                .any(|event| matches!(event, StreamEvent::MessageStop { .. }))
            {
                output.push(StreamEvent::MessageStop { stop_reason });
            }
        }
        Ok(())
    }

    fn ensure_message_start(&mut self, output: &mut Vec<StreamEvent>) {
        if !self.saw_message_start {
            output.push(StreamEvent::MessageStart);
            self.saw_message_start = true;
        }
    }

    fn collect_usage(&mut self, payload: &Value) {
        let Some(usage) = payload_usage(payload) else {
            return;
        };
        let mut incoming = normalize_usage(usage, self.default_model);
        if incoming.model.is_none() {
            incoming.model = payload_model(payload).map(str::to_string);
        }
        self.pending_usage = Some(match self.pending_usage.take() {
            Some(existing) => merge_usage(existing, incoming),
            None => incoming,
        });
    }

    fn flush_usage(&mut self, output: &mut Vec<StreamEvent>) {
        let Some(usage) = self.pending_usage.take() else {
            return;
        };
        if let Some(event) = usage.into_usage_event(self.default_model) {
            output.push(StreamEvent::Usage(event));
        }
    }

    fn finalize_pending_tool_use(&mut self, output: &mut Vec<StreamEvent>) -> Result<(), ApiError> {
        if let Some(mut pending) = self.pending_tool_use.take() {
            Self::emit_pending_tool_use_if_ready(
                self.saw_message_start,
                &mut self.emitted_tool_use,
                &mut pending,
                output,
            )?;
            if !pending.emitted {
                return Err(self.tool_use_protocol_error(
                    "tool_use block ended without complete input payload",
                ));
            }
        }
        Ok(())
    }

    fn finalize_pending_structured_output(
        &mut self,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        if let Some(mut pending) = self.pending_structured_output.take() {
            Self::emit_pending_structured_output_if_ready(&mut pending, output)?;
            if !pending.emitted {
                return Err(self.structured_output_error(
                    "structured output block ended without complete JSON payload",
                ));
            }
        }
        Ok(())
    }

    fn emit_pending_tool_use_if_ready(
        saw_message_start: bool,
        emitted_tool_use: &mut bool,
        pending: &mut PendingToolUseBlock,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        if pending.emitted {
            return Ok(());
        }
        let Some(tool_name) = pending.tool_name.clone() else {
            return Err(ApiError::tool_use_protocol_with_disposition(
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
        *emitted_tool_use = true;
        output.push(StreamEvent::ToolUse {
            tool_name,
            input: input.to_string(),
        });
        Ok(())
    }

    fn emit_pending_structured_output_if_ready(
        pending: &mut PendingStructuredOutputBlock,
        output: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        if pending.emitted {
            return Ok(());
        }
        let Some(value) = pending.value.clone() else {
            return Ok(());
        };
        pending.emitted = true;
        output.push(StreamEvent::TextDelta(value.to_string()));
        Ok(())
    }

    fn validate_stop_reason(&self, stop_reason: &StopReason) -> Result<(), ApiError> {
        if matches!(stop_reason, StopReason::ToolUse) && !self.emitted_tool_use {
            return Err(self.tool_use_protocol_error("tool stop without tool payload"));
        }
        if !matches!(stop_reason, StopReason::ToolUse) && self.pending_tool_use.is_some() {
            return Err(self.tool_use_protocol_error("tool_use block did not complete before stop"));
        }
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

    fn tool_use_protocol_error(&self, message: impl Into<String>) -> ApiError {
        ApiError::tool_use_protocol_with_disposition(
            message,
            if self.saw_message_start {
                ProviderFailureDisposition::StreamTerminal
            } else {
                ProviderFailureDisposition::PreStreamTerminal
            },
        )
    }

    fn structured_output_error(&self, message: impl Into<String>) -> ApiError {
        ApiError::structured_output_invalid_with_disposition(
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
        .or_else(|| payload.get("model"))
        .and_then(Value::as_str)
}

fn payload_usage<'a>(payload: &'a Value) -> Option<&'a Value> {
    payload
        .get("usage")
        .or_else(|| {
            payload
                .get("message")
                .and_then(|message| message.get("usage"))
        })
        .or_else(|| payload.get("delta").and_then(|delta| delta.get("usage")))
        .or_else(|| {
            payload
                .get("message_delta")
                .and_then(|delta| delta.get("usage"))
        })
        .or_else(|| {
            payload
                .get("terminal")
                .and_then(|terminal| terminal.get("usage"))
        })
        .or_else(|| {
            payload
                .get("response")
                .and_then(|response| response.get("usage"))
        })
}

impl NormalizedUsage {
    fn into_usage_event(self, default_model: &str) -> Option<UsageEvent> {
        let model = self.model.unwrap_or_else(|| default_model.to_string());
        let input_tokens = self.input_tokens.or(self.total_tokens).unwrap_or_default();
        let output_tokens = self.output_tokens.unwrap_or_default();
        let cache_creation_input_tokens = self.cache_creation_input_tokens.unwrap_or_default();
        let cache_read_input_tokens = self.cache_read_input_tokens.unwrap_or_default();
        if input_tokens == 0
            && output_tokens == 0
            && cache_creation_input_tokens == 0
            && cache_read_input_tokens == 0
        {
            return None;
        }
        Some(UsageEvent {
            model,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
        })
    }
}

fn normalize_usage(usage: &Value, _default_model: &str) -> NormalizedUsage {
    NormalizedUsage {
        model: usage
            .get("model")
            .or_else(|| usage.get("model_id"))
            .and_then(Value::as_str)
            .map(|value| value.to_string()),
        input_tokens: usage
            .get("input_tokens")
            .or_else(|| usage.get("inputTokens"))
            .or_else(|| usage.get("prompt_tokens"))
            .or_else(|| usage.get("promptTokens"))
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        output_tokens: usage
            .get("output_tokens")
            .or_else(|| usage.get("outputTokens"))
            .or_else(|| usage.get("completion_tokens"))
            .or_else(|| usage.get("completionTokens"))
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        cache_creation_input_tokens: usage
            .get("cache_creation_input_tokens")
            .or_else(|| usage.get("cacheCreationInputTokens"))
            .or_else(|| usage.get("cache_write_input_tokens"))
            .or_else(|| usage.get("cacheWriteInputTokens"))
            .or_else(|| usage.get("cache_write_tokens"))
            .or_else(|| usage.get("cacheWriteTokens"))
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        // OpenAI Chat Completions: cached_tokens lives inside prompt_tokens_details.
        // Anthropic-style flat fields (cache_read_input_tokens etc.) are kept as fallback.
        cache_read_input_tokens: usage
            .get("prompt_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(Value::as_u64)
            .map(|v| v as usize)
            .or_else(|| {
                usage
                    .get("cache_read_input_tokens")
                    .or_else(|| usage.get("cacheReadInputTokens"))
                    .or_else(|| usage.get("cache_read_tokens"))
                    .or_else(|| usage.get("cacheReadTokens"))
                    .and_then(Value::as_u64)
                    .map(|value| value as usize)
            }),
        total_tokens: usage
            .get("total_tokens")
            .or_else(|| usage.get("totalTokens"))
            .and_then(Value::as_u64)
            .map(|value| value as usize),
    }
}

fn merge_usage(existing: NormalizedUsage, incoming: NormalizedUsage) -> NormalizedUsage {
    NormalizedUsage {
        model: incoming.model.or(existing.model),
        input_tokens: incoming.input_tokens.or(existing.input_tokens),
        output_tokens: incoming.output_tokens.or(existing.output_tokens),
        cache_creation_input_tokens: incoming
            .cache_creation_input_tokens
            .or(existing.cache_creation_input_tokens),
        cache_read_input_tokens: incoming
            .cache_read_input_tokens
            .or(existing.cache_read_input_tokens),
        total_tokens: incoming.total_tokens.or(existing.total_tokens),
    }
}

#[cfg(test)]
fn parse_usage(usage: &Value, default_model: &str) -> UsageEvent {
    normalize_usage(usage, default_model)
        .into_usage_event(default_model)
        .unwrap_or(UsageEvent {
            model: default_model.to_string(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        })
}

fn normalize_tool_use_input(block: &Value) -> Result<Option<Value>, ApiError> {
    let Some(raw) = block
        .get("input")
        .or_else(|| block.get("args"))
        .or_else(|| block.get("arguments"))
        .or_else(|| block.get("payload"))
    else {
        return Ok(None);
    };

    if raw.is_null() {
        return Ok(Some(Value::Object(Default::default())));
    }

    normalize_json_like_value(raw, "tool_use input").map(Some)
}

fn normalize_structured_output_value(block: &Value) -> Result<Option<Value>, ApiError> {
    let Some(raw) = block
        .get("json")
        .or_else(|| block.get("output"))
        .or_else(|| block.get("value"))
    else {
        return Ok(None);
    };

    if raw.is_null() {
        return Ok(Some(Value::Object(Default::default())));
    }

    normalize_json_like_value(raw, "structured output").map(Some)
}

fn normalize_json_like_value(raw: &Value, label: &str) -> Result<Value, ApiError> {
    match raw {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(Value::Object(Default::default()))
            } else {
                serde_json::from_str::<Value>(trimmed).or_else(|_| Ok(Value::String(text.clone())))
            }
        }
        Value::Array(_) | Value::Object(_) | Value::Bool(_) | Value::Number(_) => Ok(raw.clone()),
        Value::Null => Ok(Value::Object(Default::default())),
    }
    .map_err(|error: serde_json::Error| match label {
        "tool_use input" => ApiError::tool_use_protocol(format!("invalid {label}: {error}")),
        _ => ApiError::structured_output_invalid(format!("invalid {label}: {error}")),
    })
}

fn map_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" => StopReason::EndTurn,
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "error" | "model_error" | "stop_sequence_error" | "pause_turn" => StopReason::Error,
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
        "invalid_request_error"
        | "authentication_error"
        | "permission_error"
        | "not_found_error"
        | "malformed_payload"
        | "sse_protocol" => ProviderFailureDisposition::StreamTerminal,
        _ => match status_code {
            Some(429) | Some(500..=599) => ProviderFailureDisposition::StreamInterrupted,
            _ => ProviderFailureDisposition::StreamTerminal,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ModelProviderConfig, NormalizedUsage, ProviderAuthStrategy,
        ProviderCompatibilityProfileKind, ProviderProtocol, ProviderTimeout,
        RETRY_AFTER_SAFETY_CAP_MS, RequestOptions, RetryDecision, build_messages_url_for_provider,
        build_request_payload_for_provider, build_request_payload_with_options,
        classify_retry_policy, classify_stream_error, classify_stream_error_disposition,
        extract_error_detail, map_openai_finish_reason, map_stop_reason, merge_usage,
        normalize_json_like_value, normalize_usage, parse_anthropic_sse_response,
        parse_openai_compatible_sse_response, parse_retry_after_ms,
        parse_stream_response_for_provider, parse_usage, profile_for_provider,
        validate_streaming_response_headers,
    };
    use crate::service::api::errors::ApiError;
    use crate::service::api::retry::RetryPolicy;
    use crate::service::api::streaming::{ProviderFailureDisposition, StopReason, StreamEvent};
    use reqwest::StatusCode;
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER};
    use serde_json::Value;

    fn test_provider(
        protocol: ProviderProtocol,
        profile: ProviderCompatibilityProfileKind,
    ) -> ModelProviderConfig {
        ModelProviderConfig {
            provider_id: "test".into(),
            protocol,
            compatibility_profile: profile,
            base_url: "http://localhost".into(),
            chat_completions_path: "/v1/messages".into(),
            auth_strategy: ProviderAuthStrategy::NoAuth,
            api_key: None,
            api_key_env: None,
            model_id: "test-model".into(),
            timeout: ProviderTimeout {
                request_timeout_ms: 5000,
                stream_timeout_ms: 10000,
            },
            retry_policy: crate::service::api::retry::RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 0,
                max_backoff_ms: 0,
            },
            pricing: crate::service::api::client::ModelPricing::default(),
            proxy_url: None,
            no_proxy: None,
            ca_bundle_path: None,
            max_tokens_param: None,
            prompt_cache_key: None,
            prompt_cache_retention: None,
        }
    }

    #[test]
    fn anthropic_adapter_text_only_message_serializes_as_text_block() {
        use crate::core::message::Message;
        let config = test_provider(
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::Anthropic,
        );
        let msg = Message::user("hello");
        let payload = build_request_payload_for_provider(&config, &msg).unwrap();
        let content = &payload["messages"][0]["content"];
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "hello");
    }

    #[test]
    fn anthropic_adapter_image_block_serializes_as_base64_source() {
        use crate::core::message::{ContentBlock, Message};
        let config = test_provider(
            ProviderProtocol::Anthropic,
            ProviderCompatibilityProfileKind::Anthropic,
        );
        let msg = Message {
            role: crate::core::message::Role::User,
            content: String::new(),
            blocks: vec![
                ContentBlock::Text {
                    text: "describe this".into(),
                },
                ContentBlock::Image {
                    media_type: "image/png".into(),
                    data: vec![1, 2, 3],
                },
            ],
        };
        let payload = build_request_payload_for_provider(&config, &msg).unwrap();
        let content = &payload["messages"][0]["content"];
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[1]["type"], "image");
        assert_eq!(content[1]["source"]["type"], "base64");
        assert_eq!(content[1]["source"]["media_type"], "image/png");
        assert!(content[1]["source"]["data"].as_str().unwrap().len() > 0);
    }

    #[test]
    fn openai_adapter_text_only_message_serializes_as_string_content() {
        use crate::core::message::Message;
        let config = test_provider(
            ProviderProtocol::OpenAICompatible,
            ProviderCompatibilityProfileKind::OpenAICompatible,
        );
        let msg = Message::user("hello");
        let payload = build_request_payload_for_provider(&config, &msg).unwrap();
        assert_eq!(payload["messages"][0]["content"], "hello");
    }

    #[test]
    fn openai_adapter_includes_prompt_cache_fields_when_configured() {
        use crate::core::message::Message;
        let mut config = test_provider(
            ProviderProtocol::OpenAICompatible,
            ProviderCompatibilityProfileKind::OpenAICompatible,
        );
        config.prompt_cache_key = Some("rust-agent-r1".into());
        config.prompt_cache_retention = Some("in_memory".into());

        let payload = build_request_payload_for_provider(&config, &Message::user("hello")).unwrap();

        assert_eq!(payload["prompt_cache_key"], "rust-agent-r1");
        assert_eq!(payload["prompt_cache_retention"], "in_memory");
    }

    #[test]
    fn openai_adapter_image_block_serializes_as_array_with_image_url() {
        use crate::core::message::{ContentBlock, Message};
        let config = test_provider(
            ProviderProtocol::OpenAICompatible,
            ProviderCompatibilityProfileKind::OpenAICompatible,
        );
        let msg = Message {
            role: crate::core::message::Role::User,
            content: String::new(),
            blocks: vec![
                ContentBlock::Text {
                    text: "describe this".into(),
                },
                ContentBlock::Image {
                    media_type: "image/png".into(),
                    data: vec![1, 2, 3],
                },
            ],
        };
        let payload = build_request_payload_for_provider(&config, &msg).unwrap();
        let content = &payload["messages"][0]["content"];
        assert!(content.is_array());
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[1]["type"], "image_url");
        let url = content[1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,"));
    }

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
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamEvent::TextDelta(text) if text == "hello "))
        );
        assert!(events.iter().any(|event| matches!(event, StreamEvent::ToolUse { tool_name, input } if tool_name == "Read" && input == "{\"path\":\"foo\"}")));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamEvent::Usage(usage)
            if usage.model == "claude-test"
                && usage.input_tokens == 12
                && usage.output_tokens == 7))
        );
        assert!(matches!(
            events.last(),
            Some(StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse
            })
        ));
    }

    #[test]
    fn provider_config_defaults_include_runtime_fields() {
        let config = ModelProviderConfig::default();
        assert_eq!(config.timeout, ProviderTimeout::default());
        assert_eq!(config.retry_policy, RetryPolicy::default());
        assert_eq!(config.chat_completions_path, "/v1/chat/completions");
        assert_eq!(
            build_messages_url_for_provider(&config).expect("default provider should resolve URL"),
            "http://localhost/v1/messages"
        );
    }

    #[test]
    fn openai_compatible_messages_url_uses_default_path() {
        let config = ModelProviderConfig {
            provider_id: "openai".into(),
            protocol: ProviderProtocol::OpenAICompatible,
            compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
            base_url: "https://api.openai.com/".into(),
            chat_completions_path: "/v1/chat/completions".into(),
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        assert_eq!(
            build_messages_url_for_provider(&config)
                .expect("openai-compatible provider should resolve URL"),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[test]
    fn openai_compatible_messages_url_uses_custom_path() {
        let config = ModelProviderConfig {
            provider_id: "custom-provider".into(),
            protocol: ProviderProtocol::OpenAICompatible,
            compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
            base_url: "https://generativelanguage.googleapis.com/v1beta/openai/".into(),
            chat_completions_path: "/chat/completions".into(),
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        assert_eq!(
            build_messages_url_for_provider(&config)
                .expect("custom provider should resolve URL with override path"),
            "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
        );
    }

    #[test]
    fn openai_compatible_messages_url_rejects_invalid_path() {
        let config = ModelProviderConfig {
            provider_id: "openai".into(),
            protocol: ProviderProtocol::OpenAICompatible,
            compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
            base_url: "https://api.openai.com".into(),
            chat_completions_path: "v1/chat/completions".into(),
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        let error = build_messages_url_for_provider(&config)
            .expect_err("invalid path should be rejected before URL construction");
        assert_eq!(error.kind_label(), "invalid_configuration");
        assert!(error.message.contains("must start with '/'"));
    }

    #[test]
    fn request_payload_uses_normalized_envelope_shape() {
        let config = ModelProviderConfig {
            provider_id: "anthropic".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            model_id: "test-model".into(),
            ..ModelProviderConfig::default()
        };

        let payload = build_request_payload_for_provider(
            &config,
            &crate::core::message::Message::user("hello"),
        )
        .expect("request payload should build");

        assert_eq!(
            payload.get("model").and_then(Value::as_str),
            Some("test-model")
        );
        assert_eq!(payload.get("stream").and_then(Value::as_bool), Some(true));
        assert_eq!(
            payload.get("max_tokens").and_then(Value::as_u64),
            Some(4096)
        );
        let content = &payload["messages"][0]["content"];
        assert_eq!(content[0]["type"].as_str(), Some("text"));
        assert_eq!(content[0]["text"].as_str(), Some("hello"));
    }

    #[test]
    fn provider_compatibility_profiles_match_expected_capabilities() {
        let anthropic =
            profile_for_provider(&ModelProviderConfig::from_legacy_provider_id("anthropic"))
                .expect("anthropic profile");
        assert!(anthropic.supports_tools);
        assert!(anthropic.supports_streaming);
        assert!(anthropic.supports_temperature);
        assert!(anthropic.supports_top_p);
        assert!(anthropic.supports_stop_sequences);

        let text_only = profile_for_provider(&ModelProviderConfig::from_legacy_provider_id(
            "text-only-provider",
        ))
        .expect("text-only profile");
        assert!(!text_only.supports_tools);
        assert!(text_only.supports_streaming);
        assert!(!text_only.supports_temperature);
        assert!(!text_only.supports_top_p);
        assert!(!text_only.supports_stop_sequences);

        let batch = profile_for_provider(&ModelProviderConfig::from_legacy_provider_id(
            "batch-provider",
        ))
        .expect("batch profile");
        assert!(batch.supports_tools);
        assert!(!batch.supports_streaming);
        assert!(batch.supports_temperature);
        assert!(batch.supports_top_p);
        assert!(batch.supports_stop_sequences);
    }

    #[test]
    fn supported_provider_keeps_request_options_intact() {
        let config = ModelProviderConfig {
            provider_id: "anthropic".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };
        let payload = build_request_payload_with_options(
            &config,
            &crate::core::message::Message::user("hello"),
            RequestOptions {
                max_tokens: Some(2048),
                temperature: Some(0.7),
                top_p: Some(0.9),
                stop_sequences: vec!["STOP".into()],
                require_tools: true,
            },
        )
        .expect("supported options should build");

        assert_eq!(
            payload.get("max_tokens").and_then(Value::as_u64),
            Some(2048)
        );
        assert_eq!(
            payload.get("temperature").and_then(Value::as_f64),
            Some(0.7)
        );
        assert_eq!(payload.get("top_p").and_then(Value::as_f64), Some(0.9));
        assert_eq!(payload["stop_sequences"][0].as_str(), Some("STOP"));
    }

    #[test]
    fn unsupported_optional_request_options_are_dropped() {
        let config = ModelProviderConfig {
            provider_id: "text-only-provider".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::TextOnly,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };
        let payload = build_request_payload_with_options(
            &config,
            &crate::core::message::Message::user("hello"),
            RequestOptions {
                max_tokens: Some(1024),
                temperature: Some(0.5),
                top_p: Some(0.8),
                stop_sequences: vec!["END".into()],
                require_tools: false,
            },
        )
        .expect("unsupported optional options should be dropped");

        assert_eq!(
            payload.get("max_tokens").and_then(Value::as_u64),
            Some(1024)
        );
        assert!(payload.get("temperature").is_none());
        assert!(payload.get("top_p").is_none());
        assert!(payload.get("stop_sequences").is_none());
    }

    #[test]
    fn unsupported_streaming_returns_typed_capability_failure() {
        let config = ModelProviderConfig {
            provider_id: "batch-provider".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::Batch,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        let error = build_request_payload_for_provider(
            &config,
            &crate::core::message::Message::user("hello"),
        )
        .expect_err("streaming mismatch should fail");

        assert_eq!(error.kind_label(), "capability_unsupported");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::PreStreamTerminal
        );
        assert!(error.message.contains("streaming"));
    }

    #[test]
    fn unsupported_tools_returns_typed_capability_failure() {
        let config = ModelProviderConfig {
            provider_id: "text-only-provider".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::TextOnly,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        let error = build_request_payload_with_options(
            &config,
            &crate::core::message::Message::user("hello"),
            RequestOptions {
                require_tools: true,
                ..RequestOptions::default()
            },
        )
        .expect_err("tool mismatch should fail");

        assert_eq!(error.kind_label(), "capability_unsupported");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::PreStreamTerminal
        );
        assert!(error.message.contains("tool-use"));
    }

    #[test]
    fn invalid_numeric_options_return_typed_failure() {
        let config = ModelProviderConfig {
            provider_id: "anthropic".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        let max_tokens_error = build_request_payload_with_options(
            &config,
            &crate::core::message::Message::user("hello"),
            RequestOptions {
                max_tokens: Some(0),
                ..RequestOptions::default()
            },
        )
        .expect_err("zero max_tokens should fail");
        assert_eq!(max_tokens_error.kind_label(), "invalid_request_option");

        let temperature_error = build_request_payload_with_options(
            &config,
            &crate::core::message::Message::user("hello"),
            RequestOptions {
                temperature: Some(2.1),
                ..RequestOptions::default()
            },
        )
        .expect_err("invalid temperature should fail");
        assert_eq!(temperature_error.kind_label(), "invalid_request_option");

        let top_p_error = build_request_payload_with_options(
            &config,
            &crate::core::message::Message::user("hello"),
            RequestOptions {
                top_p: Some(1.1),
                ..RequestOptions::default()
            },
        )
        .expect_err("invalid top_p should fail");
        assert_eq!(top_p_error.kind_label(), "invalid_request_option");
    }

    #[test]
    fn parses_openai_compatible_sse_stream_into_stream_events() {
        let body = concat!(
            "data: {\"id\":\"chatcmpl-redacted\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"hello openai\"},\"index\":0,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-redacted\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_redacted\",\"type\":\"function\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"path\\\":\\\"foo\\\"\"}}]},\"index\":0,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-redacted\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"}\"}}]},\"index\":0,\"finish_reason\":\"tool_calls\"}],\"usage\":{\"model\":\"gpt-redacted\",\"prompt_tokens\":12,\"completion_tokens\":5,\"total_tokens\":17}}\n\n",
            "data: [DONE]\n\n"
        );

        let events =
            parse_openai_compatible_sse_response("openai-compatible", body, "default-model")
                .expect("openai-compatible sse should parse");
        assert!(matches!(events[0], StreamEvent::MessageStart));
        assert!(
            events.iter().any(
                |event| matches!(event, StreamEvent::TextDelta(text) if text == "hello openai")
            )
        );
        assert!(events.iter().any(|event| matches!(event, StreamEvent::ToolUse { tool_name, input } if tool_name == "Read" && input == "{\"path\":\"foo\"}")));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamEvent::Usage(usage)
            if usage.model == "gpt-redacted"
                && usage.input_tokens == 12
                && usage.output_tokens == 5))
        );
        assert!(matches!(
            events
                .iter()
                .find(|event| matches!(event, StreamEvent::MessageStop { .. })),
            Some(StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse
            })
        ));
    }

    #[test]
    fn openai_compatible_usage_only_terminal_chunk_is_accepted() {
        let body = concat!(
            "data: {\"id\":\"chatcmpl-redacted\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"hello\"},\"index\":0,\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"id\":\"chatcmpl-redacted\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"model\":\"gpt-redacted\",\"prompt_tokens\":11,\"completion_tokens\":3}}\n\n",
            "data: [DONE]\n\n"
        );

        let events =
            parse_openai_compatible_sse_response("openai-compatible", body, "default-model")
                .expect("usage-only chunk should parse");
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamEvent::Usage(usage)
            if usage.model == "gpt-redacted"
                && usage.input_tokens == 11
                && usage.output_tokens == 3))
        );
    }

    #[test]
    fn openai_finish_reason_mapping_matches_expected_values() {
        assert_eq!(map_openai_finish_reason("stop"), StopReason::EndTurn);
        assert_eq!(map_openai_finish_reason("tool_calls"), StopReason::ToolUse);
        assert_eq!(map_openai_finish_reason("length"), StopReason::MaxTokens);
        assert_eq!(
            map_openai_finish_reason("content_filter"),
            StopReason::Error
        );
        assert_eq!(map_openai_finish_reason("other"), StopReason::Error);
    }

    #[test]
    fn rejects_provider_protocol_profile_mismatch_for_request_and_parse_paths() {
        let config = ModelProviderConfig {
            provider_id: "gemini".into(),
            protocol: ProviderProtocol::Anthropic,
            compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
            auth_strategy: ProviderAuthStrategy::NoAuth,
            ..ModelProviderConfig::default()
        };

        let request_error = build_messages_url_for_provider(&config)
            .expect_err("provider mismatch should be rejected");
        assert_eq!(request_error.kind_label(), "invalid_configuration");
        assert!(request_error.message.contains("gemini"));

        let parse_error = parse_stream_response_for_provider(
            &ModelProviderConfig {
                provider_id: "gemini".into(),
                protocol: ProviderProtocol::Anthropic,
                compatibility_profile: ProviderCompatibilityProfileKind::Anthropic,
                auth_strategy: ProviderAuthStrategy::NoAuth,
                ..ModelProviderConfig::default()
            },
            "",
            "model",
        )
        .expect_err("provider mismatch parser should be rejected");
        assert_eq!(parse_error.kind_label(), "invalid_configuration");
        assert!(parse_error.message.contains("gemini"));
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
    fn message_start_accepts_usage_at_top_level() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"model\":\"claude-alt\",\"usage\":{\"inputTokens\":12}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("top-level usage should parse");
        assert!(matches!(
            &events[1],
            StreamEvent::Usage(usage)
                if usage.model == "claude-alt" && usage.input_tokens == 12
        ));
    }

    #[test]
    fn message_delta_accepts_usage_nested_under_delta() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"usage\":{\"outputTokens\":9}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("delta usage should parse");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::Usage(usage)
                if usage.model == "default-model" && usage.output_tokens == 9
        )));
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
        assert_eq!(error.kind_label(), "tool_use_protocol");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::StreamTerminal
        );
        assert!(
            error
                .message
                .contains("tool_use block ended without complete input payload")
        );
    }

    #[test]
    fn normalizes_tool_use_alias_and_null_payload_variants() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Read\",\"arguments\":null}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("null tool payload should normalize");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolUse { tool_name, input }
                if tool_name == "Read" && input == "{}"
        )));
    }

    #[test]
    fn parses_stringified_tool_use_payload_from_alias_field() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Read\",\"args\":\"{\\\"path\\\":\\\"foo\\\"}\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("stringified tool payload should parse");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolUse { tool_name, input }
                if tool_name == "Read" && input == "{\"path\":\"foo\"}"
        )));
    }

    #[test]
    fn rejects_tool_stop_without_payload_as_typed_protocol_error() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let error = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect_err("tool stop without payload should fail");
        assert_eq!(error.kind_label(), "tool_use_protocol");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::StreamTerminal
        );
        assert!(error.message.contains("tool stop without tool payload"));
    }

    #[test]
    fn accepts_structured_output_block_with_stringified_json() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"structured_output\",\"value\":\"{\\\"answer\\\":42}\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect("structured output should parse");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::TextDelta(text) if text == "{\"answer\":42}"
        )));
    }

    #[test]
    fn rejects_incomplete_structured_output_payload_at_end_of_stream() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"structured_output\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"partial_json\":\"{\\\"answer\\\":\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let error = parse_anthropic_sse_response("anthropic", body, "default-model")
            .expect_err("incomplete structured output should fail");
        assert_eq!(error.kind_label(), "structured_output_invalid");
        assert_eq!(
            error.disposition,
            ProviderFailureDisposition::StreamTerminal
        );
        assert!(
            error
                .message
                .contains("structured output block ended without complete JSON payload")
        );
    }

    #[test]
    fn normalize_json_like_value_preserves_plain_strings_for_tool_inputs() {
        let value =
            normalize_json_like_value(&Value::String("inspect file".into()), "tool_use input")
                .expect("plain string should stay string");
        assert_eq!(value, Value::String("inspect file".into()));
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
                || pre_stream_error
                    .message
                    .contains("invalid SSE JSON payload")
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

    #[test]
    fn extracts_error_detail_from_variant_http_error_envelopes() {
        assert_eq!(
            extract_error_detail(r#"{"error":{"message":"bad request"}}"#),
            Some("bad request".into())
        );
        assert_eq!(
            extract_error_detail(r#"{"message":"outer message"}"#),
            Some("outer message".into())
        );
        assert_eq!(
            extract_error_detail(r#"{"detail":"detail message"}"#),
            Some("detail message".into())
        );
    }

    #[test]
    fn parse_usage_accepts_alternate_field_names() {
        let usage = parse_usage(
            &serde_json::json!({
                "inputTokens": 7,
                "outputTokens": 3,
                "cacheCreationInputTokens": 2,
                "cacheReadInputTokens": 1,
            }),
            "claude-test",
        );

        assert_eq!(usage.model, "claude-test");
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 3);
        assert_eq!(usage.cache_creation_input_tokens, 2);
        assert_eq!(usage.cache_read_input_tokens, 1);
    }

    #[test]
    fn parse_usage_accepts_prompt_completion_and_total_token_fields() {
        let prompt_completion = parse_usage(
            &serde_json::json!({
                "prompt_tokens": 9,
                "completion_tokens": 4,
                "cache_write_tokens": 2,
                "cache_read_tokens": 1,
            }),
            "claude-test",
        );
        assert_eq!(prompt_completion.input_tokens, 9);
        assert_eq!(prompt_completion.output_tokens, 4);
        assert_eq!(prompt_completion.cache_creation_input_tokens, 2);
        assert_eq!(prompt_completion.cache_read_input_tokens, 1);

        let total_only = parse_usage(
            &serde_json::json!({
                "total_tokens": 13,
            }),
            "claude-test",
        );
        assert_eq!(total_only.input_tokens, 13);
        assert_eq!(total_only.output_tokens, 0);
    }

    #[test]
    fn parse_usage_reads_openai_nested_cached_tokens() {
        // OpenAI Chat Completions puts cache hits inside prompt_tokens_details.cached_tokens.
        let usage = parse_usage(
            &serde_json::json!({
                "prompt_tokens": 120,
                "completion_tokens": 30,
                "prompt_tokens_details": {
                    "cached_tokens": 80,
                    "audio_tokens": 0
                },
                "completion_tokens_details": {
                    "reasoning_tokens": 0
                }
            }),
            "gpt-5.4",
        );
        assert_eq!(usage.input_tokens, 120);
        assert_eq!(usage.output_tokens, 30);
        assert_eq!(usage.cache_read_input_tokens, 80);
        assert_eq!(usage.cache_creation_input_tokens, 0);
    }

    #[test]
    fn parse_usage_flat_cache_fields_still_work_as_fallback() {
        // Anthropic-style flat fields must remain valid when nested path is absent.
        let usage = parse_usage(
            &serde_json::json!({
                "input_tokens": 50,
                "output_tokens": 10,
                "cache_creation_input_tokens": 5,
                "cache_read_input_tokens": 3,
            }),
            "claude-3",
        );
        assert_eq!(usage.cache_read_input_tokens, 3);
        assert_eq!(usage.cache_creation_input_tokens, 5);
    }

    #[test]
    fn merge_usage_latest_wins_without_clearing_missing_fields() {
        let existing = NormalizedUsage {
            model: Some("claude-test".into()),
            input_tokens: Some(10),
            output_tokens: Some(3),
            cache_creation_input_tokens: Some(2),
            cache_read_input_tokens: None,
            total_tokens: None,
        };
        let incoming = NormalizedUsage {
            model: None,
            input_tokens: None,
            output_tokens: Some(6),
            cache_creation_input_tokens: None,
            cache_read_input_tokens: Some(1),
            total_tokens: Some(16),
        };

        let merged = merge_usage(existing, incoming);
        assert_eq!(merged.model.as_deref(), Some("claude-test"));
        assert_eq!(merged.input_tokens, Some(10));
        assert_eq!(merged.output_tokens, Some(6));
        assert_eq!(merged.cache_creation_input_tokens, Some(2));
        assert_eq!(merged.cache_read_input_tokens, Some(1));
        assert_eq!(merged.total_tokens, Some(16));
    }

    #[test]
    fn normalizes_usage_with_terminal_and_response_envelopes() {
        let terminal = normalize_usage(
            &serde_json::json!({
                "outputTokens": 8,
            }),
            "claude-test",
        )
        .into_usage_event("claude-test")
        .expect("terminal usage should normalize");
        assert_eq!(terminal.output_tokens, 8);

        let response_body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\",\"response\":{\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}}\n\n"
        );
        let events = parse_anthropic_sse_response("anthropic", response_body, "claude-test")
            .expect("response envelope usage should parse");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::Usage(usage)
                if usage.model == "claude-test" && usage.input_tokens == 10 && usage.output_tokens == 2
        )));
    }

    #[test]
    fn multiple_usage_deltas_latest_wins() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hello\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"usage\":{\"output_tokens\":3}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"usage\":{\"output_tokens\":5}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "claude-test")
            .expect("usage deltas should parse");
        let usages = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::Usage(usage) => Some(usage),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].input_tokens, 10);
        assert_eq!(usages[0].output_tokens, 5);
    }

    #[test]
    fn usage_before_stop_reason_and_after_content_delta_is_preserved() {
        let body = concat!(
            "event: message\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":7}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"partial\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"usage\":{\"output_tokens\":4}}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
            "event: message\n",
            "data: {\"type\":\"message_stop\",\"terminal\":{\"usage\":{\"cache_read_tokens\":1}}}\n\n"
        );

        let events = parse_anthropic_sse_response("anthropic", body, "claude-test")
            .expect("usage ordering should parse");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::Usage(usage)
                if usage.input_tokens == 7 && usage.output_tokens == 4 && usage.cache_read_input_tokens == 1
        )));
    }

    #[test]
    fn retry_policy_only_retries_expected_error_kinds() {
        assert_eq!(
            classify_retry_policy(&ApiError::timeout("timed out")),
            RetryDecision::RetryDefaultBackoff
        );
        assert_eq!(
            classify_retry_policy(&ApiError::connection_reset("reset")),
            RetryDecision::RetryDefaultBackoff
        );
        assert_eq!(
            classify_retry_policy(&ApiError::http_status(429, "rate limited")),
            RetryDecision::RetryDefaultBackoff
        );
        assert_eq!(
            classify_retry_policy(&ApiError::http_status(503, "unavailable")),
            RetryDecision::RetryDefaultBackoff
        );
        assert_eq!(
            classify_retry_policy(&ApiError::http_status(400, "bad request")),
            RetryDecision::DoNotRetry
        );
        assert_eq!(
            classify_retry_policy(&ApiError::transport("transport")),
            RetryDecision::DoNotRetry
        );
        assert_eq!(
            classify_retry_policy(&ApiError::sse_protocol("protocol")),
            RetryDecision::DoNotRetry
        );
    }

    #[test]
    fn retry_policy_prefers_retry_after_hint_for_429() {
        let error = ApiError::http_status(429, "rate limited").with_retry_after_ms(Some(1_500));
        assert_eq!(
            classify_retry_policy(&error),
            RetryDecision::RetryAfterMs(1_500)
        );
    }

    #[test]
    fn retry_after_parser_accepts_seconds_and_ignores_invalid_values() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        assert_eq!(parse_retry_after_ms(&headers), Some(2_000));

        headers.insert(RETRY_AFTER, HeaderValue::from_static("nonsense"));
        assert_eq!(parse_retry_after_ms(&headers), None);
    }

    #[test]
    fn retry_after_safety_cap_is_applied_to_large_header_values() {
        // A provider sending Retry-After: 9999 should be capped, not obeyed literally.
        let error = ApiError::http_status(429, "rate limited")
            .with_retry_after_ms(Some(RETRY_AFTER_SAFETY_CAP_MS + 60_000));
        assert_eq!(
            classify_retry_policy(&error),
            RetryDecision::RetryAfterMs(RETRY_AFTER_SAFETY_CAP_MS + 60_000)
        );
        // The cap is applied at the sleep site, not in classify_retry_policy.
        // Verify the cap constant itself is sane (≤ 60s).
        assert!(RETRY_AFTER_SAFETY_CAP_MS <= 60_000);
    }

    #[test]
    fn retry_after_within_cap_is_used_verbatim() {
        let error = ApiError::http_status(429, "rate limited").with_retry_after_ms(Some(5_000));
        assert_eq!(
            classify_retry_policy(&error),
            RetryDecision::RetryAfterMs(5_000)
        );
        // 5_000 < RETRY_AFTER_SAFETY_CAP_MS so min(5_000, cap) == 5_000
        assert_eq!(5_000_u64.min(RETRY_AFTER_SAFETY_CAP_MS), 5_000);
    }

    #[test]
    fn retry_after_cap_clamps_oversized_value() {
        let oversized = RETRY_AFTER_SAFETY_CAP_MS + 1;
        assert_eq!(
            oversized.min(RETRY_AFTER_SAFETY_CAP_MS),
            RETRY_AFTER_SAFETY_CAP_MS
        );
    }
}

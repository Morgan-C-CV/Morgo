/// Opt-in real provider smoke tests.
///
/// These tests hit live provider endpoints and are NOT run in default CI.
/// Gate: set the relevant env var to enable each provider.
///
///   RUST_AGENT_SMOKE_OPENAI_API_KEY=sk-...   cargo test --test integration provider_smoke -- --nocapture
///   RUST_AGENT_SMOKE_GEMINI_API_KEY=AIza...  cargo test --test integration provider_smoke -- --nocapture
///
/// Failure output is structured to distinguish:
///   auth_error       — 401/403 from provider
///   endpoint_error   — connection refused / DNS / wrong path
///   protocol_error   — unexpected response shape (parse failure)
///   model_error      — 404 model not found / quota exceeded
///   unexpected_error — anything else
use rust_agent::core::message::Message;
use rust_agent::service::api::client::{
    ModelPricing, ModelProviderClient, ModelProviderConfig, ProviderAuthStrategy,
    ProviderCompatibilityProfileKind, ProviderProtocol, ProviderTimeout,
};
use rust_agent::service::api::retry::RetryPolicy;
use rust_agent::service::api::streaming::StreamEvent;

const SMOKE_PROMPT: &str = "Reply with exactly: ok";

#[derive(Debug)]
enum SmokeFailureKind {
    AuthError,
    EndpointError,
    ProtocolError,
    ModelError,
    UnexpectedError,
}

impl SmokeFailureKind {
    fn classify(err: &str) -> Self {
        let lower = err.to_lowercase();
        if lower.contains("401")
            || lower.contains("403")
            || lower.contains("unauthorized")
            || lower.contains("forbidden")
            || lower.contains("invalid_api_key")
        {
            Self::AuthError
        } else if lower.contains("connection refused")
            || lower.contains("dns")
            || lower.contains("no route")
            || lower.contains("timed out")
            || lower.contains("connect error")
        {
            Self::EndpointError
        } else if lower.contains("parse")
            || lower.contains("deserializ")
            || lower.contains("unexpected token")
            || lower.contains("invalid json")
        {
            Self::ProtocolError
        } else if lower.contains("404")
            || lower.contains("model not found")
            || lower.contains("quota")
            || lower.contains("rate limit")
        {
            Self::ModelError
        } else {
            Self::UnexpectedError
        }
    }
}

async fn run_smoke(config: ModelProviderConfig, label: &str) {
    let client = ModelProviderClient::from_config(config);
    let message = Message::user(SMOKE_PROMPT);
    let events = client.stream_message(&message).await;

    let mut reply = String::new();
    let mut stop_reason_seen = false;
    let mut error_text: Option<String> = None;

    for event in &events {
        match event {
            StreamEvent::TextDelta(chunk) => reply.push_str(chunk),
            StreamEvent::MessageStop { .. } => stop_reason_seen = true,
            StreamEvent::Error(err) => {
                let msg = format!("{err:?}");
                let kind = SmokeFailureKind::classify(&msg);
                error_text = Some(format!("({kind:?}): {msg}"));
            }
            _ => {}
        }
    }

    if let Some(err) = error_text {
        panic!("[{label}] provider returned error {err}");
    }

    assert!(
        stop_reason_seen,
        "[{label}] stream ended without MessageStop — events: {events:?}"
    );

    let reply_lower = reply.trim().to_lowercase();
    assert!(
        reply_lower.contains("ok"),
        "[{label}] expected reply containing 'ok', got: {reply:?}"
    );

    println!("[{label}] PASS — reply={reply:?}");
}

#[tokio::test]
async fn smoke_openai_direct() {
    let key = match std::env::var("RUST_AGENT_SMOKE_OPENAI_API_KEY") {
        Ok(k) if !k.trim().is_empty() => k,
        _ => {
            println!("[smoke_openai_direct] SKIPPED — RUST_AGENT_SMOKE_OPENAI_API_KEY not set");
            return;
        }
    };

    let config = ModelProviderConfig {
        provider_id: "openai".into(),
        protocol: ProviderProtocol::OpenAICompatible,
        compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
        base_url: "https://api.openai.com".into(),
        chat_completions_path: "/v1/chat/completions".into(),
        auth_strategy: ProviderAuthStrategy::BearerApiKey,
        api_key: Some(key),
        api_key_env: Some("RUST_AGENT_SMOKE_OPENAI_API_KEY".into()),
        model_id: "gpt-4.1-mini".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 30_000,
            stream_timeout_ms: 60_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
        pricing: ModelPricing::default(),
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    };

    run_smoke(config, "smoke_openai_direct").await;
}

#[tokio::test]
async fn smoke_gemini_openai_compatible() {
    let key = match std::env::var("RUST_AGENT_SMOKE_GEMINI_API_KEY") {
        Ok(k) if !k.trim().is_empty() => k,
        _ => {
            println!(
                "[smoke_gemini_openai_compatible] SKIPPED — RUST_AGENT_SMOKE_GEMINI_API_KEY not set"
            );
            return;
        }
    };

    // Gemini via OpenAI-compatible endpoint with custom path
    let config = ModelProviderConfig {
        provider_id: "gemini-openai".into(),
        protocol: ProviderProtocol::OpenAICompatible,
        compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
        base_url: "https://generativelanguage.googleapis.com".into(),
        chat_completions_path: "/v1beta/openai/chat/completions".into(),
        auth_strategy: ProviderAuthStrategy::BearerApiKey,
        api_key: Some(key),
        api_key_env: Some("RUST_AGENT_SMOKE_GEMINI_API_KEY".into()),
        model_id: "gemini-2.0-flash".into(),
        timeout: ProviderTimeout {
            request_timeout_ms: 30_000,
            stream_timeout_ms: 60_000,
        },
        retry_policy: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
        pricing: ModelPricing::default(),
        proxy_url: None,
        no_proxy: None,
        ca_bundle_path: None,
    };

    run_smoke(config, "smoke_gemini_openai_compatible").await;
}

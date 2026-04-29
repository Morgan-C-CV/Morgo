use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout, validate_provider_config,
};
use crate::service::api::retry::RetryPolicy;

#[derive(Debug, Clone, Deserialize)]
pub struct ModelProfilesFile {
    pub active: String,
    pub profiles: BTreeMap<String, ModelProfileSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfileSpec {
    pub provider_id: String,
    pub protocol: String,
    pub compatibility_profile: String,
    pub base_url: String,
    #[serde(default = "default_chat_completions_path")]
    pub chat_completions_path: String,
    pub model: String,
    #[serde(default = "default_auth_strategy")]
    pub auth_strategy: String,
    pub api_key_env: Option<String>,
    pub request_timeout_ms: Option<u64>,
    pub stream_timeout_ms: Option<u64>,
    pub retry_max_attempts: Option<usize>,
    pub retry_initial_backoff_ms: Option<u64>,
    pub retry_max_backoff_ms: Option<u64>,
    pub proxy_url: Option<String>,
    pub no_proxy: Option<String>,
    pub ca_bundle_path: Option<String>,
    #[serde(default)]
    pub max_tokens_param: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedModelProfile {
    pub name: String,
    pub config: ModelProviderConfig,
}

#[derive(Debug, Clone)]
pub struct ModelProfileRegistry {
    pub active: String,
    pub profiles: BTreeMap<String, ModelProfileSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelProfileDisplayView {
    pub name: String,
    pub provider_id: String,
    pub protocol: String,
    pub compatibility_profile: String,
    pub base_url: String,
    pub chat_completions_path: String,
    pub model: String,
    pub auth_strategy: String,
    pub api_key_env: Option<String>,
    pub api_key_env_status: Option<String>,
    pub request_timeout_ms: u64,
    pub stream_timeout_ms: u64,
    pub retry_max_attempts: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

pub fn load_active_model_profile_from_root(
    config_root: &Path,
) -> anyhow::Result<Option<ResolvedModelProfile>> {
    load_model_profiles_registry_from_root(config_root)?
        .map(|registry| resolve_active_model_profile_from_registry(&registry))
        .transpose()
}

pub fn load_model_profiles_registry_from_root(
    config_root: &Path,
) -> anyhow::Result<Option<ModelProfileRegistry>> {
    let path = config_root.join("models.toml");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("invalid_configuration: failed to read {}", path.display()))?;
    parse_model_profiles_registry(&content).map(Some)
}

pub fn parse_model_profiles_registry(content: &str) -> anyhow::Result<ModelProfileRegistry> {
    let file: ModelProfilesFile = toml::from_str(content)
        .map_err(|error| anyhow::anyhow!("invalid_configuration: invalid models.toml: {error}"))?;
    let active = file.active.trim();
    if active.is_empty() {
        bail!("invalid_configuration: models.toml active profile is empty");
    }
    if !file.profiles.contains_key(active) {
        bail!("invalid_configuration: active model profile '{active}' was not found");
    }
    for (name, spec) in &file.profiles {
        let config = spec.to_model_provider_config(name)?;
        validate_provider_config(&config).map_err(|error| anyhow::anyhow!(error.to_string()))?;
    }
    Ok(ModelProfileRegistry {
        active: active.to_string(),
        profiles: file.profiles,
    })
}

pub fn resolve_active_model_profile(content: &str) -> anyhow::Result<ResolvedModelProfile> {
    let registry = parse_model_profiles_registry(content)?;
    resolve_active_model_profile_from_registry(&registry)
}

pub fn resolve_active_model_profile_from_registry(
    registry: &ModelProfileRegistry,
) -> anyhow::Result<ResolvedModelProfile> {
    resolve_model_profile_from_registry(registry, registry.active.as_str()).map_err(|error| {
        if error.to_string().contains("model profile '")
            && error.to_string().contains("' was not found")
        {
            anyhow::anyhow!(
                "invalid_configuration: active model profile '{}' was not found",
                registry.active
            )
        } else {
            error
        }
    })
}

pub fn resolve_model_profile_from_registry(
    registry: &ModelProfileRegistry,
    profile: &str,
) -> anyhow::Result<ResolvedModelProfile> {
    let spec = registry.profiles.get(profile).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid_configuration: model profile '{}' was not found",
            profile
        )
    })?;
    let config = spec.to_model_provider_config(profile)?;
    validate_provider_config(&config).map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(ResolvedModelProfile {
        name: profile.to_string(),
        config,
    })
}

pub fn build_model_profile_display_view(
    name: &str,
    spec: &ModelProfileSpec,
) -> anyhow::Result<ModelProfileDisplayView> {
    let auth_strategy = parse_auth_strategy(&spec.auth_strategy)?;
    let api_key_env = spec
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let api_key_env_status = match auth_strategy {
        ProviderAuthStrategy::BearerApiKey => {
            let env_name = api_key_env.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid_configuration: bearer model profile '{name}' requires api_key_env"
                )
            })?;
            Some(match std::env::var(env_name) {
                Ok(value) if !value.trim().is_empty() => "set".to_string(),
                _ => "unset".to_string(),
            })
        }
        ProviderAuthStrategy::NoAuth => None,
    };

    Ok(ModelProfileDisplayView {
        name: name.to_string(),
        provider_id: spec.provider_id.trim().to_string(),
        protocol: spec.protocol.trim().to_string(),
        compatibility_profile: spec.compatibility_profile.trim().to_string(),
        base_url: spec.base_url.trim().to_string(),
        chat_completions_path: spec.chat_completions_path.trim().to_string(),
        model: spec.model.trim().to_string(),
        auth_strategy: spec.auth_strategy.trim().to_string(),
        api_key_env,
        api_key_env_status,
        request_timeout_ms: spec.request_timeout_ms.unwrap_or(30_000),
        stream_timeout_ms: spec.stream_timeout_ms.unwrap_or(120_000),
        retry_max_attempts: spec.retry_max_attempts.unwrap_or(3),
        retry_initial_backoff_ms: spec.retry_initial_backoff_ms.unwrap_or(200),
        retry_max_backoff_ms: spec.retry_max_backoff_ms.unwrap_or(1_000),
    })
}

impl ModelProfileSpec {
    fn to_model_provider_config(&self, name: &str) -> anyhow::Result<ModelProviderConfig> {
        if self.provider_id.trim().is_empty() {
            bail!("invalid_configuration: model profile '{name}' missing provider_id");
        }
        if self.base_url.trim().is_empty() {
            bail!("invalid_configuration: model profile '{name}' missing base_url");
        }
        if self.model.trim().is_empty() {
            bail!("invalid_configuration: model profile '{name}' missing model");
        }
        validate_chat_completions_path(&self.chat_completions_path, name)?;

        let auth_strategy = parse_auth_strategy(&self.auth_strategy)?;
        let api_key = match auth_strategy {
            ProviderAuthStrategy::BearerApiKey => {
                let env_name = self.api_key_env.as_deref().filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| anyhow::anyhow!("invalid_configuration: bearer model profile '{name}' requires api_key_env"))?;
                Some(std::env::var(env_name).map_err(|_| {
                    anyhow::anyhow!(
                        "invalid_configuration: model profile '{name}' api_key_env {env_name} is not set"
                    )
                })?)
            }
            ProviderAuthStrategy::NoAuth => None,
        };

        let config = ModelProviderConfig {
            provider_id: self.provider_id.trim().to_string(),
            protocol: parse_protocol(&self.protocol)?,
            compatibility_profile: parse_compatibility_profile(&self.compatibility_profile)?,
            base_url: self.base_url.trim().to_string(),
            chat_completions_path: self.chat_completions_path.trim().to_string(),
            auth_strategy,
            api_key,
            api_key_env: self
                .api_key_env
                .as_deref()
                .filter(|v| !v.trim().is_empty())
                .map(str::to_string),
            model_id: self.model.trim().to_string(),
            timeout: ProviderTimeout {
                request_timeout_ms: self.request_timeout_ms.unwrap_or(30_000),
                stream_timeout_ms: self.stream_timeout_ms.unwrap_or(120_000),
            },
            retry_policy: RetryPolicy {
                max_attempts: self.retry_max_attempts.unwrap_or(3),
                initial_backoff_ms: self.retry_initial_backoff_ms.unwrap_or(200),
                max_backoff_ms: self.retry_max_backoff_ms.unwrap_or(1_000),
            },
            pricing: ModelPricing::default(),
            proxy_url: self.proxy_url.clone(),
            no_proxy: self.no_proxy.clone(),
            ca_bundle_path: self.ca_bundle_path.clone(),
            max_tokens_param: self.max_tokens_param.clone(),
        };
        Ok(config)
    }
}

fn default_chat_completions_path() -> String {
    "/v1/chat/completions".into()
}

fn default_auth_strategy() -> String {
    "bearer".into()
}

fn validate_chat_completions_path(path: &str, name: &str) -> anyhow::Result<()> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("invalid_configuration: model profile '{name}' chat_completions_path is empty");
    }
    if trimmed.contains("://") {
        bail!(
            "invalid_configuration: model profile '{name}' chat_completions_path must not be a full URL"
        );
    }
    if !trimmed.starts_with('/') {
        bail!(
            "invalid_configuration: model profile '{name}' chat_completions_path must start with '/'"
        );
    }
    Ok(())
}

fn parse_protocol(value: &str) -> anyhow::Result<ProviderProtocol> {
    match value.trim() {
        "anthropic" => Ok(ProviderProtocol::Anthropic),
        "openai" | "openai-compatible" | "openai_compatible" => {
            Ok(ProviderProtocol::OpenAICompatible)
        }
        "gemini" | "gemini-native" | "gemini_native" => Ok(ProviderProtocol::GeminiNative),
        other => bail!("invalid_configuration: unsupported provider protocol {other}"),
    }
}

fn parse_compatibility_profile(value: &str) -> anyhow::Result<ProviderCompatibilityProfileKind> {
    match value.trim() {
        "anthropic" => Ok(ProviderCompatibilityProfileKind::Anthropic),
        "text-only" | "text_only" | "textonly" => Ok(ProviderCompatibilityProfileKind::TextOnly),
        "batch" => Ok(ProviderCompatibilityProfileKind::Batch),
        "openai" | "openai-compatible" | "openai_compatible" => {
            Ok(ProviderCompatibilityProfileKind::OpenAICompatible)
        }
        "gemini" | "gemini-native-unsupported" | "gemini_native_unsupported" => {
            Ok(ProviderCompatibilityProfileKind::GeminiNativeUnsupported)
        }
        other => bail!("invalid_configuration: unsupported provider compatibility profile {other}"),
    }
}

fn parse_auth_strategy(value: &str) -> anyhow::Result<ProviderAuthStrategy> {
    match value.trim() {
        "bearer" | "bearer_api_key" | "bearer-api-key" => Ok(ProviderAuthStrategy::BearerApiKey),
        "none" | "no_auth" | "no-auth" => Ok(ProviderAuthStrategy::NoAuth),
        other => bail!("invalid_configuration: unsupported auth strategy {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env_lock<F: FnOnce()>(f: F) {
        let lock = env_lock();
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        f();
    }

    fn set_env(key: &str, value: &str) {
        unsafe { std::env::set_var(key, value) }
    }

    fn remove_env(key: &str) {
        unsafe { std::env::remove_var(key) }
    }

    #[test]
    fn models_toml_active_profile_resolves_to_model_provider_config() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        set_env("OPENAI_API_KEY", "test-openai-key");
        let resolved = resolve_active_model_profile(
            r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"
request_timeout_ms = 10000
stream_timeout_ms = 90000
retry_max_attempts = 2
retry_initial_backoff_ms = 100
retry_max_backoff_ms = 500
"#,
        )
        .expect("active profile should resolve");
        remove_env("OPENAI_API_KEY");

        assert_eq!(resolved.name, "openai-fast");
        assert_eq!(resolved.config.provider_id, "openai");
        assert_eq!(resolved.config.protocol, ProviderProtocol::OpenAICompatible);
        assert_eq!(
            resolved.config.compatibility_profile,
            ProviderCompatibilityProfileKind::OpenAICompatible
        );
        assert_eq!(resolved.config.base_url, "https://api.openai.com");
        assert_eq!(
            resolved.config.chat_completions_path,
            "/v1/chat/completions"
        );
        assert_eq!(resolved.config.model_id, "gpt-4.1-mini");
        assert_eq!(resolved.config.api_key.as_deref(), Some("test-openai-key"));
        assert_eq!(resolved.config.timeout.request_timeout_ms, 10_000);
        assert_eq!(resolved.config.timeout.stream_timeout_ms, 90_000);
        assert_eq!(resolved.config.retry_policy.max_attempts, 2);
        assert_eq!(resolved.config.retry_policy.initial_backoff_ms, 100);
        assert_eq!(resolved.config.retry_policy.max_backoff_ms, 500);
    }

    #[test]
    fn models_toml_invalid_profile_fails_fast() {
        let error = resolve_active_model_profile(
            r#"
active = "missing"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"
"#,
        )
        .expect_err("missing active profile should fail");

        assert!(
            error
                .to_string()
                .contains("active model profile 'missing' was not found")
        );
    }

    #[test]
    fn models_toml_bearer_profile_requires_api_key_env() {
        let error = resolve_active_model_profile(
            r#"
active = "openai-fast"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
"#,
        )
        .expect_err("bearer profile without api_key_env should fail");

        assert!(error.to_string().contains("requires api_key_env"));
    }

    #[test]
    fn models_toml_rejects_full_url_chat_completions_path() {
        let error = resolve_active_model_profile(
            r#"
active = "gemini-flash"

[profiles.gemini-flash]
provider_id = "gemini-openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://generativelanguage.googleapis.com/v1beta/openai"
chat_completions_path = "https://example.com/chat/completions"
model = "gemini-2.5-flash"
auth_strategy = "none"
"#,
        )
        .expect_err("full URL path should fail");

        assert!(error.to_string().contains("must not be a full URL"));
    }

    #[test]
    fn models_toml_rejects_unknown_protocol_profile_combination() {
        let error = resolve_active_model_profile(
            r#"
active = "bad"

[profiles.bad]
provider_id = "custom-local"
protocol = "anthropic"
compatibility_profile = "openai_compatible"
base_url = "http://localhost:8080"
model = "local-model"
auth_strategy = "none"
"#,
        )
        .expect_err("protocol/profile mismatch should fail");

        assert!(error.to_string().contains("incompatible protocol/profile"));
    }

    #[test]
    fn models_toml_multi_profile_registry_resolves_named_profile() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        set_env("WORKER_API_KEY", "worker-key");
        let registry = parse_model_profiles_registry(
            r#"
active = "default"

[profiles.default]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "WORKER_API_KEY"

[profiles.worker-override]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "http://127.0.0.1:9999"
model = "gpt-4.1-nano"
api_key_env = "WORKER_API_KEY"
request_timeout_ms = 5000
stream_timeout_ms = 10000
"#,
        )
        .expect("registry should parse");

        let resolved = resolve_model_profile_from_registry(&registry, "worker-override")
            .expect("worker-override should resolve");
        remove_env("WORKER_API_KEY");

        assert_eq!(resolved.name, "worker-override");
        assert_eq!(resolved.config.base_url, "http://127.0.0.1:9999");
        assert_eq!(resolved.config.model_id, "gpt-4.1-nano");
        assert_eq!(resolved.config.timeout.request_timeout_ms, 5_000);
        assert_eq!(resolved.config.timeout.stream_timeout_ms, 10_000);
    }

    #[test]
    fn models_toml_no_auth_profile_resolves_without_api_key() {
        let registry = parse_model_profiles_registry(
            r#"
active = "local"

[profiles.local]
provider_id = "ollama"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "http://localhost:11434"
model = "llama3.2"
auth_strategy = "none"
"#,
        )
        .expect("no-auth registry should parse");

        let resolved = resolve_model_profile_from_registry(&registry, "local")
            .expect("local profile should resolve");

        assert_eq!(resolved.config.provider_id, "ollama");
        assert_eq!(resolved.config.base_url, "http://localhost:11434");
        assert_eq!(resolved.config.model_id, "llama3.2");
        assert!(resolved.config.api_key.is_none());
    }

    #[test]
    fn models_toml_proxy_fields_are_preserved() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        set_env("PROXY_API_KEY", "proxy-key");
        let registry = parse_model_profiles_registry(
            r#"
active = "proxied"

[profiles.proxied]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "PROXY_API_KEY"
proxy_url = "http://proxy.corp.example:8080"
no_proxy = "localhost,127.0.0.1"
"#,
        )
        .expect("proxied registry should parse");

        let resolved = resolve_model_profile_from_registry(&registry, "proxied")
            .expect("proxied profile should resolve");
        remove_env("PROXY_API_KEY");

        assert_eq!(
            resolved.config.proxy_url.as_deref(),
            Some("http://proxy.corp.example:8080")
        );
        assert_eq!(
            resolved.config.no_proxy.as_deref(),
            Some("localhost,127.0.0.1")
        );
    }

    #[test]
    fn models_toml_gemini_custom_path_resolves() {
        let registry = parse_model_profiles_registry(
            r#"
active = "gemini-flash"

[profiles.gemini-flash]
provider_id = "gemini-openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://generativelanguage.googleapis.com"
chat_completions_path = "/v1beta/openai/chat/completions"
model = "gemini-2.0-flash"
auth_strategy = "none"
"#,
        )
        .expect("gemini registry should parse");

        let resolved = resolve_model_profile_from_registry(&registry, "gemini-flash")
            .expect("gemini-flash should resolve");

        assert_eq!(
            resolved.config.chat_completions_path,
            "/v1beta/openai/chat/completions"
        );
        assert_eq!(resolved.config.model_id, "gemini-2.0-flash");
    }

    #[test]
    fn models_toml_resolve_unknown_profile_returns_error() {
        let registry = parse_model_profiles_registry(
            r#"
active = "default"

[profiles.default]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
auth_strategy = "none"
"#,
        )
        .expect("registry should parse");

        let error = resolve_model_profile_from_registry(&registry, "nonexistent")
            .expect_err("unknown profile should fail");

        assert!(error.to_string().contains("model profile 'nonexistent' was not found"));
    }

    #[test]
    fn models_toml_display_view_shows_api_key_env_status() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        set_env("DISPLAY_TEST_KEY", "some-key");
        let spec = ModelProfileSpec {
            provider_id: "openai".into(),
            protocol: "openai_compatible".into(),
            compatibility_profile: "openai_compatible".into(),
            base_url: "https://api.openai.com".into(),
            chat_completions_path: "/v1/chat/completions".into(),
            model: "gpt-4.1-mini".into(),
            auth_strategy: "bearer".into(),
            api_key_env: Some("DISPLAY_TEST_KEY".into()),
            request_timeout_ms: None,
            stream_timeout_ms: None,
            retry_max_attempts: None,
            retry_initial_backoff_ms: None,
            retry_max_backoff_ms: None,
            proxy_url: None,
            no_proxy: None,
            ca_bundle_path: None,
            max_tokens_param: None,
        };
        let view = build_model_profile_display_view("test-profile", &spec)
            .expect("display view should build");
        remove_env("DISPLAY_TEST_KEY");

        assert_eq!(view.api_key_env_status.as_deref(), Some("set"));
        assert_eq!(view.request_timeout_ms, 30_000);
        assert_eq!(view.retry_max_attempts, 3);
    }
}

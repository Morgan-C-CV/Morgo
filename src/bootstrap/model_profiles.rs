use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::service::api::client::{
    ModelPricing, ModelProviderConfig, ProviderAuthStrategy, ProviderCompatibilityProfileKind,
    ProviderProtocol, ProviderTimeout, validate_provider_config,
};
use crate::service::api::retry::RetryPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelLevel {
    Low,
    Medium,
    High,
    Xhigh,
}

impl ModelLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::Xhigh),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelProfilesFile {
    pub active: Option<String>,
    pub active_level: Option<ModelLevel>,
    #[serde(default)]
    pub levels: ModelLevelsFile,
    #[serde(default)]
    pub profiles: BTreeMap<String, ModelProfileSpec>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLevelsFile {
    pub low: Option<String>,
    pub medium: Option<String>,
    pub high: Option<String>,
    pub xhigh: Option<String>,
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
    #[serde(default)]
    pub prompt_cache_key: Option<String>,
    #[serde(default)]
    pub prompt_cache_retention: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedModelProfile {
    pub name: String,
    pub config: ModelProviderConfig,
    pub level: Option<ModelLevel>,
}

#[derive(Debug, Clone)]
pub struct ModelProfileRegistry {
    pub active: Option<String>,
    pub active_level: Option<ModelLevel>,
    pub levels: BTreeMap<ModelLevel, String>,
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
    pub prompt_cache_key: Option<String>,
    pub prompt_cache_retention: Option<String>,
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
    let active = file
        .active
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let levels = file.levels.into_map();
    if file.profiles.is_empty() {
        bail!("invalid_configuration: models.toml must define at least one profile");
    }
    if active.is_none() && file.active_level.is_none() {
        bail!("invalid_configuration: models.toml requires active or active_level");
    }
    if let Some(active_profile) = active.as_deref() {
        if !file.profiles.contains_key(active_profile) {
            bail!("invalid_configuration: active model profile '{active_profile}' was not found");
        }
    }
    if let Some(active_level) = file.active_level {
        let Some(profile_name) = levels.get(&active_level) else {
            bail!(
                "invalid_configuration: active_level '{}' has no profile mapping",
                active_level.as_str()
            );
        };
        if !file.profiles.contains_key(profile_name) {
            bail!(
                "invalid_configuration: level '{}' references missing profile '{}'",
                active_level.as_str(),
                profile_name
            );
        }
    }
    for (level, profile_name) in &levels {
        if !file.profiles.contains_key(profile_name) {
            bail!(
                "invalid_configuration: level '{}' references missing profile '{}'",
                level.as_str(),
                profile_name
            );
        }
    }
    for (name, spec) in &file.profiles {
        let config = spec.to_model_provider_config(name)?;
        validate_provider_config(&config).map_err(|error| anyhow::anyhow!(error.to_string()))?;
    }
    Ok(ModelProfileRegistry {
        active,
        active_level: file.active_level,
        levels,
        profiles: file.profiles,
    })
}

pub fn merge_model_profiles_registry(
    base: Option<&ModelProfileRegistry>,
    overlay: Option<&ModelProfileRegistry>,
) -> Option<ModelProfileRegistry> {
    match (base, overlay) {
        (None, None) => None,
        (Some(registry), None) | (None, Some(registry)) => Some(registry.clone()),
        (Some(base), Some(overlay)) => {
            let mut levels = base.levels.clone();
            levels.extend(overlay.levels.clone());
            let mut profiles = base.profiles.clone();
            profiles.extend(overlay.profiles.clone());
            Some(ModelProfileRegistry {
                active: overlay.active.clone().or_else(|| base.active.clone()),
                active_level: overlay.active_level.or(base.active_level),
                levels,
                profiles,
            })
        }
    }
}

pub fn resolve_active_model_profile(content: &str) -> anyhow::Result<ResolvedModelProfile> {
    let registry = parse_model_profiles_registry(content)?;
    resolve_active_model_profile_from_registry(&registry)
}

pub fn resolve_active_model_profile_from_registry(
    registry: &ModelProfileRegistry,
) -> anyhow::Result<ResolvedModelProfile> {
    if let Some(level) = registry.active_level {
        return resolve_model_level_from_registry(registry, level);
    }
    let active_profile = registry.active.as_deref().ok_or_else(|| {
        anyhow::anyhow!("invalid_configuration: active model profile is not configured")
    })?;
    resolve_model_profile_from_registry(registry, active_profile).map_err(|error| {
        if error.to_string().contains("model profile '")
            && error.to_string().contains("' was not found")
        {
            anyhow::anyhow!(
                "invalid_configuration: active model profile '{}' was not found",
                active_profile
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
        level: registry
            .levels
            .iter()
            .find_map(|(level, name)| (name == profile).then_some(*level)),
    })
}

pub fn resolve_model_level_from_registry(
    registry: &ModelProfileRegistry,
    level: ModelLevel,
) -> anyhow::Result<ResolvedModelProfile> {
    let profile = registry.levels.get(&level).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid_configuration: model level '{}' is not configured",
            level.as_str()
        )
    })?;
    let mut resolved = resolve_model_profile_from_registry(registry, profile)?;
    resolved.level = Some(level);
    Ok(resolved)
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
        prompt_cache_key: spec.prompt_cache_key.clone(),
        prompt_cache_retention: spec.prompt_cache_retention.clone(),
    })
}

impl ModelLevelsFile {
    fn into_map(self) -> BTreeMap<ModelLevel, String> {
        let mut map = BTreeMap::new();
        for (level, value) in [
            (ModelLevel::Low, self.low),
            (ModelLevel::Medium, self.medium),
            (ModelLevel::High, self.high),
            (ModelLevel::Xhigh, self.xhigh),
        ] {
            if let Some(profile) = value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
            {
                map.insert(level, profile);
            }
        }
        map
    }
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
            prompt_cache_key: self.prompt_cache_key.clone(),
            prompt_cache_retention: self.prompt_cache_retention.clone(),
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
        "morgo" | "messages-api" | "messages_api" | "anthropic" => {
            Ok(ProviderProtocol::MessagesApi)
        }
        "openai" | "openai-compatible" | "openai_compatible" => {
            Ok(ProviderProtocol::OpenAICompatible)
        }
        "gemini" | "gemini-native" | "gemini_native" => Ok(ProviderProtocol::GeminiNative),
        other => bail!("invalid_configuration: unsupported provider protocol {other}"),
    }
}

fn parse_compatibility_profile(value: &str) -> anyhow::Result<ProviderCompatibilityProfileKind> {
    match value.trim() {
        "morgo" | "messages-api" | "messages_api" | "anthropic" => {
            Ok(ProviderCompatibilityProfileKind::MessagesApi)
        }
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

    fn set_env(key: &str, value: &str) {
        unsafe { std::env::set_var(key, value) }
    }

    fn remove_env(key: &str) {
        unsafe { std::env::remove_var(key) }
    }

    #[test]
    fn models_toml_active_level_resolves_to_model_provider_config() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        set_env("OPENAI_API_KEY", "test-openai-key");
        let resolved = resolve_active_model_profile(
            r#"
active_level = "medium"

[levels]
low = "openai-fast"
medium = "openai-strong"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
api_key_env = "OPENAI_API_KEY"

[profiles.openai-strong]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-5.4"
api_key_env = "OPENAI_API_KEY"
request_timeout_ms = 10000
stream_timeout_ms = 90000
retry_max_attempts = 2
retry_initial_backoff_ms = 100
retry_max_backoff_ms = 500
prompt_cache_key = "rust-agent-r1"
prompt_cache_retention = "in_memory"
"#,
        )
        .expect("active level should resolve");
        remove_env("OPENAI_API_KEY");

        assert_eq!(resolved.name, "openai-strong");
        assert_eq!(resolved.level, Some(ModelLevel::Medium));
        assert_eq!(resolved.config.provider_id, "openai");
        assert_eq!(resolved.config.protocol, ProviderProtocol::OpenAICompatible);
        assert_eq!(resolved.config.model_id, "gpt-5.4");
        assert_eq!(resolved.config.api_key.as_deref(), Some("test-openai-key"));
        assert_eq!(resolved.config.timeout.request_timeout_ms, 10_000);
        assert_eq!(resolved.config.timeout.stream_timeout_ms, 90_000);
        assert_eq!(resolved.config.retry_policy.max_attempts, 2);
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
    fn models_toml_rejects_active_level_without_mapping() {
        let error = resolve_active_model_profile(
            r#"
active_level = "high"

[profiles.openai-fast]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-4.1-mini"
auth_strategy = "none"
"#,
        )
        .expect_err("missing level mapping should fail");

        assert!(error.to_string().contains("active_level 'high'"));
    }

    #[test]
    fn models_toml_multi_profile_registry_resolves_named_profile() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        set_env("WORKER_API_KEY", "worker-key");
        let registry = parse_model_profiles_registry(
            r#"
active_level = "medium"

[levels]
medium = "worker-override"

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
"#,
        )
        .expect("registry should parse");
        let resolved = resolve_model_profile_from_registry(&registry, "worker-override")
            .expect("profile should resolve");
        remove_env("WORKER_API_KEY");

        assert_eq!(registry.active_level, Some(ModelLevel::Medium));
        assert_eq!(resolved.level, Some(ModelLevel::Medium));
        assert_eq!(resolved.config.model_id, "gpt-4.1-nano");
    }

    #[test]
    fn merged_registry_overlays_levels_profiles_and_active_level() {
        let home = parse_model_profiles_registry(
            r#"
active_level = "low"

[levels]
low = "home-low"
medium = "home-medium"

[profiles.home-low]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-5.4-mini"
auth_strategy = "none"

[profiles.home-medium]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://api.openai.com"
model = "gpt-5.4"
auth_strategy = "none"
"#,
        )
        .expect("home registry should parse");
        let workspace = parse_model_profiles_registry(
            r#"
active_level = "medium"

[levels]
medium = "workspace-medium"

[profiles.workspace-medium]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://workspace.example"
model = "gpt-5.5"
auth_strategy = "none"
"#,
        )
        .expect("workspace registry should parse");
        let merged = merge_model_profiles_registry(Some(&home), Some(&workspace))
            .expect("merged registry should exist");
        let resolved = resolve_active_model_profile_from_registry(&merged)
            .expect("merged active profile should resolve");

        assert_eq!(merged.active_level, Some(ModelLevel::Medium));
        assert_eq!(
            merged.levels.get(&ModelLevel::Low).map(String::as_str),
            Some("home-low")
        );
        assert_eq!(
            merged.levels.get(&ModelLevel::Medium).map(String::as_str),
            Some("workspace-medium")
        );
        assert_eq!(resolved.level, Some(ModelLevel::Medium));
        assert_eq!(resolved.config.base_url, "https://workspace.example");
    }
}

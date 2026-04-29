use anyhow::Context;

use crate::bootstrap::model_profiles::{
    ModelProfileRegistry, resolve_model_profile_from_registry,
};
use crate::core::state_frame_model_router::ModelRoute;
use crate::service::observability::ServiceObservabilityTracker;
use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepModelResolution {
    Inherited,
    ProfileOverride { profile_name: String },
}

#[derive(Debug, Clone)]
pub struct ResolvedStepModel {
    pub snapshot: ActiveModelRuntimeSnapshot,
    pub resolution: StepModelResolution,
}

pub fn resolve_step_model(
    route: &ModelRoute,
    inherited: &ActiveModelRuntimeSnapshot,
    registry: Option<&ModelProfileRegistry>,
    observability: ServiceObservabilityTracker,
) -> anyhow::Result<ResolvedStepModel> {
    let Some(profile_name) = route.provider_profile_id.as_deref() else {
        return Ok(ResolvedStepModel {
            snapshot: inherited.clone(),
            resolution: StepModelResolution::Inherited,
        });
    };

    let registry = registry.ok_or_else(|| {
        anyhow::anyhow!(
            "model route requested provider profile '{}' but model profile registry is unavailable",
            profile_name
        )
    })?;
    let resolved = resolve_model_profile_from_registry(registry, profile_name)
        .with_context(|| format!("failed to resolve step model profile '{profile_name}'"))?;
    let snapshot =
        ActiveModelRuntimeSnapshot::from_resolved_profile(&resolved, observability);

    Ok(ResolvedStepModel {
        snapshot,
        resolution: StepModelResolution::ProfileOverride {
            profile_name: resolved.name,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{StepModelResolution, resolve_step_model};
    use crate::bootstrap::model_profiles::parse_model_profiles_registry;
    use crate::core::state_frame_model_router::{ModelRoute, ModelTier};
    use crate::service::api::client::{
        ModelPricing, ModelProviderConfig, ProviderAuthStrategy,
        ProviderCompatibilityProfileKind, ProviderProtocol, ProviderTimeout,
    };
    use crate::service::api::retry::RetryPolicy;
    use crate::service::observability::ServiceObservabilityTracker;
    use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;
    use crate::state::app_state::{ActiveModelProfileSource, ActiveModelProviderSummary};

    fn inherited_snapshot() -> ActiveModelRuntimeSnapshot {
        let config = ModelProviderConfig {
            provider_id: "inherited-provider".into(),
            protocol: ProviderProtocol::OpenAICompatible,
            compatibility_profile: ProviderCompatibilityProfileKind::OpenAICompatible,
            base_url: "https://inherited.example".into(),
            auth_strategy: ProviderAuthStrategy::NoAuth,
            api_key: None,
            api_key_env: None,
            chat_completions_path: "/v1/chat/completions".into(),
            model_id: "inherited-model".into(),
            timeout: ProviderTimeout {
                request_timeout_ms: 1_000,
                stream_timeout_ms: 1_000,
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
            max_tokens_param: None,
        };
        ActiveModelRuntimeSnapshot {
            config: config.clone(),
            client: crate::service::api::client::ModelProviderClient::from_config(config),
            active_profile_name: Some("inherited-fast".into()),
            source: ActiveModelProfileSource::ModelsToml,
            summary: ActiveModelProviderSummary {
                provider_id: "inherited-provider".into(),
                protocol: "OpenAICompatible".into(),
                compatibility_profile: "OpenAICompatible".into(),
                base_url_host: "inherited.example".into(),
                model: "inherited-model".into(),
                auth_status: "none".into(),
            },
        }
    }

    fn model_registry() -> crate::bootstrap::model_profiles::ModelProfileRegistry {
        parse_model_profiles_registry(
            r#"
active = "default"

[profiles.default]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://default.example"
model = "default-model"
auth_strategy = "none"

[profiles.high-tier]
provider_id = "anthropic"
protocol = "anthropic"
compatibility_profile = "anthropic"
base_url = "https://api.anthropic.com"
model = "claude-sonnet-4-6"
auth_strategy = "none"
request_timeout_ms = 9000
stream_timeout_ms = 9000
retry_max_attempts = 2
retry_initial_backoff_ms = 50
retry_max_backoff_ms = 100
"#,
        )
        .expect("registry should parse")
    }

    #[test]
    fn resolve_step_model_without_profile_override_uses_inherited_snapshot() {
        let inherited = inherited_snapshot();
        let route = ModelRoute {
            tier: ModelTier::Medium,
            provider_profile_id: None,
        };

        let resolved = resolve_step_model(
            &route,
            &inherited,
            None,
            ServiceObservabilityTracker::default(),
        )
        .expect("resolver should succeed");

        assert!(matches!(resolved.resolution, StepModelResolution::Inherited));
        assert_eq!(resolved.snapshot.config, inherited.config);
        assert_eq!(
            resolved.snapshot.active_profile_name,
            inherited.active_profile_name
        );
        assert_eq!(resolved.snapshot.summary, inherited.summary);
    }

    #[test]
    fn resolve_step_model_with_profile_override_builds_step_local_snapshot() {
        let inherited = inherited_snapshot();
        let registry = model_registry();
        let route = ModelRoute {
            tier: ModelTier::High,
            provider_profile_id: Some("high-tier".into()),
        };

        let resolved = resolve_step_model(
            &route,
            &inherited,
            Some(&registry),
            ServiceObservabilityTracker::default(),
        )
        .expect("resolver should succeed");

        assert_eq!(
            resolved.resolution,
            StepModelResolution::ProfileOverride {
                profile_name: "high-tier".into(),
            }
        );
        assert_eq!(resolved.snapshot.active_profile_name.as_deref(), Some("high-tier"));
        assert_eq!(resolved.snapshot.config.provider_id, "anthropic");
        assert_eq!(resolved.snapshot.config.model_id, "claude-sonnet-4-6");
        assert_eq!(resolved.snapshot.summary.provider_id, "anthropic");
        assert_eq!(resolved.snapshot.summary.model, "claude-sonnet-4-6");
        assert_eq!(inherited.active_profile_name.as_deref(), Some("inherited-fast"));
    }

    #[test]
    fn resolve_step_model_errors_when_registry_missing_for_profile_override() {
        let inherited = inherited_snapshot();
        let route = ModelRoute {
            tier: ModelTier::High,
            provider_profile_id: Some("high-tier".into()),
        };

        let error = resolve_step_model(
            &route,
            &inherited,
            None,
            ServiceObservabilityTracker::default(),
        )
        .expect_err("resolver should fail");

        assert!(error
            .to_string()
            .contains("model profile registry is unavailable"));
    }

    #[test]
    fn resolve_step_model_errors_when_profile_is_unknown() {
        let inherited = inherited_snapshot();
        let registry = model_registry();
        let route = ModelRoute {
            tier: ModelTier::High,
            provider_profile_id: Some("missing-profile".into()),
        };

        let error = resolve_step_model(
            &route,
            &inherited,
            Some(&registry),
            ServiceObservabilityTracker::default(),
        )
        .expect_err("resolver should fail");

        assert!(error
            .to_string()
            .contains("failed to resolve step model profile 'missing-profile'"));
    }
}

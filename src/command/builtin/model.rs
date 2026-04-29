use async_trait::async_trait;

use crate::bootstrap::config_root::resolve_config_root;
use crate::bootstrap::has_explicit_provider_env_override;
use crate::bootstrap::model_profiles::{
    build_model_profile_display_view, load_model_profiles_registry_from_root,
    resolve_model_profile_from_registry,
};
use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;
use crate::state::app_state::{ActiveModelProfileSource, AppState};

pub struct ModelCommand;

#[async_trait]
impl Command for ModelCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "model".into(),
            description: "Inspect active model state and available model profiles".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.trim();
        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or("");

        match action {
            "" => Ok(CommandResult::Message(render_active_model_summary(
                app_state,
            ))),
            "list" => Ok(CommandResult::Message(render_model_list(app_state)?)),
            "show" => {
                let profile = parts.collect::<Vec<_>>().join(" ");
                if profile.trim().is_empty() {
                    return Ok(CommandResult::Message(
                        "Usage: /model [list|show <profile>|use <profile>|reload]".into(),
                    ));
                }
                Ok(CommandResult::Message(render_model_show(
                    app_state,
                    profile.trim(),
                )?))
            }
            "use" => {
                let profile = parts.collect::<Vec<_>>().join(" ");
                if profile.trim().is_empty() {
                    return Ok(CommandResult::Message(
                        "Usage: /model [list|show <profile>|use <profile>|reload]".into(),
                    ));
                }
                apply_model_use(app_state, profile.trim()).await
            }
            "reload" => Ok(CommandResult::Message(render_model_reload(app_state)?)),
            other => Ok(CommandResult::Denied(format!(
                "Unknown /model action: {other}. Usage: /model [list|show <profile>|use <profile>|reload]"
            ))),
        }
    }
}

fn render_active_model_summary(app_state: &AppState) -> String {
    [
        "Model".to_string(),
        String::new(),
        format!(
            "- active_profile: {}",
            app_state
                .active_model_profile_name
                .as_deref()
                .unwrap_or("default")
        ),
        format!(
            "- source: {}",
            app_state.active_model_profile_source.as_str()
        ),
        format!(
            "- provider_id: {}",
            app_state.active_model_provider_summary.provider_id
        ),
        format!("- protocol: {}", app_state.active_model_provider_summary.protocol),
        format!(
            "- compatibility_profile: {}",
            app_state.active_model_provider_summary.compatibility_profile
        ),
        format!(
            "- base_url_host: {}",
            app_state.active_model_provider_summary.base_url_host
        ),
        format!("- model: {}", app_state.active_model_provider_summary.model),
        format!(
            "- auth_status: {}",
            app_state.active_model_provider_summary.auth_status
        ),
        String::new(),
        "Note: /model applies changes on the next turn only. Use /model list|show|use|reload to inspect or update the runtime handle; reload does not switch the active runtime client.".into(),
    ]
    .join("\n")
}

fn render_model_list(app_state: &AppState) -> anyhow::Result<String> {
    let config_root = resolve_runtime_config_root(app_state)?;
    let Some(registry) = load_model_profiles_registry_from_root(&config_root)? else {
        return Ok(format!(
            "Model registry unavailable: models.toml not found under {}",
            config_root.display()
        ));
    };

    let mut lines = vec![
        "Model profiles".to_string(),
        String::new(),
        format!("- config_root: {}", config_root.display()),
        format!("- active_profile: {}", registry.active),
        format!("- profiles: {}", registry.profiles.len()),
    ];

    for (name, spec) in &registry.profiles {
        let view = build_model_profile_display_view(name, spec)?;
        lines.push(format!(
            "- {}: provider_id={}, protocol={}, model={}, auth_strategy={}",
            view.name, view.provider_id, view.protocol, view.model, view.auth_strategy
        ));
    }

    Ok(lines.join("\n"))
}

fn render_model_show(app_state: &AppState, profile: &str) -> anyhow::Result<String> {
    let config_root = resolve_runtime_config_root(app_state)?;
    let Some(registry) = load_model_profiles_registry_from_root(&config_root)? else {
        return Ok(format!(
            "Model registry unavailable: models.toml not found under {}",
            config_root.display()
        ));
    };
    let Some(spec) = registry.profiles.get(profile) else {
        return Ok(format!("Profile not found: {profile}"));
    };

    let view = build_model_profile_display_view(profile, spec)?;
    let mut lines = vec![
        format!("Model profile: {}", view.name),
        String::new(),
        format!("- provider_id: {}", view.provider_id),
        format!("- protocol: {}", view.protocol),
        format!("- compatibility_profile: {}", view.compatibility_profile),
        format!("- base_url: {}", view.base_url),
        format!("- chat_completions_path: {}", view.chat_completions_path),
        format!("- model: {}", view.model),
        format!("- auth_strategy: {}", view.auth_strategy),
    ];

    match (
        view.api_key_env.as_deref(),
        view.api_key_env_status.as_deref(),
    ) {
        (Some(env_name), Some(status)) => {
            lines.push(format!("- api_key_env: {} ({})", env_name, status));
        }
        (Some(env_name), None) => {
            lines.push(format!("- api_key_env: {}", env_name));
        }
        (None, _) => {
            lines.push("- api_key_env: none".into());
        }
    }

    lines.extend([
        format!("- request_timeout_ms: {}", view.request_timeout_ms),
        format!("- stream_timeout_ms: {}", view.stream_timeout_ms),
        format!("- retry_max_attempts: {}", view.retry_max_attempts),
        format!(
            "- retry_initial_backoff_ms: {}",
            view.retry_initial_backoff_ms
        ),
        format!("- retry_max_backoff_ms: {}", view.retry_max_backoff_ms),
    ]);

    Ok(lines.join("\n"))
}

async fn apply_model_use(app_state: &AppState, profile: &str) -> anyhow::Result<CommandResult> {
    if has_explicit_provider_env_override()
        || matches!(
            app_state.active_model_profile_source,
            ActiveModelProfileSource::EnvOverride
        )
    {
        return Ok(CommandResult::Denied(
            "runtime model selection is locked by RUST_AGENT_PROVIDER_* environment overrides; /model use is unavailable until those overrides are removed".into(),
        ));
    }

    let Some(active_model_runtime) = app_state.active_model_runtime.as_ref() else {
        return Ok(CommandResult::Denied(
            "active model runtime is unavailable; /model use cannot update the runtime handle"
                .into(),
        ));
    };

    let config_root = resolve_runtime_config_root(app_state)?;
    let Some(registry) = load_model_profiles_registry_from_root(&config_root)? else {
        return Ok(CommandResult::Denied(format!(
            "models.toml not found under {}; /model use requires a model registry",
            config_root.display()
        )));
    };

    let resolved = match resolve_model_profile_from_registry(&registry, profile) {
        Ok(resolved) => resolved,
        Err(error) if error.to_string().contains("was not found") => {
            return Ok(CommandResult::Denied(format!(
                "Profile not found: {profile}"
            )));
        }
        Err(error) => return Err(error),
    };

    let snapshot = ActiveModelRuntimeSnapshot::from_resolved_profile(
        &resolved,
        app_state.service_observability_tracker.clone(),
    );
    active_model_runtime.replace(snapshot).await;

    Ok(CommandResult::Message(format!(
        "Updated runtime model handle to profile {} from {}. This will apply on next turn; in-flight turns and existing subagents keep their current snapshot.",
        resolved.name,
        config_root.display()
    )))
}

fn render_model_reload(app_state: &AppState) -> anyhow::Result<String> {
    let config_root = resolve_runtime_config_root(app_state)?;
    let Some(registry) = load_model_profiles_registry_from_root(&config_root)? else {
        return Ok(format!(
            "Reloaded model profiles from {}. models.toml not found; runtime active model remains unchanged.",
            config_root.display()
        ));
    };

    Ok(format!(
        "Reloaded model profiles from {}. active_profile={} profiles={} runtime active model remains unchanged.",
        config_root.display(),
        registry.active,
        registry.profiles.len()
    ))
}

fn resolve_runtime_config_root(app_state: &AppState) -> anyhow::Result<std::path::PathBuf> {
    let cwd = app_state
        .session
        .as_ref()
        .map(|session| std::path::Path::new(session.cwd.as_str()))
        .unwrap_or_else(|| std::path::Path::new("."));
    resolve_config_root(cwd)
}

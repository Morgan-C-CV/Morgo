use std::fs;
use std::path::PathBuf;

use anyhow::bail;
use async_trait::async_trait;

use crate::bootstrap::config_root::{preferred_home_config_root, resolve_config_root};
use crate::bootstrap::has_explicit_provider_env_override;
use crate::bootstrap::model_profiles::{
    ModelLevel, ModelProfileRegistry, build_model_profile_display_view,
    load_model_profiles_registry_from_root, merge_model_profiles_registry,
    resolve_model_level_from_registry,
};
use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::active_model_runtime::ActiveModelRuntimeSnapshot;
use crate::state::app_state::{ActiveModelProfileSource, AppState};

pub struct ModelCommand;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ModelScope {
    Session,
    Workspace,
}

#[async_trait]
impl Command for ModelCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "model".into(),
            description: "Inspect active model state and available model levels".into(),
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
            )?)),
            "list" => Ok(CommandResult::Message(render_model_list(app_state)?)),
            "show" => Ok(CommandResult::Message(render_model_show(app_state)?)),
            "use" => {
                let args = parts.collect::<Vec<_>>();
                apply_model_use(app_state, &args).await
            }
            "clear" => {
                let args = parts.collect::<Vec<_>>();
                apply_model_clear(app_state, &args).await
            }
            "reload" => Ok(CommandResult::Message(render_model_reload(app_state)?)),
            other => Ok(CommandResult::Denied(format!(
                "Unknown /model action: {other}. Usage: /model [list|show|use <low|medium|high|xhigh> [--workspace]|clear [--workspace]|reload]"
            ))),
        }
    }
}

fn render_active_model_summary(app_state: &AppState) -> anyhow::Result<String> {
    let snapshot = current_runtime_snapshot(app_state)?;
    let session_level = app_state
        .session_store
        .as_ref()
        .and_then(|store| store.load_model_level_override(&app_state.current_session_id()));
    let (workspace_root, home_root, registry) = load_effective_registry(app_state)?;

    Ok([
        "Model".to_string(),
        String::new(),
        format!(
            "- active_level: {}",
            snapshot.active_level.map(|level| level.as_str()).unwrap_or("none")
        ),
        format!(
            "- active_profile: {}",
            snapshot.active_profile_name.as_deref().unwrap_or("default")
        ),
        format!("- source: {}", snapshot.source.as_str()),
        format!(
            "- session_override: {}",
            session_level.map(|level| level.as_str()).unwrap_or("none")
        ),
        format!("- workspace_root: {}", workspace_root.display()),
        format!(
            "- home_root: {}",
            home_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".into())
        ),
        format!(
            "- workspace_active_level: {}",
            registry
                .as_ref()
                .and_then(|registry| registry.active_level)
                .map(|level| level.as_str())
                .unwrap_or("none")
        ),
        format!("- provider_id: {}", snapshot.summary.provider_id),
        format!("- protocol: {}", snapshot.summary.protocol),
        format!("- compatibility_profile: {}", snapshot.summary.compatibility_profile),
        format!("- base_url_host: {}", snapshot.summary.base_url_host),
        format!("- model: {}", snapshot.summary.model),
        format!("- auth_status: {}", snapshot.summary.auth_status),
        String::new(),
        "Note: /model use defaults to session scope; add --workspace to change the workspace default for future sessions.".into(),
    ]
    .join("\n"))
}

fn render_model_list(app_state: &AppState) -> anyhow::Result<String> {
    let (workspace_root, home_root, registry) = load_effective_registry(app_state)?;
    let Some(registry) = registry else {
        return Ok(format!(
            "Model registry unavailable: no models.toml found under workspace {} or home {}",
            workspace_root.display(),
            home_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".into())
        ));
    };

    let mut lines = vec![
        "Model levels".to_string(),
        String::new(),
        format!("- workspace_root: {}", workspace_root.display()),
        format!(
            "- home_root: {}",
            home_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".into())
        ),
        format!(
            "- active_level: {}",
            registry
                .active_level
                .map(|level| level.as_str())
                .unwrap_or("none")
        ),
        format!("- profiles: {}", registry.profiles.len()),
    ];

    for level in [
        ModelLevel::Low,
        ModelLevel::Medium,
        ModelLevel::High,
        ModelLevel::Xhigh,
    ] {
        match registry.levels.get(&level) {
            Some(profile_name) => {
                let spec = registry
                    .profiles
                    .get(profile_name)
                    .expect("mapped profile exists");
                let view = build_model_profile_display_view(profile_name, spec)?;
                lines.push(format!(
                    "- {} -> {}: provider_id={}, model={}, auth_strategy={}",
                    level.as_str(),
                    view.name,
                    view.provider_id,
                    view.model,
                    view.auth_strategy
                ));
            }
            None => lines.push(format!("- {} -> unconfigured", level.as_str())),
        }
    }

    Ok(lines.join("\n"))
}

fn render_model_show(app_state: &AppState) -> anyhow::Result<String> {
    let (_, _, registry) = load_effective_registry(app_state)?;
    let Some(registry) = registry else {
        return Ok("Model registry unavailable".into());
    };
    let mut lines = vec!["Model mapping".to_string(), String::new()];
    for level in [
        ModelLevel::Low,
        ModelLevel::Medium,
        ModelLevel::High,
        ModelLevel::Xhigh,
    ] {
        if let Some(profile_name) = registry.levels.get(&level) {
            let spec = registry
                .profiles
                .get(profile_name)
                .expect("mapped profile exists");
            let view = build_model_profile_display_view(profile_name, spec)?;
            lines.push(format!(
                "- {} -> profile={} provider_id={} model={} base_url={}",
                level.as_str(),
                view.name,
                view.provider_id,
                view.model,
                view.base_url
            ));
        } else {
            lines.push(format!("- {} -> unconfigured", level.as_str()));
        }
    }
    Ok(lines.join("\n"))
}

async fn apply_model_use(app_state: &AppState, args: &[&str]) -> anyhow::Result<CommandResult> {
    if has_explicit_provider_env_override()
        || matches!(
            current_runtime_snapshot(app_state)?.source,
            ActiveModelProfileSource::EnvOverride
        )
    {
        return Ok(CommandResult::Denied(
            "runtime model selection is locked by RUST_AGENT_PROVIDER_* environment overrides; /model use is unavailable until those overrides are removed".into(),
        ));
    }

    let Some(level_arg) = args.first().copied() else {
        return Ok(CommandResult::Message(
            "Usage: /model use <low|medium|high|xhigh> [--workspace]".into(),
        ));
    };
    let Some(level) = ModelLevel::parse(level_arg) else {
        return Ok(CommandResult::Denied(format!(
            "Unknown model level: {level_arg}. Expected low, medium, high, or xhigh."
        )));
    };
    let scope = parse_scope_flag(&args[1..])?;
    let Some(active_model_runtime) = app_state.active_model_runtime.as_ref() else {
        return Ok(CommandResult::Denied(
            "active model runtime is unavailable; /model use cannot update the runtime handle"
                .into(),
        ));
    };

    let (workspace_root, _, registry) = load_effective_registry(app_state)?;
    let Some(registry) = registry else {
        return Ok(CommandResult::Denied(format!(
            "models.toml not found for workspace {}; /model use requires a model registry",
            workspace_root.display()
        )));
    };

    let resolved = resolve_model_level_from_registry(&registry, level)?;
    let mut snapshot = ActiveModelRuntimeSnapshot::from_resolved_profile(
        &resolved,
        app_state.service_observability_tracker.clone(),
    );
    snapshot.active_level = Some(level);
    snapshot.source = match scope {
        ModelScope::Session => ActiveModelProfileSource::SessionOverride,
        ModelScope::Workspace => ActiveModelProfileSource::WorkspaceModelsToml,
    };
    active_model_runtime.replace(snapshot).await;

    match scope {
        ModelScope::Session => {
            let Some(store) = app_state.session_store.as_ref() else {
                return Ok(CommandResult::Denied(
                    "session store is unavailable; cannot persist session model override".into(),
                ));
            };
            store
                .save_model_level_override(&app_state.current_session_id(), Some(level))
                .map_err(|error| anyhow::anyhow!(error.detail.clone()))?;
            Ok(CommandResult::Message(format!(
                "Updated session model level to {}. This will apply on next turn; in-flight turns and existing subagents keep their current snapshot.",
                level.as_str()
            )))
        }
        ModelScope::Workspace => {
            write_workspace_active_level(&workspace_root, Some(level))?;
            let Some(store) = app_state.session_store.as_ref() else {
                return Ok(CommandResult::Message(format!(
                    "Updated workspace model level to {} at {}. Runtime handle will apply on next turn.",
                    level.as_str(),
                    workspace_root.display()
                )));
            };
            store
                .save_model_level_override(&app_state.current_session_id(), None)
                .map_err(|error| anyhow::anyhow!(error.detail.clone()))?;
            Ok(CommandResult::Message(format!(
                "Updated workspace model level to {} at {}. Cleared any session override; runtime handle will apply on next turn.",
                level.as_str(),
                workspace_root.display()
            )))
        }
    }
}

async fn apply_model_clear(app_state: &AppState, args: &[&str]) -> anyhow::Result<CommandResult> {
    let scope = parse_scope_flag(args)?;
    match scope {
        ModelScope::Session => {
            let Some(store) = app_state.session_store.as_ref() else {
                return Ok(CommandResult::Denied(
                    "session store is unavailable; cannot clear session model override".into(),
                ));
            };
            store
                .save_model_level_override(&app_state.current_session_id(), None)
                .map_err(|error| anyhow::anyhow!(error.detail.clone()))?;
            Ok(CommandResult::Message(
                "Cleared session model override. The next turn will fall back to workspace/home configuration.".into(),
            ))
        }
        ModelScope::Workspace => {
            let (workspace_root, _, _) = load_effective_registry(app_state)?;
            write_workspace_active_level(&workspace_root, None)?;
            Ok(CommandResult::Message(format!(
                "Cleared workspace active_level in {}. Future sessions will fall back to home/bootstrap configuration.",
                workspace_root.display()
            )))
        }
    }
}

fn render_model_reload(app_state: &AppState) -> anyhow::Result<String> {
    let (workspace_root, home_root, registry) = load_effective_registry(app_state)?;
    let Some(registry) = registry else {
        return Ok(format!(
            "Reloaded model profiles from workspace {} and home {}. No models.toml found; runtime active model remains unchanged.",
            workspace_root.display(),
            home_root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "none".into())
        ));
    };

    Ok(format!(
        "Reloaded model profiles from workspace {} and home {}. active_level={} profiles={} runtime active model remains unchanged.",
        workspace_root.display(),
        home_root
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".into()),
        registry
            .active_level
            .map(|level| level.as_str())
            .unwrap_or("none"),
        registry.profiles.len()
    ))
}

fn current_runtime_snapshot(app_state: &AppState) -> anyhow::Result<ActiveModelRuntimeSnapshot> {
    app_state
        .active_model_runtime
        .as_ref()
        .map(|runtime| runtime.snapshot_blocking())
        .ok_or_else(|| anyhow::anyhow!("active model runtime is unavailable"))
}

fn load_effective_registry(
    app_state: &AppState,
) -> anyhow::Result<(PathBuf, Option<PathBuf>, Option<ModelProfileRegistry>)> {
    let workspace_root = resolve_runtime_config_root(app_state)?;
    let home_root = preferred_home_config_root();
    let home_registry = match home_root.as_ref() {
        Some(path) if path != &workspace_root => load_model_profiles_registry_from_root(path)?,
        _ => None,
    };
    let workspace_registry = load_model_profiles_registry_from_root(&workspace_root)?;
    Ok((
        workspace_root,
        home_root,
        merge_model_profiles_registry(home_registry.as_ref(), workspace_registry.as_ref()),
    ))
}

fn parse_scope_flag(args: &[&str]) -> anyhow::Result<ModelScope> {
    let mut scope = ModelScope::Session;
    for arg in args {
        match *arg {
            "--workspace" => scope = ModelScope::Workspace,
            "--session" => scope = ModelScope::Session,
            other => bail!("unknown /model option: {other}"),
        }
    }
    Ok(scope)
}

fn write_workspace_active_level(
    config_root: &std::path::Path,
    level: Option<ModelLevel>,
) -> anyhow::Result<()> {
    fs::create_dir_all(config_root)?;
    let path = config_root.join("models.toml");
    let mut doc = if path.exists() {
        fs::read_to_string(&path)
            .ok()
            .and_then(|text| text.parse::<toml::Value>().ok())
            .unwrap_or_else(|| toml::Value::Table(toml::map::Map::new()))
    } else {
        toml::Value::Table(toml::map::Map::new())
    };
    let Some(table) = doc.as_table_mut() else {
        bail!("invalid_configuration: workspace models.toml root must be a table");
    };
    match level {
        Some(level) => {
            table.insert(
                "active_level".into(),
                toml::Value::String(level.as_str().to_string()),
            );
        }
        None => {
            table.remove("active_level");
        }
    }
    let serialized = toml::to_string_pretty(&doc).map_err(|error| {
        anyhow::anyhow!("invalid_configuration: failed to serialize models.toml: {error}")
    })?;
    fs::write(path, serialized)?;
    Ok(())
}

fn resolve_runtime_config_root(app_state: &AppState) -> anyhow::Result<PathBuf> {
    let cwd = app_state
        .session
        .as_ref()
        .map(|session| std::path::Path::new(session.cwd.as_str()))
        .unwrap_or_else(|| std::path::Path::new("."));
    resolve_config_root(cwd)
}

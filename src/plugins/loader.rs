use std::fs;
use std::path::{Path, PathBuf};

use crate::command::types::CommandAvailability;
use crate::hook::registry::HookEventMatcher;
use crate::plugins::state::load_plugin_state_from_root;
use crate::plugins::types::PluginCommandDefinition;
use crate::plugins::types::{
    PluginActivationSummary, PluginApplyStatus, PluginCapability, PluginConfigSource,
    PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity, PluginDiagnosticsMetadata,
    PluginGovernanceState, PluginHookDefinition, PluginHookManifest, PluginLifecycleState,
    PluginLoadResult, PluginManifest, PluginRuntimeKind, PluginRuntimeSpec, PluginToolDefinition,
};

pub fn load_plugins(cwd: &Path) -> PluginLoadResult {
    load_plugins_from_root(&cwd.join(".claude"), cwd)
}

pub fn load_plugins_from_root(config_root: &Path, _cwd: &Path) -> PluginLoadResult {
    let root = config_root.join("plugins");
    let mut diagnostics = Vec::new();
    let mut plugins = Vec::new();
    let governance_state = load_plugin_state_from_root(config_root);
    diagnostics.extend(
        governance_state
            .diagnostics
            .into_iter()
            .map(|message| PluginDiagnostic {
                plugin_name: None,
                manifest_path: Some(governance_state.path.clone()),
                severity: PluginDiagnosticSeverity::Info,
                code: format!("plugin-state-{}", governance_state.source.as_str()),
                message,
            }),
    );

    if !root.exists() {
        return PluginLoadResult {
            root,
            source: PluginConfigSource::Missing,
            plugins,
            diagnostics,
            orphaned_governance_entries: governance_state.states.keys().cloned().collect(),
        };
    }

    visit_plugin_dirs(
        &root,
        &governance_state.states,
        &mut plugins,
        &mut diagnostics,
    );
    let discovered_names = plugins
        .iter()
        .map(|plugin| plugin.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let orphaned_governance_entries = governance_state
        .states
        .keys()
        .filter(|name| !discovered_names.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    diagnostics.extend(orphaned_governance_entries.iter().map(|name| PluginDiagnostic {
        plugin_name: Some(name.clone()),
        manifest_path: Some(governance_state.path.clone()),
        severity: PluginDiagnosticSeverity::Warning,
        code: "plugin-governance-orphaned".into(),
        message: format!(
            "persisted governance exists for plugin {} but no plugin manifest is currently discoverable",
            name
        ),
    }));
    PluginLoadResult {
        root,
        source: PluginConfigSource::Directory,
        plugins,
        diagnostics,
        orphaned_governance_entries,
    }
}

fn visit_plugin_dirs(
    dir: &Path,
    governance_states: &std::collections::BTreeMap<String, PluginGovernanceState>,
    plugins: &mut Vec<PluginDefinition>,
    diagnostics: &mut Vec<PluginDiagnostic>,
) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            diagnostics.push(PluginDiagnostic {
                plugin_name: None,
                manifest_path: None,
                severity: PluginDiagnosticSeverity::Error,
                code: "plugin-directory-read-failed".into(),
                message: format!("Failed to read plugin directory {}: {error}", dir.display()),
            });
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let manifest = path.join("plugin.json");
            if manifest.is_file() {
                match load_plugin_manifest(&manifest, governance_states) {
                    Ok((plugin, plugin_diagnostics)) => {
                        diagnostics.extend(plugin_diagnostics);
                        plugins.push(plugin);
                    }
                    Err(error) => diagnostics.push(PluginDiagnostic {
                        plugin_name: None,
                        manifest_path: Some(manifest.clone()),
                        severity: PluginDiagnosticSeverity::Error,
                        code: "plugin-manifest-load-failed".into(),
                        message: error.to_string(),
                    }),
                }
            }
            visit_plugin_dirs(&path, governance_states, plugins, diagnostics);
        }
    }
}

fn load_plugin_manifest(
    path: &PathBuf,
    governance_states: &std::collections::BTreeMap<String, PluginGovernanceState>,
) -> anyhow::Result<(PluginDefinition, Vec<PluginDiagnostic>)> {
    let raw = fs::read_to_string(path)?;
    let manifest: PluginManifest = serde_json::from_str(&raw)?;
    let manifest_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut diagnostics = Vec::new();
    let mut commands = Vec::new();
    let mut tools = Vec::new();
    let mut hooks = Vec::new();
    let mut capabilities = Vec::new();
    let governance = governance_states
        .get(&manifest.name)
        .cloned()
        .unwrap_or_default();
    let runtime = validate_runtime_spec(
        &manifest.name,
        path,
        manifest_dir,
        manifest.runtime.as_ref(),
        &mut diagnostics,
    );

    for capability in &manifest.capabilities {
        match parse_plugin_capability(capability) {
            Some(capability) => capabilities.push(capability),
            None => diagnostics.push(plugin_diagnostic(
                Some(manifest.name.as_str()),
                Some(path),
                PluginDiagnosticSeverity::Warning,
                "plugin-capability-unknown",
                format!("unknown plugin capability declared: {capability}"),
            )),
        }
    }

    for command in manifest.commands {
        let prompt = match load_prompt(&command.prompt, &command.prompt_file, manifest_dir) {
            Ok(prompt) => prompt,
            Err(error) => {
                diagnostics.push(plugin_diagnostic(
                    Some(manifest.name.as_str()),
                    Some(path),
                    PluginDiagnosticSeverity::Error,
                    "plugin-command-prompt-invalid",
                    format!("plugin command {}: {error}", command.name),
                ));
                continue;
            }
        };
        let availability = match parse_command_availability(command.availability.as_deref()) {
            Ok(availability) => availability,
            Err(error) => {
                diagnostics.push(plugin_diagnostic(
                    Some(manifest.name.as_str()),
                    Some(path),
                    PluginDiagnosticSeverity::Error,
                    "plugin-command-availability-invalid",
                    format!("plugin command {}: {error}", command.name),
                ));
                continue;
            }
        };
        commands.push(PluginCommandDefinition {
            plugin_name: manifest.name.clone(),
            name: command.name,
            description: command.description,
            category: command.category,
            availability,
            disable_model_invocation: command.disable_model_invocation,
            immediate: command.immediate,
            is_sensitive: command.is_sensitive,
            aliases: command.aliases,
            prompt,
            manifest_path: path.clone(),
        });
    }

    for tool in manifest.tools {
        let prompt = match load_prompt(&tool.prompt, &tool.prompt_file, manifest_dir) {
            Ok(prompt) => prompt,
            Err(error) => {
                diagnostics.push(plugin_diagnostic(
                    Some(manifest.name.as_str()),
                    Some(path),
                    PluginDiagnosticSeverity::Error,
                    "plugin-tool-prompt-invalid",
                    format!("plugin tool {}: {error}", tool.name),
                ));
                continue;
            }
        };
        tools.push(PluginToolDefinition {
            plugin_name: manifest.name.clone(),
            name: tool.name,
            description: tool.description,
            aliases: tool.aliases,
            prompt,
            search_hint: tool.search_hint,
            read_only: tool.read_only,
            destructive: tool.destructive,
            requires_auth: tool.requires_auth,
            requires_user_interaction: tool.requires_user_interaction,
            manifest_path: path.clone(),
        });
    }

    for hook in manifest.hooks {
        match normalize_hook_definition(&manifest.name, path, hook) {
            Ok(hook) => hooks.push(hook),
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
    }

    if !commands.is_empty() && !capabilities.contains(&PluginCapability::Commands) {
        diagnostics.push(plugin_diagnostic(
            Some(manifest.name.as_str()),
            Some(path),
            PluginDiagnosticSeverity::Warning,
            "plugin-capability-commands-missing",
            "plugin defines commands but does not declare commands capability; commands will remain inactive"
                .into(),
        ));
    }
    if !tools.is_empty() && !capabilities.contains(&PluginCapability::Tools) {
        diagnostics.push(plugin_diagnostic(
            Some(manifest.name.as_str()),
            Some(path),
            PluginDiagnosticSeverity::Warning,
            "plugin-capability-tools-missing",
            "plugin defines tools but does not declare tools capability; tools will remain inactive"
                .into(),
        ));
    }
    if !hooks.is_empty() && !capabilities.contains(&PluginCapability::Hooks) {
        diagnostics.push(plugin_diagnostic(
            Some(manifest.name.as_str()),
            Some(path),
            PluginDiagnosticSeverity::Warning,
            "plugin-capability-hooks-missing",
            "plugin defines hooks but does not declare hooks capability; hooks will remain inactive"
                .into(),
        ));
    }

    if capabilities.contains(&PluginCapability::Commands) && commands.is_empty() {
        diagnostics.push(plugin_diagnostic(
            Some(manifest.name.as_str()),
            Some(path),
            PluginDiagnosticSeverity::Warning,
            "plugin-capability-commands-empty",
            "plugin declares commands capability but no valid commands were loaded".into(),
        ));
    }
    if capabilities.contains(&PluginCapability::Tools) && tools.is_empty() {
        diagnostics.push(plugin_diagnostic(
            Some(manifest.name.as_str()),
            Some(path),
            PluginDiagnosticSeverity::Warning,
            "plugin-capability-tools-empty",
            "plugin declares tools capability but no valid tools were loaded".into(),
        ));
    }
    if capabilities.contains(&PluginCapability::Hooks) && hooks.is_empty() {
        diagnostics.push(plugin_diagnostic(
            Some(manifest.name.as_str()),
            Some(path),
            PluginDiagnosticSeverity::Warning,
            "plugin-capability-hooks-empty",
            "plugin declares hooks capability but no valid hooks were loaded".into(),
        ));
    }

    let diagnostics_metadata = manifest.diagnostics.map(PluginDiagnosticsMetadata::from);
    let lifecycle_state = if !governance.enabled {
        PluginLifecycleState::Disabled
    } else if diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == PluginDiagnosticSeverity::Error)
    {
        PluginLifecycleState::Error
    } else {
        PluginLifecycleState::Enabled
    };

    let apply_status = match lifecycle_state {
        PluginLifecycleState::Enabled => PluginApplyStatus::Applied,
        PluginLifecycleState::Disabled => PluginApplyStatus::SkippedDisabled,
        PluginLifecycleState::Error => PluginApplyStatus::SkippedError,
    };

    let mut plugin = PluginDefinition {
        name: manifest.name,
        version: manifest.version,
        description: manifest.description,
        manifest_path: path.clone(),
        capabilities,
        runtime,
        diagnostics_metadata,
        commands,
        tools,
        hooks,
        governance,
        lifecycle_state,
        apply_status,
        activation: PluginActivationSummary::default(),
    };
    plugin.refresh_activation_summary();

    Ok((plugin, diagnostics))
}

fn normalize_hook_definition(
    plugin_name: &str,
    manifest_path: &Path,
    hook: PluginHookManifest,
) -> Result<PluginHookDefinition, PluginDiagnostic> {
    let event = parse_hook_event_matcher(&hook.event).ok_or_else(|| {
        plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-hook-event-invalid",
            format!("unknown hook event: {}", hook.event),
        )
    })?;
    Ok(PluginHookDefinition {
        plugin_name: plugin_name.to_string(),
        event,
        deny_match: hook.deny_match,
        append_message: hook.append_message,
        prevent_continuation: hook.prevent_continuation,
        block_continuation: hook.block_continuation,
        permission_decision: hook.permission_decision,
        updated_input: hook.updated_input,
        additional_context: hook.additional_context,
        manifest_path: manifest_path.to_path_buf(),
    })
}

fn load_prompt(
    inline_prompt: &Option<String>,
    prompt_file: &Option<String>,
    manifest_dir: &Path,
) -> anyhow::Result<String> {
    match (inline_prompt, prompt_file) {
        (Some(prompt), None) => Ok(prompt.clone()),
        (None, Some(prompt_file)) => Ok(fs::read_to_string(manifest_dir.join(prompt_file))?),
        (Some(prompt), Some(_)) => Ok(prompt.clone()),
        (None, None) => anyhow::bail!("missing prompt or prompt_file"),
    }
}

fn parse_command_availability(value: Option<&str>) -> anyhow::Result<CommandAvailability> {
    Ok(match value {
        Some("cli-only") => CommandAvailability::CliOnly,
        Some("remote-safe") => CommandAvailability::RemoteSafe,
        Some("everywhere") | None => CommandAvailability::Everywhere,
        Some(other) => anyhow::bail!("unknown plugin command availability: {other}"),
    })
}

fn parse_hook_event_matcher(value: &str) -> Option<HookEventMatcher> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sessionstart" | "session_start" => Some(HookEventMatcher::SessionStart),
        "setup" => Some(HookEventMatcher::Setup),
        "userpromptsubmit" | "user_prompt_submit" => Some(HookEventMatcher::UserPromptSubmit),
        "pretooluse" | "pre_tool_use" => Some(HookEventMatcher::PreToolUse),
        "posttooluse" | "post_tool_use" => Some(HookEventMatcher::PostToolUse),
        "posttoolusefailure" | "post_tool_use_failure" => {
            Some(HookEventMatcher::PostToolUseFailure)
        }
        "permissionrequest" | "permission_request" => Some(HookEventMatcher::PermissionRequest),
        "permissiondenied" | "permission_denied" => Some(HookEventMatcher::PermissionDenied),
        "stop" => Some(HookEventMatcher::Stop),
        "subagentstop" | "subagent_stop" => Some(HookEventMatcher::SubagentStop),
        "notification" => Some(HookEventMatcher::Notification),
        _ => None,
    }
}

fn parse_plugin_capability(value: &str) -> Option<PluginCapability> {
    match value.trim().to_ascii_lowercase().as_str() {
        "commands" => Some(PluginCapability::Commands),
        "tools" => Some(PluginCapability::Tools),
        "hooks" => Some(PluginCapability::Hooks),
        _ => None,
    }
}

fn validate_runtime_spec(
    plugin_name: &str,
    manifest_path: &Path,
    manifest_dir: &Path,
    runtime: Option<&PluginRuntimeSpec>,
    diagnostics: &mut Vec<PluginDiagnostic>,
) -> Option<PluginRuntimeSpec> {
    let Some(runtime) = runtime.cloned() else {
        return None;
    };

    if let Some(timeout_ms) = runtime.timeout_ms {
        if timeout_ms == 0 {
            diagnostics.push(plugin_diagnostic(
                Some(plugin_name),
                Some(manifest_path),
                PluginDiagnosticSeverity::Error,
                "plugin-runtime-timeout-invalid",
                "runtime.timeout_ms must be > 0".into(),
            ));
        }
    }

    if let Some(output_cap_bytes) = runtime.output_cap_bytes {
        if output_cap_bytes == 0 {
            diagnostics.push(plugin_diagnostic(
                Some(plugin_name),
                Some(manifest_path),
                PluginDiagnosticSeverity::Error,
                "plugin-runtime-output-cap-invalid",
                "runtime.output_cap_bytes must be > 0".into(),
            ));
        }
    }

    match runtime.kind {
        PluginRuntimeKind::Prompt => {
            if let Some(artifact) = runtime.artifact.as_deref() {
                validate_runtime_artifact(
                    plugin_name,
                    manifest_path,
                    manifest_dir,
                    artifact,
                    false,
                )
                .map_err(|diagnostic| diagnostics.push(diagnostic))
                .ok();
            }
        }
        PluginRuntimeKind::Wasm | PluginRuntimeKind::Deno => {
            let Some(artifact) = runtime.artifact.as_deref() else {
                diagnostics.push(plugin_diagnostic(
                    Some(plugin_name),
                    Some(manifest_path),
                    PluginDiagnosticSeverity::Error,
                    "plugin-runtime-artifact-missing",
                    format!(
                        "runtime.kind={} requires runtime.artifact",
                        runtime.kind.as_str()
                    ),
                ));
                return Some(runtime);
            };
            validate_runtime_artifact(plugin_name, manifest_path, manifest_dir, artifact, true)
                .map_err(|diagnostic| diagnostics.push(diagnostic))
                .ok();
        }
    }

    Some(runtime)
}

fn validate_runtime_artifact(
    plugin_name: &str,
    manifest_path: &Path,
    manifest_dir: &Path,
    artifact: &str,
    must_exist: bool,
) -> Result<(), PluginDiagnostic> {
    let artifact_path = Path::new(artifact);
    if artifact_path.is_absolute() {
        return Err(plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-runtime-artifact-path-invalid",
            format!("runtime.artifact must be relative: {artifact}"),
        ));
    }
    if artifact_path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-runtime-artifact-path-traversal",
            format!("runtime.artifact must stay within plugin root: {artifact}"),
        ));
    }

    let resolved = manifest_dir.join(artifact_path);
    if !resolved.starts_with(manifest_dir) {
        return Err(plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-runtime-artifact-path-traversal",
            format!("runtime.artifact must stay within plugin root: {artifact}"),
        ));
    }

    if must_exist && !resolved.is_file() {
        return Err(plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-runtime-artifact-missing",
            format!("runtime.artifact not found: {}", resolved.display()),
        ));
    }

    Ok(())
}

pub fn validate_runtime_artifact_canonicalized(
    plugin_name: &str,
    manifest_path: &Path,
    manifest_dir: &Path,
    artifact: &str,
) -> Result<std::path::PathBuf, PluginDiagnostic> {
    validate_runtime_artifact(plugin_name, manifest_path, manifest_dir, artifact, true)?;

    let canonical_plugin_root = manifest_dir.canonicalize().map_err(|error| {
        plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-runtime-plugin-root-canonicalize-failed",
            format!(
                "failed to canonicalize plugin root {}: {error}",
                manifest_dir.display()
            ),
        )
    })?;
    let canonical_artifact = manifest_dir
        .join(artifact)
        .canonicalize()
        .map_err(|error| {
            plugin_diagnostic(
                Some(plugin_name),
                Some(manifest_path),
                PluginDiagnosticSeverity::Error,
                "plugin-runtime-artifact-canonicalize-failed",
                format!("failed to canonicalize runtime.artifact {artifact}: {error}"),
            )
        })?;

    if !canonical_artifact.starts_with(&canonical_plugin_root) {
        return Err(plugin_diagnostic(
            Some(plugin_name),
            Some(manifest_path),
            PluginDiagnosticSeverity::Error,
            "plugin-runtime-artifact-symlink-escape",
            format!(
                "runtime.artifact resolves outside plugin root: {}",
                canonical_artifact.display()
            ),
        ));
    }

    Ok(canonical_artifact)
}

fn plugin_diagnostic(
    plugin_name: Option<&str>,
    manifest_path: Option<&Path>,
    severity: PluginDiagnosticSeverity,
    code: &str,
    message: String,
) -> PluginDiagnostic {
    PluginDiagnostic {
        plugin_name: plugin_name.map(str::to_string),
        manifest_path: manifest_path.map(Path::to_path_buf),
        severity,
        code: code.into(),
        message,
    }
}

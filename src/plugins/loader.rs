use std::fs;
use std::path::{Path, PathBuf};

use crate::command::types::CommandAvailability;
use crate::plugins::types::{
    PluginCommandDefinition, PluginConfigSource, PluginDefinition, PluginLoadResult,
    PluginManifest,
};

pub fn load_plugins(cwd: &Path) -> PluginLoadResult {
    let root = cwd.join(".claude").join("plugins");
    let mut diagnostics = Vec::new();
    let mut plugins = Vec::new();

    if !root.exists() {
        return PluginLoadResult {
            root,
            source: PluginConfigSource::Missing,
            plugins,
            diagnostics,
        };
    }

    visit_plugin_dirs(&root, &mut plugins, &mut diagnostics);
    PluginLoadResult {
        root,
        source: PluginConfigSource::Directory,
        plugins,
        diagnostics,
    }
}

fn visit_plugin_dirs(dir: &Path, plugins: &mut Vec<PluginDefinition>, diagnostics: &mut Vec<String>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            diagnostics.push(format!("Failed to read plugin directory {}: {error}", dir.display()));
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let manifest = path.join("plugin.json");
            if manifest.is_file() {
                match load_plugin_manifest(&manifest) {
                    Ok(plugin) => plugins.push(plugin),
                    Err(error) => diagnostics.push(format!(
                        "Failed to load plugin manifest {}: {error}",
                        manifest.display()
                    )),
                }
            }
            visit_plugin_dirs(&path, plugins, diagnostics);
        }
    }
}

fn load_plugin_manifest(path: &PathBuf) -> anyhow::Result<PluginDefinition> {
    let raw = fs::read_to_string(path)?;
    let manifest: PluginManifest = serde_json::from_str(&raw)?;
    let manifest_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut commands = Vec::new();

    for command in manifest.commands {
        let prompt = match (command.prompt, command.prompt_file) {
            (Some(prompt), None) => prompt,
            (None, Some(prompt_file)) => fs::read_to_string(manifest_dir.join(prompt_file))?,
            (Some(prompt), Some(_)) => prompt,
            (None, None) => anyhow::bail!("plugin command {} is missing prompt or prompt_file", command.name),
        };
        let availability = match command.availability.as_deref() {
            Some("cli-only") => CommandAvailability::CliOnly,
            Some("remote-safe") => CommandAvailability::RemoteSafe,
            Some("everywhere") | None => CommandAvailability::Everywhere,
            Some(other) => anyhow::bail!("unknown plugin command availability: {other}"),
        };
        commands.push(PluginCommandDefinition {
            plugin_name: manifest.name.clone(),
            name: command.name,
            description: command.description,
            category: command.category,
            availability,
            disable_model_invocation: command.disable_model_invocation,
            aliases: command.aliases,
            prompt,
            manifest_path: path.clone(),
        });
    }

    Ok(PluginDefinition {
        name: manifest.name,
        description: manifest.description,
        manifest_path: path.clone(),
        commands,
    })
}

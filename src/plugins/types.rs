use std::path::PathBuf;

use serde::Deserialize;

use crate::command::types::CommandAvailability;

#[derive(Debug, Clone)]
pub struct PluginLoadResult {
    pub root: PathBuf,
    pub source: PluginConfigSource,
    pub plugins: Vec<PluginDefinition>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginConfigSource {
    Directory,
    Missing,
}

impl PluginConfigSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PluginDefinition {
    pub name: String,
    pub description: String,
    pub manifest_path: PathBuf,
    pub commands: Vec<PluginCommandDefinition>,
}

#[derive(Debug, Clone)]
pub struct PluginCommandDefinition {
    pub plugin_name: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub availability: CommandAvailability,
    pub disable_model_invocation: bool,
    pub aliases: Vec<String>,
    pub prompt: String,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginManifest {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub commands: Vec<PluginCommandManifest>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginCommandManifest {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default = "default_plugin_category")]
    pub category: String,
    pub availability: Option<String>,
    #[serde(default)]
    pub disable_model_invocation: bool,
    pub prompt: Option<String>,
    pub prompt_file: Option<String>,
}

fn default_plugin_category() -> String {
    "plugin".to_string()
}

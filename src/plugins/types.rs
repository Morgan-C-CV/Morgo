use std::path::PathBuf;

use serde::Deserialize;

use crate::command::types::CommandAvailability;
use crate::hook::registry::HookEventMatcher;

#[derive(Debug, Clone)]
pub struct PluginLoadResult {
    pub root: PathBuf,
    pub source: PluginConfigSource,
    pub plugins: Vec<PluginDefinition>,
    pub diagnostics: Vec<PluginDiagnostic>,
}

impl PluginLoadResult {
    pub fn discovered_command_count(&self) -> usize {
        self.plugins.iter().map(|plugin| plugin.commands.len()).sum()
    }

    pub fn discovered_tool_count(&self) -> usize {
        self.plugins.iter().map(|plugin| plugin.tools.len()).sum()
    }

    pub fn discovered_hook_count(&self) -> usize {
        self.plugins.iter().map(|plugin| plugin.hooks.len()).sum()
    }

    pub fn diagnostic_count_for_severity(&self, severity: PluginDiagnosticSeverity) -> usize {
        self.diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == severity)
            .count()
    }
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
    pub version: Option<String>,
    pub description: String,
    pub manifest_path: PathBuf,
    pub capabilities: Vec<String>,
    pub diagnostics_metadata: Option<PluginDiagnosticsMetadata>,
    pub commands: Vec<PluginCommandDefinition>,
    pub tools: Vec<PluginToolDefinition>,
    pub hooks: Vec<PluginHookDefinition>,
}

#[derive(Debug, Clone)]
pub struct PluginCommandDefinition {
    pub plugin_name: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub availability: CommandAvailability,
    pub disable_model_invocation: bool,
    pub immediate: bool,
    pub is_sensitive: bool,
    pub aliases: Vec<String>,
    pub prompt: String,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PluginToolDefinition {
    pub plugin_name: String,
    pub name: String,
    pub description: String,
    pub aliases: Vec<String>,
    pub prompt: String,
    pub search_hint: Option<String>,
    pub read_only: bool,
    pub destructive: bool,
    pub requires_auth: bool,
    pub requires_user_interaction: bool,
    pub manifest_path: PathBuf,
}

impl PluginToolDefinition {
    pub fn qualified_tool_name(&self) -> String {
        format!("plugin.{}.{}", sanitize_plugin_identifier(&self.plugin_name), self.name)
    }
}

#[derive(Debug, Clone)]
pub struct PluginHookDefinition {
    pub plugin_name: String,
    pub event: HookEventMatcher,
    pub deny_match: Option<String>,
    pub append_message: Option<String>,
    pub prevent_continuation: bool,
    pub permission_decision: Option<String>,
    pub updated_input: Option<String>,
    pub additional_context: Option<String>,
    pub manifest_path: PathBuf,
}

impl PluginHookDefinition {
    pub fn to_rule(&self) -> crate::hook::registry::HookRule {
        crate::hook::registry::HookRule {
            event: self.event.clone(),
            deny_match: self.deny_match.clone(),
            append_message: self.append_message.clone(),
            prevent_continuation: self.prevent_continuation,
            permission_decision: self.permission_decision.clone(),
            updated_input: self.updated_input.clone(),
            additional_context: self.additional_context.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginDiagnosticsMetadata {
    pub homepage: Option<String>,
    pub docs: Option<String>,
    pub issues: Option<String>,
    pub support_level: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginDiagnostic {
    pub plugin_name: Option<String>,
    pub manifest_path: Option<PathBuf>,
    pub severity: PluginDiagnosticSeverity,
    pub code: String,
    pub message: String,
}

impl PluginDiagnostic {
    pub fn render_line(&self) -> String {
        let plugin = self
            .plugin_name
            .as_ref()
            .map(|name| format!("plugin={name}; "))
            .unwrap_or_default();
        let manifest = self
            .manifest_path
            .as_ref()
            .map(|path| format!("manifest={}; ", path.display()))
            .unwrap_or_default();
        format!(
            "[{}:{}] {}{}{}",
            self.severity.as_str(),
            self.code,
            plugin,
            manifest,
            self.message
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

impl PluginDiagnosticSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginManifest {
    pub name: String,
    pub version: Option<String>,
    pub description: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub diagnostics: Option<PluginDiagnosticsManifest>,
    #[serde(default)]
    pub commands: Vec<PluginCommandManifest>,
    #[serde(default)]
    pub tools: Vec<PluginToolManifest>,
    #[serde(default)]
    pub hooks: Vec<PluginHookManifest>,
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
    #[serde(default)]
    pub immediate: bool,
    #[serde(default)]
    pub is_sensitive: bool,
    pub prompt: Option<String>,
    pub prompt_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginToolManifest {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub prompt: Option<String>,
    pub prompt_file: Option<String>,
    #[serde(default)]
    pub search_hint: Option<String>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub destructive: bool,
    #[serde(default)]
    pub requires_auth: bool,
    #[serde(default)]
    pub requires_user_interaction: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginHookManifest {
    pub event: String,
    #[serde(default)]
    pub deny_match: Option<String>,
    #[serde(default)]
    pub append_message: Option<String>,
    #[serde(default)]
    pub prevent_continuation: bool,
    #[serde(default)]
    pub permission_decision: Option<String>,
    #[serde(default)]
    pub updated_input: Option<String>,
    #[serde(default)]
    pub additional_context: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginDiagnosticsManifest {
    #[serde(default)]
    pub homepage: Option<String>,
    #[serde(default)]
    pub docs: Option<String>,
    #[serde(default)]
    pub issues: Option<String>,
    #[serde(default)]
    pub support_level: Option<String>,
}

impl From<PluginDiagnosticsManifest> for PluginDiagnosticsMetadata {
    fn from(value: PluginDiagnosticsManifest) -> Self {
        Self {
            homepage: value.homepage,
            docs: value.docs,
            issues: value.issues,
            support_level: value.support_level,
        }
    }
}

fn default_plugin_category() -> String {
    "plugin".to_string()
}

fn sanitize_plugin_identifier(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

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
    pub orphaned_governance_entries: Vec<String>,
}

impl PluginLoadResult {
    pub fn discovered_command_count(&self) -> usize {
        self.plugins
            .iter()
            .map(|plugin| plugin.commands.len())
            .sum()
    }

    pub fn discovered_tool_count(&self) -> usize {
        self.plugins.iter().map(|plugin| plugin.tools.len()).sum()
    }

    pub fn discovered_hook_count(&self) -> usize {
        self.plugins.iter().map(|plugin| plugin.hooks.len()).sum()
    }

    pub fn active_plugin_count(&self) -> usize {
        self.plugins
            .iter()
            .filter(|plugin| plugin.governance.enabled)
            .count()
    }

    pub fn disabled_plugin_count(&self) -> usize {
        self.plugins
            .iter()
            .filter(|plugin| !plugin.governance.enabled)
            .count()
    }

    pub fn error_plugin_count(&self) -> usize {
        self.plugins
            .iter()
            .filter(|plugin| plugin.lifecycle_state == PluginLifecycleState::Error)
            .count()
    }

    pub fn active_command_count(&self) -> usize {
        self.plugins
            .iter()
            .map(|plugin| plugin.activation.commands)
            .sum()
    }

    pub fn active_tool_count(&self) -> usize {
        self.plugins
            .iter()
            .map(|plugin| plugin.activation.tools)
            .sum()
    }

    pub fn active_hook_count(&self) -> usize {
        self.plugins
            .iter()
            .map(|plugin| plugin.activation.hooks)
            .sum()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginCapability {
    Commands,
    Tools,
    Hooks,
}

impl PluginCapability {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Commands => "commands",
            Self::Tools => "tools",
            Self::Hooks => "hooks",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginLifecycleState {
    Enabled,
    Disabled,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginApplyStatus {
    Applied,
    SkippedDisabled,
    SkippedError,
    ApplyFailed,
}

impl PluginLifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Error => "error",
        }
    }
}

impl PluginApplyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::SkippedDisabled => "skipped_disabled",
            Self::SkippedError => "skipped_error",
            Self::ApplyFailed => "apply_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginGovernanceSource {
    Default,
    File,
}

impl PluginGovernanceSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::File => "file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginGovernanceState {
    pub enabled: bool,
    pub disable_reason: Option<String>,
    pub source: PluginGovernanceSource,
}

impl Default for PluginGovernanceState {
    fn default() -> Self {
        Self {
            enabled: true,
            disable_reason: None,
            source: PluginGovernanceSource::Default,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PluginActivationSummary {
    pub commands: usize,
    pub tools: usize,
    pub hooks: usize,
}

#[derive(Debug, Clone)]
pub struct PluginDefinition {
    pub name: String,
    pub version: Option<String>,
    pub description: String,
    pub manifest_path: PathBuf,
    pub capabilities: Vec<PluginCapability>,
    pub diagnostics_metadata: Option<PluginDiagnosticsMetadata>,
    pub commands: Vec<PluginCommandDefinition>,
    pub tools: Vec<PluginToolDefinition>,
    pub hooks: Vec<PluginHookDefinition>,
    pub governance: PluginGovernanceState,
    pub lifecycle_state: PluginLifecycleState,
    pub apply_status: PluginApplyStatus,
    pub activation: PluginActivationSummary,
}

impl PluginDefinition {
    pub fn declares_capability(&self, capability: PluginCapability) -> bool {
        self.capabilities.contains(&capability)
    }

    pub fn active_commands(&self) -> Vec<PluginCommandDefinition> {
        if self.lifecycle_state == PluginLifecycleState::Enabled
            && self.governance.enabled
            && self.declares_capability(PluginCapability::Commands)
        {
            self.commands.clone()
        } else {
            Vec::new()
        }
    }

    pub fn active_tools(&self) -> Vec<PluginToolDefinition> {
        if self.lifecycle_state == PluginLifecycleState::Enabled
            && self.governance.enabled
            && self.declares_capability(PluginCapability::Tools)
        {
            self.tools.clone()
        } else {
            Vec::new()
        }
    }

    pub fn active_hooks(&self) -> Vec<PluginHookDefinition> {
        if self.lifecycle_state == PluginLifecycleState::Enabled
            && self.governance.enabled
            && self.declares_capability(PluginCapability::Hooks)
        {
            self.hooks.clone()
        } else {
            Vec::new()
        }
    }

    pub fn refresh_activation_summary(&mut self) {
        self.activation = PluginActivationSummary {
            commands: self.active_commands().len(),
            tools: self.active_tools().len(),
            hooks: self.active_hooks().len(),
        };
    }
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
        format!(
            "plugin.{}.{}",
            sanitize_plugin_identifier(&self.plugin_name),
            self.name
        )
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginRuntimeApplyOutcome {
    Applied,
    RetainedPreviousSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginRuntimeApplyReport {
    pub outcome: PluginRuntimeApplyOutcome,
    pub generation: u64,
    pub message: String,
    pub diagnostics: Vec<PluginDiagnostic>,
    pub orphaned_governance_entries: Vec<String>,
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

impl PluginRuntimeApplyOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::RetainedPreviousSnapshot => "retained_previous_snapshot",
        }
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

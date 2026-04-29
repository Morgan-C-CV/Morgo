use crate::plugins::types::{
    PluginCapability, PluginDefinition, PluginDiagnostic, PluginDiagnosticSeverity,
    PluginLifecycleState, PluginLoadResult,
};
use crate::skills::visibility::{
    SkillActivationDecision, SkillConflictRecord, SkillVisibilityResult,
};

// ── Capability block reasons ──────────────────────────────────────────────────

/// Why a plugin capability is not active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityBlockReason {
    /// Plugin governance is disabled (admin/user explicitly disabled it).
    GovernanceDisabled { reason: Option<String> },
    /// Plugin lifecycle is in error state (load or apply failure).
    LifecycleError,
    /// Plugin does not declare this capability in its manifest.
    CapabilityNotDeclared,
    /// Plugin is active but the capability produced zero active items.
    NoActiveItems,
}

impl CapabilityBlockReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GovernanceDisabled { .. } => "governance_disabled",
            Self::LifecycleError => "lifecycle_error",
            Self::CapabilityNotDeclared => "capability_not_declared",
            Self::NoActiveItems => "no_active_items",
        }
    }

    pub fn render_line(&self) -> String {
        match self {
            Self::GovernanceDisabled { reason: Some(r) } => {
                format!("governance_disabled: {r}")
            }
            Self::GovernanceDisabled { reason: None } => "governance_disabled".to_string(),
            Self::LifecycleError => "lifecycle_error".to_string(),
            Self::CapabilityNotDeclared => "capability_not_declared".to_string(),
            Self::NoActiveItems => "no_active_items".to_string(),
        }
    }
}

// ── Per-plugin capability status ──────────────────────────────────────────────

/// Activation status for a single capability of a single plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginCapabilityStatus {
    Active { item_count: usize },
    Blocked(CapabilityBlockReason),
}

impl PluginCapabilityStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active { .. } => "active",
            Self::Blocked(_) => "blocked",
        }
    }
}

/// Full capability picture for one plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginCapabilityRecord {
    pub plugin_name: String,
    pub commands: PluginCapabilityStatus,
    pub tools: PluginCapabilityStatus,
    pub hooks: PluginCapabilityStatus,
}

impl PluginCapabilityRecord {
    pub fn any_active(&self) -> bool {
        self.commands.is_active() || self.tools.is_active() || self.hooks.is_active()
    }

    pub fn render_summary_line(&self) -> String {
        format!(
            "{}: commands={}, tools={}, hooks={}",
            self.plugin_name,
            render_capability_status(&self.commands),
            render_capability_status(&self.tools),
            render_capability_status(&self.hooks),
        )
    }
}

fn render_capability_status(status: &PluginCapabilityStatus) -> String {
    match status {
        PluginCapabilityStatus::Active { item_count } => format!("active({item_count})"),
        PluginCapabilityStatus::Blocked(reason) => format!("blocked({})", reason.as_str()),
    }
}

// ── Skill conflict summary ────────────────────────────────────────────────────

/// Human-readable summary of a skill name collision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillConflictSummary {
    pub skill_name: String,
    pub winner_source: String,
    pub shadowed_sources: Vec<String>,
}

impl SkillConflictSummary {
    pub fn render_line(&self) -> String {
        format!(
            "skill '{}': winner={}, shadowed=[{}]",
            self.skill_name,
            self.winner_source,
            self.shadowed_sources.join(", ")
        )
    }
}

// ── Unified diagnostic report ─────────────────────────────────────────────────

/// Aggregated capability + activation + conflict diagnostic report.
///
/// Intended for consumption by the boss path, `/status`, and any surface that
/// needs to explain "why is X not available" without parsing raw plugin structs.
#[derive(Debug, Clone, Default)]
pub struct PluginDiagnosticReport {
    /// Per-plugin capability records.
    pub capability_records: Vec<PluginCapabilityRecord>,
    /// Skill name-collision conflicts from the visibility resolver.
    pub skill_conflicts: Vec<SkillConflictSummary>,
    /// Skill definitions that are explicitly disabled (hidden = true).
    pub disabled_skill_names: Vec<String>,
    /// Plugin-level diagnostics (errors/warnings from load/apply).
    pub plugin_diagnostics: Vec<PluginDiagnostic>,
    /// Orphaned governance entries (plugin names in state file with no matching plugin).
    pub orphaned_governance_entries: Vec<String>,
}

impl PluginDiagnosticReport {
    pub fn active_plugin_count(&self) -> usize {
        self.capability_records
            .iter()
            .filter(|r| r.any_active())
            .count()
    }

    pub fn blocked_plugin_count(&self) -> usize {
        self.capability_records
            .iter()
            .filter(|r| !r.any_active())
            .count()
    }

    pub fn error_diagnostic_count(&self) -> usize {
        self.plugin_diagnostics
            .iter()
            .filter(|d| d.severity == PluginDiagnosticSeverity::Error)
            .count()
    }

    pub fn warning_diagnostic_count(&self) -> usize {
        self.plugin_diagnostics
            .iter()
            .filter(|d| d.severity == PluginDiagnosticSeverity::Warning)
            .count()
    }

    pub fn has_issues(&self) -> bool {
        self.error_diagnostic_count() > 0
            || !self.skill_conflicts.is_empty()
            || self.blocked_plugin_count() > 0
    }

    /// One-line summary suitable for boss path observability output.
    pub fn render_summary(&self) -> String {
        format!(
            "plugins: active={}, blocked={}, errors={}, warnings={}, skill_conflicts={}, disabled_skills={}",
            self.active_plugin_count(),
            self.blocked_plugin_count(),
            self.error_diagnostic_count(),
            self.warning_diagnostic_count(),
            self.skill_conflicts.len(),
            self.disabled_skill_names.len(),
        )
    }

    /// Multi-line diagnostic output for `/status` or boss path detail view.
    pub fn render_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        lines.push(self.render_summary());

        for record in &self.capability_records {
            lines.push(record.render_summary_line());
        }

        for conflict in &self.skill_conflicts {
            lines.push(format!("conflict: {}", conflict.render_line()));
        }

        for name in &self.disabled_skill_names {
            lines.push(format!("disabled_skill: {name}"));
        }

        for diagnostic in &self.plugin_diagnostics {
            lines.push(format!("diagnostic: {}", diagnostic.render_line()));
        }

        for entry in &self.orphaned_governance_entries {
            lines.push(format!("orphaned_governance: {entry}"));
        }

        lines
    }
}

// ── Builder functions ─────────────────────────────────────────────────────────

/// Build a `PluginCapabilityRecord` for a single plugin definition.
pub fn build_capability_record(plugin: &PluginDefinition) -> PluginCapabilityRecord {
    PluginCapabilityRecord {
        plugin_name: plugin.name.clone(),
        commands: capability_status(
            plugin,
            PluginCapability::Commands,
            plugin.activation.commands,
        ),
        tools: capability_status(plugin, PluginCapability::Tools, plugin.activation.tools),
        hooks: capability_status(plugin, PluginCapability::Hooks, plugin.activation.hooks),
    }
}

fn capability_status(
    plugin: &PluginDefinition,
    capability: PluginCapability,
    active_count: usize,
) -> PluginCapabilityStatus {
    if !plugin.governance.enabled {
        return PluginCapabilityStatus::Blocked(CapabilityBlockReason::GovernanceDisabled {
            reason: plugin.governance.disable_reason.clone(),
        });
    }
    if plugin.lifecycle_state == PluginLifecycleState::Error {
        return PluginCapabilityStatus::Blocked(CapabilityBlockReason::LifecycleError);
    }
    if !plugin.declares_capability(capability) {
        return PluginCapabilityStatus::Blocked(CapabilityBlockReason::CapabilityNotDeclared);
    }
    if active_count == 0 {
        return PluginCapabilityStatus::Blocked(CapabilityBlockReason::NoActiveItems);
    }
    PluginCapabilityStatus::Active {
        item_count: active_count,
    }
}

/// Build a `PluginDiagnosticReport` from a `PluginLoadResult` and optional skill visibility.
pub fn build_diagnostic_report(
    load_result: &PluginLoadResult,
    skill_visibility: Option<&SkillVisibilityResult>,
) -> PluginDiagnosticReport {
    let capability_records = load_result
        .plugins
        .iter()
        .map(build_capability_record)
        .collect();

    let (skill_conflicts, disabled_skill_names) = match skill_visibility {
        Some(visibility) => {
            let conflicts = visibility
                .conflicts
                .iter()
                .map(|c| SkillConflictSummary {
                    skill_name: c.name.clone(),
                    winner_source: c.winner_source.as_str().to_string(),
                    shadowed_sources: c
                        .shadowed_sources
                        .iter()
                        .map(|s| s.as_str().to_string())
                        .collect(),
                })
                .collect();

            let disabled = visibility
                .decisions
                .iter()
                .filter_map(|d| match d {
                    SkillActivationDecision::Disabled(s) => Some(s.name.clone()),
                    _ => None,
                })
                .collect();

            (conflicts, disabled)
        }
        None => (Vec::new(), Vec::new()),
    };

    PluginDiagnosticReport {
        capability_records,
        skill_conflicts,
        disabled_skill_names,
        plugin_diagnostics: load_result.diagnostics.clone(),
        orphaned_governance_entries: load_result.orphaned_governance_entries.clone(),
    }
}

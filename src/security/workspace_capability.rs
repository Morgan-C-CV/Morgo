use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

// ── Capability tiers ──────────────────────────────────────────────────────────

/// The capability tier required to perform a bash action.
/// Ordered: Read < Write < AdminBash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityTier {
    /// Read-only operations: cat, ls, grep, find, etc.
    Read,
    /// Workspace-scoped writes: edit files within the project directory.
    Write,
    /// Unrestricted shell: destructive patterns, shell operators, out-of-scope paths.
    AdminBash,
}

impl CapabilityTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::AdminBash => "admin_bash",
        }
    }
}

// ── Per-directory scope entry ─────────────────────────────────────────────────

/// A capability grant scoped to a directory prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCapabilityScope {
    /// Absolute path prefix this scope applies to.
    pub directory: PathBuf,
    /// Maximum tier allowed within this directory.
    pub max_tier: CapabilityTier,
}

// ── Workspace capability config ───────────────────────────────────────────────

/// Workspace-level capability configuration.
///
/// Defines the maximum bash capability tier allowed globally and optionally
/// per directory. The most-specific matching scope wins; global_max_tier is
/// the fallback when no scope matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCapabilityConfig {
    /// Global ceiling — applies when no directory scope matches.
    pub global_max_tier: CapabilityTier,
    /// Per-directory overrides, matched by longest prefix.
    #[serde(default)]
    pub scopes: Vec<WorkspaceCapabilityScope>,
    /// When true, any command that requires escalation beyond the allowed tier
    /// is routed to PendingApproval rather than denied outright.
    #[serde(default = "default_true")]
    pub escalate_to_pending_approval: bool,
    /// When true, all capability decisions are written to the audit log.
    #[serde(default)]
    pub audit_capability_decisions: bool,
}

fn default_true() -> bool {
    true
}

impl Default for WorkspaceCapabilityConfig {
    fn default() -> Self {
        Self {
            global_max_tier: CapabilityTier::Write,
            scopes: vec![],
            escalate_to_pending_approval: true,
            audit_capability_decisions: false,
        }
    }
}

impl WorkspaceCapabilityConfig {
    /// Beta deny-by-default preset: only read operations are allowed without approval.
    pub fn beta_deny_by_default() -> Self {
        Self {
            global_max_tier: CapabilityTier::Read,
            scopes: vec![],
            escalate_to_pending_approval: true,
            audit_capability_decisions: true,
        }
    }

    /// Resolve the effective max tier for a given working directory.
    /// Longest matching directory prefix wins; falls back to global_max_tier.
    pub fn effective_max_tier(&self, cwd: &Path) -> CapabilityTier {
        let mut best: Option<(&WorkspaceCapabilityScope, usize)> = None;
        for scope in &self.scopes {
            if cwd == scope.directory || cwd.starts_with(&scope.directory) {
                let depth = scope.directory.components().count();
                if best.map_or(true, |(_, best_depth)| depth > best_depth) {
                    best = Some((scope, depth));
                }
            }
        }
        best.map(|(scope, _)| scope.max_tier).unwrap_or(self.global_max_tier)
    }

    pub fn load_from_json(json: &str) -> anyhow::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("failed to parse workspace capability config: {e}"))
    }
}

// ── Required tier classification ──────────────────────────────────────────────

/// Why a command requires a particular capability tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityRequirementReason {
    ReadOnlyCommand,
    WorkspaceWrite,
    DestructivePattern,
    ShellOperator,
    OutOfScopePath,
    EscalationRequired,
}

impl CapabilityRequirementReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnlyCommand => "read_only_command",
            Self::WorkspaceWrite => "workspace_write",
            Self::DestructivePattern => "destructive_pattern",
            Self::ShellOperator => "shell_operator",
            Self::OutOfScopePath => "out_of_scope_path",
            Self::EscalationRequired => "escalation_required",
        }
    }
}

/// The capability tier a command requires, with the primary reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandCapabilityRequirement {
    pub required_tier: CapabilityTier,
    pub reason: CapabilityRequirementReason,
}

impl CommandCapabilityRequirement {
    pub fn read() -> Self {
        Self { required_tier: CapabilityTier::Read, reason: CapabilityRequirementReason::ReadOnlyCommand }
    }

    pub fn write() -> Self {
        Self { required_tier: CapabilityTier::Write, reason: CapabilityRequirementReason::WorkspaceWrite }
    }

    pub fn admin_bash(reason: CapabilityRequirementReason) -> Self {
        Self { required_tier: CapabilityTier::AdminBash, reason }
    }
}

// ── Capability check result ───────────────────────────────────────────────────

/// Outcome of checking a command against the workspace capability config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityCheckOutcome {
    /// Command is within the allowed tier — proceed.
    Allowed,
    /// Command exceeds the allowed tier and should be routed to PendingApproval.
    RequiresApproval {
        required_tier: CapabilityTier,
        allowed_tier: CapabilityTier,
        reason: CapabilityRequirementReason,
    },
    /// Command exceeds the allowed tier and escalation is disabled — deny outright.
    Denied {
        required_tier: CapabilityTier,
        allowed_tier: CapabilityTier,
        reason: CapabilityRequirementReason,
    },
}

impl CapabilityCheckOutcome {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    pub fn requires_approval(&self) -> bool {
        matches!(self, Self::RequiresApproval { .. })
    }

    pub fn is_denied(&self) -> bool {
        matches!(self, Self::Denied { .. })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::RequiresApproval { .. } => "requires_approval",
            Self::Denied { .. } => "denied",
        }
    }

    pub fn render_line(&self) -> String {
        match self {
            Self::Allowed => "capability_check: allowed".into(),
            Self::RequiresApproval { required_tier, allowed_tier, reason } => format!(
                "capability_check: requires_approval required={} allowed={} reason={}",
                required_tier.as_str(),
                allowed_tier.as_str(),
                reason.as_str(),
            ),
            Self::Denied { required_tier, allowed_tier, reason } => format!(
                "capability_check: denied required={} allowed={} reason={}",
                required_tier.as_str(),
                allowed_tier.as_str(),
                reason.as_str(),
            ),
        }
    }
}

// ── Pure check function ───────────────────────────────────────────────────────

/// Check whether `requirement` is satisfied by `config` for the given `cwd`.
///
/// This is a pure function — no I/O, no side effects.
pub fn check_bash_capability(
    requirement: &CommandCapabilityRequirement,
    config: &WorkspaceCapabilityConfig,
    cwd: &Path,
) -> CapabilityCheckOutcome {
    let allowed_tier = config.effective_max_tier(cwd);
    if requirement.required_tier <= allowed_tier {
        return CapabilityCheckOutcome::Allowed;
    }
    if config.escalate_to_pending_approval {
        CapabilityCheckOutcome::RequiresApproval {
            required_tier: requirement.required_tier,
            allowed_tier,
            reason: requirement.reason.clone(),
        }
    } else {
        CapabilityCheckOutcome::Denied {
            required_tier: requirement.required_tier,
            allowed_tier,
            reason: requirement.reason.clone(),
        }
    }
}

/// Derive the capability requirement from a `BashPolicyDecision`.
///
/// Maps the existing policy analysis onto the three-tier model:
/// - read_only && path_safe && no escalation → Read
/// - requires_escalation (destructive/operator/path) → AdminBash
/// - otherwise → Write
pub fn requirement_from_policy(
    policy: &crate::tool::builtin::bash::permissions::BashPolicyDecision,
) -> CommandCapabilityRequirement {
    if policy.read_only && policy.path_safe && !policy.requires_escalation {
        return CommandCapabilityRequirement::read();
    }
    if policy.requires_escalation {
        // Pick the most specific reason from escalation_reasons
        let reason = if policy.escalation_reasons.iter().any(|r| r.contains("destructive")) {
            CapabilityRequirementReason::DestructivePattern
        } else if policy.escalation_reasons.iter().any(|r| r.contains("shell_operator") || r.starts_with("pipe") || r.starts_with("redirect") || r.starts_with("subshell") || r.starts_with("background")) {
            CapabilityRequirementReason::ShellOperator
        } else if !policy.path_safe {
            CapabilityRequirementReason::OutOfScopePath
        } else {
            CapabilityRequirementReason::EscalationRequired
        };
        return CommandCapabilityRequirement::admin_bash(reason);
    }
    CommandCapabilityRequirement::write()
}

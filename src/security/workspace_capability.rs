use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// ── Workspace permissions ────────────────────────────────────────────────────

pub const WORKSPACE_PERMISSIONS_FILENAME: &str = "workspace-permissions.json";
pub const LEGACY_WORKSPACE_CAPABILITY_FILENAME: &str = "workspace-capability.json";

/// Persistent workspace permission grant.
///
/// Ordered: View < Edit < Worker < Admin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspacePermissionLevel {
    View,
    Edit,
    Worker,
    Admin,
}

impl WorkspacePermissionLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Edit => "edit",
            Self::Worker => "worker",
            Self::Admin => "admin",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "view" | "read" => Some(Self::View),
            "edit" => Some(Self::Edit),
            "worker" | "write" => Some(Self::Worker),
            "admin" | "admin_bash" => Some(Self::Admin),
            _ => None,
        }
    }
}

impl std::fmt::Display for WorkspacePermissionLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePermissionEntry {
    pub path: PathBuf,
    pub permission: WorkspacePermissionLevel,
    pub trusted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePermissionConfig {
    pub version: u32,
    #[serde(default)]
    pub workspaces: Vec<WorkspacePermissionEntry>,
}

impl Default for WorkspacePermissionConfig {
    fn default() -> Self {
        Self {
            version: 1,
            workspaces: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePermissionMatch {
    pub path: PathBuf,
    pub permission: WorkspacePermissionLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspacePermissionCheck {
    Allowed {
        matched_path: PathBuf,
        permission: WorkspacePermissionLevel,
    },
    RequiresApproval {
        target_path: PathBuf,
        required: WorkspacePermissionLevel,
        current: Option<WorkspacePermissionLevel>,
        matched_path: Option<PathBuf>,
        reason: String,
    },
}

impl WorkspacePermissionConfig {
    pub fn load_from_json(json: &str) -> anyhow::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("failed to parse workspace permission config: {e}"))
    }

    pub fn load_from_path(path: &Path) -> anyhow::Result<Self> {
        let json = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!(
                "failed to read workspace permission config {}: {error}",
                path.display()
            )
        })?;
        Self::load_from_json(&json)
    }

    pub fn save_to_path(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                anyhow::anyhow!(
                    "failed to create workspace permission config directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, format!("{json}\n")).map_err(|error| {
            anyhow::anyhow!(
                "failed to write workspace permission config {}: {error}",
                path.display()
            )
        })?;
        set_user_read_write(path);
        Ok(())
    }

    pub fn effective_permission(&self, target: &Path) -> Option<WorkspacePermissionMatch> {
        let target = normalize_existing_or_create_path(target);
        self.workspaces
            .iter()
            .filter_map(|entry| {
                let path = normalize_existing_or_create_path(&entry.path);
                if target == path || target.starts_with(&path) {
                    Some(WorkspacePermissionMatch {
                        path,
                        permission: entry.permission,
                    })
                } else {
                    None
                }
            })
            .max_by_key(|entry| entry.path.components().count())
    }

    pub fn check_path(
        &self,
        target: &Path,
        required: WorkspacePermissionLevel,
    ) -> WorkspacePermissionCheck {
        let target_path = normalize_existing_or_create_path(target);
        match self.effective_permission(&target_path) {
            Some(matched) if matched.permission >= required => WorkspacePermissionCheck::Allowed {
                matched_path: matched.path,
                permission: matched.permission,
            },
            Some(matched) => WorkspacePermissionCheck::RequiresApproval {
                target_path,
                required,
                current: Some(matched.permission),
                matched_path: Some(matched.path),
                reason: "workspace_permission_insufficient".into(),
            },
            None => WorkspacePermissionCheck::RequiresApproval {
                target_path,
                required,
                current: None,
                matched_path: None,
                reason: "workspace_untrusted".into(),
            },
        }
    }

    pub fn trust_workspace(
        &mut self,
        path: impl AsRef<Path>,
        permission: WorkspacePermissionLevel,
    ) {
        let path = normalize_existing_or_create_path(path.as_ref());
        let trusted_at = rfc3339_like_now();
        if let Some(entry) = self
            .workspaces
            .iter_mut()
            .find(|entry| normalize_existing_or_create_path(&entry.path) == path)
        {
            entry.permission = permission;
            entry.trusted_at = trusted_at;
            entry.path = path;
            return;
        }
        self.workspaces.push(WorkspacePermissionEntry {
            path,
            permission,
            trusted_at,
        });
        self.workspaces
            .sort_by(|left, right| left.path.cmp(&right.path));
    }
}

pub fn default_workspace_permissions_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join(".morgo")
            .join(WORKSPACE_PERMISSIONS_FILENAME)
    })
}

pub fn load_global_workspace_permissions() -> anyhow::Result<WorkspacePermissionConfig> {
    let Some(path) = default_workspace_permissions_path() else {
        return Ok(WorkspacePermissionConfig::default());
    };
    if !path.exists() {
        return Ok(WorkspacePermissionConfig::default());
    }
    WorkspacePermissionConfig::load_from_path(&path)
}

pub fn save_global_workspace_permissions(config: &WorkspacePermissionConfig) -> anyhow::Result<()> {
    let Some(path) = default_workspace_permissions_path() else {
        anyhow::bail!("HOME is not set; cannot save workspace permissions")
    };
    config.save_to_path(&path)
}

pub fn normalize_existing_or_create_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    if absolute.exists() {
        return std::fs::canonicalize(&absolute)
            .unwrap_or_else(|_| normalize_path_lexically(&absolute));
    }
    let mut current = absolute.as_path();
    loop {
        if current.exists() {
            if let Ok(canonical_base) = std::fs::canonicalize(current) {
                if let Ok(remainder) = absolute.strip_prefix(current) {
                    return normalize_path_lexically(&canonical_base.join(remainder));
                }
            }
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }
    normalize_path_lexically(&absolute)
}

pub fn workspace_permission_from_capability_tier(tier: CapabilityTier) -> WorkspacePermissionLevel {
    match tier {
        CapabilityTier::Read => WorkspacePermissionLevel::View,
        CapabilityTier::Write => WorkspacePermissionLevel::Worker,
        CapabilityTier::AdminBash => WorkspacePermissionLevel::Admin,
    }
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn rfc3339_like_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let days = (secs / 86_400) as i64;
    let seconds_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

#[cfg(unix)]
fn set_user_read_write(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = std::fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        let _ = std::fs::set_permissions(path, permissions);
    }
}

#[cfg(not(unix))]
fn set_user_read_write(_path: &Path) {}

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
        best.map(|(scope, _)| scope.max_tier)
            .unwrap_or(self.global_max_tier)
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
        Self {
            required_tier: CapabilityTier::Read,
            reason: CapabilityRequirementReason::ReadOnlyCommand,
        }
    }

    pub fn write() -> Self {
        Self {
            required_tier: CapabilityTier::Write,
            reason: CapabilityRequirementReason::WorkspaceWrite,
        }
    }

    pub fn admin_bash(reason: CapabilityRequirementReason) -> Self {
        Self {
            required_tier: CapabilityTier::AdminBash,
            reason,
        }
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
            Self::RequiresApproval {
                required_tier,
                allowed_tier,
                reason,
            } => format!(
                "capability_check: requires_approval required={} allowed={} reason={}",
                required_tier.as_str(),
                allowed_tier.as_str(),
                reason.as_str(),
            ),
            Self::Denied {
                required_tier,
                allowed_tier,
                reason,
            } => format!(
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
        let reason = if policy
            .escalation_reasons
            .iter()
            .any(|r| r.contains("destructive"))
        {
            CapabilityRequirementReason::DestructivePattern
        } else if policy.escalation_reasons.iter().any(|r| {
            r.contains("shell_operator")
                || r.starts_with("pipe")
                || r.starts_with("redirect")
                || r.starts_with("subshell")
                || r.starts_with("background")
        }) {
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

#[cfg(test)]
mod workspace_permission_tests {
    use super::{
        WorkspacePermissionCheck, WorkspacePermissionConfig, WorkspacePermissionEntry,
        WorkspacePermissionLevel, civil_from_days,
    };
    use std::path::Path;

    #[test]
    fn workspace_permissions_longest_prefix_wins() {
        let config = WorkspacePermissionConfig {
            version: 1,
            workspaces: vec![
                WorkspacePermissionEntry {
                    path: "/project".into(),
                    permission: WorkspacePermissionLevel::Worker,
                    trusted_at: "2026-05-19T00:00:00Z".into(),
                },
                WorkspacePermissionEntry {
                    path: "/project/readonly".into(),
                    permission: WorkspacePermissionLevel::View,
                    trusted_at: "2026-05-19T00:00:00Z".into(),
                },
            ],
        };

        assert_eq!(
            config
                .effective_permission(Path::new("/project/src/lib.rs"))
                .unwrap()
                .permission,
            WorkspacePermissionLevel::Worker
        );
        assert_eq!(
            config
                .effective_permission(Path::new("/project/readonly/data.txt"))
                .unwrap()
                .permission,
            WorkspacePermissionLevel::View
        );
        assert!(config.effective_permission(Path::new("/other")).is_none());
    }

    #[test]
    fn workspace_permission_check_unmatched_requires_approval() {
        let config = WorkspacePermissionConfig::default();
        let outcome = config.check_path(
            Path::new("/untrusted/file.txt"),
            WorkspacePermissionLevel::View,
        );
        assert!(matches!(
            outcome,
            WorkspacePermissionCheck::RequiresApproval {
                reason,
                current: None,
                ..
            } if reason == "workspace_untrusted"
        ));
    }

    #[test]
    fn trust_workspace_updates_existing_entry() {
        let mut config = WorkspacePermissionConfig::default();
        config.trust_workspace("/project", WorkspacePermissionLevel::View);
        config.trust_workspace("/project", WorkspacePermissionLevel::Worker);

        assert_eq!(config.workspaces.len(), 1);
        assert_eq!(
            config.workspaces[0].permission,
            WorkspacePermissionLevel::Worker
        );
    }

    #[test]
    fn civil_from_days_matches_unix_epoch() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_593), (2026, 5, 20));
    }
}

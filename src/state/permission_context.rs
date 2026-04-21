use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::bootstrap::InteractionSurface;
use crate::hook::registry::HookRegistry;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::plan::manager::PlanManager;
use crate::plugins::runtime_state::RuntimePluginState;
use crate::security::authorizer::SurfaceAdmissionPolicy;
use crate::security::filesystem_policy::FilesystemPolicy;
use crate::service::mcp::runtime::McpRuntime;
use crate::skills::registry::SkillRegistry;
use crate::task::list_manager::TaskListManager;
use crate::task::manager::TaskManager;
use crate::tool::registry::ToolRegistry;
use std::sync::atomic::AtomicU64;
use crate::core::concurrency::SubagentLimiter;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionMode {
    Default,
    AcceptEdits,
    BypassPermissions,
    Plan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingApproval {
    pub tool_name: String,
    pub tool_input: String,
    pub message: String,
    pub code: Option<String>,
    pub summary: Option<String>,
    pub detail: Option<String>,
    pub approval_kind: Option<String>,
    pub escalation_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ToolPermissionContext {
    mode: Arc<RwLock<PermissionMode>>,
    always_allow_rules: Arc<RwLock<Vec<String>>>,
    always_deny_rules: Arc<RwLock<Vec<String>>>,
    always_ask_rules: Arc<RwLock<Vec<String>>>,
    pub include_deferred_tools: bool,
    pub include_interactive_tools: bool,
    pub task_manager: Option<Arc<TaskManager>>,
    pub task_list_manager: Option<Arc<TaskListManager>>,
    pub plan_manager: Option<Arc<PlanManager>>,
    pub skill_registry: Option<Arc<SkillRegistry>>,
    pub mcp_runtime: Option<Arc<McpRuntime>>,
    pub active_session_id: Option<String>,
    pub active_surface: Option<InteractionSurface>,
    pub notification_dispatcher: Option<NotificationDispatcher>,
    pub pending_approval: Arc<RwLock<Option<PendingApproval>>>,
    pub filesystem_policy: Option<Arc<FilesystemPolicy>>,
    pub subagent_scripted_turns: Option<Vec<Vec<crate::service::api::streaming::StreamEvent>>>,
    pub inherited_tool_registry: Option<ToolRegistry>,
    pub inherited_hook_registry: Option<HookRegistry>,
    pub runtime_plugin_state: Option<RuntimePluginState>,
    remote_surface_admission_policy: Arc<RwLock<SurfaceAdmissionPolicy>>,
    telegram_surface_admission_policy: Arc<RwLock<SurfaceAdmissionPolicy>>,
    pub external_memory_entries: Arc<RwLock<Vec<String>>>,
    pub nested_memory_lineage: Arc<RwLock<Vec<String>>>,
    pub last_activity_ts: Option<Arc<AtomicU64>>,
    pub cancellation_token: Option<CancellationToken>,
    pub subagent_limiter: Option<Arc<SubagentLimiter>>,
}

impl ToolPermissionContext {
    pub fn new(mode: PermissionMode) -> Self {
        Self {
            mode: Arc::new(RwLock::new(mode)),
            always_allow_rules: Arc::new(RwLock::new(Vec::new())),
            always_deny_rules: Arc::new(RwLock::new(Vec::new())),
            always_ask_rules: Arc::new(RwLock::new(Vec::new())),
            include_deferred_tools: false,
            include_interactive_tools: true,
            task_manager: None,
            task_list_manager: None,
            plan_manager: None,
            skill_registry: None,
            mcp_runtime: None,
            active_session_id: None,
            active_surface: None,
            notification_dispatcher: None,
            pending_approval: Arc::new(RwLock::new(None)),
            filesystem_policy: None,
            subagent_scripted_turns: None,
            inherited_tool_registry: None,
            inherited_hook_registry: None,
            runtime_plugin_state: None,
            remote_surface_admission_policy: Arc::new(RwLock::new(
                SurfaceAdmissionPolicy::default(),
            )),
            telegram_surface_admission_policy: Arc::new(RwLock::new(
                SurfaceAdmissionPolicy::default(),
            )),
            external_memory_entries: Arc::new(RwLock::new(Vec::new())),
            nested_memory_lineage: Arc::new(RwLock::new(Vec::new())),
            last_activity_ts: None,
            cancellation_token: None,
            subagent_limiter: None,
        }
    }

    pub fn with_task_manager(mut self, task_manager: Arc<TaskManager>) -> Self {
        self.task_manager = Some(task_manager);
        self
    }

    pub fn always_allow_rules(&self) -> Vec<String> {
        self.always_allow_rules
            .read()
            .map(|rules| rules.clone())
            .unwrap_or_default()
    }

    pub fn always_deny_rules(&self) -> Vec<String> {
        self.always_deny_rules
            .read()
            .map(|rules| rules.clone())
            .unwrap_or_default()
    }

    pub fn always_ask_rules(&self) -> Vec<String> {
        self.always_ask_rules
            .read()
            .map(|rules| rules.clone())
            .unwrap_or_default()
    }

    pub fn add_always_allow_rule(&self, rule: impl Into<String>) -> bool {
        add_rule(&self.always_allow_rules, rule)
    }

    pub fn add_always_deny_rule(&self, rule: impl Into<String>) -> bool {
        add_rule(&self.always_deny_rules, rule)
    }

    pub fn add_always_ask_rule(&self, rule: impl Into<String>) -> bool {
        add_rule(&self.always_ask_rules, rule)
    }

    pub fn with_task_list_manager(mut self, task_list_manager: Arc<TaskListManager>) -> Self {
        self.task_list_manager = Some(task_list_manager);
        self
    }

    pub fn with_plan_manager(mut self, plan_manager: Arc<PlanManager>) -> Self {
        self.plan_manager = Some(plan_manager);
        self
    }

    pub fn with_skill_registry(mut self, skill_registry: Arc<SkillRegistry>) -> Self {
        self.skill_registry = Some(skill_registry);
        self
    }

    pub fn with_mcp_runtime(mut self, mcp_runtime: Arc<McpRuntime>) -> Self {
        self.mcp_runtime = Some(mcp_runtime);
        self
    }

    pub fn with_active_session_id(mut self, active_session_id: impl Into<String>) -> Self {
        self.active_session_id = Some(active_session_id.into());
        self
    }

    pub fn with_active_surface(mut self, active_surface: InteractionSurface) -> Self {
        self.active_surface = Some(active_surface);
        self
    }

    pub fn with_notification_dispatcher(
        mut self,
        notification_dispatcher: NotificationDispatcher,
    ) -> Self {
        self.notification_dispatcher = Some(notification_dispatcher);
        self
    }

    pub fn with_pending_approval(self, pending_approval: PendingApproval) -> Self {
        if let Ok(mut slot) = self.pending_approval.write() {
            *slot = Some(pending_approval);
        }
        self
    }

    pub fn with_filesystem_policy(mut self, filesystem_policy: Arc<FilesystemPolicy>) -> Self {
        self.filesystem_policy = Some(filesystem_policy);
        self
    }

    pub fn filesystem_policy(&self) -> Option<Arc<FilesystemPolicy>> {
        self.filesystem_policy.clone()
    }

    pub fn mode(&self) -> PermissionMode {
        self.mode
            .read()
            .map(|mode| *mode)
            .unwrap_or(PermissionMode::Default)
    }

    pub fn set_mode(&self, mode: PermissionMode) {
        if let Ok(mut slot) = self.mode.write() {
            *slot = mode;
        }
    }

    pub fn set_pending_approval(&self, pending_approval: Option<PendingApproval>) {
        if let Ok(mut slot) = self.pending_approval.write() {
            *slot = pending_approval;
        }
    }

    pub fn pending_approval(&self) -> Option<PendingApproval> {
        self.pending_approval
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    pub fn with_subagent_scripted_turns(
        mut self,
        scripted_turns: Vec<Vec<crate::service::api::streaming::StreamEvent>>,
    ) -> Self {
        self.subagent_scripted_turns = Some(scripted_turns);
        self
    }

    pub fn with_inherited_tool_registry(mut self, tool_registry: ToolRegistry) -> Self {
        self.inherited_tool_registry = Some(tool_registry);
        self
    }

    pub fn with_inherited_hook_registry(mut self, hook_registry: HookRegistry) -> Self {
        self.inherited_hook_registry = Some(hook_registry);
        self
    }

    pub fn with_runtime_plugin_state(mut self, runtime_plugin_state: RuntimePluginState) -> Self {
        self.runtime_plugin_state = Some(runtime_plugin_state);
        self
    }

    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = Some(cancellation_token);
        self
    }

    pub fn with_subagent_limiter(mut self, subagent_limiter: Arc<SubagentLimiter>) -> Self {
        self.subagent_limiter = Some(subagent_limiter);
        self
    }

    pub fn with_remote_surface_admission_policy(self, policy: SurfaceAdmissionPolicy) -> Self {
        self.set_remote_surface_admission_policy(policy);
        self
    }

    pub fn set_remote_surface_admission_policy(&self, policy: SurfaceAdmissionPolicy) {
        if let Ok(mut slot) = self.remote_surface_admission_policy.write() {
            *slot = policy;
        }
    }

    pub fn remote_surface_admission_policy(&self) -> SurfaceAdmissionPolicy {
        self.remote_surface_admission_policy
            .read()
            .map(|policy| policy.clone())
            .unwrap_or_default()
    }

    pub fn with_telegram_surface_admission_policy(self, policy: SurfaceAdmissionPolicy) -> Self {
        self.set_telegram_surface_admission_policy(policy);
        self
    }

    pub fn set_telegram_surface_admission_policy(&self, policy: SurfaceAdmissionPolicy) {
        if let Ok(mut slot) = self.telegram_surface_admission_policy.write() {
            *slot = policy;
        }
    }

    pub fn telegram_surface_admission_policy(&self) -> SurfaceAdmissionPolicy {
        self.telegram_surface_admission_policy
            .read()
            .map(|policy| policy.clone())
            .unwrap_or_default()
    }

    pub fn with_external_memory_entries(self, entries: Vec<String>) -> Self {
        self.set_external_memory_entries(entries);
        self
    }

    pub fn set_external_memory_entries(&self, entries: Vec<String>) {
        if let Ok(mut slot) = self.external_memory_entries.write() {
            *slot = sanitize_external_memory_entries(entries);
        }
    }

    pub fn external_memory_entries(&self) -> Vec<String> {
        self.external_memory_entries
            .read()
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }

    pub fn with_nested_memory_lineage(self, lineage: Vec<String>) -> Self {
        self.set_nested_memory_lineage(lineage);
        self
    }

    pub fn set_nested_memory_lineage(&self, lineage: Vec<String>) {
        if let Ok(mut slot) = self.nested_memory_lineage.write() {
            *slot = sanitize_nested_memory_lineage(lineage);
        }
    }

    pub fn nested_memory_lineage(&self) -> Vec<String> {
        self.nested_memory_lineage
            .read()
            .map(|lineage| lineage.clone())
            .unwrap_or_default()
    }

    pub fn with_deferred_tools(mut self, include_deferred_tools: bool) -> Self {
        self.include_deferred_tools = include_deferred_tools;
        self
    }

    pub fn with_interactive_tools(mut self, include_interactive_tools: bool) -> Self {
        self.include_interactive_tools = include_interactive_tools;
        self
    }

    pub fn with_last_activity_ts(mut self, last_activity_ts: Arc<AtomicU64>) -> Self {
        self.last_activity_ts = Some(last_activity_ts);
        self
    }

    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = Some(cancellation_token);
        self
    }

    /// Records activity for the active session to prevent it from being flagged as a zombie.
    /// Should be called by long-running tools or during progress milestones.
    pub fn record_activity(&self) {
        if let Some(ref ts) = self.last_activity_ts {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            ts.store(now, std::sync::atomic::Ordering::Release);
        }
    }
}

fn add_rule(slot: &Arc<RwLock<Vec<String>>>, rule: impl Into<String>) -> bool {
    let rule = rule.into();
    if let Ok(mut rules) = slot.write() {
        if rules.iter().any(|existing| existing == &rule) {
            return false;
        }
        rules.push(rule);
        return true;
    }
    false
}

const MAX_EXTERNAL_MEMORY_ENTRIES: usize = 32;
const MAX_EXTERNAL_MEMORY_ENTRY_CHARS: usize = 240;
pub const MAX_NESTED_MEMORY_DEPTH: usize = 8;
const MAX_NESTED_MEMORY_MARKER_CHARS: usize = 120;

pub fn sanitize_external_memory_entries(entries: Vec<String>) -> Vec<String> {
    normalize_memory_entries(entries, MAX_EXTERNAL_MEMORY_ENTRIES, |entry| {
        truncate_chars(entry, MAX_EXTERNAL_MEMORY_ENTRY_CHARS)
    })
}

pub fn sanitize_nested_memory_lineage(lineage: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for entry in lineage {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(candidate) = truncate_chars(trimmed, MAX_NESTED_MEMORY_MARKER_CHARS) else {
            continue;
        };
        if normalized.is_empty() {
            if candidate.starts_with("session:") && is_valid_nested_memory_marker(&candidate) {
                normalized.push(candidate);
            }
            continue;
        }
        if candidate.starts_with("agent:")
            && is_valid_nested_memory_marker(&candidate)
            && !normalized.iter().any(|existing| existing == &candidate)
        {
            normalized.push(candidate);
        }
        if normalized.len() >= MAX_NESTED_MEMORY_DEPTH {
            break;
        }
    }
    normalized
}

fn normalize_memory_entries(
    entries: Vec<String>,
    max_entries: usize,
    transform: impl Fn(&str) -> Option<String>,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    for entry in entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(candidate) = transform(trimmed) else {
            continue;
        };
        if seen.insert(candidate.clone()) {
            normalized.push(candidate);
        }
        if normalized.len() >= max_entries {
            break;
        }
    }
    normalized
}

fn truncate_chars(value: &str, max_chars: usize) -> Option<String> {
    let truncated = value.chars().take(max_chars).collect::<String>();
    if truncated.trim().is_empty() {
        None
    } else {
        Some(truncated)
    }
}

fn is_valid_nested_memory_marker(value: &str) -> bool {
    if let Some(session_id) = value.strip_prefix("session:") {
        return is_valid_memory_token(session_id);
    }
    let Some(rest) = value.strip_prefix("agent:") else {
        return false;
    };
    let Some((agent_id, inherit_context)) = rest.split_once(":inherit_context=") else {
        return false;
    };
    is_valid_memory_token(agent_id) && matches!(inherit_context, "true" | "false")
}

fn is_valid_memory_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_permission_context_heartbeat() {
        let ts = Arc::new(AtomicU64::new(1000));
        let ctx =
            ToolPermissionContext::new(PermissionMode::Default).with_last_activity_ts(ts.clone());

        ctx.record_activity();

        let val = ts.load(Ordering::Acquire);
        assert!(val > 1000, "Heartbeat should have updated the timestamp");
    }
}

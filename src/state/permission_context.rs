use std::sync::{Arc, RwLock};

use crate::bootstrap::InteractionSurface;
use crate::hook::registry::HookRegistry;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::plan::manager::PlanManager;
use crate::service::mcp::runtime::McpRuntime;
use crate::skills::registry::SkillRegistry;
use crate::task::list_manager::TaskListManager;
use crate::task::manager::TaskManager;
use crate::tool::registry::ToolRegistry;

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
    pub subagent_scripted_turns: Option<Vec<Vec<crate::service::api::streaming::StreamEvent>>>,
    pub inherited_tool_registry: Option<ToolRegistry>,
    pub inherited_hook_registry: Option<HookRegistry>,
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
            subagent_scripted_turns: None,
            inherited_tool_registry: None,
            inherited_hook_registry: None,
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

    pub fn mode(&self) -> PermissionMode {
        self.mode.read().map(|mode| *mode).unwrap_or(PermissionMode::Default)
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
        self.pending_approval.read().ok().and_then(|slot| slot.clone())
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

    pub fn with_deferred_tools(mut self, include_deferred_tools: bool) -> Self {
        self.include_deferred_tools = include_deferred_tools;
        self
    }

    pub fn with_interactive_tools(mut self, include_interactive_tools: bool) -> Self {
        self.include_interactive_tools = include_interactive_tools;
        self
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

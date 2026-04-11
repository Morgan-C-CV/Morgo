use std::sync::Arc;

use crate::task::manager::TaskManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionMode {
    Default,
    AcceptEdits,
    BypassPermissions,
    Plan,
}

#[derive(Debug, Clone)]
pub struct ToolPermissionContext {
    pub mode: PermissionMode,
    pub always_allow_rules: Vec<String>,
    pub always_deny_rules: Vec<String>,
    pub task_manager: Option<Arc<TaskManager>>,
    pub active_session_id: Option<String>,
    pub subagent_scripted_turns: Option<Vec<Vec<crate::service::api::streaming::StreamEvent>>>,
}

impl ToolPermissionContext {
    pub fn new(mode: PermissionMode) -> Self {
        Self {
            mode,
            always_allow_rules: Vec::new(),
            always_deny_rules: Vec::new(),
            task_manager: None,
            active_session_id: None,
            subagent_scripted_turns: None,
        }
    }

    pub fn with_task_manager(mut self, task_manager: Arc<TaskManager>) -> Self {
        self.task_manager = Some(task_manager);
        self
    }

    pub fn with_active_session_id(mut self, active_session_id: impl Into<String>) -> Self {
        self.active_session_id = Some(active_session_id.into());
        self
    }

    pub fn with_subagent_scripted_turns(
        mut self,
        scripted_turns: Vec<Vec<crate::service::api::streaming::StreamEvent>>,
    ) -> Self {
        self.subagent_scripted_turns = Some(scripted_turns);
        self
    }
}

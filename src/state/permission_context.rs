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
}

impl ToolPermissionContext {
    pub fn new(mode: PermissionMode) -> Self {
        Self {
            mode,
            always_allow_rules: Vec::new(),
            always_deny_rules: Vec::new(),
            task_manager: None,
        }
    }

    pub fn with_task_manager(mut self, task_manager: Arc<TaskManager>) -> Self {
        self.task_manager = Some(task_manager);
        self
    }
}

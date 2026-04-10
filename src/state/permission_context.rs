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
}

impl ToolPermissionContext {
    pub fn new(mode: PermissionMode) -> Self {
        Self {
            mode,
            always_allow_rules: Vec::new(),
            always_deny_rules: Vec::new(),
        }
    }
}

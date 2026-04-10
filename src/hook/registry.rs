#[derive(Debug, Clone, Default)]
pub struct HookRegistry {
    pub session_start_hooks: Vec<String>,
    pub pre_tool_use_hooks: Vec<String>,
    pub post_tool_use_hooks: Vec<String>,
}

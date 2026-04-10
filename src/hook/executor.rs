use crate::hook::registry::HookRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookDecision {
    Allow,
    Deny(String),
}

pub fn run_pre_tool_hooks(registry: &HookRegistry, tool_name: &str) -> HookDecision {
    if registry
        .pre_tool_use_hooks
        .iter()
        .any(|rule| rule == tool_name)
    {
        HookDecision::Deny(format!("tool {tool_name} denied by hook policy"))
    } else {
        HookDecision::Allow
    }
}

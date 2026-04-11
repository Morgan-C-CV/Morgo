#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HookPayload {
    pub updated_input: Option<String>,
    pub additional_context: Option<String>,
    pub permission_decision: Option<String>,
    pub permission_reason: Option<String>,
}

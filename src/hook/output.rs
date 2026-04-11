#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HookPayload {
    pub updated_input: Option<String>,
    pub additional_context: Option<String>,
    pub permission_result: HookPermissionResult,
    pub retry_on_denied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HookPermissionResult {
    #[default]
    Passthrough,
    Allow {
        updated_input: Option<String>,
        reason: Option<String>,
    },
    Ask {
        updated_input: Option<String>,
        reason: Option<String>,
    },
    Deny {
        updated_input: Option<String>,
        reason: Option<String>,
    },
}

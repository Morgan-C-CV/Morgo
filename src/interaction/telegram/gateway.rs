use crate::interaction::telegram::binding::SessionBinding;

#[derive(Debug, Clone, Default)]
pub struct TelegramGateway {
    pub allowed_bindings: Vec<SessionBinding>,
}

impl TelegramGateway {
    pub fn is_authorized(&self, actor_id: &str, session_id: &str) -> bool {
        self.allowed_bindings
            .iter()
            .any(|binding| binding.actor_id == actor_id && binding.session_id == session_id)
    }
}

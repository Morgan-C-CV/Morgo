use crate::interaction::notification::{Notification, NotificationTarget};
use crate::interaction::telegram::binding::{SessionBinding, TelegramDeliveryTarget};

#[derive(Debug, Clone, Default)]
pub struct TelegramGateway {
    pub allowed_bindings: Vec<SessionBinding>,
}

impl TelegramGateway {
    pub fn with_bindings(mut self, bindings: Vec<SessionBinding>) -> Self {
        self.allowed_bindings = bindings;
        self
    }

    pub fn is_authorized(&self, actor_id: &str, session_id: &str) -> bool {
        self.allowed_bindings
            .iter()
            .any(|binding| binding.actor_id == actor_id && binding.session_id == session_id)
    }

    pub fn resolve_delivery_target(&self, session_id: &str) -> Option<TelegramDeliveryTarget> {
        self.allowed_bindings
            .iter()
            .find(|binding| binding.session_id == session_id)
            .and_then(|binding| binding.delivery_target.clone())
    }

    pub fn prepare_delivery(&self, notification: &Notification) -> Option<Notification> {
        let target = match &notification.target {
            Some(NotificationTarget::Telegram(target)) => Some(target.clone()),
            Some(NotificationTarget::Session { session_id }) => {
                self.resolve_delivery_target(session_id)
            }
            Some(NotificationTarget::RemoteActor { session_id, .. }) => {
                self.resolve_delivery_target(session_id)
            }
            None => self.resolve_delivery_target(&notification.session_id),
        }?;

        let mut prepared = notification.clone();
        prepared.target = Some(NotificationTarget::Telegram(target));
        Some(prepared)
    }

    pub fn can_deliver(&self, notification: &Notification) -> bool {
        self.prepare_delivery(notification).is_some()
    }
}

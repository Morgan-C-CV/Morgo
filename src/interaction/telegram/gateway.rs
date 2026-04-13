use crate::interaction::notification::{Notification, NotificationTarget};
use crate::interaction::telegram::binding::{
    SessionBinding, TelegramDeliveryTarget, TelegramOutgoingMessage,
};
use crate::interaction::view::{TelegramItem, TelegramView, build_telegram_view};

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

    pub fn build_outgoing_messages(
        &self,
        session_id: &str,
        view: &crate::interaction::view::SurfaceView,
    ) -> Vec<TelegramOutgoingMessage> {
        let Some(target) = self.resolve_delivery_target(session_id) else {
            return Vec::new();
        };
        let telegram_view = build_telegram_view(view);
        telegram_outgoing_messages(target, telegram_view)
    }
}

fn telegram_outgoing_messages(
    target: TelegramDeliveryTarget,
    view: TelegramView,
) -> Vec<TelegramOutgoingMessage> {
    let mut messages = Vec::new();
    if !view.primary_text.is_empty() {
        messages.push(TelegramOutgoingMessage {
            target: target.clone(),
            text: view.primary_text,
        });
    }
    for item in view.items {
        messages.push(TelegramOutgoingMessage {
            target: target.clone(),
            text: render_telegram_item(&item),
        });
    }
    messages
}

fn render_telegram_item(item: &TelegramItem) -> String {
    match item {
        TelegramItem::TaskUpdate(task) => {
            let mut lines = vec![
                format!("Task: {}", task.summary),
                format!("Status: {}", task.status),
                format!("Result: {}", task.result),
                format!("Next: {}", task.next_action),
                format!("Output: {}", task.output_file),
            ];
            if let Some(worker_role) = task.worker_role {
                lines.push(format!("Worker: {worker_role}"));
            }
            if let Some(phase) = task.phase {
                lines.push(format!("Phase: {phase}"));
            }
            if let Some(validation_state) = task.validation_state {
                lines.push(format!("Validation: {validation_state}"));
            }
            lines.join("\n")
        }
        TelegramItem::ApprovalRequired { tool_name, message } => {
            format!("Approval required for {tool_name}\n{message}")
        }
        TelegramItem::RuntimeNotice { kind, message } => {
            format!("Notice: {kind}\n{message}")
        }
    }
}

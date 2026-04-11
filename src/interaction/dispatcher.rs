use std::sync::{Arc, RwLock};

use crate::bootstrap::InteractionSurface;
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry};
use crate::interaction::notification::{Notification, NotificationType};
use crate::interaction::telegram::gateway::TelegramGateway;

#[derive(Debug, Clone, Default)]
pub struct NotificationDispatcher {
    delivered: Arc<RwLock<Vec<Notification>>>,
    telegram_gateway: TelegramGateway,
    hook_registry: HookRegistry,
}

impl NotificationDispatcher {
    pub fn new(telegram_gateway: TelegramGateway) -> Self {
        Self {
            delivered: Arc::new(RwLock::new(Vec::new())),
            telegram_gateway,
            hook_registry: HookRegistry::default(),
        }
    }

    pub fn with_hook_registry(mut self, hook_registry: HookRegistry) -> Self {
        self.hook_registry = hook_registry;
        self
    }

    pub fn dispatch(&self, surface: InteractionSurface, notification: Notification) {
        let notification_event = HookEvent::Notification {
            title: notification.title.clone(),
            body: notification.body.clone(),
            notification_type: match notification.notification_type {
                NotificationType::TaskUpdate => "task_update".into(),
            },
            task_id: notification.task_id.clone(),
            status: notification.status.clone(),
            output_file: notification.output_file.clone(),
        };
        let hook_result = run_hook(&self.hook_registry, notification_event);
        if matches!(
            hook_result.decision,
            crate::hook::executor::HookDecision::Deny(_)
        ) {
            return;
        }

        match surface {
            InteractionSurface::Cli | InteractionSurface::Remote => {
                self.delivered
                    .write()
                    .expect("dispatcher state poisoned")
                    .push(notification);
            }
            InteractionSurface::Telegram => {
                if self.telegram_gateway.can_deliver(&notification) {
                    self.delivered
                        .write()
                        .expect("dispatcher state poisoned")
                        .push(notification);
                }
            }
        }
    }

    pub fn delivered(&self) -> Vec<Notification> {
        self.delivered
            .read()
            .expect("dispatcher state poisoned")
            .clone()
    }
}

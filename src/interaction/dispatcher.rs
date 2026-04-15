use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::bootstrap::InteractionSurface;
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry};
use crate::interaction::notification::{Notification, NotificationTarget, NotificationType};
use crate::interaction::telegram::gateway::TelegramGateway;

#[derive(Debug, Clone, Default)]
pub struct NotificationDispatcher {
    delivered: Arc<RwLock<Vec<Notification>>>,
    remote_inboxes: Arc<RwLock<HashMap<String, Vec<Notification>>>>,
    telegram_gateway: TelegramGateway,
    hook_registry: HookRegistry,
}

impl NotificationDispatcher {
    pub fn new(telegram_gateway: TelegramGateway) -> Self {
        Self {
            delivered: Arc::new(RwLock::new(Vec::new())),
            remote_inboxes: Arc::new(RwLock::new(HashMap::new())),
            telegram_gateway,
            hook_registry: HookRegistry::default(),
        }
    }

    pub fn with_hook_registry(mut self, hook_registry: HookRegistry) -> Self {
        self.hook_registry = hook_registry;
        self
    }

    pub fn set_hook_registry(&mut self, hook_registry: HookRegistry) {
        self.hook_registry = hook_registry;
    }

    pub fn dispatch(&self, surface: InteractionSurface, notification: Notification) {
        let notification_event = HookEvent::Notification {
            title: notification.title.clone(),
            body: notification.body.clone(),
            notification_type: match notification.notification_type {
                NotificationType::TaskUpdate => "task_update".into(),
                NotificationType::ApprovalRequired => "approval_required".into(),
                NotificationType::RuntimeNotice => "runtime_notice".into(),
            },
            task_id: notification.task_id.clone(),
            task_type: notification.task_type.clone(),
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
            InteractionSurface::Cli => {
                self.delivered
                    .write()
                    .expect("dispatcher state poisoned")
                    .push(notification);
            }
            InteractionSurface::Remote => {
                self.delivered
                    .write()
                    .expect("dispatcher state poisoned")
                    .push(notification.clone());
                self.enqueue_remote(notification);
            }
            InteractionSurface::Telegram => {
                if let Some(prepared) = self.telegram_gateway.prepare_delivery(&notification) {
                    self.delivered
                        .write()
                        .expect("dispatcher state poisoned")
                        .push(prepared);
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

    pub fn drain_remote_notifications(
        &self,
        session_id: &str,
        actor_id: Option<&str>,
    ) -> Vec<Notification> {
        let mut inboxes = self
            .remote_inboxes
            .write()
            .expect("dispatcher state poisoned");
        let session_notifications = inboxes.remove(session_id).unwrap_or_default();
        let actor_notifications = actor_id
            .and_then(|actor_id| inboxes.remove(&Self::remote_actor_key(session_id, actor_id)))
            .unwrap_or_default();

        let mut seen = std::collections::HashSet::new();
        let mut drained = Vec::new();
        for notification in session_notifications.into_iter().chain(actor_notifications) {
            let dedupe_key = notification.dedupe_key.clone();
            if dedupe_key
                .as_ref()
                .is_some_and(|key| !seen.insert(key.clone()))
            {
                continue;
            }
            drained.push(notification);
        }
        drained
    }

    fn enqueue_remote(&self, notification: Notification) {
        let inbox_key = match &notification.target {
            Some(NotificationTarget::RemoteActor {
                session_id,
                actor_id,
            }) => Self::remote_actor_key(session_id, actor_id),
            Some(NotificationTarget::Session { session_id }) => session_id.clone(),
            _ => notification.session_id.clone(),
        };
        self.remote_inboxes
            .write()
            .expect("dispatcher state poisoned")
            .entry(inbox_key)
            .or_default()
            .push(notification);
    }

    fn remote_actor_key(session_id: &str, actor_id: &str) -> String {
        format!("{session_id}::{actor_id}")
    }
}

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::bootstrap::InteractionSurface;
use crate::hook::executor::run_hook;
use crate::hook::registry::{HookEvent, HookRegistry};
use crate::interaction::notification::{Notification, NotificationTarget, NotificationType};
use crate::interaction::remote::{RemoteDeliveryMode, remote_delivery_mode_for_notification};
use crate::interaction::telegram::gateway::TelegramGateway;

#[derive(Debug, Clone, Default)]
pub struct NotificationDispatcher {
    delivered: Arc<RwLock<Vec<Notification>>>,
    remote_inboxes: Arc<RwLock<HashMap<String, Vec<Notification>>>>,
    telegram_inboxes: Arc<RwLock<HashMap<String, Vec<Notification>>>>,
    telegram_gateway: TelegramGateway,
    hook_registry: HookRegistry,
    boss_coordinator: Arc<RwLock<Option<Arc<crate::core::boss::BossCoordinator>>>>,
}

impl NotificationDispatcher {
    pub fn new(telegram_gateway: TelegramGateway) -> Self {
        Self {
            delivered: Arc::new(RwLock::new(Vec::new())),
            remote_inboxes: Arc::new(RwLock::new(HashMap::new())),
            telegram_inboxes: Arc::new(RwLock::new(HashMap::new())),
            telegram_gateway,
            hook_registry: HookRegistry::default(),
            boss_coordinator: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_hook_registry(mut self, hook_registry: HookRegistry) -> Self {
        self.hook_registry = hook_registry;
        self
    }

    pub fn set_hook_registry(&mut self, hook_registry: HookRegistry) {
        self.hook_registry = hook_registry;
    }

    pub fn with_boss_coordinator(
        self,
        boss_coordinator: Arc<crate::core::boss::BossCoordinator>,
    ) -> Self {
        if let Ok(mut guard) = self.boss_coordinator.write() {
            *guard = Some(boss_coordinator);
        }
        self
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

        // Notify BossCoordinator if it's a task update
        if notification.notification_type == NotificationType::TaskUpdate {
            if let Some(boss) = self.boss_coordinator.read().unwrap().clone() {
                let n = notification.clone();
                tokio::spawn(async move {
                    if let Err(e) = boss.on_notification(&n).await {
                        tracing::error!("Failed to update BossCoordinator: {}", e);
                    }
                });
            }
        }
        if matches!(
            hook_result.decision,
            crate::hook::executor::HookDecision::Deny(_)
        ) {
            return;
        }

        let prepared = self.prepare_notification_for_surface(surface, notification);
        let Some(prepared) = prepared else {
            return;
        };

        self.delivered
            .write()
            .expect("dispatcher state poisoned")
            .push(prepared.clone());

        if self.should_enqueue_async(surface, &prepared) {
            self.enqueue_async(surface, prepared);
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
        Self::drain_inboxes(&mut inboxes, session_id, actor_id)
    }

    pub fn drain_telegram_notifications(&self, session_id: &str) -> Vec<Notification> {
        let mut inboxes = self
            .telegram_inboxes
            .write()
            .expect("dispatcher state poisoned");
        Self::drain_inboxes(&mut inboxes, session_id, None)
    }

    fn prepare_notification_for_surface(
        &self,
        surface: InteractionSurface,
        notification: Notification,
    ) -> Option<Notification> {
        match surface {
            InteractionSurface::Cli | InteractionSurface::Remote => Some(notification),
            InteractionSurface::Telegram => self.telegram_gateway.prepare_delivery(&notification),
        }
    }

    fn should_enqueue_async(
        &self,
        surface: InteractionSurface,
        notification: &Notification,
    ) -> bool {
        match surface {
            InteractionSurface::Cli => false,
            InteractionSurface::Remote => matches!(
                remote_delivery_mode_for_notification(&notification.notification_type),
                RemoteDeliveryMode::AsyncOnly | RemoteDeliveryMode::DualChannel
            ),
            InteractionSurface::Telegram => notification.wake_up,
        }
    }

    fn enqueue_async(&self, surface: InteractionSurface, notification: Notification) {
        match surface {
            InteractionSurface::Cli => {}
            InteractionSurface::Remote => self.enqueue_remote(notification),
            InteractionSurface::Telegram => self.enqueue_telegram(notification),
        }
    }

    fn enqueue_remote(&self, notification: Notification) {
        let inbox_key = match &notification.target {
            Some(NotificationTarget::RemoteActor {
                session_id,
                actor_id,
            }) => Self::actor_inbox_key(session_id, actor_id),
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

    fn enqueue_telegram(&self, notification: Notification) {
        let inbox_key = match &notification.target {
            Some(NotificationTarget::Session { session_id }) => session_id.clone(),
            Some(NotificationTarget::Telegram(_))
            | Some(NotificationTarget::RemoteActor { .. }) => notification.session_id.clone(),
            None => notification.session_id.clone(),
        };
        self.telegram_inboxes
            .write()
            .expect("dispatcher state poisoned")
            .entry(inbox_key)
            .or_default()
            .push(notification);
    }

    fn drain_inboxes(
        inboxes: &mut HashMap<String, Vec<Notification>>,
        session_id: &str,
        actor_id: Option<&str>,
    ) -> Vec<Notification> {
        let session_notifications = inboxes.remove(session_id).unwrap_or_default();
        let actor_notifications = actor_id
            .and_then(|actor_id| inboxes.remove(&Self::actor_inbox_key(session_id, actor_id)))
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

    fn actor_inbox_key(session_id: &str, actor_id: &str) -> String {
        format!("{session_id}::{actor_id}")
    }
}

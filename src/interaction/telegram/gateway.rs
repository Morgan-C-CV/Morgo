use crate::interaction::envelope::NormalizedInput;
use crate::interaction::notification::{Notification, NotificationTarget};
use crate::interaction::telegram::binding::{
    SessionBinding, TelegramBindingAuthorization, TelegramDeliveryTarget,
    TelegramInboundBindingAuthorization, TelegramOutgoingMessage,
};
use crate::interaction::view::{TelegramItem, TelegramView, build_telegram_view};
use crate::security::authorizer::{
    AuthDecision, AuthDenyCategory, DefaultSurfaceAuthorizer, SurfaceAdmissionPolicy,
    SurfaceAuthorizer,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramInboundRequest {
    pub telegram_user_id: String,
    pub bot_id: String,
    pub actor_id: String,
    pub session_id: String,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramInboundIntake {
    Authorized {
        binding: SessionBinding,
        input: NormalizedInput,
    },
    Rejected(TelegramInboundBindingAuthorization),
}

#[derive(Debug, Clone)]
pub struct TelegramGateway {
    pub allowed_bindings: Vec<SessionBinding>,
    pub surface_authorizer: DefaultSurfaceAuthorizer,
}

impl Default for TelegramGateway {
    fn default() -> Self {
        Self {
            allowed_bindings: Vec::new(),
            surface_authorizer: DefaultSurfaceAuthorizer::default(),
        }
    }
}

impl TelegramGateway {
    pub fn with_bindings(mut self, bindings: Vec<SessionBinding>) -> Self {
        self.allowed_bindings = bindings;
        self
    }

    pub fn with_admission_policy(mut self, policy: SurfaceAdmissionPolicy) -> Self {
        self.surface_authorizer = self.surface_authorizer.clone().with_telegram_policy(policy);
        self
    }

    pub fn authorize_binding(
        &self,
        actor_id: &str,
        session_id: &str,
    ) -> TelegramBindingAuthorization {
        let Some(binding) = self
            .allowed_bindings
            .iter()
            .find(|binding| binding.matches_actor_session(actor_id, session_id))
        else {
            return TelegramBindingAuthorization::Unauthorized;
        };
        match binding.delivery_target.clone() {
            Some(target) => TelegramBindingAuthorization::DeliveryReady(target),
            None => TelegramBindingAuthorization::AuthorizedNoDeliveryTarget,
        }
    }

    pub fn authorize_principal(
        &self,
        telegram_user_id: &str,
        bot_id: &str,
        session_id: &str,
    ) -> TelegramBindingAuthorization {
        let Some(binding) = self.allowed_bindings.iter().find(|binding| {
            binding.matches_session(session_id)
                && binding.matches_telegram_principal(telegram_user_id, bot_id)
        }) else {
            return TelegramBindingAuthorization::Unauthorized;
        };
        match binding.delivery_target.clone() {
            Some(target) => TelegramBindingAuthorization::DeliveryReady(target),
            None => TelegramBindingAuthorization::AuthorizedNoDeliveryTarget,
        }
    }

    pub fn authorize_inbound_binding(
        &self,
        telegram_user_id: &str,
        bot_id: &str,
        actor_id: &str,
        session_id: &str,
    ) -> TelegramInboundBindingAuthorization {
        let Some(session_binding) = self
            .allowed_bindings
            .iter()
            .find(|binding| binding.matches_session(session_id))
        else {
            return TelegramInboundBindingAuthorization::SessionNotBound;
        };

        if session_binding.bot_id.as_deref() != Some(bot_id) {
            return TelegramInboundBindingAuthorization::BotMismatch;
        }

        if session_binding.telegram_user_id.as_deref() != Some(telegram_user_id) {
            return TelegramInboundBindingAuthorization::PrincipalMismatch;
        }

        if session_binding.actor_id != actor_id {
            return TelegramInboundBindingAuthorization::ActorMismatch;
        }

        TelegramInboundBindingAuthorization::Authorized(session_binding.clone())
    }

    pub fn intake_inbound(&self, request: TelegramInboundRequest) -> TelegramInboundIntake {
        match self.authorize_inbound_binding(
            &request.telegram_user_id,
            &request.bot_id,
            &request.actor_id,
            &request.session_id,
        ) {
            TelegramInboundBindingAuthorization::Authorized(binding) => {
                let input = NormalizedInput::from_telegram_raw(
                    request.session_id,
                    request.actor_id,
                    true,
                    request.raw,
                );
                match self.surface_authorizer.authorize(&input) {
                    AuthDecision::Allow => TelegramInboundIntake::Authorized { input, binding },
                    AuthDecision::Deny { category, .. } => {
                        TelegramInboundIntake::Rejected(rejection_from_auth_category(category))
                    }
                }
            }
            rejection => TelegramInboundIntake::Rejected(rejection),
        }
    }

    pub fn is_authorized(&self, actor_id: &str, session_id: &str) -> bool {
        !matches!(
            self.authorize_binding(actor_id, session_id),
            TelegramBindingAuthorization::Unauthorized
        )
    }

    pub fn resolve_delivery_target(&self, session_id: &str) -> Option<TelegramDeliveryTarget> {
        self.allowed_bindings
            .iter()
            .find(|binding| binding.matches_session(session_id) && binding.is_delivery_ready())
            .and_then(|binding| binding.delivery_target.clone())
    }

    pub fn resolve_actor_delivery_target(
        &self,
        actor_id: &str,
        session_id: &str,
    ) -> Option<TelegramDeliveryTarget> {
        match self.authorize_binding(actor_id, session_id) {
            TelegramBindingAuthorization::DeliveryReady(target) => Some(target),
            TelegramBindingAuthorization::AuthorizedNoDeliveryTarget
            | TelegramBindingAuthorization::Unauthorized => None,
        }
    }

    pub fn can_deliver(&self, notification: &Notification) -> bool {
        self.prepare_delivery(notification).is_some()
    }

    pub fn prepare_delivery(&self, notification: &Notification) -> Option<Notification> {
        let target = match &notification.target {
            Some(NotificationTarget::Telegram(target)) => self
                .allowed_bindings
                .iter()
                .find(|binding| binding.matches_session(&notification.session_id))
                .filter(|binding| binding.delivery_target_matches(target))
                .map(|_| target.clone()),
            Some(NotificationTarget::Session { session_id }) => {
                self.resolve_delivery_target(session_id)
            }
            Some(NotificationTarget::RemoteActor {
                session_id,
                actor_id,
            }) => self.resolve_actor_delivery_target(actor_id, session_id),
            None => self.resolve_delivery_target(&notification.session_id),
        }?;

        let mut prepared = notification.clone();
        prepared.target = Some(NotificationTarget::Telegram(target));
        Some(prepared)
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

fn rejection_from_auth_category(category: AuthDenyCategory) -> TelegramInboundBindingAuthorization {
    match category {
        AuthDenyCategory::NotAllowlisted => TelegramInboundBindingAuthorization::NotAllowlisted,
        AuthDenyCategory::RateLimited => TelegramInboundBindingAuthorization::RateLimited,
        AuthDenyCategory::AbuseBlocked => TelegramInboundBindingAuthorization::AbuseBlocked,
        AuthDenyCategory::Unauthenticated | AuthDenyCategory::SurfaceCommandBlocked => {
            TelegramInboundBindingAuthorization::ActorMismatch
        }
    }
}

fn render_telegram_item(item: &TelegramItem) -> String {
    match item {
        TelegramItem::TaskUpdate(task) => {
            let mut lines = vec![
                format!("Task: {}", task.summary),
                format!("Type: {}", task.task_type),
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

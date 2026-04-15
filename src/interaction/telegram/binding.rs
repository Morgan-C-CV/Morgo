#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramDeliveryTarget {
    pub chat_id: String,
    pub thread_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBinding {
    pub actor_id: String,
    pub session_id: String,
    pub telegram_user_id: Option<String>,
    pub bot_id: Option<String>,
    pub delivery_target: Option<TelegramDeliveryTarget>,
}

impl SessionBinding {
    pub fn matches_actor_session(&self, actor_id: &str, session_id: &str) -> bool {
        self.actor_id == actor_id && self.session_id == session_id
    }

    pub fn matches_session(&self, session_id: &str) -> bool {
        self.session_id == session_id
    }

    pub fn is_delivery_ready(&self) -> bool {
        self.delivery_target.is_some()
    }

    pub fn delivery_target_matches(&self, target: &TelegramDeliveryTarget) -> bool {
        self.delivery_target.as_ref() == Some(target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramBindingAuthorization {
    Unauthorized,
    AuthorizedNoDeliveryTarget,
    DeliveryReady(TelegramDeliveryTarget),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramOutgoingMessage {
    pub target: TelegramDeliveryTarget,
    pub text: String,
}

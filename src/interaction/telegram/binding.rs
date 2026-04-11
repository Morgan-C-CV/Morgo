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

use crate::interaction::telegram::gateway::{
    TelegramGateway, TelegramInboundIntake, TelegramInboundRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramInboundEnvelope {
    pub telegram_user_id: String,
    pub bot_id: String,
    pub actor_id: String,
    pub session_id: String,
    pub raw_text: String,
}

pub fn intake_transport_envelope(
    gateway: &TelegramGateway,
    envelope: TelegramInboundEnvelope,
) -> TelegramInboundIntake {
    gateway.intake_inbound(TelegramInboundRequest {
        telegram_user_id: envelope.telegram_user_id,
        bot_id: envelope.bot_id,
        actor_id: envelope.actor_id,
        session_id: envelope.session_id,
        raw: envelope.raw_text,
    })
}

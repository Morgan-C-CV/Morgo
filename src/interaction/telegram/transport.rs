use crate::core::engine::QueryEngine;
use crate::interaction::router::CommandRouter;
use crate::interaction::telegram::adapter::TelegramInboundEnvelope;
use crate::interaction::telegram::binding::TelegramInboundBindingAuthorization;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::interaction::telegram::runtime::{TelegramRuntimeResponse, handle_telegram_envelope};
use crate::state::app_state::AppState;

/// Raw Telegram Bot API message sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramMessageFrom {
    pub user_id: String,
    pub username: Option<String>,
}

/// Raw Telegram Bot API message payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramMessage {
    pub message_id: u64,
    pub chat_id: String,
    pub from: Option<TelegramMessageFrom>,
    pub text: Option<String>,
}

/// Typed Telegram Bot API update — the unit delivered by both webhook and polling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramUpdate {
    pub update_id: u64,
    pub bot_id: String,
    pub message: Option<TelegramMessage>,
}

/// How the update arrived — carried through for observability, not used for dispatch logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramTransportMode {
    Webhook,
    Polling,
}

/// Result of normalizing a raw `TelegramUpdate` into a dispatchable envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramUpdateIntake {
    /// Update normalized successfully — ready for runtime dispatch.
    Accepted(TelegramInboundEnvelope),
    /// Update is structurally valid but has no dispatchable content (e.g. no text).
    Skipped { update_id: u64, reason: &'static str },
    /// Update is missing required fields.
    Malformed { update_id: u64, reason: &'static str },
}

/// Full response from `handle_telegram_update`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramUpdateResponse {
    pub update_id: u64,
    pub transport_mode: TelegramTransportMode,
    pub outcome: TelegramUpdateOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramUpdateOutcome {
    Dispatched(TelegramRuntimeResponse),
    Skipped { reason: &'static str },
    Malformed { reason: &'static str },
}

impl TelegramUpdateResponse {
    pub fn is_dispatched(&self) -> bool {
        matches!(self.outcome, TelegramUpdateOutcome::Dispatched(_))
    }

    pub fn is_authorized(&self) -> bool {
        matches!(
            &self.outcome,
            TelegramUpdateOutcome::Dispatched(TelegramRuntimeResponse::Authorized { .. })
        )
    }

    pub fn rejection(&self) -> Option<&TelegramInboundBindingAuthorization> {
        match &self.outcome {
            TelegramUpdateOutcome::Dispatched(TelegramRuntimeResponse::Rejected(r)) => Some(r),
            _ => None,
        }
    }
}

/// Normalize a raw `TelegramUpdate` into a `TelegramInboundEnvelope`.
///
/// The `actor_id` and `session_id` are derived from the Telegram user_id + chat_id — this is the
/// minimal stable identity contract for v1. Callers that need richer session binding should
/// pre-register `SessionBinding` entries in the gateway.
pub fn normalize_telegram_update(update: &TelegramUpdate) -> TelegramUpdateIntake {
    let Some(message) = &update.message else {
        return TelegramUpdateIntake::Skipped {
            update_id: update.update_id,
            reason: "no_message",
        };
    };

    let Some(text) = &message.text else {
        return TelegramUpdateIntake::Skipped {
            update_id: update.update_id,
            reason: "no_text",
        };
    };

    let Some(from) = &message.from else {
        return TelegramUpdateIntake::Malformed {
            update_id: update.update_id,
            reason: "missing_from",
        };
    };

    let actor_id = format!("tg:{}", from.user_id);
    let session_id = format!("tg:{}:{}", from.user_id, message.chat_id);

    TelegramUpdateIntake::Accepted(TelegramInboundEnvelope {
        telegram_user_id: from.user_id.clone(),
        bot_id: update.bot_id.clone(),
        actor_id,
        session_id,
        raw_text: text.clone(),
    })
}

/// Full webhook/polling dispatch: normalize update → intake → runtime → response.
pub async fn handle_telegram_update(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    gateway: &TelegramGateway,
    update: TelegramUpdate,
    transport_mode: TelegramTransportMode,
) -> anyhow::Result<TelegramUpdateResponse> {
    let update_id = update.update_id;
    match normalize_telegram_update(&update) {
        TelegramUpdateIntake::Accepted(envelope) => {
            let runtime_response =
                handle_telegram_envelope(router, engine, app_state, gateway, envelope).await?;
            Ok(TelegramUpdateResponse {
                update_id,
                transport_mode,
                outcome: TelegramUpdateOutcome::Dispatched(runtime_response),
            })
        }
        TelegramUpdateIntake::Skipped { reason, .. } => Ok(TelegramUpdateResponse {
            update_id,
            transport_mode,
            outcome: TelegramUpdateOutcome::Skipped { reason },
        }),
        TelegramUpdateIntake::Malformed { reason, .. } => Ok(TelegramUpdateResponse {
            update_id,
            transport_mode,
            outcome: TelegramUpdateOutcome::Malformed { reason },
        }),
    }
}

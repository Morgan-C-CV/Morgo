use crate::bootstrap::SessionMode;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::history::resume::{
    ResolvedSessionState, RestoreRequest, RestoreSource, resolve_session_state,
    resolved_from_snapshot,
};
use crate::interaction::cli::repl::handle_normalized_input;
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::CommandRouter;
use crate::interaction::telegram::adapter::{TelegramInboundEnvelope, intake_transport_envelope};
use crate::interaction::telegram::binding::{
    TelegramInboundBindingAuthorization, TelegramOutgoingMessage,
};
use crate::interaction::telegram::gateway::{TelegramGateway, TelegramInboundIntake};
use crate::interaction::view::build_surface_view;
use crate::state::app_state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramRuntimeResponse {
    Authorized {
        primary_text: String,
        messages: Vec<TelegramOutgoingMessage>,
    },
    Rejected(TelegramInboundBindingAuthorization),
}

pub async fn handle_telegram_envelope(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    gateway: &TelegramGateway,
    envelope: TelegramInboundEnvelope,
) -> anyhow::Result<TelegramRuntimeResponse> {
    match intake_transport_envelope(gateway, envelope) {
        TelegramInboundIntake::Authorized { input, .. } => {
            let telegram_engine = bind_telegram_engine(engine, app_state, &input);
            let output = handle_normalized_input(
                router,
                &telegram_engine,
                &telegram_engine.context.app_state,
                input.clone(),
            )
            .await?;
            let view = build_surface_view(&output);
            Ok(TelegramRuntimeResponse::Authorized {
                primary_text: view.primary_text.clone(),
                messages: gateway.build_outgoing_messages(&input.session_id, &view),
            })
        }
        TelegramInboundIntake::Rejected(reason) => Ok(TelegramRuntimeResponse::Rejected(reason)),
    }
}

fn bind_telegram_engine(
    engine: &QueryEngine,
    app_state: &AppState,
    input: &NormalizedInput,
) -> QueryEngine {
    let mut telegram_app_state = engine.context.app_state.clone();
    let resolved = resolve_telegram_session_state(app_state, input);
    telegram_app_state.apply_resolved_session_state(&resolved);
    telegram_app_state.persist_resolved_session_state(&resolved);

    QueryEngine::new(QueryContext {
        app_state: telegram_app_state,
        tool_registry: engine.context.tool_registry.clone(),
        api_client: engine.context.api_client.clone(),
        compactor: engine.context.compactor.clone(),
        hook_registry: engine.context.hook_registry.clone(),
        agent_id: engine.context.agent_id.clone(),
        system_prompt: engine.context.system_prompt.clone(),
        tools_prompt: engine.context.tools_prompt.clone(),
        context_prompt: engine.context.context_prompt.clone(),
    })
}

fn resolve_telegram_session_state(
    app_state: &AppState,
    input: &NormalizedInput,
) -> ResolvedSessionState {
    let fallback_cwd = app_state
        .session
        .as_ref()
        .map(|existing| existing.cwd.clone())
        .unwrap_or_default();
    if let Some(session_store) = app_state.session_store.as_deref() {
        return resolve_session_state(
            session_store,
            Some(&RestoreRequest {
                source: RestoreSource::ResumeSession,
                session_id: Some(input.session_id.clone()),
            }),
            input.surface,
            SessionMode::Interactive,
            std::path::Path::new(&fallback_cwd),
        );
    }

    resolved_from_snapshot(
        crate::history::session::SessionSnapshot {
            session_id: crate::history::session::SessionId(input.session_id.clone()),
            surface: input.surface,
            session_mode: SessionMode::Interactive,
            cwd: fallback_cwd,
            last_turn_at: None,
            prompt_seed: None,
        },
        crate::history::session::SessionHistory::default(),
        false,
        Vec::new(),
        Vec::new(),
    )
}

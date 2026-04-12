use crate::bootstrap::SessionMode;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::history::session::{SessionHistory, SessionId, SessionRestoreRequest, SessionSnapshot};
use crate::interaction::cli::repl::{CliDisplayEvent, handle_normalized_input};
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRequest {
    pub session_id: String,
    pub actor_id: String,
    pub is_authenticated: bool,
    pub from_trusted_surface: bool,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteResponse {
    pub primary_text: String,
    pub events: Vec<String>,
}

pub async fn handle_remote_request(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    request: RemoteRequest,
) -> anyhow::Result<RemoteResponse> {
    let input = NormalizedInput::from_remote_raw(
        request.session_id,
        request.actor_id,
        request.is_authenticated,
        request.from_trusted_surface,
        request.raw,
    );
    let remote_engine = bind_remote_engine(engine, app_state, &input);
    let output = handle_normalized_input(
        router,
        &remote_engine,
        &remote_engine.context.app_state,
        input,
    )
    .await?;

    Ok(RemoteResponse {
        primary_text: output.primary_text,
        events: output
            .events
            .into_iter()
            .map(|event| match event {
                CliDisplayEvent::TaskEvent(task_event) => format!(
                    "task:{}:{}:{}",
                    task_event.task_id, task_event.summary, task_event.next_action
                ),
                CliDisplayEvent::RuntimeEvent(text) => text,
            })
            .collect(),
    })
}

fn bind_remote_engine(engine: &QueryEngine, app_state: &AppState, input: &NormalizedInput) -> QueryEngine {
    let mut remote_app_state = engine.context.app_state.clone();
    let (session_snapshot, session_history) = ensure_remote_session(app_state, input);
    remote_app_state.active_session_id = input.session_id.clone();
    remote_app_state.surface = input.surface;
    remote_app_state.session_mode = SessionMode::Interactive;
    remote_app_state.session = Some(session_snapshot);
    remote_app_state.history = Some(session_history);
    remote_app_state.restored_session = None;
    remote_app_state.permission_context = remote_app_state
        .permission_context
        .clone()
        .with_active_session_id(input.session_id.clone());

    QueryEngine::new(QueryContext {
        app_state: remote_app_state,
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

fn ensure_remote_session(app_state: &AppState, input: &NormalizedInput) -> (SessionSnapshot, SessionHistory) {
    if let Some(session_store) = &app_state.session_store {
        if let Some((snapshot, history)) = session_store.load(&SessionRestoreRequest {
            resume: Some(input.session_id.clone()),
            continue_session: false,
        }) {
            return (snapshot, history);
        }

        let snapshot = SessionSnapshot {
            session_id: SessionId(input.session_id.clone()),
            surface: input.surface,
            session_mode: SessionMode::Interactive,
            cwd: app_state
                .session
                .as_ref()
                .map(|existing| existing.cwd.clone())
                .unwrap_or_default(),
            last_turn_at: None,
            prompt_seed: None,
        };
        let history = SessionHistory::default();
        session_store.save(snapshot.clone(), history.clone());
        return (snapshot, history);
    }

    (
        SessionSnapshot {
            session_id: SessionId(input.session_id.clone()),
            surface: input.surface,
            session_mode: SessionMode::Interactive,
            cwd: app_state
                .session
                .as_ref()
                .map(|existing| existing.cwd.clone())
                .unwrap_or_default(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    )
}

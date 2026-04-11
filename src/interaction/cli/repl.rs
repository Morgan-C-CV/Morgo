use crate::command::types::CommandResult;
use crate::core::engine::QueryEngine;
use crate::core::message::Message;
use crate::history::session::{SessionHistoryEntry, SessionId};
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;
use crate::task::types::TaskEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliDisplayEvent {
    TaskEvent(TaskEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliTurnOutput {
    pub primary_text: String,
    pub events: Vec<CliDisplayEvent>,
}

pub async fn handle_cli_input(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    raw: impl Into<String>,
) -> anyhow::Result<CliTurnOutput> {
    let input = NormalizedInput::from_session_raw(
        app_state.surface,
        app_state.active_session_id.clone(),
        raw,
    );
    let persisted_messages = match router.route(&input, app_state).await? {
        CommandResult::Message(message) => vec![Message::assistant(message)],
        CommandResult::Prompt(prompt) => engine.submit_message(Message::user(prompt)).await,
        CommandResult::ContinueToQuery => {
            engine
                .submit_message(Message::user(input.raw.clone()))
                .await
        }
        CommandResult::Denied(reason) => vec![Message::assistant(format!("Denied: {reason}"))],
    };
    persist_cli_turn(app_state, &input.raw, &persisted_messages);
    let primary_text = collect_message_content(persisted_messages.clone());

    Ok(CliTurnOutput {
        primary_text,
        events: engine
            .drain_task_events()
            .into_iter()
            .map(CliDisplayEvent::TaskEvent)
            .collect(),
    })
}

pub async fn handle_cli_inputs<I, S>(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    raws: I,
) -> anyhow::Result<Vec<CliTurnOutput>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut outputs = Vec::new();
    for raw in raws {
        outputs.push(handle_cli_input(router, engine, app_state, raw).await?);
    }
    Ok(outputs)
}

fn persist_cli_turn(app_state: &AppState, raw_input: &str, messages: &[Message]) {
    let Some(session_store) = &app_state.session_store else {
        return;
    };
    let session_id = SessionId(app_state.active_session_id.clone());
    session_store.append_entry(
        &session_id,
        SessionHistoryEntry {
            message: Message::user(raw_input.to_string()),
            timestamp: None,
            tool_refs: Vec::new(),
        },
    );
    for message in messages {
        session_store.append_entry(
            &session_id,
            SessionHistoryEntry {
                message: message.clone(),
                timestamp: None,
                tool_refs: Vec::new(),
            },
        );
    }
}

fn collect_message_content(messages: Vec<Message>) -> String {
    messages
        .into_iter()
        .map(|message| message.content)
        .collect::<Vec<_>>()
        .join("\n")
}

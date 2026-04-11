use crate::command::types::CommandResult;
use crate::core::engine::QueryEngine;
use crate::core::message::Message;
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
    let primary_text = match router.route(&input, app_state).await? {
        CommandResult::Message(message) => message,
        CommandResult::Prompt(prompt) => {
            collect_message_content(engine.submit_message(Message::user(prompt)).await)
        }
        CommandResult::ContinueToQuery => {
            collect_message_content(engine.submit_message(Message::user(input.raw)).await)
        }
        CommandResult::Denied(reason) => format!("Denied: {reason}"),
    };

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

fn collect_message_content(messages: Vec<Message>) -> String {
    messages
        .into_iter()
        .map(|message| message.content)
        .collect::<Vec<_>>()
        .join("\n")
}

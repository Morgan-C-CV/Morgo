use crate::command::types::CommandResult;
use crate::core::engine::QueryEngine;
use crate::core::message::Message;
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;

pub async fn handle_cli_input(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    raw: impl Into<String>,
) -> anyhow::Result<String> {
    let input = NormalizedInput::from_raw(app_state.surface, raw);
    match router.route(&input, app_state).await? {
        CommandResult::Message(message) => Ok(message),
        CommandResult::Prompt(prompt) => {
            let responses = engine.submit_message(Message::user(prompt)).await;
            Ok(responses
                .into_iter()
                .map(|message| message.content)
                .collect::<Vec<_>>()
                .join("\n"))
        }
        CommandResult::ContinueToQuery => {
            let responses = engine.submit_message(Message::user(input.raw)).await;
            Ok(responses
                .into_iter()
                .map(|message| message.content)
                .collect::<Vec<_>>()
                .join("\n"))
        }
        CommandResult::Denied(reason) => Ok(format!("Denied: {reason}")),
    }
}

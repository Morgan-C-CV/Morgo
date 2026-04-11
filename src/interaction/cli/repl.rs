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
    let input = NormalizedInput::from_session_raw(
        app_state.surface,
        app_state.active_session_id.clone(),
        raw,
    );
    let primary_output = match router.route(&input, app_state).await? {
        CommandResult::Message(message) => message,
        CommandResult::Prompt(prompt) => {
            collect_message_content(engine.submit_message(Message::user(prompt)).await)
        }
        CommandResult::ContinueToQuery => {
            collect_message_content(engine.submit_message(Message::user(input.raw)).await)
        }
        CommandResult::Denied(reason) => format!("Denied: {reason}"),
    };

    Ok(render_cli_turn_output(
        primary_output,
        collect_message_content(engine.drain_task_notification_messages()),
    ))
}

pub async fn handle_cli_inputs<I, S>(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    raws: I,
) -> anyhow::Result<Vec<String>>
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

fn render_cli_turn_output(primary_output: String, task_notifications: String) -> String {
    if primary_output.is_empty() {
        task_notifications
    } else if task_notifications.is_empty() {
        primary_output
    } else {
        format!("{primary_output}\n{task_notifications}")
    }
}

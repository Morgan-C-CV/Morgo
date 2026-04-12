use crate::command::types::CommandResult;
use crate::core::engine::QueryEngine;
use crate::core::events::EngineEvent;
use crate::core::message::Message;
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;
use crate::task::types::TaskEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliRuntimeEvent {
    AssistantDelta { text: String },
    ToolCallStarted { tool_name: String, input: String },
    ToolResult { tool_name: String, content: String },
    PendingApproval { tool_name: String, message: String },
    Notice { kind: String, message: String },
    Transition { text: String },
    Terminal { text: String },
    SessionMilestone { text: String },
}

impl CliRuntimeEvent {
    pub fn to_legacy_line(&self) -> String {
        match self {
            Self::AssistantDelta { text } => format!("[delta] {text}"),
            Self::ToolCallStarted { tool_name, input } => format!("[tool-start] {tool_name}: {input}"),
            Self::ToolResult { tool_name, content } => format!("[tool-result] {tool_name}: {content}"),
            Self::PendingApproval { tool_name, message } => format!("[approval] {tool_name}: {message}"),
            Self::Notice { kind, message } => format!("[notice:{kind}] {message}"),
            Self::Transition { text } => format!("[transition] {text}"),
            Self::Terminal { text } => format!("[terminal] {text}"),
            Self::SessionMilestone { text } => format!("[milestone] {text}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliDisplayEvent {
    TaskEvent(TaskEvent),
    RuntimeEvent(CliRuntimeEvent),
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
    handle_normalized_input(router, engine, app_state, input).await
}

pub async fn handle_normalized_input(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    input: NormalizedInput,
) -> anyhow::Result<CliTurnOutput> {
    let route_result = router.route(&input, app_state).await?;
    let (persisted_messages, runtime_events, engine_persisted) = match route_result {
        CommandResult::Message(message) => (vec![Message::assistant(message)], Vec::new(), false),
        CommandResult::Prompt(prompt) => {
            let (messages, events) = collect_stream_messages(engine, Message::user(prompt)).await;
            (messages, events, true)
        }
        CommandResult::ContinueToQuery => {
            let (messages, events) = collect_stream_messages(engine, Message::user(input.raw.clone())).await;
            (messages, events, true)
        }
        CommandResult::Denied(reason) => (
            vec![Message::assistant(format!("Denied: {reason}"))],
            Vec::new(),
            false,
        ),
        CommandResult::UpdateConfig { key, value } => (
            vec![Message::assistant(format!("Config updated: {key}={value}"))],
            Vec::new(),
            false,
        ),
        CommandResult::SystemTrap(action) => (
            vec![Message::assistant(format!("System trap: {:?}", action))],
            Vec::new(),
            false,
        ),
    };
    if !engine_persisted {
        engine.persist_messages(
            Message::user(input.raw.clone()),
            &persisted_messages,
            crate::core::events::SessionMilestone::AssistantMessageCommitted,
        );
    }
    let primary_text = collect_message_content(persisted_messages.clone());

    let mut events = runtime_events
        .into_iter()
        .map(CliDisplayEvent::RuntimeEvent)
        .collect::<Vec<_>>();
    events.extend(
        engine
            .drain_task_events()
            .into_iter()
            .map(CliDisplayEvent::TaskEvent),
    );

    Ok(CliTurnOutput {
        primary_text,
        events,
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

async fn collect_stream_messages(
    engine: &QueryEngine,
    input: Message,
) -> (Vec<Message>, Vec<CliRuntimeEvent>) {
    let mut receiver = engine.stream_turn(input).await;
    let mut messages = Vec::new();
    let mut runtime_events = Vec::new();
    while let Some(event) = receiver.recv().await {
        match event {
            EngineEvent::MessageCommitted(message) => messages.push(message),
            EngineEvent::AssistantDelta(delta) => {
                runtime_events.push(CliRuntimeEvent::AssistantDelta { text: delta });
            }
            EngineEvent::ToolCallStarted { tool_name, input } => {
                runtime_events.push(CliRuntimeEvent::ToolCallStarted { tool_name, input });
            }
            EngineEvent::ToolResultCommitted { tool_name, content } => {
                runtime_events.push(CliRuntimeEvent::ToolResult { tool_name, content });
            }
            EngineEvent::PendingApproval { tool_name, message } => {
                runtime_events.push(CliRuntimeEvent::PendingApproval { tool_name, message });
            }
            EngineEvent::Notice { kind, message } => {
                runtime_events.push(CliRuntimeEvent::Notice {
                    kind: kind.to_string(),
                    message,
                });
            }
            EngineEvent::Transition(transition) => {
                runtime_events.push(CliRuntimeEvent::Transition {
                    text: format!("{:?}", transition),
                });
            }
            EngineEvent::Terminal(terminal) => {
                runtime_events.push(CliRuntimeEvent::Terminal {
                    text: format!("{:?}", terminal),
                });
            }
            EngineEvent::SessionMilestoneWritten(milestone) => {
                runtime_events.push(CliRuntimeEvent::SessionMilestone {
                    text: format!("{:?}", milestone),
                });
            }
        }
    }
    (messages, runtime_events)
}

fn collect_message_content(messages: Vec<Message>) -> String {
    messages
        .into_iter()
        .map(|message| message.content)
        .collect::<Vec<_>>()
        .join("\n")
}

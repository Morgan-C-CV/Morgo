use crate::command::types::CommandResult;
use crate::core::attachment::load_attachment;
use crate::core::engine::QueryEngine;
use crate::core::events::{EngineEvent, ServiceFailureCode, ServiceFailureNotice};
use crate::core::message::{ContentBlock, Message};
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::{CommandRouter, RouteExecution};
use crate::plugins::runtime_state::{build_turn_engine, build_turn_router};
use crate::state::app_state::AppState;
use crate::task::types::TaskEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliRuntimeEvent {
    AssistantDelta {
        text: String,
    },
    ToolCallStarted {
        tool_name: String,
        input: String,
    },
    ToolResult {
        tool_name: String,
        content: String,
        summary: Option<String>,
        detail: Option<String>,
    },
    PendingApproval {
        tool_name: String,
        message: String,
        code: Option<String>,
        summary: Option<String>,
        detail: Option<String>,
        approval_kind: Option<String>,
        escalation_reasons: Vec<String>,
    },
    Notice {
        kind: String,
        message: String,
        code: Option<String>,
        runtime_kind: Option<String>,
        service_failure_code: Option<String>,
        provider_kind: Option<String>,
        status_code: Option<u16>,
        retryable: Option<bool>,
        surface_visible: Option<bool>,
    },
    Transition {
        kind: String,
        text: String,
    },
    Terminal {
        kind: String,
        text: String,
    },
    SessionMilestone {
        kind: String,
        text: String,
    },
}

impl CliRuntimeEvent {
    pub fn to_legacy_line(&self) -> String {
        match self {
            Self::AssistantDelta { text } => format!("[delta] {text}"),
            Self::ToolCallStarted { tool_name, input } => {
                format!("[tool-start] {tool_name}: {input}")
            }
            Self::ToolResult {
                tool_name, content, ..
            } => {
                format!("[tool-result] {tool_name}: {content}")
            }
            Self::PendingApproval {
                tool_name, message, ..
            } => {
                format!("[approval] {tool_name}: {message}")
            }
            Self::Notice { kind, message, .. } => format!("[notice:{kind}] {message}"),
            Self::Transition { text, .. } => format!("[transition] {text}"),
            Self::Terminal { text, .. } => format!("[terminal] {text}"),
            Self::SessionMilestone { text, .. } => format!("[milestone] {text}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliDisplayEvent {
    TaskEvent(TaskEvent),
    RuntimeEvent(CliRuntimeEvent),
}

/// Build a user Message from NormalizedInput: text block first, then any
/// successfully loaded image blocks from attachments. Attachment errors are
/// logged but do not abort the turn.
fn build_user_message(input: &NormalizedInput) -> Message {
    let mut blocks = vec![ContentBlock::Text {
        text: input.raw.clone(),
    }];
    for path in &input.attachments {
        match load_attachment(path) {
            Ok(block) => blocks.push(block),
            Err(e) => {
                tracing::warn!("attachment skipped: {e}");
            }
        }
    }
    Message {
        role: crate::core::message::Role::User,
        content: input.raw.clone(),
        blocks,
    }
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

pub async fn handle_cli_input_streaming<F>(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    raw: impl Into<String>,
    on_update: F,
) -> anyhow::Result<CliTurnOutput>
where
    F: FnMut(&CliTurnOutput),
{
    let input = NormalizedInput::from_session_raw(
        app_state.surface,
        app_state.active_session_id.clone(),
        raw,
    );
    handle_normalized_input_streaming(router, engine, app_state, input, on_update).await
}

pub async fn handle_normalized_input(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    input: NormalizedInput,
) -> anyhow::Result<CliTurnOutput> {
    handle_normalized_input_streaming(router, engine, app_state, input, |_| {}).await
}

pub async fn handle_normalized_input_streaming<F>(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    input: NormalizedInput,
    mut on_update: F,
) -> anyhow::Result<CliTurnOutput>
where
    F: FnMut(&CliTurnOutput),
{
    let turn_router;
    let turn_engine;
    let turn_app_state;
    let (router, engine, app_state) = if let Some(runtime_plugin_state) =
        app_state.permission_context.runtime_plugin_state.as_ref()
    {
        let snapshot = runtime_plugin_state.snapshot().await;
        turn_router = build_turn_router(&snapshot);
        turn_engine = build_turn_engine(app_state, &snapshot, engine);
        turn_app_state = turn_engine.context.app_state.clone();
        (&turn_router, &turn_engine, &turn_app_state)
    } else {
        (router, engine, app_state)
    };
    let route_result = router.route(&input, app_state).await?;
    let (persisted_messages, runtime_events, engine_persisted) = match route_result {
        RouteExecution::CommandResult(command_result) => match command_result {
            CommandResult::Message(message) => {
                (vec![Message::assistant(message)], Vec::new(), false)
            }
            CommandResult::Blocks(blocks) => {
                use crate::core::output::blocks_to_plain_text;
                (
                    vec![Message::assistant(blocks_to_plain_text(&blocks))],
                    Vec::new(),
                    false,
                )
            }
            CommandResult::Prompt(prompt) => (vec![Message::assistant(prompt)], Vec::new(), false),
            CommandResult::ContinueToQuery => {
                let (messages, events) =
                    collect_stream_messages(engine, build_user_message(&input), &mut on_update)
                        .await;
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
        },
        RouteExecution::EnterQuery { prompt, source } => {
            let user_message = source.to_user_message(&input, &prompt);
            let (messages, events) =
                collect_stream_messages(engine, user_message, &mut on_update).await;
            (messages, events, true)
        }
    };
    if !engine_persisted {
        engine.persist_messages(
            build_user_message(&input),
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
    on_update: &mut dyn FnMut(&CliTurnOutput),
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
            EngineEvent::ToolResultCommitted {
                tool_name,
                content,
                summary,
                detail,
                ..
            } => {
                runtime_events.push(CliRuntimeEvent::ToolResult {
                    tool_name,
                    content: detail.clone().unwrap_or_else(|| content.clone()),
                    summary: Some(summary),
                    detail,
                });
            }
            EngineEvent::PendingApproval {
                tool_name,
                message,
                code,
                summary,
                detail,
                approval_kind,
                escalation_reasons,
                ..
            } => {
                runtime_events.push(CliRuntimeEvent::PendingApproval {
                    tool_name,
                    message: detail.clone().unwrap_or(message),
                    code,
                    summary: Some(summary),
                    detail,
                    approval_kind,
                    escalation_reasons,
                });
            }
            EngineEvent::Notice {
                kind,
                message,
                code,
                service_failure,
            } => {
                let service_failure_code = service_failure_code_string(service_failure.as_ref());
                runtime_events.push(CliRuntimeEvent::Notice {
                    kind: kind.to_string(),
                    message,
                    code: code.map(|value| value.as_str().to_string()),
                    runtime_kind: None,
                    service_failure_code,
                    provider_kind: service_failure
                        .as_ref()
                        .and_then(|value| value.provider_kind.clone()),
                    status_code: service_failure.as_ref().and_then(|value| value.status_code),
                    retryable: service_failure.as_ref().map(|value| value.retryable),
                    surface_visible: service_failure.as_ref().map(|value| value.surface_visible),
                });
            }
            EngineEvent::CompactPlanIssued { kind, message } => {
                runtime_events.push(CliRuntimeEvent::Notice {
                    kind: format!(
                        "compact:{}",
                        match kind {
                            crate::service::compact::CompactPlanKind::AutoCompact => "auto",
                            crate::service::compact::CompactPlanKind::ReactiveCompact => "reactive",
                            crate::service::compact::CompactPlanKind::CollapseDrain =>
                                "collapse_drain",
                            crate::service::compact::CompactPlanKind::Exhausted => "exhausted",
                        }
                    ),
                    message,
                    code: Some(ServiceFailureCode::CompactRecoveryError.as_str().into()),
                    runtime_kind: Some("CompactPlan".into()),
                    service_failure_code: Some(
                        ServiceFailureCode::CompactRecoveryError.as_str().into(),
                    ),
                    provider_kind: None,
                    status_code: None,
                    retryable: Some(true),
                    surface_visible: Some(true),
                });
            }
            EngineEvent::Transition(transition) => {
                runtime_events.push(CliRuntimeEvent::Transition {
                    kind: transition.as_str().to_string(),
                    text: transition.as_str().to_string(),
                });
            }
            EngineEvent::RuntimeEvent(runtime) => {
                let service_failure_code =
                    service_failure_code_string(runtime.service_failure.as_ref());
                runtime_events.push(CliRuntimeEvent::Notice {
                    kind: "runtime".into(),
                    message: format!("{:?}: {}", runtime.kind, runtime.detail),
                    code: runtime.code.map(|value| value.as_str().to_string()),
                    runtime_kind: Some(format!("{:?}", runtime.kind)),
                    service_failure_code,
                    provider_kind: runtime
                        .service_failure
                        .as_ref()
                        .and_then(|value| value.provider_kind.clone()),
                    status_code: runtime
                        .service_failure
                        .as_ref()
                        .and_then(|value| value.status_code),
                    retryable: runtime
                        .service_failure
                        .as_ref()
                        .map(|value| value.retryable),
                    surface_visible: runtime
                        .service_failure
                        .as_ref()
                        .map(|value| value.surface_visible),
                });
            }
            EngineEvent::Terminal(terminal) => {
                runtime_events.push(CliRuntimeEvent::Terminal {
                    kind: terminal.as_str().to_string(),
                    text: terminal.as_str().to_string(),
                });
            }
            EngineEvent::SessionMilestoneWritten(milestone) => {
                runtime_events.push(CliRuntimeEvent::SessionMilestone {
                    kind: milestone.as_str().to_string(),
                    text: milestone.as_str().to_string(),
                });
            }
        }
        on_update(&CliTurnOutput {
            primary_text: collect_message_content(messages.clone()),
            events: runtime_events
                .iter()
                .cloned()
                .map(CliDisplayEvent::RuntimeEvent)
                .collect(),
        });
    }
    (messages, runtime_events)
}

fn service_failure_code_string(service_failure: Option<&ServiceFailureNotice>) -> Option<String> {
    service_failure.map(|value| value.service_failure_code.as_str().to_string())
}

fn collect_message_content(messages: Vec<Message>) -> String {
    messages
        .into_iter()
        .map(|message| message.text())
        .collect::<Vec<_>>()
        .join("\n")
}

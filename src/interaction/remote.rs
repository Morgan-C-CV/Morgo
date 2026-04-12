use crate::bootstrap::SessionMode;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::core::events::SessionMilestone;
use crate::history::session::{SessionHistory, SessionId, SessionRestoreRequest, SessionSnapshot};
use crate::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, handle_normalized_input};
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;
use crate::task::types::TaskEvent;
use std::fmt::Write as _;

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
    pub events: Vec<RemoteEventEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteEventEnvelope {
    pub event_type: &'static str,
    pub payload: RemoteEventPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEventPayload {
    TaskUpdate(RemoteTaskEvent),
    ApprovalRequired { tool_name: String, message: String },
    RuntimeNotice { kind: String, message: String },
    ToolCallStarted { tool_name: String, input: String },
    ToolResult { tool_name: String, content: String },
    AssistantDelta { text: String },
    Transition { kind: String, text: String },
    Terminal { kind: String, text: String },
    SessionMilestone { kind: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTaskEvent {
    pub task_id: String,
    pub status: &'static str,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<&'static str>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<&'static str>,
    pub validation_state: Option<&'static str>,
    pub output_file: String,
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
            .map(RemoteEventEnvelope::from)
            .collect(),
    })
}

impl From<CliDisplayEvent> for RemoteEventEnvelope {
    fn from(event: CliDisplayEvent) -> Self {
        match event {
            CliDisplayEvent::TaskEvent(task_event) => Self {
                event_type: "task_update",
                payload: RemoteEventPayload::TaskUpdate(RemoteTaskEvent::from(task_event)),
            },
            CliDisplayEvent::RuntimeEvent(runtime_event) => Self::from(runtime_event),
        }
    }
}

impl From<CliRuntimeEvent> for RemoteEventEnvelope {
    fn from(event: CliRuntimeEvent) -> Self {
        match event {
            CliRuntimeEvent::AssistantDelta { text } => Self {
                event_type: "assistant_delta",
                payload: RemoteEventPayload::AssistantDelta { text },
            },
            CliRuntimeEvent::ToolCallStarted { tool_name, input } => Self {
                event_type: "tool_call_started",
                payload: RemoteEventPayload::ToolCallStarted { tool_name, input },
            },
            CliRuntimeEvent::ToolResult { tool_name, content } => Self {
                event_type: "tool_result",
                payload: RemoteEventPayload::ToolResult { tool_name, content },
            },
            CliRuntimeEvent::PendingApproval { tool_name, message } => Self {
                event_type: "approval_required",
                payload: RemoteEventPayload::ApprovalRequired { tool_name, message },
            },
            CliRuntimeEvent::Notice { kind, message } => Self {
                event_type: "runtime_notice",
                payload: RemoteEventPayload::RuntimeNotice { kind, message },
            },
            CliRuntimeEvent::Transition { text } => Self {
                event_type: "transition",
                payload: RemoteEventPayload::Transition {
                    kind: stable_transition_kind(&text).to_string(),
                    text,
                },
            },
            CliRuntimeEvent::Terminal { text } => Self {
                event_type: "terminal",
                payload: RemoteEventPayload::Terminal {
                    kind: stable_terminal_kind(&text).to_string(),
                    text,
                },
            },
            CliRuntimeEvent::SessionMilestone { text } => Self {
                event_type: "session_milestone",
                payload: RemoteEventPayload::SessionMilestone {
                    kind: stable_session_milestone_kind(&text).to_string(),
                },
            },
        }
    }
}

impl From<TaskEvent> for RemoteTaskEvent {
    fn from(value: TaskEvent) -> Self {
        Self {
            task_id: value.task_id,
            status: value.status.as_str(),
            summary: value.summary,
            result: value.result,
            next_action: value.next_action,
            worker_role: value.worker_role.map(|role| role.as_str()),
            orchestration_group_id: value.orchestration_group_id,
            phase: value.phase.map(|phase| phase.as_str()),
            validation_state: value.validation_state.map(|state| state.as_str()),
            output_file: value.output_file,
        }
    }
}

fn stable_transition_kind(text: &str) -> &str {
    match text {
        "next_turn" => "next_turn",
        "tool_use_follow_up" => "tool_use_follow_up",
        "max_output_tokens_escalate" => "max_output_tokens_escalate",
        "max_output_tokens_recovery" => "max_output_tokens_recovery",
        "collapse_drain_retry" => "collapse_drain_retry",
        "reactive_compact_retry" => "reactive_compact_retry",
        "stop_hook_blocking" => "stop_hook_blocking",
        "token_budget_continuation" => "token_budget_continuation",
        "model_fallback_retry" => "model_fallback_retry",
        _ => "unknown_transition",
    }
}

fn stable_terminal_kind(text: &str) -> &str {
    match text {
        "completed" => "completed",
        "max_turns" => "max_turns",
        "max_budget" => "max_budget",
        "stop_hook_prevented" => "stop_hook_prevented",
        "aborted_streaming" => "aborted_streaming",
        "aborted_tools" => "aborted_tools",
        "model_error" => "model_error",
        _ => "unknown_terminal",
    }
}

fn stable_session_milestone_kind(text: &str) -> &'static str {
    match text {
        "user_input_committed" => SessionMilestone::UserInputCommitted.as_str(),
        "assistant_message_committed" => SessionMilestone::AssistantMessageCommitted.as_str(),
        "tool_result_committed" => SessionMilestone::ToolResultCommitted.as_str(),
        "turn_completed" => SessionMilestone::TurnCompleted.as_str(),
        _ => "unknown_milestone",
    }
}

pub fn render_remote_response_debug(response: &RemoteResponse) -> String {
    let mut output = String::new();
    if !response.primary_text.is_empty() {
        output.push_str(&response.primary_text);
    }
    for event in &response.events {
        if !output.is_empty() {
            output.push('\n');
        }
        write!(&mut output, "[remote:{}] ", event.event_type).expect("write remote event prefix");
        match &event.payload {
            RemoteEventPayload::TaskUpdate(task) => {
                write!(
                    &mut output,
                    "task_id={} status={} summary={} next_action={}",
                    task.task_id, task.status, task.summary, task.next_action
                )
                .expect("write task event");
            }
            RemoteEventPayload::ApprovalRequired { tool_name, message } => {
                write!(&mut output, "tool_name={} message={}", tool_name, message)
                    .expect("write approval event");
            }
            RemoteEventPayload::RuntimeNotice { kind, message } => {
                write!(&mut output, "kind={} message={}", kind, message)
                    .expect("write notice event");
            }
            RemoteEventPayload::ToolCallStarted { tool_name, input } => {
                write!(&mut output, "tool_name={} input={}", tool_name, input)
                    .expect("write tool call event");
            }
            RemoteEventPayload::ToolResult { tool_name, content } => {
                write!(&mut output, "tool_name={} content={}", tool_name, content)
                    .expect("write tool result event");
            }
            RemoteEventPayload::AssistantDelta { text } => {
                write!(&mut output, "text={}", text).expect("write delta event");
            }
            RemoteEventPayload::Transition { kind, text } => {
                write!(&mut output, "kind={} text={}", kind, text).expect("write transition event");
            }
            RemoteEventPayload::Terminal { kind, text } => {
                write!(&mut output, "kind={} text={}", kind, text).expect("write terminal event");
            }
            RemoteEventPayload::SessionMilestone { kind } => {
                write!(&mut output, "kind={}", kind).expect("write milestone event");
            }
        }
    }
    output
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

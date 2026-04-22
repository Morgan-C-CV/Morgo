use crate::bootstrap::SessionMode;
use crate::core::context::QueryContext;
use crate::core::engine::QueryEngine;
use crate::history::resume::{
    ResolvedSessionState, RestoreRequest, RestoreSource, resolve_session_state,
    resolved_from_snapshot,
};
use crate::interaction::cli::repl::handle_normalized_input;
use crate::interaction::envelope::NormalizedInput;
use crate::interaction::notification::{Notification, NotificationTarget, NotificationType};
use crate::interaction::router::CommandRouter;
use crate::interaction::view::{SurfaceItem, SurfaceView, TaskView, build_surface_view};
use crate::security::audit::AuditEvent;
use crate::security::authorizer::{
    AuthDecision, AuthDenyCategory, DefaultSurfaceAuthorizer, SurfaceAuthorizer,
};
use crate::state::app_state::AppState;
use crate::state::permission_context::PendingApproval;
use crate::task::types::{TaskEvent, TaskUsageSummary};
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
    ApprovalRequired {
        tool_name: String,
        message: String,
        code: Option<String>,
        summary: Option<String>,
        detail: Option<String>,
        approval_kind: Option<String>,
        escalation_reasons: Vec<String>,
    },
    RuntimeNotice {
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
    AssistantDelta {
        text: String,
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
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteNotificationEnvelope {
    pub event_type: &'static str,
    pub payload: RemoteEventPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteDeliveryMode {
    ResponseOnly,
    AsyncOnly,
    DualChannel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteChannelEventKind {
    TaskUpdate,
    ApprovalRequired,
    RuntimeNotice,
    ToolCallStarted,
    ToolResult,
    AssistantDelta,
    Transition,
    Terminal,
    SessionMilestone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteChannelRule {
    pub kind: RemoteChannelEventKind,
    pub mode: RemoteDeliveryMode,
}

pub const REMOTE_CHANNEL_MATRIX: &[RemoteChannelRule] = &[
    RemoteChannelRule {
        kind: RemoteChannelEventKind::TaskUpdate,
        mode: RemoteDeliveryMode::DualChannel,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::ApprovalRequired,
        mode: RemoteDeliveryMode::DualChannel,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::RuntimeNotice,
        mode: RemoteDeliveryMode::DualChannel,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::ToolCallStarted,
        mode: RemoteDeliveryMode::ResponseOnly,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::ToolResult,
        mode: RemoteDeliveryMode::ResponseOnly,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::AssistantDelta,
        mode: RemoteDeliveryMode::ResponseOnly,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::Transition,
        mode: RemoteDeliveryMode::ResponseOnly,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::Terminal,
        mode: RemoteDeliveryMode::ResponseOnly,
    },
    RemoteChannelRule {
        kind: RemoteChannelEventKind::SessionMilestone,
        mode: RemoteDeliveryMode::ResponseOnly,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTaskEvent {
    pub task_id: String,
    pub task_type: &'static str,
    pub status: &'static str,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<&'static str>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<&'static str>,
    pub validation_state: Option<&'static str>,
    pub output_file: String,
    pub usage: Option<TaskUsageSummary>,
}

// Current remote delivery contract:
// - response channel: turn-scoped assistant/runtime/task events produced during the request
// - async inbox: session/actor-targeted notifications that may be drained after the request
// - dual channel: task_update / approval_required / runtime_notice emitted in-request may appear in both response and async inbox
// - async-only by practice: out-of-band queued notifications have no response event counterpart unless a request emits them directly
//
// REMOTE_CHANNEL_MATRIX is the single source of truth for per-event channel rules.
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
    let authorizer = remote_surface_authorizer(app_state);
    if let AuthDecision::Deny { category, reason } = authorizer.authorize(&input) {
        let denied_app_state = bind_remote_engine(engine, app_state, &input)
            .context
            .app_state;
        record_remote_audit(
            &denied_app_state,
            AuditEvent::RemoteRequestDenied {
                session_id: input.session_id.clone(),
                actor_id: input.actor.actor_id.clone(),
                reason,
                outcome: category.code().into(),
            },
        );
        return Ok(RemoteResponse {
            primary_text: denial_message_for_category(category),
            events: Vec::new(),
        });
    }
    let remote_engine = bind_remote_engine(engine, app_state, &input);
    let output = match handle_normalized_input(
        router,
        &remote_engine,
        &remote_engine.context.app_state,
        input.clone(),
    )
    .await
    {
        Ok(output) => {
            record_remote_audit(
                &remote_engine.context.app_state,
                AuditEvent::RemoteRequestAccepted {
                    session_id: input.session_id.clone(),
                    actor_id: input.actor.actor_id.clone(),
                    from_trusted_surface: input.metadata.from_trusted_surface,
                },
            );
            output
        }
        Err(error) => {
            record_remote_audit(
                &remote_engine.context.app_state,
                AuditEvent::RemoteRequestDenied {
                    session_id: input.session_id.clone(),
                    actor_id: input.actor.actor_id.clone(),
                    reason: error.to_string(),
                    outcome: "runtime_error".into(),
                },
            );
            return Err(error);
        }
    };

    let view = build_surface_view(&output);
    dispatch_remote_runtime_notifications(&remote_engine.context.app_state, &input, &view);

    Ok(RemoteResponse {
        primary_text: view.primary_text,
        events: remote_response_events_from_surface_items(&view.items),
    })
}

pub fn drain_remote_notifications(
    app_state: &AppState,
    session_id: &str,
    actor_id: Option<&str>,
) -> Vec<RemoteNotificationEnvelope> {
    let notifications = app_state
        .notification_dispatcher
        .drain_remote_notifications(session_id, actor_id);

    for notification in &notifications {
        record_remote_notification_dispatched_audit(app_state, notification);
    }

    notifications
        .into_iter()
        .filter(is_surface_visible_remote_notification)
        .map(RemoteNotificationEnvelope::from)
        .collect()
}

fn record_remote_notification_dispatched_audit(app_state: &AppState, notification: &Notification) {
    let actor_id = remote_actor_id_for_notification(notification);
    let notification_kind = remote_notification_kind(notification);
    record_remote_audit(
        app_state,
        AuditEvent::RemoteNotificationDispatched {
            session_id: notification.session_id.clone(),
            actor_id,
            notification_kind: notification_kind.into(),
            channel: "async_inbox".into(),
            request_id: notification.session_id.clone(),
        },
    );
}

pub fn remote_response_events_from_surface_items(
    items: &[SurfaceItem],
) -> Vec<RemoteEventEnvelope> {
    items
        .iter()
        .filter(|item| !is_surface_invisible_runtime_notice(item))
        .cloned()
        .map(RemoteEventEnvelope::from)
        .collect()
}

impl From<SurfaceItem> for RemoteEventEnvelope {
    fn from(item: SurfaceItem) -> Self {
        match item {
            SurfaceItem::TaskUpdate(task) => Self {
                event_type: "task_update",
                payload: RemoteEventPayload::TaskUpdate(RemoteTaskEvent::from(task)),
            },
            SurfaceItem::ApprovalRequired {
                tool_name,
                message,
                code,
                summary,
                detail,
                approval_kind,
                escalation_reasons,
            } => Self {
                event_type: "approval_required",
                payload: RemoteEventPayload::ApprovalRequired {
                    tool_name,
                    message,
                    code,
                    summary,
                    detail,
                    approval_kind,
                    escalation_reasons,
                },
            },
            SurfaceItem::RuntimeNotice {
                kind,
                message,
                code,
                runtime_kind,
                service_failure_code,
                provider_kind,
                status_code,
                retryable,
                surface_visible,
            } => Self {
                event_type: "runtime_notice",
                payload: RemoteEventPayload::RuntimeNotice {
                    kind,
                    message,
                    code,
                    runtime_kind,
                    service_failure_code,
                    provider_kind,
                    status_code,
                    retryable,
                    surface_visible,
                },
            },
            SurfaceItem::ToolCallStarted { tool_name, input } => Self {
                event_type: "tool_call_started",
                payload: RemoteEventPayload::ToolCallStarted { tool_name, input },
            },
            SurfaceItem::ToolResult {
                tool_name,
                content,
                summary,
                detail,
            } => Self {
                event_type: "tool_result",
                payload: RemoteEventPayload::ToolResult {
                    tool_name,
                    content,
                    summary,
                    detail,
                },
            },
            SurfaceItem::AssistantDelta { text } => Self {
                event_type: "assistant_delta",
                payload: RemoteEventPayload::AssistantDelta { text },
            },
            SurfaceItem::Transition { kind, text } => Self {
                event_type: "transition",
                payload: RemoteEventPayload::Transition { kind, text },
            },
            SurfaceItem::Terminal { kind, text } => Self {
                event_type: "terminal",
                payload: RemoteEventPayload::Terminal { kind, text },
            },
            SurfaceItem::SessionMilestone { kind, .. } => Self {
                event_type: "session_milestone",
                payload: RemoteEventPayload::SessionMilestone { kind },
            },
        }
    }
}

impl From<TaskEvent> for RemoteTaskEvent {
    fn from(value: TaskEvent) -> Self {
        Self {
            task_id: value.task_id,
            task_type: value.task_type.as_str(),
            status: value.status.as_str(),
            summary: value.summary,
            result: value.result,
            next_action: value.next_action,
            worker_role: value.worker_role.map(|role| role.as_str()),
            orchestration_group_id: value.orchestration_group_id,
            phase: value.phase.map(|phase| phase.as_str()),
            validation_state: value.validation_state.map(|state| state.as_str()),
            output_file: value.output_file,
            usage: value.usage,
        }
    }
}

impl From<TaskView> for RemoteTaskEvent {
    fn from(value: TaskView) -> Self {
        Self {
            task_id: value.task_id,
            task_type: value.task_type,
            status: value.status,
            summary: value.summary,
            result: value.result,
            next_action: value.next_action,
            worker_role: value.worker_role,
            orchestration_group_id: value.orchestration_group_id,
            phase: value.phase,
            validation_state: value.validation_state,
            output_file: value.output_file,
            usage: value.usage,
        }
    }
}

impl From<Notification> for RemoteNotificationEnvelope {
    fn from(notification: Notification) -> Self {
        debug_assert!(matches!(
            remote_delivery_mode_for_notification(&notification.notification_type),
            RemoteDeliveryMode::AsyncOnly | RemoteDeliveryMode::DualChannel
        ));
        match notification.notification_type {
            NotificationType::TaskUpdate => Self {
                event_type: "task_update",
                payload: RemoteEventPayload::TaskUpdate(RemoteTaskEvent {
                    task_id: notification.task_id.unwrap_or_default(),
                    task_type: leak_string(
                        notification.task_type.unwrap_or_else(|| "generic".into()),
                    ),
                    status: leak_string(notification.status.unwrap_or_else(|| "unknown".into())),
                    summary: notification.body,
                    result: notification.title,
                    next_action: notification.next_action.unwrap_or_default(),
                    worker_role: notification.worker_role.map(leak_string),
                    orchestration_group_id: notification.orchestration_group_id,
                    phase: notification.phase.map(leak_string),
                    validation_state: notification.validation_state.map(leak_string),
                    output_file: notification.output_file.unwrap_or_default(),
                    usage: notification.usage,
                }),
            },
            NotificationType::ApprovalRequired => Self {
                event_type: "approval_required",
                payload: RemoteEventPayload::ApprovalRequired {
                    tool_name: notification.tool_name.unwrap_or_default(),
                    message: notification.body,
                    code: notification.approval_code,
                    summary: notification.approval_summary,
                    detail: notification.approval_detail,
                    approval_kind: notification.approval_kind,
                    escalation_reasons: notification.approval_escalation_reasons,
                },
            },
            NotificationType::RuntimeNotice => Self {
                event_type: "runtime_notice",
                payload: RemoteEventPayload::RuntimeNotice {
                    kind: notification.notice_kind.unwrap_or_else(|| "runtime".into()),
                    message: notification.body,
                    code: notification.notice_code,
                    runtime_kind: notification.runtime_kind,
                    service_failure_code: notification.service_failure_code,
                    provider_kind: notification.provider_kind,
                    status_code: notification.status_code,
                    retryable: notification.retryable,
                    surface_visible: notification.surface_visible,
                },
            },
        }
    }
}

fn is_surface_invisible_runtime_notice(item: &SurfaceItem) -> bool {
    matches!(
        item,
        SurfaceItem::RuntimeNotice {
            surface_visible: Some(false),
            ..
        }
    )
}

fn is_surface_visible_remote_notification(notification: &Notification) -> bool {
    !matches!(
        notification,
        Notification {
            notification_type: NotificationType::RuntimeNotice,
            surface_visible: Some(false),
            ..
        }
    )
}

fn dispatch_remote_runtime_notifications(
    app_state: &AppState,
    input: &NormalizedInput,
    view: &SurfaceView,
) {
    for item in &view.items {
        if !matches!(
            remote_delivery_mode_for_surface_item(item),
            RemoteDeliveryMode::DualChannel
        ) {
            continue;
        }
        let Some(notification) = notification_from_surface_item(input, item) else {
            continue;
        };
        record_remote_notification_audit(app_state, &notification);
        app_state
            .notification_dispatcher
            .dispatch(input.surface, notification);
    }
}

fn notification_from_surface_item(
    input: &NormalizedInput,
    item: &SurfaceItem,
) -> Option<Notification> {
    match item {
        SurfaceItem::TaskUpdate(task) => Some(notification_from_task_view(
            &input.session_id,
            &input.actor.actor_id,
            task,
        )),
        SurfaceItem::ApprovalRequired {
            tool_name,
            message,
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        } => Some(notification_from_pending_approval(
            &input.session_id,
            &input.actor.actor_id,
            PendingApproval {
                tool_name: tool_name.clone(),
                tool_input: String::new(),
                message: message.clone(),
                code: code.clone(),
                summary: summary.clone(),
                detail: detail.clone(),
                approval_kind: approval_kind.clone(),
                escalation_reasons: escalation_reasons.clone(),
            },
        )),
        SurfaceItem::RuntimeNotice {
            kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        } => {
            if matches!(surface_visible, Some(false)) {
                return None;
            }
            let mut notification = Notification::runtime_notice(
                input.session_id.clone(),
                kind.clone(),
                message.clone(),
                code.clone(),
                runtime_kind.clone(),
                service_failure_code.clone(),
                provider_kind.clone(),
                *status_code,
                *retryable,
                *surface_visible,
            );
            notification.target = Some(NotificationTarget::RemoteActor {
                session_id: input.session_id.clone(),
                actor_id: input.actor.actor_id.clone(),
            });
            Some(notification)
        }
        SurfaceItem::ToolCallStarted { .. }
        | SurfaceItem::ToolResult { .. }
        | SurfaceItem::AssistantDelta { .. }
        | SurfaceItem::Transition { .. }
        | SurfaceItem::Terminal { .. }
        | SurfaceItem::SessionMilestone { .. } => None,
    }
}

pub fn remote_delivery_mode_for_surface_item(item: &SurfaceItem) -> RemoteDeliveryMode {
    remote_delivery_mode_for_kind(remote_channel_kind_for_surface_item(item))
}

pub fn remote_delivery_mode_for_notification(
    notification_type: &NotificationType,
) -> RemoteDeliveryMode {
    remote_delivery_mode_for_kind(remote_channel_kind_for_notification(notification_type))
}

pub fn remote_delivery_mode_for_kind(kind: RemoteChannelEventKind) -> RemoteDeliveryMode {
    REMOTE_CHANNEL_MATRIX
        .iter()
        .find(|rule| rule.kind == kind)
        .map(|rule| rule.mode)
        .expect("remote channel matrix must cover every event kind")
}

pub fn remote_channel_kind_for_surface_item(item: &SurfaceItem) -> RemoteChannelEventKind {
    match item {
        SurfaceItem::TaskUpdate(_) => RemoteChannelEventKind::TaskUpdate,
        SurfaceItem::ApprovalRequired { .. } => RemoteChannelEventKind::ApprovalRequired,
        SurfaceItem::RuntimeNotice { .. } => RemoteChannelEventKind::RuntimeNotice,
        SurfaceItem::ToolCallStarted { .. } => RemoteChannelEventKind::ToolCallStarted,
        SurfaceItem::ToolResult { .. } => RemoteChannelEventKind::ToolResult,
        SurfaceItem::AssistantDelta { .. } => RemoteChannelEventKind::AssistantDelta,
        SurfaceItem::Transition { .. } => RemoteChannelEventKind::Transition,
        SurfaceItem::Terminal { .. } => RemoteChannelEventKind::Terminal,
        SurfaceItem::SessionMilestone { .. } => RemoteChannelEventKind::SessionMilestone,
    }
}

pub fn remote_channel_kind_for_notification(
    notification_type: &NotificationType,
) -> RemoteChannelEventKind {
    match notification_type {
        NotificationType::TaskUpdate => RemoteChannelEventKind::TaskUpdate,
        NotificationType::ApprovalRequired => RemoteChannelEventKind::ApprovalRequired,
        NotificationType::RuntimeNotice => RemoteChannelEventKind::RuntimeNotice,
    }
}

fn notification_from_task_view(session_id: &str, actor_id: &str, task: &TaskView) -> Notification {
    let mut notification = Notification::task_update(
        session_id.to_string(),
        task.result.clone(),
        task.summary.clone(),
        task.task_id.clone(),
        Some(task.task_type),
        task.status,
        task.next_action.clone(),
        task.worker_role,
        task.orchestration_group_id.as_deref(),
        task.phase,
        task.validation_state,
        None,
        task.output_file.clone(),
        task.usage.clone(),
    );
    notification.target = Some(NotificationTarget::RemoteActor {
        session_id: session_id.to_string(),
        actor_id: actor_id.to_string(),
    });
    notification.dedupe_key = Some(format!(
        "task_update:{}:{}:{}",
        session_id, task.task_id, task.status
    ));
    notification
}

fn notification_from_pending_approval(
    session_id: &str,
    actor_id: &str,
    pending: PendingApproval,
) -> Notification {
    let mut notification = Notification::approval_required(
        session_id.to_string(),
        pending.tool_name,
        pending.message,
        pending.code,
        pending.summary,
        pending.detail,
        pending.approval_kind,
        pending.escalation_reasons,
    );
    notification.target = Some(NotificationTarget::RemoteActor {
        session_id: session_id.to_string(),
        actor_id: actor_id.to_string(),
    });
    notification
}

fn record_remote_audit(app_state: &AppState, event: AuditEvent) {
    app_state
        .audit_log
        .lock()
        .expect("audit log poisoned")
        .record(event);
}

fn record_remote_notification_audit(app_state: &AppState, notification: &Notification) {
    let actor_id = remote_actor_id_for_notification(notification);
    let notification_kind = remote_notification_kind(notification);
    record_remote_audit(
        app_state,
        AuditEvent::RemoteNotificationQueued {
            session_id: notification.session_id.clone(),
            actor_id,
            notification_kind: notification_kind.into(),
            channel: "async_inbox".into(),
            request_id: notification.session_id.clone(),
        },
    );
}

fn remote_actor_id_for_notification(notification: &Notification) -> Option<String> {
    match &notification.target {
        Some(NotificationTarget::RemoteActor { actor_id, .. }) => Some(actor_id.clone()),
        _ => None,
    }
}

fn remote_notification_kind(notification: &Notification) -> &'static str {
    match notification.notification_type {
        NotificationType::TaskUpdate => "task_update",
        NotificationType::ApprovalRequired => "approval_required",
        NotificationType::RuntimeNotice => "runtime_notice",
    }
}

fn remote_surface_authorizer(app_state: &AppState) -> DefaultSurfaceAuthorizer {
    DefaultSurfaceAuthorizer::default().with_remote_policy(
        app_state
            .permission_context
            .remote_surface_admission_policy(),
    )
}

fn denial_message_for_category(category: AuthDenyCategory) -> String {
    category.remote_denial_message().into()
}

fn leak_string(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
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
            RemoteEventPayload::ApprovalRequired {
                tool_name,
                message,
                code,
                summary,
                detail,
                approval_kind,
                escalation_reasons,
            } => {
                write!(
                    &mut output,
                    "tool_name={} message={} code={:?} summary={:?} detail={:?} approval_kind={:?} escalation_reasons={:?}",
                    tool_name, message, code, summary, detail, approval_kind, escalation_reasons
                )
                .expect("write approval event");
            }
            RemoteEventPayload::RuntimeNotice {
                kind,
                message,
                code,
                runtime_kind,
                service_failure_code,
                provider_kind,
                status_code,
                retryable,
                surface_visible,
            } => {
                write!(
                    &mut output,
                    "kind={} message={} code={:?} runtime_kind={:?} service_failure_code={:?} provider_kind={:?} status_code={:?} retryable={:?} surface_visible={:?}",
                    kind,
                    message,
                    code,
                    runtime_kind,
                    service_failure_code,
                    provider_kind,
                    status_code,
                    retryable,
                    surface_visible
                )
                .expect("write notice event");
            }
            RemoteEventPayload::ToolCallStarted { tool_name, input } => {
                write!(&mut output, "tool_name={} input={}", tool_name, input)
                    .expect("write tool call event");
            }
            RemoteEventPayload::ToolResult {
                tool_name,
                content,
                summary,
                detail,
            } => {
                write!(
                    &mut output,
                    "tool_name={} content={} summary={:?} detail={:?}",
                    tool_name, content, summary, detail
                )
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

fn bind_remote_engine(
    engine: &QueryEngine,
    app_state: &AppState,
    input: &NormalizedInput,
) -> QueryEngine {
    let mut remote_app_state = engine.context.app_state.clone();
    let resolved = resolve_remote_session_state(app_state, input);
    remote_app_state.apply_resolved_session_state(&resolved);
    if let Err(error) = remote_app_state.persist_resolved_session_state(&resolved) {
        remote_app_state
            .service_observability_tracker
            .record_runtime_lifecycle_failure(
                "surface.remote.persist_resolved_session_state",
                &error.reason(),
                &remote_app_state.active_session_id,
                1,
            );
        tracing::warn!(
            "failed to persist resolved remote session state: session_id={} reason={}",
            remote_app_state.active_session_id,
            error.reason()
        );
    }

    let active_model_snapshot = remote_app_state
        .active_model_runtime
        .as_ref()
        .map(|runtime| runtime.snapshot_blocking());
    if let Some(active_model_snapshot) = active_model_snapshot.as_ref() {
        remote_app_state.active_model_profile_name =
            active_model_snapshot.active_profile_name.clone();
        remote_app_state.active_model_profile_source = active_model_snapshot.source.clone();
        remote_app_state.active_model_provider_summary = active_model_snapshot.summary.clone();
    }

    QueryEngine::new(QueryContext {
        app_state: remote_app_state,
        tool_registry: engine.context.tool_registry.clone(),
        api_client: active_model_snapshot
            .map(|snapshot| snapshot.client)
            .unwrap_or_else(|| engine.context.api_client.clone()),
        compactor: engine.context.compactor.clone(),
        hook_registry: engine.context.hook_registry.clone(),
        agent_id: engine.context.agent_id.clone(),
        system_prompt: engine.context.system_prompt.clone(),
        tools_prompt: engine.context.tools_prompt.clone(),
        context_prompt: engine.context.context_prompt.clone(),
    })
}

fn resolve_remote_session_state(
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

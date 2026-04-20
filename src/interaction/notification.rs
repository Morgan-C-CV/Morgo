use crate::interaction::telegram::binding::TelegramDeliveryTarget;
use crate::task::types::TaskUsageSummary;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationType {
    TaskUpdate,
    ApprovalRequired,
    RuntimeNotice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationTarget {
    Session {
        session_id: String,
    },
    RemoteActor {
        session_id: String,
        actor_id: String,
    },
    Telegram(TelegramDeliveryTarget),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub session_id: String,
    pub title: String,
    pub body: String,
    pub notification_type: NotificationType,
    pub task_id: Option<String>,
    pub task_type: Option<String>,
    pub status: Option<String>,
    pub next_action: Option<String>,
    pub worker_role: Option<String>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<String>,
    pub validation_state: Option<String>,
    pub output_file: Option<String>,
    pub usage: Option<TaskUsageSummary>,
    pub tool_name: Option<String>,
    pub approval_code: Option<String>,
    pub approval_summary: Option<String>,
    pub approval_detail: Option<String>,
    pub approval_kind: Option<String>,
    pub approval_escalation_reasons: Vec<String>,
    pub notice_kind: Option<String>,
    pub notice_code: Option<String>,
    pub runtime_kind: Option<String>,
    pub service_failure_code: Option<String>,
    pub provider_kind: Option<String>,
    pub status_code: Option<u16>,
    pub retryable: Option<bool>,
    pub surface_visible: Option<bool>,
    pub dedupe_key: Option<String>,
    pub wake_up: bool,
    pub target: Option<NotificationTarget>,
}

fn approval_required_dedupe_key(
    tool_name: &str,
    approval_code: Option<&str>,
    approval_kind: Option<&str>,
) -> String {
    format!(
        "approval_required:{tool_name}:{}:{}",
        approval_kind.unwrap_or("unknown_kind"),
        approval_code.unwrap_or("unknown_code")
    )
}

fn runtime_notice_dedupe_key(
    kind: &str,
    notice_code: Option<&str>,
    runtime_kind: Option<&str>,
    service_failure_code: Option<&str>,
    provider_kind: Option<&str>,
    status_code: Option<u16>,
    retryable: Option<bool>,
    surface_visible: Option<bool>,
) -> String {
    format!(
        "runtime_notice:{kind}:{}:{}:{}:{}:{}:{}:{}",
        notice_code.unwrap_or("unknown_notice_code"),
        runtime_kind.unwrap_or("unknown_runtime_kind"),
        service_failure_code.unwrap_or("unknown_failure_code"),
        provider_kind.unwrap_or("unknown_provider"),
        status_code
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown_status".into()),
        retryable
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown_retryable".into()),
        surface_visible
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown_surface_visible".into())
    )
}

impl Notification {
    pub fn task_update(
        session_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
        task_id: impl Into<String>,
        task_type: Option<&str>,
        status: impl Into<String>,
        next_action: impl Into<String>,
        worker_role: Option<&str>,
        orchestration_group_id: Option<&str>,
        phase: Option<&str>,
        validation_state: Option<&str>,
        output_file: impl Into<String>,
        usage: Option<TaskUsageSummary>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            title: title.into(),
            body: body.into(),
            notification_type: NotificationType::TaskUpdate,
            task_id: Some(task_id.into()),
            task_type: task_type.map(str::to_string),
            status: Some(status.into()),
            next_action: Some(next_action.into()),
            worker_role: worker_role.map(str::to_string),
            orchestration_group_id: orchestration_group_id.map(str::to_string),
            phase: phase.map(str::to_string),
            validation_state: validation_state.map(str::to_string),
            output_file: Some(output_file.into()),
            usage,
            tool_name: None,
            approval_code: None,
            approval_summary: None,
            approval_detail: None,
            approval_kind: None,
            approval_escalation_reasons: Vec::new(),
            notice_kind: None,
            notice_code: None,
            runtime_kind: None,
            service_failure_code: None,
            provider_kind: None,
            status_code: None,
            retryable: None,
            surface_visible: None,
            dedupe_key: None,
            wake_up: true,
            target: None,
        }
    }

    pub fn approval_required(
        session_id: impl Into<String>,
        tool_name: impl Into<String>,
        message: impl Into<String>,
        approval_code: Option<String>,
        approval_summary: Option<String>,
        approval_detail: Option<String>,
        approval_kind: Option<String>,
        approval_escalation_reasons: Vec<String>,
    ) -> Self {
        let tool_name = tool_name.into();
        let message = message.into();
        let dedupe_key = approval_required_dedupe_key(
            &tool_name,
            approval_code.as_deref(),
            approval_kind.as_deref(),
        );
        Self {
            session_id: session_id.into(),
            title: format!("Approval required: {tool_name}"),
            body: message.clone(),
            notification_type: NotificationType::ApprovalRequired,
            task_id: None,
            task_type: None,
            status: None,
            next_action: None,
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file: None,
            usage: None,
            tool_name: Some(tool_name),
            approval_code,
            approval_summary,
            approval_detail,
            approval_kind,
            approval_escalation_reasons,
            notice_kind: None,
            notice_code: None,
            runtime_kind: None,
            service_failure_code: None,
            provider_kind: None,
            status_code: None,
            retryable: None,
            surface_visible: None,
            dedupe_key: Some(dedupe_key),
            wake_up: true,
            target: None,
        }
    }

    pub fn runtime_notice(
        session_id: impl Into<String>,
        kind: impl Into<String>,
        message: impl Into<String>,
        notice_code: Option<String>,
        runtime_kind: Option<String>,
        service_failure_code: Option<String>,
        provider_kind: Option<String>,
        status_code: Option<u16>,
        retryable: Option<bool>,
        surface_visible: Option<bool>,
    ) -> Self {
        let kind = kind.into();
        let message = message.into();
        let dedupe_key = runtime_notice_dedupe_key(
            &kind,
            notice_code.as_deref(),
            runtime_kind.as_deref(),
            service_failure_code.as_deref(),
            provider_kind.as_deref(),
            status_code,
            retryable,
            surface_visible,
        );
        Self {
            session_id: session_id.into(),
            title: format!("Runtime notice: {kind}"),
            body: message.clone(),
            notification_type: NotificationType::RuntimeNotice,
            task_id: None,
            task_type: None,
            status: None,
            next_action: None,
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file: None,
            usage: None,
            tool_name: None,
            approval_code: None,
            approval_summary: None,
            approval_detail: None,
            approval_kind: None,
            approval_escalation_reasons: Vec::new(),
            notice_kind: Some(kind),
            notice_code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
            dedupe_key: Some(dedupe_key),
            wake_up: false,
            target: None,
        }
    }
}

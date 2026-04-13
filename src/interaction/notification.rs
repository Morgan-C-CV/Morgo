use crate::interaction::telegram::binding::TelegramDeliveryTarget;

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
    pub status: Option<String>,
    pub next_action: Option<String>,
    pub worker_role: Option<String>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<String>,
    pub validation_state: Option<String>,
    pub output_file: Option<String>,
    pub tool_name: Option<String>,
    pub notice_kind: Option<String>,
    pub dedupe_key: Option<String>,
    pub wake_up: bool,
    pub target: Option<NotificationTarget>,
}

impl Notification {
    pub fn task_update(
        session_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
        task_id: impl Into<String>,
        status: impl Into<String>,
        next_action: impl Into<String>,
        worker_role: Option<&str>,
        orchestration_group_id: Option<&str>,
        phase: Option<&str>,
        validation_state: Option<&str>,
        output_file: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            title: title.into(),
            body: body.into(),
            notification_type: NotificationType::TaskUpdate,
            task_id: Some(task_id.into()),
            status: Some(status.into()),
            next_action: Some(next_action.into()),
            worker_role: worker_role.map(str::to_string),
            orchestration_group_id: orchestration_group_id.map(str::to_string),
            phase: phase.map(str::to_string),
            validation_state: validation_state.map(str::to_string),
            output_file: Some(output_file.into()),
            tool_name: None,
            notice_kind: None,
            dedupe_key: None,
            wake_up: true,
            target: None,
        }
    }

    pub fn approval_required(
        session_id: impl Into<String>,
        tool_name: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let tool_name = tool_name.into();
        let message = message.into();
        let dedupe_key = format!("approval_required:{tool_name}:{message}");
        Self {
            session_id: session_id.into(),
            title: format!("Approval required: {tool_name}"),
            body: message.clone(),
            notification_type: NotificationType::ApprovalRequired,
            task_id: None,
            status: None,
            next_action: None,
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file: None,
            tool_name: Some(tool_name),
            notice_kind: None,
            dedupe_key: Some(dedupe_key),
            wake_up: true,
            target: None,
        }
    }

    pub fn runtime_notice(
        session_id: impl Into<String>,
        kind: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let kind = kind.into();
        let message = message.into();
        let dedupe_key = format!("runtime_notice:{kind}:{message}");
        Self {
            session_id: session_id.into(),
            title: format!("Runtime notice: {kind}"),
            body: message.clone(),
            notification_type: NotificationType::RuntimeNotice,
            task_id: None,
            status: None,
            next_action: None,
            worker_role: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file: None,
            tool_name: None,
            notice_kind: Some(kind),
            dedupe_key: Some(dedupe_key),
            wake_up: false,
            target: None,
        }
    }
}

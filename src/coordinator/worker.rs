use crate::interaction::notification::Notification;
use crate::task::types::{TaskEvent, TaskStatus};
use crate::tool::definition::ToolMetadata;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskNotification {
    pub task_id: String,
    pub status: TaskStatus,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<crate::state::app_state::WorkerRole>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<crate::task::types::WorkerPhase>,
    pub validation_state: Option<crate::task::types::ValidationState>,
    pub output_file: String,
}

impl TaskNotification {
    pub fn from_task_event(event: &TaskEvent) -> Self {
        Self {
            task_id: event.task_id.clone(),
            status: event.status.clone(),
            summary: event.summary.clone(),
            result: event.result.clone(),
            next_action: event.next_action.clone(),
            worker_role: event.worker_role,
            orchestration_group_id: event.orchestration_group_id.clone(),
            phase: event.phase,
            validation_state: event.validation_state,
            output_file: event.output_file.clone(),
        }
    }

    pub fn format_as_user_message(&self) -> String {
        TaskEvent {
            owner: crate::task::types::TaskOwner {
                session_id: String::new(),
                surface: crate::bootstrap::InteractionSurface::Cli,
            },
            target_task_id: None,
            task_id: self.task_id.clone(),
            status: self.status.clone(),
            summary: self.summary.clone(),
            result: self.result.clone(),
            next_action: self.next_action.clone(),
            worker_role: self.worker_role,
            orchestration_group_id: self.orchestration_group_id.clone(),
            phase: self.phase,
            validation_state: self.validation_state,
            output_file: self.output_file.clone(),
        }
        .format_notification()
    }
}

pub fn filter_tools_for_worker(all_tools: &[ToolMetadata]) -> Vec<ToolMetadata> {
    all_tools
        .iter()
        .filter(|tool| tool.name != "Agent" && tool.name != "SendMessage")
        .filter(|tool| !tool.requires_user_interaction)
        .filter(|tool| !tool.should_defer || tool.always_load)
        .cloned()
        .collect()
}

fn parse_notification_status(status: Option<&str>) -> TaskStatus {
    match status {
        Some(value) if value.eq_ignore_ascii_case("pending") => TaskStatus::Pending,
        Some(value) if value.eq_ignore_ascii_case("running") => TaskStatus::Running,
        Some(value) if value.eq_ignore_ascii_case("completed") => TaskStatus::Completed,
        Some(value) if value.eq_ignore_ascii_case("failed") => TaskStatus::Failed,
        Some(value) if value.eq_ignore_ascii_case("killed") => TaskStatus::Killed,
        _ => TaskStatus::Pending,
    }
}

pub fn notification_to_task_notification(notification: &Notification) -> Option<TaskNotification> {
    Some(TaskNotification {
        task_id: notification.task_id.clone()?,
        status: parse_notification_status(notification.status.as_deref()),
        summary: notification.body.clone(),
        result: notification.title.clone(),
        next_action: notification
            .next_action
            .clone()
            .unwrap_or_else(|| "inspect task notification".to_string()),
        worker_role: notification
            .worker_role
            .as_deref()
            .and_then(|role| match role {
                "research" => Some(crate::state::app_state::WorkerRole::Research),
                "implement" => Some(crate::state::app_state::WorkerRole::Implement),
                "verify" => Some(crate::state::app_state::WorkerRole::Verify),
                _ => None,
            }),
        orchestration_group_id: notification.orchestration_group_id.clone(),
        phase: notification.phase.as_deref().and_then(|phase| match phase {
            "research" => Some(crate::task::types::WorkerPhase::Research),
            "implement" => Some(crate::task::types::WorkerPhase::Implement),
            "verify" => Some(crate::task::types::WorkerPhase::Verify),
            _ => None,
        }),
        validation_state: notification
            .validation_state
            .as_deref()
            .and_then(|state| match state {
                "not_needed" => Some(crate::task::types::ValidationState::NotNeeded),
                "pending_verification" => {
                    Some(crate::task::types::ValidationState::PendingVerification)
                }
                "verified" => Some(crate::task::types::ValidationState::Verified),
                "verification_failed" => {
                    Some(crate::task::types::ValidationState::VerificationFailed)
                }
                "unverified" => Some(crate::task::types::ValidationState::Unverified),
                _ => None,
            }),
        output_file: notification.output_file.clone().unwrap_or_default(),
    })
}

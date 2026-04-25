use crate::bootstrap::InteractionSurface;
use crate::interaction::notification::Notification;
use crate::state::app_state::WorkerRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    Generic,
    LocalBash,
    LocalAgent,
}

impl TaskType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::LocalBash => "local_bash",
            Self::LocalAgent => "local_agent",
        }
    }

    pub fn summary_status_label(&self, status: &TaskStatus) -> &'static str {
        match (self, status) {
            (Self::Generic, _) => status.as_str(),
            (Self::LocalBash, TaskStatus::Pending) => "command pending",
            (Self::LocalBash, TaskStatus::Running) => "command running",
            (Self::LocalBash, TaskStatus::Completed) => "command completed",
            (Self::LocalBash, TaskStatus::Failed) => "command failed",
            (Self::LocalBash, TaskStatus::Killed) => "command killed",
            (Self::LocalAgent, TaskStatus::Pending) => "worker pending",
            (Self::LocalAgent, TaskStatus::Running) => "worker running",
            (Self::LocalAgent, TaskStatus::Completed) => "worker completed",
            (Self::LocalAgent, TaskStatus::Failed) => "worker failed",
            (Self::LocalAgent, TaskStatus::Killed) => "worker killed",
        }
    }

    pub fn result_title(&self, status: &TaskStatus) -> &'static str {
        match (self, status) {
            (Self::Generic, TaskStatus::Pending) => "Task pending",
            (Self::Generic, TaskStatus::Running) => "Task running",
            (Self::Generic, TaskStatus::Completed) => "Task completed",
            (Self::Generic, TaskStatus::Failed) => "Task failed",
            (Self::Generic, TaskStatus::Killed) => "Task killed",
            (Self::LocalBash, TaskStatus::Pending) => "Command pending",
            (Self::LocalBash, TaskStatus::Running) => "Command running",
            (Self::LocalBash, TaskStatus::Completed) => "Command completed",
            (Self::LocalBash, TaskStatus::Failed) => "Command failed",
            (Self::LocalBash, TaskStatus::Killed) => "Command killed",
            (Self::LocalAgent, TaskStatus::Pending) => "Agent task pending",
            (Self::LocalAgent, TaskStatus::Running) => "Agent task running",
            (Self::LocalAgent, TaskStatus::Completed) => "Agent task completed",
            (Self::LocalAgent, TaskStatus::Failed) => "Agent task failed",
            (Self::LocalAgent, TaskStatus::Killed) => "Agent task killed",
        }
    }

    pub fn default_next_action(&self, task_id: &str) -> String {
        match self {
            Self::Generic | Self::LocalAgent => format!("inspect task output for {}", task_id),
            Self::LocalBash => format!("inspect command output for {}", task_id),
        }
    }

    pub fn running_next_action(&self, task_id: &str) -> String {
        match self {
            Self::Generic => format!("continue running task {}", task_id),
            Self::LocalBash => format!("continue running command task {}", task_id),
            Self::LocalAgent => format!("continue running worker task {}", task_id),
        }
    }

    pub fn group_summary_description(&self) -> &'static str {
        match self {
            Self::Generic => "grouped tasks completed",
            Self::LocalBash => "grouped bash tasks completed",
            Self::LocalAgent => "grouped research tasks completed",
        }
    }

    pub fn group_next_action(&self, group_id: &str) -> String {
        match self {
            Self::Generic => format!("inspect grouped task results for {}", group_id),
            Self::LocalBash => format!("inspect grouped command output for {}", group_id),
            Self::LocalAgent => format!("synthesize grouped findings for {}", group_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Killed,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Killed => "killed",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Killed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPhase {
    Research,
    Implement,
    Verify,
}

impl WorkerPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Research => "research",
            Self::Implement => "implement",
            Self::Verify => "verify",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationState {
    NotNeeded,
    PendingVerification,
    Verified,
    VerificationFailed,
    Unverified,
}

impl ValidationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotNeeded => "not_needed",
            Self::PendingVerification => "pending_verification",
            Self::Verified => "verified",
            Self::VerificationFailed => "verification_failed",
            Self::Unverified => "unverified",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskUsageSummary {
    pub requests: usize,
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub cache_creation_input_tokens: usize,
    pub cache_read_input_tokens: usize,
    pub estimated_cost_micros_usd: u64,
}

impl TaskUsageSummary {
    pub fn is_empty(&self) -> bool {
        self.requests == 0
            && self.input_tokens == 0
            && self.output_tokens == 0
            && self.cache_creation_input_tokens == 0
            && self.cache_read_input_tokens == 0
            && self.estimated_cost_micros_usd == 0
    }

    pub fn format_compact(&self) -> String {
        format!(
            "requests={}, input_tokens={}, output_tokens={}, cache_write_tokens={}, cache_read_tokens={}, estimated_cost_usd={:.6}",
            self.requests,
            self.input_tokens,
            self.output_tokens,
            self.cache_creation_input_tokens,
            self.cache_read_input_tokens,
            self.estimated_cost_micros_usd as f64 / 1_000_000.0
        )
    }
}

pub fn format_task_summary(
    description: &str,
    task_id: &str,
    task_type: TaskType,
    status: &TaskStatus,
    usage: Option<&TaskUsageSummary>,
) -> String {
    let mut summary = format!(
        "{} ({}) — {}",
        description,
        task_id,
        task_type.summary_status_label(status)
    );
    if let Some(usage) = usage.filter(|usage| !usage.is_empty()) {
        summary.push_str(" — ");
        summary.push_str(&usage.format_compact());
    }
    summary
}

pub fn format_task_result(
    task_type: TaskType,
    status: &TaskStatus,
    validation_state: Option<ValidationState>,
    usage: Option<&TaskUsageSummary>,
) -> String {
    let mut result = task_type.result_title(status).to_string();
    if let Some(validation_state) =
        validation_state.filter(|state| *state != ValidationState::NotNeeded)
    {
        result.push_str(" — validation: ");
        result.push_str(validation_state.as_str());
    }
    if let Some(usage) = usage.filter(|usage| !usage.is_empty()) {
        result.push_str(" — usage: ");
        result.push_str(&usage.format_compact());
    }
    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDeliveryState {
    pub notified: bool,
    pub notification: Option<Notification>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOwner {
    pub session_id: String,
    pub surface: InteractionSurface,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: String,
    pub description: String,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub owner: TaskOwner,
    pub worker_role: Option<WorkerRole>,
    pub parent_task_id: Option<String>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<WorkerPhase>,
    pub validation_state: Option<ValidationState>,
    pub step_id: Option<usize>,
    pub output_file: String,
    pub output_offset: usize,
    pub delivery: TaskDeliveryState,
    /// Actor id of the boss actor that spawned this task, if any.
    pub boss_actor_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOutputSlice {
    pub content: String,
    pub next_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEvent {
    pub owner: TaskOwner,
    pub target_task_id: Option<String>,
    pub task_id: String,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<WorkerRole>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<WorkerPhase>,
    pub validation_state: Option<ValidationState>,
    pub step_id: Option<usize>,
    pub output_file: String,
    pub usage: Option<TaskUsageSummary>,
}

impl TaskEvent {
    pub fn format_notification(&self) -> String {
        let usage_block = self
            .usage
            .as_ref()
            .filter(|usage| !usage.is_empty())
            .map(|usage| format!("\n<usage>{}</usage>", usage.format_compact()))
            .unwrap_or_default();

        if self.output_file.is_empty() {
            return format!(
                "<task-notification>\n<task-id>{}</task-id>\n<task-type>{}</task-type>\n<status>{:?}</status>\n<summary>{}</summary>\n<result>{}</result>\n<next-action>{}</next-action>\n<worker-role>{}</worker-role>\n<orchestration-group>{}</orchestration-group>\n<phase>{}</phase>\n<validation-state>{}</validation-state>\n<step-id>{}</step-id>{}\n</task-notification>",
                self.task_id,
                self.task_type.as_str(),
                self.status,
                self.summary,
                self.result,
                self.next_action,
                self.worker_role.map(|role| role.as_str()).unwrap_or("none"),
                self.orchestration_group_id.as_deref().unwrap_or("none"),
                self.phase.map(|phase| phase.as_str()).unwrap_or("none"),
                self.validation_state
                    .map(|state| state.as_str())
                    .unwrap_or("none"),
                self.step_id
                    .map(|id| id.to_string())
                    .unwrap_or("none".into()),
                usage_block,
            );
        }

        format!(
            "<task-notification>\n<task-id>{}</task-id>\n<task-type>{}</task-type>\n<status>{:?}</status>\n<summary>{}</summary>\n<result>{}</result>\n<next-action>{}</next-action>\n<worker-role>{}</worker-role>\n<orchestration-group>{}</orchestration-group>\n<phase>{}</phase>\n<validation-state>{}</validation-state>\n<step-id>{}</step-id>{}\n<output-file>{}</output-file>\n</task-notification>",
            self.task_id,
            self.task_type.as_str(),
            self.status,
            self.summary,
            self.result,
            self.next_action,
            self.worker_role.map(|role| role.as_str()).unwrap_or("none"),
            self.orchestration_group_id.as_deref().unwrap_or("none"),
            self.phase.map(|phase| phase.as_str()).unwrap_or("none"),
            self.validation_state
                .map(|state| state.as_str())
                .unwrap_or("none"),
            self.step_id
                .map(|id| id.to_string())
                .unwrap_or("none".into()),
            usage_block,
            self.output_file,
        )
    }
}

use crate::bootstrap::InteractionSurface;
use crate::interaction::notification::Notification;
use crate::state::app_state::WorkerRole;

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
    pub status: TaskStatus,
    pub owner: TaskOwner,
    pub worker_role: Option<WorkerRole>,
    pub parent_task_id: Option<String>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<WorkerPhase>,
    pub validation_state: Option<ValidationState>,
    pub output_file: String,
    pub output_offset: usize,
    pub delivery: TaskDeliveryState,
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
    pub status: TaskStatus,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<WorkerRole>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<WorkerPhase>,
    pub validation_state: Option<ValidationState>,
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
                "<task-notification>\n<task-id>{}</task-id>\n<status>{:?}</status>\n<summary>{}</summary>\n<result>{}</result>\n<next-action>{}</next-action>\n<worker-role>{}</worker-role>\n<orchestration-group>{}</orchestration-group>\n<phase>{}</phase>\n<validation-state>{}</validation-state>{}\n</task-notification>",
                self.task_id,
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
                usage_block,
            );
        }

        format!(
            "<task-notification>\n<task-id>{}</task-id>\n<status>{:?}</status>\n<summary>{}</summary>\n<result>{}</result>\n<next-action>{}</next-action>\n<worker-role>{}</worker-role>\n<orchestration-group>{}</orchestration-group>\n<phase>{}</phase>\n<validation-state>{}</validation-state>{}\n<output-file>{}</output-file>\n</task-notification>",
            self.task_id,
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
            usage_block,
            self.output_file,
        )
    }
}

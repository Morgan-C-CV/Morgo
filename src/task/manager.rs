use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::bootstrap::InteractionSurface;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::notification::Notification;
use crate::task::output_store::TaskOutputStore;
use crate::task::types::{
    TaskDeliveryState, TaskEvent, TaskOutputSlice, TaskOwner, TaskRecord, TaskStatus,
    TaskUsageSummary, ValidationState, WorkerPhase,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskGroupSummary {
    pub group_id: String,
    pub tasks: Vec<TaskRecord>,
    pub hint: String,
}

#[derive(Debug, Default)]
struct TaskStore {
    next_id: usize,
    tasks: Vec<TaskRecord>,
}

#[derive(Debug, Default)]
struct TaskRuntimeStore {
    abort_handles: HashMap<String, AbortHandle>,
    running_owners: HashMap<String, TaskOwner>,
    mailboxes: HashMap<String, Vec<String>>,
    mailbox_notifiers: HashMap<String, Arc<Notify>>,
    events: Vec<TaskEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskManager {
    store: Arc<RwLock<TaskStore>>,
    runtime_store: Arc<RwLock<TaskRuntimeStore>>,
    output_store: TaskOutputStore,
}

impl TaskManager {
    pub fn create(
        &self,
        description: impl Into<String>,
        owner_session_id: impl Into<String>,
        owner_surface: InteractionSurface,
    ) -> TaskRecord {
        let mut store = self.store.write().expect("task store poisoned");
        let id = format!("task-{}", store.next_id);
        store.next_id += 1;
        let output_file = self
            .output_store
            .init(&id)
            .expect("task output file should be initialized");
        let task = TaskRecord {
            id: id.clone(),
            description: description.into(),
            status: TaskStatus::Pending,
            owner: TaskOwner {
                session_id: owner_session_id.into(),
                surface: owner_surface,
            },
            worker_role: None,
            parent_task_id: None,
            orchestration_group_id: None,
            phase: None,
            validation_state: None,
            output_file,
            output_offset: 0,
            delivery: TaskDeliveryState {
                notified: false,
                notification: None,
            },
        };
        store.tasks.push(task.clone());
        task
    }

    pub fn start(&self, id: &str) {
        self.update_status(id, TaskStatus::Running);
    }

    pub fn set_worker_role(&self, id: &str, worker_role: crate::state::app_state::WorkerRole) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.worker_role = Some(worker_role);
            task.phase = Some(match worker_role {
                crate::state::app_state::WorkerRole::Research => WorkerPhase::Research,
                crate::state::app_state::WorkerRole::Implement => WorkerPhase::Implement,
                crate::state::app_state::WorkerRole::Verify => WorkerPhase::Verify,
            });
        }
    }

    pub fn set_parent_task_id(&self, id: &str, parent_task_id: Option<String>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.parent_task_id = parent_task_id;
        }
    }

    pub fn set_orchestration_group_id(&self, id: &str, orchestration_group_id: Option<String>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.orchestration_group_id = orchestration_group_id;
        }
    }

    pub fn set_phase(&self, id: &str, phase: Option<WorkerPhase>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.phase = phase;
        }
    }

    pub fn set_validation_state(&self, id: &str, validation_state: Option<ValidationState>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.validation_state = validation_state;
        }
    }

    pub fn children_of(&self, parent_task_id: &str) -> Vec<TaskRecord> {
        self.list()
            .into_iter()
            .filter(|task| task.parent_task_id.as_deref() == Some(parent_task_id))
            .collect()
    }

    pub fn group_tasks(&self, orchestration_group_id: &str) -> Vec<TaskRecord> {
        self.list()
            .into_iter()
            .filter(|task| task.orchestration_group_id.as_deref() == Some(orchestration_group_id))
            .collect()
    }

    pub fn group_ready_for_fan_in(&self, orchestration_group_id: &str) -> bool {
        let tasks = self.group_tasks(orchestration_group_id);
        !tasks.is_empty()
            && tasks
                .iter()
                .all(|task| !matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
    }

    pub fn has_pending_orchestration(&self, session_id: &str) -> bool {
        self.list().into_iter().any(|task| {
            task.owner.session_id == session_id
                && (task.validation_state == Some(ValidationState::PendingVerification)
                    || task
                        .orchestration_group_id
                        .as_ref()
                        .is_some_and(|group_id| !self.group_ready_for_fan_in(group_id)))
        })
    }

    pub fn grouped_tasks(&self) -> (Vec<TaskGroupSummary>, Vec<TaskRecord>) {
        let tasks = self.list();
        let mut grouped = std::collections::BTreeMap::<String, Vec<TaskRecord>>::new();
        let mut standalone = Vec::new();
        for task in tasks {
            if let Some(group_id) = task.orchestration_group_id.clone() {
                grouped.entry(group_id).or_default().push(task);
            } else {
                standalone.push(task);
            }
        }
        let groups = grouped
            .into_iter()
            .map(|(group_id, tasks)| self.build_group_summary(group_id, tasks))
            .collect();
        standalone.sort_by(|left, right| left.id.cmp(&right.id));
        (groups, standalone)
    }

    pub fn group_summary(&self, orchestration_group_id: &str) -> Option<TaskGroupSummary> {
        let tasks = self.group_tasks(orchestration_group_id);
        if tasks.is_empty() {
            return None;
        }
        Some(self.build_group_summary(orchestration_group_id.to_string(), tasks))
    }

    pub fn task_hint(&self, task: &TaskRecord) -> String {
        match task.validation_state {
            Some(ValidationState::PendingVerification) => {
                format!("verification next for {}", task.id)
            }
            Some(ValidationState::Verified) => {
                format!("ready for validated synthesis for {}", task.id)
            }
            Some(ValidationState::VerificationFailed) => {
                format!("verification failure needs inspection for {}", task.id)
            }
            Some(ValidationState::Unverified) => {
                format!("synthesize with explicit unverified risk for {}", task.id)
            }
            _ => next_action_for_task(
                &task.status,
                task.worker_role,
                task.validation_state,
                &task.id,
            ),
        }
    }

    pub fn launch<F>(&self, id: &str, _input: impl Into<String>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.start(id);
        let manager = self.clone();
        let task_id = id.to_string();
        let join_handle = tokio::spawn(async move {
            future.await;
            manager.clear_running_handle(&task_id);
        });
        let owner = self
            .get(id)
            .map(|task| task.owner)
            .expect("task should exist before launch");
        let mut runtime_store = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned");
        runtime_store
            .abort_handles
            .insert(id.to_string(), join_handle.abort_handle());
        runtime_store.running_owners.insert(id.to_string(), owner);
        runtime_store.mailboxes.insert(id.to_string(), Vec::new());
        runtime_store
            .mailbox_notifiers
            .insert(id.to_string(), Arc::new(Notify::new()));
    }

    pub fn append_output(&self, id: &str, chunk: impl AsRef<str>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            let appended = self
                .output_store
                .append(&task.output_file, chunk.as_ref())
                .expect("task output should append");
            task.output_offset += appended;
        }
    }

    pub fn complete(&self, id: &str, dispatcher: &NotificationDispatcher) {
        self.complete_with_usage(id, dispatcher, None);
    }

    pub fn complete_with_usage(
        &self,
        id: &str,
        dispatcher: &NotificationDispatcher,
        usage: Option<TaskUsageSummary>,
    ) {
        self.finish(
            id,
            TaskStatus::Completed,
            "Task completed",
            dispatcher,
            usage,
        );
    }

    pub fn fail(&self, id: &str, dispatcher: &NotificationDispatcher) {
        self.fail_with_usage(id, dispatcher, None);
    }

    pub fn fail_with_usage(
        &self,
        id: &str,
        dispatcher: &NotificationDispatcher,
        usage: Option<TaskUsageSummary>,
    ) {
        self.finish(id, TaskStatus::Failed, "Task failed", dispatcher, usage);
    }

    pub fn kill(
        &self,
        id: &str,
        requester_session_id: &str,
        dispatcher: &NotificationDispatcher,
    ) -> bool {
        let owner = self.running_owner(id);
        if owner
            .as_ref()
            .map(|owner| owner.session_id.as_str() != requester_session_id)
            .unwrap_or(false)
        {
            return false;
        }
        if let Some(handle) = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned")
            .abort_handles
            .remove(id)
        {
            handle.abort();
        }
        self.finish(id, TaskStatus::Killed, "Task killed", dispatcher, None);
        true
    }

    pub fn get(&self, id: &str) -> Option<TaskRecord> {
        self.store
            .read()
            .expect("task store poisoned")
            .tasks
            .iter()
            .find(|task| task.id == id)
            .cloned()
    }

    pub fn list(&self) -> Vec<TaskRecord> {
        self.store
            .read()
            .expect("task store poisoned")
            .tasks
            .clone()
    }

    pub fn get_output(&self, id: &str, offset: usize) -> Option<TaskOutputSlice> {
        let output_file = self
            .store
            .read()
            .expect("task store poisoned")
            .tasks
            .iter()
            .find(|task| task.id == id)
            .map(|task| task.output_file.clone())?;

        self.output_store.read_slice(&output_file, offset).ok()
    }

    pub fn drain_events(&self, session_id: &str) -> Vec<TaskEvent> {
        self.drain_events_for_target(session_id, None)
    }

    pub fn drain_events_for_target(
        &self,
        session_id: &str,
        target_task_id: Option<&str>,
    ) -> Vec<TaskEvent> {
        let mut runtime_store = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned");
        let events = std::mem::take(&mut runtime_store.events);
        let (matched, unmatched): (Vec<_>, Vec<_>) = events.into_iter().partition(|event| {
            event.owner.session_id == session_id
                && match target_task_id {
                    Some(task_id) => event.target_task_id.as_deref() == Some(task_id),
                    None => true,
                }
        });
        runtime_store.events = unmatched;
        matched
    }

    pub fn running_owner(&self, id: &str) -> Option<TaskOwner> {
        self.runtime_store
            .read()
            .expect("task runtime store poisoned")
            .running_owners
            .get(id)
            .cloned()
    }

    pub fn is_running_owned_by(&self, id: &str, requester_session_id: &str) -> bool {
        self.running_owner(id)
            .map(|owner| owner.session_id == requester_session_id)
            .unwrap_or(false)
    }

    pub fn status(&self, id: &str) -> Option<TaskStatus> {
        self.get(id).map(|task| task.status)
    }

    pub fn is_terminal(&self, id: &str) -> Option<bool> {
        self.status(id)
            .map(|status| !matches!(status, TaskStatus::Pending | TaskStatus::Running))
    }

    pub fn send_message(
        &self,
        id: &str,
        requester_session_id: &str,
        message: impl Into<String>,
    ) -> bool {
        let message = message.into();
        let notifier = {
            let mut runtime_store = self
                .runtime_store
                .write()
                .expect("task runtime store poisoned");
            if runtime_store
                .running_owners
                .get(id)
                .map(|owner| owner.session_id.as_str() != requester_session_id)
                .unwrap_or(true)
            {
                return false;
            }
            runtime_store
                .mailboxes
                .entry(id.to_string())
                .or_default()
                .push(message);
            runtime_store.mailbox_notifiers.get(id).cloned()
        };
        if let Some(notifier) = notifier {
            notifier.notify_one();
        }
        true
    }

    pub fn drain_mailbox(&self, id: &str) -> Vec<String> {
        let mut runtime_store = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned");
        runtime_store
            .mailboxes
            .get_mut(id)
            .map(std::mem::take)
            .unwrap_or_default()
    }

    pub async fn wait_for_mailbox_message(&self, id: &str) -> Option<String> {
        loop {
            let notifier = {
                let mut runtime_store = self
                    .runtime_store
                    .write()
                    .expect("task runtime store poisoned");
                let mailbox = runtime_store.mailboxes.get_mut(id)?;
                if !mailbox.is_empty() {
                    return Some(mailbox.remove(0));
                }
                runtime_store.mailbox_notifiers.get(id).cloned()?
            };
            notifier.notified().await;
        }
    }

    fn update_status(&self, id: &str, status: TaskStatus) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.status = status;
        }
    }

    fn clear_running_handle(&self, id: &str) {
        let mut runtime_store = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned");
        runtime_store.abort_handles.remove(id);
        runtime_store.running_owners.remove(id);
        runtime_store.mailboxes.remove(id);
        runtime_store.mailbox_notifiers.remove(id);
    }

    fn finish(
        &self,
        id: &str,
        status: TaskStatus,
        title: &str,
        dispatcher: &NotificationDispatcher,
        usage: Option<TaskUsageSummary>,
    ) {
        self.clear_running_handle(id);
        let mut barrier_candidate = None;
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.status = status.clone();
            task.delivery.notified = true;
            task.validation_state =
                transition_validation_state(task.worker_role, &status, task.validation_state);
            let next_action =
                next_action_for_task(&status, task.worker_role, task.validation_state, &task.id);
            let event = TaskEvent {
                owner: task.owner.clone(),
                target_task_id: Some(task.id.clone()),
                task_id: task.id.clone(),
                status,
                summary: format!("{} ({})", task.description, task.id),
                result: title.to_string(),
                next_action,
                worker_role: task.worker_role,
                orchestration_group_id: task.orchestration_group_id.clone(),
                phase: task.phase,
                validation_state: task.validation_state,
                output_file: task.output_file.clone(),
                usage: usage.clone(),
            };
            self.enqueue_task_event(event.clone());
            let notification = self.dispatch_task_notification(title, &event, dispatcher);
            task.delivery.notification = Some(notification);

            barrier_candidate = task.orchestration_group_id.clone().map(|group_id| {
                (
                    group_id,
                    task.owner.clone(),
                    task.parent_task_id.clone().or(Some(task.id.clone())),
                    task.output_file.clone(),
                )
            });
        }
        if let Some((group_id, owner, target_task_id, output_file)) = barrier_candidate {
            if self.group_ready_for_fan_in(&group_id) {
                let event = TaskEvent {
                    owner,
                    target_task_id,
                    task_id: format!("group-{}", group_id),
                    status: TaskStatus::Completed,
                    summary: format!("grouped research tasks completed ({})", group_id),
                    result: "Task group completed".into(),
                    next_action: format!("synthesize grouped findings for {}", group_id),
                    worker_role: None,
                    orchestration_group_id: Some(group_id.clone()),
                    phase: None,
                    validation_state: None,
                    output_file,
                    usage: None,
                };
                self.enqueue_task_event(event.clone());
                let _ = self.dispatch_task_notification("Task group completed", &event, dispatcher);
            }
        }
        self.propagate_verification_to_parent(id);
    }

    fn propagate_verification_to_parent(&self, id: &str) {
        let Some(task) = self.get(id) else {
            return;
        };
        let Some(parent_task_id) = task.parent_task_id.clone() else {
            return;
        };
        let propagated_state = match (task.worker_role, task.status, task.validation_state) {
            (
                Some(crate::state::app_state::WorkerRole::Verify),
                TaskStatus::Completed,
                Some(ValidationState::Verified),
            ) => Some(ValidationState::Verified),
            (
                Some(crate::state::app_state::WorkerRole::Verify),
                TaskStatus::Failed,
                Some(ValidationState::VerificationFailed),
            ) => Some(ValidationState::VerificationFailed),
            (Some(crate::state::app_state::WorkerRole::Verify), TaskStatus::Killed, _) => {
                Some(ValidationState::Unverified)
            }
            _ => None,
        };
        let Some(propagated_state) = propagated_state else {
            return;
        };
        if let Some(parent) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == parent_task_id)
        {
            parent.validation_state = Some(propagated_state);
        }
    }

    fn build_group_summary(
        &self,
        group_id: String,
        mut tasks: Vec<TaskRecord>,
    ) -> TaskGroupSummary {
        tasks.sort_by(|left, right| {
            left.parent_task_id
                .is_some()
                .cmp(&right.parent_task_id.is_some())
                .then_with(|| left.id.cmp(&right.id))
        });
        let hint = self.group_hint(&group_id, &tasks);
        TaskGroupSummary {
            group_id,
            tasks,
            hint,
        }
    }

    fn group_hint(&self, group_id: &str, tasks: &[TaskRecord]) -> String {
        if tasks
            .iter()
            .any(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
        {
            return format!("group {} still in progress", group_id);
        }
        if tasks
            .iter()
            .any(|task| task.validation_state == Some(ValidationState::PendingVerification))
        {
            return format!("group {} is waiting for verification", group_id);
        }
        if self.group_ready_for_fan_in(group_id) {
            return format!("group {} is ready for synthesis", group_id);
        }
        format!("group {} needs follow-up inspection", group_id)
    }

    fn enqueue_task_event(&self, event: TaskEvent) {
        self.runtime_store
            .write()
            .expect("task runtime store poisoned")
            .events
            .push(event);
    }

    fn dispatch_task_notification(
        &self,
        title: &str,
        event: &TaskEvent,
        dispatcher: &NotificationDispatcher,
    ) -> Notification {
        let mut notification = Notification::task_update(
            &event.owner.session_id,
            title,
            event.summary.clone(),
            event.task_id.clone(),
            event.status.as_str(),
            event.next_action.clone(),
            event.worker_role.map(|role| role.as_str()),
            event.orchestration_group_id.as_deref(),
            event.phase.map(|phase| phase.as_str()),
            event.validation_state.map(|state| state.as_str()),
            event.output_file.clone(),
            event.usage.clone(),
        );
        if matches!(
            event.owner.surface,
            InteractionSurface::Telegram | InteractionSurface::Remote
        ) {
            notification.target = Some(
                crate::interaction::notification::NotificationTarget::Session {
                    session_id: event.owner.session_id.clone(),
                },
            );
            if matches!(event.owner.surface, InteractionSurface::Remote) {
                notification.dedupe_key = Some(format!(
                    "task_update:{}:{}:{}",
                    event.owner.session_id,
                    event.task_id,
                    event.status.as_str()
                ));
            }
        }
        dispatcher.dispatch(event.owner.surface, notification.clone());
        notification
    }
}

fn next_action_for_task(
    status: &TaskStatus,
    worker_role: Option<crate::state::app_state::WorkerRole>,
    validation_state: Option<ValidationState>,
    task_id: &str,
) -> String {
    match status {
        TaskStatus::Running => format!("continue running task {}", task_id),
        TaskStatus::Completed => match (worker_role, validation_state) {
            (_, Some(ValidationState::PendingVerification)) => {
                format!("dispatch verify worker for {}", task_id)
            }
            (_, Some(ValidationState::Verified)) => {
                format!("synthesize validated result for {}", task_id)
            }
            (_, Some(ValidationState::VerificationFailed)) => {
                format!("inspect verification failure for {}", task_id)
            }
            (_, Some(ValidationState::Unverified)) => {
                format!("synthesize with explicit unverified risk for {}", task_id)
            }
            (Some(crate::state::app_state::WorkerRole::Research), _) => {
                format!(
                    "synthesize findings or request follow-up research for {}",
                    task_id
                )
            }
            (Some(crate::state::app_state::WorkerRole::Implement), _) => {
                format!("dispatch verify worker for {}", task_id)
            }
            (Some(crate::state::app_state::WorkerRole::Verify), _) => {
                format!("synthesize validated result for {}", task_id)
            }
            (None, _) => format!("inspect task output for {}", task_id),
        },
        TaskStatus::Failed => match (worker_role, validation_state) {
            (_, Some(ValidationState::VerificationFailed))
            | (Some(crate::state::app_state::WorkerRole::Verify), _) => {
                format!("inspect verification failure for {}", task_id)
            }
            _ => format!("inspect task output for {}", task_id),
        },
        TaskStatus::Killed => match (worker_role, validation_state) {
            (_, Some(ValidationState::Unverified))
            | (Some(crate::state::app_state::WorkerRole::Verify), _) => {
                format!("synthesize with explicit unverified risk for {}", task_id)
            }
            _ => format!("inspect task output for {}", task_id),
        },
        TaskStatus::Pending => format!("inspect task output for {}", task_id),
    }
}

fn transition_validation_state(
    worker_role: Option<crate::state::app_state::WorkerRole>,
    status: &TaskStatus,
    current: Option<ValidationState>,
) -> Option<ValidationState> {
    match (worker_role, status, current) {
        (
            Some(crate::state::app_state::WorkerRole::Implement),
            TaskStatus::Completed,
            Some(ValidationState::PendingVerification),
        ) => Some(ValidationState::PendingVerification),
        (Some(crate::state::app_state::WorkerRole::Verify), TaskStatus::Completed, _) => {
            Some(ValidationState::Verified)
        }
        (Some(crate::state::app_state::WorkerRole::Verify), TaskStatus::Failed, _) => {
            Some(ValidationState::VerificationFailed)
        }
        (Some(crate::state::app_state::WorkerRole::Verify), TaskStatus::Killed, _) => {
            Some(ValidationState::Unverified)
        }
        (_, TaskStatus::Completed, Some(state)) => Some(state),
        (_, _, current) => current,
    }
}

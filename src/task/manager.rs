use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::bootstrap::InteractionSurface;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::notification::Notification;
use crate::task::output_store::TaskOutputStore;
use crate::task::types::{
    TaskDeliveryState, TaskEvent, TaskOutputSlice, TaskOwner, TaskRecord, TaskStatus, TaskType,
    TaskUsageSummary, ValidationState, WorkerPhase, format_task_result, format_task_summary,
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

const MAX_QUEUED_EVENTS: usize = 256;

#[derive(Debug, Clone, Default)]
pub struct TaskManager {
    store: Arc<RwLock<TaskStore>>,
    runtime_store: Arc<RwLock<TaskRuntimeStore>>,
    output_store: TaskOutputStore,
    activity_tracker: Arc<RwLock<Option<Arc<AtomicU64>>>>,
}

impl TaskManager {
    pub fn new_with_output_root(root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            store: Arc::new(RwLock::new(TaskStore::default())),
            runtime_store: Arc::new(RwLock::new(TaskRuntimeStore::default())),
            output_store: TaskOutputStore::new(root),
            activity_tracker: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_activity_tracker(&self, tracker: Arc<AtomicU64>) {
        if let Ok(mut guard) = self.activity_tracker.write() {
            *guard = Some(tracker);
        }
    }
}

impl TaskManager {
    pub fn create(
        &self,
        description: impl Into<String>,
        owner_session_id: impl Into<String>,
        owner_surface: InteractionSurface,
    ) -> TaskRecord {
        self.create_with_type(
            description,
            TaskType::Generic,
            owner_session_id,
            owner_surface,
        )
    }

    pub fn create_with_type(
        &self,
        description: impl Into<String>,
        task_type: TaskType,
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
            task_type,
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
            step_id: None,
            output_file,
            output_offset: 0,
            delivery: TaskDeliveryState {
                notified: false,
                notification: None,
            },
            usage: None,
            boss_actor_id: None,
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

    pub fn set_step_id(&self, id: &str, step_id: Option<usize>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.step_id = step_id;
        }
    }

    pub fn set_boss_actor_id(&self, id: &str, boss_actor_id: Option<String>) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.boss_actor_id = boss_actor_id;
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
            && (derive_group_task_type(&tasks) != TaskType::LocalAgent
                || tasks.iter().all(|task| {
                    task.validation_state != Some(ValidationState::PendingVerification)
                }))
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
                task.task_type,
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
        self.record_activity();
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
        self.append_output_with_activity(id, chunk, true);
    }

    fn append_output_without_activity(&self, id: &str, chunk: impl AsRef<str>) {
        self.append_output_with_activity(id, chunk, false);
    }

    fn append_output_with_activity(&self, id: &str, chunk: impl AsRef<str>, record_activity: bool) {
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
            if record_activity {
                self.record_activity();
            }
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
        self.finish(id, TaskStatus::Completed, dispatcher, usage);
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
        self.finish(id, TaskStatus::Failed, dispatcher, usage);
    }

    pub fn kill(
        &self,
        id: &str,
        requester_session_id: &str,
        dispatcher: &NotificationDispatcher,
    ) -> bool {
        if !self.is_running_owned_by(id, requester_session_id) {
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
        self.finish(id, TaskStatus::Killed, dispatcher, None);
        true
    }

    pub fn force_kill(&self, id: &str, dispatcher: &NotificationDispatcher) -> bool {
        let is_active = matches!(
            self.status(id),
            Some(TaskStatus::Pending | TaskStatus::Running)
        );
        if !is_active {
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
        self.finish(id, TaskStatus::Killed, dispatcher, None);
        true
    }

    pub fn hibernate_owned_running_tasks(
        &self,
        owner_session_id: &str,
        dispatcher: &NotificationDispatcher,
    ) -> Vec<String> {
        let task_ids = self
            .list()
            .into_iter()
            .filter(|task| {
                task.owner.session_id == owner_session_id && task.status == TaskStatus::Running
            })
            .map(|task| task.id)
            .collect::<Vec<_>>();
        for task_id in &task_ids {
            self.append_output_without_activity(
                task_id,
                "housekeeping: task hibernated because the owning session became zombie\n",
            );
            if let Some(handle) = self
                .runtime_store
                .write()
                .expect("task runtime store poisoned")
                .abort_handles
                .remove(task_id)
            {
                handle.abort();
            }
            self.finish_with_activity(task_id, TaskStatus::Killed, dispatcher, None, false);
        }
        task_ids
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

    pub fn has_running_tasks_for_session(&self, session_id: &str) -> bool {
        self.list()
            .into_iter()
            .any(|task| task.owner.session_id == session_id && task.status == TaskStatus::Running)
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
        self.status(id).map(|status| status.is_terminal())
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
        self.record_activity();
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
        dispatcher: &NotificationDispatcher,
        usage: Option<TaskUsageSummary>,
    ) {
        self.finish_with_activity(id, status, dispatcher, usage, true);
    }

    fn finish_with_activity(
        &self,
        id: &str,
        status: TaskStatus,
        dispatcher: &NotificationDispatcher,
        usage: Option<TaskUsageSummary>,
        record_activity: bool,
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
            if task.status.is_terminal() {
                return;
            }
            task.status = status.clone();
            task.usage = usage.clone();
            task.delivery.notified = true;
            task.validation_state = transition_validation_state(
                task.task_type,
                task.worker_role,
                &status,
                task.validation_state,
            );
            let next_action = next_action_for_task(
                task.task_type,
                &status,
                task.worker_role,
                task.validation_state,
                &task.id,
            );
            let summary = format_task_summary(
                &task.description,
                &task.id,
                task.task_type,
                &status,
                usage.as_ref(),
            );
            let result = format_task_result(
                task.task_type,
                &status,
                task.validation_state,
                usage.as_ref(),
            );
            let event = TaskEvent {
                owner: task.owner.clone(),
                target_task_id: Some(task.id.clone()),
                task_id: task.id.clone(),
                task_type: task.task_type,
                status,
                summary,
                result,
                next_action,
                worker_role: task.worker_role,
                orchestration_group_id: task.orchestration_group_id.clone(),
                phase: task.phase,
                validation_state: task.validation_state,
                step_id: task.step_id,
                output_file: task.output_file.clone(),
                usage: usage.clone(),
            };
            self.enqueue_task_event(event.clone());
            let notification = self.dispatch_task_notification(&event, dispatcher);
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
                let group_task_id = format!("group-{}", group_id);
                let group_task_type = self.derive_group_task_type(&group_id);
                let event = TaskEvent {
                    owner,
                    target_task_id,
                    task_id: group_task_id.clone(),
                    task_type: group_task_type,
                    status: TaskStatus::Completed,
                    summary: format_task_summary(
                        group_task_type.group_summary_description(),
                        &group_task_id,
                        group_task_type,
                        &TaskStatus::Completed,
                        None,
                    ),
                    result: format_task_result(group_task_type, &TaskStatus::Completed, None, None),
                    next_action: group_task_type.group_next_action(&group_id),
                    worker_role: None,
                    orchestration_group_id: Some(group_id.clone()),
                    phase: None,
                    validation_state: None,
                    step_id: None,
                    output_file,
                    usage: None,
                };
                self.enqueue_task_event(event.clone());
                let _ = self.dispatch_task_notification(&event, dispatcher);
            }
        }
        self.propagate_verification_to_parent(id);
        if record_activity {
            self.record_activity();
        }
    }

    fn record_activity(&self) {
        let Some(tracker) = self
            .activity_tracker
            .read()
            .ok()
            .and_then(|tracker| tracker.clone())
        else {
            return;
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        tracker.store(now, Ordering::Release);
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

    fn derive_group_task_type(&self, orchestration_group_id: &str) -> TaskType {
        let tasks = self.group_tasks(orchestration_group_id);
        derive_group_task_type(&tasks)
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
        let group_type = derive_group_task_type(tasks);
        if tasks
            .iter()
            .any(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
        {
            return match group_type {
                TaskType::Generic => format!("group {} still in progress", group_id),
                TaskType::LocalBash => {
                    format!("group {} still has running command tasks", group_id)
                }
                TaskType::LocalAgent => format!("group {} still has running workers", group_id),
            };
        }
        if group_type == TaskType::LocalAgent
            && tasks
                .iter()
                .any(|task| task.validation_state == Some(ValidationState::PendingVerification))
        {
            return format!("group {} is waiting for verification", group_id);
        }
        if self.group_ready_for_fan_in(group_id) {
            return match group_type {
                TaskType::Generic => format!("group {} is ready for inspection", group_id),
                TaskType::LocalBash => {
                    format!("group {} is ready for command-result review", group_id)
                }
                TaskType::LocalAgent => format!("group {} is ready for synthesis", group_id),
            };
        }
        format!("group {} needs follow-up inspection", group_id)
    }

    fn enqueue_task_event(&self, event: TaskEvent) {
        let mut runtime_store = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned");
        if runtime_store.events.len() >= MAX_QUEUED_EVENTS {
            runtime_store.events.remove(0);
        }
        runtime_store.events.push(event);
    }

    fn dispatch_task_notification(
        &self,
        event: &TaskEvent,
        dispatcher: &NotificationDispatcher,
    ) -> Notification {
        let mut notification = Notification::task_update(
            &event.owner.session_id,
            event.result.clone(),
            event.summary.clone(),
            event.task_id.clone(),
            Some(event.task_type.as_str()),
            event.status.as_str(),
            event.next_action.clone(),
            event.worker_role.map(|role| role.as_str()),
            event.orchestration_group_id.as_deref(),
            event.phase.map(|phase| phase.as_str()),
            event.validation_state.map(|state| state.as_str()),
            event.step_id,
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
    task_type: TaskType,
    status: &TaskStatus,
    worker_role: Option<crate::state::app_state::WorkerRole>,
    validation_state: Option<ValidationState>,
    task_id: &str,
) -> String {
    match status {
        TaskStatus::Running => task_type.running_next_action(task_id),
        TaskStatus::Completed => match task_type {
            TaskType::LocalAgent => match (worker_role, validation_state) {
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
                (None, _) => task_type.default_next_action(task_id),
            },
            TaskType::LocalBash => format!("inspect command output for {}", task_id),
            TaskType::Generic => task_type.default_next_action(task_id),
        },
        TaskStatus::Failed => match task_type {
            TaskType::LocalAgent => match (worker_role, validation_state) {
                (_, Some(ValidationState::VerificationFailed))
                | (Some(crate::state::app_state::WorkerRole::Verify), _) => {
                    format!("inspect verification failure for {}", task_id)
                }
                _ => task_type.default_next_action(task_id),
            },
            _ => task_type.default_next_action(task_id),
        },
        TaskStatus::Killed => match task_type {
            TaskType::LocalAgent => match (worker_role, validation_state) {
                (_, Some(ValidationState::Unverified))
                | (Some(crate::state::app_state::WorkerRole::Verify), _) => {
                    format!("synthesize with explicit unverified risk for {}", task_id)
                }
                _ => task_type.default_next_action(task_id),
            },
            _ => task_type.default_next_action(task_id),
        },
        TaskStatus::Pending => task_type.default_next_action(task_id),
    }
}

fn transition_validation_state(
    task_type: TaskType,
    worker_role: Option<crate::state::app_state::WorkerRole>,
    status: &TaskStatus,
    current: Option<ValidationState>,
) -> Option<ValidationState> {
    if task_type != TaskType::LocalAgent {
        return current.filter(|state| *state != ValidationState::PendingVerification);
    }
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

fn derive_group_task_type(tasks: &[TaskRecord]) -> TaskType {
    let mut iter = tasks.iter();
    let Some(first) = iter.next() else {
        return TaskType::Generic;
    };
    let first_type = first.task_type;
    if iter.all(|task| task.task_type == first_type) {
        first_type
    } else {
        TaskType::Generic
    }
}

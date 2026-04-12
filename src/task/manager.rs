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
};

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
        self.finish(id, TaskStatus::Completed, "Task completed", dispatcher);
    }

    pub fn fail(&self, id: &str, dispatcher: &NotificationDispatcher) {
        self.finish(id, TaskStatus::Failed, "Task failed", dispatcher);
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
        self.finish(id, TaskStatus::Killed, "Task killed", dispatcher);
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
        self.status(id).map(|status| !matches!(status, TaskStatus::Pending | TaskStatus::Running))
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
    ) {
        self.clear_running_handle(id);
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
            let next_action = next_action_for_task(&status, task.worker_role, &task.id);
            let event = TaskEvent {
                owner: task.owner.clone(),
                target_task_id: Some(task.id.clone()),
                task_id: task.id.clone(),
                status,
                summary: format!("{} ({})", task.description, task.id),
                result: title.to_string(),
                next_action,
                worker_role: task.worker_role,
                output_file: task.output_file.clone(),
            };
            self.enqueue_task_event(event.clone());
            let notification = self.dispatch_task_notification(title, &event, dispatcher);
            task.delivery.notification = Some(notification);
        }
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
            format!("{:?}", event.status),
            event.next_action.clone(),
            event.worker_role.map(|role| role.as_str()),
            event.output_file.clone(),
        );
        if matches!(event.owner.surface, InteractionSurface::Telegram) {
            notification.target = Some(crate::interaction::notification::NotificationTarget::Session {
                session_id: event.owner.session_id.clone(),
            });
        }
        dispatcher.dispatch(event.owner.surface, notification.clone());
        notification
    }
}

fn next_action_for_task(
    status: &TaskStatus,
    worker_role: Option<crate::state::app_state::WorkerRole>,
    task_id: &str,
) -> String {
    match status {
        TaskStatus::Running => format!("continue running task {}", task_id),
        TaskStatus::Completed => match worker_role {
            Some(crate::state::app_state::WorkerRole::Research) => {
                format!("synthesize findings or request follow-up research for {}", task_id)
            }
            Some(crate::state::app_state::WorkerRole::Implement) => {
                format!("dispatch verify worker for {}", task_id)
            }
            Some(crate::state::app_state::WorkerRole::Verify) => {
                format!("synthesize validated result for {}", task_id)
            }
            None => format!("inspect task output for {}", task_id),
        },
        TaskStatus::Pending | TaskStatus::Failed | TaskStatus::Killed => {
            format!("inspect task output for {}", task_id)
        }
    }
}

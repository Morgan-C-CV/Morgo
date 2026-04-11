use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use tokio::task::AbortHandle;

use crate::bootstrap::InteractionSurface;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::notification::Notification;
use crate::task::output_store::TaskOutputStore;
use crate::task::types::{
    TaskDeliveryState, TaskEvent, TaskOutputSlice, TaskOwner, TaskRecord, TaskStatus,
};

#[derive(Debug, Clone)]
struct ContinuationRecord {
    owner: TaskOwner,
    input: String,
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
    continuations: HashMap<String, ContinuationRecord>,
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

    pub fn launch<F>(&self, id: &str, input: impl Into<String>, future: F)
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
        runtime_store
            .running_owners
            .insert(id.to_string(), owner.clone());
        runtime_store.continuations.insert(
            id.to_string(),
            ContinuationRecord {
                owner,
                input: input.into(),
            },
        );
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

    pub fn continuation_input(&self, id: &str, requester_session_id: &str) -> Option<String> {
        self.runtime_store
            .read()
            .expect("task runtime store poisoned")
            .continuations
            .get(id)
            .filter(|record| record.owner.session_id == requester_session_id)
            .map(|record| record.input.clone())
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
            let event = TaskEvent {
                owner: task.owner.clone(),
                target_task_id: Some(task.id.clone()),
                task_id: task.id.clone(),
                status,
                summary: format!("{} ({})", task.description, task.id),
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
        let notification = Notification::task_update(
            &event.owner.session_id,
            title,
            event.summary.clone(),
            event.task_id.clone(),
            format!("{:?}", event.status),
            event.output_file.clone(),
        );
        dispatcher.dispatch(event.owner.surface, notification.clone());
        notification
    }
}

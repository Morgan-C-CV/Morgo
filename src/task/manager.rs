use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use tokio::task::AbortHandle;

use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::notification::Notification;
use crate::task::output_store::TaskOutputStore;
use crate::task::types::{
    TaskDeliveryState, TaskNotification, TaskOutputSlice, TaskRecord, TaskStatus,
};

#[derive(Debug, Default)]
struct TaskStore {
    next_id: usize,
    tasks: Vec<TaskRecord>,
}

#[derive(Debug, Default)]
struct TaskRuntimeStore {
    abort_handles: HashMap<String, AbortHandle>,
    notifications: Vec<TaskNotification>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskManager {
    store: Arc<RwLock<TaskStore>>,
    runtime_store: Arc<RwLock<TaskRuntimeStore>>,
    output_store: TaskOutputStore,
}

impl TaskManager {
    pub fn create(&self, description: impl Into<String>) -> TaskRecord {
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

    pub fn launch<F>(&self, id: &str, future: F)
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
        self.runtime_store
            .write()
            .expect("task runtime store poisoned")
            .abort_handles
            .insert(id.to_string(), join_handle.abort_handle());
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

    pub fn complete(&self, id: &str, session_id: &str, dispatcher: &NotificationDispatcher) {
        self.finish(
            id,
            TaskStatus::Completed,
            session_id,
            "Task completed",
            dispatcher,
        );
    }

    pub fn fail(&self, id: &str, session_id: &str, dispatcher: &NotificationDispatcher) {
        self.finish(
            id,
            TaskStatus::Failed,
            session_id,
            "Task failed",
            dispatcher,
        );
    }

    pub fn kill(&self, id: &str, session_id: &str, dispatcher: &NotificationDispatcher) {
        if let Some(handle) = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned")
            .abort_handles
            .remove(id)
        {
            handle.abort();
        }
        self.finish(
            id,
            TaskStatus::Killed,
            session_id,
            "Task killed",
            dispatcher,
        );
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

    pub fn drain_notifications(&self, session_id: &str) -> Vec<TaskNotification> {
        let mut runtime_store = self
            .runtime_store
            .write()
            .expect("task runtime store poisoned");
        let notifications = std::mem::take(&mut runtime_store.notifications);
        let (matched, unmatched): (Vec<_>, Vec<_>) = notifications
            .into_iter()
            .partition(|notification| notification.session_id == session_id);
        runtime_store.notifications = unmatched;
        matched
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
        self.runtime_store
            .write()
            .expect("task runtime store poisoned")
            .abort_handles
            .remove(id);
    }

    fn finish(
        &self,
        id: &str,
        status: TaskStatus,
        session_id: &str,
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
            let summary = format!("{} ({})", task.description, task.id);
            self.runtime_store
                .write()
                .expect("task runtime store poisoned")
                .notifications
                .push(TaskNotification {
                    session_id: session_id.to_string(),
                    task_id: task.id.clone(),
                    status: status.clone(),
                    summary: summary.clone(),
                    output_file: task.output_file.clone(),
                });
            let notification = Notification::task_update(
                session_id,
                title,
                summary,
                task.id.clone(),
                format!("{status:?}"),
                task.output_file.clone(),
            );
            dispatcher.dispatch(notification_surface(session_id), notification.clone());
            task.delivery.notification = Some(notification);
        }
    }
}

fn notification_surface(session_id: &str) -> crate::bootstrap::InteractionSurface {
    if session_id.starts_with("telegram") {
        crate::bootstrap::InteractionSurface::Telegram
    } else if session_id.starts_with("remote") {
        crate::bootstrap::InteractionSurface::Remote
    } else {
        crate::bootstrap::InteractionSurface::Cli
    }
}

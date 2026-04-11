use std::sync::{Arc, RwLock};

use crate::interaction::notification::Notification;
use crate::task::output_store::TaskOutputStore;
use crate::task::types::{TaskDeliveryState, TaskOutputSlice, TaskRecord, TaskStatus};

#[derive(Debug, Default)]
struct TaskStore {
    next_id: usize,
    tasks: Vec<TaskRecord>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskManager {
    store: Arc<RwLock<TaskStore>>,
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

    pub fn complete(&self, id: &str, session_id: &str) {
        self.finish(id, TaskStatus::Completed, session_id, "Task completed");
    }

    pub fn fail(&self, id: &str, session_id: &str) {
        self.finish(id, TaskStatus::Failed, session_id, "Task failed");
    }

    pub fn kill(&self, id: &str, session_id: &str) {
        self.finish(id, TaskStatus::Killed, session_id, "Task killed");
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

    fn finish(&self, id: &str, status: TaskStatus, session_id: &str, title: &str) {
        if let Some(task) = self
            .store
            .write()
            .expect("task store poisoned")
            .tasks
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.status = status;
            task.delivery.notified = true;
            task.delivery.notification = Some(Notification::task_update(
                session_id,
                title,
                format!("{} ({})", task.description, task.id),
            ));
        }
    }
}

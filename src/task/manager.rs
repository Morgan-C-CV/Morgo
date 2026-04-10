use std::sync::{Arc, RwLock};

use crate::task::types::{TaskRecord, TaskStatus};

#[derive(Debug, Clone, Default)]
pub struct TaskManager {
    tasks: Arc<RwLock<Vec<TaskRecord>>>,
}

impl TaskManager {
    pub fn register(&self, id: impl Into<String>, description: impl Into<String>) -> TaskRecord {
        let task = TaskRecord {
            id: id.into(),
            description: description.into(),
            status: TaskStatus::Pending,
        };
        self.tasks
            .write()
            .expect("task store poisoned")
            .push(task.clone());
        task
    }

    pub fn transition(&self, id: &str, status: TaskStatus) {
        if let Some(task) = self
            .tasks
            .write()
            .expect("task store poisoned")
            .iter_mut()
            .find(|task| task.id == id)
        {
            task.status = status;
        }
    }

    pub fn list(&self) -> Vec<TaskRecord> {
        self.tasks.read().expect("task store poisoned").clone()
    }
}

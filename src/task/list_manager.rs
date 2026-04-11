use std::sync::{Arc, RwLock};

use crate::task::list_types::{TaskListItem, TaskListStatus};

#[derive(Debug, Default)]
struct TaskListStore {
    next_id: usize,
    tasks: Vec<TaskListItem>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskListManager {
    store: Arc<RwLock<TaskListStore>>,
}

impl TaskListManager {
    pub fn create(
        &self,
        subject: impl Into<String>,
        description: impl Into<String>,
        active_form: Option<String>,
        owner: Option<String>,
    ) -> TaskListItem {
        let mut store = self.store.write().expect("task list store poisoned");
        let id = format!("task-{}", store.next_id);
        store.next_id += 1;
        let task = TaskListItem {
            id,
            subject: subject.into(),
            description: description.into(),
            active_form,
            status: TaskListStatus::Pending,
            owner,
            blocks: Vec::new(),
            blocked_by: Vec::new(),
        };
        store.tasks.push(task.clone());
        task
    }

    pub fn list(&self) -> Vec<TaskListItem> {
        self.store
            .read()
            .expect("task list store poisoned")
            .tasks
            .clone()
    }

    pub fn get(&self, id: &str) -> Option<TaskListItem> {
        self.store
            .read()
            .expect("task list store poisoned")
            .tasks
            .iter()
            .find(|task| task.id == id)
            .cloned()
    }

    pub fn update(
        &self,
        id: &str,
        subject: Option<String>,
        description: Option<String>,
        active_form: Option<Option<String>>,
        status: Option<TaskListStatus>,
        owner: Option<Option<String>>,
    ) -> Option<TaskListItem> {
        let mut store = self.store.write().expect("task list store poisoned");
        let task = store.tasks.iter_mut().find(|task| task.id == id)?;
        if let Some(subject) = subject {
            task.subject = subject;
        }
        if let Some(description) = description {
            task.description = description;
        }
        if let Some(active_form) = active_form {
            task.active_form = active_form;
        }
        if let Some(status) = status {
            task.status = status;
        }
        if let Some(owner) = owner {
            task.owner = owner;
        }
        Some(task.clone())
    }
}

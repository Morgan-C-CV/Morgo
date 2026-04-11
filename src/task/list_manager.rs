use std::sync::{Arc, RwLock};

use crate::task::list_types::{TaskListItem, TaskListStatus};

#[derive(Debug, Default)]
pub struct TaskListUpdate {
    pub subject: Option<String>,
    pub description: Option<String>,
    pub active_form: Option<Option<String>>,
    pub status: Option<TaskListStatus>,
    pub owner: Option<Option<String>>,
    pub add_blocks: Vec<String>,
    pub add_blocked_by: Vec<String>,
}

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

    pub fn update(&self, id: &str, update: TaskListUpdate) -> anyhow::Result<TaskListItem> {
        let mut store = self.store.write().expect("task list store poisoned");
        let task_index = store
            .tasks
            .iter()
            .position(|task| task.id == id)
            .ok_or_else(|| anyhow::anyhow!("task {id} is unknown"))?;

        for target_id in update.add_blocks.iter().chain(update.add_blocked_by.iter()) {
            if !store.tasks.iter().any(|task| task.id == *target_id) {
                anyhow::bail!("task {target_id} is unknown");
            }
        }

        {
            let task = &mut store.tasks[task_index];
            if let Some(subject) = update.subject {
                task.subject = subject;
            }
            if let Some(description) = update.description {
                task.description = description;
            }
            if let Some(active_form) = update.active_form {
                task.active_form = active_form;
            }
            if let Some(status) = update.status {
                task.status = status;
            }
            if let Some(owner) = update.owner {
                task.owner = owner;
            }
        }

        for target_id in update.add_blocks {
            insert_dependency_edge(
                &mut store.tasks,
                task_index,
                &target_id,
                DependencyDirection::Blocks,
            );
        }
        for target_id in update.add_blocked_by {
            insert_dependency_edge(
                &mut store.tasks,
                task_index,
                &target_id,
                DependencyDirection::BlockedBy,
            );
        }

        Ok(store.tasks[task_index].clone())
    }
}

#[derive(Clone, Copy)]
enum DependencyDirection {
    Blocks,
    BlockedBy,
}

fn insert_dependency_edge(
    tasks: &mut [TaskListItem],
    source_index: usize,
    target_id: &str,
    direction: DependencyDirection,
) {
    let Some(target_index) = tasks.iter().position(|task| task.id == target_id) else {
        return;
    };

    if source_index == target_index {
        return;
    }

    let (source, target) = get_two_tasks_mut(tasks, source_index, target_index);
    match direction {
        DependencyDirection::Blocks => {
            push_unique(&mut source.blocks, target_id.to_string());
            let source_id = source.id.clone();
            push_unique(&mut target.blocked_by, source_id);
        }
        DependencyDirection::BlockedBy => {
            push_unique(&mut source.blocked_by, target_id.to_string());
            let source_id = source.id.clone();
            push_unique(&mut target.blocks, source_id);
        }
    }
}

fn get_two_tasks_mut(
    tasks: &mut [TaskListItem],
    left_index: usize,
    right_index: usize,
) -> (&mut TaskListItem, &mut TaskListItem) {
    if left_index < right_index {
        let (left, right) = tasks.split_at_mut(right_index);
        (&mut left[left_index], &mut right[0])
    } else {
        let (left, right) = tasks.split_at_mut(left_index);
        (&mut right[0], &mut left[right_index])
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

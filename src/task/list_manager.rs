use std::sync::{Arc, RwLock};

use crate::history::session::{SessionId, SessionStore};
use crate::plan::types::{PlanExecutionState, PlanState, PlanStep, PlanStepStatus};
use crate::task::list_types::{TaskListItem, TaskListSnapshot, TaskListStatus};

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

#[derive(Clone)]
struct TaskListPersistence {
    session_store: Arc<dyn SessionStore>,
    session_id: SessionId,
}

#[derive(Clone)]
pub struct TaskListManager {
    store: Arc<RwLock<TaskListStore>>,
    persistence: Option<TaskListPersistence>,
}

impl std::fmt::Debug for TaskListManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskListManager")
            .field("snapshot", &self.snapshot())
            .field("persistent", &self.persistence.is_some())
            .finish()
    }
}

impl Default for TaskListManager {
    fn default() -> Self {
        Self {
            store: Arc::new(RwLock::new(TaskListStore::default())),
            persistence: None,
        }
    }
}

impl TaskListManager {
    pub fn from_snapshot(snapshot: TaskListSnapshot) -> Self {
        Self {
            store: Arc::new(RwLock::new(TaskListStore {
                next_id: snapshot.next_id,
                tasks: snapshot.tasks,
            })),
            persistence: None,
        }
    }

    pub fn with_persistence(
        mut self,
        session_store: Arc<dyn SessionStore>,
        session_id: SessionId,
    ) -> Self {
        self.persistence = Some(TaskListPersistence {
            session_store,
            session_id,
        });
        self
    }

    pub fn snapshot(&self) -> TaskListSnapshot {
        let store = self.store.read().expect("task list store poisoned");
        TaskListSnapshot {
            next_id: store.next_id,
            tasks: store.tasks.clone(),
        }
    }

    pub fn create(
        &self,
        subject: impl Into<String>,
        description: impl Into<String>,
        active_form: Option<String>,
        owner: Option<String>,
        plan_step_id: Option<String>,
    ) -> TaskListItem {
        let task = {
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
                plan_step_id,
                blocks: Vec::new(),
                blocked_by: Vec::new(),
            };
            store.tasks.push(task.clone());
            task
        };
        self.persist_snapshot();
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

    pub fn sync_plan_state(&self, plan_state: &PlanState) {
        let Some(draft) = plan_state.draft.as_ref() else {
            return;
        };

        let mut changed = false;
        {
            let mut store = self.store.write().expect("task list store poisoned");
            let effective_step_statuses = draft
                .steps
                .iter()
                .map(|step| {
                    let task_status = store
                        .tasks
                        .iter()
                        .find(|task| task.plan_step_id.as_deref() == Some(step.id.as_str()))
                        .map(|task| match task.status {
                            TaskListStatus::Pending => PlanStepStatus::Pending,
                            TaskListStatus::InProgress => PlanStepStatus::InProgress,
                            TaskListStatus::Completed => PlanStepStatus::Completed,
                        })
                        .unwrap_or(step.status);
                    (step.id.as_str(), task_status)
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            let active_step_id = draft
                .steps
                .iter()
                .find(|step| {
                    effective_step_statuses
                        .get(step.id.as_str())
                        .copied()
                        .unwrap_or(step.status)
                        != PlanStepStatus::Completed
                })
                .map(|step| step.id.as_str());
            for step in &draft.steps {
                let effective_status = effective_step_statuses
                    .get(step.id.as_str())
                    .copied()
                    .unwrap_or(step.status);
                let next_status = task_list_status_for_effective_step(
                    step.id.as_str(),
                    effective_status,
                    active_step_id,
                );
                if let Some(existing) = store
                    .tasks
                    .iter_mut()
                    .find(|task| task.plan_step_id.as_deref() == Some(step.id.as_str()))
                {
                    if existing.subject != step.title {
                        existing.subject = step.title.clone();
                        changed = true;
                    }
                    let description = plan_step_description(step);
                    if existing.description != description {
                        existing.description = description;
                        changed = true;
                    }
                    if existing.status != next_status {
                        existing.status = next_status;
                        changed = true;
                    }
                    continue;
                }

                let task = TaskListItem {
                    id: format!("task-{}", store.next_id),
                    subject: step.title.clone(),
                    description: plan_step_description(step),
                    active_form: None,
                    status: next_status,
                    owner: None,
                    plan_step_id: Some(step.id.clone()),
                    blocks: Vec::new(),
                    blocked_by: Vec::new(),
                };
                store.next_id += 1;
                store.tasks.push(task);
                changed = true;
            }

            if wire_plan_task_dependencies(
                &mut store.tasks,
                draft.steps.as_slice(),
                &effective_step_statuses,
            ) {
                changed = true;
            }
            if changed {
                reorder_plan_tasks(&mut store.tasks, draft.steps.as_slice());
            }
        }

        if changed {
            self.persist_snapshot();
        }
    }

    pub fn linked_tasks_for_plan_step(&self, step_id: &str) -> Vec<TaskListItem> {
        self.store
            .read()
            .expect("task list store poisoned")
            .tasks
            .iter()
            .filter(|task| task.plan_step_id.as_deref() == Some(step_id))
            .cloned()
            .collect()
    }

    pub fn tasks_grouped_by_plan_step(
        &self,
    ) -> std::collections::BTreeMap<String, Vec<TaskListItem>> {
        let mut grouped = std::collections::BTreeMap::<String, Vec<TaskListItem>>::new();
        for task in self.list() {
            if let Some(step_id) = task.plan_step_id.clone() {
                grouped.entry(step_id).or_default().push(task);
            }
        }
        grouped
    }

    pub fn reconcile_plan_state(&self, plan_state: &PlanState) -> Option<PlanState> {
        let mut next = plan_state.clone();
        let draft = next.draft.as_mut()?;
        let grouped = self.tasks_grouped_by_plan_step();
        let mut changed = false;

        for step in &mut draft.steps {
            let Some(tasks) = grouped.get(step.id.as_str()) else {
                continue;
            };
            let next_status = if tasks
                .iter()
                .any(|task| task.status == TaskListStatus::InProgress)
            {
                PlanStepStatus::InProgress
            } else if tasks
                .iter()
                .all(|task| task.status == TaskListStatus::Completed)
            {
                PlanStepStatus::Completed
            } else {
                PlanStepStatus::Pending
            };
            if step.status != next_status {
                step.status = next_status;
                changed = true;
            }
        }

        let total_steps = draft.steps.len();
        let completed_steps = draft
            .steps
            .iter()
            .filter(|step| step.status == PlanStepStatus::Completed)
            .count();
        let active_step_id = draft
            .steps
            .iter()
            .find(|step| step.status == PlanStepStatus::InProgress)
            .or_else(|| {
                draft
                    .steps
                    .iter()
                    .find(|step| step.status == PlanStepStatus::Pending)
            })
            .map(|step| step.id.clone());
        let progress_percent = if total_steps == 0 {
            0
        } else {
            ((completed_steps * 100) / total_steps) as u8
        };
        let next_execution = PlanExecutionState {
            active_step_id,
            completed_steps,
            total_steps,
            progress_percent,
            last_updated_at: next
                .execution
                .as_ref()
                .and_then(|execution| execution.last_updated_at.clone()),
        };
        if next.execution.as_ref() != Some(&next_execution) {
            next.execution = Some(next_execution);
            changed = true;
        }

        changed.then_some(next)
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

        let updated = store.tasks[task_index].clone();
        drop(store);
        self.persist_snapshot();
        Ok(updated)
    }

    fn persist_snapshot(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        persistence
            .session_store
            .save_task_list(&persistence.session_id, self.snapshot());
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

fn task_list_status_for_effective_step(
    step_id: &str,
    status: PlanStepStatus,
    active_step_id: Option<&str>,
) -> TaskListStatus {
    match status {
        PlanStepStatus::Completed => TaskListStatus::Completed,
        PlanStepStatus::InProgress => TaskListStatus::InProgress,
        PlanStepStatus::Pending if active_step_id == Some(step_id) => TaskListStatus::InProgress,
        PlanStepStatus::Pending => TaskListStatus::Pending,
    }
}

fn wire_plan_task_dependencies(
    tasks: &mut [TaskListItem],
    steps: &[PlanStep],
    step_statuses: &std::collections::BTreeMap<&str, PlanStepStatus>,
) -> bool {
    let mut changed = false;
    for index in 0..steps.len() {
        let step = &steps[index];
        let Some(task_index) = tasks
            .iter()
            .position(|task| task.plan_step_id.as_deref() == Some(step.id.as_str()))
        else {
            continue;
        };

        let expected_blocked_by = if index == 0 {
            Vec::new()
        } else {
            let previous = &steps[index - 1];
            let previous_status = step_statuses
                .get(previous.id.as_str())
                .copied()
                .unwrap_or(previous.status);
            if previous_status == PlanStepStatus::Completed {
                Vec::new()
            } else if let Some(previous_task) = tasks
                .iter()
                .find(|task| task.plan_step_id.as_deref() == Some(previous.id.as_str()))
            {
                vec![previous_task.id.clone()]
            } else {
                Vec::new()
            }
        };

        let expected_blocks = if index + 1 >= steps.len() {
            Vec::new()
        } else {
            let next = &steps[index + 1];
            let next_status = step_statuses
                .get(next.id.as_str())
                .copied()
                .unwrap_or(next.status);
            if next_status == PlanStepStatus::Completed {
                Vec::new()
            } else if let Some(next_task) = tasks
                .iter()
                .find(|task| task.plan_step_id.as_deref() == Some(next.id.as_str()))
            {
                vec![next_task.id.clone()]
            } else {
                Vec::new()
            }
        };

        let task = &mut tasks[task_index];
        if task.blocked_by != expected_blocked_by {
            task.blocked_by = expected_blocked_by;
            changed = true;
        }
        if task.blocks != expected_blocks {
            task.blocks = expected_blocks;
            changed = true;
        }
    }
    changed
}

fn plan_step_description(step: &PlanStep) -> String {
    step.details
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| step.title.clone())
}

fn reorder_plan_tasks(tasks: &mut Vec<TaskListItem>, steps: &[PlanStep]) {
    let mut ordered = Vec::with_capacity(tasks.len());
    for step in steps {
        if let Some(index) = tasks
            .iter()
            .position(|task| task.plan_step_id.as_deref() == Some(step.id.as_str()))
        {
            ordered.push(tasks.remove(index));
        }
    }
    ordered.append(tasks);
    *tasks = ordered;
}

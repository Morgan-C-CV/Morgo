use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::TaskStatus;

#[test]
fn terminal_task_states_mark_delivery_notified() {
    let manager = TaskManager::default();
    let task = manager.register("task-1", "demo task");
    manager.transition(&task.id, TaskStatus::Completed);

    let stored = manager
        .list()
        .into_iter()
        .find(|item| item.id == "task-1")
        .unwrap();
    assert_eq!(stored.status, TaskStatus::Completed);
    assert!(stored.delivery.notified);
}

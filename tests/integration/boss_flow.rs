use std::sync::Arc;
use rust_agent::core::boss::{BossCoordinator, save_plan};
use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossStage};
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskType, TaskStatus, TaskEvent};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;

#[tokio::test]
async fn test_boss_mode_feedback_loop_and_auto_sequencing() {
    let temp_dir = std::env::temp_dir();
    let plan_path = temp_dir.join("test_boss_flow.json");

    // 1. Setup a plan with 2 steps
    let mut plan = BossPlan {
        task_description: "Multi-step task".into(),
        steps: vec![
            BossPlanStep {
                id: 0,
                description: "Step 1".into(),
                completed: false,
                result_diff: None,
                worker_task_id: None,
            },
            BossPlanStep {
                id: 1,
                description: "Step 2".into(),
                completed: false,
                result_diff: None,
                worker_task_id: None,
            },
        ],
        accepted_by_user: true,
        auto_sequence: true, // Enable auto-sequencing
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    // 2. Initialize BossCoordinator
    let coordinator = Arc::new(BossCoordinator::restore_or_init(&plan_path).await.unwrap());
    assert_eq!(coordinator.get_stage().await, BossStage::Execution);

    // 3. Setup AppState and Dispatcher
    let tasks = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default())
        .with_boss_coordinator(coordinator.clone());
    
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks.clone())
        .with_notification_dispatcher(dispatcher.clone())
        .with_boss_coordinator(coordinator.clone());

    let app_state = Arc::new(AppState {
        runtime_role: RuntimeRole::Coordinator,
        permission_context: permissions,
        boss_coordinator: Some(coordinator.clone()),
        notification_dispatcher: dispatcher.clone(),
        // ... (other fields dummy)
        ..Default::default()
    });

    // 4. Simulate Worker 1 finishing Step 0
    let event = TaskEvent {
        task_id: "worker-task-1".into(),
        task_type: TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: Some(0),
        // ... (other fields dummy)
        owner: rust_agent::task::types::TaskOwner {
            session_id: "test-session".into(),
            surface: rust_agent::bootstrap::InteractionSurface::Cli,
        },
        summary: "Step 1 done".into(),
        result: "Success".into(),
        next_action: "None".into(),
        worker_role: Some(WorkerRole::Implement),
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    };

    coordinator.on_task_event(&event).await.unwrap();

    // 5. Verify Step 0 is completed
    {
        let plan_guard = coordinator.plan.read().await;
        let plan = plan_guard.as_ref().unwrap();
        assert!(plan.steps[0].completed);
        assert_eq!(plan.steps[0].worker_task_id, Some("worker-task-1".into()));
    }

    // 6. Verify Auto-Sequencing identifies Step 1
    let next_action = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(next_action.is_some());
    assert!(next_action.unwrap().contains("Step 2"));

    // Cleanup
    let _ = std::fs::remove_file(plan_path);
}

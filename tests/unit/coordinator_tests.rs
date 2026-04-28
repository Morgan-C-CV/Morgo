use std::sync::Arc;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::coordinator::mode::{
    is_coordinator_mode, match_session_mode, set_coordinator_mode,
};
use rust_agent::coordinator::prompt::build_coordinator_system_prompt;
use rust_agent::coordinator::worker::{
    TaskNotification, filter_tools_for_worker, notification_to_task_notification,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::notification::{Notification, NotificationType};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus, ValidationState, WorkerPhase};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::builtin::ask_user::AskUserQuestionTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::builtin::web_search::WebSearchTool;
use rust_agent::tool::definition::Tool;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

#[test]
fn coordinator_mode_matches_resumed_session() {
    set_coordinator_mode(false);
    let message = match_session_mode(Some("coordinator"));
    assert!(is_coordinator_mode());
    assert_eq!(
        message.as_deref(),
        Some("Entered coordinator mode to match resumed session.")
    );
}

#[test]
fn worker_notification_formats_as_task_notification_xml() {
    let event = TaskEvent {
        owner: TaskOwner {
            session_id: "session-1".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some("task-7".into()),
        task_id: "task-7".into(),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        summary: "Worker finished research".into(),
        result: "Task completed".into(),
        next_action: "inspect task output for task-7".into(),
        worker_role: None,
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        step_id: None,
        output_file: "/tmp/task-7.log".into(),
        usage: None,
    };

    let notification = TaskNotification::from_task_event(&event);
    let formatted = notification.format_as_user_message();
    assert!(formatted.contains("<task-notification>"));
    assert!(formatted.contains("<task-id>task-7</task-id>"));
    assert!(formatted.contains("<task-type>local_agent</task-type>"));
    assert!(formatted.contains("<summary>Worker finished research</summary>"));
    assert!(formatted.contains("<output-file>/tmp/task-7.log</output-file>"));
}

#[test]
fn notification_conversion_preserves_worker_role_and_next_action() {
    let notification = Notification {
        session_id: "session-1".into(),
        title: "Task completed".into(),
        body: "Worker finished verify".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-8".into()),
        task_type: Some("local_agent".into()),
        status: Some("Completed".into()),
        next_action: Some("inspect task output for task-8".into()),
        worker_role: Some("verify".into()),
        orchestration_group_id: None,
        phase: Some("verify".into()),
        validation_state: Some("verified".into()),
        step_id: None,
        output_file: Some("/tmp/task-8.log".into()),
        usage: None,
        tool_name: None,
        approval_code: None,
        approval_summary: None,
        approval_detail: None,
        approval_kind: None,
        approval_escalation_reasons: Vec::new(),
        notice_kind: None,
        notice_code: None,
        runtime_kind: None,
        service_failure_code: None,
        provider_kind: None,
        status_code: None,
        retryable: None,
        surface_visible: None,
        dedupe_key: None,
        wake_up: true,
        target: None,
    };

    let converted = notification_to_task_notification(&notification).expect("should convert");
    assert_eq!(converted.task_id, "task-8");
    assert_eq!(
        converted.task_type,
        rust_agent::task::types::TaskType::LocalAgent
    );
    assert_eq!(converted.status, TaskStatus::Completed);
    assert_eq!(converted.next_action, "inspect task output for task-8");
    assert_eq!(converted.worker_role, Some(WorkerRole::Verify));
    assert_eq!(converted.phase, Some(WorkerPhase::Verify));
    assert_eq!(converted.validation_state, Some(ValidationState::Verified));
}

#[test]
fn notification_conversion_parses_status_case_insensitively_and_handles_unknown_safely() {
    let running_lowercase = Notification {
        session_id: "session-1".into(),
        title: "Task running".into(),
        body: "Worker still running".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-running".into()),
        task_type: Some("local_agent".into()),
        status: Some("running".into()),
        next_action: None,
        worker_role: None,
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        step_id: None,
        output_file: None,
        usage: None,
        tool_name: None,
        approval_code: None,
        approval_summary: None,
        approval_detail: None,
        approval_kind: None,
        approval_escalation_reasons: Vec::new(),
        notice_kind: None,
        notice_code: None,
        runtime_kind: None,
        service_failure_code: None,
        provider_kind: None,
        status_code: None,
        retryable: None,
        surface_visible: None,
        dedupe_key: None,
        wake_up: true,
        target: None,
    };
    let unknown_status = Notification {
        session_id: "session-1".into(),
        title: "Task state unknown".into(),
        body: "Worker emitted unknown status".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-unknown".into()),
        task_type: Some("local_agent".into()),
        status: Some("mystery".into()),
        next_action: None,
        worker_role: None,
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        step_id: None,
        output_file: None,
        usage: None,
        tool_name: None,
        approval_code: None,
        approval_summary: None,
        approval_detail: None,
        approval_kind: None,
        approval_escalation_reasons: Vec::new(),
        notice_kind: None,
        notice_code: None,
        runtime_kind: None,
        service_failure_code: None,
        provider_kind: None,
        status_code: None,
        retryable: None,
        surface_visible: None,
        dedupe_key: None,
        wake_up: true,
        target: None,
    };

    let running = notification_to_task_notification(&running_lowercase).expect("should convert");
    let unknown = notification_to_task_notification(&unknown_status).expect("should convert");

    assert_eq!(running.status, TaskStatus::Running);
    assert_eq!(unknown.status, TaskStatus::Pending);
}

#[test]
fn coordinator_worker_filter_excludes_interactive_and_deferred_tools() {
    let all_tools = vec![
        AgentTool.metadata(),
        AskUserQuestionTool.metadata(),
        FileReadTool.metadata(),
        WebSearchTool.metadata(),
    ];

    let filtered = filter_tools_for_worker(&all_tools);
    let names = filtered.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert!(names.contains(&"Read"));
    assert!(!names.contains(&"Agent"));
    assert!(!names.contains(&"AskUserQuestion"));
    assert!(!names.contains(&"WebSearch"));
}

fn coordinator_test_app_state() -> AppState {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default);
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(Arc::new(CommandRegistry::default())),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: Arc::new(std::sync::Mutex::new(
            rust_agent::security::audit::AuditLog::default(),
        )),
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: "test-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    }
}

#[test]
fn coordinator_prompt_describes_parallel_research_fan_out_and_fan_in_contract() {
    let prompt = build_coordinator_system_prompt(&coordinator_test_app_state());

    assert!(prompt.contains("Parallelize only independent research tasks"));
    assert!(prompt.contains("wait for their task notifications before synthesizing"));
    assert!(prompt.contains("allowed_tools"));
    assert!(prompt.contains("max_turns"));
    assert!(prompt.contains("reuse_strategy"));
}

#[test]
fn coordinator_prompt_requires_verify_after_implement_before_final_answer() {
    let prompt = build_coordinator_system_prompt(&coordinator_test_app_state());

    assert!(prompt.contains("After a non-trivial implement worker completes, dispatch a fresh verify worker before giving the user a final answer."));
    assert!(prompt.contains("The final answer belongs to the coordinator"));
    assert!(prompt.contains("describe validation status"));
}

#[test]
fn task_notification_contract_marks_implement_completion_for_verify_follow_up() {
    let event = TaskEvent {
        owner: TaskOwner {
            session_id: "session-1".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some("task-9".into()),
        task_id: "task-9".into(),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        summary: "Implement worker finished patch".into(),
        result: "Task completed".into(),
        next_action: "dispatch verify worker for task-9".into(),
        worker_role: Some(WorkerRole::Implement),
        orchestration_group_id: None,
        phase: Some(WorkerPhase::Implement),
        validation_state: Some(ValidationState::PendingVerification),
        step_id: None,
        output_file: "/tmp/task-9.log".into(),
        usage: None,
    };

    let notification = TaskNotification::from_task_event(&event);
    let formatted = notification.format_as_user_message();

    assert_eq!(
        notification.task_type,
        rust_agent::task::types::TaskType::LocalAgent
    );
    assert_eq!(notification.worker_role, Some(WorkerRole::Implement));
    assert_eq!(notification.phase, Some(WorkerPhase::Implement));
    assert_eq!(
        notification.validation_state,
        Some(ValidationState::PendingVerification)
    );
    assert_eq!(
        notification.next_action,
        "dispatch verify worker for task-9"
    );
    assert!(formatted.contains("<task-type>local_agent</task-type>"));
    assert!(formatted.contains("<worker-role>implement</worker-role>"));
    assert!(formatted.contains("<phase>implement</phase>"));
    assert!(formatted.contains("<validation-state>pending_verification</validation-state>"));
    assert!(formatted.contains("<next-action>dispatch verify worker for task-9</next-action>"));
}

#[test]
fn task_notification_contract_marks_verify_completion_for_validated_synthesis() {
    let event = TaskEvent {
        owner: TaskOwner {
            session_id: "session-1".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some("task-10".into()),
        task_id: "task-10".into(),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        summary: "Verify worker finished checks".into(),
        result: "Task completed".into(),
        next_action: "synthesize validated result for task-10".into(),
        worker_role: Some(WorkerRole::Verify),
        orchestration_group_id: None,
        phase: Some(WorkerPhase::Verify),
        validation_state: Some(ValidationState::Verified),
        step_id: None,
        output_file: "/tmp/task-10.log".into(),
        usage: None,
    };

    let notification = TaskNotification::from_task_event(&event);
    let formatted = notification.format_as_user_message();

    assert_eq!(
        notification.task_type,
        rust_agent::task::types::TaskType::LocalAgent
    );
    assert_eq!(notification.worker_role, Some(WorkerRole::Verify));
    assert_eq!(notification.phase, Some(WorkerPhase::Verify));
    assert_eq!(
        notification.validation_state,
        Some(ValidationState::Verified)
    );
    assert_eq!(
        notification.next_action,
        "synthesize validated result for task-10"
    );
    assert!(formatted.contains("<task-type>local_agent</task-type>"));
    assert!(formatted.contains("<worker-role>verify</worker-role>"));
    assert!(formatted.contains("<phase>verify</phase>"));
    assert!(formatted.contains("<validation-state>verified</validation-state>"));
    assert!(
        formatted.contains("<next-action>synthesize validated result for task-10</next-action>")
    );
}

#[test]
fn coordinator_prompt_requires_risk_callout_when_verification_is_missing() {
    let prompt = build_coordinator_system_prompt(&coordinator_test_app_state());

    assert!(prompt.contains("call out any unverified risk"));
    assert!(prompt.contains("describe validation status"));
}

#[test]
fn coordinator_notification_conversion_is_independent_of_wake_up_flag() {
    let notification = Notification {
        session_id: "session-1".into(),
        title: "Task completed".into(),
        body: "Worker finished verify".into(),
        notification_type: NotificationType::TaskUpdate,
        task_id: Some("task-11".into()),
        task_type: Some("local_agent".into()),
        status: Some("Completed".into()),
        next_action: Some("synthesize validated result for task-11".into()),
        worker_role: Some("verify".into()),
        orchestration_group_id: None,
        phase: Some("verify".into()),
        validation_state: Some("verified".into()),
        step_id: None,
        output_file: Some("/tmp/task-11.log".into()),
        usage: None,
        tool_name: None,
        approval_code: None,
        approval_summary: None,
        approval_detail: None,
        approval_kind: None,
        approval_escalation_reasons: Vec::new(),
        notice_kind: None,
        notice_code: None,
        runtime_kind: None,
        service_failure_code: None,
        provider_kind: None,
        status_code: None,
        retryable: None,
        surface_visible: None,
        dedupe_key: None,
        wake_up: false,
        target: None,
    };

    let converted = notification_to_task_notification(&notification).expect("should convert");
    assert_eq!(converted.task_id, "task-11");
    assert_eq!(
        converted.task_type,
        rust_agent::task::types::TaskType::LocalAgent
    );
    assert_eq!(
        converted.next_action,
        "synthesize validated result for task-11"
    );
    assert_eq!(converted.validation_state, Some(ValidationState::Verified));
}

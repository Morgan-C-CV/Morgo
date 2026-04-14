use std::sync::Arc;
use std::{fs, path::PathBuf, process::Command, time::SystemTime};

use tokio::sync::RwLock;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::message::Message;
use rust_agent::history::session::{SessionHistory, SessionHistoryEntry, SessionSnapshot};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::skills::registry::SkillRegistry;
use rust_agent::skills::types::{SkillDefinition, SkillExecutionContext, SkillSource};
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::state::plan_mode;
use rust_agent::task::list_manager::{TaskListManager, TaskListUpdate};
use rust_agent::task::list_types::TaskListStatus;
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{ValidationState, WorkerPhase};
use rust_agent::tool::registry::ToolRegistry;

fn sample_skill() -> SkillDefinition {
    SkillDefinition {
        name: "summarize-skill".into(),
        description: "Summarize repository state".into(),
        when_to_use: Some("Use when triaging repo state".into()),
        argument_hint: Some("target path".into()),
        workflow_hint: Some("inspect then summarize".into()),
        workflow_summary: Some(
            "inspect then summarize | args: target path | use: Use when triaging repo state".into(),
        ),
        allowed_tools: vec!["Read".into()],
        aliases: vec![],
        user_invocable: true,
        disable_model_invocation: false,
        hidden: false,
        paths: vec![],
        exclude_paths: vec![],
        requires_files: vec![],
        context: SkillExecutionContext::Inline,
        content: "skill body".into(),
        source: SkillSource::Filesystem,
        file_path: None,
    }
}

fn build_plan_permissions() -> ToolPermissionContext {
    let plan_manager = Arc::new(PlanManager::default());
    plan_manager.ensure_draft(None);
    plan_manager.set_summary("Execute approved plan");
    let inspect = plan_manager
        .add_step("Inspect state", Some("collect current signals"))
        .expect("add inspect step");
    let patch = plan_manager
        .add_step("Patch output", Some("apply smallest change"))
        .expect("add patch step");

    let task_list = Arc::new(TaskListManager::default());
    let inspect_task = task_list.create(
        "Inspect state",
        "collect current signals",
        None,
        Some("planner".into()),
        Some(inspect.id.clone()),
    );
    let patch_task = task_list.create(
        "Patch output",
        "apply smallest change",
        None,
        None,
        Some(patch.id.clone()),
    );
    task_list
        .update(
            &inspect_task.id,
            TaskListUpdate {
                status: Some(TaskListStatus::Completed),
                ..Default::default()
            },
        )
        .expect("complete inspect task");
    task_list
        .update(
            &patch_task.id,
            TaskListUpdate {
                status: Some(TaskListStatus::InProgress),
                ..Default::default()
            },
        )
        .expect("start patch task");

    let task_manager = Arc::new(TaskManager::default());
    let runtime_task = task_manager.create(
        "runtime patch execution",
        "context-session",
        InteractionSurface::Cli,
    );
    task_manager.set_orchestration_group_id(&runtime_task.id, Some(patch.id.clone()));
    task_manager.set_worker_role(&runtime_task.id, WorkerRole::Implement);
    task_manager.set_phase(&runtime_task.id, Some(WorkerPhase::Implement));
    task_manager.set_validation_state(&runtime_task.id, Some(ValidationState::PendingVerification));
    task_manager.start(&runtime_task.id);

    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager)
        .with_plan_manager(plan_manager.clone())
        .with_task_list_manager(task_list.clone());
    plan_mode::apply_exit_plan_mode(&permissions, "ready to execute").expect("approve plan");
    task_list
        .update(
            &inspect_task.id,
            TaskListUpdate {
                status: Some(TaskListStatus::Completed),
                ..Default::default()
            },
        )
        .expect("restore inspect task completion after sync");
    task_list
        .update(
            &patch_task.id,
            TaskListUpdate {
                status: Some(TaskListStatus::InProgress),
                ..Default::default()
            },
        )
        .expect("restore patch task progress after sync");
    permissions
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rust-agent-{label}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn run_git(cwd: &PathBuf, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .status()
        .expect("run git command");
    assert!(status.success(), "git command failed: {:?}", args);
}

fn init_test_repo(label: &str) -> PathBuf {
    let repo = unique_temp_dir(label);
    run_git(&repo, &["init"]);
    run_git(&repo, &["config", "user.email", "context-tests@example.com"]);
    run_git(&repo, &["config", "user.name", "Context Tests"]);
    fs::write(repo.join("README.md"), "seed\n").expect("write seed file");
    run_git(&repo, &["add", "README.md"]);
    run_git(&repo, &["commit", "-m", "seed"]);
    repo
}

fn build_app_state_with_cwd(cwd: &str) -> AppState {
    let mut state = build_app_state();
    if let Some(session) = state.session.as_mut() {
        session.cwd = cwd.to_string();
    }
    state
}

fn build_app_state() -> AppState {
    let permissions = build_plan_permissions()
        .with_external_memory_entries(vec![
            "linear:INGEST-42 investigate context layering".into(),
            "slack:#agents rollout note".into(),
        ])
        .with_nested_memory_lineage(vec!["session:context-session".into()]);

    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Worker,
        worker_role: Some(WorkerRole::Verify),
        permission_context: permissions,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: Some(Arc::new(SkillRegistry::new(vec![sample_skill()]))),
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        startup_trace: vec!["DetectSurface".into(), "Setup".into()],
        active_session_id: "context-session".into(),
        session_store: None,
        session: Some(SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("context-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/context-demo".into(),
            last_turn_at: None,
            prompt_seed: Some("feature/context".into()),
        }),
        history: Some(SessionHistory {
            entries: vec![
                SessionHistoryEntry {
                    message: Message::user("inspect prompt context"),
                    timestamp: None,
                    tool_refs: vec!["src/context/git.rs".into()],
                    milestone: None,
                },
                SessionHistoryEntry {
                    message: Message::assistant("summarized runtime state"),
                    timestamp: None,
                    tool_refs: vec!["src/prompt/system.rs".into()],
                    milestone: None,
                },
            ],
        }),
        restored_session: None,
    }
}

#[test]
fn context_prompt_includes_truthy_runtime_sections() {
    let repo = init_test_repo("context-prompt-repo");
    let app_state = build_app_state_with_cwd(repo.to_string_lossy().as_ref());
    let prompt = rust_agent::prompt::context::build_context_prompt(&app_state);

    assert!(prompt.contains("Runtime context summary:"));
    assert!(prompt.contains("Git context:"));
    assert!(prompt.contains(&format!("- cwd: {}", repo.to_string_lossy())));
    assert!(prompt.contains("- repository: yes"));
    assert!(prompt.contains("- branch: "));
    assert!(!prompt.contains("- branch: <unknown>"));
    assert!(prompt.contains("- dirty: "));
    assert!(!prompt.contains("- dirty: <unknown>"));
    assert!(prompt.contains("- repo_root: "));
    assert!(prompt.contains("- worktree: "));
    assert!(prompt.contains("Session memory:"));
    assert!(prompt.contains("- session_id: context-session"));
    assert!(prompt.contains("External memory:"));
    assert!(prompt.contains("- entries: 2"));
    assert!(prompt.contains("linear:INGEST-42 investigate context layering"));
    assert!(prompt.contains("Nested memory lineage:"));
    assert!(prompt.contains("- depth: 1"));
    assert!(prompt.contains("- path: session:context-session"));
    assert!(prompt.contains("Runtime user context:"));
    assert!(prompt.contains("- client_type: Cli"));
    assert!(prompt.contains("- worker_role: verify"));
    assert!(prompt.contains("Available skills:"));
    assert!(prompt.contains(
        "workflow: inspect then summarize | args: target path | use: Use when triaging repo state"
    ));
    assert!(prompt.contains("Approved plan status: approved"));
    assert!(prompt.contains("Execution summary: 1/2 completed (50%)"));
    assert!(prompt.contains("Active step: step-2"));
    assert!(prompt.contains("Next actionable step: Patch output"));
    assert!(prompt.contains("Linked task summary: linked_steps=2, blocked_tasks=0, in_progress_steps=1, completed_steps=1"));
    assert!(prompt.contains("Runtime orchestration summary: groups=1, waiting_for_verification=0, ready_for_synthesis=0, still_in_progress=1"));
    assert!(prompt.contains("Active step runtime hint: group step-2 still in progress"));
    assert!(prompt.contains("Active runtime task hint: verification next for task-0"));
    assert!(prompt.contains("runtime_group=step-2 runtime_hint=group step-2 still in progress"));

    fs::remove_dir_all(repo).expect("cleanup repo");
}

#[test]
fn git_context_reports_non_repo_fallback() {
    let dir = unique_temp_dir("context-prompt-non-repo");
    let app_state = build_app_state_with_cwd(dir.to_string_lossy().as_ref());
    let prompt = rust_agent::prompt::context::build_context_prompt(&app_state);

    assert!(prompt.contains("Git context:"));
    assert!(prompt.contains("- repository: no"));
    assert!(prompt.contains("- branch: <unknown>"));
    assert!(prompt.contains("- dirty: <unknown>"));

    fs::remove_dir_all(dir).expect("cleanup non-repo dir");
}

#[test]
fn worker_system_prompt_includes_role_specific_guidance() {
    let app_state = build_app_state();
    let prompt = rust_agent::prompt::system::build_system_prompt(&app_state);

    assert!(prompt.contains("You are a verify worker."));
    assert!(prompt.contains("Respect coordinator intent"));
    assert!(prompt.contains("surface=Cli"));
    assert!(prompt.contains("worker_role=verify"));
}

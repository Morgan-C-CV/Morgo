use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{
    BootstrapCli, BootstrapPhase, BootstrapState, InteractionSurface, PromptAugmentationMetadata,
    RuntimeBootstrap, SessionMode, SessionSource, UserAccessDecision, is_tui_exit_input,
    tui_clear_screen_prefix,
};
use rust_agent::state::app_state::{AppState, AppStateRuntimeChange, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::state::store::AppStateStore;
use rust_agent::core::message::Message;
use rust_agent::history::resume::{
    RestoreRequest, RestoreSource, resolve_session_state,
};
use rust_agent::history::session::{
    FileBackedSessionStore, InMemorySessionStore, SessionHistory, SessionHistoryEntry, SessionId,
    SessionRestoreRequest, SessionSnapshot, SessionStore,
};
use rust_agent::hook::registry::{HookConfigSource, HookEvent, load_hook_registry};
use rust_agent::task::list_types::{TaskListItem, TaskListSnapshot, TaskListStatus};

fn unique_temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn bootstrap_state_records_phase_order() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, true);
    state.record_phase(BootstrapPhase::DetectSurface);
    state.record_phase(BootstrapPhase::ResolvePermissions);
    state = state.finalize();

    assert_eq!(
        state.startup_trace(),
        "DetectSurface -> ResolvePermissions -> FinalizeState"
    );
}

#[test]
fn app_state_store_notifies_subscribers_after_committed_update() {
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: None,
        runtime_tool_registry: None,
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
        startup_trace: Vec::new(),
        active_session_id: "session-1".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let store = AppStateStore::new(app_state);
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_clone = observed.clone();
    store.subscribe(move |update| {
        observed_clone
            .lock()
            .expect("observation lock")
            .push((update.generation, update.current.permission_context.mode()));
    });

    let update = store.update(|state| {
        state.permission_context.set_mode(PermissionMode::Plan);
    });

    assert_eq!(update.generation, 1);
    assert_eq!(store.generation(), 1);
    assert_eq!(store.get().permission_context.mode(), PermissionMode::Plan);
    assert_eq!(
        observed.lock().expect("observation lock").as_slice(),
        &[(1, PermissionMode::Plan)]
    );
}

#[test]
fn app_state_classifies_runtime_visible_changes() {
    let previous = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: None,
        runtime_tool_registry: None,
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
        startup_trace: Vec::new(),
        active_session_id: "session-1".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
    };
    let mut current = AppState {
        surface: previous.surface,
        session_mode: previous.session_mode,
        client_type: previous.client_type,
        session_source: previous.session_source,
        runtime_role: previous.runtime_role,
        worker_role: previous.worker_role,
        permission_context: ToolPermissionContext::new(PermissionMode::Plan),
        command_registry: previous.command_registry.clone(),
        runtime_tool_registry: previous.runtime_tool_registry.clone(),
        skill_registry: previous.skill_registry.clone(),
        mcp_runtime: previous.mcp_runtime.clone(),
        plugin_load_result: previous.plugin_load_result.clone(),
        cost_tracker: previous.cost_tracker.clone(),
        notification_dispatcher: previous.notification_dispatcher.clone(),
        startup_trace: previous.startup_trace.clone(),
        active_session_id: previous.active_session_id.clone(),
        session_store: previous.session_store.clone(),
        session: previous.session.clone(),
        history: previous.history.clone(),
        restored_session: previous.restored_session.clone(),
    };
    current.bind_surface_session(
        InteractionSurface::Remote,
        rust_agent::bootstrap::ClientType::RemoteControl,
        SessionSource::RemoteControl,
        "remote-session",
    );

    let change_set = AppState::classify_runtime_changes(&previous, &current);

    assert!(change_set.changes.contains(&AppStateRuntimeChange::PermissionChanged));
    assert!(change_set.changes.contains(&AppStateRuntimeChange::SurfaceBindingChanged));
}

#[test]
fn in_memory_session_store_loads_latest_session_for_continue() {
    let store = InMemorySessionStore::default();
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-123".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/demo".into(),
            last_turn_at: Some("2026-04-11T00:00:00Z".into()),
            prompt_seed: Some("seed".into()),
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("restored"),
                timestamp: Some("2026-04-11T00:00:01Z".into()),
                tool_refs: vec!["Read".into()],
                milestone: None,
            }],
        },
    );

    let loaded = store.load(&SessionRestoreRequest {
        resume: None,
        continue_session: true,
    });

    let (snapshot, history) = loaded.expect("expected stored session");
    assert_eq!(snapshot.session_id.0, "session-123");
    assert_eq!(history.entries.len(), 1);
}

#[test]
fn in_memory_session_store_round_trips_task_lists_by_session() {
    let store = InMemorySessionStore::default();
    let session_id = SessionId("session-task-list".into());
    let snapshot = TaskListSnapshot {
        next_id: 3,
        tasks: vec![TaskListItem {
            id: "task-0".into(),
            subject: "persisted task".into(),
            description: "survives restore".into(),
            active_form: Some("Persisting".into()),
            status: TaskListStatus::InProgress,
            owner: Some("session-task-list".into()),
            plan_step_id: None,
            blocks: vec!["task-1".into()],
            blocked_by: vec![],
        }],
    };

    store.save_task_list(&session_id, snapshot.clone());

    assert_eq!(store.load_task_list(&session_id), Some(snapshot));
    assert_eq!(
        store.load_task_list(&SessionId("other-session".into())),
        None
    );
}

#[test]
fn resolve_session_state_reuses_store_for_continue_resume_and_fresh_start() {
    let store = InMemorySessionStore::default();
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-restore".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/restore".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("restored"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            }],
        },
    );

    let continued = resolve_session_state(
        &store,
        Some(&RestoreRequest {
            source: RestoreSource::ContinueSession,
            session_id: None,
        }),
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/fresh"),
    );
    assert_eq!(continued.snapshot.session_id.0, "session-restore");
    assert_eq!(continued.snapshot.surface, InteractionSurface::Remote);
    assert!(continued.restored_session.is_some());

    let resumed_missing = resolve_session_state(
        &store,
        Some(&RestoreRequest {
            source: RestoreSource::ResumeSession,
            session_id: Some("missing-session".into()),
        }),
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/fallback"),
    );
    assert_eq!(resumed_missing.snapshot.session_id.0, "missing-session");
    assert_eq!(resumed_missing.snapshot.surface, InteractionSurface::Cli);
    assert_eq!(resumed_missing.snapshot.session_mode, SessionMode::Headless);
    assert!(resumed_missing.restored_session.is_some());

    let fresh = resolve_session_state(
        &store,
        None,
        InteractionSurface::Cli,
        SessionMode::InitOnly,
        std::path::Path::new("/tmp/fresh-start"),
    );
    assert_eq!(fresh.snapshot.session_id.0, "local-session");
    assert_eq!(fresh.snapshot.surface, InteractionSurface::Cli);
    assert_eq!(fresh.snapshot.session_mode, SessionMode::InitOnly);
    assert!(fresh.history.entries.is_empty());
    assert!(fresh.restored_session.is_none());
}

#[test]
fn initialize_runtime_builds_consistent_runtime_bundle_shape() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");

    let bundle = runtime.initialize_runtime(
        &state,
        "session-init".into(),
        Arc::new(rust_agent::task::manager::TaskManager::default()),
        Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
        Arc::new(rust_agent::plan::manager::PlanManager::default()),
    );

    assert!(!bundle.command_registry.names().is_empty());
    assert!(!bundle.coordinator_tools.all_metadata().is_empty());
    assert_eq!(bundle.api_client.provider_config(), bundle.provider_config);
}

#[test]
fn augment_prompt_depends_on_input_state_without_mutating_store() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.current_cwd = std::env::current_dir().expect("cwd available");
    let bundle = runtime.initialize_runtime(
        &state,
        "session-prompts".into(),
        Arc::new(rust_agent::task::manager::TaskManager::default()),
        Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
        Arc::new(rust_agent::plan::manager::PlanManager::default()),
    );
    let resolved = resolve_session_state(
        &InMemorySessionStore::default(),
        None,
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/prompt"),
    );
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
        command_registry: Some(bundle.command_registry.clone()),
        runtime_tool_registry: Some(bundle.runtime_tool_registry.clone()),
        skill_registry: Some(bundle.skill_registry.clone()),
        mcp_runtime: Some(bundle.mcp_runtime.clone()),
        plugin_load_result: Some(bundle.plugin_load_result.clone()),
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: bundle.notification_dispatcher.clone(),
        startup_trace: Vec::new(),
        active_session_id: "session-prompts".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(resolved.snapshot.clone()),
        history: Some(resolved.history.clone()),
        restored_session: resolved.restored_session.clone(),
    };
    let store = AppStateStore::new(app_state.clone());
    let before = store.generation();
    let prompts = runtime.augment_prompts(&app_state, &bundle);
    let after = store.generation();

    assert_eq!(before, after);
    assert_eq!(
        prompts.metadata,
        PromptAugmentationMetadata {
            active_session_id: "session-prompts".into(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            visible_tool_count: bundle
                .coordinator_tools
                .visible_tools(&app_state.permission_context)
                .len(),
        }
    );
    assert!(!prompts.system_prompt.is_empty());
    assert!(!prompts.tools_prompt.is_empty());
    assert!(!prompts.context_prompt.is_empty());
}

#[test]
fn gate_user_access_matches_cli_remote_and_telegram_expectations() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });

    let cli_state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Interactive, false);
    let cli_input = rust_agent::interaction::envelope::NormalizedInput::from_session_raw(
        InteractionSurface::Cli,
        "session-cli",
        "/permissions",
    );
    assert_eq!(
        runtime.gate_user_access(&cli_state, Some(&cli_input)),
        UserAccessDecision {
            allowed: true,
            reason: None,
        }
    );

    let remote_state = BootstrapState::new(InteractionSurface::Remote, SessionMode::Interactive, false);
    let remote_input = rust_agent::interaction::envelope::NormalizedInput::from_remote_raw(
        "session-remote",
        "actor-a",
        true,
        true,
        "/permissions",
    );
    assert_eq!(runtime.gate_user_access(&remote_state, Some(&remote_input)).allowed, false);

    let telegram_state = BootstrapState::new(InteractionSurface::Telegram, SessionMode::Interactive, false);
    let telegram_input = rust_agent::interaction::envelope::NormalizedInput::from_session_raw(
        InteractionSurface::Telegram,
        "session-telegram",
        "hello",
    );
    assert_eq!(
        runtime.gate_user_access(&telegram_state, Some(&telegram_input)),
        UserAccessDecision {
            allowed: true,
            reason: None,
        }
    );
}

#[test]
fn file_backed_session_store_round_trips_across_store_instances() {
    let root = unique_temp_path("rust-agent-session-store");
    let store_a = FileBackedSessionStore::new(root.clone());
    let session_id = SessionId("session-file-backed".into());
    let snapshot = SessionSnapshot {
        session_id: session_id.clone(),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        cwd: "/tmp/file-backed".into(),
        last_turn_at: Some("2026-04-11T12:00:00Z".into()),
        prompt_seed: Some("seed".into()),
    };
    let history = SessionHistory {
        entries: vec![SessionHistoryEntry {
            message: Message::assistant("persisted history"),
            timestamp: Some("2026-04-11T12:00:01Z".into()),
            tool_refs: vec!["TaskList".into()],
            milestone: None,
        }],
    };
    let task_list = TaskListSnapshot {
        next_id: 2,
        tasks: vec![TaskListItem {
            id: "task-0".into(),
            subject: "persisted task".into(),
            description: "from file-backed store".into(),
            active_form: Some("Persisting".into()),
            status: TaskListStatus::Pending,
            owner: Some("session-file-backed".into()),
            plan_step_id: None,
            blocks: vec![],
            blocked_by: vec![],
        }],
    };

    store_a.save(snapshot.clone(), history.clone());
    store_a.save_task_list(&session_id, task_list.clone());

    let store_b = FileBackedSessionStore::new(root.clone());
    let loaded = store_b.load(&SessionRestoreRequest {
        resume: Some("session-file-backed".into()),
        continue_session: false,
    });
    assert_eq!(loaded, Some((snapshot, history)));
    assert_eq!(store_b.load_task_list(&session_id), Some(task_list));

    std::fs::remove_dir_all(root).expect("cleanup file-backed session store");
}

#[test]
fn finalize_runtime_state_is_single_writeback_entrypoint() {
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    });
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Headless, false);
    state.record_phase(BootstrapPhase::InitializeRuntime);
    state.record_phase(BootstrapPhase::AugmentPrompt);
    state.record_phase(BootstrapPhase::GateUserAccess);
    let state = state.finalize();

    let resolved = resolve_session_state(
        &InMemorySessionStore::default(),
        None,
        InteractionSurface::Cli,
        SessionMode::Headless,
        std::path::Path::new("/tmp/finalize"),
    );
    let bundle = runtime.initialize_runtime(
        &state,
        resolved.active_session_id(),
        Arc::new(rust_agent::task::manager::TaskManager::default()),
        Arc::new(rust_agent::task::list_manager::TaskListManager::default()),
        Arc::new(rust_agent::plan::manager::PlanManager::default()),
    );
    let prompt_state = AppState {
        surface: state.surface,
        session_mode: state.session_mode,
        client_type: state.client_type,
        session_source: state.session_source,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_active_session_id(resolved.active_session_id())
            .with_active_surface(state.surface),
        command_registry: Some(bundle.command_registry.clone()),
        runtime_tool_registry: Some(bundle.runtime_tool_registry.clone()),
        skill_registry: Some(bundle.skill_registry.clone()),
        mcp_runtime: Some(bundle.mcp_runtime.clone()),
        plugin_load_result: Some(bundle.plugin_load_result.clone()),
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        notification_dispatcher: bundle.notification_dispatcher.clone(),
        startup_trace: state
            .phases
            .iter()
            .map(|phase| format!("{phase:?}"))
            .collect(),
        active_session_id: resolved.active_session_id(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: Some(resolved.snapshot.clone()),
        history: Some(resolved.history.clone()),
        restored_session: resolved.restored_session.clone(),
    };
    let prompts = runtime.augment_prompts(&prompt_state, &bundle);
    let finalized = runtime.finalize_runtime_state(
        &state,
        resolved.clone(),
        bundle,
        prompts.clone(),
        resolved.active_session_id(),
    );

    assert_eq!(finalized.app_state.active_session_id, resolved.active_session_id());
    assert_eq!(finalized.store.generation(), 0);
    assert_eq!(finalized.engine.context.system_prompt, prompts.system_prompt);
    assert_eq!(
        finalized.engine.context.tools_prompt,
        rust_agent::prompt::tools::build_tools_prompt(
            &finalized.engine.context.tool_registry,
            &finalized.app_state.permission_context,
        )
    );
    assert_eq!(finalized.engine.context.context_prompt, prompts.context_prompt);
}

#[tokio::test]
async fn runtime_continue_session_uses_restored_snapshot() {
    let store = Arc::new(InMemorySessionStore::default());
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-continue".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            cwd: "/tmp/continue".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("hello again"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            }],
        },
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: true,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_session_store(store);

    runtime.run().await.expect("runtime should run");
}

#[tokio::test]
async fn runtime_resume_prefers_restored_surface_and_mode() {
    let store = Arc::new(InMemorySessionStore::default());
    store.save(
        SessionSnapshot {
            session_id: SessionId("session-remote".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/resume".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: Some("session-remote".into()),
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_session_store(store);

    runtime
        .run()
        .await
        .expect("runtime should run with restored mode");
}

#[tokio::test]
async fn runtime_restores_persisted_task_list_for_resumed_session() {
    let store = Arc::new(InMemorySessionStore::default());
    let session_id = SessionId("session-with-tasks".into());
    store.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/tasks".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    store.insert_task_list(
        session_id,
        TaskListSnapshot {
            next_id: 2,
            tasks: vec![TaskListItem {
                id: "task-0".into(),
                subject: "restored task".into(),
                description: "from persisted state".into(),
                active_form: Some("Restoring".into()),
                status: TaskListStatus::Pending,
                owner: Some("session-with-tasks".into()),
                plan_step_id: None,
                blocks: vec![],
                blocked_by: vec![],
            }],
        },
    );

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: Some("TaskGet task-0".into()),
        interactive: false,
        init_only: false,
        continue_session: false,
        resume: Some("session-with-tasks".into()),
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_session_store(store);

    runtime
        .run()
        .await
        .expect("runtime should restore persisted task list");
}

#[tokio::test]
async fn runtime_continue_restores_from_file_backed_store_across_instances() {
    let root = unique_temp_path("rust-agent-runtime-continue");
    let session_id = SessionId("session-continue-durable".into());
    let store_a = Arc::new(FileBackedSessionStore::new(root.clone()));
    store_a.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/runtime-continue".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::assistant("durable session"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            }],
        },
    );
    store_a.save_task_list(
        &session_id,
        TaskListSnapshot {
            next_id: 2,
            tasks: vec![TaskListItem {
                id: "task-0".into(),
                subject: "durable task".into(),
                description: "restored across instances".into(),
                active_form: Some("Durably restoring".into()),
                status: TaskListStatus::Pending,
                owner: Some("session-continue-durable".into()),
                plan_step_id: None,
                blocks: vec![],
                blocked_by: vec![],
            }],
        },
    );

    let store_b = Arc::new(FileBackedSessionStore::new(root.clone()));
    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: Some("TaskGet task-0".into()),
        interactive: false,
        init_only: false,
        continue_session: true,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_session_store(store_b);

    runtime
        .run()
        .await
        .expect("runtime should continue from durable session store");

    std::fs::remove_dir_all(root).expect("cleanup durable runtime test store");
}

#[tokio::test]
async fn runtime_initializes_fresh_session_record_in_store() {
    let store = Arc::new(InMemorySessionStore::default());

    let runtime = RuntimeBootstrap::from_cli(BootstrapCli {
        print: None,
        interactive: false,
        init_only: true,
        continue_session: false,
        resume: None,
        trace_startup: false,
        show_tools: false,
        tui: false,
        surface: "cli".into(),
    })
    .with_session_store(store.clone());

    runtime
        .run()
        .await
        .expect("runtime should initialize session");

    let loaded = store.load(&SessionRestoreRequest {
        resume: None,
        continue_session: true,
    });
    let (snapshot, history) = loaded.expect("expected initialized session record");
    assert_eq!(snapshot.session_id.0, "local-session");
    assert_eq!(snapshot.surface, InteractionSurface::Cli);
    assert_eq!(snapshot.session_mode, SessionMode::InitOnly);
    assert!(history.entries.is_empty());
}

#[test]
fn file_backed_session_store_persists_appended_turns_across_instances() {
    let root = unique_temp_path("rust-agent-session-history");
    let session_id = SessionId("session-history-durable".into());
    let store_a = FileBackedSessionStore::new(root.clone());
    store_a.save(
        SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/history".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    store_a.append_entry(
        &session_id,
        SessionHistoryEntry {
            message: Message::user("hello"),
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: None,
        },
    );
    store_a.append_entry(
        &session_id,
        SessionHistoryEntry {
            message: Message::assistant("hi there"),
            timestamp: None,
            tool_refs: Vec::new(),
            milestone: None,
        },
    );

    let store_b = FileBackedSessionStore::new(root.clone());
    let (_, history) = store_b
        .load(&SessionRestoreRequest {
            resume: Some("session-history-durable".into()),
            continue_session: false,
        })
        .expect("expected durable history after append");
    assert_eq!(history.entries.len(), 2);
    assert_eq!(history.entries[0].message, Message::user("hello"));
    assert_eq!(history.entries[1].message, Message::assistant("hi there"));

    std::fs::remove_dir_all(root).expect("cleanup durable history store");
}

#[test]
fn hook_event_enum_exposes_bootstrap_lifecycle_markers() {
    assert_eq!(HookEvent::SessionStart, HookEvent::SessionStart);
    assert_eq!(HookEvent::Setup, HookEvent::Setup);
}

#[test]
fn bootstrap_hook_loader_defaults_without_project_config() {
    let root = unique_temp_path("rust-agent-bootstrap-hooks");
    std::fs::create_dir_all(&root).expect("create bootstrap hook root");

    let registry = load_hook_registry(&root);
    let load_result = registry
        .config_load_result()
        .expect("bootstrap should retain hook load metadata");
    assert_eq!(load_result.source, HookConfigSource::Defaults);
    assert!(
        load_result
            .diagnostics
            .iter()
            .any(|line| line.contains("No .claude/hooks.json found"))
    );

    std::fs::remove_dir_all(root).expect("cleanup bootstrap hook root");
}

#[test]
fn tui_exit_input_matches_expected_commands() {
    assert!(is_tui_exit_input("/exit"));
    assert!(is_tui_exit_input("exit"));
    assert!(is_tui_exit_input("quit"));
    assert!(is_tui_exit_input("  quit  "));
    assert!(!is_tui_exit_input("/help"));
    assert!(!is_tui_exit_input(""));
}

#[test]
fn tui_clear_screen_prefix_uses_terminal_escape_sequence() {
    assert_eq!(tui_clear_screen_prefix(), "\x1B[2J\x1B[H");
}

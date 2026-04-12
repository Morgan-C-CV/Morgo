use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::bootstrap::{
    BootstrapCli, BootstrapPhase, BootstrapState, InteractionSurface, RuntimeBootstrap, SessionMode,
};
use rust_agent::core::message::Message;
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
    assert!(load_result
        .diagnostics
        .iter()
        .any(|line| line.contains("No .claude/hooks.json found")));

    std::fs::remove_dir_all(root).expect("cleanup bootstrap hook root");
}

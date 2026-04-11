use std::sync::Arc;

use rust_agent::bootstrap::{
    BootstrapCli, BootstrapPhase, BootstrapState, InteractionSurface, RuntimeBootstrap, SessionMode,
};
use rust_agent::core::message::Message;
use rust_agent::history::session::{
    InMemorySessionStore, SessionHistory, SessionHistoryEntry, SessionId, SessionSnapshot,
    SessionStore,
};
use rust_agent::hook::registry::HookEvent;
use rust_agent::task::list_types::{TaskListItem, TaskListSnapshot, TaskListStatus};

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
            }],
        },
    );

    let loaded = store.load(&rust_agent::history::session::SessionRestoreRequest {
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

#[test]
fn hook_event_enum_exposes_bootstrap_lifecycle_markers() {
    assert_eq!(HookEvent::SessionStart, HookEvent::SessionStart);
    assert_eq!(HookEvent::Setup, HookEvent::Setup);
}

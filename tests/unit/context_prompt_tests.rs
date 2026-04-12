use std::sync::Arc;

use tokio::sync::RwLock;

use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::skills::registry::SkillRegistry;
use rust_agent::skills::types::{SkillDefinition, SkillExecutionContext, SkillSource};
use rust_agent::core::message::Message;
use rust_agent::history::session::{SessionHistory, SessionHistoryEntry, SessionSnapshot};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::{AppState, RuntimeRole, WorkerRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::tool::registry::ToolRegistry;

fn sample_skill() -> SkillDefinition {
    SkillDefinition {
        name: "summarize-skill".into(),
        description: "Summarize repository state".into(),
        when_to_use: Some("Use when triaging repo state".into()),
        argument_hint: Some("target path".into()),
        workflow_hint: Some("inspect then summarize".into()),
        workflow_summary: Some("inspect then summarize | args: target path | use: Use when triaging repo state".into()),
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

fn build_app_state() -> AppState {
    AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Worker,
        worker_role: Some(WorkerRole::Verify),
        permission_context: ToolPermissionContext::new(PermissionMode::Default),
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
    let app_state = build_app_state();
    let prompt = rust_agent::prompt::context::build_context_prompt(&app_state);

    assert!(prompt.contains("Runtime context summary:"));
    assert!(prompt.contains("Git context:"));
    assert!(prompt.contains("- cwd: /tmp/context-demo"));
    assert!(prompt.contains("- branch: feature/context"));
    assert!(prompt.contains("- dirty: yes"));
    assert!(prompt.contains("Session memory:"));
    assert!(prompt.contains("- session_id: context-session"));
    assert!(prompt.contains("Runtime user context:"));
    assert!(prompt.contains("- client_type: Cli"));
    assert!(prompt.contains("- worker_role: verify"));
    assert!(prompt.contains("Available skills:"));
    assert!(prompt.contains("workflow: inspect then summarize | args: target path | use: Use when triaging repo state"));
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

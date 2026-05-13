use std::path::{Path, PathBuf};
use std::sync::Arc;

use rust_agent::bootstrap::{
    BootstrapPhase, BootstrapState, ClientType, InteractionSurface, SessionMode, SessionSource,
};
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::events::EngineEvent;
use rust_agent::core::message::Message;
use rust_agent::core::query_loop::{QueryLoopState, Terminal};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::hook::registry::HookRegistry;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::security::filesystem_policy::{
    FilesystemPermissionLevel, FilesystemPolicy, FilesystemPolicyConfig, FilesystemPolicyRule,
};
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::service::observability::ServiceObservabilityTracker;
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::bash::BashTool;
use rust_agent::tool::builtin::file_edit::FileEditTool;
use rust_agent::tool::builtin::file_read::FileReadTool;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

#[path = "plan_resume_flow.rs"]
mod plan_resume_flow;
#[path = "plugin_flow.rs"]
mod plugin_flow;
#[path = "remote_flow.rs"]
mod remote_flow;
#[path = "skills_visibility.rs"]
mod skills_visibility;
#[path = "telegram_transport_flow.rs"]
mod telegram_transport_flow;
#[path = "web_flow.rs"]
mod web_flow;

#[tokio::test]
async fn startup_trace_contains_detect_surface_phase() {
    let mut state = BootstrapState::new(InteractionSurface::Cli, SessionMode::Print, false);
    state.enter_phase(BootstrapPhase::DetectSurface);
    state.enter_phase(BootstrapPhase::InjectSessionMetadata);

    assert!(state.startup_trace().contains("DetectSurface"));
}

fn allow_write_policy_for(root: &Path) -> Arc<FilesystemPolicy> {
    Arc::new(
        FilesystemPolicy::from_config(FilesystemPolicyConfig {
            protected_paths: Vec::new(),
            rules: vec![
                FilesystemPolicyRule {
                    path: root.to_string_lossy().to_string(),
                    level: FilesystemPermissionLevel::Allow,
                },
                FilesystemPolicyRule {
                    path: std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .to_string_lossy()
                        .to_string(),
                    level: FilesystemPermissionLevel::Allow,
                },
            ],
        })
        .expect("filesystem policy should build"),
    )
}

fn coding_smoke_context(
    turns: Vec<Vec<StreamEvent>>,
    workspace_root: &Path,
) -> QueryContext {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace_root));
    permission_context.add_always_allow_rule("Edit");

    let tool_registry = ToolRegistry::new()
        .register(Arc::new(FileReadTool))
        .register(Arc::new(FileEditTool))
        .register(Arc::new(BashTool));

    QueryContext {
        app_state: AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: Some(Arc::new(RwLock::new(tool_registry.clone()))),
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker: ServiceObservabilityTracker::default(),
            notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
            audit_log: Arc::new(std::sync::Mutex::new(
                rust_agent::security::audit::AuditLog::default(),
            )),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source:
                rust_agent::state::app_state::ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary:
                rust_agent::state::app_state::ActiveModelProviderSummary {
                    provider_id: "scripted".into(),
                    protocol: "Scripted".into(),
                    compatibility_profile: "Scripted".into(),
                    base_url_host: "localhost".into(),
                    model: "cli-smoke-model".into(),
                    auth_status: "none".into(),
                },
            active_session_id: "cli-smoke-coding-session".into(),
            session_store: None,
            session: None,
            history: None,
            restored_session: None,
            last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            subagent_limiter: None,
            boss_coordinator: None,
            remote_actor_store: None,
        },
        tool_registry,
        api_client: ModelProviderClient::with_scripted_turns(turns),
        compactor: ReactiveCompactor,
        hook_registry: HookRegistry::default(),
        agent_id: None,
        system_prompt: "test system".into(),
        tools_prompt: "test tools".into(),
        context_prompt: "test context".into(),
    }
}

fn assert_tool_started(events: &[EngineEvent], tool_name: &str) {
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::ToolCallStarted { tool_name: name, .. } if name == tool_name
        )),
        "coding smoke stalled before {tool_name}: missing ToolCallStarted event"
    );
}

fn assert_tool_result_contains(events: &[EngineEvent], tool_name: &str, expected: &str) {
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::ToolResultCommitted {
                tool_name: name,
                content,
                ..
            } if name == tool_name && content.contains(expected)
        )),
        "coding smoke stalled at {tool_name}: missing tool result containing {expected:?}"
    );
}

#[tokio::test]
async fn cli_smoke_coding_loop_reads_edits_verifies_and_concludes() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let target = workspace.path().join("smoke_target.txt");
    std::fs::write(&target, "status = \"todo\"\n").expect("seed smoke target");

    let target_display = target.to_string_lossy().to_string();
    let read_input = serde_json::json!({
        "file_path": target_display,
    })
    .to_string();
    let edit_input = serde_json::json!({
        "file_path": target_display,
        "old_string": "status = \"todo\"",
        "new_string": "status = \"done\""
    })
    .to_string();
    let bash_input = serde_json::json!({
        "command": format!("grep -n 'status = \"done\"' {}", target.display()),
        "timeout": 5_000
    })
    .to_string();
    let final_summary = format!(
        "Updated {} by changing status from todo to done. Verification passed: grep found status = \"done\".",
        target.display()
    );

    let engine = QueryEngine::new(coding_smoke_context(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Inspecting the target file before editing.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Read".into(),
                    input: read_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Applying the requested edit.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Edit".into(),
                    input: edit_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Running a local verification command.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: bash_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta(final_summary.clone()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        workspace.path(),
    ));

    let result = engine
        .submit_turn(Message::user(
            "Open the local file, change status from todo to done, run a local verification command, and tell me if it passed.",
        ))
        .await;

    assert_eq!(
        result.state,
        QueryLoopState::Completed,
        "coding smoke did not complete"
    );
    assert_eq!(
        result.terminal,
        Terminal::Completed,
        "coding smoke did not reach a completed terminal state"
    );

    assert_tool_started(&result.events, "Read");
    assert_tool_result_contains(&result.events, "Read", "status = \"todo\"");

    assert_tool_started(&result.events, "Edit");
    assert_tool_result_contains(&result.events, "Edit", "edited");

    assert_tool_started(&result.events, "Bash");
    assert_tool_result_contains(&result.events, "Bash", "status = \"done\"");

    let final_message = result
        .messages
        .iter()
        .rev()
        .find(|message| message.role == rust_agent::core::message::Role::Assistant)
        .map(|message| message.text())
        .unwrap_or_default();
    assert!(
        final_message.contains("status from todo to done"),
        "coding smoke stalled at final summary: missing concrete change description; final={final_message:?}"
    );
    assert!(
        final_message.contains("Verification passed"),
        "coding smoke stalled at final summary: missing verification verdict; final={final_message:?}"
    );

    let updated = std::fs::read_to_string(&target).expect("read updated target");
    assert_eq!(
        updated,
        "status = \"done\"\n",
        "coding smoke stalled at edit verification: file contents were not updated"
    );
}

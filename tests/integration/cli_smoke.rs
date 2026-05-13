use std::path::{Path, PathBuf};
use std::sync::Arc;

use rust_agent::bootstrap::{
    BootstrapPhase, BootstrapState, ClientType, InteractionSurface, SessionMode, SessionSource,
};
use rust_agent::command::builtin::resume::ResumeCommand;
use rust_agent::command::builtin::tasks::TasksCommand;
use rust_agent::command::types::Command;
use rust_agent::command::types::CommandResult;
use rust_agent::core::context::QueryContext;
use rust_agent::core::engine::QueryEngine;
use rust_agent::core::events::EngineEvent;
use rust_agent::core::message::Message;
use rust_agent::core::query_loop::{
    QueryLoopState, QueryParams, Terminal, run_query_loop_with_params,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::resume::resolved_from_snapshot;
use rust_agent::history::session::{
    SessionHistory, SessionHistoryEntry, SessionId, SessionSnapshot,
};
use rust_agent::hook::registry::HookRegistry;
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
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
use rust_agent::tool::result::ToolExecutionOutcomeKind;
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

fn coding_smoke_context(turns: Vec<Vec<StreamEvent>>, workspace_root: &Path) -> QueryContext {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace_root));
    permission_context.add_always_allow_rule("Edit");

    coding_smoke_context_with_permissions(turns, permission_context)
}

fn coding_smoke_context_with_permissions(
    turns: Vec<Vec<StreamEvent>>,
    permission_context: ToolPermissionContext,
) -> QueryContext {
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

fn final_assistant_message_text(messages: &[rust_agent::core::message::Message]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| message.role == rust_agent::core::message::Role::Assistant)
        .map(|message| message.text())
        .unwrap_or_default()
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

    let context = coding_smoke_context(
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
    );

    let mut engine = QueryEngine::new(context);
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

    let final_message = final_assistant_message_text(&result.messages);
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
        updated, "status = \"done\"\n",
        "coding smoke stalled at edit verification: file contents were not updated"
    );
}

#[tokio::test]
async fn cli_smoke_coding_loop_repairs_after_failed_verification() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let target = workspace.path().join("smoke_repair_target.txt");
    std::fs::write(&target, "status = \"todo\"\n").expect("seed repair smoke target");

    let target_display = target.to_string_lossy().to_string();
    let read_input = serde_json::json!({
        "file_path": target_display,
    })
    .to_string();
    let first_edit_input = serde_json::json!({
        "file_path": target_display,
        "old_string": "status = \"todo\"",
        "new_string": "status = \"don\""
    })
    .to_string();
    let verify_input = serde_json::json!({
        "command": format!("grep -n 'status = \"done\"' {}", target.display()),
        "timeout": 5_000
    })
    .to_string();
    let repair_edit_input = serde_json::json!({
        "file_path": target_display,
        "old_string": "status = \"don\"",
        "new_string": "status = \"done\""
    })
    .to_string();
    let final_summary = format!(
        "Initial verification failed because {} still did not contain status = \"done\". I repaired the file by changing status from don to done, reran verification, and the second check passed.",
        target.display()
    );

    let context = coding_smoke_context(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Reading the file before attempting the change.".into()),
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
                StreamEvent::TextDelta("Applying the first draft edit.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Edit".into(),
                    input: first_edit_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Running the first verification command.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: verify_input.clone(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("The first verification failed, repairing the file.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Edit".into(),
                    input: repair_edit_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Rerunning verification after the repair.".into()),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: verify_input,
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
    );

    let result = run_query_loop_with_params(
        &context,
        Message::user(
            "Change the local file to status done, verify it locally, repair any failure, and report the final outcome clearly.",
        ),
        QueryParams {
            max_turns: Some(8),
            ..QueryParams::default()
        },
    )
    .await;

    assert_eq!(
        result.state,
        QueryLoopState::Completed,
        "repair smoke did not complete"
    );
    assert_eq!(
        result.terminal,
        Terminal::Completed,
        "repair smoke did not reach a completed terminal state"
    );

    assert_tool_started(&result.events, "Read");
    assert_tool_result_contains(&result.events, "Read", "status = \"todo\"");

    let edit_successes = result
        .events
        .iter()
        .filter(|event| {
            matches!(
                event,
                EngineEvent::ToolResultCommitted { tool_name, .. } if tool_name == "Edit"
            )
        })
        .count();
    assert!(
        edit_successes >= 2,
        "repair smoke stalled at file repair: expected two successful Edit results, got {edit_successes}"
    );
    assert_tool_result_contains(&result.events, "Edit", "edited");

    let bash_results = result
        .events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::ToolResultCommitted {
                tool_name, content, ..
            } if tool_name == "Bash" => Some(content.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        bash_results.len() >= 2,
        "repair smoke stalled at verification loop: expected two Bash results, got {}",
        bash_results.len()
    );
    assert!(
        bash_results
            .iter()
            .any(|content| content.contains("exit_code: 1")),
        "repair smoke stalled at first verification failure: missing Bash result with exit_code 1"
    );
    assert!(
        bash_results.iter().any(|content| {
            content.contains("exit_code: 0") && content.contains("status = \"done\"")
        }),
        "repair smoke stalled at second verification pass: missing successful Bash result"
    );

    let final_message = final_assistant_message_text(&result.messages);
    assert!(
        final_message.contains("Initial verification failed"),
        "repair smoke stalled at final summary: missing failed-first explanation; final={final_message:?}"
    );
    assert!(
        final_message.contains("second check passed"),
        "repair smoke stalled at final summary: missing repaired verification verdict; final={final_message:?}"
    );

    let updated = std::fs::read_to_string(&target).expect("read repaired target");
    assert_eq!(
        updated, "status = \"done\"\n",
        "repair smoke stalled at final file verification: file contents were not repaired"
    );
}

#[tokio::test]
async fn cli_smoke_coding_loop_requests_more_context_when_target_is_underspecified() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let sentinel = workspace.path().join("do_not_touch.txt");
    std::fs::write(&sentinel, "leave me alone\n").expect("seed sentinel file");

    let mut engine = QueryEngine::new(coding_smoke_context(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta(
                "I need more context before changing anything: please provide the target file path or a specific failing test, because the request is too underspecified to edit safely."
                    .into(),
            ),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
            },
        ]],
        workspace.path(),
    ));

    let result = engine
        .submit_turn(Message::user(
            "Please fix the bug somewhere in this project and make it work.",
        ))
        .await;

    assert_eq!(
        result.state,
        QueryLoopState::Completed,
        "underspecified-context smoke did not complete cleanly"
    );
    assert_eq!(
        result.terminal,
        Terminal::Completed,
        "underspecified-context smoke did not stop cleanly"
    );

    assert!(
        !result.events.iter().any(|event| matches!(
            event,
            EngineEvent::ToolCallStarted { tool_name, .. }
                if tool_name == "Edit" || tool_name == "Bash"
        )),
        "underspecified-context smoke incorrectly escalated into Edit or Bash without enough context"
    );
    assert!(
        !result.events.iter().any(|event| matches!(
            event,
            EngineEvent::ToolResultCommitted { tool_name, .. }
                if tool_name == "Edit" || tool_name == "Bash"
        )),
        "underspecified-context smoke incorrectly committed Edit or Bash results"
    );
    assert!(
        !result.events.iter().any(|event| matches!(
            event,
            EngineEvent::ToolResultCommitted {
                tool_name,
                summary,
                ..
            } if (tool_name == "Edit" || tool_name == "Bash")
                && summary.ends_with("succeeded")
        )),
        "underspecified-context smoke incorrectly entered a success path for Edit or Bash"
    );

    let final_message = final_assistant_message_text(&result.messages).to_lowercase();
    assert!(
        final_message.contains("need more context")
            || final_message.contains("target file")
            || final_message.contains("specific failing test")
            || final_message.contains("underspecified"),
        "underspecified-context smoke stalled at final summary: missing clear request for context; final={final_message:?}"
    );
    assert!(
        !final_message.contains("verification passed")
            && !final_message.contains("updated ")
            && !final_message.contains("fixed "),
        "underspecified-context smoke incorrectly reported success; final={final_message:?}"
    );

    let sentinel_contents = std::fs::read_to_string(&sentinel).expect("read sentinel");
    assert_eq!(
        sentinel_contents, "leave me alone\n",
        "underspecified-context smoke should not modify files when the target is unclear"
    );
}

#[tokio::test]
async fn cli_smoke_coding_loop_surfaces_bash_pending_approval_without_false_success() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let target = workspace.path().join("smoke_approval_target.txt");
    std::fs::write(&target, "status = \"todo\"\n").expect("seed approval smoke target");

    let target_display = target.to_string_lossy().to_string();
    let read_input = serde_json::json!({
        "file_path": target_display,
    })
    .to_string();
    let bash_input = serde_json::json!({
        "command": format!("grep -n 'status = \"todo\"' {}", target.display()),
        "timeout": 5_000
    })
    .to_string();

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace.path()));
    permission_context.add_always_allow_rule("Edit");
    permission_context.add_always_ask_rule("Bash");

    let mut engine = QueryEngine::new(coding_smoke_context_with_permissions(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Inspecting the target file before verification.".into()),
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
                StreamEvent::TextDelta(
                    "I need to run a local verification command, which may require approval."
                        .into(),
                ),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: bash_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
        ],
        permission_context,
    ));

    let result = engine
        .submit_turn(Message::user(
            "Read the local file and run a verification command, but stop for approval if the command needs it.",
        ))
        .await;

    assert_eq!(
        result.state,
        QueryLoopState::Interrupted,
        "pending-approval smoke should stop in an interrupted approval state"
    );
    assert_eq!(
        result.terminal,
        Terminal::AbortedTools,
        "pending-approval smoke should stop at the tool approval barrier"
    );

    assert_tool_started(&result.events, "Read");
    assert_tool_result_contains(&result.events, "Read", "status = \"todo\"");

    assert_tool_started(&result.events, "Bash");
    assert!(
        result.events.iter().any(|event| matches!(
            event,
            EngineEvent::PendingApproval { tool_name, message, .. }
                if tool_name == "Bash"
                    && (message.contains("approval") || message.contains("requires"))
        )),
        "pending-approval smoke stalled before approval handoff: missing EngineEvent::PendingApproval"
    );
    assert!(
        !result.events.iter().any(|event| matches!(
            event,
            EngineEvent::ToolResultCommitted {
                tool_name,
                kind,
                summary,
                ..
            } if tool_name == "Bash"
                && *kind == ToolExecutionOutcomeKind::Success
                && summary.ends_with("succeeded")
        )),
        "pending-approval smoke incorrectly committed a successful Bash result"
    );

    let final_message = final_assistant_message_text(&result.messages);
    assert!(
        final_message.contains("approval required for Bash")
            || final_message.contains("approve")
            || final_message.contains("reject"),
        "pending-approval smoke stalled at final summary: missing explicit approve/reject guidance; final={final_message:?}"
    );
    assert!(
        !final_message.contains("Verification passed")
            && !final_message.contains("task completed")
            && !final_message.contains("I finished"),
        "pending-approval smoke should not falsely claim success; final={final_message:?}"
    );
}

#[tokio::test]
async fn cli_smoke_coding_loop_surfaces_bash_denial_with_clear_next_step() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let target = workspace.path().join("smoke_denial_target.txt");
    let marker = workspace.path().join("should_not_exist_after_denial.txt");
    std::fs::write(&target, "status = \"todo\"\n").expect("seed denial smoke target");

    let target_display = target.to_string_lossy().to_string();
    let marker_display = marker.to_string_lossy().to_string();
    let read_input = serde_json::json!({
        "file_path": target_display,
    })
    .to_string();
    let bash_input = serde_json::json!({
        "command": format!(
            "grep -n 'status = \"todo\"' {} && printf 'approved\\n' > {}",
            target.display(),
            marker.display()
        ),
        "timeout": 5_000
    })
    .to_string();

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace.path()));
    permission_context.add_always_allow_rule("Edit");
    permission_context.add_always_ask_rule("Bash");

    let mut engine = QueryEngine::new(coding_smoke_context_with_permissions(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Inspecting the target file before verification.".into()),
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
                StreamEvent::TextDelta(
                    "I need approval before running the verification command.".into(),
                ),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: bash_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
        ],
        permission_context,
    ));

    let pending_result = engine
        .submit_turn(Message::user(
            "Read the local file and run the local verification command, but ask for approval first if needed.",
        ))
        .await;

    assert_eq!(
        pending_result.state,
        QueryLoopState::Interrupted,
        "denial smoke should first stop in pending approval"
    );
    assert_eq!(
        pending_result.terminal,
        Terminal::AbortedTools,
        "denial smoke should first stop at the approval barrier"
    );
    assert_tool_started(&pending_result.events, "Read");
    assert_tool_started(&pending_result.events, "Bash");
    assert!(
        pending_result.events.iter().any(|event| matches!(
            event,
            EngineEvent::PendingApproval { tool_name, .. } if tool_name == "Bash"
        )),
        "denial smoke must first reach a real pending approval event"
    );

    let denial = engine
        .context
        .app_state
        .resolve_pending_approval(false)
        .await
        .expect("denial should resolve");

    let CommandResult::Message(denial_message) = denial else {
        panic!("expected message result after denying pending approval");
    };
    assert!(
        denial_message.contains("Denied")
            || denial_message.contains("rejected")
            || denial_message.contains("declined"),
        "denial smoke should explicitly say the command was denied; message={denial_message:?}"
    );
    assert!(
        denial_message.contains("modify")
            || denial_message.contains("safer")
            || denial_message.contains("alternative")
            || denial_message.contains("instruction")
            || denial_message.contains("next"),
        "denial smoke should give a clear next step after rejection; message={denial_message:?}"
    );
    assert!(
        !denial_message.contains("Verification passed")
            && !denial_message.contains("task completed")
            && !denial_message.contains("I finished"),
        "denial smoke should not falsely claim success after rejection; message={denial_message:?}"
    );

    assert!(
        !marker.exists(),
        "denial smoke should not execute the pending Bash command after user rejection: marker={marker_display}"
    );
    assert!(
        engine
            .context
            .app_state
            .permission_context
            .pending_approval()
            .is_none(),
        "denial smoke should clear the pending approval after rejection"
    );
}

#[tokio::test]
async fn cli_smoke_coding_loop_resumes_after_bash_approval_and_completes() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let target = workspace.path().join("smoke_approval_resume_target.txt");
    let marker = workspace.path().join("approved_execution_marker.txt");
    std::fs::write(&target, "status = \"todo\"\n").expect("seed approval-resume smoke target");

    let target_display = target.to_string_lossy().to_string();
    let read_input = serde_json::json!({
        "file_path": target_display,
    })
    .to_string();
    let bash_input = serde_json::json!({
        "command": format!(
            "grep -n 'status = \"todo\"' {} && printf 'approved\\n' > {} && printf 'verification ok\\n'",
            target.display(),
            marker.display()
        ),
        "timeout": 5_000
    })
    .to_string();

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace.path()));
    permission_context.add_always_allow_rule("Edit");
    permission_context.add_always_ask_rule("Bash");

    let mut engine = QueryEngine::new(coding_smoke_context_with_permissions(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Inspecting the target file before verification.".into()),
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
                StreamEvent::TextDelta(
                    "I need approval before running the verification command.".into(),
                ),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: bash_input.clone(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
        ],
        permission_context,
    ));

    let pending_result = engine
        .submit_turn(Message::user(
            "Read the local file and run the verification command, but pause for approval if Bash needs it.",
        ))
        .await;

    assert_eq!(
        pending_result.state,
        QueryLoopState::Interrupted,
        "approval-resume smoke should first stop in pending approval"
    );
    assert_eq!(
        pending_result.terminal,
        Terminal::AbortedTools,
        "approval-resume smoke should first stop at the approval barrier"
    );
    assert!(
        pending_result.events.iter().any(|event| matches!(
            event,
            EngineEvent::PendingApproval { tool_name, .. } if tool_name == "Bash"
        )),
        "approval-resume smoke must first reach a real pending approval event"
    );
    assert!(
        engine
            .context
            .app_state
            .permission_context
            .pending_approval()
            .is_some(),
        "approval-resume smoke should leave a pending approval before approval replay"
    );
    assert!(
        !marker.exists(),
        "approval-resume smoke should not execute the pending Bash command before approval"
    );

    let approved = engine
        .context
        .app_state
        .resolve_pending_approval(true)
        .await
        .expect("approval should resolve");

    let CommandResult::Message(approved_message) = approved else {
        panic!("expected message result after approving pending Bash");
    };
    assert!(
        approved_message.contains("exit_code: 0"),
        "approval-resume smoke should surface a successful Bash execution after approval; message={approved_message:?}"
    );
    assert!(
        approved_message.contains("stdout:\n")
            || approved_message.contains("verification ok")
            || approved_message.contains("status = \"todo\""),
        "approval-resume smoke should preserve verification output after approval; message={approved_message:?}"
    );
    assert!(
        engine
            .context
            .app_state
            .permission_context
            .pending_approval()
            .is_none(),
        "approval-resume smoke should clear the pending approval after approval replay"
    );
    assert!(
        marker.exists(),
        "approval-resume smoke should execute the original pending Bash command after approval"
    );

    let marker_contents = std::fs::read_to_string(&marker).expect("read approval marker");
    assert_eq!(
        marker_contents, "approved\n",
        "approval-resume smoke should run the approved Bash command exactly once"
    );
}

#[tokio::test]
async fn cli_smoke_pending_approval_user_facing_copy_is_actionable() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let target = workspace.path().join("smoke_user_copy_target.txt");
    std::fs::write(&target, "status = \"todo\"\n").expect("seed user-copy smoke target");

    let target_display = target.to_string_lossy().to_string();
    let read_input = serde_json::json!({
        "file_path": target_display,
    })
    .to_string();
    let bash_input = serde_json::json!({
        "command": format!("sudo grep -n 'status = \"todo\"' {}", target.display()),
        "timeout": 5_000
    })
    .to_string();

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace.path()));
    permission_context.add_always_allow_rule("Edit");
    permission_context.add_always_ask_rule("Bash");

    let mut engine = QueryEngine::new(coding_smoke_context_with_permissions(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Inspecting the target file before verification.".into()),
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
                StreamEvent::TextDelta(
                    "I need approval before running the verification command.".into(),
                ),
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: bash_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
        ],
        permission_context,
    ));

    let result = engine
        .submit_turn(Message::user(
            "Read the local file and run the verification command, but stop for approval if needed.",
        ))
        .await;

    assert_eq!(
        result.state,
        QueryLoopState::Interrupted,
        "user-facing approval copy smoke should stop in pending approval"
    );
    assert_eq!(
        result.terminal,
        Terminal::AbortedTools,
        "user-facing approval copy smoke should stop at the approval barrier"
    );

    let final_message = final_assistant_message_text(&result.messages).to_lowercase();
    assert!(
        final_message.contains("approval required for bash"),
        "user-facing approval copy should clearly identify Bash approval, not a generic interruption; final={final_message:?}"
    );
    assert!(
        final_message.contains("reason:")
            || final_message.contains("bash_warning")
            || final_message.contains("privileged")
            || final_message.contains("warning"),
        "user-facing approval copy should include a reason or warning context; final={final_message:?}"
    );
    assert!(
        final_message.contains("approve")
            || final_message.contains("deny")
            || final_message.contains("next_step:"),
        "user-facing approval copy should tell the user what action to take next; final={final_message:?}"
    );
    assert!(
        !final_message.contains("verification passed")
            && !final_message.contains("task completed")
            && !final_message.contains("exit_code:")
            && !final_message.contains("failed to"),
        "user-facing approval copy should not look like a success summary or a generic runtime error; final={final_message:?}"
    );
}

#[tokio::test]
async fn cli_smoke_resume_continuation_restores_interrupted_coding_context_without_false_completion()
 {
    let workspace = tempfile::tempdir().expect("tempdir");
    let marker = workspace.path().join("resume_continuation_marker.txt");

    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(workspace.path()));
    permission_context.add_always_allow_rule("Edit");
    permission_context.add_always_ask_rule("Bash");

    let mut engine = QueryEngine::new(coding_smoke_context_with_permissions(
        vec![vec![
            StreamEvent::MessageStart,
            StreamEvent::TextDelta(
                "I need approval before I can continue the interrupted verification.".into(),
            ),
            StreamEvent::ToolUse {
                tool_name: "Bash".into(),
                input: serde_json::json!({
                    "command": format!("printf 'should-not-run\\n' > {}", marker.display()),
                    "timeout": 5_000
                })
                .to_string(),
            },
            StreamEvent::MessageStop {
                stop_reason: StopReason::ToolUse,
            },
        ]],
        permission_context,
    ));

    let pending_result = engine
        .submit_turn(Message::user(
            "Continue the interrupted local verification, but stop if Bash needs approval.",
        ))
        .await;

    assert_eq!(
        pending_result.state,
        QueryLoopState::Interrupted,
        "resume-continuation smoke should first stop at a real interruption boundary"
    );
    assert_eq!(
        pending_result.terminal,
        Terminal::AbortedTools,
        "resume-continuation smoke should stop at the approval barrier instead of pretending to finish"
    );
    let pending_final = final_assistant_message_text(&pending_result.messages);
    assert!(
        !pending_final.to_ascii_lowercase().contains("completed")
            && !pending_final
                .to_ascii_lowercase()
                .contains("verification passed")
            && !pending_final.to_ascii_lowercase().contains("finished"),
        "resume-continuation smoke should not falsely claim completion before restore; final={pending_final:?}"
    );
    assert!(
        !marker.exists(),
        "resume-continuation smoke should not execute the interrupted Bash command before restore"
    );

    let task_manager = engine
        .context
        .app_state
        .permission_context
        .task_manager
        .as_ref()
        .expect("coding smoke context should attach a task manager")
        .clone();
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let interrupted_task = task_manager.create(
        "continue interrupted local verification",
        "cli-smoke-coding-session",
        InteractionSurface::Cli,
    );
    task_manager.launch(&interrupted_task.id, "work", std::future::pending::<()>());
    assert!(
        task_manager.kill(
            &interrupted_task.id,
            "cli-smoke-coding-session",
            &dispatcher
        ),
        "resume-continuation smoke fixture should produce a stopped task"
    );

    let mut restored_app_state = engine.context.app_state.clone();
    let resolved = resolved_from_snapshot(
        SessionSnapshot {
            session_id: SessionId("cli-smoke-coding-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: workspace.path().display().to_string(),
            last_turn_at: Some("2026-05-13T11:30:00Z".into()),
            prompt_seed: None,
        },
        SessionHistory {
            entries: vec![SessionHistoryEntry {
                message: Message::user(
                    "Continue the interrupted local verification, but stop if Bash needs approval.",
                ),
                timestamp: Some("2026-05-13T11:29:00Z".into()),
                tool_refs: vec!["Bash".into()],
                milestone: None,
            }],
        },
        true,
        None,
        Vec::new(),
        Vec::new(),
    );
    restored_app_state.apply_resolved_session_state(&resolved);

    let resume_result = ResumeCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/resume"),
            &restored_app_state,
        )
        .await
        .expect("resume command should render restored interrupted continuation");
    let resume_text = resume_result
        .to_plain_text()
        .expect("resume command should produce plain text");

    let tasks_result = TasksCommand
        .execute(
            &NormalizedInput::from_raw(InteractionSurface::Cli, "/tasks"),
            &restored_app_state,
        )
        .await
        .expect("tasks command should render restored interrupted continuation");
    let tasks_text = tasks_result
        .to_plain_text()
        .expect("tasks command should produce plain text");

    assert!(
        resume_text.contains("last task: continue interrupted local verification"),
        "resume-continuation smoke should restore a concrete continuation target instead of a generic session-only summary; text={resume_text}"
    );
    assert!(
        tasks_text.contains("Stopped tasks:")
            && tasks_text.contains("continue interrupted local verification")
            && tasks_text.contains("status: stopped"),
        "resume-continuation smoke should keep the interrupted coding target visible in stopped tasks after restore; text={tasks_text}"
    );
    assert!(
        !tasks_text.contains("Failed tasks:\n- [")
            || !tasks_text.contains("continue interrupted local verification"),
        "resume-continuation smoke should not misclassify interrupted work as failed after restore; text={tasks_text}"
    );
}

#[tokio::test]
async fn v1_release_gate_coding_smoke_bundle_stays_green() {
    let success_workspace = tempfile::tempdir().expect("tempdir");
    let success_target = success_workspace
        .path()
        .join("release_gate_success_target.txt");
    std::fs::write(&success_target, "status = \"todo\"\n")
        .expect("seed release gate success target");
    let success_read_input = serde_json::json!({
        "file_path": success_target.to_string_lossy().to_string(),
    })
    .to_string();
    let success_edit_input = serde_json::json!({
        "file_path": success_target.to_string_lossy().to_string(),
        "old_string": "status = \"todo\"",
        "new_string": "status = \"done\""
    })
    .to_string();
    let success_bash_input = serde_json::json!({
        "command": format!("grep -n 'status = \"done\"' {}", success_target.display()),
        "timeout": 5_000
    })
    .to_string();
    let mut success_engine = QueryEngine::new(coding_smoke_context(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "Read".into(),
                    input: success_read_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "Edit".into(),
                    input: success_edit_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: success_bash_input,
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("Updated the file and verification passed.".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ],
        ],
        success_workspace.path(),
    ));
    let success_result = success_engine
        .submit_turn(Message::user(
            "Inspect the file, edit it, run verification, and conclude clearly.",
        ))
        .await;
    assert_eq!(
        success_result.state,
        QueryLoopState::Completed,
        "release gate smoke: straight-line coding loop should complete"
    );
    assert_tool_started(&success_result.events, "Read");
    assert_tool_started(&success_result.events, "Edit");
    assert_tool_started(&success_result.events, "Bash");
    assert!(
        final_assistant_message_text(&success_result.messages).contains("verification passed"),
        "release gate smoke: straight-line path should conclude with a concrete success summary"
    );

    let approval_workspace = tempfile::tempdir().expect("tempdir");
    let approval_target = approval_workspace
        .path()
        .join("release_gate_approval_target.txt");
    let approval_marker = approval_workspace
        .path()
        .join("release_gate_approval_marker.txt");
    std::fs::write(&approval_target, "status = \"todo\"\n")
        .expect("seed release gate approval target");
    let approval_permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_active_session_id("cli-smoke-coding-session")
        .with_active_surface(InteractionSurface::Cli)
        .with_notification_dispatcher(NotificationDispatcher::new(TelegramGateway::default()))
        .with_filesystem_policy(allow_write_policy_for(approval_workspace.path()));
    approval_permission_context.add_always_allow_rule("Edit");
    approval_permission_context.add_always_ask_rule("Bash");
    let mut approval_engine = QueryEngine::new(coding_smoke_context_with_permissions(
        vec![
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "Read".into(),
                    input: serde_json::json!({
                        "file_path": approval_target.to_string_lossy().to_string(),
                    })
                    .to_string(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
            vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: serde_json::json!({
                        "command": format!(
                            "grep -n 'status = \"todo\"' {} && printf 'approved\\n' > {}",
                            approval_target.display(),
                            approval_marker.display()
                        ),
                        "timeout": 5_000
                    })
                    .to_string(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ],
        ],
        approval_permission_context,
    ));
    let approval_pending = approval_engine
        .submit_turn(Message::user(
            "Read the file and run verification, but pause for approval if Bash needs it.",
        ))
        .await;
    assert_eq!(
        approval_pending.state,
        QueryLoopState::Interrupted,
        "release gate smoke: approval path should stop at a real approval barrier"
    );
    assert!(
        approval_pending.events.iter().any(|event| matches!(
            event,
            EngineEvent::PendingApproval { tool_name, .. } if tool_name == "Bash"
        )),
        "release gate smoke: approval path should emit PendingApproval for Bash"
    );
    let pending_copy = final_assistant_message_text(&approval_pending.messages).to_lowercase();
    assert!(
        pending_copy.contains("approval required for bash")
            && (pending_copy.contains("approve") || pending_copy.contains("deny")),
        "release gate smoke: approval barrier should produce actionable user-facing approval copy; final={pending_copy:?}"
    );
    assert!(
        !approval_marker.exists(),
        "release gate smoke: pending approval should not execute Bash before approval"
    );

    let approved = approval_engine
        .context
        .app_state
        .resolve_pending_approval(true)
        .await
        .expect("approval should resolve");
    let CommandResult::Message(approved_message) = approved else {
        panic!("expected message result after approving bundled smoke gate");
    };
    assert!(
        approved_message.contains("exit_code: 0"),
        "release gate smoke: approved Bash should run and report a successful command result; message={approved_message:?}"
    );
    assert!(
        approval_marker.exists(),
        "release gate smoke: approved Bash should execute exactly after approval replay"
    );
}

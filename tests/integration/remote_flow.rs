use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::command::registry::CommandRegistry;
use rust_agent::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{
    InMemorySessionStore, SessionHistory, SessionRestoreRequest, SessionSnapshot, SessionStore,
};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::envelope::NormalizedInput;
use rust_agent::interaction::notification::{Notification, NotificationTarget};
use rust_agent::interaction::remote::{
    RemoteEventPayload, RemoteRequest, drain_remote_notifications, handle_remote_request,
};
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::plan::manager::PlanManager;
use rust_agent::security::audit::{AuditEvent, AuditLog};
use rust_agent::security::authorizer::{DefaultSurfaceAuthorizer, SurfaceAdmissionPolicy};
use rust_agent::service::api::client::ModelProviderClient;
use rust_agent::service::api::streaming::{StopReason, StreamEvent};
use rust_agent::service::compact::reactive_compact::ReactiveCompactor;
use rust_agent::state::active_model_runtime::{ActiveModelRuntime, ActiveModelRuntimeSnapshot};
use rust_agent::state::app_state::{AppState, RuntimeRole};
use rust_agent::state::permission_context::{PermissionMode, ToolPermissionContext};
use rust_agent::task::manager::TaskManager;
use rust_agent::tool::builtin::bash::BashTool;
use rust_agent::tool::registry::ToolRegistry;
use tokio::sync::RwLock;

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

struct RemoteSpawnTaskCommand;

#[async_trait]
impl Command for RemoteSpawnTaskCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "remote-spawn-task".into(),
            description: "Spawn a remote-owned task for integration tests".into(),
            source: CommandSource::Builtin,
            category: "test".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::RemoteSafe,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let tasks = app_state
            .permission_context
            .task_manager
            .as_ref()
            .expect("task manager should exist");
        let dispatcher = app_state.notification_dispatcher.clone();
        let task = tasks.create(
            "remote async task",
            app_state.active_session_id.clone(),
            InteractionSurface::Remote,
        );
        tasks.complete(&task.id, &dispatcher);
        Ok(CommandResult::Message(format!("spawned {}", task.id)))
    }
}

#[tokio::test]
async fn remote_request_prefers_active_model_runtime_client_for_bound_turns() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let runtime_client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::MessageStart,
        StreamEvent::TextDelta("runtime handle reply".into()),
        StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
        },
    ]]);
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default()))
            .with_plan_manager(Arc::new(PlanManager::default())),
        command_registry: Some(command_registry),
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
        active_model_runtime: Some(ActiveModelRuntime::new(ActiveModelRuntimeSnapshot {
            config: rust_agent::service::api::client::ModelProviderConfig::default(),
            client: runtime_client.clone(),
            active_profile_name: Some("remote-runtime".into()),
            source: rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
            summary: rust_agent::state::app_state::ActiveModelProviderSummary {
                provider_id: "runtime-provider".into(),
                protocol: "OpenAICompatible".into(),
                compatibility_profile: "OpenAICompatible".into(),
                base_url_host: "runtime.example".into(),
                model: "runtime-model".into(),
                auth_status: "env:RUNTIME_KEY(set)".into(),
            },
        })),
        active_model_profile_name: Some("remote-runtime".into()),
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "runtime-provider".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "runtime.example".into(),
            model: "runtime-model".into(),
            auth_status: "env:RUNTIME_KEY(set)".into(),
        },
        active_session_id: "remote-runtime-session".into(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("stale engine reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-runtime-bound".into(),
            actor_id: "remote-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "summarize remote chain".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.primary_text.contains("runtime handle reply"));
    assert!(!response.primary_text.contains("stale engine reply"));
}

#[tokio::test]
async fn remote_request_runs_minimal_query_chain() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-e2e-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-e2e".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
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
        active_session_id: "remote-e2e-session".into(),
        session_store: Some(session_store.clone()),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("remote integration reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-bound-session".into(),
            actor_id: "remote-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "summarize remote chain".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.primary_text.contains("remote integration reply"));
    assert!(
        response
            .events
            .iter()
            .any(|event| event.event_type == "assistant_delta")
    );
    assert!(
        response
            .events
            .iter()
            .any(|event| event.event_type == "session_milestone")
    );

    let (_, default_history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-e2e-session".into()),
            continue_session: false,
        })
        .expect("default app-state session should still exist");
    assert!(
        default_history.entries.is_empty(),
        "query persistence should not fall back to app_state.active_session_id"
    );

    let (remote_snapshot, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-bound-session".into()),
            continue_session: false,
        })
        .expect("remote request session should persist");
    assert_eq!(remote_snapshot.session_id.0, "remote-bound-session");
    assert_eq!(remote_snapshot.surface, InteractionSurface::Remote);
    assert_eq!(remote_snapshot.session_mode, SessionMode::Interactive);
    assert_eq!(history.entries.len(), 2);
    assert_eq!(
        history.entries[0].message,
        rust_agent::core::message::Message::user("summarize remote chain")
    );
    assert_eq!(
        history.entries[1].message,
        rust_agent::core::message::Message::assistant("remote integration reply")
    );
}

#[tokio::test]
async fn remote_request_uses_shared_session_apply_contract() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-shared-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-shared".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("bootstrap-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            cwd: "/tmp/bootstrap".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
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
        active_session_id: "bootstrap-session".into(),
        session_store: Some(session_store.clone()),
        session: Some(SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("bootstrap-session".into()),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Headless,
            cwd: "/tmp/bootstrap".into(),
            last_turn_at: None,
            prompt_seed: None,
        }),
        history: Some(SessionHistory::default()),
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("shared contract reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-shared-session".into(),
            actor_id: "actor-a".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "hello shared contract".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.primary_text.contains("shared contract reply"));
    let (snapshot, history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("remote-shared-session".into()),
            continue_session: false,
        })
        .expect("shared remote session should persist");
    assert_eq!(snapshot.surface, InteractionSurface::Remote);
    assert_eq!(snapshot.session_mode, SessionMode::Interactive);
    assert_eq!(snapshot.cwd, "/tmp/remote-shared");
    assert_eq!(history.entries.len(), 2);

    let (bootstrap_snapshot, bootstrap_history) = session_store
        .load(&SessionRestoreRequest {
            resume: Some("bootstrap-session".into()),
            continue_session: false,
        })
        .expect("bootstrap session should stay untouched");
    assert_eq!(bootstrap_snapshot.surface, InteractionSurface::Cli);
    assert_eq!(bootstrap_snapshot.session_mode, SessionMode::Headless);
    assert!(bootstrap_history.entries.is_empty());
}

#[tokio::test]
async fn remote_request_records_accept_and_notification_audit_events() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    let audit_root = unique_temp_path("remote-audit");
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::file_backed(
        audit_root.clone(),
    )));
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: audit_log.clone(),
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
        active_session_id: "remote-audit-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new().register(Arc::new(BashTool)),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::ToolUse {
                    tool_name: "Bash".into(),
                    input: serde_json::json!({
                        "command": "ls",
                        "dangerously_disable_sandbox": true
                    })
                    .to_string(),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-audit-session".into(),
            actor_id: "audit-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "hello audit".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(
        response
            .primary_text
            .contains("approval required for Bash:")
    );
    assert!(response.events.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::ApprovalRequired {
            tool_name,
            message,
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        } if tool_name == "Bash"
            && message == "command requests disabling sandbox protections"
            && code.as_deref() == Some("sandbox_disable")
            && summary.as_deref() == Some("Bash pending approval")
            && detail.as_deref() == Some("command requests disabling sandbox protections")
            && approval_kind.as_deref() == Some("tool_permission")
            && escalation_reasons.as_slice() == ["sandbox_disable"]
    )));

    let drained =
        drain_remote_notifications(&app_state, "remote-audit-session", Some("audit-actor"));
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::ApprovalRequired {
            tool_name,
            message,
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        } if tool_name == "Bash"
            && message == "command requests disabling sandbox protections"
            && code.as_deref() == Some("sandbox_disable")
            && summary.as_deref() == Some("Bash pending approval")
            && detail.as_deref() == Some("command requests disabling sandbox protections")
            && approval_kind.as_deref() == Some("tool_permission")
            && escalation_reasons.as_slice() == ["sandbox_disable"]
    )));

    let events = audit_log
        .lock()
        .expect("audit log poisoned")
        .events()
        .to_vec();
    assert!(events.iter().any(|event| matches!(
        event,
        AuditEvent::RemoteRequestAccepted {
            session_id,
            actor_id,
            from_trusted_surface,
        } if session_id == "remote-audit-session"
            && actor_id == "audit-actor"
            && *from_trusted_surface
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AuditEvent::RemoteNotificationQueued {
            session_id,
            actor_id,
            notification_kind,
            channel,
            request_id,
        } if session_id == "remote-audit-session"
            && actor_id.as_deref() == Some("audit-actor")
            && notification_kind == "approval_required"
            && channel == "async_inbox"
            && request_id == "remote-audit-session"
    )));
    let records = audit_log.lock().expect("audit log poisoned").load_records();
    assert!(records.iter().any(|record| {
        record.event_kind == "remote_request_accepted"
            && record.session_id.as_deref() == Some("remote-audit-session")
            && record.actor_id.as_deref() == Some("audit-actor")
            && record.surface.as_deref() == Some("remote")
            && record.outcome == "accepted"
    }));
    assert!(records.iter().any(|record| {
        record.event_kind == "remote_notification_queued"
            && record.session_id.as_deref() == Some("remote-audit-session")
            && record.actor_id.as_deref() == Some("audit-actor")
            && record.surface.as_deref() == Some("remote")
            && record.request_id.as_deref() == Some("remote-audit-session")
            && record.notification_kind.as_deref() == Some("approval_required")
            && record.channel.as_deref() == Some("async_inbox")
            && record.outcome == "queued"
    }));
    assert!(records.iter().any(|record| {
        record.event_kind == "remote_notification_dispatched"
            && record.session_id.as_deref() == Some("remote-audit-session")
            && record.actor_id.as_deref() == Some("audit-actor")
            && record.surface.as_deref() == Some("remote")
            && record.request_id.as_deref() == Some("remote-audit-session")
            && record.notification_kind.as_deref() == Some("approval_required")
            && record.channel.as_deref() == Some("async_inbox")
            && record.outcome == "dispatched"
    }));
    let _ = fs::remove_dir_all(audit_root);
}

#[tokio::test]
async fn remote_request_denies_not_allowlisted_and_records_audit_event() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_remote_surface_admission_policy(SurfaceAdmissionPolicy {
            allowlisted_actors: ["approved-actor".to_string()].into_iter().collect(),
            max_requests_per_window: None,
            window_seconds: 60,
            abuse_denial_threshold: None,
        });
    let session_store = Arc::new(InMemorySessionStore::default());
    let audit_root = unique_temp_path("remote-audit-denied");
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::file_backed(
        audit_root.clone(),
    )));
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: audit_log.clone(),
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
        active_session_id: "remote-audit-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(Vec::new()),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-audit-session".into(),
            actor_id: "audit-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "hello audit".into(),
        },
    )
    .await
    .expect("remote denial should return response");

    assert_eq!(
        response.primary_text,
        "Denied: actor is not allowlisted for remote surface"
    );
    assert!(response.events.is_empty());

    let events = audit_log
        .lock()
        .expect("audit log poisoned")
        .events()
        .to_vec();
    assert!(events.iter().any(|event| matches!(
        event,
        AuditEvent::RemoteRequestDenied {
            session_id,
            actor_id,
            reason,
            outcome,
        } if session_id == "remote-audit-session"
            && actor_id == "audit-actor"
            && outcome == "not_allowlisted"
            && reason.starts_with("not_allowlisted:")
    )));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AuditEvent::RemoteRequestAccepted { .. }))
    );
    let records = audit_log.lock().expect("audit log poisoned").load_records();
    assert!(records.iter().any(|record| {
        record.event_kind == "remote_request_denied_not_allowlisted"
            && record.session_id.as_deref() == Some("remote-audit-session")
            && record.actor_id.as_deref() == Some("audit-actor")
            && record.surface.as_deref() == Some("remote")
            && record.outcome == "not_allowlisted"
    }));
    let _ = fs::remove_dir_all(audit_root);
}

#[tokio::test]
async fn remote_request_drains_async_remote_notifications() {
    let command_registry = Arc::new(CommandRegistry::new());
    let _router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
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
        active_session_id: "remote-async-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };

    let mut actor_notification = Notification::approval_required(
        "remote-async-session",
        "Bash",
        "requires explicit approval",
        Some("bash_warning".into()),
        Some("Bash pending approval".into()),
        Some("requires explicit approval".into()),
        Some("tool_permission".into()),
        vec!["privileged_system".into()],
    );
    actor_notification.target = Some(NotificationTarget::RemoteActor {
        session_id: "remote-async-session".into(),
        actor_id: "remote-actor".into(),
    });
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, actor_notification);
    app_state.notification_dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice(
            "remote-async-session",
            "tool",
            "background update",
            Some("api_stream_interrupted".into()),
            Some("RetryScheduled".into()),
            Some("api_stream_interrupted".into()),
            Some("anthropic".into()),
            Some(503),
            Some(true),
            Some(true),
        ),
    );

    let drained =
        drain_remote_notifications(&app_state, "remote-async-session", Some("remote-actor"));
    assert_eq!(drained.len(), 2);
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::ApprovalRequired {
            tool_name,
            message,
            code,
            summary,
            detail,
            approval_kind,
            escalation_reasons,
        }
            if tool_name == "Bash"
                && message == "requires explicit approval"
                && code.as_deref() == Some("bash_warning")
                && summary.as_deref() == Some("Bash pending approval")
                && detail.as_deref() == Some("requires explicit approval")
                && approval_kind.as_deref() == Some("tool_permission")
                && escalation_reasons.as_slice() == ["privileged_system"]
    )));
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::RuntimeNotice {
            kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        }
            if kind == "tool"
                && message == "background update"
                && code.as_deref() == Some("api_stream_interrupted")
                && runtime_kind.as_deref() == Some("RetryScheduled")
                && service_failure_code.as_deref() == Some("api_stream_interrupted")
                && provider_kind.as_deref() == Some("anthropic")
                && status_code == &Some(503)
                && retryable == &Some(true)
                && surface_visible == &Some(true)
    )));
    assert!(
        drain_remote_notifications(&app_state, "remote-async-session", Some("remote-actor"))
            .is_empty()
    );
}

#[tokio::test]
async fn remote_request_drains_async_task_update_notifications() {
    let command_registry =
        Arc::new(CommandRegistry::new().register(Arc::new(RemoteSpawnTaskCommand)));
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    let audit_root = unique_temp_path("remote-task-audit");
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::file_backed(
        audit_root.clone(),
    )));
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: audit_log.clone(),
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
        active_session_id: "remote-task-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-task-session".into(),
            actor_id: "task-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "/remote-spawn-task".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.primary_text.contains("spawned task-0"));
    assert!(response.events.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-0"
                && task.task_type == "generic"
                && task.status == "completed"
                && task.summary.contains("remote async task")
    )));

    let drained = drain_remote_notifications(&app_state, "remote-task-session", Some("task-actor"));
    assert_eq!(drained.len(), 1);
    assert!(matches!(
        &drained[0].payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-0"
                && task.task_type == "generic"
                && task.status == "completed"
                && task.summary.contains("remote async task")
    ));

    let records = audit_log.lock().expect("audit log poisoned").load_records();
    let queued: Vec<_> = records
        .iter()
        .filter(|record| record.event_kind == "remote_notification_queued")
        .collect();
    assert_eq!(queued.len(), 1);
    assert!(queued.iter().all(|record| {
        record.request_id.as_deref() == Some("remote-task-session")
            && record.notification_kind.as_deref() == Some("task_update")
            && record.channel.as_deref() == Some("async_inbox")
            && record.outcome == "queued"
    }));

    let dispatched: Vec<_> = records
        .iter()
        .filter(|record| record.event_kind == "remote_notification_dispatched")
        .collect();
    assert_eq!(dispatched.len(), 1);
    assert!(dispatched.iter().all(|record| {
        record.request_id.as_deref() == Some("remote-task-session")
            && record.notification_kind.as_deref() == Some("task_update")
            && record.channel.as_deref() == Some("async_inbox")
            && record.outcome == "dispatched"
    }));

    let _ = std::fs::remove_dir_all(audit_root);
}

#[tokio::test]
async fn remote_request_preserves_response_boundary_and_async_inbox_semantics() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()))
        .with_pending_approval(rust_agent::state::permission_context::PendingApproval {
            tool_name: "Bash".into(),
            tool_input: serde_json::json!({"command": "ls"}).to_string(),
            message: "requires explicit approval".into(),
            code: Some("bash_warning".into()),
            summary: Some("Bash pending approval".into()),
            detail: Some("requires explicit approval".into()),
            approval_kind: Some("tool_permission".into()),
            escalation_reasons: vec!["privileged_system".into()],
        });
    let session_store = Arc::new(InMemorySessionStore::default());
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
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
        active_session_id: "remote-boundary-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    app_state.notification_dispatcher.dispatch(
        InteractionSurface::Remote,
        Notification::runtime_notice(
            "remote-boundary-session",
            "tool",
            "background only",
            Some("api_stream_terminal".into()),
            Some("ModelError".into()),
            Some("api_stream_terminal".into()),
            Some("anthropic".into()),
            Some(400),
            Some(false),
            Some(true),
        ),
    );
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("boundary reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-boundary-session".into(),
            actor_id: "actor-a".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "hello boundary".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.events.iter().all(|event| {
        !matches!(
            &event.payload,
            RemoteEventPayload::RuntimeNotice {
                kind,
                message,
                code,
                runtime_kind,
                service_failure_code,
                provider_kind,
                status_code,
                retryable,
                surface_visible,
            }
                if kind == "tool"
                    && message == "background only"
                    && code.as_deref() == Some("api_stream_terminal")
                    && runtime_kind.as_deref() == Some("ModelError")
                    && service_failure_code.as_deref() == Some("api_stream_terminal")
                    && provider_kind.as_deref() == Some("anthropic")
                    && status_code == &Some(400)
                    && retryable == &Some(false)
                    && surface_visible == &Some(true)
        )
    }));

    let drained =
        drain_remote_notifications(&app_state, "remote-boundary-session", Some("actor-a"));
    assert!(!drained.is_empty());
    assert!(drained.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::RuntimeNotice {
            kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        }
            if kind == "tool"
                && message == "background only"
                && code.as_deref() == Some("api_stream_terminal")
                && runtime_kind.as_deref() == Some("ModelError")
                && service_failure_code.as_deref() == Some("api_stream_terminal")
                && provider_kind.as_deref() == Some("anthropic")
                && status_code == &Some(400)
                && retryable == &Some(false)
                && surface_visible == &Some(true)
    )));
    assert!(
        drain_remote_notifications(&app_state, "remote-boundary-session", Some("other-actor"))
            .is_empty()
    );
}

#[tokio::test]
async fn remote_request_dual_channel_events_appear_in_response_and_async_inbox() {
    let command_registry =
        Arc::new(CommandRegistry::new().register(Arc::new(RemoteSpawnTaskCommand)));
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
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
        active_session_id: "remote-dual-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-dual-session".into(),
            actor_id: "actor-a".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "/remote-spawn-task".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(response.events.iter().any(|event| matches!(
        &event.payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-0" && task.status == "completed"
    )));

    let drained = drain_remote_notifications(&app_state, "remote-dual-session", Some("actor-a"));
    assert_eq!(drained.len(), 1);
    assert!(matches!(
        &drained[0].payload,
        RemoteEventPayload::TaskUpdate(task)
            if task.task_id == "task-0" && task.status == "completed"
    ));
}

#[tokio::test]
async fn remote_request_returns_typed_remote_event_envelopes() {
    let command_registry = Arc::new(CommandRegistry::new());
    let router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    session_store.save(
        SessionSnapshot {
            session_id: rust_agent::history::session::SessionId("remote-event-session".into()),
            surface: InteractionSurface::Remote,
            session_mode: SessionMode::Interactive,
            cwd: "/tmp/remote-events".into(),
            last_turn_at: None,
            prompt_seed: None,
        },
        SessionHistory::default(),
    );
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
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
        active_session_id: "remote-event-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };
    let engine =
        rust_agent::core::engine::QueryEngine::new(rust_agent::core::context::QueryContext {
            app_state: app_state.clone(),
            tool_registry: ToolRegistry::new(),
            api_client: ModelProviderClient::with_scripted_turns(vec![vec![
                StreamEvent::MessageStart,
                StreamEvent::TextDelta("typed remote reply".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                },
            ]]),
            compactor: ReactiveCompactor,
            hook_registry: rust_agent::hook::registry::HookRegistry::default(),
            agent_id: None,
            system_prompt: "test system".into(),
            tools_prompt: "test tools".into(),
            context_prompt: "test context".into(),
        });

    let response = handle_remote_request(
        &router,
        &engine,
        &app_state,
        RemoteRequest {
            session_id: "remote-bound-events".into(),
            actor_id: "remote-actor".into(),
            is_authenticated: true,
            from_trusted_surface: true,
            raw: "typed remote chain".into(),
        },
    )
    .await
    .expect("remote request should succeed");

    assert!(
        response
            .events
            .iter()
            .any(|event| event.event_type == "assistant_delta")
    );
    assert!(response
        .events
        .iter()
        .any(|event| matches!(&event.payload, RemoteEventPayload::AssistantDelta { text } if text == "typed remote reply")));
    assert!(response
        .events
        .iter()
        .any(|event| matches!(&event.payload, RemoteEventPayload::SessionMilestone { kind } if kind == "assistant_message_committed")));
    assert!(
        response
            .events
            .iter()
            .all(|event| event.event_type
                != "task:task-0:remote task:inspect task output for task-0")
    );
}

#[tokio::test]
async fn surface_visible_false_notice_excluded_from_response_events() {
    use rust_agent::interaction::remote::remote_response_events_from_surface_items;
    use rust_agent::interaction::view::SurfaceItem;

    let items = vec![
        SurfaceItem::RuntimeNotice {
            kind: "info".into(),
            message: "visible notice".into(),
            code: None,
            runtime_kind: None,
            service_failure_code: None,
            provider_kind: None,
            status_code: None,
            retryable: None,
            surface_visible: Some(true),
        },
        SurfaceItem::RuntimeNotice {
            kind: "error".into(),
            message: "should not appear".into(),
            code: Some("hidden_notice".into()),
            runtime_kind: Some("HiddenRuntime".into()),
            service_failure_code: Some("hidden_notice".into()),
            provider_kind: Some("provider".into()),
            status_code: Some(500),
            retryable: Some(false),
            surface_visible: Some(false),
        },
    ];

    let events = remote_response_events_from_surface_items(&items);
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].payload,
        RemoteEventPayload::RuntimeNotice {
            kind,
            message,
            surface_visible,
            ..
        } if kind == "info"
            && message == "visible notice"
            && surface_visible == &Some(true)
    ));
}

#[tokio::test]
async fn drain_remote_notifications_skips_surface_invisible_notice_and_records_dispatched_audit_event()
 {
    let command_registry = Arc::new(CommandRegistry::new());
    let _router = rust_agent::interaction::router::CommandRouter::new(
        command_registry.clone(),
        Box::new(DefaultSurfaceAuthorizer::default()),
    );
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(Arc::new(TaskManager::default()))
        .with_plan_manager(Arc::new(PlanManager::default()));
    let session_store = Arc::new(InMemorySessionStore::default());
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::default()));
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: Some(command_registry),
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log: audit_log.clone(),
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
        active_session_id: "audit-lifecycle-session".into(),
        session_store: Some(session_store),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };

    let mut visible_notice = Notification::runtime_notice(
        "audit-lifecycle-session",
        "info",
        "visible notice",
        Some("visible_notice".into()),
        Some("VisibleRuntime".into()),
        Some("visible_notice".into()),
        Some("provider".into()),
        Some(200),
        Some(false),
        Some(true),
    );
    visible_notice.target = Some(NotificationTarget::RemoteActor {
        session_id: "audit-lifecycle-session".into(),
        actor_id: "test-actor".into(),
    });

    let mut hidden_notice = Notification::runtime_notice(
        "audit-lifecycle-session",
        "error",
        "hidden notice",
        Some("hidden_notice".into()),
        Some("HiddenRuntime".into()),
        Some("hidden_notice".into()),
        Some("provider".into()),
        Some(500),
        Some(false),
        Some(false),
    );
    hidden_notice.target = Some(NotificationTarget::RemoteActor {
        session_id: "audit-lifecycle-session".into(),
        actor_id: "test-actor".into(),
    });

    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, visible_notice);
    app_state
        .notification_dispatcher
        .dispatch(InteractionSurface::Remote, hidden_notice);

    let drained =
        drain_remote_notifications(&app_state, "audit-lifecycle-session", Some("test-actor"));
    assert_eq!(drained.len(), 1);
    assert!(matches!(
        &drained[0].payload,
        RemoteEventPayload::RuntimeNotice {
            kind,
            message,
            code,
            runtime_kind,
            service_failure_code,
            provider_kind,
            status_code,
            retryable,
            surface_visible,
        } if kind == "info"
            && message == "visible notice"
            && code.as_deref() == Some("visible_notice")
            && runtime_kind.as_deref() == Some("VisibleRuntime")
            && service_failure_code.as_deref() == Some("visible_notice")
            && provider_kind.as_deref() == Some("provider")
            && status_code == &Some(200)
            && retryable == &Some(false)
            && surface_visible == &Some(true)
    ));

    let records = audit_log.lock().expect("audit log poisoned").load_records();
    let dispatched: Vec<_> = records
        .iter()
        .filter(|record| record.event_kind == "remote_notification_dispatched")
        .collect();

    assert_eq!(
        dispatched.len(),
        2,
        "both drained notifications should be audited as dispatched"
    );
    assert!(dispatched.iter().all(|record| {
        record.request_id.as_deref() == Some("audit-lifecycle-session")
            && record.notification_kind.as_deref() == Some("runtime_notice")
            && record.channel.as_deref() == Some("async_inbox")
            && record.surface.as_deref() == Some("remote")
            && record.actor_id.as_deref() == Some("test-actor")
            && record.outcome == "dispatched"
    }));
}

// ── R3 remote actor persistence tests ────────────────────────────────────────

use rust_agent::interaction::remote_actor::{RemoteActorStatus, RemoteActorStore};

fn make_actor_app_state(
    audit_log: Arc<std::sync::Mutex<AuditLog>>,
    store: Arc<RemoteActorStore>,
) -> AppState {
    AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default()))
            .with_plan_manager(Arc::new(PlanManager::default())),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log,
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "test".into(),
            protocol: "test".into(),
            compatibility_profile: "test".into(),
            base_url_host: "test".into(),
            model: "test".into(),
            auth_status: "none".into(),
        },
        active_session_id: "actor-test-session".to_string(),
        session_store: Some(Arc::new(InMemorySessionStore::default())),
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: Some(store),
    }
}

#[test]
fn r3_remote_actor_record_created_on_first_request() {
    let store = Arc::new(RemoteActorStore::in_memory());
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::default()));
    let app_state = make_actor_app_state(audit_log, store.clone());

    let input = NormalizedInput::from_remote_raw(
        "sess-1".to_string(),
        "actor-1".to_string(),
        true,
        false,
        "hello".to_string(),
    );

    rust_agent::interaction::remote::upsert_remote_actor_for_test(&app_state, &input);

    let record = store.get("sess-1", "actor-1").expect("record must exist");
    assert_eq!(record.request_count, 1);
    assert_eq!(record.status, RemoteActorStatus::Active);
    assert!(record.is_authenticated);
    assert!(!record.from_trusted_surface);
}

#[test]
fn r3_remote_actor_record_resumed_on_second_request() {
    let store = Arc::new(RemoteActorStore::in_memory());
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::default()));
    let app_state = make_actor_app_state(audit_log, store.clone());

    let input = NormalizedInput::from_remote_raw(
        "sess-2".to_string(),
        "actor-2".to_string(),
        false,
        true,
        "first".to_string(),
    );
    rust_agent::interaction::remote::upsert_remote_actor_for_test(&app_state, &input);

    let input2 = NormalizedInput::from_remote_raw(
        "sess-2".to_string(),
        "actor-2".to_string(),
        false,
        true,
        "second".to_string(),
    );
    rust_agent::interaction::remote::upsert_remote_actor_for_test(&app_state, &input2);

    let record = store.get("sess-2", "actor-2").expect("record must exist");
    assert_eq!(record.request_count, 2);
    assert_eq!(record.status, RemoteActorStatus::Active);
}

#[test]
fn r3_remote_actor_lifecycle_audit_events_recorded() {
    let store = Arc::new(RemoteActorStore::in_memory());
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::default()));
    let app_state = make_actor_app_state(audit_log.clone(), store.clone());

    let input = NormalizedInput::from_remote_raw(
        "sess-3".to_string(),
        "actor-3".to_string(),
        true,
        false,
        "msg".to_string(),
    );
    rust_agent::interaction::remote::upsert_remote_actor_for_test(&app_state, &input);
    rust_agent::interaction::remote::upsert_remote_actor_for_test(&app_state, &input);

    let records = audit_log.lock().expect("poisoned").load_records();
    let created: Vec<_> = records
        .iter()
        .filter(|r| r.event_kind == "remote_actor_created")
        .collect();
    let resumed: Vec<_> = records
        .iter()
        .filter(|r| r.event_kind == "remote_actor_resumed")
        .collect();

    assert_eq!(created.len(), 1, "exactly one created event");
    assert_eq!(resumed.len(), 1, "exactly one resumed event");
    assert_eq!(created[0].actor_id.as_deref(), Some("actor-3"));
    assert_eq!(resumed[0].actor_id.as_deref(), Some("actor-3"));
    assert_eq!(created[0].outcome, "created");
    assert_eq!(resumed[0].outcome, "resumed");
}

#[test]
fn r3_remote_actor_store_file_backed_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();

    {
        let store = RemoteActorStore::file_backed(root.clone());
        let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::default()));
        let app_state = make_actor_app_state(audit_log, Arc::new(store));
        let input = NormalizedInput::from_remote_raw(
            "sess-4".to_string(),
            "actor-4".to_string(),
            true,
            true,
            "persist".to_string(),
        );
        rust_agent::interaction::remote::upsert_remote_actor_for_test(&app_state, &input);
    }

    // Reload from same path
    let store2 = RemoteActorStore::file_backed(root);
    let record = store2.get("sess-4", "actor-4").expect("record must survive reload");
    assert_eq!(record.request_count, 1);
    assert!(record.is_authenticated);
    assert!(record.from_trusted_surface);
}

#[test]
fn r3_actor_snapshot_returns_none_when_store_absent() {
    let audit_log = Arc::new(std::sync::Mutex::new(AuditLog::default()));
    let app_state = AppState {
        surface: InteractionSurface::Remote,
        session_mode: SessionMode::Interactive,
        client_type: ClientType::RemoteControl,
        session_source: SessionSource::RemoteControl,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(Arc::new(TaskManager::default()))
            .with_plan_manager(Arc::new(PlanManager::default())),
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(ToolRegistry::new()))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: NotificationDispatcher::new(TelegramGateway::default()),
        audit_log,
        startup_trace: Vec::new(),
        active_model_runtime: None,
        active_model_profile_name: None,
        active_model_profile_source:
            rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        active_model_provider_summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "t".into(),
            protocol: "t".into(),
            compatibility_profile: "t".into(),
            base_url_host: "t".into(),
            model: "t".into(),
            auth_status: "none".into(),
        },
        active_session_id: "no-store-session".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    };

    assert!(
        app_state.actor_snapshot("any-session", "any-actor").is_none(),
        "actor_snapshot must return None when remote_actor_store is absent"
    );
}

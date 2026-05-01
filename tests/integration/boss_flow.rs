use std::sync::Arc;

use rust_agent::bootstrap::model_profiles::parse_model_profiles_registry;
use rust_agent::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use rust_agent::core::boss::{
    B_CONTEXT_KEEP_CHARS, B_CONTEXT_TRIM_THRESHOLD, BossCoordinator, assemble_summarized_payload,
    load_plan, save_plan, trim_context_payload,
};
use rust_agent::core::boss_actor_runtime::{
    BossActorRegistry, DesignerARuntime, ExecutionFn, ExecutorBRuntime, SpecReviewFn,
};
use rust_agent::core::boss_context_brief::{
    BossContextBrief, BossContextStrategy, BossStateFrame, PermissionScopeView, RelevantFileHandle,
    TargetArtifact, assemble_brief_prompt,
};
use rust_agent::core::boss_runtime::BossRuntimeHost;
use rust_agent::core::boss_state::{
    BossActorRole, BossActorStatus, BossControlRequest, BossControlResponse, BossPlan,
    BossPlanStep, BossPlanStepStatus, BossStage, BossStopStage,
};
use rust_agent::core::boss_state::{CompressionStrategy, ContextMode};
use rust_agent::core::boss_test_readiness::BossTestRunOutcome;
use rust_agent::core::concurrency::{
    BossBudgetDecision, MemoryPressureLevel, evaluate_boss_budget,
};
use rust_agent::core::context::{SubagentConfig, WorkerLisMPolicy};
use rust_agent::core::lism_ab_sample::new_shared_ab_sink;
use rust_agent::core::prompt_budget::{
    BudgetDecision, PromptCacheCapability, ProviderProfile, evaluate_prompt_budget,
};
use rust_agent::core::prompt_cache_adapter::apply_cache_control;
use rust_agent::core::prompt_segment::{PromptAssembly, PromptSegment, PromptSegmentKind};
use rust_agent::cost::tracker::CostTracker;
use rust_agent::history::session::{
    InMemorySessionStore, SessionHistory, SessionHistoryEntry, SessionId, SessionSnapshot,
};
use rust_agent::interaction::dispatcher::NotificationDispatcher;
use rust_agent::interaction::telegram::gateway::TelegramGateway;
use rust_agent::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole, WorkerRole,
};
use rust_agent::state::permission_context::{
    BossActorPolicy, PermissionMode, ToolPermissionContext,
};
use rust_agent::task::manager::TaskManager;
use rust_agent::task::types::{TaskEvent, TaskOwner, TaskStatus, TaskType, TaskUsageSummary};
use rust_agent::tool::builtin::agent::AgentTool;
use rust_agent::tool::definition::{Tool, ToolCall};
use rust_agent::tool::registry::{ToolAssemblyContext, ToolRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

fn make_inherited_runtime_snapshot_with_scripted_turns(
    scripted_turns: Vec<Vec<rust_agent::service::api::streaming::StreamEvent>>,
) -> rust_agent::state::active_model_runtime::ActiveModelRuntimeSnapshot {
    rust_agent::state::active_model_runtime::ActiveModelRuntimeSnapshot {
        config: rust_agent::service::api::client::ModelProviderConfig {
            provider_id: "scripted".into(),
            protocol: rust_agent::service::api::client::ProviderProtocol::OpenAICompatible,
            compatibility_profile:
                rust_agent::service::api::client::ProviderCompatibilityProfileKind::OpenAICompatible,
            base_url: "http://localhost".into(),
            auth_strategy: rust_agent::service::api::client::ProviderAuthStrategy::NoAuth,
            api_key: None,
            api_key_env: None,
            chat_completions_path: "/v1/chat/completions".into(),
            model_id: "scripted-inherited".into(),
            timeout: rust_agent::service::api::client::ProviderTimeout {
                request_timeout_ms: 1_000,
                stream_timeout_ms: 1_000,
            },
            retry_policy: rust_agent::service::api::retry::RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 0,
                max_backoff_ms: 0,
            },
            pricing: rust_agent::service::api::client::ModelPricing::default(),
            proxy_url: None,
            no_proxy: None,
            ca_bundle_path: None,
            max_tokens_param: None,
            prompt_cache_key: None,
            prompt_cache_retention: None,
        },
        client: rust_agent::service::api::client::ModelProviderClient::with_scripted_turns(
            scripted_turns,
        ),
        active_profile_name: Some("inherited-fast".into()),
        source: ActiveModelProfileSource::ModelsToml,
        summary: ActiveModelProviderSummary {
            provider_id: "scripted".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "localhost".into(),
            model: "scripted-inherited".into(),
            auth_status: "test".into(),
        },
    }
}

fn make_step_model_registry_with_base_url(
    override_base_url: &str,
) -> rust_agent::bootstrap::model_profiles::ModelProfileRegistry {
    parse_model_profiles_registry(&format!(
        r#"
active = "default"

[profiles.default]
provider_id = "openai"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "https://default.example"
model = "default-model"
auth_strategy = "none"

[profiles.worker-override]
provider_id = "override-provider"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "{override_base_url}"
model = "override-model"
auth_strategy = "none"
"#
    ))
    .expect("registry should parse")
}

fn make_step_model_registry() -> rust_agent::bootstrap::model_profiles::ModelProfileRegistry {
    make_step_model_registry_with_base_url("https://override.example")
}

async fn run_minimal_openai_mock_server(listener: TcpListener) {
    let (mut stream, _) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"{\\\"state\\\":\\\"done\\\",\\\"decision\\\":\\\"done\\\"}\"}}]}\n\n",
        "data: [DONE]\n\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write scripted response");
    stream.flush().await.expect("flush scripted response");
}

async fn run_minimal_openai_mock_server_rejected(listener: TcpListener) {
    let (mut stream, _) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"{\\\"state\\\":\\\"rejected\\\",\\\"reason\\\":\\\"override-provider-rejected\\\"}\"}}]}\n\n",
        "data: [DONE]\n\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write scripted response");
    stream.flush().await.expect("flush scripted response");
}

async fn run_mock_server_with_json_content(listener: TcpListener, content_json: String) {
    let (mut stream, _) = listener
        .accept()
        .await
        .expect("accept mock provider request");
    let mut buffer = vec![0_u8; 16 * 1024];
    let _ = stream.read(&mut buffer).await.expect("read request");
    let escaped = content_json.replace('"', "\\\"");
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{escaped}\"}}}}]}}\n\ndata: [DONE]\n\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write scripted response");
    stream.flush().await.expect("flush scripted response");
}

async fn run_openai_write_tool_loop_mock_server(
    listener: TcpListener,
    request_bodies: Arc<std::sync::Mutex<Vec<String>>>,
    artifact_path: String,
    artifact_content: String,
) {
    for response_body in [
        format!(
            concat!(
                "data: {{\"id\":\"chatcmpl-tool\",\"object\":\"chat.completion.chunk\",\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call_write\",\"type\":\"function\",\"function\":{{\"name\":\"Write\",\"arguments\":\"{{\\\"file_path\\\":\\\"{}\\\",\\\"content\\\":\\\"{}\\\"}}\"}}}}]}},\"index\":0,\"finish_reason\":\"tool_calls\"}}],\"usage\":{{\"model\":\"test-model\",\"prompt_tokens\":64,\"completion_tokens\":12,\"total_tokens\":76}}}}\n\n",
                "data: [DONE]\n\n"
            ),
            artifact_path.replace('\\', "\\\\").replace('"', "\\\""),
            artifact_content.replace('\\', "\\\\").replace('"', "\\\""),
        ),
        concat!(
            "data: {\"id\":\"chatcmpl-final\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"artifact written\"},\"index\":0,\"finish_reason\":\"stop\"}],\"usage\":{\"model\":\"test-model\",\"prompt_tokens\":72,\"completion_tokens\":8,\"total_tokens\":80}}\n\n",
            "data: [DONE]\n\n"
        )
        .to_string(),
    ] {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut buffer = vec![0_u8; 32 * 1024];
        let bytes_read = stream.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
        request_bodies
            .lock()
            .expect("request bodies poisoned")
            .push(request);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        stream.flush().await.expect("flush response");
    }
}

async fn run_openai_text_only_mock_server(
    listener: TcpListener,
    request_bodies: Arc<std::sync::Mutex<Vec<String>>>,
    content: String,
) {
    let (mut stream, _) = listener.accept().await.expect("accept request");
    let mut buffer = vec![0_u8; 32 * 1024];
    let bytes_read = stream.read(&mut buffer).await.expect("read request");
    let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
    request_bodies
        .lock()
        .expect("request bodies poisoned")
        .push(request);
    let response_body = format!(
        concat!(
            "data: {{\"id\":\"chatcmpl-final\",\"object\":\"chat.completion.chunk\",\"choices\":[{{\"delta\":{{\"content\":\"{}\"}},\"index\":0,\"finish_reason\":\"stop\"}}],\"usage\":{{\"model\":\"test-model\",\"prompt_tokens\":48,\"completion_tokens\":6,\"total_tokens\":54}}}}\n\n",
            "data: [DONE]\n\n"
        ),
        content.replace('\\', "\\\\").replace('"', "\\\""),
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write response");
    stream.flush().await.expect("flush response");
}

fn make_openai_runtime_snapshot_for_base_url(
    base_url: &str,
    observability: rust_agent::service::observability::ServiceObservabilityTracker,
) -> rust_agent::state::active_model_runtime::ActiveModelRuntimeSnapshot {
    let config = rust_agent::service::api::client::ModelProviderConfig {
        provider_id: "openai-compatible".into(),
        protocol: rust_agent::service::api::client::ProviderProtocol::OpenAICompatible,
        compatibility_profile:
            rust_agent::service::api::client::ProviderCompatibilityProfileKind::OpenAICompatible,
        base_url: base_url.into(),
        chat_completions_path: "/v1/chat/completions".into(),
        auth_strategy: rust_agent::service::api::client::ProviderAuthStrategy::NoAuth,
        model_id: "test-model".into(),
        retry_policy: rust_agent::service::api::retry::RetryPolicy::default(),
        ..rust_agent::service::api::client::ModelProviderConfig::default()
    };
    rust_agent::state::active_model_runtime::ActiveModelRuntimeSnapshot {
        config: config.clone(),
        client:
            rust_agent::service::api::client::ModelProviderClient::from_config_with_observability(
                config,
                observability.clone(),
            ),
        active_profile_name: Some("test-openai-compatible".into()),
        source: rust_agent::state::app_state::ActiveModelProfileSource::ModelsToml,
        summary: rust_agent::state::app_state::ActiveModelProviderSummary {
            provider_id: "openai-compatible".into(),
            protocol: "OpenAICompatible".into(),
            compatibility_profile: "OpenAICompatible".into(),
            base_url_host: "localhost".into(),
            model: "test-model".into(),
            auth_status: "none".into(),
        },
    }
}

fn allow_write_policy_for(
    root: &std::path::Path,
) -> Arc<rust_agent::security::filesystem_policy::FilesystemPolicy> {
    std::fs::create_dir_all(root).expect("writable root should exist");
    let canonical_root = std::fs::canonicalize(root).expect("writable root should canonicalize");
    let mut rules = vec![
        rust_agent::security::filesystem_policy::FilesystemPolicyRule {
            path: root.to_string_lossy().to_string(),
            level: rust_agent::security::filesystem_policy::FilesystemPermissionLevel::Allow,
        },
    ];
    if canonical_root != root {
        rules.push(
            rust_agent::security::filesystem_policy::FilesystemPolicyRule {
                path: canonical_root.to_string_lossy().to_string(),
                level: rust_agent::security::filesystem_policy::FilesystemPermissionLevel::Allow,
            },
        );
    }
    Arc::new(
        rust_agent::security::filesystem_policy::FilesystemPolicy::from_config(
            rust_agent::security::filesystem_policy::FilesystemPolicyConfig {
                protected_paths: Vec::new(),
                rules,
            },
        )
        .expect("filesystem policy should parse"),
    )
}

fn app_state_with_boss_worker_runtime(
    active_session_id: &str,
    task_manager: Arc<TaskManager>,
    boss: Arc<BossCoordinator>,
    tool_registry: ToolRegistry,
    runtime_snapshot: rust_agent::state::active_model_runtime::ActiveModelRuntimeSnapshot,
    writable_root: &std::path::Path,
) -> Arc<AppState> {
    let mut app = (*app_state_with_tasks(active_session_id, task_manager)).clone();
    app.permission_context = app
        .permission_context
        .clone()
        .with_boss_coordinator(boss.clone())
        .with_inherited_tool_registry(tool_registry.clone())
        .with_inherited_active_model_snapshot(runtime_snapshot.clone())
        .with_filesystem_policy(allow_write_policy_for(writable_root));
    app.runtime_tool_registry = Some(Arc::new(RwLock::new(tool_registry)));
    app.active_model_runtime = Some(
        rust_agent::state::active_model_runtime::ActiveModelRuntime::new(runtime_snapshot.clone()),
    );
    app.active_model_profile_name = runtime_snapshot.active_profile_name.clone();
    app.active_model_profile_source = runtime_snapshot.source.clone();
    app.active_model_provider_summary = runtime_snapshot.summary.clone();
    app.boss_coordinator = Some(boss);
    Arc::new(app)
}

async fn seed_fake_a_review_session(
    coordinator: &Arc<BossCoordinator>,
    task_manager: Arc<TaskManager>,
    session_id: &str,
    response: &'static str,
) {
    let fake_a_task = task_manager.create_with_type(
        "fake designer A reviewer".to_string(),
        TaskType::LocalAgent,
        session_id.to_string(),
        InteractionSurface::Cli,
    );
    let aid = fake_a_task.id.clone();
    let tm_for_a = task_manager.clone();
    let aid_for_loop = aid.clone();
    task_manager.launch(&aid, "", async move {
        loop {
            let messages = tm_for_a.drain_mailbox(&aid_for_loop);
            for _ in messages {
                tm_for_a.append_output(&aid_for_loop, response);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_a_session_id_pub(&aid).await;
}

async fn wait_for_step_status(
    coordinator: &Arc<BossCoordinator>,
    step_id: usize,
    expected: BossPlanStepStatus,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status = {
            let guard = coordinator.plan.read().await;
            let plan = guard.as_ref().expect("plan must exist");
            plan.steps
                .iter()
                .find(|step| step.id == step_id)
                .expect("step must exist")
                .status
        };
        if status == expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for step {step_id} to become {expected:?}; latest={status:?}"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
}

async fn run_minimal_openai_mock_server_n(listener: TcpListener, n: usize) {
    let done_body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"{\\\"state\\\":\\\"done\\\",\\\"decision\\\":\\\"done\\\"}\"}}]}\n\n",
        "data: [DONE]\n\n"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        done_body.len(),
        done_body
    );
    for _ in 0..n {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("accept mock provider request");
        let mut buffer = vec![0_u8; 16 * 1024];
        let _ = stream.read(&mut buffer).await.expect("read request");
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write scripted response");
        stream.flush().await.expect("flush scripted response");
    }
}

fn make_orchestrator_route_override_plan(step_id: usize) -> BossPlan {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus};
    BossPlan {
        plan_id: format!("p-route-override-{step_id}"),
        task_description: "runtime resolution test".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![BossPlanStep {
            id: step_id,
            description: "worker override step".into(),
            objective: None,
            acceptance: vec![],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            review_task_id: None,
        }],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

/// Write a models.toml with a `worker-override` profile pointing to `base_url`
/// into `<dir>/.claude/models.toml`. Returns the dir path for cleanup.
fn write_worker_override_models_toml(dir: &std::path::Path, base_url: &str) {
    let claude_dir = dir.join(".claude");
    std::fs::create_dir_all(&claude_dir).unwrap();
    std::fs::write(
        claude_dir.join("models.toml"),
        format!(
            r#"active = "default"
[profiles.default]
provider_id = "test"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "{base_url}"
model = "test-model"
auth_strategy = "none"

[profiles.worker-override]
provider_id = "worker-override-provider"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "{base_url}"
model = "worker-override-model"
auth_strategy = "none"
"#
        ),
    )
    .unwrap();
}

fn write_two_profile_models_toml(
    dir: &std::path::Path,
    default_base_url: &str,
    worker_override_base_url: &str,
) {
    let claude_dir = dir.join(".claude");
    std::fs::create_dir_all(&claude_dir).unwrap();
    std::fs::write(
        claude_dir.join("models.toml"),
        format!(
            r#"active = "default"
[profiles.default]
provider_id = "test"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "{default_base_url}"
model = "default-model"
auth_strategy = "none"

[profiles.worker-override]
provider_id = "worker-override-provider"
protocol = "openai_compatible"
compatibility_profile = "openai_compatible"
base_url = "{worker_override_base_url}"
model = "worker-override-model"
auth_strategy = "none"
"#
        ),
    )
    .unwrap();
}

fn boss_step(id: usize, description: &str) -> BossPlanStep {
    BossPlanStep {
        id,
        description: description.into(),
        objective: Some(format!("objective {id}")),
        acceptance: vec![format!("acceptance {id}")],
        requires_approval: false,
        status: BossPlanStepStatus::Pending,
        completed: false,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 0,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    }
}

fn boss_plan(steps: Vec<BossPlanStep>) -> BossPlan {
    BossPlan {
        plan_id: "plan-alpha".into(),
        task_description: "Multi-step task".into(),
        steps,
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

fn app_state(active_session_id: &str) -> Arc<AppState> {
    app_state_with_tasks(active_session_id, Arc::new(TaskManager::default()))
}

fn app_state_with_tasks(active_session_id: &str, task_manager: Arc<TaskManager>) -> Arc<AppState> {
    let permission_context = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager)
        .with_active_session_id(active_session_id)
        .with_active_surface(InteractionSurface::Cli);
    Arc::new(AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
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
        active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
        active_model_provider_summary: ActiveModelProviderSummary {
            provider_id: "default-provider".into(),
            protocol: "Anthropic".into(),
            compatibility_profile: "Anthropic".into(),
            base_url_host: "localhost".into(),
            model: "default-model".into(),
            auth_status: "env:OPENAI_API_KEY(unset)".into(),
        },
        active_session_id: active_session_id.into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    })
}

fn task_event(task_id: &str, step_id: usize, status: TaskStatus) -> TaskEvent {
    TaskEvent {
        task_id: task_id.into(),
        task_type: TaskType::LocalAgent,
        status,
        step_id: Some(step_id),
        owner: TaskOwner {
            session_id: "test-session".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some(task_id.into()),
        summary: format!("{task_id} summary"),
        result: format!("{task_id} result"),
        next_action: "None".into(),
        worker_role: Some(WorkerRole::Implement),
        orchestration_group_id: None,
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    }
}

fn app_state_with_history(
    active_session_id: &str,
    task_manager: Arc<TaskManager>,
    session_store: Arc<InMemorySessionStore>,
    history: SessionHistory,
) -> Arc<AppState> {
    let mut app = (*app_state_with_tasks(active_session_id, task_manager)).clone();
    app.session_store = Some(session_store);
    app.session = Some(SessionSnapshot {
        session_id: SessionId(active_session_id.into()),
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        cwd: "/tmp".into(),
        last_turn_at: None,
        prompt_seed: None,
    });
    app.history = Some(history);
    Arc::new(app)
}

async fn coordinator_with_plan(
    plan: BossPlan,
    file_name: &str,
) -> (Arc<BossCoordinator>, std::path::PathBuf) {
    let plan_path = std::env::temp_dir().join(file_name);
    save_plan(&plan, &plan_path).await.unwrap();
    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = Arc::new(
        BossCoordinator::restore_or_init_with_owner(&plan_path, owner)
            .await
            .unwrap(),
    );
    (coordinator, plan_path)
}

#[tokio::test]
async fn report_interrupt_includes_active_children_and_attempt_review_summary() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Long running step")]),
        "test_boss_report_interrupt.json",
    )
    .await;

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some("task-b".into());
        snapshot.executor_b.status = BossActorStatus::Active;
        snapshot
            .active_children
            .push(rust_agent::core::boss_state::BossActorHandle {
                actor_id: "boss-plan-alpha-child-1".into(),
                session_id: "boss-plan-alpha-child-1".into(),
                role: BossActorRole::ImplementChild,
                status: BossActorStatus::Active,
                task_id: Some("task-child".into()),
                last_snapshot: None,
                lineage_depth: 1,
                mailbox_id: None,
                cancel_id: None,
                last_assignment_fingerprint: None,
                last_assignment_plan_version: None,
                last_assignment_step_revision: None,
            });
    }

    let task = task_manager.create_with_type(
        "Spawned implement worker for Long running step",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_worker_role(&task.id, WorkerRole::Implement);
    task_manager.set_boss_actor_id(&task.id, Some("executor_b:depth=0".into()));
    task_manager.start(&task.id);

    {
        let mut plan = coordinator.plan.write().await;
        let plan = plan.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Reviewing;
        plan.steps[0].worker_task_id = Some(task.id.clone());
        plan.steps[0].attempt_count = 2;
        plan.steps[0].last_review_summary = Some("A review: tighten edge-case handling".into());
    }

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert!(matches!(
        report.stage,
        BossStage::Execution | BossStage::Documentation
    ));
    assert_eq!(report.executor_b.status, BossActorStatus::Active);
    assert_eq!(report.active_children.len(), 1);
    assert_eq!(
        report.active_children[0].role,
        BossActorRole::ImplementChild
    );
    assert_eq!(report.steps.len(), 1);
    assert_eq!(report.steps[0].attempt_count, 2);
    assert_eq!(
        report.steps[0].last_review_summary.as_deref(),
        Some("A review: tighten edge-case handling")
    );
    assert_eq!(
        report.steps[0].worker_task_id.as_deref(),
        Some(task.id.as_str())
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn report_control_request_does_not_require_query_loop_return() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Waiting step")]),
        "test_boss_report_control_request.json",
    )
    .await;

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();

    match response {
        BossControlResponse::Report(payload) => {
            assert_eq!(payload.total_steps, Some(1));
            assert_eq!(payload.steps.len(), 1);
        }
        other => panic!("expected report payload, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn stop_interrupt_returns_typed_stop_outcome_and_kills_tasks() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Force-drain step")]),
        "test_boss_stop_interrupt.json",
    )
    .await;

    let b_task = task_manager.create_with_type(
        "executor b",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_boss_actor_id(&b_task.id, Some("executor_b:depth=0".into()));
    task_manager.start(&b_task.id);

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some(b_task.id.clone());
        snapshot.executor_b.status = BossActorStatus::Active;
    }

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Stop(outcome) => {
            assert_eq!(
                outcome.stages,
                vec![
                    BossStopStage::CancelIssued,
                    BossStopStage::DeadlineExpired,
                    BossStopStage::ForceDrain,
                ]
            );
            assert!(outcome.killed_task_ids.contains(&b_task.id));
        }
        other => panic!("expected stop outcome, got {other:?}"),
    }
    assert_eq!(task_manager.status(&b_task.id), Some(TaskStatus::Killed));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn stop_interrupt_immediate_cancel_only_reports_cancel_issued() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Immediate cancel step")]),
        "test_boss_stop_immediate_cancel.json",
    )
    .await;

    let b_task = task_manager.create_with_type(
        "executor b",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_boss_actor_id(&b_task.id, Some("executor_b:depth=0".into()));
    task_manager.launch(&b_task.id, "executor b running", async {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    });

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some(b_task.id.clone());
        snapshot.executor_b.status = BossActorStatus::Active;
    }

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Stop(outcome) => {
            assert_eq!(outcome.stages, vec![BossStopStage::CancelIssued]);
            assert!(!outcome.stages.contains(&BossStopStage::DeadlineExpired));
            assert!(!outcome.stages.contains(&BossStopStage::ForceDrain));
        }
        other => panic!("expected stop outcome, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn stop_interrupt_records_deadline_without_force_drain_when_task_finishes_in_time() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Deadline-only stop step")]),
        "test_boss_stop_deadline_no_force.json",
    )
    .await;

    let b_task = task_manager.create_with_type(
        "executor b",
        TaskType::LocalAgent,
        "test-session",
        InteractionSurface::Cli,
    );
    task_manager.set_boss_actor_id(&b_task.id, Some("executor_b:depth=0".into()));
    task_manager.start(&b_task.id);

    {
        let mut session = coordinator.session.write().await;
        let snapshot = session.as_mut().unwrap();
        snapshot.executor_b.task_id = Some(b_task.id.clone());
        snapshot.executor_b.status = BossActorStatus::Active;
    }

    let task_manager_for_finish = task_manager.clone();
    let dispatcher_for_finish = dispatcher.clone();
    let b_task_id = b_task.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        task_manager_for_finish.complete(&b_task_id, &dispatcher_for_finish);
    });

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session".into(),
                deadline_ms: 20,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Stop(outcome) => {
            assert_eq!(
                outcome.stages,
                vec![BossStopStage::CancelIssued, BossStopStage::DeadlineExpired]
            );
            assert!(!outcome.stages.contains(&BossStopStage::ForceDrain));
        }
        other => panic!("expected stop outcome, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn report_payload_uses_historystore_derived_summary() {
    let task_manager = Arc::new(TaskManager::default());
    let store = Arc::new(InMemorySessionStore::default());
    let history = SessionHistory {
        entries: vec![
            SessionHistoryEntry {
                message: rust_agent::core::message::Message::user("first user note"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            },
            SessionHistoryEntry {
                message: rust_agent::core::message::Message::assistant("second assistant summary"),
                timestamp: None,
                tool_refs: Vec::new(),
                milestone: None,
            },
        ],
    };
    let app_state = app_state_with_history("history-session", task_manager.clone(), store, history);
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "History-backed step")]),
        "test_boss_historystore_report.json",
    )
    .await;
    coordinator
        .attach_app_state_for_report_testing(app_state)
        .await;

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Report,
            &task_manager,
            &NotificationDispatcher::new(TelegramGateway::default()),
        )
        .await
        .unwrap();

    match response {
        BossControlResponse::Report(payload) => {
            assert_eq!(payload.history_summary.len(), 2);
            assert_eq!(payload.history_summary[0], "second assistant summary");
            assert_eq!(payload.history_summary[1], "first user note");
        }
        other => panic!("expected report payload, got {other:?}"),
    }

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn report_control_request_uses_dedicated_mailbox_runtime() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Mailbox report step")]),
        "test_boss_report_mailbox_runtime.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    assert!(coordinator.has_control_runtime().await);

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    assert!(matches!(response, BossControlResponse::Report(_)));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn control_mailbox_runtime_remains_available_after_rebind() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Mailbox rebind step")]),
        "test_boss_mailbox_rebind.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    assert!(coordinator.has_control_runtime().await);

    coordinator.rebind_control_runtime().await;
    assert!(coordinator.has_control_runtime().await);

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    assert!(matches!(response, BossControlResponse::Report(_)));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn coordinators_with_same_plan_id_do_not_collide_in_runtime_registry() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let plan = boss_plan(vec![boss_step(0, "Same plan id step")]);
    let (coordinator_a, path_a) =
        coordinator_with_plan(plan.clone(), "test_boss_same_plan_a.json").await;
    let (coordinator_b, path_b) = coordinator_with_plan(plan, "test_boss_same_plan_b.json").await;

    coordinator_a.ensure_control_runtime().await;
    coordinator_b.ensure_control_runtime().await;

    let key_a = coordinator_a.current_runtime_key().await.unwrap();
    let key_b = coordinator_b.current_runtime_key().await.unwrap();
    assert_ne!(
        key_a, key_b,
        "same plan_id coordinators must have distinct runtime keys"
    );

    let response_a = coordinator_a
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    let response_b = coordinator_b
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await
        .unwrap();
    assert!(matches!(response_a, BossControlResponse::Report(_)));
    assert!(matches!(response_b, BossControlResponse::Report(_)));

    let _ = std::fs::remove_file(path_a);
    let _ = std::fs::remove_file(path_b);
}

#[tokio::test]
async fn old_runtime_is_shutdown_and_unavailable_after_rebind() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Shutdown old runtime step")]),
        "test_boss_old_runtime_shutdown.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    let old_key = coordinator.current_runtime_key().await.unwrap();

    coordinator.rebind_control_runtime().await;
    let new_key = coordinator.current_runtime_key().await.unwrap();
    assert_ne!(old_key, new_key);
    assert!(
        coordinator.runtime_is_closed_for_testing(&old_key).await,
        "old runtime must be explicitly shut down"
    );
    assert!(coordinator.has_control_runtime().await);
    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response.is_ok(),
        "new runtime must accept requests after rebind"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn runtime_owner_shutdown_makes_runtime_unaddressable() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Owner shutdown step")]),
        "test_boss_runtime_owner_shutdown.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    let runtime_key = coordinator.current_runtime_key().await.unwrap();
    assert!(coordinator.has_control_runtime().await);

    coordinator.shutdown_runtime_owner();

    assert!(
        coordinator
            .runtime_is_closed_for_testing(&runtime_key)
            .await
    );
    assert!(!coordinator.has_control_runtime().await);
    assert!(
        coordinator
            .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
            .await
            .is_err(),
        "owner shutdown must block fresh runtime bootstrap"
    );
    coordinator.restart_runtime_owner();

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn shutdown_all_runtimes_allows_fresh_bootstrap_after_cleanup() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Owner cleanup step")]),
        "test_boss_runtime_cleanup.json",
    )
    .await;

    coordinator.ensure_control_runtime().await;
    let runtime_key = coordinator.current_runtime_key().await.unwrap();
    coordinator.shutdown_all_runtime_instances();

    assert!(
        coordinator
            .runtime_is_closed_for_testing(&runtime_key)
            .await
    );
    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response.is_ok(),
        "cleanup-only shutdown must allow fresh bootstrap"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn shutdown_owner_does_not_block_fresh_coordinator_with_fresh_owner() {
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let (closed_coordinator, closed_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Closed owner step")]),
        "test_boss_closed_owner_isolation.json",
    )
    .await;
    closed_coordinator.ensure_control_runtime().await;
    closed_coordinator.shutdown_runtime_owner();
    assert!(
        closed_coordinator
            .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
            .await
            .is_err()
    );

    let (fresh_coordinator, fresh_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Fresh owner step")]),
        "test_boss_fresh_owner_isolation.json",
    )
    .await;
    let response = fresh_coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response.is_ok(),
        "fresh owner must remain usable after another owner shuts down"
    );

    let _ = std::fs::remove_file(closed_path);
    let _ = std::fs::remove_file(fresh_path);
}

#[tokio::test]
async fn boss_auto_advances_to_next_step_after_completion() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                worker_task_id: Some("worker-task-0".into()),
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
        ]),
        "test_boss_flow_auto_advance.json",
    )
    .await;

    assert_eq!(coordinator.get_stage().await, BossStage::Execution);
    let payload = coordinator
        .advance_plan(&app_state("parent-session-1"))
        .await
        .unwrap()
        .expect("next step should dispatch");

    assert!(payload.contains("\"boss_plan_id\":\"plan-alpha\""));
    assert!(payload.contains("\"step_id\":1"));
    assert!(payload.contains("\"step_objective\":\"objective 1\""));
    assert!(payload.contains("\"step_acceptance\":[\"acceptance 1\"]"));
    assert!(payload.contains("\"parent_session_id\":\"parent-session-1\""));

    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[1];
    assert_eq!(step.status, BossPlanStepStatus::Running);
    assert_eq!(coordinator.status.read().await.current_step, Some(1));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_stops_before_approval_barrier() {
    let mut approval_step = boss_step(1, "Approval-gated step");
    approval_step.requires_approval = true;
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(0, "Step 1")
            },
            approval_step,
        ]),
        "test_boss_flow_approval_stop.json",
    )
    .await;

    let outcome = coordinator
        .advance_plan(&app_state("parent-session-2"))
        .await
        .unwrap()
        .expect("approval barrier should be reported");

    assert!(outcome.contains("paused before step 1"));
    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[1];
    assert_eq!(step.status, BossPlanStepStatus::WaitingForApproval);
    assert!(step.worker_task_id.is_none());

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_stops_after_step_failure() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step 1"), boss_step(1, "Step 2")]),
        "test_boss_flow_failure_stop.json",
    )
    .await;

    coordinator
        .on_task_event(&task_event("worker-task-failed", 0, TaskStatus::Failed))
        .await
        .unwrap();
    let outcome = coordinator
        .advance_plan(&app_state("parent-session-3"))
        .await
        .unwrap()
        .expect("failure should be reported");

    assert!(outcome.contains("terminal step failure"));
    let plan = coordinator.plan.read().await;
    assert_eq!(
        plan.as_ref().unwrap().steps[0].status,
        BossPlanStepStatus::Failed
    );
    assert_eq!(
        plan.as_ref().unwrap().steps[1].status,
        BossPlanStepStatus::Pending
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_advance_plan_actually_spawns_worker() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-dispatch", task_manager.clone());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
        ]),
        "test_boss_flow_real_dispatch.json",
    )
    .await;

    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("worker dispatch payload should be returned");

    assert!(payload.contains("\"step_id\":1"));
    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_type, TaskType::LocalAgent);
    assert_eq!(tasks[0].worker_role, Some(WorkerRole::Implement));
    assert_eq!(tasks[0].step_id, Some(1));
    assert_eq!(tasks[0].owner.session_id, "parent-session-dispatch");
    assert!(matches!(
        tasks[0].status,
        TaskStatus::Running | TaskStatus::Completed
    ));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn concurrent_worker_updates_do_not_cross_step_boundaries() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step 1"), boss_step(1, "Step 2")]),
        "test_boss_flow_concurrent_isolation.json",
    )
    .await;

    let left = coordinator.clone();
    let right = coordinator.clone();
    let left_event = task_event("worker-task-left", 0, TaskStatus::Completed);
    let right_event = task_event("worker-task-right", 1, TaskStatus::Completed);

    let (left_result, right_result) = tokio::join!(
        async move { left.on_task_event(&left_event).await },
        async move { right.on_task_event(&right_event).await }
    );
    left_result.unwrap();
    right_result.unwrap();

    let plan = coordinator.plan.read().await;
    let steps = &plan.as_ref().unwrap().steps;
    assert!(steps[0].completed);
    assert!(steps[1].completed);
    assert_eq!(steps[0].worker_task_id.as_deref(), Some("worker-task-left"));
    assert_eq!(
        steps[1].worker_task_id.as_deref(),
        Some("worker-task-right")
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_step_complete_auto_dispatches_next() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-auto-chain", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                status: BossPlanStepStatus::Running,
                worker_task_id: Some("worker-task-step0".into()),
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
        ]),
        "test_boss_flow_auto_chain.json",
    )
    .await;

    // Seed the auto-advance app_state by calling advance_plan once.
    // With step 0 Running, advance_plan returns None (already running) but stores app_state.
    let _ = coordinator.advance_plan(&app_state).await.unwrap();

    // Fire the completion event for step 0 — should auto-trigger advance_plan for step 1.
    coordinator
        .on_task_event(&task_event("worker-task-step0", 0, TaskStatus::Completed))
        .await
        .unwrap();

    let plan = coordinator.plan.read().await;
    let steps = &plan.as_ref().unwrap().steps;
    assert_eq!(steps[0].status, BossPlanStepStatus::Completed);
    assert!(steps[0].completed);
    assert_eq!(steps[1].status, BossPlanStepStatus::Running);
    drop(plan);

    let tasks = task_manager.list();
    assert_eq!(
        tasks.len(),
        1,
        "one worker should have been spawned for step 1"
    );
    assert_eq!(tasks[0].step_id, Some(1));
    assert_eq!(tasks[0].owner.session_id, "parent-session-auto-chain");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_starts_two_global_agents_and_restores_handles() {
    let plan = BossPlan {
        plan_id: "restore-test".into(),
        task_description: "restore test".into(),
        steps: vec![boss_step(0, "step 0")],
        accepted_by_user: true,
        auto_sequence: false,
        ..Default::default()
    };

    let dir = std::env::temp_dir().join("boss_restore_handles_test");
    std::fs::create_dir_all(&dir).unwrap();
    let plan_path = dir.join("planning.json");
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path)
        .await
        .expect("restore should succeed");

    let session_guard = coordinator.session.read().await;
    let session = session_guard
        .as_ref()
        .expect("session should be populated after restore");

    assert_eq!(session.plan_id, "restore-test");
    assert_eq!(session.designer_a.actor_id, "boss-restore-test-a");
    assert_eq!(session.executor_b.actor_id, "boss-restore-test-b");
    assert_eq!(session.designer_a.role, BossActorRole::DesignerA);
    assert_eq!(session.executor_b.role, BossActorRole::ExecutorB);
    assert_eq!(session.designer_a.status, BossActorStatus::Pending);
    assert_eq!(session.executor_b.status, BossActorStatus::Pending);
    assert!(session.active_children.is_empty());

    let _ = std::fs::remove_file(&plan_path);
    let _ = std::fs::remove_dir(dir);
}

#[tokio::test]
async fn boss_actor_registry_tracks_a_b_and_children() {
    let coordinator = BossCoordinator::new();

    let empty = coordinator.actor_registry_snapshot().await;
    assert!(empty.is_empty(), "no session means empty registry");

    coordinator
        .ensure_actor_session("plan-beta", BossStage::Execution)
        .await;

    let snapshot = coordinator.actor_registry_snapshot().await;
    assert_eq!(snapshot.len(), 2, "A and B should be present");
    assert!(snapshot.iter().any(|h| h.role == BossActorRole::DesignerA));
    assert!(snapshot.iter().any(|h| h.role == BossActorRole::ExecutorB));

    // Idempotent: same plan_id must not duplicate handles.
    coordinator
        .ensure_actor_session("plan-beta", BossStage::Execution)
        .await;
    let snapshot2 = coordinator.actor_registry_snapshot().await;
    assert_eq!(snapshot2.len(), 2);

    coordinator
        .update_actor_status("boss-plan-beta-a", BossActorStatus::Active)
        .await;
    let snapshot3 = coordinator.actor_registry_snapshot().await;
    let a = snapshot3
        .iter()
        .find(|h| h.role == BossActorRole::DesignerA)
        .unwrap();
    assert_eq!(a.status, BossActorStatus::Active);
    let b = snapshot3
        .iter()
        .find(|h| h.role == BossActorRole::ExecutorB)
        .unwrap();
    assert_eq!(b.status, BossActorStatus::Pending);

    // Inject one of each child role and verify the registry distinguishes them.
    {
        use rust_agent::core::boss_state::BossActorHandle;
        let mut guard = coordinator.session.write().await;
        let session = guard.as_mut().unwrap();
        session.active_children.push(BossActorHandle::new(
            "child-review-1",
            "child-review-1",
            BossActorRole::ReviewChild,
        ));
        session.active_children.push(BossActorHandle::new(
            "child-impl-1",
            "child-impl-1",
            BossActorRole::ImplementChild,
        ));
        session.active_children.push(BossActorHandle::new(
            "child-verify-1",
            "child-verify-1",
            BossActorRole::VerifyChild,
        ));
    }

    let snapshot4 = coordinator.actor_registry_snapshot().await;
    assert_eq!(snapshot4.len(), 5, "A + B + 3 children");
    assert!(
        snapshot4
            .iter()
            .any(|h| h.role == BossActorRole::ReviewChild)
    );
    assert!(
        snapshot4
            .iter()
            .any(|h| h.role == BossActorRole::ImplementChild)
    );
    assert!(
        snapshot4
            .iter()
            .any(|h| h.role == BossActorRole::VerifyChild)
    );

    // All three child roles must report is_child() == true.
    let children: Vec<_> = snapshot4.iter().filter(|h| h.role.is_child()).collect();
    assert_eq!(children.len(), 3);
    assert!(children.iter().all(|h| h.role.is_child()));

    // A and B must NOT be classified as children.
    assert!(!BossActorRole::DesignerA.is_child());
    assert!(!BossActorRole::ExecutorB.is_child());
}

// --- T16.6.B: Boss-aware spawn policy ---

#[test]
fn boss_b_executor_b_context_is_boss_executor_b() {
    let ctx = ToolAssemblyContext::executor_b(InteractionSurface::Cli, SessionMode::Headless);
    assert!(
        ctx.is_boss_executor_b(),
        "executor_b context must report is_boss_executor_b"
    );
}

#[test]
fn boss_worker_context_is_not_boss_executor_b() {
    let ctx = ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless);
    assert!(
        !ctx.is_boss_executor_b(),
        "plain worker must not report is_boss_executor_b"
    );
}

#[test]
fn subagent_limiter_enforces_total_and_role_caps_under_memory_pressure() {
    let tasks = TaskManager::default();

    for index in 0..2 {
        let task = tasks.create_with_type(
            format!("research-{index}"),
            TaskType::LocalAgent,
            "boss-session",
            InteractionSurface::Cli,
        );
        tasks.set_worker_role(&task.id, WorkerRole::Research);
        tasks.set_boss_actor_id(&task.id, Some(format!("review_child:depth={index}")));
    }

    assert!(matches!(
        evaluate_boss_budget(&tasks, WorkerRole::Research, 1, MemoryPressureLevel::Normal),
        BossBudgetDecision::Queue { .. }
    ));

    for index in 0..4 {
        let task = tasks.create_with_type(
            format!("implement-{index}"),
            TaskType::LocalAgent,
            "boss-session",
            InteractionSurface::Cli,
        );
        tasks.set_worker_role(&task.id, WorkerRole::Implement);
        tasks.set_boss_actor_id(&task.id, Some(format!("implement_child:depth={index}")));
    }

    assert!(matches!(
        evaluate_boss_budget(
            &tasks,
            WorkerRole::Implement,
            1,
            MemoryPressureLevel::Normal
        ),
        BossBudgetDecision::Queue { .. }
    ));
}

#[tokio::test]
async fn boss_budget_blocks_low_priority_children_when_pressure_is_critical() {
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));

    let decision = evaluate_boss_budget(
        permissions.task_manager.as_ref().unwrap(),
        WorkerRole::Research,
        1,
        MemoryPressureLevel::Critical,
    );
    assert!(matches!(decision, BossBudgetDecision::Deny { .. }));

    let decision = evaluate_boss_budget(
        permissions.task_manager.as_ref().unwrap(),
        WorkerRole::Verify,
        1,
        MemoryPressureLevel::Critical,
    );
    assert!(matches!(decision, BossBudgetDecision::Queue { .. }));

    let decision = evaluate_boss_budget(
        permissions.task_manager.as_ref().unwrap(),
        WorkerRole::Implement,
        1,
        MemoryPressureLevel::Critical,
    );
    assert_eq!(decision, BossBudgetDecision::Allow);
}

#[tokio::test]
async fn boss_agent_spawn_gate_surfaces_budget_queue_reason() {
    let tasks = Arc::new(TaskManager::default());
    for index in 0..6 {
        let task = tasks.create_with_type(
            format!("active-boss-{index}"),
            TaskType::LocalAgent,
            "boss-session",
            InteractionSurface::Cli,
        );
        tasks.set_worker_role(&task.id, WorkerRole::Implement);
        tasks.set_boss_actor_id(&task.id, Some(format!("implement_child:depth={index}")));
    }

    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));

    let err = AgentTool
        .invoke(
            &ToolCall::new(
                "Agent",
                serde_json::json!({
                    "task": "implement overflow child",
                    "role": "implement"
                })
                .to_string(),
            ),
            &permissions,
        )
        .await
        .expect_err("budget gate must reject spawning beyond the boss active cap");

    assert!(
        err.to_string().contains("boss budget queued"),
        "budget gate should surface queue reason, got: {err}"
    );
}

#[test]
fn boss_spawn_policy_denies_out_of_phase_child_spawn() {
    // A policy with phase != Execution must not allow spawning.
    let policy = BossActorPolicy {
        actor_role: BossActorRole::ExecutorB,
        lineage_depth: 0,
        phase: BossStage::Documentation,
    };
    assert!(
        !policy.may_spawn(),
        "ExecutorB outside Execution phase must not be allowed to spawn"
    );
}

#[tokio::test]
async fn boss_child_cannot_spawn_grandchild_agent() {
    // Build a ToolPermissionContext that looks like a ReviewChild.
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy::child(
            BossActorRole::ReviewChild,
            1,
            BossStage::Execution,
        ));

    let call = ToolCall::new(
        "Agent",
        serde_json::json!({
            "prompt": "do something",
            "session_id": "child-session"
        })
        .to_string(),
    );

    let err = AgentTool
        .invoke(&call, &permissions)
        .await
        .expect_err("child actor must not be allowed to spawn a grandchild");

    assert!(
        err.to_string().contains("boss spawn policy"),
        "error must mention boss spawn policy, got: {err}"
    );
    assert!(
        err.to_string().contains("review_child"),
        "error must name the role, got: {err}"
    );
}

// --- T16.6.C.1: Persistent ExecutorB routing ---

#[tokio::test]
async fn execution_reuses_persistent_b_instead_of_fresh_worker_per_step() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-b-reuse", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            BossPlanStep {
                completed: true,
                status: BossPlanStepStatus::Completed,
                ..boss_step(0, "Step 1")
            },
            boss_step(1, "Step 2"),
            boss_step(2, "Step 3"),
        ]),
        "test_boss_flow_b_reuse.json",
    )
    .await;

    // Dispatch step 1 — spawns B fresh (no running B yet).
    let payload1 = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 1 should dispatch");

    assert!(
        payload1.contains("\"step_id\":1"),
        "spawn payload must carry step_id"
    );
    assert!(
        payload1.contains("\"reuse_strategy\":\"running_only\""),
        "spawn payload must use running_only reuse strategy"
    );

    let tasks_after_step1 = task_manager.list();
    assert_eq!(
        tasks_after_step1.len(),
        1,
        "exactly one B task spawned for step 1"
    );
    let b_task_id = tasks_after_step1[0].id.clone();

    // B's actor id is deterministically derived from the plan id.
    let v1: serde_json::Value = serde_json::from_str(&payload1).unwrap();
    let group_id = v1["orchestration_group_id"].as_str().unwrap_or("");
    assert!(
        group_id.contains("plan-alpha"),
        "orchestration_group_id must embed the plan id, got: {group_id}"
    );

    // Manually mark B's task as Running so the Continue path triggers for step 2.
    task_manager.start(&b_task_id);
    // Record B's task id in the session so find_running_b_task_id can find it.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some(b_task_id.clone());
        }
    }

    // Mark step 1 completed so advance_plan can move to step 2.
    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[1].completed = true;
        plan.steps[1].status = BossPlanStepStatus::Completed;
    }

    // Dispatch step 2 — B is running, so this must use Continue (no new task).
    let payload2 = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 2 should dispatch via continue");

    // Continue payload carries task_id, not reuse_strategy.
    let v2: serde_json::Value = serde_json::from_str(&payload2).unwrap();
    assert_eq!(
        v2["task_id"].as_str().unwrap_or(""),
        b_task_id,
        "continue payload must target the existing B task"
    );
    assert_eq!(v2["step_id"], 2, "continue payload must carry step_id 2");
    assert_eq!(v2["stale_brief_action"], "refresh");
    assert!(
        v2["reuse_strategy"].is_null(),
        "continue payload must NOT have reuse_strategy"
    );

    // Critically: still only one task in the manager — no new task was spawned.
    let tasks_after_step2 = task_manager.list();
    assert_eq!(
        tasks_after_step2.len(),
        1,
        "step 2 must reuse B's task via Continue — no new task should be created"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_advance_plan_uses_continue_payload_when_b_is_running() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-continue", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A"), boss_step(1, "Step B")]),
        "test_boss_flow_continue_path.json",
    )
    .await;

    // Dispatch step 0 — spawns B fresh.
    let _ = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 0 should dispatch");

    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1, "one B task after step 0");
    let b_task_id = tasks[0].id.clone();

    // Mark B as Running and record its id in the session.
    task_manager.start(&b_task_id);
    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some(b_task_id.clone());
        }
    }

    // Mark step 0 completed so advance_plan can move to step 1.
    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[0].completed = true;
        plan.steps[0].status = BossPlanStepStatus::Completed;
    }

    // Dispatch step 1 — B is running, must use Continue.
    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 1 should dispatch via continue");

    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        v["task_id"].as_str().unwrap_or(""),
        b_task_id,
        "continue payload must target the running B task"
    );
    assert_eq!(v["step_id"], 1, "continue payload must carry step_id 1");
    assert_eq!(v["boss_plan_id"], "plan-alpha");
    assert_eq!(v["step_objective"], "objective 1");
    assert_eq!(v["step_acceptance"][0], "acceptance 1");

    // No new task created.
    assert_eq!(
        task_manager.list().len(),
        1,
        "no new task — B was reused via Continue"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_b_receives_step_context_via_continue_or_mailbox() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A")]),
        "test_boss_flow_b_context.json",
    )
    .await;

    // build_step_spawn_payload must embed the step objective and acceptance criteria.
    let b_actor_id = format!("boss-{}-b", "plan-alpha");
    let payload = coordinator
        .build_step_spawn_payload(0, "parent-ctx-session", &b_actor_id)
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(v["step_id"], 0, "step_id must be embedded");
    assert_eq!(v["boss_plan_id"], "plan-alpha", "plan_id must be embedded");
    assert_eq!(
        v["step_objective"], "objective 0",
        "step objective must be embedded"
    );
    assert_eq!(
        v["step_acceptance"][0], "acceptance 0",
        "acceptance criteria must be embedded"
    );
    assert_eq!(
        v["parent_session_id"], "parent-ctx-session",
        "parent session id must be embedded"
    );
    assert_eq!(
        v["reuse_strategy"], "running_only",
        "reuse strategy must be running_only"
    );
    assert_eq!(
        v["orchestration_group_id"], b_actor_id,
        "orchestration_group_id must be B's actor id"
    );
    let task = v["task"].as_str().unwrap_or("");
    assert!(
        task.contains("open_items:"),
        "spawn prompt must carry open items to the worker"
    );
    assert!(
        task.contains("acceptance 0"),
        "spawn prompt must carry acceptance as open items"
    );
    assert!(
        task.contains("plan_version: plan-alpha:steps=1"),
        "spawn prompt must carry plan version"
    );
    assert!(
        task.contains("permission_scope:"),
        "spawn prompt must carry permission scope"
    );
    assert_eq!(
        v["allowed_tools"][0], "Read",
        "spawn payload must pass allowed tools to the worker runtime"
    );

    // build_step_continue_payload must embed step context and target the B task id.
    let continue_payload = coordinator
        .build_step_continue_payload(0, "b-task-42", "parent-ctx-session")
        .await
        .unwrap();

    let vc: serde_json::Value = serde_json::from_str(&continue_payload).unwrap();
    assert_eq!(
        vc["task_id"], "b-task-42",
        "continue payload must target B's task id"
    );
    assert_eq!(vc["step_id"], 0);
    assert_eq!(vc["boss_plan_id"], "plan-alpha");
    assert_eq!(vc["step_objective"], "objective 0");
    assert_eq!(vc["step_acceptance"][0], "acceptance 0");
    assert_eq!(vc["parent_session_id"], "parent-ctx-session");
    assert_eq!(vc["stale_brief_action"], "refresh");
    assert_eq!(vc["plan_version"], "plan-alpha:steps=1");
    assert_eq!(vc["step_revision"], "step-0-attempt-0");
    // Continue payload must NOT have reuse_strategy or task field.
    assert!(
        vc["reuse_strategy"].is_null(),
        "continue payload must not have reuse_strategy"
    );
    assert!(
        vc["task"].is_null(),
        "continue payload must not have task field"
    );
    assert!(
        vc["refresh_task"]
            .as_str()
            .unwrap_or("")
            .contains("permission_scope:"),
        "refresh continue payload must carry a replacement brief"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_spawn_payload_carries_recent_decisions_from_prior_steps() {
    let mut step0 = boss_step(0, "Step A");
    step0.status = BossPlanStepStatus::Completed;
    step0.last_review_summary = Some("keep the JSONL parsing flow".into());
    let mut step1 = boss_step(1, "Step B");
    step1.objective =
        Some("任务目标：\n- 目标文件：src/core/boss.rs\n- 调整 worker spawn payload".into());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![step0, step1]),
        "test_boss_flow_recent_decisions.json",
    )
    .await;

    let payload = coordinator
        .build_step_spawn_payload(1, "parent-ctx-session", "boss-plan-alpha-b")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    let task = v["task"].as_str().unwrap_or("");
    assert!(
        task.contains("recent_decisions:"),
        "spawn prompt must include recent decisions when prior step reviews exist"
    );
    assert!(
        task.contains("keep the JSONL parsing flow"),
        "spawn prompt must carry the prior review summary"
    );
    assert!(
        task.contains("relevant_file_handles:"),
        "spawn prompt must include typed file handles"
    );
    assert!(
        task.contains("path=src/core/boss.rs"),
        "typed file handles must include the referenced source path"
    );
    assert!(
        task.contains("target_files:"),
        "spawn prompt must include structured target files"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_continue_payload_reuses_brief_when_assignment_contract_is_unchanged() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A")]),
        "test_boss_flow_continue_reuse_assignment.json",
    )
    .await;

    let spawn_payload = coordinator
        .build_step_spawn_payload(0, "parent-ctx-session", "boss-plan-alpha-b")
        .await
        .unwrap();
    let spawn_json: serde_json::Value = serde_json::from_str(&spawn_payload).unwrap();
    {
        let mut session = coordinator.session.write().await;
        let session = session.as_mut().unwrap();
        session.executor_b.task_id = Some("b-task-42".into());
        session.executor_b.last_assignment_fingerprint = spawn_json["assignment_fingerprint"]
            .as_str()
            .map(str::to_string);
        session.executor_b.last_assignment_plan_version =
            spawn_json["plan_version"].as_str().map(str::to_string);
        session.executor_b.last_assignment_step_revision =
            spawn_json["step_revision"].as_str().map(str::to_string);
    }

    let continue_payload = coordinator
        .build_step_continue_payload(0, "b-task-42", "parent-ctx-session")
        .await
        .unwrap();
    let continue_json: serde_json::Value = serde_json::from_str(&continue_payload).unwrap();

    assert_eq!(continue_json["stale_brief_action"], "reuse");
    assert!(
        continue_json["refresh_task"].is_null(),
        "unchanged assignment must not resend a replacement brief"
    );
    assert!(
        continue_json["message"]
            .as_str()
            .unwrap_or("")
            .contains("Boss step 0"),
        "unchanged assignment should use the lightweight continue message"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_continue_payload_refreshes_brief_when_worker_permission_scope_changes() {
    use rust_agent::core::context::WorkerLisMPolicy;

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A")]),
        "test_boss_flow_continue_refresh_permission.json",
    )
    .await;

    let spawn_payload = coordinator
        .build_step_spawn_payload(0, "parent-ctx-session", "boss-plan-alpha-b")
        .await
        .unwrap();
    let spawn_json: serde_json::Value = serde_json::from_str(&spawn_payload).unwrap();
    {
        let mut session = coordinator.session.write().await;
        let session = session.as_mut().unwrap();
        session.executor_b.task_id = Some("b-task-42".into());
        session.executor_b.last_assignment_fingerprint = spawn_json["assignment_fingerprint"]
            .as_str()
            .map(str::to_string);
        session.executor_b.last_assignment_plan_version =
            spawn_json["plan_version"].as_str().map(str::to_string);
        session.executor_b.last_assignment_step_revision =
            spawn_json["step_revision"].as_str().map(str::to_string);
    }
    coordinator
        .set_worker_lism_policy(WorkerLisMPolicy::ForceOff)
        .await;

    let continue_payload = coordinator
        .build_step_continue_payload(0, "b-task-42", "parent-ctx-session")
        .await
        .unwrap();
    let continue_json: serde_json::Value = serde_json::from_str(&continue_payload).unwrap();

    assert_eq!(continue_json["stale_brief_action"], "refresh");
    assert!(
        continue_json["refresh_task"]
            .as_str()
            .unwrap_or("")
            .contains("lism_policy=force-off"),
        "permission scope drift must refresh the worker brief"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_continue_payload_refreshes_brief_when_step_objective_changes() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step A")]),
        "test_boss_flow_continue_refresh_objective.json",
    )
    .await;

    let spawn_payload = coordinator
        .build_step_spawn_payload(0, "parent-ctx-session", "boss-plan-alpha-b")
        .await
        .unwrap();
    let spawn_json: serde_json::Value = serde_json::from_str(&spawn_payload).unwrap();
    {
        let mut session = coordinator.session.write().await;
        let session = session.as_mut().unwrap();
        session.executor_b.task_id = Some("b-task-42".into());
        session.executor_b.last_assignment_fingerprint = spawn_json["assignment_fingerprint"]
            .as_str()
            .map(str::to_string);
        session.executor_b.last_assignment_plan_version =
            spawn_json["plan_version"].as_str().map(str::to_string);
        session.executor_b.last_assignment_step_revision =
            spawn_json["step_revision"].as_str().map(str::to_string);
    }
    {
        let mut plan = coordinator.plan.write().await;
        let plan = plan.as_mut().unwrap();
        plan.steps[0].objective = Some(
            "任务目标：\n- 目标文件：RustAgent/Agent/src/core/boss.rs\n- 重写 worker brief".into(),
        );
    }

    let continue_payload = coordinator
        .build_step_continue_payload(0, "b-task-42", "parent-ctx-session")
        .await
        .unwrap();
    let continue_json: serde_json::Value = serde_json::from_str(&continue_payload).unwrap();

    assert_eq!(continue_json["stale_brief_action"], "refresh");
    assert!(
        continue_json["refresh_task"]
            .as_str()
            .unwrap_or("")
            .contains("重写 worker brief"),
        "objective drift must regenerate the worker brief"
    );

    let _ = std::fs::remove_file(plan_path);
}

// --- T16.6.C.3: B child spawn contract + fan-in summary ---

#[test]
fn boss_b_spawns_children_with_child_policy_and_depth() {
    use rust_agent::state::permission_context::BossActorPolicy;

    // Simulate B (ExecutorB, depth=0) spawning a child with explicit role.
    let b_policy = BossActorPolicy::executor_b(BossStage::Execution);
    assert!(b_policy.may_spawn(), "ExecutorB must be allowed to spawn");

    // Child policy: implement_child at depth 1.
    let child_policy = BossActorPolicy {
        actor_role: BossActorRole::ImplementChild,
        lineage_depth: b_policy.lineage_depth + 1,
        phase: BossStage::Execution,
    };
    assert_eq!(child_policy.lineage_depth, 1, "child must be at depth 1");
    assert!(
        !child_policy.may_spawn(),
        "ImplementChild must not be allowed to spawn"
    );
    assert!(
        child_policy.actor_role.is_child(),
        "ImplementChild must be classified as child"
    );

    // Verify all three child roles are blocked from spawning.
    for role in [
        BossActorRole::ReviewChild,
        BossActorRole::ImplementChild,
        BossActorRole::VerifyChild,
    ] {
        let p = BossActorPolicy {
            actor_role: role,
            lineage_depth: 1,
            phase: BossStage::Execution,
        };
        assert!(
            !p.may_spawn(),
            "{} must not be allowed to spawn",
            role.as_str()
        );
    }

    // boss_actor_id recorded on task must encode role and depth.
    let boss_actor_id = format!(
        "{}:depth={}",
        child_policy.actor_role.as_str(),
        child_policy.lineage_depth
    );
    assert_eq!(boss_actor_id, "implement_child:depth=1");
}

#[tokio::test]
async fn boss_b_coerces_non_child_spawn_policy_to_child_depth() {
    let task_manager = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(task_manager.clone())
        .with_active_session_id("parent-session-policy")
        .with_active_surface(InteractionSurface::Cli)
        .with_boss_actor_policy(BossActorPolicy::executor_b(BossStage::Execution));

    let payload = serde_json::json!({
        "task": "spawn child from B",
        "role": "implement",
        "inherit_context": false,
        "max_turns": 0,
        "boss_actor_role": "executor_b",
        "boss_lineage_depth": 0
    })
    .to_string();

    AgentTool
        .invoke(&ToolCall::new("Agent", payload), &permissions)
        .await
        .expect("ExecutorB should be allowed to spawn a child");

    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].boss_actor_id.as_deref(),
        Some("implement_child:depth=1"),
        "non-child explicit role must be coerced to implement_child at depth 1"
    );
}

#[tokio::test]
async fn boss_b_fans_out_children_and_fans_in_summary() {
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-fan-in", task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Fan-out step")]),
        "test_boss_flow_fan_in.json",
    )
    .await;

    // Dispatch step 0 — spawns B fresh.
    let _ = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step 0 should dispatch");

    let tasks = task_manager.list();
    assert_eq!(tasks.len(), 1, "one B task after step 0 dispatch");
    let b_task_id = tasks[0].id.clone();

    // Record B's task id in the session so fan-in can find the step.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some(b_task_id.clone());
        }
    }
    // Also record B's task id in the step's worker_task_id so fan-in lookup works.
    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some(b_task_id.clone());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Simulate B spawning two children with orchestration_group_id = B's task id.
    let child1 = task_manager.create_with_type(
        "child-impl-1".to_string(),
        rust_agent::task::types::TaskType::LocalAgent,
        "parent-session-fan-in".to_string(),
        InteractionSurface::Cli,
    );
    let child2 = task_manager.create_with_type(
        "child-impl-2".to_string(),
        rust_agent::task::types::TaskType::LocalAgent,
        "parent-session-fan-in".to_string(),
        InteractionSurface::Cli,
    );
    task_manager.set_orchestration_group_id(&child1.id, Some(b_task_id.clone()));
    task_manager.set_orchestration_group_id(&child2.id, Some(b_task_id.clone()));
    task_manager.set_boss_actor_id(&child1.id, Some("implement_child:depth=1".into()));
    task_manager.set_boss_actor_id(&child2.id, Some("implement_child:depth=1".into()));

    // Verify group is not yet ready (children still pending).
    assert!(
        !task_manager.group_ready_for_fan_in(&b_task_id),
        "group must not be ready while children are pending"
    );

    // Complete both children — group fan-in fires.
    let dispatcher = rust_agent::interaction::dispatcher::NotificationDispatcher::new(
        rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
    );
    task_manager.complete_with_usage(&child1.id, &dispatcher, None);
    task_manager.complete_with_usage(&child2.id, &dispatcher, None);

    assert!(
        task_manager.group_ready_for_fan_in(&b_task_id),
        "group must be ready after all children complete"
    );

    // Verify group_summary returns a summary for B's group.
    let summary = task_manager.group_summary(&b_task_id);
    assert!(
        summary.is_some(),
        "group_summary must return a summary when all children complete"
    );

    // Simulate the group fan-in event arriving at the coordinator.
    let fan_in_event = TaskEvent {
        task_id: format!("group-{}", b_task_id),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: None,
        owner: rust_agent::task::types::TaskOwner {
            session_id: "parent-session-fan-in".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some(b_task_id.clone()),
        summary: "grouped research tasks completed".into(),
        result: "Agent task completed".into(),
        next_action: "synthesize grouped findings".into(),
        worker_role: None,
        orchestration_group_id: Some(b_task_id.clone()),
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    };

    coordinator.on_task_event(&fan_in_event).await.unwrap();

    // T16.6.D: fan-in now transitions to Reviewing (not Completed directly).
    // A's review gate must accept before the step is Completed.
    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Reviewing,
        "fan-in event must mark the step as Reviewing (pending A's review)"
    );
    assert!(
        !step.completed,
        "step.completed must be false until A accepts"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_child_event_cannot_complete_step_before_group_fan_in() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Child must not complete directly")]),
        "test_boss_flow_child_no_direct_complete.json",
    )
    .await;

    {
        let mut plan_guard = coordinator.plan.write().await;
        let plan = plan_guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-child-guard".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let child_event = TaskEvent {
        task_id: "child-impl-direct".into(),
        task_type: rust_agent::task::types::TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: Some(0),
        owner: rust_agent::task::types::TaskOwner {
            session_id: "parent-session-child-guard".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some("child-impl-direct".into()),
        summary: "child completed".into(),
        result: "child result".into(),
        next_action: "wait for group fan-in".into(),
        worker_role: Some(WorkerRole::Implement),
        orchestration_group_id: Some("b-task-child-guard".into()),
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    };

    coordinator.on_task_event(&child_event).await.unwrap();

    let plan = coordinator.plan.read().await;
    let step = &plan.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Running,
        "child event with orchestration_group_id must not complete the step directly"
    );
    assert!(
        !step.completed,
        "step must wait for group fan-in and A review"
    );

    let _ = std::fs::remove_file(plan_path);
}

// --- T16.6.C.2: ExecutorB policy injection ---

#[tokio::test]
async fn documentation_stage_runs_designer_reviewer_revision_loop() {
    let plan = BossPlan {
        plan_id: "plan-doc-loop".into(),
        task_description: "Design a safe execution plan".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        draft_spec: None,
        review_feedback: None,
        revision_notes: None,
        finalized: false,
        documentation_feedback: Vec::new(),
        steps: vec![boss_step(0, "Implement validated step")],
        accepted_by_user: false,
        auto_sequence: true,
        session_snapshot: None,
    };

    let (coordinator, plan_path) =
        coordinator_with_plan(plan, "test_boss_documentation_loop.json").await;

    assert_eq!(coordinator.get_stage().await, BossStage::Documentation);

    coordinator
        .finalize_documentation_loop(
            "A draft: outline the implementation and risks.",
            "B review: add feasibility notes, test plan, and edge-case risks.",
            "A revision: tighten scope and clarify acceptance criteria.",
            "Final spec: scoped implementation with explicit acceptance criteria.",
            "Pseudo-code: validate -> execute -> review -> complete.",
        )
        .await
        .unwrap();

    assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);

    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert_eq!(
        plan.draft_spec.as_deref(),
        Some("A draft: outline the implementation and risks.")
    );
    assert_eq!(
        plan.review_feedback.as_deref(),
        Some("B review: add feasibility notes, test plan, and edge-case risks.")
    );
    assert_eq!(
        plan.revision_notes.as_deref(),
        Some("A revision: tighten scope and clarify acceptance criteria.")
    );
    assert_eq!(
        plan.document_spec,
        "Final spec: scoped implementation with explicit acceptance criteria."
    );
    assert_eq!(
        plan.pseudo_code,
        "Pseudo-code: validate -> execute -> review -> complete."
    );
    assert!(plan.finalized, "documentation loop must finalize the plan");
    assert!(
        !plan.accepted_by_user,
        "documentation finalization must not skip user approval"
    );
    drop(plan_guard);

    let saved = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
    assert!(saved.finalized, "finalized plan must be persisted");
    assert_eq!(
        saved.review_feedback.as_deref(),
        Some("B review: add feasibility notes, test plan, and edge-case risks.")
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn user_feedback_reopens_documentation_loop_before_execution() {
    let plan = BossPlan {
        plan_id: "plan-doc-feedback".into(),
        task_description: "Refine plan from user notes".into(),
        document_spec: "Initial final spec".into(),
        pseudo_code: "Initial pseudo-code".into(),
        draft_spec: Some("Initial draft".into()),
        review_feedback: Some("Initial B review".into()),
        revision_notes: Some("Initial A revision".into()),
        finalized: true,
        documentation_feedback: Vec::new(),
        steps: vec![boss_step(0, "Implement after approval")],
        accepted_by_user: false,
        auto_sequence: true,
        session_snapshot: None,
    };

    let (coordinator, plan_path) =
        coordinator_with_plan(plan, "test_boss_documentation_feedback.json").await;

    coordinator
        .transition_to(BossStage::WaitingForApproval)
        .await
        .unwrap();

    let confirmed = coordinator
        .handle_user_approval("Please add rollback handling and explicit failure cases")
        .await
        .unwrap();

    assert!(
        !confirmed,
        "non-confirmation input must not enter execution"
    );
    assert_eq!(coordinator.get_stage().await, BossStage::Documentation);

    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert!(
        !plan.finalized,
        "user feedback must reopen the documentation loop"
    );
    assert!(
        !plan.accepted_by_user,
        "user feedback must keep approval unset"
    );
    assert_eq!(plan.documentation_feedback.len(), 1);
    assert_eq!(
        plan.documentation_feedback[0],
        "Please add rollback handling and explicit failure cases"
    );
    drop(plan_guard);

    let saved = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
    assert_eq!(saved.documentation_feedback.len(), 1);
    assert!(!saved.finalized);

    let _ = std::fs::remove_file(plan_path);
}
#[test]
fn boss_spawned_b_runtime_has_executor_policy_and_agent_tool() {
    use rust_agent::tool::builtin::{agent::AgentTool, bash::BashTool};

    // Build a registry with Boss ExecutorB production tools registered.
    let registry = ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(BashTool));

    // Assemble with executor_b context — Agent must be visible.
    let b_ctx = ToolAssemblyContext::executor_b(InteractionSurface::Cli, SessionMode::Headless);
    assert!(
        b_ctx.is_boss_executor_b(),
        "executor_b context must report is_boss_executor_b"
    );

    let b_registry = registry.assemble(b_ctx);
    let b_tools: Vec<_> = b_registry.all_metadata();
    assert!(
        b_tools.iter().any(|m| m.name == "Agent"),
        "ExecutorB registry must include Agent tool"
    );
    assert!(
        b_tools.iter().any(|m| m.name == "Bash"),
        "ExecutorB registry must include Bash so execution workers can run/verify scripts"
    );

    // Assemble with plain worker context — Agent must NOT be visible.
    let worker_ctx = ToolAssemblyContext::worker(InteractionSurface::Cli, SessionMode::Headless);
    let worker_registry = registry.assemble(worker_ctx);
    let worker_tools: Vec<_> = worker_registry.all_metadata();
    assert!(
        !worker_tools.iter().any(|m| m.name == "Agent"),
        "plain worker registry must NOT include Agent tool"
    );
    assert!(
        !worker_tools.iter().any(|m| m.name == "Bash"),
        "plain worker registry must NOT include open-world Bash by default"
    );

    // SubagentConfig with boss_actor_policy set must carry the policy through.
    let policy = BossActorPolicy::executor_b(BossStage::Execution);
    let config = SubagentConfig {
        worker_role: WorkerRole::Implement,
        inherit_context: false,
        max_turns: None,
        allowed_tools: None,
        lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Implement),
        boss_actor_policy: Some(policy),
    };
    assert!(
        config.boss_actor_policy.is_some(),
        "SubagentConfig must carry boss_actor_policy"
    );
    assert!(
        config.boss_actor_policy.unwrap().may_spawn(),
        "executor_b policy must allow spawning"
    );
}

#[test]
fn boss_spawn_payload_contains_executor_b_role_fields() {
    // Verify build_step_spawn_payload emits boss_actor_role and boss_lineage_depth.
    // We test this by parsing a known payload JSON directly.
    let payload = serde_json::json!({
        "task": "Boss mode step 0",
        "role": "implement",
        "reuse_strategy": "running_only",
        "boss_actor_role": "executor_b",
        "boss_lineage_depth": 0,
        "orchestration_group_id": "boss-plan-alpha-b",
    });
    assert_eq!(payload["boss_actor_role"], "executor_b");
    assert_eq!(payload["boss_lineage_depth"], 0);
    assert_eq!(payload["orchestration_group_id"], "boss-plan-alpha-b");
}

// --- T16.6.D: A review gate ---

fn fan_in_event(b_task_id: &str) -> TaskEvent {
    TaskEvent {
        task_id: format!("group-{}", b_task_id),
        task_type: TaskType::LocalAgent,
        status: TaskStatus::Completed,
        step_id: None,
        owner: TaskOwner {
            session_id: "test-session".into(),
            surface: InteractionSurface::Cli,
        },
        target_task_id: Some(b_task_id.into()),
        summary: "grouped research tasks completed".into(),
        result: "Agent task completed".into(),
        next_action: "synthesize grouped findings".into(),
        worker_role: None,
        orchestration_group_id: Some(b_task_id.into()),
        phase: None,
        validation_state: None,
        output_file: "".into(),
        usage: None,
    }
}

#[tokio::test]
async fn boss_a_review_accepts_diff_before_step_completion() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step to review")]),
        "test_boss_review_accept.json",
    )
    .await;

    // Seed B's task id in the step so fan-in lookup works.
    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-review".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Fan-in fires — step must enter Reviewing, not Completed.
    coordinator
        .on_task_event(&fan_in_event("b-task-review"))
        .await
        .unwrap();

    {
        let guard = coordinator.plan.read().await;
        let step = &guard.as_ref().unwrap().steps[0];
        assert_eq!(
            step.status,
            BossPlanStepStatus::Reviewing,
            "fan-in must enter Reviewing"
        );
        assert!(
            !step.completed,
            "step must not be completed before A accepts"
        );
    }

    // A accepts — step must move to Completed.
    coordinator
        .on_review_event(0, true, "LGTM, all acceptance criteria met", None)
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Completed,
        "A accept must complete the step"
    );
    assert!(
        step.completed,
        "step.completed must be true after A accepts"
    );
    assert_eq!(
        step.last_review_summary.as_deref(),
        Some("LGTM, all acceptance criteria met")
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_a_review_rejects_and_sends_correction_to_b() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step to reject")]),
        "test_boss_review_reject.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-reject".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    coordinator
        .on_task_event(&fan_in_event("b-task-reject"))
        .await
        .unwrap();

    // A rejects with a correction.
    coordinator
        .on_review_event(
            0,
            false,
            "Missing error handling in step output",
            Some("Add error handling for the edge case in section 3"),
        )
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Rejected,
        "A reject must set Rejected status"
    );
    assert!(
        !step.completed,
        "step must not be completed after rejection"
    );
    assert_eq!(
        step.attempt_count, 1,
        "attempt_count must increment on rejection"
    );
    assert_eq!(
        step.last_correction.as_deref(),
        Some("Add error handling for the edge case in section 3")
    );
    assert_eq!(
        step.last_review_summary.as_deref(),
        Some("Missing error handling in step output")
    );

    // Rejected step must be runnable — advance_plan should re-dispatch B.
    drop(guard);
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-reject", task_manager.clone());
    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("rejected step must be re-dispatched");

    // Spawn payload must embed the correction.
    assert!(
        payload.contains("correction from review"),
        "retry payload must embed the correction"
    );
    assert!(
        payload.contains("Add error handling for the edge case in section 3"),
        "retry payload must contain the correction text"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_step_fails_only_after_retry_budget_exhausted() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![BossPlanStep {
            retry_budget: 2,
            ..boss_step(0, "Budget-limited step")
        }]),
        "test_boss_retry_budget.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-budget".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // First rejection — attempt_count = 1, still under budget (2).
    coordinator
        .on_task_event(&fan_in_event("b-task-budget"))
        .await
        .unwrap();
    coordinator
        .on_review_event(0, false, "Not good enough", Some("Fix it"))
        .await
        .unwrap();

    {
        let guard = coordinator.plan.read().await;
        let step = &guard.as_ref().unwrap().steps[0];
        assert_eq!(
            step.status,
            BossPlanStepStatus::Rejected,
            "first rejection must be Rejected"
        );
        assert_eq!(step.attempt_count, 1);
    }

    // Reset to Reviewing for second rejection.
    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Reviewing;
    }

    // Second rejection — attempt_count = 2, hits budget → Failed.
    coordinator
        .on_review_event(0, false, "Still not good enough", Some("Fix it again"))
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Failed,
        "step must be Failed after retry budget exhausted"
    );
    assert_eq!(
        step.attempt_count, 2,
        "attempt_count must equal retry_budget"
    );
    assert!(!step.completed, "failed step must not be marked completed");

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn boss_a_replan_step_does_not_redispatch_b_and_is_distinct_from_rejected() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step to replan")]),
        "test_boss_review_replan.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-replan".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Force state-only fallback path so the provided review signal is interpreted directly.
    *coordinator.actor_registry.write().await = None;

    coordinator
        .on_review_event(
            0,
            false,
            "Current step needs strategy rewrite",
            Some("REPLAN_STEP. REASON: split implementation from validation"),
        )
        .await
        .unwrap();

    {
        let guard = coordinator.plan.read().await;
        let step = &guard.as_ref().unwrap().steps[0];
        assert_eq!(
            step.status,
            BossPlanStepStatus::ReplanRequired,
            "replan decision must not collapse into Rejected"
        );
        assert_eq!(
            step.last_review_summary.as_deref(),
            Some("Current step needs strategy rewrite")
        );
        assert_eq!(
            step.last_correction.as_deref(),
            Some("replan required: split implementation from validation")
        );
    }

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("parent-session-replan", task_manager.clone());
    let payload = coordinator.advance_plan(&app_state).await.unwrap();
    assert_eq!(
        payload.as_deref(),
        Some(
            "Boss step 0 requires replanning before execution can continue. Reason: split implementation from validation"
        )
    );

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(report.steps[0].status, BossPlanStepStatus::ReplanRequired);
    assert_eq!(
        report.steps[0].last_review_summary.as_deref(),
        Some("Current step needs strategy rewrite")
    );
    assert_eq!(
        report.steps[0].action_required.as_deref(),
        Some("replan_current_step")
    );
    assert_eq!(
        report.steps[0].blocker_reason.as_deref(),
        Some("split implementation from validation")
    );

    // Distinguish from ordinary correction-based rejection: status is different,
    // and the step carries the structured replan signal instead of a normal patch retry path.
    assert_ne!(report.steps[0].status, BossPlanStepStatus::Rejected);

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn repair_replan_step_restores_pending_and_requires_manual_advance() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Original step")]),
        "test_boss_replan_repair.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::ReplanRequired;
        plan.steps[0].attempt_count = 2;
        plan.steps[0].worker_task_id = Some("old-b-task".into());
        plan.steps[0].review_task_id = Some("old-review-task".into());
        plan.steps[0].last_review_summary = Some("Current step needs strategy rewrite".into());
        plan.steps[0].last_correction =
            Some("replan required: split implementation from validation".into());
    }

    coordinator
        .repair_replan_step(
            0,
            "Patched step".into(),
            Some("Patched objective".into()),
            vec!["patched acceptance a".into(), "patched acceptance b".into()],
        )
        .await
        .unwrap();

    {
        let guard = coordinator.plan.read().await;
        let step = &guard.as_ref().unwrap().steps[0];
        assert_eq!(step.status, BossPlanStepStatus::Pending);
        assert!(!step.completed);
        assert_eq!(step.description, "Patched step");
        assert_eq!(step.objective.as_deref(), Some("Patched objective"));
        assert_eq!(
            step.acceptance,
            vec![
                "patched acceptance a".to_string(),
                "patched acceptance b".to_string()
            ]
        );
        assert_eq!(step.attempt_count, 0);
        assert!(step.worker_task_id.is_none());
        assert!(step.review_task_id.is_none());
        assert_eq!(
            step.last_review_summary.as_deref(),
            Some("Current step needs strategy rewrite")
        );
        assert!(step.last_correction.is_none());
    }

    let persisted = load_plan(&plan_path).await.unwrap();
    let persisted_step = &persisted.steps[0];
    assert_eq!(persisted_step.status, BossPlanStepStatus::Pending);
    assert_eq!(persisted_step.description, "Patched step");
    assert_eq!(persisted_step.attempt_count, 0);

    let app_state = app_state_with_tasks("parent-session-repair", Arc::new(TaskManager::default()));
    let payload = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(
        payload.is_some(),
        "step should only resume after explicit advance_plan call"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn repaired_step_redispatch_payload_does_not_carry_old_replan_reason() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Original step")]),
        "test_boss_replan_payload_cleanup.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::ReplanRequired;
        plan.steps[0].last_review_summary = Some("Current step needs strategy rewrite".into());
        plan.steps[0].last_correction =
            Some("replan required: split implementation from validation".into());
    }

    coordinator
        .repair_replan_step(
            0,
            "Patched step".into(),
            Some("Patched objective".into()),
            vec!["patched acceptance a".into()],
        )
        .await
        .unwrap();

    let app_state = app_state_with_tasks(
        "parent-session-repair-payload",
        Arc::new(TaskManager::default()),
    );
    let payload = coordinator
        .advance_plan(&app_state)
        .await
        .unwrap()
        .expect("step should redispatch after explicit advance_plan");

    assert!(!payload.contains("replan required:"));
    assert!(!payload.contains("split implementation from validation"));
    assert!(payload.contains("Patched objective"));
    assert!(payload.contains("patched acceptance a"));

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn replan_required_is_persisted_and_repairable_after_reload() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Original step")]),
        "test_boss_replan_persist.json",
    )
    .await;

    // Force state-only fallback path so the provided review signal is interpreted directly.
    *coordinator.actor_registry.write().await = None;

    coordinator
        .on_review_event(
            0,
            false,
            "Current step needs strategy rewrite",
            Some("REPLAN_STEP. REASON: split implementation from validation"),
        )
        .await
        .unwrap();

    let persisted = load_plan(&plan_path).await.unwrap();
    let persisted_step = &persisted.steps[0];
    assert_eq!(persisted_step.status, BossPlanStepStatus::ReplanRequired);
    assert_eq!(
        persisted_step.last_review_summary.as_deref(),
        Some("Current step needs strategy rewrite")
    );
    assert_eq!(
        persisted_step.last_correction.as_deref(),
        Some("replan required: split implementation from validation")
    );

    let restored = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    restored
        .repair_replan_step(
            0,
            "Patched step after reload".into(),
            Some("Patched objective after reload".into()),
            vec!["patched acceptance after reload".into()],
        )
        .await
        .unwrap();

    let repaired = load_plan(&plan_path).await.unwrap();
    let repaired_step = &repaired.steps[0];
    assert_eq!(repaired_step.status, BossPlanStepStatus::Pending);
    assert_eq!(repaired_step.description, "Patched step after reload");
    assert_eq!(
        repaired_step.objective.as_deref(),
        Some("Patched objective after reload")
    );
    assert_eq!(
        repaired_step.acceptance,
        vec!["patched acceptance after reload".to_string()]
    );
    assert_eq!(repaired_step.attempt_count, 0);

    let _ = std::fs::remove_file(plan_path);
}

// --- T16.6.G.5: BossRuntimeHost assembly layer ---

#[tokio::test]
async fn production_assembly_uses_explicit_runtime_host_not_global_singleton() {
    let host_a = BossRuntimeHost::new();
    let host_b = BossRuntimeHost::new();

    assert!(
        !Arc::ptr_eq(&host_a.owner(), &host_b.owner()),
        "each BossRuntimeHost must produce an independent owner"
    );

    let coordinator_a = BossCoordinator::new_with_runtime_owner(host_a.owner());
    let coordinator_b = BossCoordinator::new_with_runtime_owner(host_b.owner());

    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    coordinator_a.shutdown_runtime_owner();
    assert!(
        coordinator_a
            .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
            .await
            .is_err(),
        "coordinator_a must be blocked after its host owner shuts down"
    );

    let response = coordinator_b
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response.is_ok(),
        "coordinator_b must remain usable after an unrelated host shuts down"
    );
}

#[tokio::test]
async fn runtime_host_owner_survives_rebind_and_restart() {
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let coordinator = BossCoordinator::new_with_runtime_owner(host.owner());

    coordinator.ensure_control_runtime().await;
    let key_before = coordinator.current_runtime_key().await.unwrap();
    coordinator.rebind_control_runtime().await;
    let key_after = coordinator.current_runtime_key().await.unwrap();
    assert_ne!(key_before, key_after, "rebind must produce a new key");

    let response = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response.is_ok(),
        "control request must succeed after rebind via host"
    );

    coordinator.shutdown_runtime_owner();
    coordinator.restart_runtime_owner();

    let response2 = coordinator
        .handle_control_request(BossControlRequest::Report, &task_manager, &dispatcher)
        .await;
    assert!(
        response2.is_ok(),
        "control request must succeed after owner restart via host"
    );
}

// --- T16.6.H: Boss actor runtime mailbox seam ---

use rust_agent::core::boss_actor_runtime::{DesignerACommand, ExecutorBCommand};
use rust_agent::core::boss_state::BossActorStatus as ActorStatus;

#[tokio::test]
async fn restore_bootstraps_actor_runtimes_that_are_addressable() {
    let plan_path = std::env::temp_dir().join("boss_h_restore_actor.json");
    let plan = BossPlan {
        plan_id: "plan-h-restore".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    // Actor registry must be bootstrapped after restore.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard
        .as_ref()
        .expect("actor registry must be bootstrapped after restore");

    // Both mailboxes must be open and addressable.
    assert!(
        !registry.a_mailbox().is_closed(),
        "A mailbox must be open after restore"
    );
    assert!(
        !registry.b_mailbox().is_closed(),
        "B mailbox must be open after restore"
    );

    // Send a command to A and verify it processes without error.
    let event = registry
        .a_mailbox()
        .request(DesignerACommand::Plan {
            plan_id: "plan-h-restore".into(),
            document_spec: "spec".into(),
        })
        .await;
    assert!(
        event.is_ok(),
        "A mailbox must accept Plan command after restore"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn advance_plan_dispatches_step_through_b_mailbox() {
    let plan_path = std::env::temp_dir().join("boss_h_advance_b.json");
    let plan = BossPlan {
        plan_id: "plan-h-advance".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    // Ensure actor registry is live.
    coordinator.ensure_actor_registry().await;

    // Manually send a DispatchStep to B's mailbox (simulating what advance_plan does).
    let event = {
        let registry_guard = coordinator.actor_registry.read().await;
        let registry = registry_guard.as_ref().unwrap();
        registry
            .b_mailbox()
            .request(ExecutorBCommand::DispatchStep {
                step_id: 1,
                payload: "test-payload".into(),
            })
            .await
    };

    assert!(event.is_ok(), "B mailbox must accept DispatchStep command");
    let event = event.unwrap();
    match event {
        rust_agent::core::boss_actor_runtime::BossActorEvent::StepDispatched {
            step_id, ..
        } => {
            assert_eq!(step_id, 1, "dispatched step_id must match");
        }
        other => panic!("expected StepDispatched, got {:?}", other),
    }

    // B's state must reflect the active step.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().unwrap();
    let b_status = registry.executor_b.status().await;
    assert_eq!(
        b_status,
        ActorStatus::Active,
        "B must be Active after DispatchStep"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn stop_sends_stop_command_to_actor_mailboxes() {
    let plan_path = std::env::temp_dir().join("boss_h_stop_actors.json");
    let plan = BossPlan {
        plan_id: "plan-h-stop".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    coordinator.ensure_actor_registry().await;

    // Activate both actors first.
    {
        let registry_guard = coordinator.actor_registry.read().await;
        let registry = registry_guard.as_ref().unwrap();
        let _ = registry
            .a_mailbox()
            .send(DesignerACommand::Plan {
                plan_id: "plan-h-stop".into(),
                document_spec: "spec".into(),
            })
            .await;
        let _ = registry
            .b_mailbox()
            .send(ExecutorBCommand::DispatchStep {
                step_id: 1,
                payload: "payload".into(),
            })
            .await;
    }
    // Give the actor loops a tick to process.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Now send Stop to both via their mailboxes directly (mirrors what stop() does).
    {
        let registry_guard = coordinator.actor_registry.read().await;
        let registry = registry_guard.as_ref().unwrap();
        let a_event = registry.a_mailbox().request(DesignerACommand::Stop).await;
        let b_event = registry.b_mailbox().request(ExecutorBCommand::Stop).await;
        assert!(a_event.is_ok(), "A must accept Stop command");
        assert!(b_event.is_ok(), "B must accept Stop command");
    }

    // Give the actor loops a tick to process the Stop.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // After Stop, both mailboxes must be closed.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().unwrap();
    assert!(
        registry.a_mailbox().is_closed(),
        "A mailbox must be closed after Stop"
    );
    assert!(
        registry.b_mailbox().is_closed(),
        "B mailbox must be closed after Stop"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// --- T16.6.H.1: mailbox-driven production entry points ---

#[tokio::test]
async fn advance_plan_sends_dispatch_to_b_mailbox_and_b_state_is_active() {
    let plan_path = std::env::temp_dir().join("boss_h1_advance_b_state.json");
    let plan = BossPlan {
        plan_id: "plan-h1-advance".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h1-advance", task_manager.clone());

    let result = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(result.is_some(), "step 0 should dispatch");

    // B's actor state must be Active — proves the mailbox handler ran before advance_plan returned.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard
        .as_ref()
        .expect("actor registry must exist after advance_plan");
    let b_status = registry.executor_b.status().await;
    assert_eq!(
        b_status,
        rust_agent::core::boss_state::BossActorStatus::Active,
        "B must be Active after advance_plan — mailbox handler must have run before tool call"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn on_review_event_sends_review_to_a_mailbox_and_a_state_reflects_step() {
    let plan_path = std::env::temp_dir().join("boss_h1_review_a_state.json");
    let plan = BossPlan {
        plan_id: "plan-h1-review".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step to review")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    {
        let mut guard = coordinator.plan.write().await;
        let p = guard.as_mut().unwrap();
        p.steps[0].status = BossPlanStepStatus::Reviewing;
        p.steps[0].worker_task_id = Some("b-task-h1".into());
    }

    coordinator
        .on_review_event(0, true, "LGTM", None)
        .await
        .unwrap();

    // A's actor state must reflect the reviewed step — proves mailbox handler ran before plan mutation.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard
        .as_ref()
        .expect("actor registry must exist after on_review_event");
    let a_state = registry.designer_a.state.read().await;
    assert_eq!(
        a_state.current_step,
        Some(0),
        "A's current_step must be 0 after on_review_event — mailbox handler must have run"
    );
    drop(a_state);
    drop(registry_guard);

    // Plan state must also be updated correctly.
    let plan_guard = coordinator.plan.read().await;
    let step = &plan_guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Completed,
        "step must be Completed after accepted review"
    );
    assert!(step.completed, "step.completed must be true");

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn stop_via_handle_control_request_closes_a_and_b_mailboxes() {
    let plan_path = std::env::temp_dir().join("boss_h1_stop_mailboxes.json");
    let plan = BossPlan {
        plan_id: "plan-h1-stop".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    coordinator.ensure_actor_registry().await;

    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let response = coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "test-session-h1".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    assert!(
        matches!(response, BossControlResponse::Stop(_)),
        "handle_control_request(Stop) must return Stop outcome"
    );

    // Both mailboxes must be closed — stop() awaits Stopped from both before returning.
    let registry_guard = coordinator.actor_registry.read().await;
    let registry = registry_guard.as_ref().unwrap();
    assert!(
        registry.a_mailbox().is_closed(),
        "A mailbox must be closed after Stop via handle_control_request"
    );
    assert!(
        registry.b_mailbox().is_closed(),
        "B mailbox must be closed after Stop via handle_control_request"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// --- T16.6.H.2: execution side effects owned by B runtime ---

#[tokio::test]
async fn advance_plan_records_dispatch_payload_via_b_runtime_callback() {
    let plan_path = std::env::temp_dir().join("boss_h2_b_callback.json");
    let plan = BossPlan {
        plan_id: "plan-h2-callback".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h2-callback", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let status = coordinator.status.read().await;
    assert!(
        status.last_b_dispatch_payload.is_some(),
        "B's execution callback must have fired and recorded the dispatch payload"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn advance_plan_does_not_call_invoke_agent_tool_directly_after_h2() {
    let plan_path = std::env::temp_dir().join("boss_h2_no_inline_tool.json");
    let plan = BossPlan {
        plan_id: "plan-h2-no-inline".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h2-no-inline", task_manager.clone());

    let result = coordinator.advance_plan(&app_state).await;
    assert!(
        result.is_ok(),
        "advance_plan must succeed without inline tool call: {:?}",
        result
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let status = coordinator.status.read().await;
    assert!(
        status.last_b_dispatch_payload.is_some(),
        "B's callback must have fired — execution side effect is B-owned"
    );

    let _ = std::fs::remove_file(&plan_path);
}

#[tokio::test]
async fn b_runtime_callback_fires_for_continue_step_as_well() {
    let plan_path = std::env::temp_dir().join("boss_h2_continue_callback.json");
    let plan = BossPlan {
        plan_id: "plan-h2-continue".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero"), boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h2-continue", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let first_payload = coordinator
        .status
        .read()
        .await
        .last_b_dispatch_payload
        .clone();
    assert!(
        first_payload.is_some(),
        "first dispatch must record payload"
    );

    {
        let mut guard = coordinator.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.task_id = Some("b-running-task".into());
            session.executor_b.status = rust_agent::core::boss_state::BossActorStatus::Active;
        }
    }
    {
        let mut guard = coordinator.plan.write().await;
        if let Some(plan) = guard.as_mut() {
            plan.steps[0].completed = true;
            plan.steps[0].status = BossPlanStepStatus::Completed;
        }
    }

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    let second_payload = coordinator
        .status
        .read()
        .await
        .last_b_dispatch_payload
        .clone();
    assert!(
        second_payload.is_some(),
        "ContinueStep must also record payload via B's callback"
    );
    assert_ne!(
        first_payload, second_payload,
        "second dispatch payload must differ from first"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.3 — A-side orchestration owned by DesignerARuntime
// ---------------------------------------------------------------------------

/// on_review_event() side effect (plan mutation + auto-advance) is triggered from
/// A's runtime handler, not inline in the coordinator.
#[tokio::test]
async fn on_review_event_side_effect_triggered_from_a_runtime_handler() {
    let plan_path = std::env::temp_dir().join("boss_h3_review_side_effect.json");
    let plan = BossPlan {
        plan_id: "plan-h3-review".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h3-review", task_manager.clone());

    // Advance to get step 0 running.
    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // Wire A's callbacks via the auto path (uses auto_advance_app_state).
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }

    // Pre-seed designer_a.session_id to a non-placeholder value so ensure_a_session
    // skips the real LLM spawn. send_message will return false (task not in running_owners),
    // causing ask_a_session to bail and fall back to coordinator's accepted=true verdict.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = "fake-a-session-h3".into();
        }
    }

    // Call on_review_event — A's callback should mutate the plan.
    coordinator
        .on_review_event(0, true, "looks good", None)
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // Plan step 0 must be Completed — set by A's callback, not coordinator inline.
    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert_eq!(
        plan.steps[0].status,
        BossPlanStepStatus::Completed,
        "A runtime callback must mark step Completed"
    );
    assert_eq!(
        plan.steps[0].last_review_summary.as_deref(),
        Some("looks good"),
        "A runtime callback must record review summary"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// finalize_documentation_loop() wires A callbacks and sends FinalizeDocumentation to A mailbox;
/// has_a_callbacks must be true and A's handler drives the WaitingForApproval stage transition.
#[tokio::test]
async fn finalize_documentation_loop_routes_through_a_mailbox() {
    let plan_path = std::env::temp_dir().join("boss_h3_finalize_doc.json");
    let plan = BossPlan {
        plan_id: "plan-h3-finalize".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h3-finalize", task_manager.clone());

    // Set auto_advance_app_state so ensure_actor_registry_with_a_callbacks_auto can wire callbacks.
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }

    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    // has_a_callbacks must be true — A callbacks were wired, not the coordinator fallback.
    let has_a_callbacks = coordinator
        .actor_registry
        .read()
        .await
        .as_ref()
        .map(|r| r.has_a_callbacks)
        .unwrap_or(false);
    assert!(
        has_a_callbacks,
        "finalize_documentation_loop must wire A callbacks (has_a_callbacks == true)"
    );

    // A's mailbox handler must have updated A's internal stage to WaitingForApproval.
    let a_stage = {
        let guard = coordinator.actor_registry.read().await;
        if let Some(r) = guard.as_ref() {
            Some(r.designer_a.state.read().await.stage)
        } else {
            None
        }
    };
    assert_eq!(
        a_stage,
        Some(BossStage::WaitingForApproval),
        "A runtime handler must set stage to WaitingForApproval — not coordinator fallback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// handle_user_approval() wires A callbacks and sends UserApproval to A mailbox;
/// has_a_callbacks must be true and A's handler drives the Execution stage transition.
#[tokio::test]
async fn handle_user_approval_routes_through_a_mailbox_and_a_drives_stage_transition() {
    let plan_path = std::env::temp_dir().join("boss_h3_user_approval.json");
    let plan = BossPlan {
        plan_id: "plan-h3-approval".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h3-approval", task_manager.clone());

    // Set auto_advance_app_state so ensure_actor_registry_with_a_callbacks_auto can wire callbacks.
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }

    // Finalize first so approval is valid.
    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    let approved = coordinator.handle_user_approval("Y").await.unwrap();
    assert!(approved, "Y input must return approved=true");

    // has_a_callbacks must be true — A callbacks were wired, not the coordinator fallback.
    let has_a_callbacks = coordinator
        .actor_registry
        .read()
        .await
        .as_ref()
        .map(|r| r.has_a_callbacks)
        .unwrap_or(false);
    assert!(
        has_a_callbacks,
        "handle_user_approval must wire A callbacks (has_a_callbacks == true)"
    );

    // A's mailbox handler must have updated A's internal stage to Execution.
    let a_stage = {
        let guard = coordinator.actor_registry.read().await;
        if let Some(r) = guard.as_ref() {
            Some(r.designer_a.state.read().await.stage)
        } else {
            None
        }
    };
    assert_eq!(
        a_stage,
        Some(BossStage::Execution),
        "A runtime handler must set stage to Execution — not coordinator fallback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.4 — Unified actor runtime bootstrap, no lazy rewiring
// ---------------------------------------------------------------------------

/// After bootstrap_actor_registry_with_app_state, the registry has both
/// has_executor and has_a_callbacks set — no subsequent call replaces it.
#[tokio::test]
async fn bootstrap_with_app_state_produces_full_registry_in_one_shot() {
    let plan_path = std::env::temp_dir().join("boss_h4_one_shot.json");
    let plan = BossPlan {
        plan_id: "plan-h4-oneshot".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h4-oneshot", task_manager.clone());

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let (has_exec, has_a) = {
        let guard = coordinator.actor_registry.read().await;
        let r = guard.as_ref().unwrap();
        (r.has_executor, r.has_a_callbacks)
    };
    assert!(
        has_exec,
        "bootstrap_actor_registry_with_app_state must set has_executor"
    );
    assert!(
        has_a,
        "bootstrap_actor_registry_with_app_state must set has_a_callbacks"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// Registry identity is stable across multiple advance_plan calls — no rewiring replaces it.
#[tokio::test]
async fn registry_identity_stable_across_multiple_advance_plan_calls() {
    let plan_path = std::env::temp_dir().join("boss_h4_identity.json");
    let plan = BossPlan {
        plan_id: "plan-h4-identity".into(),
        accepted_by_user: true,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero"), boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h4-identity", task_manager.clone());

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_first = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_second = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_first, b_ptr_second,
        "B mailbox identity must be stable — registry must not be replaced on second advance_plan"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After restore_or_init + bootstrap_actor_registry_with_app_state, advance_plan
/// does not replace the registry (already fully bootstrapped).
#[tokio::test]
async fn restore_then_bootstrap_with_app_state_is_immediately_ready() {
    let plan_path = std::env::temp_dir().join("boss_h4_restore_ready.json");
    let plan = BossPlan {
        plan_id: "plan-h4-restore".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h4-restore", task_manager.clone());

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let b_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_before, b_ptr_after,
        "advance_plan must not replace a fully-bootstrapped registry"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.5 — Converged restore/bootstrap: full registry from restore
// ---------------------------------------------------------------------------

/// restore_or_init_with_app_state produces a full registry immediately —
/// no state-only phase, no lazy upgrade needed.
#[tokio::test]
async fn restore_or_init_with_app_state_produces_full_registry_immediately() {
    let plan_path = std::env::temp_dir().join("boss_h5_full_restore.json");
    let plan = BossPlan {
        plan_id: "plan-h5-full".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h5-full", task_manager.clone());

    let coordinator = BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
        .await
        .unwrap();

    // Registry must be full immediately — no lazy upgrade required.
    let (has_exec, has_a) = {
        let guard = coordinator.actor_registry.read().await;
        let r = guard.as_ref().unwrap();
        (r.has_executor, r.has_a_callbacks)
    };
    assert!(
        has_exec,
        "restore_or_init_with_app_state must produce has_executor=true"
    );
    assert!(
        has_a,
        "restore_or_init_with_app_state must produce has_a_callbacks=true"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After restore_or_init_with_app_state, advance_plan does not replace the registry.
#[tokio::test]
async fn advance_plan_after_full_restore_does_not_replace_registry() {
    let plan_path = std::env::temp_dir().join("boss_h5_advance_stable.json");
    let plan = BossPlan {
        plan_id: "plan-h5-advance".into(),
        accepted_by_user: true,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h5-advance", task_manager.clone());

    let coordinator = BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
        .await
        .unwrap();

    let b_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_before, b_ptr_after,
        "advance_plan must not replace registry after restore_or_init_with_app_state"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After restore_or_init_with_app_state, finalize_documentation_loop does not replace the registry.
#[tokio::test]
async fn finalize_documentation_loop_after_full_restore_does_not_replace_registry() {
    let plan_path = std::env::temp_dir().join("boss_h5_finalize_stable.json");
    let plan = BossPlan {
        plan_id: "plan-h5-finalize".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h5-finalize", task_manager.clone());

    let coordinator = BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
        .await
        .unwrap();

    let a_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    let a_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    assert_eq!(
        a_ptr_before, a_ptr_after,
        "finalize_documentation_loop must not replace registry after restore_or_init_with_app_state"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.6 — Production assembly default: full registry from new_with_runtime_owner
// ---------------------------------------------------------------------------

/// Simulates the production assembly path: new_with_runtime_owner + bootstrap_actor_registry_with_app_state.
/// The coordinator must have has_executor && has_a_callbacks immediately after bootstrap.
#[tokio::test]
async fn production_assembly_produces_full_registry() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h6-prod", task_manager.clone());

    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let (has_exec, has_a) = {
        let guard = coordinator.actor_registry.read().await;
        let r = guard.as_ref().unwrap();
        (r.has_executor, r.has_a_callbacks)
    };
    assert!(
        has_exec,
        "production assembly must produce has_executor=true"
    );
    assert!(
        has_a,
        "production assembly must produce has_a_callbacks=true"
    );
}

/// After production assembly bootstrap, advance_plan does not trigger a mode upgrade.
#[tokio::test]
async fn advance_plan_after_production_assembly_does_not_upgrade_registry() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let plan_path = std::env::temp_dir().join("boss_h6_advance_no_upgrade.json");
    let plan = BossPlan {
        plan_id: "plan-h6-advance".into(),
        accepted_by_user: true,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h6-advance", task_manager.clone());

    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    {
        let loaded = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
        let mut guard = coordinator.plan.write().await;
        *guard = Some(loaded);
        let mut status = coordinator.status.write().await;
        status.planning_file = Some(plan_path.to_string_lossy().into_owned());
        status.stage = rust_agent::core::boss_state::BossStage::Execution;
    }

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let b_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let b_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        b_ptr_before, b_ptr_after,
        "advance_plan must not upgrade registry after production assembly bootstrap"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// After production assembly bootstrap, finalize_documentation_loop does not trigger a mode upgrade.
#[tokio::test]
async fn finalize_documentation_loop_after_production_assembly_does_not_upgrade_registry() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let plan_path = std::env::temp_dir().join("boss_h6_finalize_no_upgrade.json");
    let plan = BossPlan {
        plan_id: "plan-h6-finalize".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h6-finalize", task_manager.clone());

    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    {
        let loaded = rust_agent::core::boss::load_plan(&plan_path).await.unwrap();
        let mut guard = coordinator.plan.write().await;
        *guard = Some(loaded);
        let mut status = coordinator.status.write().await;
        status.planning_file = Some(plan_path.to_string_lossy().into_owned());
    }

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let a_ptr_before = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    coordinator
        .finalize_documentation_loop("draft", "feedback", "notes", "final spec", "pseudo")
        .await
        .unwrap();

    let a_ptr_after = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().designer_a.state) as usize
    };

    assert_eq!(
        a_ptr_before, a_ptr_after,
        "finalize_documentation_loop must not upgrade registry after production assembly bootstrap"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T16.6.H.7 — API surface hardening: state-only paths are pub(crate) only
// ---------------------------------------------------------------------------

/// new() is pub(crate): production code must use new_with_runtime_owner + bootstrap.
/// This test verifies that new_with_runtime_owner produces a state-only registry
/// (has_executor == false) before bootstrap, and full registry after.
#[tokio::test]
async fn h7_new_with_runtime_owner_is_state_only_before_bootstrap() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    // Before bootstrap: no registry at all.
    let has_registry = coordinator.actor_registry.read().await.is_some();
    assert!(
        !has_registry,
        "new_with_runtime_owner must not pre-populate registry"
    );

    // After bootstrap_actor_registry_with_app_state: full mode.
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h7-new", task_manager);
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor,
        "h7: has_executor must be true after bootstrap"
    );
    assert!(
        registry.has_a_callbacks,
        "h7: has_a_callbacks must be true after bootstrap"
    );
}

/// bootstrap_actor_registry is pub(crate): calling it produces a state-only registry.
/// Production code must not rely on it for full-mode operation.
#[tokio::test]
async fn h7_bootstrap_actor_registry_is_state_only() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);
    coordinator.bootstrap_actor_registry().await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        !registry.has_executor,
        "h7: state-only bootstrap must not set has_executor"
    );
    assert!(
        !registry.has_a_callbacks,
        "h7: state-only bootstrap must not set has_a_callbacks"
    );
}

/// Production assembly contract: new_with_runtime_owner + bootstrap_actor_registry_with_app_state
/// is the only path that produces has_executor && has_a_callbacks == true.
/// Calling bootstrap_actor_registry_with_app_state a second time is a no-op (idempotent).
#[tokio::test]
async fn h7_production_assembly_is_full_mode_and_idempotent() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h7-prod", task_manager);

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let ptr_first = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    // Second call must be a no-op — registry identity must be stable.
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let ptr_second = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        ptr_first, ptr_second,
        "h7: second bootstrap call must not replace registry"
    );

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor && registry.has_a_callbacks,
        "h7: production assembly must be full mode"
    );
}

// ---------------------------------------------------------------------------
// T16.6.H.8 — new_with_app_state is the first-class full-mode constructor
// ---------------------------------------------------------------------------

/// new_with_app_state produces a full-mode registry immediately — no separate bootstrap call needed.
#[tokio::test]
async fn h8_new_with_app_state_is_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h8-new", task_manager);

    let coordinator = BossCoordinator::new_with_app_state(runtime_owner, &app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor,
        "h8: new_with_app_state must set has_executor"
    );
    assert!(
        registry.has_a_callbacks,
        "h8: new_with_app_state must set has_a_callbacks"
    );
}

/// restore_or_init_with_app_state produces a full-mode registry immediately.
/// Symmetric with new_with_app_state for the restore path.
#[tokio::test]
async fn h8_restore_or_init_with_app_state_is_full_mode() {
    let plan_path = std::env::temp_dir().join("h8_restore_test_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h8-restore", task_manager);

    // No file — falls back to fresh coordinator.
    let coordinator = BossCoordinator::restore_or_init_with_app_state(&plan_path, &app_state)
        .await
        .unwrap();

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor,
        "h8: restore_or_init_with_app_state must set has_executor"
    );
    assert!(
        registry.has_a_callbacks,
        "h8: restore_or_init_with_app_state must set has_a_callbacks"
    );
}

/// new_with_app_state and restore_or_init_with_app_state are the only paths that produce
/// has_executor && has_a_callbacks == true without a separate bootstrap call.
/// new_with_runtime_owner alone must NOT produce a full-mode registry.
#[tokio::test]
async fn h8_new_with_runtime_owner_alone_is_not_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = BossCoordinator::new_with_runtime_owner(runtime_owner);

    // No bootstrap call — registry must be absent.
    let has_registry = coordinator.actor_registry.read().await.is_some();
    assert!(
        !has_registry,
        "h8: new_with_runtime_owner alone must not produce a registry"
    );
}

// ---------------------------------------------------------------------------
// T16.6.H.9 — BossRuntimeHost is the first-class factory / host contract
// ---------------------------------------------------------------------------

/// BossRuntimeHost::build_coordinator produces a full-mode coordinator in one call.
#[tokio::test]
async fn h9_host_build_coordinator_is_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h9-build", task_manager);

    let coordinator = host.build_coordinator(&app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor,
        "h9: host.build_coordinator must set has_executor"
    );
    assert!(
        registry.has_a_callbacks,
        "h9: host.build_coordinator must set has_a_callbacks"
    );
}

/// BossRuntimeHost::bootstrap_coordinator brings an existing coordinator to full mode.
/// This is the production path when coordinator is a field of AppState.
#[tokio::test]
async fn h9_host_bootstrap_coordinator_brings_existing_to_full_mode() {
    use rust_agent::core::boss_runtime::{BossRuntimeHost, BossRuntimeOwner};
    let host = BossRuntimeHost::new();
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h9-bootstrap", task_manager);

    // Before: no registry.
    assert!(coordinator.actor_registry.read().await.is_none());

    host.bootstrap_coordinator(&coordinator, &app_state).await;

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor,
        "h9: host.bootstrap_coordinator must set has_executor"
    );
    assert!(
        registry.has_a_callbacks,
        "h9: host.bootstrap_coordinator must set has_a_callbacks"
    );
}

/// bootstrap_coordinator is idempotent — calling it twice does not replace the registry.
#[tokio::test]
async fn h9_host_bootstrap_coordinator_is_idempotent() {
    use rust_agent::core::boss_runtime::{BossRuntimeHost, BossRuntimeOwner};
    let host = BossRuntimeHost::new();
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h9-idem", task_manager);

    host.bootstrap_coordinator(&coordinator, &app_state).await;
    let ptr_first = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    host.bootstrap_coordinator(&coordinator, &app_state).await;
    let ptr_second = {
        let guard = coordinator.actor_registry.read().await;
        Arc::as_ptr(&guard.as_ref().unwrap().executor_b.state) as usize
    };

    assert_eq!(
        ptr_first, ptr_second,
        "h9: bootstrap_coordinator must be idempotent"
    );
}

// ---------------------------------------------------------------------------
// T16.6.H.10 — BossRuntimeHost::restore_or_init_coordinator completes the API triad
// ---------------------------------------------------------------------------

/// host.restore_or_init_coordinator with no existing file produces a fresh full-mode coordinator.
#[tokio::test]
async fn h10_host_restore_or_init_coordinator_fresh_is_full_mode() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let plan_path = std::env::temp_dir().join("h10_restore_fresh_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-fresh", task_manager);

    let coordinator = host
        .restore_or_init_coordinator(&plan_path, &app_state)
        .await
        .unwrap();

    let guard = coordinator.actor_registry.read().await;
    let registry = guard.as_ref().unwrap();
    assert!(
        registry.has_executor,
        "h10: restore_or_init_coordinator (fresh) must set has_executor"
    );
    assert!(
        registry.has_a_callbacks,
        "h10: restore_or_init_coordinator (fresh) must set has_a_callbacks"
    );
}

/// host.restore_or_init_coordinator uses the host's BossRuntimeOwner (not a throwaway one).
/// Verify by checking the coordinator's runtime_owner is the same Arc as the host's owner.
#[tokio::test]
async fn h10_host_restore_or_init_coordinator_uses_host_owner() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let plan_path = std::env::temp_dir().join("h10_owner_check_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-owner", task_manager);

    let coordinator = host
        .restore_or_init_coordinator(&plan_path, &app_state)
        .await
        .unwrap();

    // Direct owner identity assertion: coordinator must hold the same BossRuntimeOwner Arc as host.
    assert_eq!(
        host.owner_ptr(),
        coordinator.runtime_owner_ptr(),
        "h10: coordinator from restore_or_init_coordinator must hold host's BossRuntimeOwner"
    );
}

/// The host API triad (build / bootstrap / restore_or_init) all produce full-mode coordinators.
/// This test exercises all three in sequence to confirm the contract is uniform.
#[tokio::test]
async fn h10_host_api_triad_all_produce_full_mode() {
    use rust_agent::core::boss_runtime::{BossRuntimeHost, BossRuntimeOwner};
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-triad", task_manager);

    // build_coordinator
    let c1 = host.build_coordinator(&app_state).await;
    let g1 = c1.actor_registry.read().await;
    let r1 = g1.as_ref().unwrap();
    assert!(
        r1.has_executor && r1.has_a_callbacks,
        "h10: build_coordinator must be full-mode"
    );
    drop(g1);

    // bootstrap_coordinator
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let c2 = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    host.bootstrap_coordinator(&c2, &app_state).await;
    let g2 = c2.actor_registry.read().await;
    let r2 = g2.as_ref().unwrap();
    assert!(
        r2.has_executor && r2.has_a_callbacks,
        "h10: bootstrap_coordinator must be full-mode"
    );
    drop(g2);

    // restore_or_init_coordinator
    let plan_path = std::env::temp_dir().join("h10_triad_plan.json");
    let _ = std::fs::remove_file(&plan_path);
    let c3 = host
        .restore_or_init_coordinator(&plan_path, &app_state)
        .await
        .unwrap();
    let g3 = c3.actor_registry.read().await;
    let r3 = g3.as_ref().unwrap();
    assert!(
        r3.has_executor && r3.has_a_callbacks,
        "h10: restore_or_init_coordinator must be full-mode"
    );
}

// ---------------------------------------------------------------------------
// T16.6.H.10.1 — Direct owner identity assertion for host API triad
// ---------------------------------------------------------------------------

/// build_coordinator: coordinator holds the host's BossRuntimeOwner (direct identity check).
#[tokio::test]
async fn h10_1_build_coordinator_uses_host_owner() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-1-build", task_manager);

    let coordinator = host.build_coordinator(&app_state).await;

    assert_eq!(
        host.owner_ptr(),
        coordinator.runtime_owner_ptr(),
        "h10.1: coordinator from build_coordinator must hold host's BossRuntimeOwner"
    );
}

/// restore_or_init_coordinator: coordinator holds the host's BossRuntimeOwner (direct identity check).
/// This replaces the indirect smoke test from H.10.
#[tokio::test]
async fn h10_1_restore_or_init_coordinator_uses_host_owner_direct() {
    use rust_agent::core::boss_runtime::BossRuntimeHost;
    let host = BossRuntimeHost::new();
    let plan_path = std::env::temp_dir().join("h10_1_owner_direct_plan.json");
    let _ = std::fs::remove_file(&plan_path);

    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-h10-1-restore", task_manager);

    let coordinator = host
        .restore_or_init_coordinator(&plan_path, &app_state)
        .await
        .unwrap();

    assert_eq!(
        host.owner_ptr(),
        coordinator.runtime_owner_ptr(),
        "h10.1: coordinator from restore_or_init_coordinator must hold host's BossRuntimeOwner"
    );
}

// ---------------------------------------------------------------------------
// T22.1 — Designer A becomes a real LLM agent session
// ---------------------------------------------------------------------------

/// After ReviewFn fires, designer_a.session_id must no longer be the deterministic placeholder.
#[tokio::test]
async fn t22_1_review_fn_initializes_a_session_id() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-1-review", task_manager);

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;
    coordinator
        .ensure_actor_session("t22-1-review", BossStage::Execution)
        .await;

    // Record the deterministic placeholder before any callback fires.
    let placeholder = {
        let guard = coordinator.session.read().await;
        guard
            .as_ref()
            .map(|s| s.designer_a.session_id.clone())
            .unwrap_or_default()
    };
    assert!(
        placeholder.starts_with("boss-"),
        "pre-condition: session_id must be deterministic placeholder"
    );

    // Fire ReviewFn via A mailbox.
    {
        let guard = coordinator.actor_registry.read().await;
        if let Some(registry) = guard.as_ref() {
            let _ = registry
                .a_mailbox()
                .send(
                    rust_agent::core::boss_actor_runtime::DesignerACommand::Review {
                        step_id: 0,
                        accepted: true,
                        summary: "looks good".into(),
                        correction: None,
                    },
                )
                .await;
        }
    }
    // Give the actor loop time to process.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after = {
        let guard = coordinator.session.read().await;
        guard
            .as_ref()
            .map(|s| s.designer_a.session_id.clone())
            .unwrap_or_default()
    };
    assert_ne!(
        after, placeholder,
        "t22.1: ReviewFn must update designer_a.session_id from placeholder"
    );
    assert!(
        !after.is_empty(),
        "t22.1: designer_a.session_id must be non-empty after ReviewFn"
    );

    // Verify send_to_a_session was called with a review message.
    let dispatch_msg = coordinator
        .status
        .read()
        .await
        .last_a_dispatch_message
        .clone();
    assert!(
        dispatch_msg.is_some(),
        "t22.1: last_a_dispatch_message must be set after ReviewFn"
    );
    let msg = dispatch_msg.unwrap();
    assert!(
        msg.contains("step 0"),
        "t22.1: dispatch message must reference step id"
    );
    assert!(
        msg.contains("accepted"),
        "t22.1: dispatch message must contain verdict"
    );
}

/// After DocumentationFn fires, designer_a.session_id must no longer be the deterministic placeholder.
#[tokio::test]
async fn t22_1_doc_fn_initializes_a_session_id() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-1-doc", task_manager);

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;
    coordinator
        .ensure_actor_session("t22-1-doc", BossStage::Execution)
        .await;

    let placeholder = {
        let guard = coordinator.session.read().await;
        guard
            .as_ref()
            .map(|s| s.designer_a.session_id.clone())
            .unwrap_or_default()
    };
    assert!(
        placeholder.starts_with("boss-"),
        "pre-condition: session_id must be deterministic placeholder"
    );

    // Fire DocumentationFn via A mailbox.
    {
        let guard = coordinator.actor_registry.read().await;
        if let Some(registry) = guard.as_ref() {
            let _ = registry
                .a_mailbox()
                .send(
                    rust_agent::core::boss_actor_runtime::DesignerACommand::FinalizeDocumentation {
                        signal: "finalize".into(),
                    },
                )
                .await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after = {
        let guard = coordinator.session.read().await;
        guard
            .as_ref()
            .map(|s| s.designer_a.session_id.clone())
            .unwrap_or_default()
    };
    assert_ne!(
        after, placeholder,
        "t22.1: DocumentationFn must update designer_a.session_id from placeholder"
    );
    assert!(
        !after.is_empty(),
        "t22.1: designer_a.session_id must be non-empty after DocumentationFn"
    );

    // Verify send_to_a_session was called with a documentation signal message.
    let dispatch_msg = coordinator
        .status
        .read()
        .await
        .last_a_dispatch_message
        .clone();
    assert!(
        dispatch_msg.is_some(),
        "t22.1: last_a_dispatch_message must be set after DocumentationFn"
    );
    let msg = dispatch_msg.unwrap();
    assert!(
        msg.contains("finalize"),
        "t22.1: dispatch message must contain the documentation signal"
    );
}

/// ensure_a_session is idempotent: second call must not change the session_id.
#[tokio::test]
async fn t22_1_ensure_a_session_is_idempotent() {
    use rust_agent::core::boss_runtime::BossRuntimeOwner;
    let runtime_owner = Arc::new(BossRuntimeOwner::default());
    let coordinator = Arc::new(BossCoordinator::new_with_runtime_owner(runtime_owner));
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-1-idem", task_manager);

    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;
    coordinator
        .ensure_actor_session("t22-1-idem", BossStage::Execution)
        .await;

    // Fire DocumentationFn twice.
    for _ in 0..2 {
        let guard = coordinator.actor_registry.read().await;
        if let Some(registry) = guard.as_ref() {
            let _ = registry
                .a_mailbox()
                .send(
                    rust_agent::core::boss_actor_runtime::DesignerACommand::FinalizeDocumentation {
                        signal: "finalize".into(),
                    },
                )
                .await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Both calls should have produced the same session_id (idempotent).
    let session_id = {
        let guard = coordinator.session.read().await;
        guard
            .as_ref()
            .map(|s| s.designer_a.session_id.clone())
            .unwrap_or_default()
    };
    // The session_id must be a real task id (not the placeholder) and stable.
    assert!(
        !session_id.starts_with("boss-"),
        "t22.1: session_id must be a real task id after idempotent calls"
    );
    // The last dispatch message must be set (second call still sends to A session).
    let dispatch_msg = coordinator
        .status
        .read()
        .await
        .last_a_dispatch_message
        .clone();
    assert!(
        dispatch_msg.is_some(),
        "t22.1: last_a_dispatch_message must be set after idempotent calls"
    );
}

// ── T22.1.B: parse_a_review_decision unit tests ─────────────────────────────

#[test]
fn t22_1b_parse_a_review_decision_accept() {
    let decision = rust_agent::core::boss::BossCoordinator::parse_a_review_decision_pub(
        "ACCEPT: looks good",
        "review summary",
    );
    assert!(matches!(
        decision,
        rust_agent::core::boss_actor_runtime::ReviewDecision::Accept { .. }
    ));
}

#[test]
fn t22_1b_parse_a_review_decision_reject_with_correction() {
    let decision = rust_agent::core::boss::BossCoordinator::parse_a_review_decision_pub(
        "REJECT: step output is incomplete. CORRECTION: add error handling for the edge case",
        "review summary",
    );
    assert!(matches!(
        decision,
        rust_agent::core::boss_actor_runtime::ReviewDecision::Correct { .. }
    ));
    let rust_agent::core::boss_actor_runtime::ReviewDecision::Correct { correction, .. } = decision
    else {
        unreachable!();
    };
    assert_eq!(
        correction.as_deref(),
        Some("add error handling for the edge case"),
        "correction must be extracted after CORRECTION:"
    );
}

#[test]
fn t22_1b_parse_a_review_decision_replan_step() {
    let decision = rust_agent::core::boss::BossCoordinator::parse_a_review_decision_pub(
        "REPLAN_STEP. REASON: step mixes migration and validation and must be split",
        "review summary",
    );
    let rust_agent::core::boss_actor_runtime::ReviewDecision::ReplanStep { reason, .. } = decision
    else {
        panic!("expected ReplanStep decision");
    };
    assert_eq!(
        reason,
        "step mixes migration and validation and must be split"
    );
}

#[test]
fn t22_1b_parse_a_review_decision_default_accept_when_no_keyword() {
    let decision = rust_agent::core::boss::BossCoordinator::parse_a_review_decision_pub(
        "Looks fine to me.",
        "review summary",
    );
    assert!(matches!(
        decision,
        rust_agent::core::boss_actor_runtime::ReviewDecision::Accept { .. }
    ));
}

// ── T22.1.B: A verdict drives state machine (fallback path) ─────────────────

#[tokio::test]
async fn t22_1b_review_fn_falls_back_to_coordinator_verdict_when_a_unavailable() {
    // When A's session is not running, ask_a_session fails and build_review_fn
    // falls back to the coordinator-supplied accepted value. Assert step.status directly.
    let tmp = std::env::temp_dir().join("t22_1b_fallback_tasks");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "t22-1b-fallback-strong";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step for fallback test")]),
        "t22_1b_fallback_strong.json",
    )
    .await;
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-fallback".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    // Pre-seed designer_a.session_id with a non-running task id so ensure_a_session
    // skips the real LLM spawn, and ask_a_session fails fast (task not running).
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = "fake-a-not-running".to_string();
        }
    }

    // No fake A task running — ask_a_session will fail fast.
    // Coordinator says accepted=true → fallback must complete the step.
    coordinator
        .on_review_event(0, true, "Fallback accept", None)
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Completed,
        "fallback must use coordinator verdict (accepted=true → Completed)"
    );
    assert!(
        step.completed,
        "step.completed must be true on fallback accept"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t22_1b_review_fn_uses_a_verdict_when_a_responds_accept() {
    // A responds ACCEPT; coordinator passes accepted=false. A's verdict must win.
    let tmp = std::env::temp_dir().join("t22_1b_accept_tasks");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "t22-1b-a-accept";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step for A accept override")]),
        "t22_1b_a_accept.json",
    )
    .await;
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-accept".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let fake_a_task = task_manager.create_with_type(
        "fake designer A".to_string(),
        TaskType::LocalAgent,
        session_id.to_string(),
        InteractionSurface::Cli,
    );
    // Launch the fake A task so it's in running_owners (required for send_message).
    let aid_clone = fake_a_task.id.clone();
    task_manager.launch(&fake_a_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        drop(aid_clone);
    });
    // Pre-seed designer_a.session_id so ensure_a_session skips the real LLM spawn.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = fake_a_task.id.clone();
        }
    }
    // Append A's response after a short delay so ask_a_session's polling loop finds it.
    let tm = task_manager.clone();
    let aid = fake_a_task.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tm.append_output(&aid, "ACCEPT: step output looks good\n");
    });

    // Coordinator says accepted=false — A's ACCEPT must override to Completed.
    coordinator
        .on_review_event(0, false, "Step output looks good", None)
        .await
        .unwrap();
    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Completed,
        "A ACCEPT must complete the step even when coordinator says rejected"
    );
    assert!(
        step.completed,
        "step.completed must be true after A accepts"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t22_1b_review_fn_uses_a_verdict_when_a_responds_reject() {
    // A responds REJECT + CORRECTION; coordinator passes accepted=true. A's verdict must win.
    let tmp = std::env::temp_dir().join("t22_1b_reject_tasks");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "t22-1b-a-reject";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Step for A reject override")]),
        "t22_1b_a_reject.json",
    )
    .await;
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].worker_task_id = Some("b-task-reject".into());
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let fake_a_task = task_manager.create_with_type(
        "fake designer A".to_string(),
        TaskType::LocalAgent,
        session_id.to_string(),
        InteractionSurface::Cli,
    );
    // Launch the fake A task so it's in running_owners (required for send_message).
    let aid_clone = fake_a_task.id.clone();
    task_manager.launch(&fake_a_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        drop(aid_clone);
    });
    // Pre-seed designer_a.session_id so ensure_a_session skips the real LLM spawn.
    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.session_id = fake_a_task.id.clone();
        }
    }
    // Append A's response after a short delay so ask_a_session's polling loop finds it.
    let tm = task_manager.clone();
    let aid = fake_a_task.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tm.append_output(
            &aid,
            "REJECT: output incomplete. CORRECTION: add retry logic for transient failures\n",
        );
    });

    // Coordinator says accepted=true — A's REJECT must override to Rejected.
    coordinator
        .on_review_event(0, true, "Output incomplete", None)
        .await
        .unwrap();

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Rejected,
        "A REJECT must set Rejected status even when coordinator says accepted"
    );
    assert!(
        !step.completed,
        "step must not be completed after A rejects"
    );
    assert_eq!(
        step.attempt_count, 1,
        "attempt_count must increment on rejection"
    );
    assert_eq!(
        step.last_correction.as_deref(),
        Some("add retry logic for transient failures"),
        "A's correction must be recorded"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn r0_single_step_task_event_completed_routes_through_review_gate() {
    let tmp = std::env::temp_dir().join("r0_single_step_task_event_review_gate");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "r0-single-step-task-event";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "single-step review gate")]),
        "r0_single_step_task_event_review_gate.json",
    )
    .await;
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;
    seed_fake_a_review_session(
        &coordinator,
        task_manager.clone(),
        session_id,
        "REJECT: needs a better result. CORRECTION: add the missing artifact\n",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Running;
        plan.steps[0].worker_task_id = Some("worker-r0-task-event".into());
    }

    coordinator
        .on_task_event(&task_event(
            "worker-r0-task-event",
            0,
            TaskStatus::Completed,
        ))
        .await
        .unwrap();

    wait_for_step_status(&coordinator, 0, BossPlanStepStatus::Rejected).await;

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Rejected,
        "completed task event must not bypass A review"
    );
    assert!(!step.completed, "rejected review must keep completed=false");
    assert_eq!(step.attempt_count, 1);
    assert_eq!(
        step.last_correction.as_deref(),
        Some("add the missing artifact"),
        "A correction must be preserved"
    );

    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(tmp);
}

#[tokio::test]
async fn r0_single_step_notification_completed_routes_through_review_gate() {
    let tmp = std::env::temp_dir().join("r0_single_step_notification_review_gate");
    let task_manager = Arc::new(TaskManager::new_with_output_root(&tmp));
    let session_id = "r0-single-step-notification";
    let app_state = app_state_with_tasks(session_id, task_manager.clone());

    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "notification review gate")]),
        "r0_single_step_notification_review_gate.json",
    )
    .await;
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;
    seed_fake_a_review_session(
        &coordinator,
        task_manager.clone(),
        session_id,
        "REJECT: the notification result is not enough. CORRECTION: verify output before accepting\n",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Running;
        plan.steps[0].worker_task_id = Some("worker-r0-notify".into());
    }

    let notification = rust_agent::interaction::notification::Notification::task_update(
        session_id,
        "worker step complete",
        "worker says the step is done",
        "worker-r0-notify",
        Some("local_agent"),
        "completed",
        "await review",
        Some("implement"),
        None,
        None,
        None,
        Some(0),
        "",
        None,
    );

    coordinator.on_notification(&notification).await.unwrap();

    wait_for_step_status(&coordinator, 0, BossPlanStepStatus::Rejected).await;

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(
        step.status,
        BossPlanStepStatus::Rejected,
        "completed notification must not bypass A review"
    );
    assert!(!step.completed, "rejected review must keep completed=false");
    assert_eq!(
        step.last_correction.as_deref(),
        Some("verify output before accepting"),
        "notification path must preserve A's correction"
    );

    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(tmp);
}

// ---------------------------------------------------------------------------
// T22.2 — Executor B becomes a real LLM agent session
// ---------------------------------------------------------------------------

/// After the first DispatchStep fires exec_fn, executor_b.session_id must be
/// a real task id (not the deterministic placeholder "boss-{plan_id}-b").
#[tokio::test]
async fn t22_2_b_session_id_is_non_placeholder_after_first_dispatch() {
    let plan_id = "t22-2-first-dispatch";
    let plan_path = std::env::temp_dir().join("t22_2_first_dispatch.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-2-first", task_manager.clone());

    let placeholder = format!("boss-{plan_id}-b");
    assert_eq!(
        coordinator.b_session_id().await,
        placeholder,
        "session_id must start as placeholder"
    );

    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let session_id_after = coordinator.b_session_id().await;
    assert_ne!(
        session_id_after, placeholder,
        "session_id must be non-placeholder after first dispatch"
    );
    assert!(
        !session_id_after.is_empty(),
        "session_id must not be empty after first dispatch"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// Two consecutive DispatchStep/ContinueStep calls must reuse the same B session id
/// when B's task is still running between dispatches.
#[tokio::test]
async fn t22_2_two_dispatches_reuse_same_b_session_id() {
    let plan_id = "t22-2-reuse-session";
    let plan_path = std::env::temp_dir().join("t22_2_reuse_session.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero"), boss_step(1, "step one")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-2-reuse", task_manager.clone());

    // Create a fake B task that stays Running (simulates a live B session).
    let fake_b_task = task_manager.create_with_type(
        "fake executor B",
        TaskType::LocalAgent,
        "session-t22-2-reuse",
        InteractionSurface::Cli,
    );
    task_manager.launch(&fake_b_task.id, "", async move {
        // Keep running until test ends.
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    });
    let b_task_id = fake_b_task.id.clone();

    // Pre-seed B's session with the running task id.
    coordinator.record_b_session_id_pub(&b_task_id).await;

    // First dispatch — B is already running, so ContinueStep fires.
    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let session_id_after_first = coordinator.b_session_id().await;
    assert_eq!(
        session_id_after_first, b_task_id,
        "first dispatch must keep the pre-seeded B session id"
    );

    // Advance plan state so step 0 is complete.
    {
        let mut guard = coordinator.plan.write().await;
        if let Some(p) = guard.as_mut() {
            p.steps[0].completed = true;
            p.steps[0].status = BossPlanStepStatus::Completed;
        }
    }

    // Second dispatch — B is still running, must reuse same session.
    coordinator.advance_plan(&app_state).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let session_id_after_second = coordinator.b_session_id().await;
    assert_eq!(
        session_id_after_first, session_id_after_second,
        "second dispatch must reuse the same B session id when B is still running"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// record_b_session_id_pub writes task_id to executor_b.session_id and task_id fields.
#[tokio::test]
async fn t22_2_record_b_session_id_writes_back_to_session() {
    let plan_path = std::env::temp_dir().join("t22_2_record_b.json");
    let plan = BossPlan {
        plan_id: "t22-2-record".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    coordinator
        .record_b_session_id_pub("real-task-abc123")
        .await;

    assert_eq!(coordinator.b_session_id().await, "real-task-abc123");
    assert_eq!(
        coordinator.b_task_id().await.as_deref(),
        Some("real-task-abc123")
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// When task_manager is absent, advance_plan must not panic.
#[tokio::test]
async fn t22_2_b_session_fallback_when_task_manager_absent() {
    let plan_id = "t22-2-no-tm";
    let plan_path = std::env::temp_dir().join("t22_2_no_tm.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    let permission_context = rust_agent::state::permission_context::ToolPermissionContext::new(
        rust_agent::state::permission_context::PermissionMode::Default,
    )
    .with_active_session_id("session-t22-2-no-tm")
    .with_active_surface(InteractionSurface::Cli);
    let app_state = Arc::new(AppState {
        surface: InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        client_type: rust_agent::bootstrap::ClientType::Cli,
        session_source: rust_agent::bootstrap::SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context,
        command_registry: None,
        runtime_tool_registry: Some(Arc::new(RwLock::new(
            rust_agent::tool::registry::ToolRegistry::new(),
        ))),
        skill_registry: None,
        mcp_runtime: None,
        plugin_load_result: None,
        cost_tracker: rust_agent::cost::tracker::CostTracker::default(),
        service_observability_tracker:
            rust_agent::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: rust_agent::interaction::dispatcher::NotificationDispatcher::new(
            rust_agent::interaction::telegram::gateway::TelegramGateway::default(),
        ),
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
        active_session_id: "session-t22-2-no-tm".into(),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        subagent_limiter: None,
        boss_coordinator: None,
        remote_actor_store: None,
    });

    let result = coordinator.advance_plan(&app_state).await;
    // advance_plan requires a task_manager to dispatch B — it returns a clear error when absent.
    let err = result.expect_err("advance_plan must fail when task_manager is absent");
    assert!(
        err.to_string().contains("task manager not configured"),
        "error must name the missing task manager: {err}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T22.3 — Documentation B reviewer + Execution B self-organizes sub-agents
// ---------------------------------------------------------------------------

/// T22.3.1: B reviewer receives ReviewSpec and returns real feedback via spec_review_fn.
#[tokio::test]
async fn t22_3_documentation_b_reviewer_returns_feedback() {
    use rust_agent::core::boss_actor_runtime::{BossActorEvent, ExecutorBCommand};

    let spec_review_fn: SpecReviewFn = Arc::new(|spec: String| {
        Box::pin(async move {
            Ok(format!(
                "FEEDBACK: spec '{}' is missing error handling",
                spec
            ))
        })
    });

    let runtime = ExecutorBRuntime::spawn_with_callbacks(None, Some(spec_review_fn));
    let event = runtime
        .mailbox
        .request(ExecutorBCommand::ReviewSpec {
            spec: "implement login flow".to_string(),
        })
        .await
        .expect("ReviewSpec must succeed");

    match event {
        BossActorEvent::SpecReviewed { feedback } => {
            assert!(
                feedback.contains("FEEDBACK:"),
                "B must return FEEDBACK: prefix, got: {feedback}"
            );
            assert!(
                feedback.contains("missing error handling"),
                "B must include spec content, got: {feedback}"
            );
        }
        other => panic!("expected SpecReviewed, got {other:?}"),
    }
}

/// T22.3.1: finalize_documentation_loop uses B's ReviewSpec feedback when review_feedback is empty.
#[tokio::test]
async fn t22_3_finalize_documentation_loop_uses_b_reviewer_feedback() {
    let plan_path = std::env::temp_dir().join("t22_3_doc_b_feedback.json");
    let plan = BossPlan {
        plan_id: "t22-3-doc-b".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    let spec_review_fn: SpecReviewFn = Arc::new(|_spec: String| {
        Box::pin(async move { Ok("FEEDBACK: needs more detail on auth flow".to_string()) })
    });
    let exec_fn: ExecutionFn = Arc::new(|payload: String| Box::pin(async move { Ok(payload) }));
    let registry = BossActorRegistry {
        designer_a: DesignerARuntime::spawn(),
        executor_b: ExecutorBRuntime::spawn_with_callbacks(Some(exec_fn), Some(spec_review_fn)),
        has_executor: true,
        has_a_callbacks: false,
    };
    {
        let mut guard = coordinator.actor_registry.write().await;
        *guard = Some(registry);
    }

    coordinator
        .finalize_documentation_loop(
            "draft spec: implement login",
            "",
            "revised based on B feedback",
            "final spec",
            "pseudo code",
        )
        .await
        .unwrap();

    let plan_guard = coordinator.plan.read().await;
    let plan = plan_guard.as_ref().unwrap();
    assert_eq!(
        plan.review_feedback.as_deref(),
        Some("FEEDBACK: needs more detail on auth flow"),
        "B's feedback must be stored as review_feedback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T22.3.2: Execution B's task (spawned with ExecutorB policy, depth 0) can spawn a child agent.
#[tokio::test]
async fn t22_3_execution_b_session_can_spawn_child_agent() {
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy {
            actor_role: BossActorRole::ExecutorB,
            lineage_depth: 0,
            phase: BossStage::Execution,
        });

    let call = ToolCall::new(
        "Agent",
        serde_json::json!({
            "task": "implement step 0",
            "session_id": "b-child-session"
        })
        .to_string(),
    );

    let result = AgentTool.invoke(&call, &permissions).await;
    assert!(
        result.is_ok(),
        "ExecutorB at depth 0 must be allowed to spawn a child agent: {:?}",
        result
    );
}

/// T22.3.2: B's child (ImplementChild, depth 1) cannot spawn a grandchild — policy holds.
#[tokio::test]
async fn t22_3_b_child_cannot_spawn_grandchild_agent() {
    let tasks = Arc::new(TaskManager::default());
    let permissions = ToolPermissionContext::new(PermissionMode::Default)
        .with_task_manager(tasks)
        .with_boss_actor_policy(BossActorPolicy {
            actor_role: BossActorRole::ImplementChild,
            lineage_depth: 1,
            phase: BossStage::Execution,
        });

    let call = ToolCall::new(
        "Agent",
        serde_json::json!({
            "prompt": "do something",
            "session_id": "grandchild-session"
        })
        .to_string(),
    );

    let err = AgentTool
        .invoke(&call, &permissions)
        .await
        .expect_err("ImplementChild at depth 1 must not spawn grandchild");

    assert!(
        err.to_string().contains("boss spawn policy"),
        "error must mention boss spawn policy, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// T22.3 production path evidence
// ---------------------------------------------------------------------------

/// T22.3.1 production path: finalize_documentation_loop walks the real
/// build_spec_review_fn → ensure_b_session (skipped, pre-seeded) → ask_b_session
/// → ReviewSpec mailbox → SpecReviewed feedback stored in plan.
///
/// B's session is a fake Running task that appends output when it receives a message.
#[tokio::test]
async fn t22_3_production_path_doc_b_reviewer_via_ask_b_session() {
    let plan_id = "t22-3-prod-doc";
    let plan_path = std::env::temp_dir().join("t22_3_prod_doc.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t22_3_prod_doc_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t22-3-prod-doc", task_manager.clone());

    // Create a fake B task that stays Running and responds to send_message.
    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t22-3-prod-doc",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        // Respond to any incoming message by appending output.
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for msg in messages {
                let feedback =
                    format!("FEEDBACK: B reviewed spec — {msg} needs auth error handling");
                tm_for_b.append_output(&b_id_for_loop, &feedback);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });

    // Pre-seed B's session_id so ensure_b_session skips spawning.
    coordinator.record_b_session_id_pub(&b_task_id).await;

    // Wire the production callbacks (build_spec_review_fn uses ask_b_session).
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    // finalize with empty review_feedback — B must supply it via ask_b_session.
    coordinator
        .finalize_documentation_loop(
            "implement login with OAuth",
            "",
            "revised per B feedback",
            "final spec",
            "pseudo code",
        )
        .await
        .unwrap();

    let plan_guard = coordinator.plan.read().await;
    let stored_feedback = plan_guard
        .as_ref()
        .unwrap()
        .review_feedback
        .clone()
        .unwrap_or_default();
    assert!(
        stored_feedback.contains("FEEDBACK:"),
        "B's real feedback must be stored, got: {stored_feedback}"
    );
    assert!(
        stored_feedback.contains("auth error handling"),
        "B's feedback must reference the spec content, got: {stored_feedback}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T22.3.2 production path: advance_plan walks the real build_exec_fn →
/// invoke_agent_tool_with_task_id → AgentTool.invoke → creates a child task
/// in the task manager. Verifies the task manager has a new task after dispatch.
#[tokio::test]
async fn t22_3_production_path_exec_b_creates_child_task_via_agent_tool() {
    let plan_id = "t22-3-prod-exec";
    let plan_path = std::env::temp_dir().join("t22_3_prod_exec.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "implement auth module")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let app_state = app_state_with_tasks("session-t22-3-prod-exec", task_manager.clone());

    // Wire the production exec_fn (build_exec_fn → invoke_agent_tool_with_task_id).
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    let tasks_before = task_manager.list().len();

    // advance_plan → DispatchStep → exec_fn → AgentTool.invoke → new child task.
    coordinator.advance_plan(&app_state).await.unwrap();
    // Give exec_fn time to fire asynchronously.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let tasks_after = task_manager.list().len();
    assert!(
        tasks_after > tasks_before,
        "AgentTool must have created at least one child task (before={tasks_before}, after={tasks_after})"
    );

    // The new task must have B's actor role label.
    let new_tasks: Vec<_> = task_manager
        .list()
        .into_iter()
        .filter(|t| t.boss_actor_id.is_some())
        .collect();
    assert!(
        !new_tasks.is_empty(),
        "at least one task must have a boss_actor_id set (B's child)"
    );

    // B's session_id must be non-placeholder after exec_fn fires.
    let b_session = coordinator.b_session_id().await;
    let placeholder = format!("boss-{plan_id}-b");
    assert_ne!(
        b_session, placeholder,
        "B session_id must be real after exec_fn fires"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// --- T22.4: /stop真实抢占 LLM 推理态 ---

/// T22.4.A: A's LLM session task is Running when /stop fires.
/// After stop(), A's task must be Killed.
#[tokio::test]
async fn t22_4_stop_aborts_a_session_while_waiting_for_llm() {
    let plan_path = std::env::temp_dir().join("t22_4_stop_a.json");
    let plan = BossPlan {
        plan_id: "t22-4-stop-a".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step for A abort test")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let fake_a_task = task_manager.create_with_type(
        "fake designer A LLM session".to_string(),
        TaskType::LocalAgent,
        "t22-4-stop-a-session".to_string(),
        InteractionSurface::Cli,
    );
    let aid = fake_a_task.id.clone();
    task_manager.launch(&fake_a_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        drop(aid);
    });

    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.task_id = Some(fake_a_task.id.clone());
        }
    }

    assert_eq!(
        task_manager.status(&fake_a_task.id),
        Some(TaskStatus::Running),
        "fake A task must be Running before stop"
    );

    coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "t22-4-stop-a-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.status(&fake_a_task.id),
        Some(TaskStatus::Killed),
        "A's LLM session task must be Killed after stop()"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T22.4.B: B's LLM session task is Running when /stop fires.
/// After stop(), B's task must be Killed.
#[tokio::test]
async fn t22_4_stop_aborts_b_session_while_waiting_for_llm() {
    let plan_path = std::env::temp_dir().join("t22_4_stop_b.json");
    let plan = BossPlan {
        plan_id: "t22-4-stop-b".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step for B abort test")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let fake_b_task = task_manager.create_with_type(
        "fake executor B LLM session".to_string(),
        TaskType::LocalAgent,
        "t22-4-stop-b-session".to_string(),
        InteractionSurface::Cli,
    );
    let bid = fake_b_task.id.clone();
    task_manager.launch(&fake_b_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        drop(bid);
    });

    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.executor_b.task_id = Some(fake_b_task.id.clone());
        }
    }

    assert_eq!(
        task_manager.status(&fake_b_task.id),
        Some(TaskStatus::Running),
        "fake B task must be Running before stop"
    );

    coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "t22-4-stop-b-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.status(&fake_b_task.id),
        Some(TaskStatus::Killed),
        "B's LLM session task must be Killed after stop()"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T22.4.C: Both A and B have Running LLM sessions when /stop fires.
/// Both must be Killed — abort_a_b_sessions handles both in one pass.
#[tokio::test]
async fn t22_4_stop_aborts_both_a_and_b_sessions() {
    let plan_path = std::env::temp_dir().join("t22_4_stop_both.json");
    let plan = BossPlan {
        plan_id: "t22-4-stop-both".into(),
        accepted_by_user: true,
        auto_sequence: true,
        steps: vec![boss_step(0, "step for A+B abort test")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let task_manager = Arc::new(TaskManager::default());
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());

    let fake_a_task = task_manager.create_with_type(
        "fake A LLM".to_string(),
        TaskType::LocalAgent,
        "t22-4-both-session".to_string(),
        InteractionSurface::Cli,
    );
    let fake_b_task = task_manager.create_with_type(
        "fake B LLM".to_string(),
        TaskType::LocalAgent,
        "t22-4-both-session".to_string(),
        InteractionSurface::Cli,
    );

    let aid = fake_a_task.id.clone();
    task_manager.launch(&fake_a_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        drop(aid);
    });
    let bid = fake_b_task.id.clone();
    task_manager.launch(&fake_b_task.id, "", async move {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        drop(bid);
    });

    {
        let mut guard = coordinator.session.write().await;
        if let Some(s) = guard.as_mut() {
            s.designer_a.task_id = Some(fake_a_task.id.clone());
            s.executor_b.task_id = Some(fake_b_task.id.clone());
        }
    }

    coordinator
        .handle_control_request(
            BossControlRequest::Stop {
                requester_session_id: "t22-4-both-session".into(),
                deadline_ms: 0,
            },
            &task_manager,
            &dispatcher,
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.status(&fake_a_task.id),
        Some(TaskStatus::Killed),
        "A's LLM session must be Killed"
    );
    assert_eq!(
        task_manager.status(&fake_b_task.id),
        Some(TaskStatus::Killed),
        "B's LLM session must be Killed"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T23: A spec 起草真实化
// ---------------------------------------------------------------------------

/// T23.1: draft_spec="" triggers draft_spec_with_a; A's response is written to plan.draft_spec.
#[tokio::test]
async fn t23_draft_spec_empty_triggers_a_draft() {
    let plan_id = "t23-draft-empty";
    let plan_path = std::env::temp_dir().join("t23_draft_empty.json");
    let plan = BossPlan {
        plan_id: plan_id.into(),
        task_description: "implement OAuth login".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t23_draft_empty_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t23-draft-empty", task_manager.clone());

    let fake_a = task_manager.create_with_type(
        "fake A session",
        TaskType::LocalAgent,
        "session-t23-draft-empty",
        InteractionSurface::Cli,
    );
    let a_task_id = fake_a.id.clone();
    let tm_for_a = task_manager.clone();
    let a_id_for_loop = a_task_id.clone();
    task_manager.launch(&a_task_id, "", async move {
        loop {
            let messages = tm_for_a.drain_mailbox(&a_id_for_loop);
            for _msg in messages {
                tm_for_a.append_output(
                    &a_id_for_loop,
                    "Spec: OAuth login using PKCE flow. Objectives: secure token exchange. Acceptance: token stored in keychain.",
                );
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });

    coordinator.record_a_session_id_pub(&a_task_id).await;
    {
        let mut guard = coordinator.auto_advance_app_state.write().await;
        *guard = Some(app_state.clone());
    }
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    coordinator
        .finalize_documentation_loop("", "", "no revision needed", "final spec", "pseudo code")
        .await
        .unwrap();

    let plan_guard = coordinator.plan.read().await;
    let stored_draft = plan_guard
        .as_ref()
        .unwrap()
        .draft_spec
        .clone()
        .unwrap_or_default();
    assert!(
        !stored_draft.is_empty(),
        "plan.draft_spec must be non-empty after A drafts it"
    );
    assert!(
        stored_draft.contains("Spec:") || stored_draft.contains("OAuth"),
        "plan.draft_spec must contain A's response, got: {stored_draft}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T23.2: draft_spec non-empty skips draft_spec_with_a; existing value is preserved.
#[tokio::test]
async fn t23_draft_spec_nonempty_skips_a_draft() {
    let plan_path = std::env::temp_dir().join("t23_draft_nonempty.json");
    let plan = BossPlan {
        plan_id: "t23-draft-nonempty".into(),
        task_description: "implement OAuth login".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();

    // No app_state wired — if A were called it would fail.
    coordinator
        .finalize_documentation_loop(
            "pre-existing spec content",
            "LGTM",
            "no revision",
            "final spec",
            "pseudo code",
        )
        .await
        .unwrap();

    let plan_guard = coordinator.plan.read().await;
    let stored_draft = plan_guard
        .as_ref()
        .unwrap()
        .draft_spec
        .clone()
        .unwrap_or_default();
    assert_eq!(
        stored_draft, "pre-existing spec content",
        "plan.draft_spec must preserve the caller-supplied value"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T23.3: draft_spec="" with no app_state returns Err (explicit error contract).
#[tokio::test]
async fn t23_draft_spec_with_a_unavailable_returns_error() {
    let plan_path = std::env::temp_dir().join("t23_draft_no_app.json");
    let plan = BossPlan {
        plan_id: "t23-draft-no-app".into(),
        task_description: "implement OAuth login".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    // auto_advance_app_state is None (default).

    let result = coordinator
        .finalize_documentation_loop("", "", "no revision", "final spec", "pseudo code")
        .await;

    assert!(
        result.is_err(),
        "finalize_documentation_loop must return Err when draft_spec is empty and no app_state"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("app_state") || msg.contains("A session"),
        "error message must mention app_state or A session, got: {msg}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T23.4 production path: draft_spec_with_a walks ensure_a_session (pre-seeded) → ask_a_session.
#[tokio::test]
async fn t23_production_path_a_draft_via_ask_a_session() {
    let plan_path = std::env::temp_dir().join("t23_prod_draft.json");
    let plan = BossPlan {
        plan_id: "t23-prod-draft".into(),
        task_description: "build a REST API for user management".into(),
        accepted_by_user: false,
        auto_sequence: false,
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t23_prod_draft_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t23-prod-draft", task_manager.clone());

    let fake_a = task_manager.create_with_type(
        "fake A LLM session",
        TaskType::LocalAgent,
        "session-t23-prod-draft",
        InteractionSurface::Cli,
    );
    let a_task_id = fake_a.id.clone();
    let tm_for_a = task_manager.clone();
    let a_id_for_loop = a_task_id.clone();
    task_manager.launch(&a_task_id, "", async move {
        loop {
            let messages = tm_for_a.drain_mailbox(&a_id_for_loop);
            for _msg in messages {
                tm_for_a.append_output(
                    &a_id_for_loop,
                    "REST API spec: CRUD endpoints for /users. Auth via JWT. Acceptance: all endpoints return 200 on valid input.",
                );
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });

    coordinator.record_a_session_id_pub(&a_task_id).await;

    let draft = coordinator
        .draft_spec_with_a(&app_state, "build a REST API for user management")
        .await
        .unwrap();

    assert!(
        !draft.is_empty(),
        "draft_spec_with_a must return non-empty spec"
    );
    assert!(
        draft.contains("REST API") || draft.contains("spec"),
        "draft must contain A's response, got: {draft}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ---------------------------------------------------------------------------
// T24: A/B session 跨 restart 恢复
// ---------------------------------------------------------------------------

/// T24.1: save_plan_with_session embeds A/B task_id into plan.session_snapshot.
#[tokio::test]
async fn t24_session_snapshot_persisted_on_save_plan() {
    let plan_path = std::env::temp_dir().join("t24_snapshot_persist.json");
    let plan = BossPlan {
        plan_id: "t24-persist".into(),
        task_description: "test session persistence".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    coordinator.record_a_session_id_pub("real-a-task-001").await;
    coordinator.record_b_session_id_pub("real-b-task-002").await;

    coordinator
        .finalize_documentation_loop(
            "some spec",
            "LGTM",
            "no revision",
            "final spec",
            "pseudo code",
        )
        .await
        .unwrap();

    let loaded = load_plan(&plan_path).await.unwrap();
    let snap = loaded
        .session_snapshot
        .expect("session_snapshot must be present after save");
    assert_eq!(snap.designer_a.task_id.as_deref(), Some("real-a-task-001"));
    assert_eq!(snap.executor_b.task_id.as_deref(), Some("real-b-task-002"));

    let _ = std::fs::remove_file(&plan_path);
}

/// T24.2: restore_or_init uses persisted session_snapshot instead of fresh from_plan_id.
#[tokio::test]
async fn t24_restore_uses_persisted_session_snapshot() {
    let plan_path = std::env::temp_dir().join("t24_restore_snapshot.json");
    let plan = BossPlan {
        plan_id: "t24-restore".into(),
        task_description: "test restore".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let c1 = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    c1.record_a_session_id_pub("a-task-persist-001").await;
    c1.record_b_session_id_pub("b-task-persist-002").await;
    c1.finalize_documentation_loop("spec", "LGTM", "no revision", "final spec", "pseudo")
        .await
        .unwrap();

    let c2 = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let session = c2.session.read().await;
    let s = session
        .as_ref()
        .expect("session must be present after restore");
    assert_eq!(
        s.designer_a.task_id.as_deref(),
        Some("a-task-persist-001"),
        "A task_id must survive restart"
    );
    assert_eq!(
        s.executor_b.task_id.as_deref(),
        Some("b-task-persist-002"),
        "B task_id must survive restart"
    );
    assert_eq!(
        s.designer_a.session_id, "a-task-persist-001",
        "A session_id must survive restart"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T24.3: Old plan file without session_snapshot restores cleanly (fallback to from_plan_id).
#[tokio::test]
async fn t24_restore_fallback_when_no_snapshot() {
    let plan_path = std::env::temp_dir().join("t24_no_snapshot.json");
    let raw = r#"{
        "plan_id": "t24-no-snap",
        "task_description": "old plan",
        "document_spec": "",
        "pseudo_code": "",
        "steps": [],
        "accepted_by_user": false,
        "auto_sequence": false
    }"#;
    tokio::fs::write(&plan_path, raw).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let session = coordinator.session.read().await;
    let s = session
        .as_ref()
        .expect("session must be present after restore");
    assert_eq!(
        s.designer_a.session_id, "boss-t24-no-snap-a",
        "fallback session_id must be deterministic placeholder"
    );
    assert_eq!(s.executor_b.session_id, "boss-t24-no-snap-b");
    assert!(
        s.designer_a.task_id.is_none(),
        "task_id must be None on fallback"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T24.4: Stale task_id in restored snapshot does not panic; liveness check is caller's responsibility.
#[tokio::test]
async fn t24_stale_task_id_does_not_panic_on_restore() {
    let plan_path = std::env::temp_dir().join("t24_stale_task.json");
    let plan = BossPlan {
        plan_id: "t24-stale".into(),
        task_description: "stale task test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let c1 = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    c1.record_a_session_id_pub("stale-task-id-does-not-exist")
        .await;
    c1.finalize_documentation_loop("spec", "LGTM", "no revision", "final spec", "pseudo")
        .await
        .unwrap();

    // Restore: stale task_id is present — no panic expected.
    let c2 = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let session = c2.session.read().await;
    let s = session.as_ref().unwrap();
    assert_eq!(
        s.designer_a.task_id.as_deref(),
        Some("stale-task-id-does-not-exist"),
        "stale task_id must be restored without panic"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T25: B session context window management ─────────────────────────────────

/// T25.1: Payload below threshold is returned unchanged.
#[test]
fn t25_no_trim_when_payload_below_threshold() {
    let short = "hello world".to_string();
    let result = trim_context_payload(&short, B_CONTEXT_TRIM_THRESHOLD, B_CONTEXT_KEEP_CHARS);
    assert_eq!(result, short);
}

/// T25.2: Payload above threshold is trimmed to at most keep_chars + notice line.
#[test]
fn t25_trim_triggered_when_payload_exceeds_threshold() {
    let threshold = 100usize;
    let keep = 40usize;
    let payload = "x".repeat(200);
    let result = trim_context_payload(&payload, threshold, keep);
    assert!(
        result.len() < payload.len(),
        "trimmed result should be shorter"
    );
    let lines: Vec<&str> = result.splitn(2, '\n').collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[1].len() <= keep);
}

/// T25.3: Trim notice is inserted at the head with the correct format.
#[test]
fn t25_trim_notice_inserted_at_head() {
    let threshold = 50usize;
    let keep = 20usize;
    let payload = "a".repeat(100);
    let result = trim_context_payload(&payload, threshold, keep);
    let first_line = result.lines().next().unwrap_or("");
    assert!(
        first_line.starts_with("[trimmed earlier context:")
            && first_line.contains("chars omitted]"),
        "notice line must match fixed format, got: {first_line}"
    );
}

/// T25.4: The most recent `keep_chars` characters are preserved verbatim.
#[test]
fn t25_recent_content_preserved_after_trim() {
    let threshold = 50usize;
    let keep = 20usize;
    let payload = format!("{}{}", "old_content_".repeat(10), "RECENT_TAIL_END_HERE");
    let result = trim_context_payload(&payload, threshold, keep);
    assert!(
        result.contains("RECENT_TAIL_END_HERE"),
        "recent tail must be present in trimmed result"
    );
}

/// T25.5: trim_context_payload does not modify BossPlan or session_snapshot.
#[tokio::test]
async fn t25_trim_does_not_persist_to_plan_or_snapshot() {
    let plan_path = std::env::temp_dir().join("t25_no_persist.json");
    let plan = BossPlan {
        plan_id: "t25-no-persist".into(),
        task_description: "trim persistence test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let large_payload = "context_data_".repeat(10_000);
    let _trimmed = trim_context_payload(
        &large_payload,
        B_CONTEXT_TRIM_THRESHOLD,
        B_CONTEXT_KEEP_CHARS,
    );

    let reloaded = load_plan(&plan_path).await.unwrap();
    assert_eq!(reloaded.plan_id, "t25-no-persist");
    assert!(
        reloaded.session_snapshot.is_none(),
        "session_snapshot must not be written by trim"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T25.2: B session LLM summarize ───────────────────────────────────────────

/// T25.2.1: assemble_summarized_payload produces the correct format.
#[test]
fn t25_2_summary_replaces_old_context_format() {
    let result = assemble_summarized_payload("SUMMARY_TEXT", "recent tail content");
    assert!(
        result.starts_with("[summary: SUMMARY_TEXT]"),
        "must start with summary notice"
    );
    assert!(
        result.contains("recent tail content"),
        "must contain recent tail"
    );
}

/// T25.2.2: Recent tail is preserved verbatim in the assembled payload.
#[test]
fn t25_2_summary_result_contains_recent_tail() {
    let recent = "RECENT_TAIL_END_HERE";
    let result = assemble_summarized_payload("any summary", recent);
    let lines: Vec<&str> = result.splitn(2, '\n').collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(
        lines[1], recent,
        "second line must be the exact recent tail"
    );
}

/// T25.2.3: When A is unavailable (no A session seeded), ask_b_session falls back to trim.
/// We verify the fallback contract by checking trim_context_payload directly on the same input,
/// since we cannot call ask_b_session without a live B task.
#[test]
fn t25_2_fallback_to_trim_when_a_unavailable() {
    let threshold = 100usize;
    let keep = 40usize;
    let payload = "x".repeat(200);
    // Simulate fallback: A unavailable → trim_context_payload is called.
    let result = trim_context_payload(&payload, threshold, keep);
    assert!(
        result.starts_with("[trimmed earlier context:"),
        "fallback must produce trim notice, got: {result}"
    );
}

/// T25.2.4: Payload below threshold does not trigger summarize or trim.
#[test]
fn t25_2_no_summarize_when_payload_below_threshold() {
    let short = "short payload".to_string();
    // trim_context_payload is the gate — below threshold returns unchanged.
    let result = trim_context_payload(&short, B_CONTEXT_TRIM_THRESHOLD, B_CONTEXT_KEEP_CHARS);
    assert_eq!(
        result, short,
        "payload below threshold must be returned unchanged"
    );
}

/// T25.2.5: summarize path does not persist to BossPlan or session_snapshot.
#[tokio::test]
async fn t25_2_summarize_does_not_persist_to_plan_or_snapshot() {
    let plan_path = std::env::temp_dir().join("t25_2_no_persist.json");
    let plan = BossPlan {
        plan_id: "t25-2-no-persist".into(),
        task_description: "summarize persistence test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    // Simulate the summarize assembly — plan on disk must be unaffected.
    let _assembled = assemble_summarized_payload("SUMMARY", "recent tail");

    let reloaded = load_plan(&plan_path).await.unwrap();
    assert_eq!(reloaded.plan_id, "t25-2-no-persist");
    assert!(
        reloaded.session_snapshot.is_none(),
        "session_snapshot must not be written by summarize"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T25.2.6 production path: ask_b_session with oversized payload.
/// T26.6 changed the summarize path to stateless (no A session actor).
/// In test environments without active_model_runtime, stateless summarize fails
/// and falls back to trim — outbound message starts with "[trimmed earlier context:".
#[tokio::test]
async fn t25_2_production_path_summarize_success_via_ask_b_session() {
    let plan_path = std::env::temp_dir().join("t25_2_prod_summarize.json");
    let plan = BossPlan {
        plan_id: "t25-2-prod-summarize".into(),
        task_description: "summarize production path test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t25_2_prod_summarize_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t25-2-prod-summarize", task_manager.clone());

    // No A session needed — T26.6 uses stateless path; no active_model_runtime → fallback to trim.

    // Fake B session: responds to any message so ask_b_session doesn't time out.
    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t25-2-prod-summarize",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    // Build an oversized payload (> B_CONTEXT_TRIM_THRESHOLD).
    let oversized = "context_data_".repeat(B_CONTEXT_TRIM_THRESHOLD / 12 + 1);
    assert!(
        oversized.len() > B_CONTEXT_TRIM_THRESHOLD,
        "payload must exceed threshold for this test"
    );

    let _ = coordinator.ask_b_session_pub(&app_state, oversized).await;

    // T26.6: stateless summarize has no active_model_runtime in test → fallback to trim.
    let sent = coordinator
        .status
        .read()
        .await
        .last_b_ask_message
        .clone()
        .unwrap_or_default();
    assert!(
        sent.starts_with("[trimmed earlier context:"),
        "T26.6 stateless path: no active_model_runtime → fallback trim, got: {sent:.80}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T25.2.7 production path: ask_b_session with oversized payload + no active_model_runtime
/// → stateless summarize fails → fallback to trim → outbound message starts with "[trimmed earlier context:".
#[tokio::test]
async fn t25_2_production_path_fallback_to_trim_when_a_unavailable() {
    let plan_path = std::env::temp_dir().join("t25_2_prod_fallback.json");
    let plan = BossPlan {
        plan_id: "t25-2-prod-fallback".into(),
        task_description: "fallback production path test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t25_2_prod_fallback_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t25-2-prod-fallback", task_manager.clone());

    // No A session seeded — stateless summarize has no active_model_runtime → fallback to trim.

    // Fake B session: responds so ask_b_session doesn't time out.
    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t25-2-prod-fallback",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    let oversized = "context_data_".repeat(B_CONTEXT_TRIM_THRESHOLD / 12 + 1);
    assert!(oversized.len() > B_CONTEXT_TRIM_THRESHOLD);

    let _ = coordinator.ask_b_session_pub(&app_state, oversized).await;

    let sent = coordinator
        .status
        .read()
        .await
        .last_b_ask_message
        .clone()
        .unwrap_or_default();
    assert!(
        sent.starts_with("[trimmed earlier context:"),
        "fallback path: outbound message must start with '[trimmed earlier context:', got: {sent:.80}"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T26.1: PromptSegment model + fingerprint ─────────────────────────────────

/// T26.1.1: Same kind + content → identical fingerprint (stability).
#[test]
fn t26_1_same_content_produces_stable_fingerprint() {
    let a = PromptSegment::new("sys", PromptSegmentKind::StaticSystem, "hello world");
    let b = PromptSegment::new("sys", PromptSegmentKind::StaticSystem, "hello world");
    assert_eq!(a.fingerprint, b.fingerprint);
}

/// T26.1.2: Content change → fingerprint changes.
#[test]
fn t26_1_content_change_changes_fingerprint() {
    let a = PromptSegment::new("sys", PromptSegmentKind::StaticSystem, "hello world");
    let b = PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "hello world CHANGED",
    );
    assert_ne!(a.fingerprint, b.fingerprint);
}

/// T26.1.3: Kind change → fingerprint changes even with identical content.
#[test]
fn t26_1_kind_change_changes_fingerprint() {
    let a = PromptSegment::new("seg", PromptSegmentKind::StaticSystem, "same content");
    let b = PromptSegment::new("seg", PromptSegmentKind::StateFrame, "same content");
    assert_ne!(a.fingerprint, b.fingerprint);
}

/// T26.1.4: Dynamic segment does not affect stable prefix fingerprint.
#[test]
fn t26_1_dynamic_segment_excluded_from_stable_prefix_fingerprint() {
    let mut assembly_static_only = PromptAssembly::new();
    assembly_static_only.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));

    let mut assembly_with_dynamic = PromptAssembly::new();
    assembly_with_dynamic.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    assembly_with_dynamic.push(PromptSegment::new(
        "sf",
        PromptSegmentKind::StateFrame,
        "dynamic state",
    ));

    assert_eq!(
        assembly_static_only.stable_prefix_fingerprint(),
        assembly_with_dynamic.stable_prefix_fingerprint(),
        "dynamic segment must not affect stable prefix fingerprint"
    );
}

/// T26.1.5: PromptAssembly::assemble() matches the existing string-join fallback.
#[test]
fn t26_1_assembly_fallback_matches_existing_string_join() {
    let parts = [
        "system prompt",
        "tools prompt",
        "context prompt",
        "user input",
    ];
    let expected = parts.join("\n");

    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        parts[0],
    ));
    assembly.push(PromptSegment::new(
        "tools",
        PromptSegmentKind::ToolSchema,
        parts[1],
    ));
    assembly.push(PromptSegment::new(
        "ctx",
        PromptSegmentKind::ProjectContext,
        parts[2],
    ));
    assembly.push(PromptSegment::new(
        "user",
        PromptSegmentKind::DynamicEvidence,
        parts[3],
    ));

    assert_eq!(assembly.assemble(), expected);
}

// ── T26.4: BossContextBrief / StateFrame bridge ───────────────────────────────

fn make_brief(strategy: BossContextStrategy) -> BossContextBrief {
    BossContextBrief {
        plan_id: "plan-t26-4".into(),
        step_id: 1,
        plan_version: "plan-t26-4:steps=1".into(),
        step_revision: "step-1-attempt-0".into(),
        generated_at: "1714690000".into(),
        objective: "implement the feature".into(),
        acceptance: vec!["tests pass".into()],
        last_correction: None,
        recent_decisions: Vec::new(),
        relevant_file_handles: vec![RelevantFileHandle {
            path: "RustAgent/Agent/src/core/boss.rs".into(),
            kind: "source_file".into(),
            source: "boss_step_objective".into(),
            freshness: "current".into(),
            why_relevant: "spawn payload logic lives here".into(),
            step_revision: "step-1-attempt-0".into(),
        }],
        target_files: vec!["RustAgent/Agent/src/core/boss.rs".into()],
        target_artifacts: vec![TargetArtifact {
            path: "RustAgent/Agent/src/core/boss.rs".into(),
            kind: "file".into(),
            required_state: "referenced_for_step".into(),
            source: "target_file_handle".into(),
        }],
        allowed_tools: vec!["Read".into(), "Edit".into()],
        permission_scope: PermissionScopeView {
            lism_policy: "force-on".into(),
            inherit_context: false,
            workspace_capability: "inherited_runtime_scope".into(),
            boss_actor_role: "executor_b".into(),
        },
        parent_session_id: "parent-session-1".into(),
        context_strategy: strategy,
    }
}

fn make_frame(step_id: usize) -> BossStateFrame {
    BossStateFrame {
        step_id,
        status: BossPlanStepStatus::Running,
        open_items: vec!["write tests".into()],
        blocked_items: Vec::new(),
        allowed_actions: vec!["implement".into()],
        required_output_hint: Some("return a unified diff".into()),
    }
}

/// T26.4.1: BossContextBrief renders to ActorBrief segment (cacheable), contains objective.
#[test]
fn t26_4_brief_renders_to_actor_brief_segment() {
    let brief = make_brief(BossContextStrategy::Brief);
    let seg = brief.to_prompt_segment();
    assert_eq!(seg.kind, PromptSegmentKind::ActorBrief);
    assert!(seg.is_cacheable(), "ActorBrief segment must be cacheable");
    assert!(
        seg.content.contains("implement the feature"),
        "content must include objective"
    );
    assert!(
        seg.content.contains("tests pass"),
        "content must include acceptance"
    );
    assert!(
        seg.content.contains("relevant_file_handles:"),
        "content must include typed file handles"
    );
    assert!(
        seg.content.contains("target_artifacts:"),
        "content must include target artifacts"
    );
    assert!(
        seg.content.contains("permission_scope:"),
        "content must include permission scope"
    );
}

/// T26.4.2: BossStateFrame renders to StateFrame segment (non-cacheable).
#[test]
fn t26_4_state_frame_renders_to_non_cacheable_segment() {
    let frame = make_frame(1);
    let seg = frame.to_prompt_segment();
    assert_eq!(seg.kind, PromptSegmentKind::StateFrame);
    assert!(
        !seg.is_cacheable(),
        "StateFrame segment must not be cacheable"
    );
    assert!(
        seg.content.contains("write tests"),
        "content must include open_items"
    );
}

/// T26.4.3: Brief fingerprint is stable; state_frame change does not affect it.
#[test]
fn t26_4_brief_fingerprint_stable_across_state_frame_changes() {
    let brief = make_brief(BossContextStrategy::Brief);
    let seg1 = brief.to_prompt_segment();

    let frame1 = make_frame(1);
    let frame2 = BossStateFrame {
        step_id: 1,
        status: BossPlanStepStatus::Running,
        open_items: vec!["DIFFERENT open item".into()],
        blocked_items: Vec::new(),
        allowed_actions: vec!["implement".into()],
        required_output_hint: None,
    };

    let mut assembly1 = PromptAssembly::new();
    assembly1.push(seg1.clone());
    assembly1.push(frame1.to_prompt_segment());

    let mut assembly2 = PromptAssembly::new();
    assembly2.push(seg1);
    assembly2.push(frame2.to_prompt_segment());

    assert_eq!(
        assembly1.stable_prefix_fingerprint(),
        assembly2.stable_prefix_fingerprint(),
        "brief fingerprint must not change when state_frame changes"
    );
}

/// T26.4.4: FullInherit escape hatch is observable via context_strategy field.
#[test]
fn t26_4_full_inherit_escape_hatch_is_observable() {
    let brief = make_brief(BossContextStrategy::FullInherit);
    assert_eq!(brief.context_strategy, BossContextStrategy::FullInherit);
    // FullInherit brief still renders to ActorBrief segment — strategy is metadata only.
    let seg = brief.to_prompt_segment();
    assert_eq!(seg.kind, PromptSegmentKind::ActorBrief);
}

/// T26.4.5: assemble_brief_prompt output contains both objective and open_items.
#[test]
fn t26_4_assembly_output_contains_brief_and_state_frame() {
    let brief = make_brief(BossContextStrategy::Brief);
    let frame = make_frame(1);
    let prompt = assemble_brief_prompt(&brief, &frame);
    assert!(
        prompt.contains("implement the feature"),
        "prompt must contain objective"
    );
    assert!(
        prompt.contains("write tests"),
        "prompt must contain open_items"
    );
    assert!(
        prompt.contains("return a unified diff"),
        "prompt must contain output hint"
    );
}

/// T26.4.6: objective renders before volatile plan_id so provider cache can lock
/// onto stable task semantics before run-specific identifiers.
#[test]
fn t26_4_brief_renders_stable_semantics_before_plan_id() {
    let brief = make_brief(BossContextStrategy::Brief);
    let seg = brief.to_prompt_segment();
    let objective_idx = seg
        .content
        .find("objective: implement the feature")
        .expect("objective must render");
    let plan_id_idx = seg
        .content
        .find("plan_id: plan-t26-4")
        .expect("plan_id must render");
    assert!(
        objective_idx < plan_id_idx,
        "objective should precede plan_id to stabilize the provider cache prefix"
    );
}

/// T26.4.7: build_b_step_payload uses brief/state_frame (inherit_context: false).
#[tokio::test]
async fn t26_4_dispatch_payload_uses_brief_not_full_inherit() {
    let plan_path = std::env::temp_dir().join("t26_4_dispatch.json");
    let plan = BossPlan {
        plan_id: "t26-4-dispatch".into(),
        task_description: "dispatch brief test".into(),
        steps: vec![boss_step(0, "implement feature")],
        accepted_by_user: true,
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let payload = coordinator
        .build_b_step_payload_pub(0, "parent-session", "b-actor")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();

    assert_eq!(
        v["inherit_context"], false,
        "default dispatch must use inherit_context: false"
    );
    assert_eq!(
        v["context_strategy"], "brief",
        "default dispatch must use brief strategy"
    );
    assert!(
        v["task"].as_str().unwrap_or("").contains("objective 0"),
        "task must contain objective"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T26.5: Provider-aware token budget gate ───────────────────────────────────

fn tight_profile() -> ProviderProfile {
    ProviderProfile {
        context_window: 100,
        output_reserve: 10,
        cache_min_size: 64,
        prompt_cache: PromptCacheCapability::Unsupported,
    }
}

/// T26.5.1: Prompt within budget → Pass.
#[test]
fn t26_5_pass_when_prompt_within_budget() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "short",
    ));
    let (_, decision) = evaluate_prompt_budget(&assembly, &ProviderProfile::default());
    assert_eq!(decision, BudgetDecision::Pass);
}

/// T26.5.2: Dynamic suffix pushes total over budget → Degrade.
#[test]
fn t26_5_degrade_when_dynamic_suffix_exceeds_budget() {
    let profile = tight_profile(); // 100 tokens available - 10 reserve = 90 tokens
    let mut assembly = PromptAssembly::new();
    // Static prefix: 10 chars ≈ 3 tokens (within budget)
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "0123456789",
    ));
    // Dynamic suffix: 400 chars ≈ 115 tokens (pushes total over 90)
    assembly.push(PromptSegment::new(
        "sf",
        PromptSegmentKind::StateFrame,
        "x".repeat(400),
    ));
    let (_, decision) = evaluate_prompt_budget(&assembly, &profile);
    assert!(
        matches!(decision, BudgetDecision::Degrade { .. }),
        "expected Degrade, got {decision:?}"
    );
}

/// T26.5.3: Static prefix alone exceeds budget → Reject.
#[test]
fn t26_5_reject_when_static_prefix_exceeds_budget() {
    let profile = tight_profile(); // 90 tokens available
    let mut assembly = PromptAssembly::new();
    // Static prefix: 500 chars ≈ 143 tokens (exceeds 90)
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "s".repeat(500),
    ));
    let (_, decision) = evaluate_prompt_budget(&assembly, &profile);
    assert!(
        matches!(decision, BudgetDecision::Reject { .. }),
        "expected Reject, got {decision:?}"
    );
}

/// T26.5.4: evaluate_prompt_budget is a pure function — assembly content unchanged after call.
#[test]
fn t26_5_evaluate_is_pure_function_no_side_effects() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "hello",
    ));
    let content_before = assembly.segments()[0].content.clone();
    let _ = evaluate_prompt_budget(&assembly, &ProviderProfile::default());
    assert_eq!(
        assembly.segments()[0].content,
        content_before,
        "assembly must not be modified"
    );
}

/// T26.5.5: Degrade from budget gate triggers summarize path in ask_b_session.
/// A 750k char payload (≈214k tokens) exceeds the 192k available tokens → Degrade → summarize.
/// With no A session, falls back to trim. Either way, last_b_ask_message is compressed.
#[tokio::test]
async fn t26_5_degrade_budget_triggers_compression_in_ask_b_session() {
    let plan_path = std::env::temp_dir().join("t26_5_degrade.json");
    let plan = BossPlan {
        plan_id: "t26-5-degrade".into(),
        task_description: "budget degrade test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t26_5_degrade_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t26-5-degrade", task_manager.clone());

    let fake_b = task_manager.create_with_type(
        "fake B",
        TaskType::LocalAgent,
        "session-t26-5-degrade",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm = task_manager.clone();
    let b_id = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            for _ in tm.drain_mailbox(&b_id) {
                tm.append_output(&b_id, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    // 750k chars ≈ 214k tokens > 192k available → Degrade → T25/T25.2 compression.
    let oversized = "x".repeat(750_000);
    let _ = coordinator
        .ask_b_session_pub(&app_state, oversized.clone())
        .await;

    let sent = coordinator
        .status
        .read()
        .await
        .last_b_ask_message
        .clone()
        .unwrap_or_default();
    assert!(
        sent.len() < oversized.len(),
        "ask_b_session must compress the payload when budget gate returns Degrade"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T26.2: Provider cache capability ─────────────────────────────────────────

/// T26.2.1: Default ProviderProfile (Claude baseline) has AnthropicEphemeral cache.
#[test]
fn t26_2_default_profile_has_anthropic_ephemeral_cache() {
    assert_eq!(
        ProviderProfile::default().prompt_cache,
        PromptCacheCapability::AnthropicEphemeral,
        "default profile must reflect Claude's ephemeral cache capability"
    );
}

/// T26.2.2: PromptCacheCapability::default() is Unsupported (conservative type default).
#[test]
fn t26_2_unsupported_is_type_default() {
    assert_eq!(
        PromptCacheCapability::default(),
        PromptCacheCapability::Unsupported,
        "PromptCacheCapability type default must be Unsupported"
    );
}

/// T26.2.3: cache capability is pure metadata — Unsupported vs AnthropicEphemeral
/// profiles with identical token counts produce the same BudgetDecision.
#[test]
fn t26_2_cache_capability_is_pure_metadata() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "hello world",
    ));

    let profile_unsupported = ProviderProfile {
        prompt_cache: PromptCacheCapability::Unsupported,
        ..ProviderProfile::default()
    };
    let profile_ephemeral = ProviderProfile {
        prompt_cache: PromptCacheCapability::AnthropicEphemeral,
        ..ProviderProfile::default()
    };

    let (_, decision_a) = evaluate_prompt_budget(&assembly, &profile_unsupported);
    let (_, decision_b) = evaluate_prompt_budget(&assembly, &profile_ephemeral);
    assert_eq!(
        decision_a, decision_b,
        "prompt_cache must not affect BudgetDecision"
    );
}

/// T26.2.4: ManualNone is distinct from Unsupported — different semantic intent.
#[test]
fn t26_2_manual_none_is_distinct_from_unsupported() {
    assert_ne!(
        PromptCacheCapability::ManualNone,
        PromptCacheCapability::Unsupported,
        "ManualNone (explicitly disabled) must be distinct from Unsupported (not available)"
    );
}

// ── T26.3: Request builder cache adapter ─────────────────────────────────────

fn make_payload() -> serde_json::Value {
    serde_json::json!({
        "model": "claude-3-5-sonnet",
        "messages": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}],
        "stream": true,
        "max_tokens": 4096
    })
}

/// T26.3.1: AnthropicEphemeral injects system array with cache_control on last cacheable block.
#[test]
fn t26_3_anthropic_ephemeral_injects_system_cache_control() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system content",
    ));
    assembly.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "dynamic content",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::AnthropicEphemeral,
        ..ProviderProfile::default()
    };
    let mut payload = make_payload();
    apply_cache_control(&assembly, &profile, &mut payload);

    let system = &payload["system"];
    assert!(system.is_array(), "system must be an array");
    let blocks = system.as_array().unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["cache_control"]["type"], "ephemeral");
}

/// T26.3.2: Only the last cacheable block gets cache_control; earlier ones do not.
#[test]
fn t26_3_only_last_cacheable_block_gets_cache_control() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "s1",
        PromptSegmentKind::StaticSystem,
        "first",
    ));
    assembly.push(PromptSegment::new(
        "s2",
        PromptSegmentKind::ActorBrief,
        "second",
    ));
    assembly.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "dynamic",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::AnthropicEphemeral,
        ..ProviderProfile::default()
    };
    let mut payload = make_payload();
    apply_cache_control(&assembly, &profile, &mut payload);

    let blocks = payload["system"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    assert!(
        blocks[0].get("cache_control").is_none(),
        "first block must not have cache_control"
    );
    assert_eq!(
        blocks[1]["cache_control"]["type"], "ephemeral",
        "last block must have cache_control"
    );
}

/// T26.3.3: Dynamic segments go to messages[0].content, not system.
#[test]
fn t26_3_dynamic_segments_go_to_messages_not_system() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    assembly.push(PromptSegment::new(
        "ev",
        PromptSegmentKind::DynamicEvidence,
        "evidence",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::AnthropicEphemeral,
        ..ProviderProfile::default()
    };
    let mut payload = make_payload();
    apply_cache_control(&assembly, &profile, &mut payload);

    let content = &payload["messages"][0]["content"];
    assert!(content.is_array());
    let blocks = content.as_array().unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["text"], "evidence");
    // system must not contain the dynamic segment
    let system_texts: Vec<_> = payload["system"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["text"].as_str().unwrap_or(""))
        .collect();
    assert!(!system_texts.contains(&"evidence"));
}

/// T26.3.4: Unsupported profile leaves payload unchanged.
#[test]
fn t26_3_unsupported_profile_is_noop() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::Unsupported,
        ..ProviderProfile::default()
    };
    let original = make_payload();
    let mut payload = original.clone();
    apply_cache_control(&assembly, &profile, &mut payload);

    assert_eq!(
        payload, original,
        "Unsupported must leave payload unchanged"
    );
}

/// T26.3.5: ManualNone profile leaves payload unchanged.
#[test]
fn t26_3_manual_none_is_noop() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::ManualNone,
        ..ProviderProfile::default()
    };
    let original = make_payload();
    let mut payload = original.clone();
    apply_cache_control(&assembly, &profile, &mut payload);

    assert_eq!(payload, original, "ManualNone must leave payload unchanged");
}

/// T26.3.6: Assembly with no cacheable segments leaves system field absent.
#[test]
fn t26_3_no_cacheable_segments_leaves_system_absent() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "ev",
        PromptSegmentKind::DynamicEvidence,
        "only dynamic",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::AnthropicEphemeral,
        ..ProviderProfile::default()
    };
    let mut payload = make_payload();
    apply_cache_control(&assembly, &profile, &mut payload);

    assert!(
        payload.get("system").is_none(),
        "no cacheable segments → system field must be absent"
    );
}

// ── T26.6: A/B context isolation ─────────────────────────────────────────────

/// T26.6.1: After B context summarize is triggered, A session's last_a_dispatch_message
/// must NOT contain B's old context — stateless path does not route through A session.
#[tokio::test]
async fn t26_6_a_session_not_polluted_by_b_summarize() {
    let plan_path = std::env::temp_dir().join("t26_6_a_not_polluted.json");
    let plan = BossPlan {
        plan_id: "t26-6-a-not-polluted".into(),
        task_description: "T26.6 isolation test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t26_6_a_not_polluted_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t26-6-a-not-polluted", task_manager.clone());

    // Fake B session.
    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t26-6-a-not-polluted",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    let oversized = "B_CONTEXT_MARKER ".repeat(B_CONTEXT_TRIM_THRESHOLD / 16 + 1);
    assert!(oversized.len() > B_CONTEXT_TRIM_THRESHOLD);
    let _ = coordinator.ask_b_session_pub(&app_state, oversized).await;

    let guard = coordinator.status.read().await;
    // A session dispatch message must not contain the B context marker.
    if let Some(ref a_msg) = guard.last_a_dispatch_message {
        assert!(
            !a_msg.contains("B_CONTEXT_MARKER"),
            "A session must not be polluted with B context"
        );
    }
    // If last_a_dispatch_message is None, A was never called — isolation holds.

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.6.2: Stateless summarize does not write to A session history.
/// Pre-set sentinel in last_a_dispatch_message; after B summarize fires, sentinel must be unchanged.
#[tokio::test]
async fn t26_6_stateless_summarize_does_not_write_a_session_history() {
    let plan_path = std::env::temp_dir().join("t26_6_stateless_no_a_write.json");
    let plan = BossPlan {
        plan_id: "t26-6-stateless-no-a-write".into(),
        task_description: "T26.6 stateless isolation test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t26_6_stateless_no_a_write_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state =
        app_state_with_tasks("session-t26-6-stateless-no-a-write", task_manager.clone());

    // Pre-set sentinel.
    {
        let mut guard = coordinator.status.write().await;
        guard.last_a_dispatch_message = Some("SENTINEL_BEFORE_B_SUMMARIZE".to_string());
    }

    // Fake B session.
    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t26-6-stateless-no-a-write",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    let oversized = "B_CONTEXT_MARKER ".repeat(B_CONTEXT_TRIM_THRESHOLD / 16 + 1);
    assert!(oversized.len() > B_CONTEXT_TRIM_THRESHOLD);
    let _ = coordinator.ask_b_session_pub(&app_state, oversized).await;

    let guard = coordinator.status.read().await;
    assert_eq!(
        guard.last_a_dispatch_message.as_deref(),
        Some("SENTINEL_BEFORE_B_SUMMARIZE"),
        "stateless summarize must not overwrite last_a_dispatch_message"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.6.3: When stateless summarize fails (no active_model_runtime), fallback to trim.
/// last_b_ask_message must be shorter than the original oversized payload.
#[tokio::test]
async fn t26_6_fallback_to_trim_when_stateless_summarize_fails() {
    let plan_path = std::env::temp_dir().join("t26_6_fallback_trim.json");
    let plan = BossPlan {
        plan_id: "t26-6-fallback-trim".into(),
        task_description: "T26.6 fallback trim test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t26_6_fallback_trim_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    // app_state has no active_model_runtime → stateless summarize returns Err → fallback to trim.
    let app_state = app_state_with_tasks("session-t26-6-fallback-trim", task_manager.clone());

    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t26-6-fallback-trim",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    let oversized = "TRIM_FALLBACK_MARKER ".repeat(B_CONTEXT_TRIM_THRESHOLD / 19 + 1);
    assert!(oversized.len() > B_CONTEXT_TRIM_THRESHOLD);
    let _ = coordinator
        .ask_b_session_pub(&app_state, oversized.clone())
        .await;

    let guard = coordinator.status.read().await;
    let sent = guard.last_b_ask_message.as_deref().unwrap_or("");
    assert!(
        sent.len() < oversized.len(),
        "fallback trim must compress the payload when stateless summarize fails"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.6.4: B context summary uses stateless path — A session dispatch message
/// is never set to the summarize prompt when stateless path is active.
#[tokio::test]
async fn t26_6_b_context_summary_uses_stateless_path() {
    let plan_path = std::env::temp_dir().join("t26_6_stateless_path.json");
    let plan = BossPlan {
        plan_id: "t26-6-stateless-path".into(),
        task_description: "T26.6 stateless path test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join("t26_6_stateless_path_output");
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks("session-t26-6-stateless-path", task_manager.clone());

    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        "session-t26-6-stateless-path",
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    let oversized = "STATELESS_PATH_MARKER ".repeat(B_CONTEXT_TRIM_THRESHOLD / 20 + 1);
    assert!(oversized.len() > B_CONTEXT_TRIM_THRESHOLD);
    let _ = coordinator.ask_b_session_pub(&app_state, oversized).await;

    let guard = coordinator.status.read().await;
    // Stateless path must not route summarize prompt through A session.
    if let Some(ref a_msg) = guard.last_a_dispatch_message {
        assert!(
            !a_msg.contains("Summarize the following context"),
            "stateless path must not route summarize prompt through A session"
        );
    }

    let _ = std::fs::remove_file(&plan_path);
}

// ── T26.7: Cache observability ────────────────────────────────────────────────

async fn setup_coordinator_with_b_session(
    plan_id: &str,
    output_dir_name: &str,
) -> (
    BossCoordinator,
    std::path::PathBuf,
    Arc<TaskManager>,
    Arc<AppState>,
) {
    let plan_path = std::env::temp_dir().join(format!("{plan_id}.json"));
    let plan = BossPlan {
        plan_id: plan_id.into(),
        task_description: "T26.7 metrics test".into(),
        steps: vec![boss_step(0, "step zero")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let unique_dir = std::env::temp_dir().join(output_dir_name);
    let task_manager = Arc::new(TaskManager::new_with_output_root(unique_dir));
    let app_state = app_state_with_tasks(&format!("session-{plan_id}"), task_manager.clone());

    let fake_b = task_manager.create_with_type(
        "fake B session",
        TaskType::LocalAgent,
        &format!("session-{plan_id}"),
        InteractionSurface::Cli,
    );
    let b_task_id = fake_b.id.clone();
    let tm_for_b = task_manager.clone();
    let b_id_for_loop = b_task_id.clone();
    task_manager.launch(&b_task_id, "", async move {
        loop {
            let messages = tm_for_b.drain_mailbox(&b_id_for_loop);
            for _msg in messages {
                tm_for_b.append_output(&b_id_for_loop, "B_ACK");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    coordinator.record_b_session_id_pub(&b_task_id).await;

    (coordinator, plan_path, task_manager, app_state)
}

/// T26.7.1: Short message (within budget) → CompressionStrategy::None, original_chars == sent_chars.
#[tokio::test]
async fn t26_7_no_compression_records_none_strategy() {
    let (coordinator, plan_path, _, app_state) =
        setup_coordinator_with_b_session("t26-7-none", "t26_7_none_output").await;

    let short_msg = "short message within budget".to_string();
    let original_len = short_msg.len();
    let _ = coordinator.ask_b_session_pub(&app_state, short_msg).await;

    let guard = coordinator.status.read().await;
    let metrics = guard
        .last_step_metrics
        .as_ref()
        .expect("last_step_metrics must be set");
    assert_eq!(metrics.compression_strategy, CompressionStrategy::None);
    assert_eq!(metrics.original_chars, original_len);
    assert_eq!(metrics.sent_chars, original_len);

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.7.2: Oversized message with no active_model_runtime → fallback trim → CompressionStrategy::Trimmed.
#[tokio::test]
async fn t26_7_trim_path_records_trimmed_strategy() {
    let (coordinator, plan_path, _, app_state) =
        setup_coordinator_with_b_session("t26-7-trim", "t26_7_trim_output").await;

    let oversized = "trim_data_".repeat(B_CONTEXT_TRIM_THRESHOLD / 9 + 1);
    assert!(oversized.len() > B_CONTEXT_TRIM_THRESHOLD);
    let original_len = oversized.len();
    let _ = coordinator.ask_b_session_pub(&app_state, oversized).await;

    let guard = coordinator.status.read().await;
    let metrics = guard
        .last_step_metrics
        .as_ref()
        .expect("last_step_metrics must be set");
    assert_eq!(metrics.compression_strategy, CompressionStrategy::Trimmed);
    assert_eq!(metrics.original_chars, original_len);
    assert!(
        metrics.sent_chars < original_len,
        "sent_chars must be less than original after trim"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.7.3: Default context mode is Brief.
#[tokio::test]
async fn t26_7_brief_context_mode_recorded() {
    let (coordinator, plan_path, _, app_state) =
        setup_coordinator_with_b_session("t26-7-brief", "t26_7_brief_output").await;

    let _ = coordinator
        .ask_b_session_pub(&app_state, "hello".to_string())
        .await;

    let guard = coordinator.status.read().await;
    let metrics = guard
        .last_step_metrics
        .as_ref()
        .expect("last_step_metrics must be set");
    assert_eq!(metrics.context_mode, ContextMode::Brief);

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.7.4: original_chars matches the input message length before any compression.
#[tokio::test]
async fn t26_7_metrics_original_chars_matches_input_length() {
    let (coordinator, plan_path, _, app_state) =
        setup_coordinator_with_b_session("t26-7-chars", "t26_7_chars_output").await;

    let msg = "x".repeat(42);
    let _ = coordinator.ask_b_session_pub(&app_state, msg).await;

    let guard = coordinator.status.read().await;
    let metrics = guard
        .last_step_metrics
        .as_ref()
        .expect("last_step_metrics must be set");
    assert_eq!(metrics.original_chars, 42);

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.7.5: last_step_metrics is None before any ask_b_session call.
#[test]
fn t26_7_metrics_none_before_first_dispatch() {
    let status = rust_agent::core::boss_state::BossStatus::default();
    assert!(
        status.last_step_metrics.is_none(),
        "last_step_metrics must be None before first dispatch"
    );
}

// ── T26.8: Production-path tests ─────────────────────────────────────────────

/// T26.8.1: stable_prefix_fingerprint is stable across consecutive dispatches with the same brief.
#[test]
fn t26_8_stable_prefix_fingerprint_stable_across_dispatches() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system prompt",
    ));
    assembly.push(PromptSegment::new(
        "brief",
        PromptSegmentKind::ActorBrief,
        "actor brief",
    ));
    assembly.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "dynamic content",
    ));

    let fp1 = assembly.stable_prefix_fingerprint();
    let fp2 = assembly.stable_prefix_fingerprint();
    assert_eq!(
        fp1, fp2,
        "stable_prefix_fingerprint must be deterministic across calls"
    );

    // Changing dynamic content must not change the stable prefix fingerprint.
    let mut assembly2 = PromptAssembly::new();
    assembly2.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system prompt",
    ));
    assembly2.push(PromptSegment::new(
        "brief",
        PromptSegmentKind::ActorBrief,
        "actor brief",
    ));
    assembly2.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "DIFFERENT dynamic content",
    ));

    assert_eq!(
        assembly.stable_prefix_fingerprint(),
        assembly2.stable_prefix_fingerprint(),
        "changing dynamic suffix must not affect stable prefix fingerprint"
    );
}

/// T26.8.2: Child worker dispatch payload uses context_strategy: "brief", not "full_inherit".
#[tokio::test]
async fn t26_8_child_worker_payload_uses_brief_not_full_inherit() {
    let plan_path = std::env::temp_dir().join("t26-8-brief-payload.json");
    let plan = BossPlan {
        plan_id: "t26-8-brief-payload".into(),
        task_description: "T26.8 brief payload test".into(),
        steps: vec![boss_step(0, "implement feature")],
        ..Default::default()
    };
    save_plan(&plan, &plan_path).await.unwrap();

    let coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
    let payload = coordinator
        .build_b_step_payload_pub(0, "parent-session", "b-actor")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();

    assert_eq!(
        v["inherit_context"], false,
        "child worker must not inherit full context"
    );
    assert_eq!(
        v["context_strategy"], "brief",
        "child worker must use brief context strategy"
    );

    let _ = std::fs::remove_file(&plan_path);
}

/// T26.8.3: Unsupported provider profile → apply_cache_control is no-op in production path.
#[test]
fn t26_8_unsupported_provider_noop_in_production_path() {
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    assembly.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "dynamic",
    ));

    let profile = ProviderProfile {
        prompt_cache: PromptCacheCapability::Unsupported,
        ..ProviderProfile::default()
    };
    let original = serde_json::json!({
        "model": "claude-3-5-sonnet",
        "messages": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}],
        "max_tokens": 4096
    });
    let mut payload = original.clone();
    apply_cache_control(&assembly, &profile, &mut payload);

    assert_eq!(
        payload, original,
        "Unsupported provider must leave payload unchanged in production path"
    );
}

/// T26.8.4: BossStepMetrics has cache token fields defaulting to 0 before B actor reports usage.
#[tokio::test]
async fn t26_8_cache_token_fields_default_to_zero_before_b_reports() {
    let (coordinator, plan_path, _, app_state) =
        setup_coordinator_with_b_session("t26-8-cache-tokens", "t26_8_cache_tokens_output").await;

    let _ = coordinator
        .ask_b_session_pub(&app_state, "hello".to_string())
        .await;

    let guard = coordinator.status.read().await;
    let metrics = guard
        .last_step_metrics
        .as_ref()
        .expect("last_step_metrics must be set");
    assert_eq!(
        metrics.cache_creation_tokens, 0,
        "cache_creation_tokens must default to 0 before B actor reports"
    );
    assert_eq!(
        metrics.cache_read_tokens, 0,
        "cache_read_tokens must default to 0 before B actor reports"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T26.9: Cache stability guard ─────────────────────────────────────────────

use rust_agent::core::prompt_segment::{PrefixStabilityResult, check_prefix_stability};

/// T26.9.1: StateFrame change does not affect stable prefix fingerprint.
#[test]
fn t26_9_state_frame_change_does_not_affect_prefix_fingerprint() {
    let mut a1 = PromptAssembly::new();
    a1.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    a1.push(PromptSegment::new(
        "sf",
        PromptSegmentKind::StateFrame,
        "step 1",
    ));

    let mut a2 = PromptAssembly::new();
    a2.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    a2.push(PromptSegment::new(
        "sf",
        PromptSegmentKind::StateFrame,
        "step 2 CHANGED",
    ));

    assert_eq!(
        a1.stable_prefix_fingerprint(),
        a2.stable_prefix_fingerprint(),
        "StateFrame change must not affect stable prefix fingerprint"
    );
}

/// T26.9.2: DebugTrace (tool output) change does not affect stable prefix fingerprint.
#[test]
fn t26_9_tool_output_does_not_affect_prefix_fingerprint() {
    let mut a1 = PromptAssembly::new();
    a1.push(PromptSegment::new(
        "brief",
        PromptSegmentKind::ActorBrief,
        "actor brief",
    ));
    a1.push(PromptSegment::new(
        "trace",
        PromptSegmentKind::DynamicEvidence,
        "tool output v1",
    ));

    let mut a2 = PromptAssembly::new();
    a2.push(PromptSegment::new(
        "brief",
        PromptSegmentKind::ActorBrief,
        "actor brief",
    ));
    a2.push(PromptSegment::new(
        "trace",
        PromptSegmentKind::DynamicEvidence,
        "tool output v2 CHANGED",
    ));

    assert_eq!(
        a1.stable_prefix_fingerprint(),
        a2.stable_prefix_fingerprint(),
        "DynamicEvidence change must not affect stable prefix fingerprint"
    );
}

/// T26.9.3: Changing a cacheable segment triggers Unstable result.
#[test]
fn t26_9_cacheable_change_triggers_unstable() {
    let mut a1 = PromptAssembly::new();
    a1.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "original system",
    ));

    let prev_fp = a1.stable_prefix_fingerprint();

    let mut a2 = PromptAssembly::new();
    a2.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "CHANGED system",
    ));

    let result = check_prefix_stability(prev_fp, &a2);
    assert!(
        matches!(result, PrefixStabilityResult::Unstable { .. }),
        "changing a cacheable segment must produce Unstable result"
    );
}

/// T26.9.4: Only dynamic segments change → Stable result.
#[test]
fn t26_9_stable_when_only_dynamic_changes() {
    let mut a1 = PromptAssembly::new();
    a1.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    a1.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "dynamic v1",
    ));

    let prev_fp = a1.stable_prefix_fingerprint();

    let mut a2 = PromptAssembly::new();
    a2.push(PromptSegment::new(
        "sys",
        PromptSegmentKind::StaticSystem,
        "system",
    ));
    a2.push(PromptSegment::new(
        "dyn",
        PromptSegmentKind::DynamicEvidence,
        "dynamic v2 CHANGED",
    ));

    let result = check_prefix_stability(prev_fp, &a2);
    assert!(
        matches!(result, PrefixStabilityResult::Stable { .. }),
        "only dynamic segment change must produce Stable result"
    );
}

/// T26.9.5: BossStepMetrics.cache_prefix_instability field exists and defaults to false.
#[tokio::test]
async fn t26_9_instability_recorded_in_step_metrics() {
    let (coordinator, plan_path, _, app_state) =
        setup_coordinator_with_b_session("t26-9-instability", "t26_9_instability_output").await;

    let _ = coordinator
        .ask_b_session_pub(&app_state, "hello".to_string())
        .await;

    let guard = coordinator.status.read().await;
    let metrics = guard
        .last_step_metrics
        .as_ref()
        .expect("last_step_metrics must be set");
    assert!(
        !metrics.cache_prefix_instability,
        "cache_prefix_instability must default to false"
    );

    let _ = std::fs::remove_file(&plan_path);
}

// ── T27.2 StateFrame / StateDecision model ────────────────────────────────

#[test]
fn t27_2_state_frame_serializes_and_deserializes() {
    use rust_agent::core::state_frame::{
        ActorRole, AgentState, EffortLevel, StateBudget, StateFrame,
    };

    let frame = StateFrame {
        role: ActorRole::ExecutorB,
        state: AgentState::Executing,
        objective: "implement step 3".into(),
        open_items: vec!["write tests".into()],
        blocked_items: vec![],
        accepted_summary: vec!["step 1 done".into()],
        recent_evidence: vec!["diff: +10 lines".into()],
        allowed_actions: vec!["read_file".into(), "edit_file".into()],
        toolset_id: Some("minimal-edit".into()),
        skillset_id: None,
        required_output_schema: Some("state_decision_v1".into()),
        budget: StateBudget {
            effort: EffortLevel::M,
            max_input_tokens: 50_000,
            max_tool_calls: 10,
            max_wall_time_ms: 0,
        },
    };

    let json = serde_json::to_string(&frame).expect("serialize");
    let back: StateFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.role, ActorRole::ExecutorB);
    assert_eq!(back.state, AgentState::Executing);
    assert_eq!(back.open_items, vec!["write tests"]);
    assert_eq!(back.budget.effort, EffortLevel::M);
    assert_eq!(back.budget.max_input_tokens, 50_000);
}

#[test]
fn t27_2_state_decision_valid_json_parses() {
    use rust_agent::core::state_frame::{AgentState, DecisionKind, validate_state_decision};

    let json = r#"{
        "state": "executing",
        "decision": "continue",
        "confidence": 0.9
    }"#;

    let decision = validate_state_decision(json).expect("should parse");
    assert_eq!(decision.state, AgentState::Executing);
    assert_eq!(decision.decision, DecisionKind::Continue);
    assert!((decision.confidence - 0.9).abs() < 0.001);
    assert!(!decision.escalate);
    assert!(decision.needed_context.is_empty());
}

#[test]
fn t27_2_state_decision_invalid_json_returns_repair_needed() {
    use rust_agent::core::state_frame::validate_state_decision;

    let bad = r#"{ "state": "executing", "decision": }"#;
    let err = validate_state_decision(bad).expect_err("should fail");
    assert!(
        err.reason.contains("JSON parse error"),
        "reason: {}",
        err.reason
    );
    assert_eq!(err.raw_json, bad);
}

#[test]
fn t27_2_default_effort_is_m() {
    use rust_agent::core::state_frame::{EffortLevel, StateBudget};

    let budget = StateBudget::default();
    assert_eq!(budget.effort, EffortLevel::M);
    assert_eq!(budget.max_input_tokens, 0);
    assert_eq!(budget.max_tool_calls, 0);
}

#[test]
fn t27_2_state_frame_to_prompt_segment_is_non_cacheable() {
    use rust_agent::core::prompt_segment::PromptSegmentKind;
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};

    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Planning,
        objective: "plan the task".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };

    let seg = frame.to_prompt_segment();
    assert_eq!(seg.kind, PromptSegmentKind::StateFrame);
    assert!(
        !seg.is_cacheable(),
        "StateFrame segment must not be cacheable"
    );
    assert!(
        seg.content.contains("planning"),
        "content should include state"
    );
}

#[test]
fn t27_2_state_decision_patch_aliases_deserialize() {
    use rust_agent::core::state_frame::validate_state_decision;

    let json = r#"{
        "state": "reviewing",
        "decision": "continue",
        "state_patch": {
            "accepted_summary": ["draft summary"],
            "open_items": ["verify evidence"]
        }
    }"#;

    let decision = validate_state_decision(json).expect("should parse");
    assert_eq!(
        decision.state_patch.accepted_summary_add,
        vec!["draft summary"]
    );
    assert_eq!(decision.state_patch.open_items_add, vec!["verify evidence"]);
}

#[test]
fn t27_2_state_decision_wrapper_payload_normalizes() {
    use rust_agent::core::state_frame::{AgentState, DecisionKind, validate_state_decision};

    let json = r#"{
        "type": "StateDecision",
        "valid": true,
        "decision": {
            "next_state": "Idle",
            "actions": []
        },
        "message": "StateDecision generated successfully."
    }"#;

    let decision = validate_state_decision(json).expect("should normalize");
    assert_eq!(decision.state, AgentState::Done);
    assert_eq!(decision.decision, DecisionKind::Done);
}

// ── T27.3 StateFrame projection ───────────────────────────────────────────

#[test]
fn t27_3_documentation_stage_maps_to_planning_state() {
    use rust_agent::core::boss_state::{BossPlan, BossStage};
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_projection::project_state_frame;

    let plan = BossPlan {
        plan_id: "p1".into(),
        task_description: "build the feature".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![],
        accepted_by_user: false,
        auto_sequence: false,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Documentation, None, ActorRole::DesignerA);
    assert_eq!(frame.state, AgentState::Planning);
    assert_eq!(frame.objective, "build the feature");
    assert!(frame.open_items.is_empty());
    assert!(frame.blocked_items.is_empty());
    assert_eq!(frame.allowed_actions, vec!["read_file", "write_spec"]);
    assert_eq!(
        frame.required_output_schema.as_deref(),
        Some("state_decision_v1")
    );
}

#[test]
fn t27_3_execution_stage_with_step_maps_objective_and_open_items() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_projection::project_state_frame;

    let step = BossPlanStep {
        id: 0,
        description: "implement auth".into(),
        objective: Some("add JWT middleware".into()),
        acceptance: vec!["tests pass".into(), "no regressions".into()],
        requires_approval: false,
        status: BossPlanStepStatus::Running,
        completed: false,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };
    let plan = BossPlan {
        plan_id: "p2".into(),
        task_description: "build the feature".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![step],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::ExecutorB);
    assert_eq!(frame.state, AgentState::Executing);
    assert_eq!(frame.objective, "add JWT middleware");
    assert_eq!(frame.open_items, vec!["tests pass", "no regressions"]);
    assert!(frame.blocked_items.is_empty());
    assert!(frame.allowed_actions.contains(&"edit_file".to_string()));
    assert!(
        frame
            .recent_evidence
            .iter()
            .any(|item| item.contains("fact: immutable_plan")),
        "projection should carry immutable plan facts"
    );
}

#[test]
fn t27_3_completed_steps_go_into_accepted_summary() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_projection::project_state_frame;

    let done_step = BossPlanStep {
        id: 0,
        description: "step zero done".into(),
        objective: None,
        acceptance: vec![],
        requires_approval: false,
        status: BossPlanStepStatus::Completed,
        completed: true,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };
    let current_step = BossPlanStep {
        id: 1,
        description: "step one running".into(),
        objective: None,
        acceptance: vec![],
        requires_approval: false,
        status: BossPlanStepStatus::Running,
        completed: false,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };
    let plan = BossPlan {
        plan_id: "p3".into(),
        task_description: "multi-step task".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![done_step, current_step],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Execution, Some(1), ActorRole::Worker);
    assert_eq!(frame.accepted_summary, vec!["step zero done"]);
    // current step must NOT appear in accepted_summary
    assert!(
        !frame
            .accepted_summary
            .iter()
            .any(|s| s.contains("step one"))
    );
}

#[test]
fn t27_3_waiting_for_approval_maps_to_blocked_with_blocked_item() {
    use rust_agent::core::boss_state::{BossPlan, BossStage};
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_projection::project_state_frame;

    let plan = BossPlan {
        plan_id: "p4".into(),
        task_description: "awaiting sign-off".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![],
        accepted_by_user: false,
        auto_sequence: false,
        ..Default::default()
    };

    let frame = project_state_frame(
        &plan,
        BossStage::WaitingForApproval,
        None,
        ActorRole::DesignerA,
    );
    assert_eq!(frame.state, AgentState::Blocked);
    assert_eq!(frame.blocked_items, vec!["waiting for user approval"]);
    assert!(frame.allowed_actions.is_empty());
}

#[test]
fn t27_3_readonly_audit_projection_emits_fact_ledger_and_readonly_actions() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_projection::project_state_frame;

    let step = BossPlanStep {
        id: 0,
        description: "readonly audit".into(),
        objective: Some("只读输出，不改文件；总结当前 LisM / KV cache 约束".into()),
        acceptance: vec!["Task completed successfully.".into()],
        requires_approval: false,
        status: BossPlanStepStatus::Running,
        completed: false,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 0,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };
    let plan = BossPlan {
        plan_id: "p-readonly".into(),
        task_description: "readonly task".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![step],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
    assert_eq!(
        frame.allowed_actions,
        vec!["read_file".to_string(), "summarize_findings".to_string()]
    );
    assert!(
        frame
            .recent_evidence
            .iter()
            .any(|item| item.contains("fact: execution_mode read_only_analysis")),
        "readonly tasks should project read-only mode into the fact ledger"
    );
    assert!(
        frame
            .recent_evidence
            .iter()
            .any(|item| item.contains("fact: open_blockers none")),
        "projection should explicitly say there are no open blockers"
    );
    assert!(
        frame
            .recent_evidence
            .iter()
            .any(|item| item.contains("fact: reject_correction none recorded")),
        "projection should explicitly say when reject/correction history is absent"
    );
}

#[test]
fn t27_3_projection_emits_file_change_and_test_ledgers() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_projection::project_state_frame;

    let step = BossPlanStep {
        id: 0,
        description: "implement worker ledger".into(),
        objective: Some(
            "任务目标：\n- 目标文件：src/core/state_frame_projection.rs\n- 修复 worker ledger".into(),
        ),
        acceptance: vec!["tests pass".into()],
        requires_approval: false,
        status: BossPlanStepStatus::Running,
        completed: false,
        result_diff: Some(
            "updated src/core/state_frame_projection.rs; tests failed in boss_flow due to stale file_facts placeholder".into(),
        ),
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: Some("tests failed because file_facts still said none recorded".into()),
        last_correction: None,
        review_task_id: None,
    };
    let plan = BossPlan {
        plan_id: "p-ledger".into(),
        task_description: "ledger task".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![step],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
    assert!(
        frame.recent_evidence.iter().any(|item| {
            item.contains("fact: file_facts")
                && item.contains("path=src/core/state_frame_projection.rs")
        }),
        "projection should emit file fact refs for concrete target files"
    );
    assert!(
        frame.recent_evidence.iter().any(|item| {
            item.contains("fact: recent_changes_in_files")
                && item.contains("path=src/core/state_frame_projection.rs")
        }),
        "projection should emit change refs when worker output references changed files"
    );
    assert!(
        frame
            .recent_evidence
            .iter()
            .any(|item| { item.contains("fact: test_failures") && item.contains("status=failed") }),
        "projection should emit test ledger entries when failures are reported"
    );
}

#[test]
fn t27_3_projected_frame_is_non_cacheable_segment() {
    use rust_agent::core::boss_state::{BossPlan, BossStage};
    use rust_agent::core::prompt_segment::PromptSegmentKind;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_projection::project_state_frame;

    let plan = BossPlan {
        plan_id: "p5".into(),
        task_description: "verify cacheability".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![],
        accepted_by_user: false,
        auto_sequence: false,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Documentation, None, ActorRole::Worker);
    let seg = frame.to_prompt_segment();
    assert_eq!(seg.kind, PromptSegmentKind::StateFrame);
    assert!(!seg.is_cacheable());
    assert!(seg.content.contains("state_decision_v1"));
}

// ── T27.4 JSON decision loop ──────────────────────────────────────────────

#[test]
fn t27_4_done_decision_terminates_loop() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done","confidence":1.0}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "finish the task".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: Some("state_decision_v1".into()),
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(matches!(
        outcome,
        LoopOutcome::Done {
            final_state: AgentState::Done,
            ..
        }
    ));
}

#[test]
fn t27_4_continue_decision_advances_state() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    // First turn: continue with real StateFrame progress, second turn: done.
    let continue_json = r#"{"state":"verifying","decision":"continue"}"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(continue_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::ExecutorB,
        state: AgentState::Executing,
        objective: "run tests".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_4_reject_decision_returns_rejected_outcome() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reject_json = r#"{"state":"blocked","decision":"reject","next_action":{"action_type":"reject","args":{"reason":"acceptance criteria not met"}}}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        reject_json.into(),
    )]]);
    let frame = StateFrame {
        role: ActorRole::Verifier,
        state: AgentState::Verifying,
        objective: "verify step output".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    match outcome {
        LoopOutcome::Rejected { reason, .. } => {
            assert_eq!(reason, "acceptance criteria not met");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[test]
fn t27_4_invalid_json_triggers_repair_then_done() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    // First turn: invalid JSON → triggers repair; repair turn: valid done JSON
    let bad_json = r#"{ "state": "done", "decision": }"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(bad_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "repair test".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let config = DecisionLoopConfig {
        max_iterations: 3,
        repair_budget: 2,
    };
    let outcome = rt
        .block_on(run_decision_loop(&client, frame, config))
        .expect("loop should not error");
    assert!(
        matches!(outcome, LoopOutcome::Done { .. }),
        "expected Done after repair, got {outcome:?}"
    );
}

#[test]
fn t27_4_max_iterations_reached_when_always_continue() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_planning = r#"{"state":"planning","decision":"continue"}"#;
    let continue_executing = r#"{"state":"executing","decision":"continue"}"#;
    let continue_verifying = r#"{"state":"verifying","decision":"continue"}"#;
    // 3 progress-making continue turns → max_iterations=3 → MaxIterationsReached
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(continue_planning.into())],
        vec![StreamEvent::TextDelta(continue_executing.into())],
        vec![StreamEvent::TextDelta(continue_verifying.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "never finishes".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let config = DecisionLoopConfig {
        max_iterations: 3,
        repair_budget: 1,
    };
    let outcome = rt
        .block_on(run_decision_loop(&client, frame, config))
        .expect("loop should not error");
    assert!(
        matches!(
            outcome,
            LoopOutcome::MaxIterationsReached {
                last_state: AgentState::Verifying,
                ..
            }
        ),
        "expected MaxIterationsReached, got {outcome:?}"
    );
}

#[test]
fn t27_4_noop_continue_stops_without_repeating_prompt() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::{StreamEvent, UsageEvent};

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_json =
        r#"{"state":"executing","decision":"continue","needed_context":[],"state_patch":{}}"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![
            StreamEvent::TextDelta(continue_json.into()),
            StreamEvent::Usage(UsageEvent {
                model: "scripted".into(),
                input_tokens: 100,
                output_tokens: 10,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            }),
        ],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "same prompt should not be resent".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig {
                max_iterations: 5,
                repair_budget: 1,
            },
        ))
        .expect("loop should not error");
    match outcome {
        LoopOutcome::NoProgress {
            last_state,
            reason,
            usage,
        } => {
            assert_eq!(last_state, AgentState::Executing);
            assert!(reason.contains("no StateFrame progress"));
            assert_eq!(usage.input_tokens, 100);
            assert_eq!(usage.output_tokens, 10);
        }
        other => panic!("expected NoProgress, got {other:?}"),
    }
}

#[test]
fn t27_4_request_context_hydrates_typed_selector_before_done() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["file_snippet:src/core/state_frame_projection.rs"]}"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(request_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "update src/core/state_frame_projection.rs".into(),
        open_items: vec!["tests pass".into()],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![
            "fact: file_facts ref=filefact:1 path=src/core/state_frame_projection.rs kind=target_file source=step_objective freshness=current confidence=1.00 fact=target file".into(),
        ],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_4_request_context_budget_deferred_still_counts_as_progress() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let request_json = r#"{"state":"executing","decision":"request_context","needed_context":["test_failure","change_ref:src/core/state_frame_projection.rs","symbol:BossCoordinator"]}"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(request_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "update src/core/state_frame_projection.rs around BossCoordinator".into(),
        open_items: vec!["tests pass".into()],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![
            "fact: file_facts ref=filefact:1 path=src/core/state_frame_projection.rs kind=target_file source=step_objective freshness=current confidence=1.00 symbol=BossCoordinator fact=target file".into(),
            "fact: recent_changes_in_files ref=change:1 path=src/core/state_frame_projection.rs source=worker_result freshness=after-worker-output confidence=0.90 summary=updated src/core/state_frame_projection.rs".into(),
            "fact: test_failures ref=test:1 name=worker_reported_tests status=failed source=worker_result freshness=after-worker-output confidence=0.85 summary=tests failed in boss_flow".into(),
        ],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget {
            max_input_tokens: 250,
            ..StateBudget::default()
        },
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_4_continue_with_state_patch_is_progress() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{"accepted_summary_add":["drafted rollout summary"]}}"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(continue_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "allow patch-driven progress".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig {
                max_iterations: 5,
                repair_budget: 1,
            },
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_4_continue_with_patch_alias_is_progress() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{"accepted_summary":["drafted rollout summary"]}}"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(continue_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "allow alias-driven progress".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig {
                max_iterations: 5,
                repair_budget: 1,
            },
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_4_continue_clearing_open_items_auto_completes() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::{StreamEvent, UsageEvent};

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_json = r#"{
        "state":"executing",
        "decision":"continue",
        "state_patch":{"open_items_remove":["write final report"]}
    }"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::TextDelta(continue_json.into()),
        StreamEvent::Usage(UsageEvent {
            input_tokens: 120,
            output_tokens: 18,
            cache_read_input_tokens: 64,
            cache_creation_input_tokens: 32,
            model: "scripted".into(),
        }),
    ]]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "finish readonly report".into(),
        open_items: vec!["write final report".into()],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![
            "fact: execution_mode read_only_analysis no_file_edits no_patch".into(),
        ],
        allowed_actions: vec!["read_file".into(), "summarize_findings".into()],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig {
                max_iterations: 5,
                repair_budget: 1,
            },
        ))
        .expect("loop should not error");
    match outcome {
        LoopOutcome::Done { final_state, usage } => {
            assert_eq!(final_state, AgentState::Done);
            assert_eq!(usage.input_tokens, 120);
            assert_eq!(usage.output_tokens, 18);
            assert_eq!(usage.cache_read_tokens, 64);
            assert_eq!(usage.cache_write_tokens, 32);
        }
        other => panic!("expected Done after clearing open_items, got {other:?}"),
    }
}

#[test]
fn t27_4_readonly_audit_contract_repairs_short_summary() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let invalid_done = r#"{
        "state":"done",
        "decision":"done",
        "state_patch":{"accepted_summary_add":["too short"]}
    }"#;
    let repaired_done = r#"{
        "state":"done",
        "decision":"done",
        "state_patch":{
            "accepted_summary_add":[
                "现状：当前 LisM 以只读 StateFrame 模式总结任务。",
                "主要风险：projection 漏事实与 prefix 不稳定会影响质量与成本。",
                "证据来源：结论基于提供文档摘录与当前 StateFrame 事实。",
                "下一步建议：继续保留 fallback ladder 并监控 cache 与 schema 漂移。"
            ]
        }
    }"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(invalid_done.into())],
        vec![StreamEvent::TextDelta(repaired_done.into())],
    ]);
    let frame = StateFrame {
        role: ActorRole::Worker,
        state: AgentState::Executing,
        objective: "write readonly audit".into(),
        open_items: vec!["Task completed successfully.".into()],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![
            "fact: execution_mode read_only_analysis no_file_edits no_patch".into(),
        ],
        allowed_actions: vec!["read_file".into(), "summarize_findings".into()],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: Some("readonly_audit_4_paragraphs_v1".into()),
        budget: StateBudget::default(),
    };
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig {
                max_iterations: 5,
                repair_budget: 1,
            },
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[tokio::test]
async fn report_progress_includes_lism_routed_metadata_for_completed_step() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "LisM routed metadata")]),
        "test_boss_report_lism_routed_metadata.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));

    let config_dir = std::env::temp_dir().join("lism_report_metadata_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("lism-report-session", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("lism-report-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    let _ = coordinator.advance_plan(&app_state).await.unwrap();
    let report = coordinator.report_progress(&task_manager).await.unwrap();

    assert_eq!(report.steps.len(), 1);
    let meta = report.steps[0]
        .routed_metadata
        .as_ref()
        .expect("routed_metadata must be set for LisM step");
    assert_eq!(meta.toolset_id.as_deref(), Some("worker-minimal"));
    assert_eq!(meta.skillset_id, None);
    assert_eq!(meta.model_tier.as_deref(), Some("medium"));
    assert_eq!(meta.provider_profile_id.as_deref(), Some("worker-override"));
    assert!(
        meta.state_frame_size.unwrap_or(0) > 0,
        "state_frame_size must be non-zero"
    );
    assert_eq!(meta.cache_read_tokens, Some(0));
    assert_eq!(meta.cache_write_tokens, Some(0));
    assert_eq!(meta.fallback_count, Some(0));
    assert_eq!(meta.projection_mismatch_count, Some(0));

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn report_progress_does_not_fill_routed_metadata_for_non_lism_path() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "Legacy report path")]),
        "test_boss_report_non_lism_no_routed_metadata.json",
    )
    .await;

    {
        let mut plan = coordinator.plan.write().await;
        let plan = plan.as_mut().unwrap();
        plan.steps[0].status = BossPlanStepStatus::Running;
    }

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(report.steps.len(), 1);
    assert_eq!(report.steps[0].routed_metadata, None);

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t27_r1_worker_override_hit_report_shows_routed_metadata() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "R1.1 override metadata")]),
        "test_t27_r1_override_metadata.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));

    let config_dir = std::env::temp_dir().join("t27_r1_override_metadata_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("t27-r1-session", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t27-r1-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    let result = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(result.is_some(), "advance_plan must return a message");

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(report.steps.len(), 1);

    let meta = report.steps[0]
        .routed_metadata
        .as_ref()
        .expect("routed_metadata must be populated for worker-override hit");

    assert_eq!(
        meta.provider_profile_id.as_deref(),
        Some("worker-override"),
        "provider_profile_id must reflect the router-produced override"
    );
    assert_eq!(meta.model_tier.as_deref(), Some("medium"));
    assert!(
        meta.state_frame_size.unwrap_or(0) > 0,
        "state_frame_size must be non-zero"
    );
    assert_eq!(meta.fallback_count, Some(0));
    assert_eq!(meta.projection_mismatch_count, Some(0));
    assert_eq!(meta.cache_read_tokens, Some(0));
    assert_eq!(meta.cache_write_tokens, Some(0));

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn t27_r1_report_observability_summary_aggregates_routed_steps() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "step A"), boss_step(1, "step B")]),
        "test_t27_r1_obs_summary.json",
    )
    .await;

    // Two mock server connections — one per step.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server_n(listener, 2));

    let config_dir = std::env::temp_dir().join("t27_r1_obs_summary_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("t27-r1-obs-session", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t27-r1-obs-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    // Advance both steps.
    coordinator.advance_plan(&app_state).await.unwrap();
    coordinator.advance_plan(&app_state).await.unwrap();

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    let summary = report
        .observability_summary
        .as_ref()
        .expect("observability_summary must be Some when LisM steps are routed");

    assert_eq!(summary.total_steps_routed, 2);
    assert_eq!(
        summary.override_hit_count, 2,
        "both steps use worker-override profile"
    );
    assert_eq!(
        summary
            .model_tier_counts
            .get("medium")
            .copied()
            .unwrap_or(0),
        2
    );
    assert_eq!(summary.total_fallback_count, 0);
    assert_eq!(summary.total_projection_mismatch_count, 0);

    server.await.expect("mock server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn t27_r1_report_observability_summary_none_for_non_lism_path() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "non-lism step")]),
        "test_t27_r1_obs_none.json",
    )
    .await;

    let app_state = app_state_with_tasks("t27-r1-obs-none-session", task_manager.clone());
    // LisM NOT enabled — non-LisM path.

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert!(
        report.observability_summary.is_none(),
        "observability_summary must be None when no steps have been routed"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t27_r1_lism_status_shows_summary_line() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "status summary step")]),
        "test_t27_r1_lism_status_summary.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));

    let config_dir = std::env::temp_dir().join("t27_r1_lism_status_summary_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("t27-r1-status-session", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t27-r1-status-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    coordinator.advance_plan(&app_state).await.unwrap();

    // Simulate /LisM status by reading the snapshot directly.
    let metadata = coordinator.routed_step_metadata_snapshot().await;
    assert!(
        !metadata.is_empty(),
        "metadata must be populated after advance_plan"
    );

    // Verify the summary line would contain total_steps_routed: 1.
    let total_routed = metadata.values().count();
    let override_hits = metadata
        .values()
        .filter(|m| m.provider_profile_id.is_some())
        .count();
    assert_eq!(total_routed, 1);
    assert_eq!(override_hits, 1);

    server.await.expect("mock server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

// ── cache_hit_ratio / token aggregation tests ────────────────────────────────

#[tokio::test]
async fn t27_r1_cache_hit_ratio_none_when_both_zero() {
    use rust_agent::core::boss_state::BossObservabilitySummary;
    let summary = BossObservabilitySummary::default();
    assert_eq!(summary.cache_hit_ratio(), None);
    assert_eq!(summary.estimated_tokens_saved(), 0);
}

#[tokio::test]
async fn t27_r1_cache_hit_ratio_computed_correctly() {
    use rust_agent::core::boss_state::BossObservabilitySummary;
    let summary = BossObservabilitySummary {
        total_cache_read_tokens: 300,
        total_cache_write_tokens: 100,
        ..Default::default()
    };
    let ratio = summary
        .cache_hit_ratio()
        .expect("ratio must be Some when tokens > 0");
    assert!((ratio - 0.75).abs() < 1e-9, "expected 0.75, got {ratio}");
    assert_eq!(summary.estimated_tokens_saved(), 300);
}

#[tokio::test]
async fn t27_r1_cache_hit_ratio_zero_reads() {
    use rust_agent::core::boss_state::BossObservabilitySummary;
    let summary = BossObservabilitySummary {
        total_cache_read_tokens: 0,
        total_cache_write_tokens: 500,
        ..Default::default()
    };
    let ratio = summary
        .cache_hit_ratio()
        .expect("ratio must be Some when write > 0");
    assert!((ratio - 0.0).abs() < 1e-9, "expected 0.0, got {ratio}");
    assert_eq!(summary.estimated_tokens_saved(), 0);
}

#[tokio::test]
async fn t27_r1_observability_summary_aggregates_token_fields() {
    // Verify report_progress aggregates input_tokens + output_tokens across steps.
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "tok-step-0"), boss_step(1, "tok-step-1")]),
        "test_t27_r1_token_agg.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        run_minimal_openai_mock_server_n(listener, 2).await;
    });

    let config_dir = std::env::temp_dir().join("t27_r1_token_agg_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("t27-r1-token-agg", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t27-r1-token-agg".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    // Advance both steps.
    coordinator.advance_plan(&app_state).await.unwrap();
    coordinator.advance_plan(&app_state).await.unwrap();

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    let summary = report
        .observability_summary
        .expect("summary must be Some after LisM steps");

    // v1 stubs: input/output are 0, but fields must be present and aggregated.
    assert_eq!(
        summary.total_input_tokens, 0,
        "v1 stub: input_tokens always 0"
    );
    assert_eq!(
        summary.total_output_tokens, 0,
        "v1 stub: output_tokens always 0"
    );
    assert_eq!(
        summary.estimated_cost_micros_usd, 0,
        "v1 stub: cost always 0"
    );
    assert_eq!(summary.total_steps_routed, 2);

    server.await.expect("mock server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn t27_r1_format_report_includes_hit_ratio_and_tokens_saved() {
    use rust_agent::core::boss_state::BossPlanStepStatus;
    use rust_agent::core::boss_state::{
        BossActorHandle, BossActorRole, BossObservabilitySummary, BossReportPayload, BossStage,
        BossStepReport, BossStepRoutedMetadata,
    };

    let summary = BossObservabilitySummary {
        total_steps_routed: 1,
        total_cache_read_tokens: 400,
        total_cache_write_tokens: 100,
        override_hit_count: 1,
        total_input_tokens: 0,
        total_uncached_input_tokens: 0,
        total_output_tokens: 0,
        estimated_cost_micros_usd: 0,
        ..Default::default()
    };

    let make_handle = |id: &str, role| BossActorHandle::new(id, id, role);
    let payload = BossReportPayload {
        stage: BossStage::Execution,
        current_step: Some(0),
        total_steps: Some(1),
        designer_a: make_handle("a", BossActorRole::DesignerA),
        executor_b: make_handle("b", BossActorRole::ExecutorB),
        active_children: vec![],
        steps: vec![BossStepReport {
            id: 0,
            status: BossPlanStepStatus::Completed,
            worker_task_id: None,
            attempt_count: 1,
            last_review_summary: None,
            action_required: None,
            blocker_reason: None,
            routed_metadata: Some(BossStepRoutedMetadata {
                toolset_id: None,
                skillset_id: None,
                model_tier: Some("medium".into()),
                provider_profile_id: Some("worker-override".into()),
                state_frame_size: Some(512),
                cache_read_tokens: Some(400),
                cache_write_tokens: Some(100),
                fallback_count: Some(0),
                projection_mismatch_count: Some(0),
                input_tokens: Some(0),
                uncached_input_tokens: Some(0),
                output_tokens: Some(0),
                original_prompt_chars: Some(0),
                sent_prompt_chars: Some(0),
                estimated_cost_micros_usd: Some(0),
            }),
        }],
        history_summary: vec![],
        observability_summary: Some(summary),
        lism_policy: rust_agent::core::boss_state::BossLisMPolicy::Inherit,
    };

    let report = payload.format_report();
    assert!(
        report.contains("hit_ratio=80.0%"),
        "expected hit_ratio in report, got: {report}"
    );
    assert!(
        report.contains("tokens_saved=400"),
        "expected tokens_saved in report, got: {report}"
    );
    assert!(
        report.contains("input=0"),
        "expected input= in report, got: {report}"
    );
    assert!(
        report.contains("output=0"),
        "expected output= in report, got: {report}"
    );
}

// ── BossLisMPolicy precedence tests ─────────────────────────────────────────

#[tokio::test]
async fn t27_r1_boss_lism_policy_inherit_follows_session_toggle() {
    // Inherit + session=on → LisM path (routed_metadata populated)
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "inherit-on")]),
        "test_t27_r1_policy_inherit_on.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));
    let config_dir = std::env::temp_dir().join("t27_r1_policy_inherit_on");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("t27-r1-inherit-on", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t27-r1-inherit-on".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    // policy = Inherit (default), session = on → LisM path
    coordinator.advance_plan(&app_state).await.unwrap();
    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(
        report.lism_policy,
        rust_agent::core::boss_state::BossLisMPolicy::Inherit
    );
    assert!(
        report.steps[0].routed_metadata.is_some(),
        "Inherit+session_on must use LisM path"
    );

    server.await.expect("mock server");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn t27_r1_boss_lism_policy_force_on_ignores_session_off() {
    // ForceOn + session=off → LisM path (routed_metadata populated)
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "force-on")]),
        "test_t27_r1_policy_force_on.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));
    let config_dir = std::env::temp_dir().join("t27_r1_policy_force_on");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks("t27-r1-force-on", task_manager.clone())).clone();
    // session toggle is OFF
    app.permission_context.set_lism_enabled(false);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t27-r1-force-on".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    // Boss policy = ForceOn → overrides session=off
    coordinator
        .set_lism_policy(rust_agent::core::boss_state::BossLisMPolicy::ForceOn)
        .await;

    coordinator.advance_plan(&app_state).await.unwrap();
    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(
        report.lism_policy,
        rust_agent::core::boss_state::BossLisMPolicy::ForceOn
    );
    assert!(
        report.steps[0].routed_metadata.is_some(),
        "ForceOn must use LisM path even when session toggle is off"
    );

    server.await.expect("mock server");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn t27_r1_boss_lism_policy_force_off_ignores_session_on() {
    // ForceOff + session=on → non-LisM path (routed_metadata is None)
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "force-off")]),
        "test_t27_r1_policy_force_off.json",
    )
    .await;

    let mut app = (*app_state_with_tasks("t27-r1-force-off", task_manager.clone())).clone();
    // session toggle is ON
    app.permission_context.set_lism_enabled(true);
    let app_state = Arc::new(app);

    // Boss policy = ForceOff → overrides session=on
    coordinator
        .set_lism_policy(rust_agent::core::boss_state::BossLisMPolicy::ForceOff)
        .await;

    coordinator.advance_plan(&app_state).await.unwrap();
    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(
        report.lism_policy,
        rust_agent::core::boss_state::BossLisMPolicy::ForceOff
    );
    assert!(
        report.steps[0].routed_metadata.is_none(),
        "ForceOff must use non-LisM path even when session toggle is on"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t27_r1_report_lism_policy_field_reflects_coordinator_policy() {
    // Verify report.lism_policy always reflects the coordinator's current policy.
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "policy-report")]),
        "test_t27_r1_policy_report.json",
    )
    .await;

    let app_state = app_state_with_tasks("t27-r1-policy-report", task_manager.clone());

    // Default: Inherit
    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(
        report.lism_policy,
        rust_agent::core::boss_state::BossLisMPolicy::Inherit
    );

    // Set ForceOn
    coordinator
        .set_lism_policy(rust_agent::core::boss_state::BossLisMPolicy::ForceOn)
        .await;
    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(
        report.lism_policy,
        rust_agent::core::boss_state::BossLisMPolicy::ForceOn
    );

    // Set ForceOff
    coordinator
        .set_lism_policy(rust_agent::core::boss_state::BossLisMPolicy::ForceOff)
        .await;
    let report = coordinator.report_progress(&task_manager).await.unwrap();
    assert_eq!(
        report.lism_policy,
        rust_agent::core::boss_state::BossLisMPolicy::ForceOff
    );

    let _ = std::fs::remove_file(plan_path);
    let _ = app_state;
}

#[tokio::test]
async fn t27_r2_boss_mode_end_to_end_worker_lism_modes_propagate_to_spawn_payload() {
    async fn run_case(
        case_name: &str,
        boss_policy: rust_agent::core::boss_state::BossLisMPolicy,
        worker_policy: WorkerLisMPolicy,
    ) -> String {
        let mut step = boss_step(0, case_name);
        step.objective = Some(format!(
            "创建目标文件：/tmp/{case_name}_boss_mode_worker_lism/report.md"
        ));
        step.acceptance = vec!["Task completed successfully.".into()];
        let (coordinator, plan_path) = coordinator_with_plan(
            boss_plan(vec![step]),
            &format!("test_boss_worker_lism_{case_name}.json"),
        )
        .await;

        coordinator.set_lism_policy(boss_policy).await;
        coordinator.set_worker_lism_policy(worker_policy).await;

        let app_state = app_state(&format!("worker-lism-{case_name}"));
        let result = coordinator
            .advance_plan(&app_state)
            .await
            .expect("advance_plan should succeed")
            .expect("dispatch should return payload");

        assert_eq!(coordinator.lism_policy().await, boss_policy);
        assert!(result.contains(&format!("\"lism_policy\":\"{}\"", worker_policy.as_str())));

        let _ = std::fs::remove_file(plan_path);
        let _ = std::fs::remove_dir_all(format!("/tmp/{case_name}_boss_mode_worker_lism"));
        result
    }

    let all_off = run_case(
        "all_off",
        rust_agent::core::boss_state::BossLisMPolicy::ForceOff,
        WorkerLisMPolicy::ForceOff,
    )
    .await;
    assert!(all_off.contains("\"lism_policy\":\"force-off\""));

    let boss_on_only = run_case(
        "boss_on_only",
        rust_agent::core::boss_state::BossLisMPolicy::ForceOn,
        WorkerLisMPolicy::ForceOff,
    )
    .await;
    assert!(boss_on_only.contains("\"lism_policy\":\"force-off\""));

    let all_on = run_case(
        "all_on",
        rust_agent::core::boss_state::BossLisMPolicy::ForceOn,
        WorkerLisMPolicy::ForceOn,
    )
    .await;
    assert!(all_on.contains("\"lism_policy\":\"force-on\""));
}

// ── R2 provider routing integration tests ────────────────────────────────────

/// Verifies that when models.toml has two profiles (default + worker-override)
/// pointing to different mock servers, the LisM path selects worker-override
/// (because route_model_tier(M, Worker, Executing) → provider_profile_id = "worker-override")
/// and the request lands on the worker-override server, not the default server.
#[tokio::test]
async fn r2_multi_profile_routing_selects_worker_override_endpoint() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "r2-routing-step")]),
        "test_r2_multi_profile_routing.json",
    )
    .await;

    // Two separate mock servers — one for default, one for worker-override.
    let default_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let default_addr = default_listener.local_addr().unwrap();
    let worker_addr = worker_listener.local_addr().unwrap();

    // The default server should NOT receive any request — if it does, the test
    // will hang (no response) and eventually time out, surfacing the routing bug.
    // We give it a rejected-response mock so a misrouted request fails fast.
    let default_server = tokio::spawn(async move {
        run_minimal_openai_mock_server_rejected(default_listener).await;
    });
    let worker_server = tokio::spawn(async move {
        run_minimal_openai_mock_server(worker_listener).await;
    });

    let config_dir = std::env::temp_dir().join("r2_multi_profile_routing_test");
    write_two_profile_models_toml(
        &config_dir,
        &format!("http://{default_addr}"),
        &format!("http://{worker_addr}"),
    );

    let mut app = (*app_state_with_tasks("r2-routing", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("r2-routing".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    coordinator.advance_plan(&app_state).await.unwrap();

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    let step = &report.steps[0];

    // The step must have completed via the worker-override profile.
    assert_eq!(
        step.routed_metadata
            .as_ref()
            .and_then(|m| m.provider_profile_id.as_deref()),
        Some("worker-override"),
        "expected worker-override profile to be selected"
    );

    worker_server.await.expect("worker mock server finished");
    // default_server may not have been hit — drop it.
    default_server.abort();
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

/// Verifies that when models.toml is absent, the LisM path errors out because
/// route_model_tier returns provider_profile_id = "worker-override" but the
/// registry is unavailable to resolve it.
#[tokio::test]
async fn r2_missing_registry_lism_path_errors_on_profile_override() {
    let task_manager = Arc::new(TaskManager::default());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "r2-fallback-step")]),
        "test_r2_missing_registry.json",
    )
    .await;

    // No models.toml — use a temp dir with no .claude/models.toml.
    let config_dir = std::env::temp_dir().join("r2_missing_registry_test");
    std::fs::create_dir_all(&config_dir).unwrap();

    let mut app = (*app_state_with_tasks("r2-fallback", task_manager.clone())).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("r2-fallback".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    // advance_plan must fail because worker-override profile can't be resolved.
    let result = coordinator.advance_plan(&app_state).await;
    assert!(
        result.is_err(),
        "expected error when registry is absent and profile override is required"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("registry is unavailable") || err.contains("worker-override"),
        "expected registry-unavailable error, got: {err}"
    );

    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn lism_enabled_boss_completed_step_auto_advances_to_next_step() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            boss_step(0, "LisM first"),
            boss_step(1, "LisM second"),
        ]),
        "test_boss_lism_auto_advance.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // serve 2 connections — one per step
    let server = tokio::spawn(async move {
        run_minimal_openai_mock_server_n(listener, 2).await;
    });

    let config_dir = std::env::temp_dir().join("lism_auto_advance_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app = (*app_state_with_tasks(
        "lism-auto-advance-session",
        Arc::new(TaskManager::default()),
    ))
    .clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("lism-auto-advance-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    let _ = coordinator.advance_plan(&app_state).await.unwrap();

    let guard = coordinator.plan.read().await;
    let plan = guard.as_ref().unwrap();
    assert_eq!(plan.steps[0].status, BossPlanStepStatus::Completed);
    assert_eq!(plan.steps[1].status, BossPlanStepStatus::Completed);
    assert!(
        plan.steps[1].completed,
        "second step should auto-complete via existing auto-advance contract"
    );

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn lism_enabled_boss_outcomes_are_persisted_for_reload() {
    let (coordinator_ok, plan_path_ok) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "LisM persisted complete")]),
        "test_boss_lism_persist_complete.json",
    )
    .await;

    let listener_ok = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_ok = listener_ok.local_addr().unwrap();
    let server_ok = tokio::spawn(run_minimal_openai_mock_server(listener_ok));
    let config_dir_ok = std::env::temp_dir().join("lism_persist_ok_test");
    write_worker_override_models_toml(&config_dir_ok, &format!("http://{addr_ok}"));

    let mut app_ok =
        (*app_state_with_tasks("lism-persist-complete", Arc::new(TaskManager::default()))).clone();
    app_ok.permission_context.set_lism_enabled(true);
    app_ok.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app_ok.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("lism-persist-complete".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir_ok.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_ok = Arc::new(app_ok);
    let _ = coordinator_ok.advance_plan(&app_ok).await.unwrap();
    let persisted_ok = load_plan(&plan_path_ok).await.unwrap();
    assert_eq!(persisted_ok.steps[0].status, BossPlanStepStatus::Completed);
    assert!(persisted_ok.steps[0].completed);
    server_ok.await.expect("mock provider server finished");

    let (coordinator_fail, plan_path_fail) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "LisM persisted fail")]),
        "test_boss_lism_persist_fail.json",
    )
    .await;

    let reject_json = r#"{"state":"blocked","decision":"reject","next_action":{"action_type":"reject","args":{"reason":"state frame output does not satisfy acceptance"}}}"#;
    let listener_fail = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_fail = listener_fail.local_addr().unwrap();
    let server_fail = tokio::spawn(run_mock_server_with_json_content(
        listener_fail,
        reject_json.to_string(),
    ));
    let config_dir_fail = std::env::temp_dir().join("lism_persist_fail_test");
    write_worker_override_models_toml(&config_dir_fail, &format!("http://{addr_fail}"));

    let mut app_fail =
        (*app_state_with_tasks("lism-persist-fail", Arc::new(TaskManager::default()))).clone();
    app_fail.permission_context.set_lism_enabled(true);
    app_fail.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app_fail.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("lism-persist-fail".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir_fail.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_fail = Arc::new(app_fail);
    let _ = coordinator_fail.advance_plan(&app_fail).await.unwrap();
    let persisted_fail = load_plan(&plan_path_fail).await.unwrap();
    assert_eq!(persisted_fail.steps[0].status, BossPlanStepStatus::Failed);
    assert_eq!(
        persisted_fail.steps[0].last_review_summary.as_deref(),
        Some("state frame output does not satisfy acceptance")
    );
    server_fail.await.expect("mock provider server finished");

    let _ = std::fs::remove_file(plan_path_ok);
    let _ = std::fs::remove_file(plan_path_fail);
    let _ = std::fs::remove_dir_all(config_dir_ok);
    let _ = std::fs::remove_dir_all(config_dir_fail);
}

#[tokio::test]
async fn legacy_boss_auto_advance_still_completes_next_step_after_completion() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![
            boss_step(0, "legacy first"),
            boss_step(1, "legacy second"),
        ]),
        "test_boss_legacy_auto_advance_contract.json",
    )
    .await;

    {
        let mut guard = coordinator.plan.write().await;
        let plan = guard.as_mut().unwrap();
        plan.steps[0].completed = true;
        plan.steps[0].status = BossPlanStepStatus::Completed;
    }

    let app_state = app_state_with_tasks(
        "legacy-auto-advance-session",
        Arc::new(TaskManager::default()),
    );
    let result = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(
        result.is_some(),
        "legacy path should still continue dispatch after a completed step"
    );

    let guard = coordinator.plan.read().await;
    let plan = guard.as_ref().unwrap();
    assert_eq!(plan.steps[1].status, BossPlanStepStatus::Running);

    let _ = std::fs::remove_file(plan_path);
}

#[test]
fn t27_6_build_routed_state_frame_executor_b_executing_uses_executor_edit_route() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_orchestrator::build_routed_state_frame;

    let plan = make_plan_with_step(0, "execute", vec!["criterion".into()]);
    let frame = build_routed_state_frame(&plan, BossStage::Execution, 0, ActorRole::ExecutorB);

    assert_eq!(frame.toolset_id.as_deref(), Some("executor-edit"));
    assert!(frame.allowed_actions.iter().any(|a| a == "edit_file"));
    assert!(frame.allowed_actions.iter().any(|a| a == "run_test"));
}

#[test]
fn t27_6_build_routed_state_frame_blocked_and_done_clear_tools_and_actions() {
    use rust_agent::core::boss_state::{BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_orchestrator::build_routed_state_frame;

    let blocked_plan = make_t278_plan(vec![make_t278_step(
        0,
        BossPlanStepStatus::Pending,
        false,
        vec![],
    )]);
    let blocked_frame = build_routed_state_frame(
        &blocked_plan,
        BossStage::WaitingForApproval,
        0,
        ActorRole::DesignerA,
    );
    assert_eq!(blocked_frame.toolset_id, None);
    assert!(blocked_frame.allowed_actions.is_empty());

    let done_plan = make_t278_plan(vec![make_t278_step(
        0,
        BossPlanStepStatus::Completed,
        true,
        vec![],
    )]);
    let done_frame =
        build_routed_state_frame(&done_plan, BossStage::Completed, 0, ActorRole::Worker);
    assert_eq!(done_frame.toolset_id, None);
    assert!(done_frame.allowed_actions.is_empty());
}

#[test]
fn t27_7_1_routed_frame_executor_b_executing_carries_expected_model_route() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_model_router::ModelTier;
    use rust_agent::core::state_frame_orchestrator::build_routed_state_frame_with_model_route;

    let plan = make_plan_with_step(0, "execute", vec!["criterion".into()]);
    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::ExecutorB,
    );

    assert_eq!(routed.frame.toolset_id.as_deref(), Some("executor-edit"));
    assert_eq!(routed.model_route.tier, ModelTier::Medium);
    assert_eq!(routed.model_route.provider_profile_id, None);
}

// ── T27.5 StateFrame orchestrator seam ───────────────────────────────────

#[tokio::test]
async fn lism_enabled_boss_advance_plan_marks_step_completed_via_state_frame_path() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "LisM state-frame step")]),
        "test_boss_lism_completed.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));

    let config_dir = std::env::temp_dir().join("lism_completed_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app =
        (*app_state_with_tasks("lism-complete-session", Arc::new(TaskManager::default()))).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("lism-complete-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    let result = coordinator.advance_plan(&app_state).await.unwrap();
    assert_eq!(
        result.as_deref(),
        Some("LisM executed boss step 0 to completion.")
    );

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Completed);
    assert!(step.completed);

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn lism_enabled_boss_advance_plan_marks_step_failed_with_reason() {
    let sink = new_shared_ab_sink();
    let plan_path = std::env::temp_dir().join("test_boss_lism_failed.json");
    save_plan(
        &boss_plan(vec![boss_step(0, "LisM failing step")]),
        &plan_path,
    )
    .await
    .unwrap();
    let owner = Arc::new(rust_agent::core::boss_runtime::BossRuntimeOwner::default());
    let coordinator = Arc::new(
        BossCoordinator::restore_or_init_with_owner(&plan_path, owner)
            .await
            .unwrap()
            .with_lism_ab_sink(sink.clone()),
    );

    let reject_json = r#"{"state":"blocked","decision":"reject","next_action":{"action_type":"reject","args":{"reason":"state frame output does not satisfy acceptance"}}}"#;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_mock_server_with_json_content(
        listener,
        reject_json.to_string(),
    ));

    let config_dir = std::env::temp_dir().join("lism_failed_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app =
        (*app_state_with_tasks("lism-fail-session", Arc::new(TaskManager::default()))).clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("lism-fail-session".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    let app_state = Arc::new(app);

    let result = coordinator.advance_plan(&app_state).await.unwrap();
    assert!(
        result
            .as_deref()
            .is_some_and(|text| text.contains("LisM failed boss step 0"))
    );

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Failed);
    assert!(!step.completed);
    assert_eq!(
        step.last_review_summary.as_deref(),
        Some("state frame output does not satisfy acceptance")
    );
    assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    assert!(coordinator.has_terminal_failure().await);
    assert_eq!(sink.record_count(), 1);
    let records = sink.records();
    assert_eq!(records[0].outcome, BossTestRunOutcome::Aborted);

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

fn make_plan_with_step(
    step_id: usize,
    description: &str,
    acceptance: Vec<String>,
) -> rust_agent::core::boss_state::BossPlan {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus};
    BossPlan {
        plan_id: format!("p-t275-{step_id}"),
        task_description: "orchestrator seam test".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![BossPlanStep {
            id: step_id,
            description: description.into(),
            objective: None,
            acceptance,
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            review_task_id: None,
        }],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn t27_5_runtime_override_live_seam_uses_resolved_snapshot_client() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{
        StepOutcome, StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
        run_routed_step_with_runtime,
    };

    let inherited = make_inherited_runtime_snapshot_with_scripted_turns(vec![]);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));
    let registry = make_step_model_registry_with_base_url(&format!("http://{}", addr));
    let plan = make_orchestrator_route_override_plan(0);
    // router produces provider_profile_id=Some("worker-override") for (Worker, Executing, M)
    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::Worker,
    );
    assert_eq!(
        routed.model_route.provider_profile_id.as_deref(),
        Some("worker-override")
    );

    let runtime = StepRuntimeResolutionContext {
        inherited_snapshot: &inherited,
        model_registry: Some(&registry),
        observability: rust_agent::service::observability::ServiceObservabilityTracker::default(),
    };

    let outcome = run_routed_step_with_runtime(routed, DecisionLoopConfig::default(), runtime)
        .await
        .expect("runtime-aware seam should succeed");

    assert!(matches!(outcome, StepOutcome::Completed { .. }));
    assert!(
        inherited.client.is_scripted(),
        "parent snapshot should remain scripted"
    );
    assert_eq!(
        inherited.active_profile_name.as_deref(),
        Some("inherited-fast")
    );

    server.await.expect("mock provider server finished");
}

#[test]
fn t27_5_runtime_override_missing_registry_fails_step_without_mutating_parent_snapshot() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{
        StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let inherited = make_inherited_runtime_snapshot_with_scripted_turns(vec![]);
    let original_profile = inherited.active_profile_name.clone();
    let original_source = inherited.source.clone();
    let original_config = inherited.config.clone();
    let plan = make_orchestrator_route_override_plan(0);
    // router produces worker-override for (Worker, Executing, M)
    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::Worker,
    );
    assert_eq!(
        routed.model_route.provider_profile_id.as_deref(),
        Some("worker-override")
    );

    let runtime = StepRuntimeResolutionContext {
        inherited_snapshot: &inherited,
        model_registry: None,
        observability: rust_agent::service::observability::ServiceObservabilityTracker::default(),
    };

    let error = rt
        .block_on(
            rust_agent::core::state_frame_orchestrator::run_routed_step_with_runtime(
                routed,
                DecisionLoopConfig::default(),
                runtime,
            ),
        )
        .expect_err("missing registry should fail");

    assert!(
        error
            .to_string()
            .contains("model profile registry is unavailable")
    );
    assert_eq!(inherited.active_profile_name, original_profile);
    assert_eq!(inherited.source, original_source);
    assert_eq!(inherited.config, original_config);
}

#[test]
fn t27_5_runtime_override_unknown_profile_fails_step_without_mutating_parent_snapshot() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_model_router::ModelRoute;
    use rust_agent::core::state_frame_orchestrator::{
        StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let inherited = make_inherited_runtime_snapshot_with_scripted_turns(vec![]);
    let original_profile = inherited.active_profile_name.clone();
    let original_source = inherited.source.clone();
    let original_config = inherited.config.clone();
    let registry = make_step_model_registry();
    let plan = make_orchestrator_route_override_plan(0);
    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::Worker,
    );
    let routed = rust_agent::core::state_frame_orchestrator::RoutedStateFrame {
        frame: routed.frame,
        model_route: ModelRoute {
            tier: routed.model_route.tier,
            provider_profile_id: Some("missing-profile".into()),
        },
    };

    let runtime = StepRuntimeResolutionContext {
        inherited_snapshot: &inherited,
        model_registry: Some(&registry),
        observability: rust_agent::service::observability::ServiceObservabilityTracker::default(),
    };

    let error = rt
        .block_on(
            rust_agent::core::state_frame_orchestrator::run_routed_step_with_runtime(
                routed,
                DecisionLoopConfig::default(),
                runtime,
            ),
        )
        .expect_err("unknown profile should fail");

    assert!(
        error
            .to_string()
            .contains("failed to resolve step model profile 'missing-profile'")
    );
    assert_eq!(inherited.active_profile_name, original_profile);
    assert_eq!(inherited.source, original_source);
    assert_eq!(inherited.config, original_config);
}

#[test]
fn t27_5_done_loop_outcome_maps_to_completed() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);
    let plan = make_plan_with_step(0, "implement feature", vec!["tests pass".into()]);
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            0,
            ActorRole::Worker,
            DecisionLoopConfig::default(),
        ))
        .expect("should not error");
    assert!(matches!(outcome, StepOutcome::Completed { .. }));
}

#[test]
fn t27_5_external_effect_step_fails_without_stateframe_tool_dispatch() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);
    let plan = make_plan_with_step(
        0,
        "创建 demo；目标文件：/tmp/rust-agent-stateframe-tool-dispatch-required.md",
        vec!["Task completed successfully.".into()],
    );
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            0,
            ActorRole::Worker,
            DecisionLoopConfig::default(),
        ))
        .expect("should not error");
    match outcome {
        StepOutcome::Failed { reason, usage } => {
            assert!(reason.contains("cannot yet perform required filesystem"));
            assert!(usage.is_none());
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn t27_5_rejected_loop_outcome_maps_to_failed_with_reason() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reject_json = r#"{"state":"blocked","decision":"reject","next_action":{"action_type":"reject","args":{"reason":"output does not meet criteria"}}}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        reject_json.into(),
    )]]);
    let plan = make_plan_with_step(1, "verify output", vec![]);
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            1,
            ActorRole::Verifier,
            DecisionLoopConfig::default(),
        ))
        .expect("should not error");
    match outcome {
        StepOutcome::Failed { reason, .. } => {
            assert!(reason.contains("output does not meet criteria"))
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn t27_5_max_iterations_maps_to_failed() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_planning = r#"{"state":"planning","decision":"continue"}"#;
    let continue_executing = r#"{"state":"executing","decision":"continue"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(continue_planning.into())],
        vec![StreamEvent::TextDelta(continue_executing.into())],
    ]);
    let plan = make_plan_with_step(2, "never finishes", vec![]);
    let config = DecisionLoopConfig {
        max_iterations: 2,
        repair_budget: 1,
    };
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            2,
            ActorRole::Worker,
            config,
        ))
        .expect("should not error");
    match outcome {
        StepOutcome::Failed { reason, .. } => {
            assert!(reason.contains("max iterations"), "reason: {reason}")
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn t27_5_no_progress_failure_preserves_usage() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::{StreamEvent, UsageEvent};

    let rt = tokio::runtime::Runtime::new().unwrap();
    let continue_json = r#"{"state":"executing","decision":"continue","state_patch":{}}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![
        StreamEvent::TextDelta(continue_json.into()),
        StreamEvent::Usage(UsageEvent {
            model: "scripted".into(),
            input_tokens: 321,
            output_tokens: 12,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 128,
        }),
    ]]);
    let plan = make_plan_with_step(2, "no-op continue", vec![]);
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            2,
            ActorRole::Worker,
            DecisionLoopConfig {
                max_iterations: 5,
                repair_budget: 1,
            },
        ))
        .expect("should not error");
    match outcome {
        StepOutcome::Failed {
            reason,
            usage: Some(usage),
        } => {
            assert!(
                reason.contains("no StateFrame progress"),
                "reason: {reason}"
            );
            assert_eq!(usage.input_tokens, 321);
            assert_eq!(usage.uncached_input_tokens, 193);
            assert_eq!(usage.output_tokens, 12);
            assert_eq!(usage.cache_read_tokens, 128);
        }
        other => panic!("expected Failed with usage, got {other:?}"),
    }
}

#[test]
fn t27_5_repair_exhausted_maps_to_failed() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let bad_json = r#"not json at all"#;
    // 1 initial + 1 repair attempt = repair_budget=1 exhausted
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(bad_json.into())],
        vec![StreamEvent::TextDelta(bad_json.into())],
    ]);
    let plan = make_plan_with_step(3, "bad model output", vec![]);
    let config = DecisionLoopConfig {
        max_iterations: 3,
        repair_budget: 1,
    };
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            3,
            ActorRole::Worker,
            config,
        ))
        .expect("should not error");
    match outcome {
        StepOutcome::Failed { reason, .. } => {
            assert!(reason.contains("repair exhausted"), "reason: {reason}")
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

// ── T27.6 Toolset / skillset router ──────────────────────────────────────

fn make_state_frame(
    role: rust_agent::core::state_frame::ActorRole,
    state: rust_agent::core::state_frame::AgentState,
) -> rust_agent::core::state_frame::StateFrame {
    use rust_agent::core::state_frame::{StateBudget, StateFrame};
    StateFrame {
        role,
        state,
        objective: "test".into(),
        open_items: vec![],
        blocked_items: vec![],
        accepted_summary: vec![],
        recent_evidence: vec![],
        allowed_actions: vec![],
        toolset_id: None,
        skillset_id: None,
        required_output_schema: None,
        budget: StateBudget::default(),
    }
}

#[test]
fn t27_6_designer_a_planning_gets_spec_writer_toolset() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::route_toolset;

    let frame = make_state_frame(ActorRole::DesignerA, AgentState::Planning);
    let route = route_toolset(&frame);
    assert_eq!(route.toolset_id.as_deref(), Some("designer-planning"));
    assert_eq!(route.skillset_id.as_deref(), Some("spec-writer"));
    assert!(route.allowed_actions.contains(&"write_spec".to_string()));
}

#[test]
fn t27_6_executor_b_executing_gets_edit_toolset() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::route_toolset;

    let frame = make_state_frame(ActorRole::ExecutorB, AgentState::Executing);
    let route = route_toolset(&frame);
    assert_eq!(route.toolset_id.as_deref(), Some("executor-edit"));
    assert!(route.allowed_actions.contains(&"edit_file".to_string()));
    assert!(route.allowed_actions.contains(&"run_test".to_string()));
}

#[test]
fn t27_6_verifier_gets_readonly_toolset() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::route_toolset;

    let frame = make_state_frame(ActorRole::Verifier, AgentState::Verifying);
    let route = route_toolset(&frame);
    assert_eq!(route.toolset_id.as_deref(), Some("verifier-readonly"));
    assert_eq!(route.skillset_id.as_deref(), Some("acceptance-checker"));
    assert!(!route.allowed_actions.contains(&"edit_file".to_string()));
}

#[test]
fn t27_6_blocked_state_clears_all_actions_for_any_role() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::route_toolset;

    for role in [
        ActorRole::DesignerA,
        ActorRole::ExecutorB,
        ActorRole::Worker,
        ActorRole::Verifier,
        ActorRole::Summarizer,
    ] {
        let frame = make_state_frame(role, AgentState::Blocked);
        let route = route_toolset(&frame);
        assert!(
            route.toolset_id.is_none(),
            "role {role:?} blocked should have no toolset"
        );
        assert!(
            route.allowed_actions.is_empty(),
            "role {role:?} blocked should have no actions"
        );
    }
}

#[test]
fn t27_6_done_state_clears_all_actions() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::route_toolset;

    let frame = make_state_frame(ActorRole::Worker, AgentState::Done);
    let route = route_toolset(&frame);
    assert!(route.toolset_id.is_none());
    assert!(route.allowed_actions.is_empty());
}

#[test]
fn t27_6_unknown_state_falls_back_to_readonly() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::route_toolset;

    // Worker in Verifying state — not a natural combination, should get conservative fallback.
    let frame = make_state_frame(ActorRole::Worker, AgentState::Verifying);
    let route = route_toolset(&frame);
    assert!(route.toolset_id.is_none());
    assert_eq!(route.allowed_actions, vec!["read_file"]);
    assert!(!route.allowed_actions.contains(&"edit_file".to_string()));
}

#[test]
fn t27_6_apply_route_fills_frame_fields() {
    use rust_agent::core::state_frame::{ActorRole, AgentState};
    use rust_agent::core::state_frame_router::{apply_route, route_toolset};

    let mut frame = make_state_frame(ActorRole::ExecutorB, AgentState::Executing);
    let route = route_toolset(&frame);
    apply_route(&mut frame, route);
    assert_eq!(frame.toolset_id.as_deref(), Some("executor-edit"));
    assert!(frame.allowed_actions.contains(&"edit_file".to_string()));
}

// ── T27.7 StateFrame archive / retention ─────────────────────────────────

#[test]
fn t27_7_build_accepted_archive_excludes_current_step() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus};
    use rust_agent::core::state_frame_archive::build_accepted_archive;

    let make_step = |id: usize, status: BossPlanStepStatus, completed: bool| BossPlanStep {
        id,
        description: format!("step {id}"),
        objective: None,
        acceptance: vec![format!("criterion {id}")],
        requires_approval: false,
        status,
        completed,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };

    let plan = BossPlan {
        plan_id: "p-t277".into(),
        task_description: "archive test".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![
            make_step(0, BossPlanStepStatus::Completed, true),
            make_step(1, BossPlanStepStatus::Completed, true),
            make_step(2, BossPlanStepStatus::Running, false),
        ],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    // current step = 1 → only step 0 should be in archive
    let archive = build_accepted_archive(&plan, Some(1));
    assert_eq!(archive.len(), 1);
    assert_eq!(archive[0].step_id, 0);
    assert_eq!(archive[0].description, "step 0");
}

#[test]
fn t27_7_retain_open_items_filters_already_satisfied_criteria() {
    use rust_agent::core::state_frame_archive::{AcceptedItem, retain_open_items};

    let archive = vec![AcceptedItem {
        step_id: 0,
        description: "step 0".into(),
        acceptance_criteria: vec!["tests pass".into(), "no regressions".into()],
    }];

    // "tests pass" is already in archive → should be filtered out
    let open = retain_open_items(&["tests pass".into(), "add documentation".into()], &archive);
    assert_eq!(open, vec!["add documentation"]);
}

#[test]
fn t27_7_retain_blocked_items_waiting_for_approval() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame_archive::retain_blocked_items;

    let blocked = retain_blocked_items(BossStage::WaitingForApproval, &[]);
    assert_eq!(blocked, vec!["waiting for user approval"]);

    let not_blocked = retain_blocked_items(BossStage::Execution, &[]);
    assert!(not_blocked.is_empty());
}

#[test]
fn t27_7_projection_uses_archive_for_accepted_summary() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_projection::project_state_frame;

    let make_step = |id: usize, status: BossPlanStepStatus, completed: bool| BossPlanStep {
        id,
        description: format!("step {id} description"),
        objective: None,
        acceptance: vec![],
        requires_approval: false,
        status,
        completed,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };

    let plan = BossPlan {
        plan_id: "p-t277b".into(),
        task_description: "projection archive test".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![
            make_step(0, BossPlanStepStatus::Completed, true),
            make_step(1, BossPlanStepStatus::Completed, true),
            make_step(2, BossPlanStepStatus::Running, false),
        ],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Execution, Some(2), ActorRole::Worker);
    // steps 0 and 1 are completed and not current → both in accepted_summary
    assert_eq!(frame.accepted_summary.len(), 2);
    assert!(
        frame
            .accepted_summary
            .contains(&"step 0 description".to_string())
    );
    assert!(
        frame
            .accepted_summary
            .contains(&"step 1 description".to_string())
    );
    // current step 2 must NOT appear in accepted_summary
    assert!(!frame.accepted_summary.iter().any(|s| s.contains("step 2")));
}

#[test]
fn t27_7_open_items_excludes_criteria_already_in_archive() {
    use rust_agent::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_projection::project_state_frame;

    let completed_step = BossPlanStep {
        id: 0,
        description: "step 0".into(),
        objective: None,
        acceptance: vec!["shared criterion".into()],
        requires_approval: false,
        status: BossPlanStepStatus::Completed,
        completed: true,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };
    let current_step = BossPlanStep {
        id: 1,
        description: "step 1".into(),
        objective: None,
        // "shared criterion" already satisfied in step 0; "new criterion" is genuinely open
        acceptance: vec!["shared criterion".into(), "new criterion".into()],
        requires_approval: false,
        status: BossPlanStepStatus::Running,
        completed: false,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    };
    let plan = BossPlan {
        plan_id: "p-t277c".into(),
        task_description: "open items filter test".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps: vec![completed_step, current_step],
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    };

    let frame = project_state_frame(&plan, BossStage::Execution, Some(1), ActorRole::Worker);
    // "shared criterion" is already in archive → filtered out
    assert_eq!(frame.open_items, vec!["new criterion"]);
}

// ── T27.8 Production-path tests ───────────────────────────────────────────

fn make_t278_plan(
    steps: Vec<rust_agent::core::boss_state::BossPlanStep>,
) -> rust_agent::core::boss_state::BossPlan {
    rust_agent::core::boss_state::BossPlan {
        plan_id: "p-t278".into(),
        task_description: "production path test".into(),
        document_spec: String::new(),
        pseudo_code: String::new(),
        steps,
        accepted_by_user: true,
        auto_sequence: true,
        ..Default::default()
    }
}

fn make_t278_step(
    id: usize,
    status: rust_agent::core::boss_state::BossPlanStepStatus,
    completed: bool,
    acceptance: Vec<String>,
) -> rust_agent::core::boss_state::BossPlanStep {
    rust_agent::core::boss_state::BossPlanStep {
        id,
        description: format!("step {id}"),
        objective: Some(format!("objective for step {id}")),
        acceptance,
        requires_approval: false,
        status,
        completed,
        result_diff: None,
        worker_task_id: None,
        attempt_count: 1,
        retry_budget: 3,
        last_review_summary: None,
        last_correction: None,
        review_task_id: None,
    }
}

#[test]
fn t27_8_full_pipeline_project_route_loop_done() {
    use rust_agent::core::boss_state::{BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::core::state_frame_projection::project_state_frame;
    use rust_agent::core::state_frame_router::{apply_route, route_toolset};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done","confidence":0.95}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);

    let plan = make_t278_plan(vec![make_t278_step(
        0,
        BossPlanStepStatus::Running,
        false,
        vec!["write tests".into()],
    )]);

    // Full pipeline: project → route → loop
    let mut frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
    let route = route_toolset(&frame);
    apply_route(&mut frame, route);

    // Verify route was applied
    assert_eq!(frame.toolset_id.as_deref(), Some("worker-minimal"));
    assert!(frame.allowed_actions.contains(&"edit_file".to_string()));

    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(
        matches!(outcome, LoopOutcome::Done { .. }),
        "expected Done, got {outcome:?}"
    );
}

#[test]
fn t27_8_archive_retention_affects_loop_input() {
    use rust_agent::core::boss_state::{BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::core::state_frame_projection::project_state_frame;
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);

    let plan = make_t278_plan(vec![
        make_t278_step(
            0,
            BossPlanStepStatus::Completed,
            true,
            vec!["shared criterion".into()],
        ),
        make_t278_step(
            1,
            BossPlanStepStatus::Running,
            false,
            vec!["shared criterion".into(), "new criterion".into()],
        ),
    ]);

    let frame = project_state_frame(&plan, BossStage::Execution, Some(1), ActorRole::Worker);

    // Archive retention: "shared criterion" already satisfied in step 0 → filtered from open_items
    assert_eq!(frame.open_items, vec!["new criterion"]);
    // Step 0 in accepted_summary, step 1 (current) not
    assert!(frame.accepted_summary.contains(&"step 0".to_string()));
    assert!(!frame.accepted_summary.iter().any(|s| s.contains("step 1")));

    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_8_blocked_route_produces_empty_actions_loop_still_runs() {
    use rust_agent::core::boss_state::{BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::core::state_frame_projection::project_state_frame;
    use rust_agent::core::state_frame_router::{apply_route, route_toolset};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);

    let plan = make_t278_plan(vec![make_t278_step(
        0,
        BossPlanStepStatus::Running,
        false,
        vec![],
    )]);

    let mut frame = project_state_frame(
        &plan,
        BossStage::WaitingForApproval,
        Some(0),
        ActorRole::DesignerA,
    );
    let route = route_toolset(&frame);
    apply_route(&mut frame, route);

    // Blocked state: route clears all actions
    assert!(frame.allowed_actions.is_empty());
    assert!(frame.toolset_id.is_none());
    assert_eq!(frame.blocked_items, vec!["waiting for user approval"]);

    // Loop still runs and terminates cleanly
    let outcome = rt
        .block_on(run_decision_loop(
            &client,
            frame,
            DecisionLoopConfig::default(),
        ))
        .expect("loop should not error");
    assert!(matches!(outcome, LoopOutcome::Done { .. }));
}

#[test]
fn t27_8_repair_path_in_full_pipeline() {
    use rust_agent::core::boss_state::{BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::{DecisionLoopConfig, LoopOutcome, run_decision_loop};
    use rust_agent::core::state_frame_projection::project_state_frame;
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    // First turn: bad JSON → triggers repair; repair turn: valid done
    let bad_json = r#"{ "state": "done", "decision": }"#;
    let done_json = r#"{"state":"done","decision":"done"}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![
        vec![StreamEvent::TextDelta(bad_json.into())],
        vec![StreamEvent::TextDelta(done_json.into())],
    ]);

    let plan = make_t278_plan(vec![make_t278_step(
        0,
        BossPlanStepStatus::Running,
        false,
        vec![],
    )]);

    let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::ExecutorB);
    let config = DecisionLoopConfig {
        max_iterations: 3,
        repair_budget: 2,
    };
    let outcome = rt
        .block_on(run_decision_loop(&client, frame, config))
        .expect("loop should not error");
    assert!(
        matches!(outcome, LoopOutcome::Done { .. }),
        "expected Done after repair, got {outcome:?}"
    );
}

#[test]
fn t27_8_run_step_with_state_frame_end_to_end() {
    use rust_agent::core::boss_state::{BossPlanStepStatus, BossStage};
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{StepOutcome, run_step_with_state_frame};
    use rust_agent::service::api::client::ModelProviderClient;
    use rust_agent::service::api::streaming::StreamEvent;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let done_json = r#"{"state":"done","decision":"done","confidence":1.0}"#;
    let client = ModelProviderClient::with_scripted_turns(vec![vec![StreamEvent::TextDelta(
        done_json.into(),
    )]]);

    let plan = make_t278_plan(vec![
        make_t278_step(
            0,
            BossPlanStepStatus::Completed,
            true,
            vec!["criterion A".into()],
        ),
        make_t278_step(
            1,
            BossPlanStepStatus::Running,
            false,
            vec!["criterion A".into(), "criterion B".into()],
        ),
    ]);

    // run_step_with_state_frame uses project_state_frame internally:
    // - archive filters "criterion A" from open_items (already in step 0)
    // - only "criterion B" remains open
    let outcome = rt
        .block_on(run_step_with_state_frame(
            &client,
            &plan,
            BossStage::Execution,
            1,
            ActorRole::Worker,
            DecisionLoopConfig::default(),
        ))
        .expect("should not error");

    assert!(
        matches!(outcome, StepOutcome::Completed { .. }),
        "expected Completed, got {outcome:?}"
    );
}

// ── T27.7.1 Model tier router ─────────────────────────────────────────────

#[test]
fn t27_7_1_effort_l_maps_to_low() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, EffortLevel};
    use rust_agent::core::state_frame_model_router::{ModelTier, route_model_tier};

    let route = route_model_tier(EffortLevel::L, ActorRole::Worker, AgentState::Executing);
    assert_eq!(route.tier, ModelTier::Low);
    assert!(route.provider_profile_id.is_none());
}

#[test]
fn t27_7_1_designer_planning_upgrades_low_to_medium() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, EffortLevel};
    use rust_agent::core::state_frame_model_router::{ModelTier, route_model_tier};

    let route = route_model_tier(EffortLevel::L, ActorRole::DesignerA, AgentState::Planning);
    assert_eq!(route.tier, ModelTier::Medium);
}

#[test]
fn t27_7_1_verifier_verifying_upgrades_low_to_medium() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, EffortLevel};
    use rust_agent::core::state_frame_model_router::{ModelTier, route_model_tier};

    let route = route_model_tier(EffortLevel::L, ActorRole::Verifier, AgentState::Verifying);
    assert_eq!(route.tier, ModelTier::Medium);
}

#[test]
fn t27_7_1_summarizer_caps_high_to_medium() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, EffortLevel};
    use rust_agent::core::state_frame_model_router::{ModelTier, route_model_tier};

    let route = route_model_tier(EffortLevel::H, ActorRole::Summarizer, AgentState::Executing);
    assert_eq!(route.tier, ModelTier::Medium);
}

#[test]
fn t27_7_1_uncovered_combination_uses_effort_default() {
    use rust_agent::core::state_frame::{ActorRole, AgentState, EffortLevel};
    use rust_agent::core::state_frame_model_router::{ModelTier, route_model_tier};

    let route = route_model_tier(EffortLevel::H, ActorRole::Worker, AgentState::Correcting);
    assert_eq!(route.tier, ModelTier::High);
    assert!(route.provider_profile_id.is_none());
}

// ── T27.8 Boss production-path runtime wiring ─────────────────────────────

#[tokio::test]
async fn t27_8_lism_boss_production_path_no_registry_step_fails_with_override_error() {
    // Router now produces worker-override for (Worker, Executing, M).
    // When no registry is available, advance_plan must return an error — not silently fall back.
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "runtime wiring no registry")]),
        "test_boss_t278_no_registry.json",
    )
    .await;

    let mut app =
        (*app_state_with_tasks("t278-no-registry", Arc::new(TaskManager::default()))).clone();
    app.permission_context.set_lism_enabled(true);
    // session is None → cwd defaults to "." → no models.toml → registry is None
    // router produces worker-override → resolver requires registry → must error
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    let app_state = Arc::new(app);

    let result = coordinator.advance_plan(&app_state).await;
    assert!(
        result.is_err(),
        "missing registry with override should return error"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("model profile registry is unavailable"),
        "error should mention registry unavailable"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t27_8_lism_boss_production_path_with_registry_on_disk_uses_worker_override_profile() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "runtime wiring with registry")]),
        "test_boss_t278_with_registry.json",
    )
    .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));

    let config_dir = std::env::temp_dir().join("t278_registry_test");
    write_worker_override_models_toml(&config_dir, &format!("http://{addr}"));

    let mut app =
        (*app_state_with_tasks("t278-with-registry", Arc::new(TaskManager::default()))).clone();
    app.permission_context.set_lism_enabled(true);
    app.session = Some(rust_agent::history::session::SessionSnapshot {
        session_id: rust_agent::history::session::SessionId("t278-with-registry".into()),
        surface: rust_agent::bootstrap::InteractionSurface::Cli,
        session_mode: rust_agent::bootstrap::SessionMode::Headless,
        cwd: config_dir.to_string_lossy().to_string(),
        last_turn_at: None,
        prompt_seed: None,
    });
    // inherited has empty scripted turns — override profile (mock server) is used instead
    app.permission_context.inherited_active_model_snapshot =
        Some(make_inherited_runtime_snapshot_with_scripted_turns(vec![]));
    let app_state = Arc::new(app);

    let _ = coordinator.advance_plan(&app_state).await.unwrap();

    let guard = coordinator.plan.read().await;
    let plan = guard.as_ref().unwrap();
    assert_eq!(plan.steps[0].status, BossPlanStepStatus::Completed);

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(config_dir);
}

#[tokio::test]
async fn t27_8_lism_boss_production_path_missing_inherited_snapshot_returns_error() {
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![boss_step(0, "runtime wiring missing snapshot")]),
        "test_boss_t278_missing_snapshot.json",
    )
    .await;

    let mut app =
        (*app_state_with_tasks("t278-missing-snapshot", Arc::new(TaskManager::default()))).clone();
    app.permission_context.set_lism_enabled(true);
    // inherited_active_model_snapshot is None → boss.rs should return an error
    app.permission_context.inherited_active_model_snapshot = None;
    let app_state = Arc::new(app);

    let result = coordinator.advance_plan(&app_state).await;
    assert!(
        result.is_err(),
        "missing inherited snapshot should return error"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("active model snapshot"),
        "error should mention active model snapshot"
    );

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn t27_8_lism_external_effect_step_falls_back_to_full_worker_path() {
    let mut step = boss_step(0, "external effect fallback");
    step.objective = Some(
        "创建一个工具并写入目标文件：/tmp/t278_lism_external_effect_fallback/report.md".into(),
    );
    step.acceptance = vec!["Task completed successfully.".into()];
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![step]),
        "test_boss_t278_lism_external_effect_fallback.json",
    )
    .await;

    let mut app = (*app_state_with_tasks(
        "t278-external-effect-fallback",
        Arc::new(TaskManager::default()),
    ))
    .clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot = None;
    let app_state = Arc::new(app);

    let message = coordinator
        .advance_plan(&app_state)
        .await
        .expect("fallback path should not error")
        .expect("dispatch should produce a payload");

    assert!(
        message.contains("\"boss_actor_role\":\"executor_b\""),
        "expected full worker spawn payload, got: {message}"
    );
    let metadata = coordinator.routed_step_metadata_snapshot().await;
    let meta = metadata
        .get(&0)
        .expect("fallback should record routed metadata");
    assert_eq!(meta.fallback_count, Some(1));

    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all("/tmp/t278_lism_external_effect_fallback");
}

#[tokio::test]
async fn t27_8_lism_fallback_metadata_does_not_mask_worker_usage() {
    let task_manager = Arc::new(TaskManager::default());
    let mut step = boss_step(0, "external effect fallback usage");
    step.objective = Some("写入目标文件：/tmp/t278_lism_fallback_usage/report.md".into());
    step.acceptance = vec!["Task completed successfully.".into()];
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![step]),
        "test_boss_t278_lism_fallback_usage.json",
    )
    .await;

    let mut app =
        (*app_state_with_tasks("t278-external-effect-fallback-usage", task_manager.clone()))
            .clone();
    app.permission_context.set_lism_enabled(true);
    app.permission_context.inherited_active_model_snapshot = None;
    let app_state = Arc::new(app);

    coordinator
        .advance_plan(&app_state)
        .await
        .expect("fallback path should dispatch full worker");

    let task = task_manager
        .list()
        .into_iter()
        .last()
        .expect("fallback dispatch should create a worker task");
    let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
    task_manager.complete_with_usage(
        &task.id,
        &dispatcher,
        Some(TaskUsageSummary {
            requests: 2,
            input_tokens: 2400,
            uncached_input_tokens: 1800,
            output_tokens: 321,
            cache_creation_input_tokens: 128,
            cache_read_input_tokens: 512,
            original_prompt_chars: 12000,
            sent_prompt_chars: 7600,
            cache_hit_requests: 1,
            estimated_cost_micros_usd: 9876,
        }),
    );

    {
        let mut plan = coordinator.plan.write().await;
        let step = plan
            .as_mut()
            .expect("plan should exist")
            .steps
            .iter_mut()
            .find(|step| step.id == 0)
            .expect("step should exist");
        step.worker_task_id = Some(task.id);
        step.completed = true;
        step.status = BossPlanStepStatus::Completed;
    }

    let report = coordinator.report_progress(&task_manager).await.unwrap();
    let summary = report
        .observability_summary
        .expect("fallback metadata plus worker usage should produce summary");

    assert_eq!(summary.total_steps_routed, 1);
    assert_eq!(summary.total_fallback_count, 1);
    assert_eq!(summary.total_input_tokens, 2400);
    assert_eq!(summary.total_uncached_input_tokens, 1800);
    assert_eq!(summary.total_output_tokens, 321);
    assert_eq!(summary.total_cache_read_tokens, 512);
    assert_eq!(summary.total_cache_write_tokens, 128);
    assert_eq!(summary.total_original_chars, 12000);
    assert_eq!(summary.total_sent_chars, 7600);
    assert_eq!(summary.estimated_cost_micros_usd, 9876);

    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all("/tmp/t278_lism_fallback_usage");
}

#[tokio::test]
async fn t27_8_terminal_failure_transitions_stage_to_documentation_and_surfaces_reason() {
    let mut failed_step = boss_step(0, "artifact verification should fail");
    failed_step.status = BossPlanStepStatus::Failed;
    failed_step.completed = false;
    failed_step.last_review_summary =
        Some("artifact verification failed: target file missing".into());
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![failed_step]),
        "test_boss_t278_terminal_failure_stage.json",
    )
    .await;

    {
        let mut status = coordinator.status.write().await;
        status.stage = BossStage::Execution;
        status.current_step = Some(0);
        status.total_steps = Some(1);
    }

    let app_state = app_state("t278-terminal-failure");
    let message = coordinator
        .advance_plan(&app_state)
        .await
        .expect("advance should succeed")
        .expect("terminal failure should return message");

    assert!(message.contains("terminal step failure"));
    assert!(message.contains("artifact verification failed"));
    assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    assert_eq!(coordinator.status.read().await.current_step, None);

    let _ = std::fs::remove_file(plan_path);
}

#[tokio::test]
async fn r0_boss_full_worker_real_tool_call_creates_artifact_and_completes() {
    let root = std::env::temp_dir().join("r0_boss_full_worker_real_tool_call");
    let output_root = root.join("task_output");
    let artifact_path = root.join("report.md");
    let artifact_content = "hello from the real write tool";
    let request_bodies = Arc::new(std::sync::Mutex::new(Vec::new()));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let request_bodies_for_server = request_bodies.clone();
    let server = tokio::spawn(run_openai_write_tool_loop_mock_server(
        listener,
        request_bodies_for_server,
        artifact_path.to_string_lossy().to_string(),
        artifact_content.to_string(),
    ));

    let mut step = boss_step(0, "real tool call artifact");
    step.objective = Some(format!("创建目标文件：{}", artifact_path.display()));
    step.acceptance = vec!["artifact file exists and is non-empty".into()];
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![step]),
        "r0_boss_full_worker_real_tool_call.json",
    )
    .await;

    let task_manager = Arc::new(TaskManager::new_with_output_root(&output_root));
    let observability = rust_agent::service::observability::ServiceObservabilityTracker::default();
    let runtime_snapshot =
        make_openai_runtime_snapshot_for_base_url(&format!("http://{addr}"), observability);
    let tool_registry = ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(
            rust_agent::tool::builtin::file_write::FileWriteTool,
        ));
    let app_state = app_state_with_boss_worker_runtime(
        "r0-boss-real-tool-call",
        task_manager.clone(),
        coordinator.clone(),
        tool_registry,
        runtime_snapshot,
        &root,
    );
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;
    seed_fake_a_review_session(
        &coordinator,
        task_manager.clone(),
        "r0-boss-real-tool-call",
        "ACCEPT: artifact verified\n",
    )
    .await;

    coordinator.advance_plan(&app_state).await.unwrap();
    wait_for_step_status(&coordinator, 0, BossPlanStepStatus::Completed).await;

    let artifact = std::fs::read_to_string(&artifact_path).expect("artifact should exist");
    assert_eq!(artifact, artifact_content);

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Completed);
    assert!(step.completed, "artifact-backed run must complete");
    assert!(
        step.last_review_summary
            .as_deref()
            .is_some_and(|summary| summary.contains("Objective:")),
        "completed run should retain a concrete review summary"
    );
    drop(guard);

    let bodies = request_bodies.lock().expect("request bodies poisoned");
    assert_eq!(bodies.len(), 2, "expected tool turn plus follow-up turn");
    assert!(
        bodies[0].contains("\"tools\"") && bodies[0].contains("\"Write\""),
        "first request must expose the Write tool schema"
    );
    assert!(
        bodies[1].contains(&format!(
            "tool result for Write: wrote {}",
            artifact_path.display()
        )),
        "follow-up request must carry the concrete write result"
    );
    drop(bodies);

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn r0_boss_full_worker_text_only_completion_fails_artifact_verification() {
    let root = std::env::temp_dir().join("r0_boss_full_worker_text_only_failure");
    let output_root = root.join("task_output");
    let artifact_path = root.join("missing-report.md");
    let request_bodies = Arc::new(std::sync::Mutex::new(Vec::new()));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let request_bodies_for_server = request_bodies.clone();
    let server = tokio::spawn(run_openai_text_only_mock_server(
        listener,
        request_bodies_for_server,
        "done without tools".to_string(),
    ));

    let mut step = boss_step(0, "text-only completion should fail verification");
    step.objective = Some(format!("创建目标文件：{}", artifact_path.display()));
    step.acceptance = vec!["artifact file exists and is non-empty".into()];
    let (coordinator, plan_path) = coordinator_with_plan(
        boss_plan(vec![step]),
        "r0_boss_full_worker_text_only_failure.json",
    )
    .await;

    let task_manager = Arc::new(TaskManager::new_with_output_root(&output_root));
    let observability = rust_agent::service::observability::ServiceObservabilityTracker::default();
    let runtime_snapshot =
        make_openai_runtime_snapshot_for_base_url(&format!("http://{addr}"), observability);
    let tool_registry = ToolRegistry::new()
        .register(Arc::new(AgentTool))
        .register(Arc::new(
            rust_agent::tool::builtin::file_write::FileWriteTool,
        ));
    let app_state = app_state_with_boss_worker_runtime(
        "r0-boss-text-only-failure",
        task_manager.clone(),
        coordinator.clone(),
        tool_registry,
        runtime_snapshot,
        &root,
    );
    coordinator
        .bootstrap_actor_registry_with_app_state(&app_state)
        .await;

    coordinator.advance_plan(&app_state).await.unwrap();
    wait_for_step_status(&coordinator, 0, BossPlanStepStatus::Failed).await;

    assert!(
        !artifact_path.exists(),
        "text-only completion must not create the artifact"
    );

    let guard = coordinator.plan.read().await;
    let step = &guard.as_ref().unwrap().steps[0];
    assert_eq!(step.status, BossPlanStepStatus::Failed);
    assert!(
        step.last_review_summary
            .as_deref()
            .is_some_and(|summary| summary.contains("artifact verification failed")),
        "boss must surface artifact verification failure"
    );
    assert!(
        !step.completed,
        "failed verification must not mark completed"
    );
    drop(guard);

    let bodies = request_bodies.lock().expect("request bodies poisoned");
    assert_eq!(
        bodies.len(),
        1,
        "text-only path should need one model request"
    );
    assert!(
        bodies[0].contains("\"tools\""),
        "worker request must still expose available tools even if model ignores them"
    );
    drop(bodies);

    server.await.expect("mock provider server finished");
    let _ = std::fs::remove_file(plan_path);
    let _ = std::fs::remove_dir_all(root);
}

// ── T27.9 Boss production-path override contract ──────────────────────────
//
// These tests verify the override contract at the orchestrator seam that
// advance_plan() calls. The router now produces provider_profile_id=Some("worker-override")
// for (Worker, Executing, M) — no manual ModelRoute patching needed.

#[tokio::test]
async fn t27_9_boss_production_path_override_hit_uses_resolved_runtime_not_inherited() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{
        StepOutcome, StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
        run_routed_step_with_runtime,
    };

    // inherited has empty scripted turns — if it were used, the loop would fail
    let inherited = make_inherited_runtime_snapshot_with_scripted_turns(vec![]);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_minimal_openai_mock_server(listener));
    let registry = make_step_model_registry_with_base_url(&format!("http://{}", addr));
    let plan = make_orchestrator_route_override_plan(0);

    // router produces provider_profile_id=Some("worker-override") for (Worker, Executing, M)
    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::Worker,
    );
    assert_eq!(
        routed.model_route.provider_profile_id.as_deref(),
        Some("worker-override"),
        "router must produce worker-override for this combination"
    );

    let runtime = StepRuntimeResolutionContext {
        inherited_snapshot: &inherited,
        model_registry: Some(&registry),
        observability: rust_agent::service::observability::ServiceObservabilityTracker::default(),
    };

    let outcome = run_routed_step_with_runtime(routed, DecisionLoopConfig::default(), runtime)
        .await
        .expect("override seam should succeed");

    assert!(
        matches!(outcome, StepOutcome::Completed { .. }),
        "override hit should complete via resolved runtime"
    );
    // parent snapshot must not be mutated
    assert_eq!(
        inherited.active_profile_name.as_deref(),
        Some("inherited-fast")
    );
    assert_eq!(inherited.config.provider_id, "scripted");

    server.await.expect("mock provider server finished");
}

#[tokio::test]
async fn t27_9_boss_production_path_override_missing_registry_step_fails_with_observable_reason() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{
        StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
        run_routed_step_with_runtime,
    };

    let inherited = make_inherited_runtime_snapshot_with_scripted_turns(vec![]);
    let plan = make_orchestrator_route_override_plan(0);

    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::Worker,
    );
    assert_eq!(
        routed.model_route.provider_profile_id.as_deref(),
        Some("worker-override"),
        "router must produce worker-override for this combination"
    );

    let runtime = StepRuntimeResolutionContext {
        inherited_snapshot: &inherited,
        model_registry: None, // registry missing → must fail, not silently fall back
        observability: rust_agent::service::observability::ServiceObservabilityTracker::default(),
    };

    let error = run_routed_step_with_runtime(routed, DecisionLoopConfig::default(), runtime)
        .await
        .expect_err("missing registry should return error");

    assert!(
        error
            .to_string()
            .contains("model profile registry is unavailable"),
        "error reason must be observable: {error}"
    );
    // parent snapshot must not be mutated
    assert_eq!(
        inherited.active_profile_name.as_deref(),
        Some("inherited-fast")
    );
    assert_eq!(inherited.config.provider_id, "scripted");
}

#[tokio::test]
async fn t27_9_boss_production_path_override_rejected_decision_step_fails_with_observable_reason() {
    use rust_agent::core::boss_state::BossStage;
    use rust_agent::core::state_frame::ActorRole;
    use rust_agent::core::state_frame_loop::DecisionLoopConfig;
    use rust_agent::core::state_frame_orchestrator::{
        StepOutcome, StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
        run_routed_step_with_runtime,
    };

    let inherited = make_inherited_runtime_snapshot_with_scripted_turns(vec![]);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock provider listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(run_minimal_openai_mock_server_rejected(listener));
    let registry = make_step_model_registry_with_base_url(&format!("http://{}", addr));
    let plan = make_orchestrator_route_override_plan(0);

    let routed = build_routed_state_frame_with_model_route(
        &plan,
        BossStage::Execution,
        0,
        ActorRole::Worker,
    );
    assert_eq!(
        routed.model_route.provider_profile_id.as_deref(),
        Some("worker-override"),
        "router must produce worker-override for this combination"
    );

    let runtime = StepRuntimeResolutionContext {
        inherited_snapshot: &inherited,
        model_registry: Some(&registry),
        observability: rust_agent::service::observability::ServiceObservabilityTracker::default(),
    };

    let outcome = run_routed_step_with_runtime(routed, DecisionLoopConfig::default(), runtime)
        .await
        .expect("seam should return outcome, not error");

    match outcome {
        StepOutcome::Failed { reason, .. } => {
            assert!(
                !reason.is_empty(),
                "failure reason must be observable, got empty string"
            );
        }
        StepOutcome::Completed { .. } => panic!("expected Failed outcome, got Completed"),
    }
    // parent snapshot must not be mutated
    assert_eq!(
        inherited.active_profile_name.as_deref(),
        Some("inherited-fast")
    );
    assert_eq!(inherited.config.provider_id, "scripted");

    server.await.expect("mock provider server finished");
}

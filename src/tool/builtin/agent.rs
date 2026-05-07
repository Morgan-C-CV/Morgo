use crate::core::concurrency::{
    BossBudgetDecision, current_memory_pressure_level, evaluate_boss_budget,
};
use crate::core::boss_state::{ExecutorBStageMemory, SharedStepMemory};
use async_trait::async_trait;
use serde::Deserialize;

use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
use crate::core::context::{QueryContext, SubagentConfig, WorkerLisMPolicy};
use crate::core::message::Message;
use crate::core::query_loop::{QueryParams, run_query_loop_with_params};
use crate::core::state_frame::StageContinuationContext;
use crate::cost::tracker::CostTracker;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::interaction::telegram::gateway::TelegramGateway;
use crate::security::audit::AuditLog;
use crate::service::compact::reactive_compact::ReactiveCompactor;
use crate::state::active_model_runtime::ActiveModelRuntime;
use crate::state::app_state::{
    ActiveModelProfileSource, ActiveModelProviderSummary, AppState, RuntimeRole, WorkerRole,
};
use crate::state::permission_context::ToolPermissionContext;
use crate::task::types::TaskUsageSummary;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
enum AgentRequest {
    Spawn(SpawnAgentRequest),
    Continue {
        task_id: String,
        message: String,
        boss_step_context: Option<ContinueBossStepContext>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpawnAgentRequest {
    task: String,
    task_contains_boss_context: bool,
    role: WorkerRole,
    inherit_context: bool,
    max_turns: Option<usize>,
    allowed_tools: Option<Vec<String>>,
    reuse_strategy: ReuseStrategy,
    parent_task_id: Option<String>,
    orchestration_group_id: Option<String>,
    phase: Option<crate::task::types::WorkerPhase>,
    step_id: Option<usize>,
    boss_plan_id: Option<String>,
    step_objective: Option<String>,
    step_acceptance: Vec<String>,
    parent_session_id: Option<String>,
    requires_verification: bool,
    lism_policy: WorkerLisMPolicy,
    shared_step_memory: Option<SharedStepMemory>,
    /// When set, the spawned subagent runtime is assembled with this boss actor policy.
    boss_actor_policy: Option<crate::state::permission_context::BossActorPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContinueBossStepContext {
    step_id: Option<usize>,
    boss_plan_id: Option<String>,
    step_objective: Option<String>,
    step_acceptance: Vec<String>,
    parent_session_id: Option<String>,
    continuation_context: Option<StageContinuationContext>,
    executor_b_stage_memory: Option<ExecutorBStageMemory>,
    shared_step_memory: Option<SharedStepMemory>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReuseStrategy {
    RunningOnly,
    Fresh,
}

#[derive(Debug, Deserialize)]
struct AgentJsonRequest {
    task: Option<String>,
    task_contains_boss_context: Option<bool>,
    role: Option<String>,
    inherit_context: Option<bool>,
    max_turns: Option<usize>,
    allowed_tools: Option<Vec<String>>,
    reuse_strategy: Option<String>,
    parent_task_id: Option<String>,
    orchestration_group_id: Option<String>,
    phase: Option<String>,
    step_id: Option<usize>,
    boss_plan_id: Option<String>,
    step_objective: Option<String>,
    step_acceptance: Option<Vec<String>>,
    parent_session_id: Option<String>,
    requires_verification: Option<bool>,
    lism_policy: Option<String>,
    task_id: Option<String>,
    message: Option<String>,
    continuation_payload: Option<StageContinuationContext>,
    executor_b_stage_memory: Option<ExecutorBStageMemory>,
    shared_step_memory: Option<SharedStepMemory>,
    boss_actor_role: Option<String>,
    boss_lineage_depth: Option<u32>,
}

pub struct AgentTool;

#[async_trait]
impl Tool for AgentTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "Agent".into(),
            description: "Launch a subagent with isolated context".into(),
            aliases: &["TaskAgent"],
            search_hint: Some("spawn or continue subagent"),
            read_only: false,
            destructive: false,
            concurrency_safe: false,
            always_load: true,
            should_defer: false,
            requires_auth: true,
            requires_user_interaction: true,
            is_open_world: false,
            is_search_or_read_command: false,
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let tasks = permissions
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared task manager is not configured"))?;
        let session_id = permissions
            .active_session_id
            .clone()
            .unwrap_or_else(|| "local-session".into());

        // Boss spawn policy: only ExecutorB in Execution may spawn child agents.
        if let Some(policy) = &permissions.boss_actor_policy {
            if !policy.may_spawn() {
                anyhow::bail!(
                    "boss spawn policy: {} actors (lineage_depth={}, phase={:?}) may not spawn child agents",
                    policy.actor_role.as_str(),
                    policy.lineage_depth,
                    policy.phase
                );
            }
        }

        let mut request = parse_agent_request(&call.input)?;

        // When ExecutorB spawns a child, auto-inject child depth into the child's policy.
        if let AgentRequest::Spawn(ref mut spawn) = request {
            if let Some(parent_policy) = &permissions.boss_actor_policy {
                if spawn.boss_actor_policy.is_none() {
                    // B is spawning without an explicit child role — default to ImplementChild.
                    use crate::core::boss_state::{BossActorRole, BossStage};
                    use crate::state::permission_context::BossActorPolicy;
                    spawn.boss_actor_policy = Some(BossActorPolicy {
                        actor_role: BossActorRole::ImplementChild,
                        lineage_depth: parent_policy.lineage_depth + 1,
                        phase: BossStage::Execution,
                    });
                } else if let Some(child_policy) = spawn.boss_actor_policy.as_mut() {
                    // Explicit role provided — enforce child role + depth = parent + 1.
                    if !child_policy.actor_role.is_child() {
                        use crate::core::boss_state::BossActorRole;
                        child_policy.actor_role = BossActorRole::ImplementChild;
                    }
                    child_policy.lineage_depth = parent_policy.lineage_depth + 1;
                }
            }
        }

        let parent_context = build_parent_query_context(permissions.clone());
        let dispatcher = parent_context.app_state.notification_dispatcher.clone();

        match request {
            AgentRequest::Spawn(request) => {
                let role_label = request.role.as_str().to_string();
                let task_label = request.task.clone();
                if let Some(policy) = request
                    .boss_actor_policy
                    .as_ref()
                    .or(permissions.boss_actor_policy.as_ref())
                {
                    match evaluate_boss_budget(
                        tasks,
                        request.role,
                        policy.lineage_depth,
                        current_memory_pressure_level(),
                    ) {
                        BossBudgetDecision::Allow => {}
                        BossBudgetDecision::Queue { reason }
                        | BossBudgetDecision::Deny { reason } => {
                            anyhow::bail!(reason);
                        }
                    }
                }
                let action = match request.reuse_strategy {
                    ReuseStrategy::RunningOnly => maybe_reuse_running_task(
                        tasks,
                        &session_id,
                        &request.task,
                        request.role,
                        request.orchestration_group_id.as_deref(),
                    ),
                    ReuseStrategy::Fresh => None,
                };
                if let Some(task_id) = action {
                    return Ok(ToolResult::Text(format!(
                        "agent task {} reused for {} worker: {}",
                        task_id, role_label, task_label
                    )));
                }
                let owner_surface = permissions
                    .active_surface
                    .unwrap_or(InteractionSurface::Cli);
                let task = tasks.create_with_type(
                    format!("Spawned {} worker for {}", role_label, task_label),
                    crate::task::types::TaskType::LocalAgent,
                    session_id.clone(),
                    owner_surface,
                );

                // Concurrency Control: Acquire permit before launching
                let permit = if let Some(limiter) = &permissions.subagent_limiter {
                    info!("Acquiring concurrency permit for subagent {}...", task.id);
                    Some(limiter.acquire().await)
                } else {
                    None
                };

                tasks.set_worker_role(&task.id, request.role);
                tasks.set_parent_task_id(&task.id, request.parent_task_id.clone());
                tasks.set_orchestration_group_id(&task.id, request.orchestration_group_id.clone());
                tasks.set_phase(&task.id, request.phase);
                tasks.set_step_id(&task.id, request.step_id);
                if let Some(policy) = request
                    .boss_actor_policy
                    .as_ref()
                    .or(permissions.boss_actor_policy.as_ref())
                {
                    tasks.set_boss_actor_id(
                        &task.id,
                        Some(format!(
                            "{}:depth={}",
                            policy.actor_role.as_str(),
                            policy.lineage_depth
                        )),
                    );
                }
                if request.requires_verification {
                    tasks.set_validation_state(
                        &task.id,
                        Some(crate::task::types::ValidationState::PendingVerification),
                    );
                }
                crate::coordinator::mode::set_coordinator_mode(true);
                launch_agent_task(
                    tasks.clone(),
                    &parent_context,
                    task.id.clone(),
                    request,
                    permissions,
                    dispatcher,
                    permit,
                );
                Ok(ToolResult::Text(format!(
                    "agent task {} respawned for {} worker: {}",
                    task.id, role_label, task_label
                )))
            }
            AgentRequest::Continue {
                task_id,
                message,
                boss_step_context,
            } => {
                let message = build_continue_task_input(&message, boss_step_context.as_ref());
                if !tasks.send_message(&task_id, &session_id, message.clone()) {
                    anyhow::bail!("task {task_id} is not running or not owned by this session");
                }
                Ok(ToolResult::Text(format!(
                    "agent task {} accepted message {}",
                    task_id, message
                )))
            }
        }
    }
}

fn launch_agent_task(
    tasks: std::sync::Arc<crate::task::manager::TaskManager>,
    parent_context: &QueryContext,
    task_id: String,
    request: SpawnAgentRequest,
    permissions: &ToolPermissionContext,
    dispatcher: NotificationDispatcher,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) {
    let task_input = build_worker_task_input(&request);
    let effective_max_turns = effective_max_turns(&request);
    let query_context = parent_context.create_subagent_context(
        task_id.clone(),
        permissions
            .subagent_scripted_turns
            .clone()
            .unwrap_or_default(),
        SubagentConfig {
            worker_role: request.role,
            inherit_context: request.inherit_context,
            max_turns: request.max_turns,
            allowed_tools: request.allowed_tools.clone(),
            lism_policy: request.lism_policy,
            boss_actor_policy: request.boss_actor_policy,
        },
    );
    if request.role == WorkerRole::Implement {
        for expectation in crate::core::boss_acceptance::extract_artifact_expectations(&task_input)
        {
            query_context
                .app_state
                .permission_context
                .add_delegated_write_path(expectation.path);
        }
    }
    let tasks_for_run = tasks.clone();
    let launched_task_id = task_id.clone();
    tasks.launch(&launched_task_id.clone(), task_input.clone(), async move {
        // Hold the permit for the duration of this async block
        let _permit = permit;

        let mut params = QueryParams::default();
        params.max_turns = effective_max_turns;
        params.initial_max_output_tokens = effective_max_output_tokens(&request);
        let usage_before = query_context.app_state.cost_tracker.snapshot();
        let result =
            run_query_loop_with_params(&query_context, Message::user(task_input.clone()), params)
                .await;
        let usage_after = query_context.app_state.cost_tracker.snapshot();
        let usage_delta = usage_after.delta_since(&usage_before);
        let usage_summary = usage_delta.has_usage().then_some(TaskUsageSummary {
            requests: usage_delta.requests,
            input_tokens: usage_delta.input_tokens,
            uncached_input_tokens: usage_delta.uncached_input_tokens,
            output_tokens: usage_delta.output_tokens,
            cache_creation_input_tokens: usage_delta.cache_creation_input_tokens,
            cache_read_input_tokens: usage_delta.cache_read_input_tokens,
            original_prompt_chars: usage_delta.original_prompt_chars,
            sent_prompt_chars: usage_delta.sent_prompt_chars,
            cache_hit_requests: usage_delta.cache_hit_requests,
            estimated_cost_micros_usd: usage_delta.estimated_cost_micros_usd,
        });

        append_subagent_output(
            &tasks_for_run,
            &launched_task_id,
            request.role,
            &task_input,
            &result.messages,
        );

        let artifact_verification = if request.role == WorkerRole::Implement {
            crate::core::boss_acceptance::verify_artifact_expectations(&task_input)
        } else {
            Ok(())
        };

        if !matches!(
            result.terminal,
            crate::core::query_loop::Terminal::Completed
        ) || matches!(
            result.state,
            crate::core::query_loop::QueryLoopState::Failed
                | crate::core::query_loop::QueryLoopState::Interrupted
                | crate::core::query_loop::QueryLoopState::Compacting
        ) {
            tasks_for_run.fail_with_usage(&launched_task_id, &dispatcher, usage_summary);
        } else if let Err(reason) = artifact_verification {
            tasks_for_run.append_output(
                &launched_task_id,
                format!("worker artifact verification failed: {reason}\n"),
            );
            tasks_for_run.fail_with_usage(&launched_task_id, &dispatcher, usage_summary);
        } else {
            tasks_for_run.complete_with_usage(&launched_task_id, &dispatcher, usage_summary);
        }
    });
}

fn effective_max_turns(request: &SpawnAgentRequest) -> Option<usize> {
    request.max_turns.or(match request.role {
        WorkerRole::Research => None,
        WorkerRole::Verify => request.step_id.map(|_| 6),
        WorkerRole::Implement => request.step_id.map(|_| 64),
    })
}

fn effective_max_output_tokens(request: &SpawnAgentRequest) -> Option<u64> {
    match request.role {
        WorkerRole::Verify => Some(1024),
        WorkerRole::Research | WorkerRole::Implement => None,
    }
}

fn parse_agent_request(input: &str) -> anyhow::Result<AgentRequest> {
    if let Ok(request) = serde_json::from_str::<AgentJsonRequest>(input) {
        if let (Some(task_id), Some(message)) = (request.task_id, request.message) {
            let boss_step_context = if request.step_id.is_some()
                || request.boss_plan_id.is_some()
                || request.step_objective.is_some()
                || request.step_acceptance.as_ref().is_some_and(|items| !items.is_empty())
                || request.parent_session_id.is_some()
                || request.continuation_payload.is_some()
                || request.executor_b_stage_memory.is_some()
                || request.shared_step_memory.is_some()
            {
                Some(ContinueBossStepContext {
                    step_id: request.step_id,
                    boss_plan_id: request.boss_plan_id,
                    step_objective: request.step_objective,
                    step_acceptance: request.step_acceptance.unwrap_or_default(),
                    parent_session_id: request.parent_session_id,
                    continuation_context: request.continuation_payload,
                    executor_b_stage_memory: request.executor_b_stage_memory,
                    shared_step_memory: request.shared_step_memory,
                })
            } else {
                None
            };
            return Ok(AgentRequest::Continue {
                task_id,
                message,
                boss_step_context,
            });
        }
        if let Some(task) = request.task {
            let role = parse_worker_role(request.role.as_deref())?;
            let boss_actor_policy = parse_boss_actor_policy(
                request.boss_actor_role.as_deref(),
                request.boss_lineage_depth,
            )?;
            return Ok(AgentRequest::Spawn(SpawnAgentRequest {
                task,
                task_contains_boss_context: request.task_contains_boss_context.unwrap_or(false),
                role,
                inherit_context: request.inherit_context.unwrap_or(true),
                max_turns: request.max_turns,
                allowed_tools: request.allowed_tools,
                reuse_strategy: parse_reuse_strategy(request.reuse_strategy.as_deref(), role)?,
                parent_task_id: request.parent_task_id,
                orchestration_group_id: request.orchestration_group_id,
                phase: parse_worker_phase(request.phase.as_deref())?,
                step_id: request.step_id,
                boss_plan_id: request.boss_plan_id,
                step_objective: request.step_objective,
                step_acceptance: request.step_acceptance.unwrap_or_default(),
                parent_session_id: request.parent_session_id,
                requires_verification: request.requires_verification.unwrap_or(false),
                lism_policy: parse_worker_lism_policy(request.lism_policy.as_deref(), role)?,
                shared_step_memory: request.shared_step_memory,
                boss_actor_policy,
            }));
        }
        anyhow::bail!("agent JSON input must include either task or task_id/message")
    }

    if let Some(rest) = input.strip_prefix("continue:") {
        let mut parts = rest.splitn(2, ':');
        let task_id = parts.next().unwrap_or_default().trim().to_string();
        let message = parts.next().unwrap_or_default().trim().to_string();
        return Ok(AgentRequest::Continue {
            task_id,
            message,
            boss_step_context: None,
        });
    }

    Ok(AgentRequest::Spawn(SpawnAgentRequest {
        task: input.to_string(),
        task_contains_boss_context: false,
        role: WorkerRole::Research,
        inherit_context: true,
        max_turns: None,
        allowed_tools: None,
        reuse_strategy: ReuseStrategy::RunningOnly,
        parent_task_id: None,
        orchestration_group_id: None,
        phase: None,
        step_id: None,
        boss_plan_id: None,
        step_objective: None,
        step_acceptance: Vec::new(),
        parent_session_id: None,
        requires_verification: false,
        lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Research),
        shared_step_memory: None,
        boss_actor_policy: None,
    }))
}

fn build_worker_task_input(request: &SpawnAgentRequest) -> String {
    if request.task_contains_boss_context {
        return with_verify_output_contract(request.role, request.task.clone());
    }

    let mut sections = vec![request.task.clone()];

    if request.boss_plan_id.is_some()
        || request.step_id.is_some()
        || request.step_objective.is_some()
        || !request.step_acceptance.is_empty()
        || request.parent_session_id.is_some()
        || request.shared_step_memory.is_some()
    {
        sections.push("<boss-step-context>".into());
        if let Some(plan_id) = request.boss_plan_id.as_deref() {
            sections.push(format!("plan_id: {plan_id}"));
        }
        if let Some(step_id) = request.step_id {
            sections.push(format!("step_id: {step_id}"));
        }
        if let Some(objective) = request.step_objective.as_deref() {
            sections.push(format!("objective: {objective}"));
        }
        if !request.step_acceptance.is_empty() {
            sections.push("acceptance:".into());
            sections.extend(
                request
                    .step_acceptance
                    .iter()
                    .map(|item| format!("- {item}")),
            );
        }
        if let Some(parent_session_id) = request.parent_session_id.as_deref() {
            sections.push(format!("parent_session_id: {parent_session_id}"));
        }
        if let Some(memory) = request.shared_step_memory.as_ref() {
            sections.extend(render_shared_step_memory_section(memory));
        }
        sections.push("</boss-step-context>".into());
    }

    if request.role == WorkerRole::Verify {
        sections.push("<verify-output-contract>".into());
        sections.push(
            "return exactly five short fields only: verified_target, verification_result, minimal_evidence, remaining_blocker, evidence_refs"
                .into(),
        );
        sections.push(
            "do not include analysis, summary prose, recommendations, next_action, file lists, validation steps, risk notes, multi-section report formatting, or evidence prose"
                .into(),
        );
        sections.push(
            "keep minimal_evidence to one short factual phrase, keep remaining_blocker to a single short blocker or none, and set evidence_refs to read:<verified_target> when verification_result is verified"
                .into(),
        );
        sections.push("</verify-output-contract>".into());
    }

    with_verify_output_contract(request.role, sections.join("\n"))
}

fn render_shared_step_memory_section(memory: &SharedStepMemory) -> Vec<String> {
    let mut sections = vec!["shared_step_memory:".into()];
    if let Some(step_id) = memory.step_id {
        sections.push(format!("step_id: {step_id}"));
    }
    if let Some(role) = memory.worker_role.as_deref() {
        sections.push(format!("worker_role: {role}"));
    }
    if let Some(target) = memory.target.as_deref() {
        sections.push(format!("target: {target}"));
    }
    if let Some(required_action) = memory.required_action.as_deref() {
        sections.push(format!("required_action: {required_action}"));
    }
    if !memory.verified_facts.is_empty() {
        sections.push("verified_facts:".into());
        sections.extend(memory.verified_facts.iter().map(|item| format!("- {item}")));
    }
    if let Some(remaining_blocker) = memory.remaining_blocker.as_deref() {
        sections.push(format!("remaining_blocker: {remaining_blocker}"));
    }
    if !memory.evidence_refs.is_empty() {
        sections.push("evidence_refs:".into());
        sections.extend(memory.evidence_refs.iter().map(|item| format!("- {item}")));
    }
    sections
}

fn render_worker_local_memory_section(memory: &ExecutorBStageMemory) -> Vec<String> {
    let mut sections = vec!["worker_local_memory:".into()];
    if let Some(continuity) = memory.continuity.as_ref() {
        sections.push(format!(
            "continuity: {}",
            format!("{continuity:?}").to_ascii_lowercase()
        ));
    }
    if !memory.recent_reads.is_empty() {
        sections.push("recent_reads:".into());
        sections.extend(memory.recent_reads.iter().map(|item| format!("- {item}")));
    }
    if !memory.recent_edits.is_empty() {
        sections.push("recent_edits:".into());
        sections.extend(memory.recent_edits.iter().map(|item| format!("- {item}")));
    }
    if !memory.recent_test_refs.is_empty() {
        sections.push("recent_test_refs:".into());
        sections.extend(
            memory
                .recent_test_refs
                .iter()
                .map(|item| format!("- {item}")),
        );
    }
    if !memory.recent_verification_refs.is_empty() {
        sections.push("recent_verification_refs:".into());
        sections.extend(
            memory
                .recent_verification_refs
                .iter()
                .map(|item| format!("- {item}")),
        );
    }
    if !memory.failed_targets.is_empty() {
        sections.push("failed_targets:".into());
        sections.extend(memory.failed_targets.iter().map(|item| format!("- {item}")));
    }
    if !memory.verified_targets.is_empty() {
        sections.push("verified_targets:".into());
        sections.extend(memory.verified_targets.iter().map(|item| format!("- {item}")));
    }
    sections
}

fn with_verify_output_contract(role: WorkerRole, task_input: String) -> String {
    if role != WorkerRole::Verify || task_input.contains("<verify-output-contract>") {
        return task_input;
    }

    format!(
        "{task_input}\n<verify-output-contract>\nreturn exactly five short fields only: verified_target, verification_result, minimal_evidence, remaining_blocker, evidence_refs\ndo not include analysis, summary prose, recommendations, next_action, file lists, validation steps, risk notes, multi-section report formatting, or evidence prose\nkeep minimal_evidence to one short factual phrase, keep remaining_blocker to a single short blocker or none, and set evidence_refs to read:<verified_target> when verification_result is verified\n</verify-output-contract>"
    )
}

fn build_continue_task_input(
    message: &str,
    boss_step_context: Option<&ContinueBossStepContext>,
) -> String {
    let Some(context) = boss_step_context else {
        return message.to_string();
    };
    let mut sections = vec![message.to_string()];
    if context.step_id.is_some()
        || context.boss_plan_id.is_some()
        || context.step_objective.is_some()
        || !context.step_acceptance.is_empty()
        || context.parent_session_id.is_some()
        || context.continuation_context.is_some()
        || context.shared_step_memory.is_some()
        || context.executor_b_stage_memory.is_some()
    {
        sections.push("<boss-step-context>".into());
        if let Some(plan_id) = context.boss_plan_id.as_deref() {
            sections.push(format!("plan_id: {plan_id}"));
        }
        if let Some(step_id) = context.step_id {
            sections.push(format!("step_id: {step_id}"));
        }
        if let Some(objective) = context.step_objective.as_deref() {
            sections.push(format!("objective: {objective}"));
        }
        if !context.step_acceptance.is_empty() {
            sections.push("acceptance:".into());
            sections.extend(context.step_acceptance.iter().map(|item| format!("- {item}")));
        }
        if let Some(parent_session_id) = context.parent_session_id.as_deref() {
            sections.push(format!("parent_session_id: {parent_session_id}"));
        }
        if let Some(continuation) = context.continuation_context.as_ref() {
            sections.push("stage_continuation_context:".into());
            sections.push(format!(
                "failed_target: {}",
                continuation.failed_target.as_deref().unwrap_or("none")
            ));
            sections.push(format!(
                "next_action: {}",
                continuation.next_action.as_deref().unwrap_or("none")
            ));
            sections.push(format!(
                "continuity_mode: {}",
                continuation
                    .continuity_mode
                    .as_ref()
                    .map(|mode| format!("{mode:?}").to_ascii_lowercase())
                    .unwrap_or_else(|| "none".into())
            ));
            if !continuation.verified_facts.is_empty() {
                sections.push("verified_facts:".into());
                sections.extend(
                    continuation
                        .verified_facts
                        .iter()
                        .map(|fact| format!("- {fact}")),
                );
            }
        }
        if let Some(memory) = context.shared_step_memory.as_ref() {
            sections.extend(render_shared_step_memory_section(memory));
        }
        if let Some(memory) = context.executor_b_stage_memory.as_ref() {
            sections.extend(render_worker_local_memory_section(memory));
        }
        sections.push("</boss-step-context>".into());
    }
    sections.join("\n")
}

fn append_subagent_output(
    tasks: &std::sync::Arc<crate::task::manager::TaskManager>,
    task_id: &str,
    role: WorkerRole,
    task_input: &str,
    messages: &[Message],
) {
    if messages.is_empty() {
        tasks.append_output(task_id, "subagent produced no output");
        return;
    }

    if role == WorkerRole::Verify {
        let raw_output = messages
            .iter()
            .map(|message| message.text().trim().to_string())
            .filter(|text| !text.is_empty())
            .collect::<Vec<String>>()
            .join("\n");
        let normalized = if verify_output_matches_contract(&raw_output) {
            raw_output
        } else {
            normalize_verify_output(task_input, &raw_output)
        };
        tasks.append_output(
            task_id,
            format!("{normalized}\n"),
        );
        return;
    }

    for message in messages {
        tasks.append_output(task_id, format!("{}\n", message.text()));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VerifyPatchOutput {
    verified_target: String,
    verification_result: String,
    minimal_evidence: String,
    remaining_blocker: String,
    evidence_refs: Vec<String>,
}

impl VerifyPatchOutput {
    fn render(&self) -> String {
        format!(
            "verified_target: {}\nverification_result: {}\nminimal_evidence: {}\nremaining_blocker: {}\nevidence_refs: {}",
            self.verified_target,
            self.verification_result,
            self.minimal_evidence,
            self.remaining_blocker,
            if self.evidence_refs.is_empty() {
                "none".into()
            } else {
                self.evidence_refs.join("; ")
            }
        )
    }
}

fn normalize_verify_output(task_input: &str, raw_output: &str) -> String {
    build_verify_patch_output(task_input, raw_output).render()
}

fn verify_output_matches_contract(raw_output: &str) -> bool {
    let lines = raw_output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.len() != 5 {
        return false;
    }

    if !lines[0].to_ascii_lowercase().starts_with("verified_target:")
        || !lines[1].to_ascii_lowercase().starts_with("verification_result:")
        || !lines[2].to_ascii_lowercase().starts_with("minimal_evidence:")
        || !lines[3].to_ascii_lowercase().starts_with("remaining_blocker:")
        || !lines[4].to_ascii_lowercase().starts_with("evidence_refs:")
    {
        return false;
    }

    let patch = parse_verify_patch_output(raw_output);
    verify_patch_output_matches_contract(&patch)
}

fn build_verify_patch_output(task_input: &str, raw_output: &str) -> VerifyPatchOutput {
    let scrub_templates = contains_multistage_report_template(raw_output);
    let target = extract_verify_target(raw_output)
        .or_else(|| extract_verify_target(task_input))
        .unwrap_or_else(|| "unknown".into());
    let verification_result =
        extract_labeled_value(raw_output, &["verification_result", "verification result"])
            .unwrap_or_else(|| infer_verification_result(raw_output));
    let remaining_blocker =
        extract_labeled_value(raw_output, &["remaining_blocker", "remaining blocker"])
            .or_else(|| infer_remaining_blocker(raw_output, &verification_result, scrub_templates))
            .unwrap_or_else(|| "none".into());
    let minimal_evidence =
        extract_labeled_value(raw_output, &["minimal_evidence", "minimal evidence"])
            .or_else(|| infer_minimal_evidence(raw_output, scrub_templates))
            .unwrap_or_else(|| "none recorded".into());
    let mut evidence_refs = extract_evidence_refs(raw_output)
        .or_else(|| extract_evidence_refs(task_input))
        .unwrap_or_default();
    let normalized_target = normalize_verify_patch_ref(&target);
    ensure_verified_target_read_anchor(
        &mut evidence_refs,
        &normalized_target,
        verification_result.trim(),
    );

    VerifyPatchOutput {
        verified_target: normalized_target,
        verification_result: verification_result.trim().to_string(),
        minimal_evidence: compact_verify_value(&minimal_evidence),
        remaining_blocker: compact_verify_value(&remaining_blocker),
        evidence_refs,
    }
}

fn parse_verify_patch_output(raw_output: &str) -> VerifyPatchOutput {
    let mut verified_target = None;
    let mut verification_result = None;
    let mut minimal_evidence = None;
    let mut remaining_blocker = None;
    let mut evidence_refs = None;

    for line in raw_output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        if verified_target.is_none()
            && (lower.starts_with("verified_target:") || lower.starts_with("verified target:"))
        {
            verified_target = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
        if verification_result.is_none()
            && (lower.starts_with("verification_result:")
                || lower.starts_with("verification result:"))
        {
            verification_result = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
        if minimal_evidence.is_none()
            && (lower.starts_with("minimal_evidence:") || lower.starts_with("minimal evidence:"))
        {
            minimal_evidence = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
        if remaining_blocker.is_none()
            && (lower.starts_with("remaining_blocker:")
                || lower.starts_with("remaining blocker:"))
        {
            remaining_blocker = trimmed
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
            continue;
        }
        if evidence_refs.is_none() && lower.starts_with("evidence_refs:") {
            evidence_refs = Some(
                trimmed
                .split_once(':')
                .map(|(_, value)| parse_verify_patch_refs(value))
                .unwrap_or_default(),
            );
        }
    }

    VerifyPatchOutput {
        verified_target: verified_target.unwrap_or_else(|| "unknown".into()),
        verification_result: verification_result.unwrap_or_else(|| "verified".into()),
        minimal_evidence: minimal_evidence.unwrap_or_else(|| "none recorded".into()),
        remaining_blocker: remaining_blocker.unwrap_or_else(|| "none".into()),
        evidence_refs: evidence_refs.unwrap_or_default(),
    }
}

fn parse_verify_patch_refs(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Vec::new();
    }
    trimmed
        .split(|ch| matches!(ch, ';' | '|' | ','))
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(normalize_verify_patch_ref)
        .filter(|item| !item.is_empty() && item != "none")
        .collect()
}

fn extract_evidence_refs(text: &str) -> Option<Vec<String>> {
    let refs = text
        .lines()
        .find_map(|line| {
            let trimmed = line.trim();
            let lower = trimmed.to_ascii_lowercase();
            if !(lower.starts_with("evidence_refs:") || lower.starts_with("evidence refs:")) {
                return None;
            }
            trimmed
                .split_once(':')
                .map(|(_, value)| parse_verify_patch_refs(value))
        })
        .unwrap_or_default();
    if refs.is_empty() {
        None
    } else {
        Some(refs)
    }
}

fn verify_patch_output_matches_contract(patch: &VerifyPatchOutput) -> bool {
    if !is_valid_verification_result(&patch.verification_result) {
        return false;
    }
    if !is_concise_target_value(&patch.verified_target) {
        return false;
    }
    if !is_concise_verify_value(&patch.minimal_evidence) {
        return false;
    }
    if !is_concise_verify_value(&patch.remaining_blocker) {
        return false;
    }
    if patch.evidence_refs.len() != 1
        || !patch
            .evidence_refs
            .iter()
            .all(|value| is_concise_patch_ref(value))
    {
        return false;
    }
    if patch.verification_result.eq_ignore_ascii_case("verified")
        && !has_read_anchor_for_target(&patch.evidence_refs, &patch.verified_target)
    {
        return false;
    }
    true
}

fn ensure_verified_target_read_anchor(
    evidence_refs: &mut Vec<String>,
    verified_target: &str,
    verification_result: &str,
) {
    if !verification_result.eq_ignore_ascii_case("verified")
        || verified_target.is_empty()
        || verified_target == "unknown"
    {
        return;
    }
    evidence_refs.clear();
    evidence_refs.push(format!("read:{verified_target}"));
}

fn has_read_anchor_for_target(evidence_refs: &[String], verified_target: &str) -> bool {
    let expected = format!("read:{verified_target}");
    evidence_refs.iter().any(|value| value == &expected)
}

fn normalize_verify_patch_ref(value: &str) -> String {
    let trimmed = value
        .trim()
        .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | ',' | ';'));
    if trimmed.len() <= 200 {
        trimmed.to_string()
    } else {
        let mut truncated = trimmed.chars().take(200).collect::<String>();
        if let Some(idx) = truncated.rfind(' ') {
            truncated.truncate(idx);
        }
        truncated.trim().to_string()
    }
}

fn is_concise_patch_ref(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value.len() <= 200 && !value.contains('\n') && !value.contains('\r')
}

fn extract_verify_target(text: &str) -> Option<String> {
    extract_labeled_value(text, &["verified_target", "verified target"])
        .or_else(|| extract_suffix_after(text, "Verify target artifact only:"))
        .or_else(|| extract_suffix_after(text, "target file exists and is non-empty:"))
        .or_else(|| extract_suffix_after(text, "failed_target:"))
}

fn extract_suffix_after(text: &str, prefix: &str) -> Option<String> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(prefix).map(str::trim))
        .and_then(|value| {
            let trimmed = value
                .trim_end_matches('.')
                .split_once(". Return")
                .map(|(head, _)| head)
                .unwrap_or(value)
                .trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        })
}

fn extract_labeled_value(text: &str, labels: &[&str]) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        for label in labels {
            if lower.starts_with(label) {
                if let Some((_, value)) = trimmed.split_once(':') {
                    let value = value.trim();
                    if !value.is_empty() {
                        return Some(value.to_string());
                    }
                }
            }
        }
    }
    None
}

fn infer_verification_result(raw_output: &str) -> String {
    if raw_output
        .lines()
        .map(str::trim)
        .any(|line| line.eq_ignore_ascii_case("blocked") || line.contains("blocked"))
    {
        "blocked".into()
    } else {
        "verified".into()
    }
}

fn infer_remaining_blocker(
    raw_output: &str,
    verification_result: &str,
    scrub_templates: bool,
) -> Option<String> {
    if verification_result.eq_ignore_ascii_case("blocked") {
        if scrub_templates {
            extract_labeled_value(raw_output, &["remaining_blocker", "remaining blocker"])
                .map(|value| compact_verify_value(&value))
                .or_else(|| Some("none".into()))
        } else {
            shortest_noncontract_line(raw_output).map(|value| compact_verify_value(&value))
        }
    } else {
        Some("none".into())
    }
}

fn infer_minimal_evidence(raw_output: &str, scrub_templates: bool) -> Option<String> {
    let evidence_prefixes = [
        "read succeeded",
        "write succeeded",
        "glob succeeded",
        "artifactverify succeeded",
        "bash succeeded",
        "evidence:",
        "verified:",
    ];
    let explicit_evidence = shortest_noncontract_line(raw_output).filter(|line| {
        let lower = line.to_ascii_lowercase();
        evidence_prefixes
            .iter()
            .any(|prefix| lower.starts_with(prefix))
    });
    if explicit_evidence.is_some() || scrub_templates {
        return explicit_evidence;
    }
    explicit_evidence.or_else(|| {
        shortest_noncontract_line(raw_output).map(|line| compact_verify_value(&line))
    })
}

fn shortest_noncontract_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty()
                && !line.eq_ignore_ascii_case("verified")
                && !line.eq_ignore_ascii_case("blocked")
                && !is_verify_contract_line(line)
        })
        .map(|line| compact_verify_value(line))
        .filter(|line| !line.is_empty())
        .min_by_key(|line| line.len())
}

fn is_verify_contract_line(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.starts_with("verified_target:")
        || lower.starts_with("verified target:")
        || lower.starts_with("verification_result:")
        || lower.starts_with("verification result:")
        || lower.starts_with("minimal_evidence:")
        || lower.starts_with("minimal evidence:")
        || lower.starts_with("remaining_blocker:")
        || lower.starts_with("remaining blocker:")
        || lower.starts_with("next_action for coordinator:")
        || lower.starts_with("minimal verification steps:")
        || lower.starts_with("files changed")
        || lower.starts_with("阶段 1")
        || lower.starts_with("阶段 2")
        || lower.starts_with("阶段 3")
        || lower.starts_with("阶段 4")
        || lower.starts_with("证据来源")
        || lower.starts_with("如何运行")
        || lower.starts_with("验证与运行")
        || lower.starts_with("剩余风险")
        || lower.starts_with("后续工作")
        || lower.starts_with("recommendations:")
        || lower.starts_with("recommendation:")
        || lower.starts_with("risk notes:")
        || lower.starts_with("validation steps:")
        || lower.starts_with("next_action:")
}

fn contains_multistage_report_template(raw_output: &str) -> bool {
    raw_output.lines().map(str::trim).any(|line| {
        let lower = line.to_ascii_lowercase();
        lower.starts_with("阶段 1")
            || lower.starts_with("阶段 2")
            || lower.starts_with("阶段 3")
            || lower.starts_with("阶段 4")
            || lower.starts_with("证据来源")
            || lower.starts_with("如何运行")
            || lower.starts_with("验证与运行")
            || lower.starts_with("剩余风险")
            || lower.starts_with("后续工作")
            || lower.starts_with("recommendations:")
            || lower.starts_with("recommendation:")
            || lower.starts_with("risk notes:")
            || lower.starts_with("validation steps:")
            || lower.starts_with("next_action:")
    })
}

fn compact_verify_value(value: &str) -> String {
    let trimmed = value.trim().trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | ',' | ';'));
    let mut candidates = trimmed
        .split(|ch| matches!(ch, '.' | '!' | '?' | ';' | '\n' | '\r'))
        .flat_map(|chunk| chunk.split(" and "))
        .flat_map(|chunk| chunk.split(" but "))
        .flat_map(|chunk| chunk.split(" because "))
        .flat_map(|chunk| chunk.split(" so "))
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| {
            chunk
                .split_whitespace()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|chunk| !chunk.is_empty())
        .collect::<Vec<_>>();
    let mut multi_word_candidates = candidates
        .iter()
        .filter(|chunk| chunk.split_whitespace().count() >= 2)
        .cloned()
        .collect::<Vec<_>>();
    multi_word_candidates.sort_by_key(|chunk| (chunk.len(), chunk.split_whitespace().count()));
    let compacted = multi_word_candidates
        .into_iter()
        .next()
        .or_else(|| {
            candidates.sort_by_key(|chunk| (chunk.len(), chunk.split_whitespace().count()));
            candidates.into_iter().next()
        })
        .unwrap_or_else(|| trimmed.split_whitespace().collect::<Vec<_>>().join(" "));
    if compacted.len() <= 96 {
        return compacted;
    }
    let mut truncated = compacted.chars().take(96).collect::<String>();
    if let Some(idx) = truncated.rfind(' ') {
        truncated.truncate(idx);
    }
    truncated.trim().to_string()
}

fn is_valid_verification_result(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "verified" | "blocked")
}

fn is_concise_verify_value(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return false;
    }
    if value.len() > 96 {
        return false;
    }
    if value.contains('\n') || value.contains('\r') {
        return false;
    }
    if value.contains('.') || value.contains('!') || value.contains('?') {
        return false;
    }
    value.split_whitespace().count() <= 8
}

fn is_concise_target_value(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value.len() <= 200 && !value.contains('\n') && !value.contains('\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spawn_request() -> SpawnAgentRequest {
        SpawnAgentRequest {
            task: "implement feature".into(),
            task_contains_boss_context: false,
            role: WorkerRole::Implement,
            inherit_context: false,
            max_turns: None,
            allowed_tools: None,
            reuse_strategy: ReuseStrategy::Fresh,
            parent_task_id: None,
            orchestration_group_id: None,
            phase: None,
            step_id: Some(0),
            boss_plan_id: Some("plan-1".into()),
            step_objective: Some("objective 1".into()),
            step_acceptance: vec!["acceptance 1".into()],
            parent_session_id: Some("parent-session".into()),
            requires_verification: false,
            lism_policy: WorkerLisMPolicy::default_for_role(WorkerRole::Implement),
            shared_step_memory: None,
            boss_actor_policy: None,
        }
    }

    #[test]
    fn build_worker_task_input_preserves_preassembled_boss_prompt_without_duplication() {
        let mut request = sample_spawn_request();
        request.task = "objective: already assembled\nplan_id: plan-1".into();
        request.task_contains_boss_context = true;
        let input = build_worker_task_input(&request);
        assert_eq!(input, request.task);
        assert!(
            !input.contains("<boss-step-context>"),
            "preassembled boss prompts must not receive duplicated boss-step-context"
        );
    }

    #[test]
    fn build_worker_task_input_injects_verify_contract_into_preassembled_boss_prompt() {
        let mut request = sample_spawn_request();
        request.role = WorkerRole::Verify;
        request.task = "verified_target: /tmp/preassembled.md".into();
        request.task_contains_boss_context = true;
        let input = build_worker_task_input(&request);
        assert!(input.starts_with("verified_target: /tmp/preassembled.md"));
        assert!(input.contains("<verify-output-contract>"));
        assert_eq!(input.matches("<verify-output-contract>").count(), 1);
    }

    #[test]
    fn build_worker_task_input_appends_boss_step_context_by_default() {
        let request = sample_spawn_request();
        let input = build_worker_task_input(&request);
        assert!(input.contains("<boss-step-context>"));
        assert!(input.contains("objective: objective 1"));
        assert!(input.contains("plan_id: plan-1"));
    }

    #[test]
    fn build_worker_task_input_adds_verify_output_contract_for_verify_role() {
        let mut request = sample_spawn_request();
        request.role = WorkerRole::Verify;
        let input = build_worker_task_input(&request);
        assert!(input.contains("<verify-output-contract>"));
        assert!(input.contains("return exactly five short fields only"));
        assert!(input.contains("do not include analysis"));
        assert!(input.contains("keep minimal_evidence to one short factual phrase"));
        assert!(input.contains("set evidence_refs to read:<verified_target> when verification_result is verified"));
    }

    #[test]
    fn verification_first_emits_structured_patch_instead_of_report_prose() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "verification_result: verified\nminimal_evidence: Read succeeded and the file is present.\nremaining_blocker: none\nverified_target: /tmp/report.md\nevidence_refs: artifact:/tmp/report.md";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: read:/tmp/report.md"
        );
        assert_eq!(normalized.lines().count(), 5);
    }

    #[test]
    fn verification_first_patch_rejects_multisection_report_format() {
        assert!(!verify_output_matches_contract(
            "## verification report\nverified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: artifact:/tmp/report.md"
        ));
        assert!(!verify_output_matches_contract(
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nrecommendations: keep reading docs"
        ));
    }

    #[test]
    fn continue_task_input_renders_shared_and_local_memory_as_separate_sections() {
        let shared_step_memory = SharedStepMemory {
            step_id: Some(7),
            worker_role: Some("verify".into()),
            target: Some("/tmp/shared.md".into()),
            required_action: Some("verify_artifact".into()),
            artifact_status: Some("present".into()),
            verification_status: Some("verified".into()),
            completion_evidence_status: Some("present".into()),
            verified_facts: vec![
                "verified_target: /tmp/shared.md".into(),
                "verification_result: verified".into(),
                "minimal_evidence: Read succeeded".into(),
                "remaining_blocker: none".into(),
            ],
            remaining_blocker: Some("none".into()),
            evidence_refs: vec!["artifact:shared".into()],
        };
        let local_memory = ExecutorBStageMemory {
            recent_reads: vec!["src/lib.rs".into()],
            recent_edits: vec!["src/lib.rs".into()],
            recent_test_refs: vec!["cargo test".into()],
            recent_verification_refs: vec!["verify ref".into()],
            failed_targets: vec!["/tmp/local.md".into()],
            verified_targets: vec!["/tmp/local.md".into()],
            continuity: Some(crate::core::boss_state::ExecutorBStageMemoryContinuity::ReuseWithinStep),
        };
        let context = ContinueBossStepContext {
            step_id: Some(7),
            boss_plan_id: Some("plan-7".into()),
            step_objective: Some("verify shared artifact".into()),
            step_acceptance: vec!["target file exists".into()],
            parent_session_id: Some("session-7".into()),
            continuation_context: None,
            executor_b_stage_memory: Some(local_memory),
            shared_step_memory: Some(shared_step_memory),
        };

        let rendered = build_continue_task_input("please continue", Some(&context));
        assert!(rendered.contains("shared_step_memory:"));
        assert!(rendered.contains("worker_local_memory:"));
        assert!(rendered.contains("verified_target: /tmp/shared.md"));
        assert!(rendered.contains("recent_reads:"));
        assert!(rendered.contains("src/lib.rs"));
        assert!(!rendered.contains("acceptance_contract:"));
        assert!(!rendered.contains("executor_b_stage_memory:"));
        let shared_index = rendered.find("shared_step_memory:").expect("shared section");
        let local_index = rendered.find("worker_local_memory:").expect("local section");
        assert!(shared_index < local_index);
    }

    #[test]
    fn effective_max_turns_defaults_boss_implement_steps_to_sixty_four() {
        let request = sample_spawn_request();
        assert_eq!(effective_max_turns(&request), Some(64));
    }

    #[test]
    fn effective_max_turns_preserves_explicit_override() {
        let mut request = sample_spawn_request();
        request.max_turns = Some(3);
        assert_eq!(effective_max_turns(&request), Some(3));
    }

    #[test]
    fn effective_max_output_tokens_is_lower_for_verify_role() {
        let mut request = sample_spawn_request();
        request.role = WorkerRole::Verify;
        assert_eq!(effective_max_output_tokens(&request), Some(1024));

        request.role = WorkerRole::Implement;
        assert_eq!(effective_max_output_tokens(&request), None);
    }

    #[test]
    fn parse_agent_request_defaults_worker_lism_to_force_on() {
        let request = parse_agent_request(r#"{"task":"fix it","role":"implement"}"#)
            .expect("request should parse");
        let AgentRequest::Spawn(spawn) = request else {
            panic!("expected spawn request");
        };
        assert_eq!(spawn.lism_policy, WorkerLisMPolicy::ForceOn);
    }

    #[test]
    fn parse_agent_request_accepts_explicit_worker_lism_override() {
        let request =
            parse_agent_request(r#"{"task":"fix it","role":"implement","lism_policy":"inherit"}"#)
                .expect("request should parse");
        let AgentRequest::Spawn(spawn) = request else {
            panic!("expected spawn request");
        };
        assert_eq!(spawn.lism_policy, WorkerLisMPolicy::Inherit);
    }

    #[test]
    fn normalize_verify_output_hard_clamps_to_four_lines() {
        let task_input = "Verify target artifact only: /tmp/verification-first.md. Return a short verification result only.\n<boss-step-context>\nacceptance:\n- verified_target: /tmp/verification-first.md\n</boss-step-context>";
        let raw_output = "阶段 1：scan\n阶段 2：read\n阶段 3：report\n阶段 4：close\n证据来源\n- read succeeded\n如何运行与验证\n- run stat\nverification_result: blocked\nminimal_evidence: Read succeeded and the file is present\nremaining_blocker: target missing verification\nnext_action: keep reading docs";
        let normalized = normalize_verify_output(task_input, raw_output);
        let lines = normalized.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 5);
        assert_eq!(lines[0], "verified_target: /tmp/verification-first.md");
        assert_eq!(lines[1], "verification_result: blocked");
        assert_eq!(lines[2], "minimal_evidence: Read succeeded");
        assert_eq!(lines[3], "remaining_blocker: target missing verification");
        assert_eq!(lines[4], "evidence_refs: none");
    }

    #[test]
    fn verify_output_normalization_rewrites_multistage_report_template_to_four_line_short_form() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "阶段 1：overview\n阶段 2：evidence\n阶段 3：analysis\n阶段 4：wrap\n证据来源\n- README\n- docs\n我做了什么 / 变更说明\n- wrote report\n如何运行与验证\n- cat report\n剩余风险与后续工作\n- rerun later\nverification_result: verified\nminimal_evidence: Read succeeded and the file is present.\nremaining_blocker: none\nnext_action: continue";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: read:/tmp/report.md"
        );
        assert_eq!(normalized.lines().count(), 5);
    }

    #[test]
    fn verify_output_normalization_discards_evidence_sources_and_run_instructions() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "证据来源\n- RustAgent/docs/29-memory-backpressure-and-resource-limits.md\n- RustAgent/docs/31-token-efficiency-cost-performance.md\n如何运行与验证\n- cargo test --quiet\n- bash RustAgent/Agent/tests/run_boss_lism_matrix.sh\nverification_result: blocked\nminimal_evidence: Read succeeded and the file is present.\nremaining_blocker: target missing verification\nnext_action: rerun with stat";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert!(!normalized.contains("证据来源"));
        assert!(!normalized.contains("如何运行与验证"));
        assert!(!normalized.contains("run_boss_lism_matrix"));
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target missing verification\nevidence_refs: none"
        );
    }

    #[test]
    fn verify_output_normalization_keeps_only_single_short_evidence_phrase() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded and the file is present and ready\nremaining_blocker: none";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: read:/tmp/report.md"
        );
    }

    #[test]
    fn verify_output_normalization_drops_recommendations_and_validation_steps_completely() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "recommendations:\n- keep reading docs\nvalidation steps:\n- run stat\nverified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target missing verification";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target missing verification\nevidence_refs: none"
        );
    }

    #[test]
    fn verify_output_contract_requires_exact_five_prefixed_lines() {
        assert!(verify_output_matches_contract(
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: read:/tmp/report.md"
        ));
        assert!(!verify_output_matches_contract(
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: none\nnext_action: extra"
        ));
        assert!(!verify_output_matches_contract(
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none"
        ));
    }

    #[test]
    fn verify_output_contract_rejects_multi_section_report_prose() {
        assert!(!verify_output_matches_contract(
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded and the file looks good. No further action needed.\nremaining_blocker: none\nevidence_refs: none"
        ));
        assert!(!verify_output_matches_contract(
            "verified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target missing verification. please inspect the report and rerun validation.\nevidence_refs: none"
        ));
    }

    #[test]
    fn verify_completion_is_rewritten_to_short_form_before_append_output() {
        let task_input =
            "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "## verification report\nverified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded and the file is present.\nremaining_blocker: none\n\nrecommendations:\n- keep reading docs\n- add more checks";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(normalized.lines().count(), 5);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: read:/tmp/report.md"
        );
    }

    #[test]
    fn verify_completion_short_form_drops_recommendations_and_risk_notes() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "risk notes: the workspace may drift\nverified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target missing verification\nnext_action: rerun with stat\nvalidation steps: read docs";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: target missing verification\nevidence_refs: none"
        );
    }

    #[test]
    fn normalize_verify_output_falls_back_to_task_target_and_blocked_line() {
        let task_input =
            "target file exists and is non-empty: /tmp/report.md\nfailed_target: /tmp/report.md";
        let raw_output = "blocked\nRead succeeded\nnext_action for coordinator: expand docs";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: blocked\nminimal_evidence: Read succeeded\nremaining_blocker: Read succeeded\nevidence_refs: none"
        );
    }

    #[test]
    fn normalize_verify_output_defaults_verified_when_no_blocker_is_present() {
        let task_input = "Verify target artifact only: /tmp/report.md. Return a short verification result only.";
        let raw_output = "Read succeeded\nhow to validate: stat report";
        let normalized = normalize_verify_output(task_input, raw_output);
        assert_eq!(
            normalized,
            "verified_target: /tmp/report.md\nverification_result: verified\nminimal_evidence: Read succeeded\nremaining_blocker: none\nevidence_refs: read:/tmp/report.md"
        );
    }
}

fn parse_worker_role(value: Option<&str>) -> anyhow::Result<WorkerRole> {
    match value.unwrap_or("research") {
        "research" => Ok(WorkerRole::Research),
        "implement" => Ok(WorkerRole::Implement),
        "verify" => Ok(WorkerRole::Verify),
        other => anyhow::bail!("unknown worker role: {other}"),
    }
}

fn parse_worker_phase(
    value: Option<&str>,
) -> anyhow::Result<Option<crate::task::types::WorkerPhase>> {
    match value {
        Some("research") => Ok(Some(crate::task::types::WorkerPhase::Research)),
        Some("implement") => Ok(Some(crate::task::types::WorkerPhase::Implement)),
        Some("verify") => Ok(Some(crate::task::types::WorkerPhase::Verify)),
        Some(other) => anyhow::bail!("unknown worker phase: {other}"),
        None => Ok(None),
    }
}

fn parse_reuse_strategy(value: Option<&str>, role: WorkerRole) -> anyhow::Result<ReuseStrategy> {
    match value {
        Some("running_only") => Ok(ReuseStrategy::RunningOnly),
        Some("fresh") => Ok(ReuseStrategy::Fresh),
        Some(other) => anyhow::bail!("unknown reuse strategy: {other}"),
        None => Ok(match role {
            WorkerRole::Research => ReuseStrategy::RunningOnly,
            WorkerRole::Implement | WorkerRole::Verify => ReuseStrategy::Fresh,
        }),
    }
}

fn parse_boss_actor_policy(
    role: Option<&str>,
    depth: Option<u32>,
) -> anyhow::Result<Option<crate::state::permission_context::BossActorPolicy>> {
    let Some(role_str) = role else {
        return Ok(None);
    };
    use crate::core::boss_state::{BossActorRole, BossStage};
    use crate::state::permission_context::BossActorPolicy;
    let actor_role = match role_str {
        "executor_b" => BossActorRole::ExecutorB,
        "designer_a" => BossActorRole::DesignerA,
        "review_child" => BossActorRole::ReviewChild,
        "implement_child" => BossActorRole::ImplementChild,
        "verify_child" => BossActorRole::VerifyChild,
        other => anyhow::bail!("unknown boss_actor_role: {other}"),
    };
    Ok(Some(BossActorPolicy {
        actor_role,
        lineage_depth: depth.unwrap_or(0),
        phase: BossStage::Execution,
    }))
}

fn parse_worker_lism_policy(
    value: Option<&str>,
    role: WorkerRole,
) -> anyhow::Result<WorkerLisMPolicy> {
    match value {
        Some("inherit") => Ok(WorkerLisMPolicy::Inherit),
        Some("force-on") | Some("force_on") | Some("on") => Ok(WorkerLisMPolicy::ForceOn),
        Some("force-off") | Some("force_off") | Some("off") => Ok(WorkerLisMPolicy::ForceOff),
        Some(other) => anyhow::bail!("unknown worker lism_policy: {other}"),
        None => Ok(WorkerLisMPolicy::default_for_role(role)),
    }
}

fn maybe_reuse_running_task(
    tasks: &std::sync::Arc<crate::task::manager::TaskManager>,
    session_id: &str,
    task_description: &str,
    worker_role: WorkerRole,
    orchestration_group_id: Option<&str>,
) -> Option<String> {
    tasks.list().into_iter().find_map(|task| {
        let matches_owner = task.owner.session_id == session_id;
        let matches_role = task.worker_role == Some(worker_role);
        let matches_description = task.description
            == format!(
                "Spawned {} worker for {}",
                worker_role.as_str(),
                task_description
            );
        let matches_group = task.orchestration_group_id.as_deref() == orchestration_group_id;
        if matches_owner
            && matches_role
            && matches_description
            && matches_group
            && matches!(task.status, crate::task::types::TaskStatus::Running)
        {
            Some(task.id)
        } else {
            None
        }
    })
}

fn build_parent_query_context(permissions: ToolPermissionContext) -> QueryContext {
    let mut runtime_permissions = permissions.clone();
    runtime_permissions.add_always_allow_rule(AgentTool.metadata().name);
    runtime_permissions.include_interactive_tools = false;
    runtime_permissions.include_deferred_tools = false;

    let hook_registry = permissions
        .inherited_hook_registry
        .clone()
        .unwrap_or_default();
    let tool_registry = permissions
        .inherited_tool_registry
        .clone()
        .unwrap_or_else(|| ToolRegistry::new().register(std::sync::Arc::new(AgentTool)))
        .assemble(crate::tool::registry::ToolAssemblyContext::coordinator(
            InteractionSurface::Cli,
            SessionMode::Headless,
        ));
    let inherited_active_model_snapshot = permissions.inherited_active_model_snapshot.clone();
    let app_state = AppState {
        surface: InteractionSurface::Cli,
        session_mode: SessionMode::Headless,
        client_type: ClientType::Cli,
        session_source: SessionSource::LocalCli,
        runtime_role: RuntimeRole::Coordinator,
        worker_role: None,
        permission_context: runtime_permissions,
        command_registry: None,
        runtime_tool_registry: Some(std::sync::Arc::new(tokio::sync::RwLock::new(
            tool_registry.clone(),
        ))),
        skill_registry: None,
        mcp_runtime: permissions.mcp_runtime.clone(),
        plugin_load_result: None,
        cost_tracker: CostTracker::default(),
        service_observability_tracker:
            crate::service::observability::ServiceObservabilityTracker::default(),
        notification_dispatcher: {
            let mut nd = NotificationDispatcher::new(TelegramGateway::default())
                .with_hook_registry(hook_registry.clone());
            if let Some(boss) = permissions.boss_coordinator.clone() {
                nd = nd.with_boss_coordinator(boss);
            }
            nd
        },
        audit_log: std::sync::Arc::new(std::sync::Mutex::new(AuditLog::default())),
        startup_trace: Vec::new(),
        active_model_runtime: inherited_active_model_snapshot
            .as_ref()
            .cloned()
            .map(ActiveModelRuntime::new),
        active_model_profile_name: inherited_active_model_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.active_profile_name.clone()),
        active_model_profile_source: inherited_active_model_snapshot
            .as_ref()
            .map(|snapshot| snapshot.source.clone())
            .unwrap_or(ActiveModelProfileSource::BootstrapDefault),
        active_model_provider_summary: inherited_active_model_snapshot
            .as_ref()
            .map(|snapshot| snapshot.summary.clone())
            .unwrap_or(ActiveModelProviderSummary {
                provider_id: "default-provider".into(),
                protocol: "MessagesApi".into(),
                compatibility_profile: "MessagesApi".into(),
                base_url_host: "localhost".into(),
                model: "default-model".into(),
                auth_status: "env:OPENAI_API_KEY(unset)".into(),
            }),
        active_session_id: permissions
            .active_session_id
            .unwrap_or_else(|| "local-session".into()),
        session_store: None,
        session: None,
        history: None,
        restored_session: None,
        last_activity_ts: permissions.last_activity_ts.clone().unwrap_or_else(|| {
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            ))
        }),
        cancellation_token: permissions
            .cancellation_token
            .clone()
            .unwrap_or_else(tokio_util::sync::CancellationToken::new),
        subagent_limiter: permissions.subagent_limiter.clone(),
        boss_coordinator: permissions.boss_coordinator.clone(),
        remote_actor_store: None,
    };
    let system_prompt = crate::prompt::system::build_system_prompt(&app_state);
    let tools_prompt =
        crate::prompt::tools::build_tools_prompt(&tool_registry, &app_state.permission_context);
    let context_prompt = crate::prompt::context::build_context_prompt(&app_state);
    let api_client = app_state
        .active_model_runtime
        .as_ref()
        .map(|runtime| runtime.snapshot_blocking().client)
        .unwrap_or_else(|| {
            let service_observability_tracker = app_state.service_observability_tracker.clone();
            crate::service::api::client::ModelProviderClient::from_config_with_observability(
                crate::service::api::client::ModelProviderConfig::default(),
                service_observability_tracker,
            )
        });
    QueryContext {
        app_state,
        tool_registry,
        api_client,
        compactor: ReactiveCompactor,
        hook_registry,
        agent_id: None,
        system_prompt,
        tools_prompt,
        context_prompt,
    }
}

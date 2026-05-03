use crate::bootstrap::config_root::resolve_config_root;
use crate::bootstrap::model_profiles::load_model_profiles_registry_from_root;
use crate::core::boss_acceptance::{extract_artifact_expectations, verify_artifact_expectations};
use crate::core::boss_actor_runtime::{
    BossActorEvent, BossActorRegistry, DesignerACommand, ExecutorBCommand,
};
use crate::core::boss_context_brief::{
    BossContextBrief, BossContextStrategy, BossStateFrame, PermissionScopeView, RelevantFileHandle,
    TargetArtifact, assemble_brief_prompt,
};
use crate::core::boss_runtime::{BossControlRuntime, BossRuntimeOwner};
use crate::core::boss_state::{
    BossActorHandle, BossActorStatus, BossControlRequest, BossControlResponse, BossLisMPolicy,
    BossObservabilitySummary, BossPlan, BossPlanStep, BossPlanStepStatus, BossReportPayload,
    BossSession, BossStage, BossStatus, BossStepMetrics, BossStepReport, BossStepRoutedMetadata,
    BossStopOutcome, BossStopStage, CompressionStrategy, ContextMode,
};
use crate::core::boss_test_readiness::BossTestRunOutcome;
use crate::core::context::WorkerLisMPolicy;
use crate::core::lism_ab_sample::SharedLisMAbSampleSink;
use crate::core::prompt_budget::{BudgetDecision, evaluate_message_budget};
use crate::core::state_frame::ActorRole;
use crate::core::state_frame_loop::{DecisionLoopConfig, StateFrameToolRuntime};
use crate::core::state_frame_model_router::ModelTier;
use crate::core::state_frame_orchestrator::{
    StepOutcome, StepRuntimeResolutionContext, build_routed_state_frame_with_model_route,
    requires_external_tool_execution, run_routed_step_with_runtime,
};
use crate::history::session::SessionHistory;
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::task::manager::TaskManager;
use crate::task::types::{TaskEvent, TaskStatus, TaskUsageSummary};
use crate::tool::definition::{ObservableInput, ObservableInputSource, Tool, ToolCall};
use crate::tool::result::{
    ToolBatchContext, ToolExecutionOutcomeKind, ToolExecutionRecord, ToolReportModifier,
};
use serde_json::json;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct BossCoordinator {
    pub status: Arc<RwLock<BossStatus>>,
    /// Placed here so the planner can hold and modify it in memory before flushing
    pub plan: Arc<RwLock<Option<BossPlan>>>,

    /// Structured actor topology — replaces the former loose a/b session_id/cancel fields.
    pub session: Arc<RwLock<Option<BossSession>>>,

    /// Live actor runtimes for A and B — bootstrapped on restore/init.
    pub actor_registry: Arc<RwLock<Option<BossActorRegistry>>>,

    pub auto_advance_app_state: Arc<RwLock<Option<Arc<crate::state::app_state::AppState>>>>,
    routed_step_metadata: Arc<RwLock<std::collections::HashMap<usize, BossStepRoutedMetadata>>>,
    runtime_key: Arc<RwLock<Option<String>>>,
    runtime_owner: Arc<BossRuntimeOwner>,
    lism_policy: Arc<RwLock<BossLisMPolicy>>,
    worker_lism_policy: Arc<RwLock<WorkerLisMPolicy>>,
    full_worker_dispatch_fallback_enabled: Arc<RwLock<bool>>,
    lism_ab_sink: Option<SharedLisMAbSampleSink>,
}

fn step_artifact_verification_error(step: &BossPlanStep) -> Option<String> {
    verify_artifact_expectations(step.objective())
        .err()
        .map(|reason| format!("artifact verification failed: {reason}"))
}

fn build_artifact_repair_instruction(step: &BossPlanStep, missing_reason: &str) -> Option<String> {
    let expectation = extract_artifact_expectations(step.objective())
        .into_iter()
        .next()?;
    let target_path = expectation.path.display().to_string();
    let parent_dir = expectation
        .path
        .parent()
        .map(|path| path.display().to_string())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| ".".into());
    let recommended_write_strategy = match expectation.kind {
        crate::core::boss_acceptance::BossArtifactKind::File => "write_exact_target_file",
        crate::core::boss_acceptance::BossArtifactKind::Directory => {
            "create_directory_then_write_files"
        }
    };
    Some(format!(
        "repair artifact evidence for target_path={target_path} parent_dir={parent_dir} missing_reason={missing_reason} recommended_write_strategy={recommended_write_strategy}"
    ))
}

fn seed_step_acceptance(task: &str) -> Vec<String> {
    let mut acceptance = vec!["Task completed successfully.".to_string()];
    for expectation in extract_artifact_expectations(task) {
        let line = match expectation.kind {
            crate::core::boss_acceptance::BossArtifactKind::File => {
                format!(
                    "target file exists and is non-empty: {}",
                    expectation.path.display()
                )
            }
            crate::core::boss_acceptance::BossArtifactKind::Directory => {
                format!(
                    "target directory exists and is non-empty: {}",
                    expectation.path.display()
                )
            }
        };
        if !acceptance.iter().any(|item| item == &line) {
            acceptance.push(line);
        }
    }
    acceptance
}

fn classify_relevant_file_handle(path: &str, line: &str) -> String {
    if line.contains("目标目录") || path.ends_with('/') {
        "target_directory".to_string()
    } else if line.contains("目标文件") {
        "target_file".to_string()
    } else if path.ends_with(".rs") {
        "source_file".to_string()
    } else if path.ends_with(".md") {
        "document".to_string()
    } else if path.ends_with(".jsonl") || path.ends_with(".json") || path.ends_with(".log") {
        "data_or_log".to_string()
    } else {
        "path".to_string()
    }
}

fn build_file_handle_relevance(kind: &str, line: &str, path: &str) -> String {
    if line.contains("目标文件") {
        format!("explicit target file for this step: {path}")
    } else if line.contains("目标目录") {
        format!("explicit target directory for this step: {path}")
    } else {
        format!("referenced in step objective as {kind}: {path}")
    }
}

#[derive(Debug, Clone)]
struct ExecutorBAssignmentContract {
    brief: BossContextBrief,
    state_frame: BossStateFrame,
    allowed_tools: Vec<String>,
    lism_policy: String,
    assignment_fingerprint: String,
}

#[derive(Debug, Clone)]
struct ContinuePayloadBuild {
    payload: String,
    assignment_fingerprint: String,
    plan_version: String,
    step_revision: String,
}

#[derive(Debug, Clone)]
struct SpawnPayloadBuild {
    payload: String,
    assignment_fingerprint: String,
    plan_version: String,
    step_revision: String,
}

fn assignment_fingerprint(material: &serde_json::Value) -> String {
    let mut hasher = DefaultHasher::new();
    material.to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn strip_to_path_start(candidate: &str) -> &str {
    if candidate.starts_with("./") || candidate.starts_with("../") || candidate.starts_with('/') {
        return candidate;
    }
    candidate
        .find('/')
        .map(|idx| &candidate[idx..])
        .unwrap_or(candidate)
}

fn is_probably_filesystem_hint(candidate: &str) -> bool {
    let trimmed = candidate.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return false;
    }
    let lowered = trimmed.to_ascii_lowercase();
    if matches!(
        lowered.as_str(),
        "/boss" | "/mcp" | "/skills" | "/lism" | "/effort" | "/status"
    ) {
        return false;
    }
    if trimmed.starts_with('/') {
        let path = Path::new(trimmed);
        let slash_count = trimmed.matches('/').count();
        let has_extension = path.extension().is_some();
        let looks_like_dir = trimmed.ends_with('/');
        let under_known_root = [
            "/tmp/",
            "/private/tmp/",
            "/Users/",
            "/private/var/",
            "/var/",
            "/etc/",
            "/usr/",
        ]
        .iter()
        .any(|prefix| trimmed.starts_with(prefix));
        if (slash_count < 2 && !has_extension && !looks_like_dir)
            || (!path.exists() && !has_extension && !looks_like_dir && !under_known_root)
        {
            return false;
        }
    }
    if trimmed.contains('`') {
        return false;
    }
    true
}

fn relevant_file_handle_rank(kind: &str) -> usize {
    match kind {
        "target_directory" => 5,
        "target_file" => 4,
        "source_file" => 3,
        "document" => 2,
        "data_or_log" => 1,
        _ => 0,
    }
}

fn extract_relevant_file_handles(text: &str, step_revision: &str) -> Vec<RelevantFileHandle> {
    let mut handles: Vec<RelevantFileHandle> = Vec::new();
    let cwd = std::env::current_dir().ok();
    for line in text.lines() {
        let trimmed = line.trim();
        if !(trimmed.starts_with('-')
            || trimmed.starts_with("目标文件")
            || trimmed.starts_with("目标目录"))
        {
            continue;
        }
        for token in trimmed.split_whitespace() {
            let candidate = token
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('-')
                .trim_matches('：')
                .trim_end_matches(['，', ',', '。', '.', ';', '；', ')', '）', ']']);
            let candidate = strip_to_path_start(candidate);
            let candidate = candidate
                .rsplit_once(['：', ':'])
                .map(|(_, suffix)| suffix)
                .filter(|suffix| suffix.contains('/'))
                .unwrap_or(candidate);
            if candidate.is_empty()
                || candidate == "/"
                || !candidate.contains('/')
                || !is_probably_filesystem_hint(candidate)
            {
                continue;
            }
            if !(candidate.ends_with(".rs")
                || candidate.ends_with(".md")
                || candidate.starts_with('/')
                || candidate.starts_with("./")
                || candidate.starts_with("../"))
            {
                continue;
            }
            let candidate = normalize_relevant_file_hint(candidate, cwd.as_deref())
                .unwrap_or_else(|| candidate.to_string());
            let kind = classify_relevant_file_handle(&candidate, trimmed);
            if let Some(existing) = handles
                .iter_mut()
                .find(|existing| existing.path == candidate)
            {
                if relevant_file_handle_rank(&kind) > relevant_file_handle_rank(&existing.kind) {
                    existing.kind = kind.clone();
                    existing.why_relevant = build_file_handle_relevance(&kind, trimmed, &candidate);
                    existing.step_revision = step_revision.to_string();
                }
                continue;
            }
            if !handles.iter().any(|existing| existing.path == candidate) {
                handles.push(RelevantFileHandle {
                    path: candidate.clone(),
                    kind: kind.clone(),
                    source: "boss_step_objective".to_string(),
                    freshness: "current".to_string(),
                    why_relevant: build_file_handle_relevance(&kind, trimmed, &candidate),
                    step_revision: step_revision.to_string(),
                });
            }
        }
    }
    handles
}

fn normalize_relevant_file_hint(candidate: &str, cwd: Option<&Path>) -> Option<String> {
    let cwd = cwd?;
    let candidate_path = Path::new(candidate);
    if candidate_path.is_absolute() {
        return Some(candidate.to_string());
    }

    let mut attempts: Vec<PathBuf> = vec![cwd.join(candidate_path)];
    if candidate.starts_with("src/") {
        attempts.push(cwd.join("RustAgent/Agent").join(candidate_path));
    }
    if let Some(rest) = candidate.strip_prefix("../docs/") {
        attempts.push(cwd.join("RustAgent/docs").join(rest));
    }

    for attempt in attempts {
        if attempt.exists() {
            if let Ok(relative) = attempt.strip_prefix(cwd) {
                return Some(relative.to_string_lossy().replace('\\', "/"));
            }
            return Some(attempt.to_string_lossy().replace('\\', "/"));
        }
    }

    None
}

fn summarize_acceptance_items(step: &BossPlanStep) -> String {
    if step.acceptance.is_empty() {
        "- none".to_string()
    } else {
        step.acceptance
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn collect_recent_decisions(plan: &BossPlan, current_step_id: usize) -> Vec<String> {
    let mut decisions = plan
        .steps
        .iter()
        .filter(|step| step.id < current_step_id)
        .filter_map(|step| {
            if let Some(summary) = step.last_review_summary.as_ref() {
                Some(format!("step {} review: {}", step.id, summary))
            } else if step.status == BossPlanStepStatus::Completed {
                Some(format!("step {} completed: {}", step.id, step.objective()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if decisions.len() > 3 {
        decisions = decisions.split_off(decisions.len() - 3);
    }
    decisions
}

fn collect_target_files(relevant_file_handles: &[RelevantFileHandle]) -> Vec<String> {
    let mut target_files = Vec::new();
    for handle in relevant_file_handles {
        if matches!(
            handle.kind.as_str(),
            "target_file" | "target_directory" | "source_file" | "document"
        ) && !target_files.iter().any(|path| path == &handle.path)
        {
            target_files.push(handle.path.clone());
        }
    }
    target_files
}

fn collect_target_artifacts(step: &BossPlanStep, target_files: &[String]) -> Vec<TargetArtifact> {
    let mut artifacts = Vec::new();
    for expectation in extract_artifact_expectations(step.objective()) {
        let kind = match expectation.kind {
            crate::core::boss_acceptance::BossArtifactKind::File => "file",
            crate::core::boss_acceptance::BossArtifactKind::Directory => "directory",
        };
        artifacts.push(TargetArtifact {
            path: expectation.path.display().to_string(),
            kind: kind.to_string(),
            required_state: "exists_non_empty".to_string(),
            source: "artifact_expectation".to_string(),
        });
    }
    for path in target_files {
        if !artifacts.iter().any(|artifact| artifact.path == *path) {
            artifacts.push(TargetArtifact {
                path: path.clone(),
                kind: if path.ends_with('/') {
                    "directory".to_string()
                } else {
                    "file".to_string()
                },
                required_state: "referenced_for_step".to_string(),
                source: "target_file_handle".to_string(),
            });
        }
    }
    artifacts
}

fn default_allowed_tools() -> Vec<String> {
    vec![
        "Read".into(),
        "Edit".into(),
        "Glob".into(),
        "Grep".into(),
        "LS".into(),
        "Bash".into(),
    ]
}

fn render_workspace_capability_scope() -> String {
    "inherited_runtime_scope".to_string()
}

fn collect_blocked_items(step: &BossPlanStep) -> Vec<String> {
    let mut blocked = Vec::new();
    if matches!(step.status, BossPlanStepStatus::WaitingForApproval) {
        blocked.push("waiting for approval before implementation may proceed".to_string());
    }
    if matches!(
        step.status,
        BossPlanStepStatus::Rejected | BossPlanStepStatus::Failed
    ) {
        if let Some(summary) = step
            .last_review_summary
            .as_ref()
            .filter(|text| !text.trim().is_empty())
        {
            blocked.push(summary.clone());
        }
    }
    blocked
}

fn collect_recent_local_facts(step: &BossPlanStep, limit: usize) -> Vec<String> {
    let mut facts = Vec::new();
    for (idx, record) in step.tool_execution_records.iter().enumerate().rev() {
        match record.tool_name.as_str() {
            "Read" => {
                if let Some(path) = observable_path_local(record) {
                    facts.push(format!("recent_read path={path}"));
                }
            }
            "Edit" | "Write" => {
                if let Some(path) = observable_path_local(record) {
                    facts.push(format!("recent_edit path={path}"));
                }
            }
            "Bash" => {
                if let Some(command) = observable_bash_command_local(record) {
                    facts.push(format!(
                        "recent_test command={}",
                        trim_runtime_excerpt(&command, 120)
                    ));
                }
            }
            _ => {}
        }
        if let Some(output_fact) = recent_large_output_fact(step.id, idx, record) {
            facts.push(output_fact);
        }
        if facts.len() >= limit {
            break;
        }
    }
    facts.reverse();
    facts
}

fn recent_large_output_fact(
    step_id: usize,
    record_index: usize,
    record: &ToolExecutionRecord,
) -> Option<String> {
    let detail = record.detail.as_ref()?.trim();
    if detail.is_empty() {
        return None;
    }
    let is_large = matches!(record.kind, ToolExecutionOutcomeKind::ResultTooLarge)
        || detail.len() > 160
        || detail.lines().count() > 4;
    if !is_large {
        return None;
    }
    let source_event_id = format!("tool-output:{step_id}:{record_index}");
    let ref_id = format!("output:step{step_id}:{record_index}");
    Some(format!(
        "recent_output_ref ref={ref_id} source_event_id={source_event_id} excerpt={}",
        trim_runtime_excerpt(detail, 140)
    ))
}

fn render_recent_local_facts_section(facts: &[String]) -> String {
    if facts.is_empty() {
        String::new()
    } else {
        let mut lines = vec!["recent_local_facts:".to_string()];
        for fact in facts {
            lines.push(format!("  - {fact}"));
        }
        format!("\n{}", lines.join("\n"))
    }
}

fn trim_runtime_excerpt(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let mut iter = trimmed.chars();
    let excerpt = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{excerpt}...")
    } else {
        excerpt
    }
}

fn observable_path_local(record: &ToolExecutionRecord) -> Option<String> {
    record.observable_input.as_ref().and_then(|input| {
        serde_json::from_str::<serde_json::Value>(&input.value)
            .ok()
            .and_then(|value| {
                value
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
    })
}

fn observable_bash_command_local(record: &ToolExecutionRecord) -> Option<String> {
    record.observable_input.as_ref().and_then(|input| {
        serde_json::from_str::<serde_json::Value>(&input.value)
            .ok()
            .and_then(|value| {
                value
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
    })
}

fn store_step_result_diff(step: &mut BossPlanStep, primary: &str, fallback: Option<&str>) {
    let candidate = if primary.trim().is_empty() {
        fallback.unwrap_or_default()
    } else {
        primary
    };
    if candidate.trim().is_empty() {
        return;
    }
    step.result_diff = Some(candidate.trim().to_string());
}

fn sync_step_tool_execution_records(
    step: &mut BossPlanStep,
    tasks: Option<&TaskManager>,
    task_id: &str,
) {
    step.tool_execution_records = tasks
        .map(|manager| manager.tool_execution_records(task_id))
        .unwrap_or_default();
}

fn append_step_runtime_record(step: &mut BossPlanStep, record: ToolExecutionRecord) {
    let duplicate = step.tool_execution_records.iter().any(|existing| {
        existing.tool_name == record.tool_name
            && existing.kind == record.kind
            && existing.summary == record.summary
            && existing.detail == record.detail
            && existing.observable_input == record.observable_input
    });
    if !duplicate {
        step.tool_execution_records.push(record);
    }
}

fn observable_input_json(value: serde_json::Value) -> ObservableInput {
    ObservableInput {
        value: value.to_string(),
        source: ObservableInputSource::Raw,
    }
}

fn append_review_runtime_record(
    step: &mut BossPlanStep,
    verdict: &str,
    summary: &str,
    correction: Option<&str>,
) {
    append_step_runtime_record(
        step,
        ToolExecutionRecord {
            tool_name: "BossReview".into(),
            outcome: "Text".into(),
            kind: ToolExecutionOutcomeKind::Success,
            summary: format!("Boss review verdict: {verdict}"),
            detail: Some(summary.to_string()),
            pending_approval: None,
            report_modifier: ToolReportModifier::None,
            observable_input: Some(observable_input_json(json!({
                "step_id": step.id,
                "verdict": verdict,
                "correction": correction,
            }))),
            batch_context: ToolBatchContext {
                batch_index: 0,
                batch_size: 1,
                executed_in_batch: false,
            },
        },
    );
}

fn append_artifact_verification_runtime_records(
    step: &mut BossPlanStep,
    status: &str,
    summary_prefix: &str,
) {
    for expectation in extract_artifact_expectations(step.objective()) {
        let path = expectation.path.to_string_lossy().to_string();
        let kind = match expectation.kind {
            crate::core::boss_acceptance::BossArtifactKind::File => "file",
            crate::core::boss_acceptance::BossArtifactKind::Directory => "directory",
        };
        let summary = format!("{summary_prefix}: {path}");
        append_step_runtime_record(
            step,
            ToolExecutionRecord {
                tool_name: "ArtifactVerify".into(),
                outcome: "Text".into(),
                kind: if status == "missing_or_invalid" {
                    ToolExecutionOutcomeKind::Interrupted
                } else {
                    ToolExecutionOutcomeKind::Success
                },
                summary,
                detail: Some(format!("artifact verification status={status} path={path}")),
                pending_approval: None,
                report_modifier: ToolReportModifier::None,
                observable_input: Some(observable_input_json(json!({
                    "step_id": step.id,
                    "path": path,
                    "kind": kind,
                    "status": status,
                }))),
                batch_context: ToolBatchContext {
                    batch_index: 0,
                    batch_size: 1,
                    executed_in_batch: false,
                },
            },
        );
    }
}

fn build_step_review_summary(
    step: &BossPlanStep,
    source: &str,
    details: &[(&str, &str)],
) -> String {
    let mut sections = vec![
        format!("{source} reported boss step {} complete.", step.id),
        format!("Objective: {}", step.objective()),
        "Acceptance:".to_string(),
        summarize_acceptance_items(step),
    ];
    for (label, value) in details {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            sections.push(format!("{label}: {trimmed}"));
        }
    }
    sections.join("\n")
}

impl BossCoordinator {
    /// State-only constructor — no callbacks wired. Use in tests or low-level assembly only.
    /// Production code must call `new_with_runtime_owner` followed by
    /// `bootstrap_actor_registry_with_app_state`.
    #[doc(hidden)]
    pub fn new() -> Self {
        Self::new_with_runtime_owner(Arc::new(BossRuntimeOwner::default()))
    }

    pub fn new_with_runtime_owner(runtime_owner: Arc<BossRuntimeOwner>) -> Self {
        Self {
            status: Arc::new(RwLock::new(BossStatus::default())),
            plan: Arc::new(RwLock::new(None)),
            session: Arc::new(RwLock::new(None)),
            actor_registry: Arc::new(RwLock::new(None)),
            auto_advance_app_state: Arc::new(RwLock::new(None)),
            routed_step_metadata: Arc::new(RwLock::new(std::collections::HashMap::new())),
            runtime_key: Arc::new(RwLock::new(None)),
            runtime_owner,
            lism_policy: Arc::new(RwLock::new(BossLisMPolicy::Inherit)),
            worker_lism_policy: Arc::new(RwLock::new(WorkerLisMPolicy::ForceOn)),
            full_worker_dispatch_fallback_enabled: Arc::new(RwLock::new(true)),
            lism_ab_sink: None,
        }
    }

    /// Returns the raw pointer address of the coordinator's `BossRuntimeOwner` Arc.
    /// Test-only seam for verifying owner identity without exposing the Arc itself.
    #[doc(hidden)]
    pub fn runtime_owner_ptr(&self) -> usize {
        Arc::as_ptr(&self.runtime_owner) as usize
    }

    /// Test-only seam: exposes `parse_a_review_decision` as a public associated function.
    #[doc(hidden)]
    pub fn parse_a_review_decision_pub(
        response: &str,
        summary: &str,
    ) -> crate::core::boss_actor_runtime::ReviewDecision {
        Self::parse_a_review_decision(response, summary)
    }

    /// Test-only seam: builds and returns the ReviewFn for this coordinator.
    #[doc(hidden)]
    pub fn review_fn_for_test(&self) -> crate::core::boss_actor_runtime::ReviewFn {
        Self::build_review_fn(self)
    }

    /// Test-only seam: exposes `record_b_session_id` for direct state mutation in tests.
    #[doc(hidden)]
    pub async fn record_b_session_id_pub(&self, task_id: &str) {
        self.record_b_session_id(task_id).await;
    }

    /// Test-only seam: pre-seeds designer_a.session_id so ensure_a_session skips spawning.
    #[doc(hidden)]
    pub async fn record_a_session_id_pub(&self, task_id: &str) {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.designer_a.session_id = task_id.to_string();
            session.designer_a.task_id = Some(task_id.to_string());
            session.designer_a.status = crate::core::boss_state::BossActorStatus::Active;
        }
    }

    /// Test-only seam: reads `executor_b.session_id` for assertion in tests.
    #[doc(hidden)]
    pub async fn b_session_id(&self) -> String {
        let guard = self.session.read().await;
        guard
            .as_ref()
            .map(|s| s.executor_b.session_id.clone())
            .unwrap_or_default()
    }

    /// Test-only seam: reads `executor_b.task_id` for assertion in tests.
    #[doc(hidden)]
    pub async fn b_task_id(&self) -> Option<String> {
        let guard = self.session.read().await;
        guard.as_ref().and_then(|s| s.executor_b.task_id.clone())
    }

    /// Full-mode constructor — wires A+B callbacks immediately.
    /// Prefer `BossRuntimeHost::build_coordinator` in production so the host's
    /// `BossRuntimeOwner` is used. This method is the building block used by the host.
    pub async fn new_with_app_state(
        runtime_owner: Arc<BossRuntimeOwner>,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> Self {
        let coordinator = Self::new_with_runtime_owner(runtime_owner);
        coordinator
            .bootstrap_actor_registry_with_app_state(app_state)
            .await;
        coordinator
    }

    pub async fn attach_app_state_for_report_testing(
        &self,
        app_state: Arc<crate::state::app_state::AppState>,
    ) {
        let mut auto = self.auto_advance_app_state.write().await;
        *auto = Some(app_state);
    }

    pub async fn current_runtime_key(&self) -> Option<String> {
        self.runtime_key.read().await.clone()
    }

    pub async fn runtime_is_closed_for_testing(&self, key: &str) -> bool {
        self.runtime_owner
            .get_runtime(key)
            .map(|runtime| runtime.is_closed())
            .unwrap_or(true)
    }

    pub fn shutdown_all_runtime_instances(&self) {
        self.runtime_owner.shutdown_all_runtimes();
    }

    pub fn shutdown_runtime_owner(&self) {
        self.runtime_owner.shutdown_owner();
    }

    pub fn restart_runtime_owner(&self) {
        self.runtime_owner.restart_owner();
    }

    pub async fn has_control_runtime(&self) -> bool {
        self.runtime_key
            .read()
            .await
            .as_ref()
            .and_then(|key| self.runtime_owner.get_runtime(key))
            .is_some()
    }

    pub async fn ensure_control_runtime(&self) {
        if self.runtime_owner.is_closed() {
            return;
        }
        let mut runtime_key = self.runtime_key.write().await;
        if runtime_key
            .as_ref()
            .and_then(|key| self.runtime_owner.get_runtime(key))
            .is_some()
        {
            return;
        }
        let plan_id = self
            .plan
            .read()
            .await
            .as_ref()
            .map(|plan| plan.plan_id.clone())
            .unwrap_or_else(|| "boss-default".into());
        let key = self.runtime_owner.fresh_runtime_key(&plan_id);
        let runtime = BossControlRuntime::spawn(self.clone_for_runtime());
        self.runtime_owner.bind_runtime(key.clone(), runtime);
        *runtime_key = Some(key);
    }

    pub async fn rebind_control_runtime(&self) {
        let mut runtime_key = self.runtime_key.write().await;
        if let Some(key) = runtime_key.as_ref() {
            let _ = self.runtime_owner.shutdown_runtime(key);
        }
        let plan_id = self
            .plan
            .read()
            .await
            .as_ref()
            .map(|plan| plan.plan_id.clone())
            .unwrap_or_else(|| "boss-default".into());
        let key = self.runtime_owner.fresh_runtime_key(&plan_id);
        let runtime = BossControlRuntime::spawn(self.clone_for_runtime());
        self.runtime_owner.bind_runtime(key.clone(), runtime);
        *runtime_key = Some(key);
    }

    async fn send_control_request(
        &self,
        request: BossControlRequest,
        tasks: Arc<TaskManager>,
        dispatcher: NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        self.ensure_control_runtime().await;
        if self.runtime_owner.is_closed() {
            anyhow::bail!("boss runtime owner is closed");
        }
        let key = self
            .runtime_key
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("boss control runtime key unavailable"))?;
        let runtime = self
            .runtime_owner
            .get_runtime(&key)
            .ok_or_else(|| anyhow::anyhow!("boss control runtime unavailable"))?;
        runtime.request(request, tasks, dispatcher).await
    }

    fn clone_for_runtime(&self) -> Self {
        Self {
            status: self.status.clone(),
            plan: self.plan.clone(),
            session: self.session.clone(),
            actor_registry: self.actor_registry.clone(),
            auto_advance_app_state: self.auto_advance_app_state.clone(),
            routed_step_metadata: self.routed_step_metadata.clone(),
            runtime_key: self.runtime_key.clone(),
            runtime_owner: self.runtime_owner.clone(),
            lism_policy: self.lism_policy.clone(),
            worker_lism_policy: self.worker_lism_policy.clone(),
            full_worker_dispatch_fallback_enabled: self
                .full_worker_dispatch_fallback_enabled
                .clone(),
            lism_ab_sink: self.lism_ab_sink.clone(),
        }
    }

    /// If the file doesn't exist, it falls back to a fresh coordinator.
    /// State-only restore — no callbacks wired. Prefer `restore_or_init_with_app_state` in production.
    #[doc(hidden)]
    pub async fn restore_or_init(path: &std::path::Path) -> anyhow::Result<Self> {
        Self::restore_or_init_with_owner(path, Arc::new(BossRuntimeOwner::default())).await
    }

    /// Restore (or init) and immediately bootstrap with full A+B callbacks.
    /// After this call the registry is in full mode — no lazy upgrade on first production entry.
    /// Full-mode restore — restores from file (or creates fresh) and bootstraps A+B callbacks.
    /// Prefer `BossRuntimeHost::restore_or_init_coordinator` in production so the host's
    /// `BossRuntimeOwner` is used. This static helper creates a throwaway owner.
    #[doc(hidden)]
    pub async fn restore_or_init_with_app_state(
        path: &std::path::Path,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Self> {
        let coordinator =
            Self::restore_or_init_with_owner(path, Arc::new(BossRuntimeOwner::default())).await?;
        coordinator
            .bootstrap_actor_registry_with_app_state(app_state)
            .await;
        Ok(coordinator)
    }

    pub async fn restore_or_init_with_owner(
        path: &std::path::Path,
        runtime_owner: Arc<BossRuntimeOwner>,
    ) -> anyhow::Result<Self> {
        let coordinator = Self::new_with_runtime_owner(runtime_owner);

        if path.exists() {
            let loaded_plan = load_plan(path).await?;

            // Determine stage based on plan progress
            let mut stage = BossStage::Documentation;
            if loaded_plan.accepted_by_user {
                let all_completed =
                    !loaded_plan.steps.is_empty() && loaded_plan.steps.iter().all(|s| s.completed);
                if all_completed {
                    stage = BossStage::Completed;
                } else {
                    stage = BossStage::Execution;
                }
            }

            // Figure out the current step (first uncompleted)
            let mut current_step = None;
            let total_steps = Some(loaded_plan.steps.len());
            if loaded_plan.accepted_by_user {
                current_step = loaded_plan
                    .steps
                    .iter()
                    .find(|s| !s.completed)
                    .map(|s| s.id);
            }

            {
                let mut status = coordinator.status.write().await;
                status.stage = stage;
                status.planning_file = Some(path.to_string_lossy().into_owned());
                status.current_step = current_step;
                status.total_steps = total_steps;
            }

            {
                let mut plan_guard = coordinator.plan.write().await;
                *plan_guard = Some(loaded_plan.clone());
            }

            // Init actor session — prefer persisted snapshot so A/B identity
            // (session_id / task_id) survives restart. Fallback to deterministic
            // placeholder when no snapshot exists (new plan or old plan file).
            {
                let mut session_guard = coordinator.session.write().await;
                *session_guard = Some(
                    loaded_plan
                        .session_snapshot
                        .clone()
                        .unwrap_or_else(|| BossSession::from_plan_id(&loaded_plan.plan_id, stage)),
                );
            }

            // Bootstrap actor runtimes for A and B.
            coordinator.bootstrap_actor_registry().await;
        } else {
            let mut status = coordinator.status.write().await;
            status.planning_file = Some(path.to_string_lossy().into_owned());
        }

        Ok(coordinator)
    }

    pub async fn get_stage(&self) -> BossStage {
        self.status.read().await.stage
    }

    /// Ensures a BossSession exists for the given plan_id, creating one if absent.
    /// Idempotent: if a session already exists for the same plan_id it is returned unchanged.
    pub async fn ensure_actor_session(&self, plan_id: &str, stage: BossStage) {
        let mut guard = self.session.write().await;
        if guard.as_ref().map(|s| s.plan_id.as_str()) != Some(plan_id) {
            *guard = Some(BossSession::from_plan_id(plan_id, stage));
        }
    }

    /// Spawn fresh A and B actor runtimes (state-only, no execution callback).
    /// Low-level / test-only. Production code must use `bootstrap_actor_registry_with_app_state`.
    #[doc(hidden)]
    pub async fn bootstrap_actor_registry(&self) {
        let registry = BossActorRegistry::bootstrap();
        let mut guard = self.actor_registry.write().await;
        if let Some(old) = guard.take() {
            old.shutdown_all();
        }
        *guard = Some(registry);
    }

    /// Bootstrap A and B with all callbacks wired in one shot.
    /// No-op if the registry already has both executor and A callbacks.
    /// This is the preferred production path — call once when AppState is available.
    pub async fn bootstrap_actor_registry_with_app_state(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) {
        let already_full = {
            let guard = self.actor_registry.read().await;
            guard
                .as_ref()
                .map(|r| r.has_executor && r.has_a_callbacks)
                .unwrap_or(false)
        };
        if already_full {
            return;
        }
        // Store app_state for auto path (finalize/approval may call it without app_state param).
        {
            let mut guard = self.auto_advance_app_state.write().await;
            *guard = Some(app_state.clone());
        }
        let exec_fn = Self::build_exec_fn(self, app_state);
        let spec_review_fn = Self::build_spec_review_fn(self, app_state);
        let review_fn = Self::build_review_fn(self);
        let doc_fn = Self::build_doc_fn(self, app_state);
        let registry = crate::core::boss_actor_runtime::bootstrap_with_all_callbacks(
            exec_fn,
            spec_review_fn,
            review_fn,
            doc_fn,
        );
        let mut guard = self.actor_registry.write().await;
        if let Some(old) = guard.take() {
            old.shutdown_all();
        }
        *guard = Some(registry);
    }

    fn build_exec_fn(
        coordinator: &Self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> crate::core::boss_actor_runtime::ExecutionFn {
        let c = coordinator.clone_for_runtime();
        let app = app_state.clone();
        Arc::new(move |payload: String| {
            let c = c.clone_for_runtime();
            let app = app.clone();
            Box::pin(async move {
                {
                    let mut guard = c.status.write().await;
                    guard.last_b_dispatch_payload = Some(payload.clone());
                }
                // Invoke AgentTool (Spawn or Continue — payload already encodes which).
                // Write the returned task_id back to executor_b so subsequent dispatches
                // can reuse the same B session via Continue.
                match c.invoke_agent_tool_with_task_id(&app, &payload).await {
                    Ok(task_id) => {
                        c.record_b_session_id(&task_id).await;
                        Ok(payload)
                    }
                    Err(e) => Err(e),
                }
            })
        })
    }

    /// Build B's spec-review callback for the Documentation stage.
    /// B receives the spec, calls its LLM session, and returns review feedback.
    fn build_spec_review_fn(
        coordinator: &Self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> crate::core::boss_actor_runtime::SpecReviewFn {
        let c = coordinator.clone_for_runtime();
        let app = app_state.clone();
        Arc::new(move |spec: String| {
            let c = c.clone_for_runtime();
            let app = app.clone();
            Box::pin(async move {
                c.ensure_b_session(&app, 0).await;
                let msg = format!(
                    "Please review the following spec for feasibility, risk, and testability. \
                     Respond with LGTM if acceptable, or FEEDBACK: <your feedback> if changes are needed.\n\n{spec}"
                );
                match c.ask_b_session(&app, msg).await {
                    Ok(response) => Ok(response),
                    Err(_) => Ok("LGTM".to_string()),
                }
            })
        })
    }

    /// Ask A to draft a technical spec from `task_description`.
    /// Calls `ensure_a_session` then `ask_a_session`; returns A's response as the draft spec.
    /// Returns `Err` if A's session is unavailable or times out.
    pub async fn draft_spec_with_a(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        task_description: &str,
    ) -> anyhow::Result<String> {
        self.ensure_a_session(app_state).await;
        let msg = format!(
            "Draft a technical specification for the following task. \
             Include objectives, acceptance criteria, and a high-level approach. \
             Task: {task_description}"
        );
        self.ask_a_session(app_state, msg).await
    }

    /// Write a real B task id back to BossSession.executor_b after a successful spawn/continue.
    async fn record_b_session_id(&self, task_id: &str) {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.session_id = task_id.to_string();
            session.executor_b.task_id = Some(task_id.to_string());
            session.executor_b.status = crate::core::boss_state::BossActorStatus::Active;
        }
    }

    /// Ensure Executor B has a real LLM session. On first call, spawns a B session via
    /// AgentTool and writes the task id back to BossSession.executor_b.session_id.
    /// Subsequent calls are no-ops if the session id is already a real task id.
    async fn ensure_b_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        step_id: usize,
    ) {
        if app_state.permission_context.task_manager.is_none() {
            return;
        }
        let is_placeholder = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| {
                    let placeholder = format!("boss-{}-b", s.plan_id);
                    s.executor_b.session_id == placeholder || s.executor_b.session_id.is_empty()
                })
                .unwrap_or(true)
        };
        if !is_placeholder {
            return;
        }

        let parent_session_id = app_state.active_session_id.clone();
        let b_actor_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.executor_b.actor_id.clone())
                .unwrap_or_else(|| "boss-unknown-b".into())
        };
        let spawn_build = match self
            .build_step_spawn_payload_internal(step_id, &parent_session_id, &b_actor_id)
            .await
        {
            Ok(build) => build,
            Err(_) => return,
        };
        self.record_b_assignment_contract(
            &spawn_build.assignment_fingerprint,
            &spawn_build.plan_version,
            &spawn_build.step_revision,
        )
        .await;
        let payload = spawn_build.payload;

        if let Ok(task_id) = self
            .invoke_agent_tool_with_task_id(app_state, &payload)
            .await
        {
            self.record_b_session_id(&task_id).await;
        }
    }

    fn build_review_fn(coordinator: &Self) -> crate::core::boss_actor_runtime::ReviewFn {
        let c = coordinator.clone_for_runtime();
        Arc::new(
            move |step_id, accepted, summary: String, correction: Option<String>| {
                let c = c.clone_for_runtime();
                Box::pin(async move {
                    let app_state = {
                        let guard = c.auto_advance_app_state.read().await;
                        guard.clone()
                    };
                    if let Some(app) = app_state {
                        c.ensure_a_session(&app).await;
                        let verdict_hint = if accepted { "accepted" } else { "rejected" };
                        let msg = match correction.as_deref() {
                            Some(corr) => format!(
                                "Review step {step_id}: coordinator verdict={verdict_hint}. Summary: {summary}. Correction: {corr}. Please respond with ACCEPT, REJECT, or REPLAN_STEP. If REJECT include CORRECTION: <your correction>. If REPLAN_STEP include REASON: <why this step needs replanning>."
                            ),
                            None => format!(
                                "Review step {step_id}: coordinator verdict={verdict_hint}. Summary: {summary}. Please respond with ACCEPT, REJECT, or REPLAN_STEP. If REJECT include CORRECTION: <your correction>. If REPLAN_STEP include REASON: <why this step needs replanning>."
                            ),
                        };
                        match c.ask_a_session(&app, msg).await {
                            Ok(response) => {
                                let decision = Self::parse_a_review_decision(&response, &summary);
                                c.apply_review_verdict(step_id, &decision).await?;
                                return Ok(decision);
                            }
                            Err(_) => {}
                        }
                    }
                    let decision = if accepted {
                        crate::core::boss_actor_runtime::ReviewDecision::Accept {
                            summary: summary.clone(),
                        }
                    } else {
                        crate::core::boss_actor_runtime::ReviewDecision::Correct {
                            summary: summary.clone(),
                            correction,
                        }
                    };
                    c.apply_review_verdict(step_id, &decision).await?;
                    Ok(decision)
                })
            },
        )
    }

    fn build_doc_fn(
        coordinator: &Self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> crate::core::boss_actor_runtime::DocumentationFn {
        let c = coordinator.clone_for_runtime();
        let app = app_state.clone();
        Arc::new(move |signal: String| {
            let c = c.clone_for_runtime();
            let app = app.clone();
            Box::pin(async move {
                c.ensure_a_session(&app).await;
                let msg = format!(
                    "Documentation signal: {signal}. Please acknowledge with ACCEPT or provide CORRECTION: <feedback>."
                );
                let effective_signal = match c.ask_a_session(&app, msg).await {
                    Ok(response) => {
                        let decision = Self::parse_a_review_decision(&response, &signal);
                        match decision {
                            crate::core::boss_actor_runtime::ReviewDecision::Accept { .. } => {
                                signal.clone()
                            }
                            crate::core::boss_actor_runtime::ReviewDecision::Correct {
                                correction,
                                ..
                            } => correction.unwrap_or_else(|| signal.clone()),
                            crate::core::boss_actor_runtime::ReviewDecision::ReplanStep {
                                reason,
                                ..
                            } => reason,
                        }
                    }
                    Err(_) => signal.clone(),
                };
                c.apply_documentation_signal(&app, &effective_signal).await
            })
        })
    }

    /// Ensure actor runtimes exist with a real execution callback wired to B.
    /// If the registry already has an executor, this is a no-op.
    #[deprecated(note = "use bootstrap_actor_registry_with_app_state directly")]
    pub async fn ensure_actor_registry_with_executor(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) {
        // Prefer the full bootstrap — wires A and B in one shot.
        self.bootstrap_actor_registry_with_app_state(app_state)
            .await;
    }

    /// Ensure actor runtimes exist; bootstrap if not yet initialized.
    /// State-only fallback — no callbacks wired. Prefer `bootstrap_actor_registry_with_app_state`.
    #[doc(hidden)]
    pub async fn ensure_actor_registry(&self) {
        let needs_bootstrap = self.actor_registry.read().await.is_none();
        if needs_bootstrap {
            self.bootstrap_actor_registry().await;
        }
    }

    /// Ensure A's callbacks are wired using the stored auto_advance_app_state.
    /// No-op if already fully bootstrapped or if no app_state is available.
    pub async fn ensure_actor_registry_with_a_callbacks_auto(&self) {
        let app_state = self.auto_advance_app_state.read().await.clone();
        if let Some(app) = app_state {
            self.bootstrap_actor_registry_with_app_state(&app).await;
        } else {
            self.ensure_actor_registry().await;
        }
    }

    /// Ensure A's review and documentation callbacks are wired.
    /// Delegates to bootstrap_actor_registry_with_app_state — no-op if already fully bootstrapped.
    #[deprecated(note = "use bootstrap_actor_registry_with_app_state directly")]
    pub async fn ensure_actor_registry_with_a_callbacks(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) {
        self.bootstrap_actor_registry_with_app_state(app_state)
            .await;
    }

    /// Returns a point-in-time snapshot of all tracked actor handles (A, B, and children).
    pub async fn actor_registry_snapshot(&self) -> Vec<BossActorHandle> {
        let guard = self.session.read().await;
        let Some(session) = guard.as_ref() else {
            return Vec::new();
        };
        let mut handles = vec![session.designer_a.clone(), session.executor_b.clone()];
        handles.extend(session.active_children.iter().cloned());
        handles
    }

    /// Updates the status of a tracked actor by actor_id.
    pub async fn update_actor_status(&self, actor_id: &str, status: BossActorStatus) {
        let mut guard = self.session.write().await;
        let Some(session) = guard.as_mut() else {
            return;
        };
        for handle in std::iter::once(&mut session.designer_a)
            .chain(std::iter::once(&mut session.executor_b))
            .chain(session.active_children.iter_mut())
        {
            if handle.actor_id == actor_id {
                handle.status = status;
                handle.last_snapshot = Some(std::time::SystemTime::now());
                return;
            }
        }
    }

    /// Enforces a strict DAG state transition to prevent invalid lifecycle jumps.
    pub async fn transition_to(&self, new_stage: BossStage) -> anyhow::Result<()> {
        let mut status = self.status.write().await;
        // Verify valid transition
        let valid = match (status.stage, new_stage) {
            (BossStage::Documentation, BossStage::WaitingForApproval) => true,
            (BossStage::WaitingForApproval, BossStage::Execution) => true,
            (BossStage::WaitingForApproval, BossStage::Documentation) => true, // Rejected by user
            (BossStage::Execution, BossStage::Completed) => true,
            (BossStage::Documentation, BossStage::Documentation) => true, // Re-entering valid
            (BossStage::Execution, BossStage::Documentation) => true,     // Fallback/Fatal failure
            _ => false,
        };

        if !valid {
            anyhow::bail!(
                "Invalid BossStage transition from {:?} to {:?}",
                status.stage,
                new_stage
            );
        }

        status.stage = new_stage;
        Ok(())
    }

    /// Returns the default path for the immutable planning cache.
    pub fn default_plan_path(root: &std::path::Path) -> std::path::PathBuf {
        root.join(crate::bootstrap::config_root::PRIMARY_CONFIG_DIR)
            .join("boss")
            .join("planning.json")
    }

    /// Records one Documentation-stage red/blue loop pass.
    ///
    /// A drafts the spec, B reviews feasibility/risk/testability, then A revises.
    /// The revised spec becomes the immutable planning content and the coordinator
    /// transitions to `WaitingForApproval`.
    pub async fn finalize_documentation_loop(
        &self,
        draft_spec: &str,
        review_feedback: &str,
        revision_notes: &str,
        final_document_spec: &str,
        final_pseudo_code: &str,
    ) -> anyhow::Result<()> {
        // If no draft_spec was supplied, ask A to generate one from the plan's task_description.
        let effective_draft_spec: String;
        let draft_spec = if draft_spec.is_empty() {
            let task_description = {
                let guard = self.plan.read().await;
                guard
                    .as_ref()
                    .map(|p| p.task_description.clone())
                    .unwrap_or_default()
            };
            let app_state = self.auto_advance_app_state.read().await.clone();
            effective_draft_spec = if let Some(app) = app_state {
                self.draft_spec_with_a(&app, &task_description).await?
            } else {
                anyhow::bail!("draft_spec is empty and no app_state available for A session");
            };
            effective_draft_spec.as_str()
        } else {
            draft_spec
        };

        // Ask B to review the draft spec before finalizing.
        // B's feedback is stored alongside A's revision notes.
        self.ensure_actor_registry_with_a_callbacks_auto().await;
        let b_feedback = {
            let registry_guard = self.actor_registry.read().await;
            if let Some(registry) = registry_guard.as_ref() {
                let mailbox = registry.b_mailbox().clone();
                drop(registry_guard);
                match mailbox
                    .request(
                        crate::core::boss_actor_runtime::ExecutorBCommand::ReviewSpec {
                            spec: draft_spec.to_string(),
                        },
                    )
                    .await
                {
                    Ok(crate::core::boss_actor_runtime::BossActorEvent::SpecReviewed {
                        feedback,
                    }) => Some(feedback),
                    _ => None,
                }
            } else {
                drop(registry_guard);
                None
            }
        };

        // Use B's feedback if caller didn't supply one, otherwise keep caller's value.
        let effective_review_feedback = if review_feedback.is_empty() {
            b_feedback.as_deref().unwrap_or("LGTM")
        } else {
            review_feedback
        };

        // Mutate plan state first (coordinator owns the data).
        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            plan.draft_spec = Some(draft_spec.to_string());
            plan.review_feedback = Some(effective_review_feedback.to_string());
            plan.revision_notes = Some(revision_notes.to_string());
            plan.document_spec = final_document_spec.to_string();
            plan.pseudo_code = final_pseudo_code.to_string();
            plan.finalized = true;
            plan.accepted_by_user = false;
        }

        let path_to_save = self.status.read().await.planning_file.clone();
        if let Some(path_str) = path_to_save {
            let path = std::path::PathBuf::from(path_str);
            self.save_plan_with_session(&path).await?;
        }

        // Send FinalizeDocumentation to A mailbox — A's handler drives the stage transition.
        if let Some(registry) = self.actor_registry.read().await.as_ref() {
            let _ = registry
                .a_mailbox()
                .request(DesignerACommand::FinalizeDocumentation {
                    signal: "finalize".to_string(),
                })
                .await;
        }

        // Fallback: if A's callback is not wired, coordinator transitions directly.
        let has_a_callbacks = self
            .actor_registry
            .read()
            .await
            .as_ref()
            .map(|r| r.has_a_callbacks)
            .unwrap_or(false);
        if !has_a_callbacks {
            self.transition_to(BossStage::WaitingForApproval).await?;
        }
        Ok(())
    }

    /// Records user feedback while in the documentation/approval loop.
    /// Any non-confirmation input during `WaitingForApproval` reopens Documentation,
    /// preserving a feedback trail for the next A/B revision pass.
    pub async fn record_documentation_feedback(&self, feedback: &str) -> anyhow::Result<()> {
        let trimmed = feedback.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            plan.documentation_feedback.push(trimmed.to_string());
            plan.accepted_by_user = false;
            plan.finalized = false;
        }

        let path_to_save = self.status.read().await.planning_file.clone();
        if let Some(path_str) = path_to_save {
            let path = std::path::PathBuf::from(path_str);
            self.save_plan_with_session(&path).await?;
        }

        self.transition_to(BossStage::Documentation).await?;
        Ok(())
    }

    /// Handles the user confirmation for transitioning from Documentation -> Execution.
    /// MUST only be called when in WaitingForApproval.
    /// Returns true if user confirmed (Y/enter), false if they provided feedback (re-enter Documentation).
    pub async fn handle_user_approval(&self, user_input: &str) -> anyhow::Result<bool> {
        let path_to_save = {
            let status = self.status.read().await;
            if status.stage != BossStage::WaitingForApproval {
                tracing::warn!(
                    "handle_user_approval called in wrong state: {:?}",
                    status.stage
                );
                return Ok(false);
            }
            status.planning_file.clone()
        };

        let approved = user_input.trim().to_uppercase() == "Y" || user_input.trim().is_empty();

        if approved {
            {
                let mut plan_guard = self.plan.write().await;
                if let Some(plan) = plan_guard.as_mut() {
                    if !plan.finalized {
                        tracing::warn!(
                            "handle_user_approval called before documentation loop finalized"
                        );
                        return Ok(false);
                    }
                    plan.accepted_by_user = true;
                }
            }
            if let Some(path_str) = path_to_save {
                let path = std::path::PathBuf::from(path_str);
                self.save_plan_with_session(&path).await?;
            }
        }

        // Send UserApproval to A mailbox — A's handler drives the stage transition.
        // Wire A's callbacks via auto path (uses stored auto_advance_app_state).
        self.ensure_actor_registry_with_a_callbacks_auto().await;
        let a_approved = if let Some(registry) = self.actor_registry.read().await.as_ref() {
            match registry
                .a_mailbox()
                .request(DesignerACommand::UserApproval {
                    input: user_input.to_string(),
                })
                .await
            {
                Ok(BossActorEvent::ApprovalHandled { approved: a }) => a,
                _ => approved,
            }
        } else {
            approved
        };

        // Fallback: if A's callback is not wired, coordinator transitions directly.
        let has_a_callbacks = self
            .actor_registry
            .read()
            .await
            .as_ref()
            .map(|r| r.has_a_callbacks)
            .unwrap_or(false);
        if !has_a_callbacks {
            if a_approved {
                self.transition_to(BossStage::Execution).await?;
            } else {
                self.record_documentation_feedback(user_input).await?;
            }
        }

        Ok(a_approved)
    }

    pub async fn handle_control_request(
        &self,
        request: BossControlRequest,
        tasks: &TaskManager,
        dispatcher: &NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        self.send_control_request(request, Arc::new(tasks.clone()), dispatcher.clone())
            .await
    }

    pub(crate) async fn handle_control_request_direct(
        &self,
        request: BossControlRequest,
        tasks: &TaskManager,
        dispatcher: &NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        match request {
            BossControlRequest::Report => Ok(BossControlResponse::Report(
                self.report_progress(tasks).await?,
            )),
            BossControlRequest::Stop {
                requester_session_id,
                deadline_ms,
            } => Ok(BossControlResponse::Stop(
                self.stop(tasks, &requester_session_id, dispatcher, deadline_ms)
                    .await?,
            )),
        }
    }

    pub async fn routed_step_metadata_snapshot(
        &self,
    ) -> std::collections::HashMap<usize, BossStepRoutedMetadata> {
        self.routed_step_metadata.read().await.clone()
    }

    async fn persist_plan_if_configured(&self) -> anyhow::Result<()> {
        let plan_path = self.status.read().await.planning_file.clone();
        if let Some(path) = plan_path {
            self.save_plan_with_session(std::path::Path::new(&path))
                .await?;
        }
        Ok(())
    }

    async fn trigger_review_for_completed_step(
        &self,
        step_id: usize,
        review_summary: String,
    ) -> anyhow::Result<()> {
        self.update_current_step(Some(step_id)).await;
        self.persist_plan_if_configured().await?;
        self.on_review_event(step_id, true, &review_summary, None)
            .await
    }

    pub async fn set_lism_policy(&self, policy: BossLisMPolicy) {
        *self.lism_policy.write().await = policy;
    }

    /// Synchronous policy initializer — only safe to call before the coordinator is Arc-wrapped.
    pub fn init_lism_policy(&mut self, policy: BossLisMPolicy) {
        if let Ok(mut guard) = self.lism_policy.try_write() {
            *guard = policy;
        }
    }

    pub async fn lism_policy(&self) -> BossLisMPolicy {
        *self.lism_policy.read().await
    }

    pub async fn set_worker_lism_policy(&self, policy: WorkerLisMPolicy) {
        *self.worker_lism_policy.write().await = policy;
    }

    /// Synchronous policy initializer — only safe to call before the coordinator is Arc-wrapped.
    pub fn init_worker_lism_policy(&mut self, policy: WorkerLisMPolicy) {
        if let Ok(mut guard) = self.worker_lism_policy.try_write() {
            *guard = policy;
        }
    }

    pub async fn worker_lism_policy(&self) -> WorkerLisMPolicy {
        *self.worker_lism_policy.read().await
    }

    pub async fn set_full_worker_dispatch_fallback_enabled(&self, enabled: bool) {
        *self.full_worker_dispatch_fallback_enabled.write().await = enabled;
    }

    pub fn init_full_worker_dispatch_fallback_enabled(&mut self, enabled: bool) {
        if let Ok(mut guard) = self.full_worker_dispatch_fallback_enabled.try_write() {
            *guard = enabled;
        }
    }

    pub async fn full_worker_dispatch_fallback_enabled(&self) -> bool {
        *self.full_worker_dispatch_fallback_enabled.read().await
    }

    /// Attach a LisM A/B sample sink. Call before the first `advance_plan`.
    pub fn with_lism_ab_sink(mut self, sink: SharedLisMAbSampleSink) -> Self {
        self.lism_ab_sink = Some(sink);
        self
    }

    /// Attach a LisM A/B sample sink in place (for post-construction wiring).
    pub fn set_lism_ab_sink(&mut self, sink: SharedLisMAbSampleSink) {
        self.lism_ab_sink = Some(sink);
    }

    /// Accessor for the LisM A/B sink — callers can record rolled_back outcomes.
    pub fn lism_ab_sink(&self) -> Option<&SharedLisMAbSampleSink> {
        self.lism_ab_sink.as_ref()
    }

    /// Inject a single-step execution plan for non-interactive `--boss-task` runs.
    /// Sets stage to Execution and current_step to 0 so `advance_plan` dispatches immediately.
    /// Safe to call after bootstrap_coordinator but before advance_plan.
    pub async fn seed_plan_for_task(&self, task: &str) {
        let plan_id = format!(
            "boss-task-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        let plan = BossPlan {
            plan_id: plan_id.clone(),
            task_description: task.to_string(),
            document_spec: task.to_string(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: true,
            documentation_feedback: vec![],
            steps: vec![BossPlanStep {
                id: 0,
                description: task.to_string(),
                objective: Some(task.to_string()),
                acceptance: seed_step_acceptance(task),
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
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: true,
            session_snapshot: None,
        };
        {
            let mut plan_guard = self.plan.write().await;
            *plan_guard = Some(plan);
        }
        {
            let mut status = self.status.write().await;
            status.stage = BossStage::Execution;
            status.current_step = Some(0);
            status.total_steps = Some(1);
        }
        {
            let mut session_guard = self.session.write().await;
            *session_guard = Some(BossSession::from_plan_id(&plan_id, BossStage::Execution));
        }
    }

    /// Stable run identifier derived from plan_id, or a timestamp fallback.
    async fn current_run_id(&self) -> String {
        self.session
            .read()
            .await
            .as_ref()
            .map(|s| s.plan_id.clone())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| format!("boss-run-{}", d.as_millis()))
                    .unwrap_or_else(|_| "boss-run-unknown".to_string())
            })
    }

    pub async fn has_terminal_failure(&self) -> bool {
        self.plan.read().await.as_ref().is_some_and(|plan| {
            plan.steps
                .iter()
                .any(|step| step.status.is_terminal_failure())
        })
    }

    fn add_task_usage_to_observability(
        summary: &mut BossObservabilitySummary,
        usage: &TaskUsageSummary,
    ) {
        summary.total_input_tokens += usage.input_tokens;
        summary.total_uncached_input_tokens += usage.uncached_input_tokens;
        summary.total_output_tokens += usage.output_tokens;
        summary.total_cache_read_tokens += usage.cache_read_input_tokens;
        summary.total_cache_write_tokens += usage.cache_creation_input_tokens;
        summary.total_original_chars += usage.original_prompt_chars;
        summary.total_sent_chars += usage.sent_prompt_chars;
        summary.estimated_cost_micros_usd += usage.estimated_cost_micros_usd;
    }

    fn routed_metadata_has_usage(m: &BossStepRoutedMetadata) -> bool {
        m.input_tokens.unwrap_or(0) > 0
            || m.uncached_input_tokens.unwrap_or(0) > 0
            || m.output_tokens.unwrap_or(0) > 0
            || m.cache_read_tokens.unwrap_or(0) > 0
            || m.cache_write_tokens.unwrap_or(0) > 0
            || m.original_prompt_chars.unwrap_or(0) > 0
            || m.sent_prompt_chars.unwrap_or(0) > 0
            || m.estimated_cost_micros_usd.unwrap_or(0) > 0
    }

    fn apply_loop_usage_to_routed_metadata(
        routed_metadata: &mut BossStepRoutedMetadata,
        usage: &crate::core::state_frame_loop::LoopUsage,
    ) {
        routed_metadata.input_tokens = Some(usage.input_tokens);
        routed_metadata.uncached_input_tokens = Some(usage.uncached_input_tokens);
        routed_metadata.output_tokens = Some(usage.output_tokens);
        routed_metadata.cache_read_tokens = Some(usage.cache_read_tokens);
        routed_metadata.cache_write_tokens = Some(usage.cache_write_tokens);
        routed_metadata.original_prompt_chars = Some(usage.original_prompt_chars);
        routed_metadata.sent_prompt_chars = Some(usage.sent_prompt_chars);
        routed_metadata.estimated_cost_micros_usd = Some(usage.estimated_cost_micros_usd);
        routed_metadata.fallback_count = Some(usage.fallback_count);
        routed_metadata.fallback_tier = usage.fallback_tier.clone();
        routed_metadata.fallback_reason = usage.fallback_reason.clone();
        routed_metadata.hydration_count = Some(usage.hydration_count);
        routed_metadata.stale_ref_count = Some(usage.stale_ref_count);
        routed_metadata.hydration_ref_missing = Some(usage.hydration_ref_missing);
        routed_metadata.tool_dispatch_count = Some(usage.tool_dispatch_count);
        routed_metadata.tool_dispatch_success_count = Some(usage.tool_dispatch_success_count);
        routed_metadata.tool_dispatch_failure_count = Some(usage.tool_dispatch_failure_count);
        routed_metadata.tool_dispatch_ref_write_count = Some(usage.tool_dispatch_ref_write_count);
        routed_metadata.tool_dispatch_failure_taxonomy =
            usage.tool_dispatch_failure_taxonomy.clone();
        routed_metadata.last_effective_tool_action = usage.last_effective_tool_action.clone();
        if let Some(outcome) = usage.last_failure_outcome.as_ref() {
            routed_metadata.last_failure_kind = Some(outcome.kind.as_str().to_string());
            routed_metadata.last_failure_recoverable = Some(outcome.recoverable);
            routed_metadata.last_recommended_repair = outcome.recommended_next_action.clone();
            routed_metadata.last_failure_evidence_ref = outcome.evidence_ref.clone();
            routed_metadata.last_failure_bounded_excerpt = outcome.bounded_excerpt.clone();
            routed_metadata.last_failure_truncated = Some(outcome.truncated);
        } else {
            routed_metadata.last_failure_kind = None;
            routed_metadata.last_failure_recoverable = None;
            routed_metadata.last_recommended_repair = None;
            routed_metadata.last_failure_evidence_ref = None;
            routed_metadata.last_failure_bounded_excerpt = None;
            routed_metadata.last_failure_truncated = None;
        }
        routed_metadata.recovery_attempted = Some(usage.recovery_attempted);
        routed_metadata.recovery_tier = usage.recovery_tier.clone();
        routed_metadata.recovery_outcome = usage.recovery_outcome.clone();
        routed_metadata.terminal_blocker_kind = usage.terminal_blocker_kind.clone();
        routed_metadata.completion_evidence_status = usage
            .completion_evidence_status
            .as_ref()
            .map(|status| status.as_str().to_string());
        routed_metadata.worker_report = usage.worker_report.clone();
        routed_metadata.completion_evidence_gaps = usage
            .worker_report
            .as_ref()
            .map(|report| report.completion_evidence_gaps.clone())
            .unwrap_or_default();
    }

    async fn mark_routed_metadata_artifact_recovery(
        &self,
        step_id: usize,
        recovery_outcome: &str,
        terminal_blocker_kind: Option<&str>,
    ) {
        let mut routed_step_metadata = self.routed_step_metadata.write().await;
        if let Some(metadata) = routed_step_metadata.get_mut(&step_id) {
            metadata.recovery_attempted = Some(true);
            metadata.recovery_tier = Some("boss_artifact_repair".into());
            metadata.recovery_outcome = Some(recovery_outcome.into());
            metadata.terminal_blocker_kind = terminal_blocker_kind.map(str::to_string);
        }
    }

    fn build_observability_summary(
        steps: &[BossStepReport],
        tasks: Option<&TaskManager>,
        step_metrics: Option<&BossStepMetrics>,
    ) -> Option<BossObservabilitySummary> {
        let mut summary = BossObservabilitySummary::default();
        let mut has_observability = false;

        for step in steps {
            if let Some(m) = &step.routed_metadata {
                has_observability = true;
                summary.total_steps_routed += 1;
                summary.total_cache_read_tokens += m.cache_read_tokens.unwrap_or(0);
                summary.total_cache_write_tokens += m.cache_write_tokens.unwrap_or(0);
                summary.total_fallback_count += m.fallback_count.unwrap_or(0);
                if let Some(tier) = &m.fallback_tier {
                    *summary
                        .fallback_tier_counts
                        .entry(tier.clone())
                        .or_insert(0) += 1;
                }
                if let Some(reason) = &m.fallback_reason {
                    *summary
                        .fallback_reason_counts
                        .entry(reason.clone())
                        .or_insert(0) += 1;
                }
                summary.total_projection_mismatch_count += m.projection_mismatch_count.unwrap_or(0);
                summary.total_hydration_count += m.hydration_count.unwrap_or(0);
                summary.total_stale_ref_count += m.stale_ref_count.unwrap_or(0);
                summary.total_hydration_ref_missing += m.hydration_ref_missing.unwrap_or(0);
                summary.total_tool_dispatch_count += m.tool_dispatch_count.unwrap_or(0);
                summary.total_tool_dispatch_success_count +=
                    m.tool_dispatch_success_count.unwrap_or(0);
                summary.total_tool_dispatch_failure_count +=
                    m.tool_dispatch_failure_count.unwrap_or(0);
                summary.total_tool_dispatch_ref_write_count +=
                    m.tool_dispatch_ref_write_count.unwrap_or(0);
                for (reason, count) in &m.tool_dispatch_failure_taxonomy {
                    *summary
                        .tool_dispatch_failure_taxonomy
                        .entry(reason.clone())
                        .or_insert(0) += count;
                }
                summary.total_input_tokens += m.input_tokens.unwrap_or(0);
                summary.total_uncached_input_tokens += m.uncached_input_tokens.unwrap_or(0);
                summary.total_output_tokens += m.output_tokens.unwrap_or(0);
                summary.total_original_chars += m.original_prompt_chars.unwrap_or(0);
                summary.total_sent_chars += m.sent_prompt_chars.unwrap_or(0);
                summary.estimated_cost_micros_usd += m.estimated_cost_micros_usd.unwrap_or(0);
                if m.provider_profile_id.is_some() {
                    summary.override_hit_count += 1;
                }
                if let Some(tier) = &m.model_tier {
                    *summary.model_tier_counts.entry(tier.clone()).or_insert(0) += 1;
                }
                if Self::routed_metadata_has_usage(m) {
                    continue;
                }
            }

            let Some(task_usage) = step
                .worker_task_id
                .as_deref()
                .and_then(|task_id| tasks.and_then(|tasks| tasks.get(task_id)))
                .and_then(|task| task.usage)
            else {
                continue;
            };
            if !task_usage.is_empty() {
                has_observability = true;
                Self::add_task_usage_to_observability(&mut summary, &task_usage);
            }
        }

        if let Some(metrics) = step_metrics {
            has_observability = true;
            summary.total_original_chars += metrics.original_chars;
            summary.total_sent_chars += metrics.sent_chars;
            summary.total_cache_read_tokens += metrics.cache_read_tokens;
            summary.total_cache_write_tokens += metrics.cache_creation_tokens;
        }

        has_observability.then_some(summary)
    }

    /// Build a `BossReportPayload` snapshot suitable for LisM sampling.
    /// `tasks` is optional so LisM direct execution can still report routed metadata,
    /// while full-context worker runs can contribute persisted task usage.
    async fn build_lism_sample_report(&self, tasks: Option<&TaskManager>) -> BossReportPayload {
        let status = self.status.read().await.clone();
        let session = self.session.read().await.clone();
        let plan = self.plan.read().await.clone();
        let routed_step_metadata = self.routed_step_metadata.read().await.clone();
        let empty_session = BossSession::from_plan_id("unknown", status.stage);
        let session = session.unwrap_or(empty_session);
        let steps = plan
            .as_ref()
            .map(|plan| {
                plan.steps
                    .iter()
                    .map(|step| BossStepReport {
                        id: step.id,
                        status: step.status,
                        worker_task_id: step.worker_task_id.clone(),
                        attempt_count: step.attempt_count,
                        last_review_summary: step.last_review_summary.clone(),
                        action_required: None,
                        blocker_reason: None,
                        routed_metadata: routed_step_metadata.get(&step.id).cloned(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let step_metrics = status.last_step_metrics.clone();
        let observability_summary =
            Self::build_observability_summary(&steps, tasks, step_metrics.as_ref());

        BossReportPayload {
            stage: status.stage,
            current_step: status.current_step,
            total_steps: status.total_steps,
            designer_a: session.designer_a,
            executor_b: session.executor_b,
            active_children: session.active_children,
            steps,
            history_summary: vec![],
            observability_summary,
            lism_policy: self.lism_policy().await,
        }
    }

    /// Fire-and-forget: record a LisM A/B sample if a sink is configured.
    /// Never blocks the main flow; failures are logged as warnings.
    async fn emit_lism_sample(
        &self,
        run_id: &str,
        lism_enabled: bool,
        outcome: BossTestRunOutcome,
        pending_approval_count: usize,
    ) {
        let Some(sink) = &self.lism_ab_sink else {
            return;
        };
        let task_manager = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| app_state.permission_context.task_manager.clone());
        let report = self.build_lism_sample_report(task_manager.as_deref()).await;
        sink.record_run(
            run_id,
            lism_enabled,
            &report,
            outcome,
            pending_approval_count,
        );
    }
    pub async fn report_progress(&self, tasks: &TaskManager) -> anyhow::Result<BossReportPayload> {
        let status = self.status.read().await.clone();
        let session = self.session.read().await.clone();
        let plan = self.plan.read().await.clone();
        let routed_step_metadata = self.routed_step_metadata.read().await.clone();
        let empty_session = BossSession::from_plan_id("unknown", status.stage);
        let session = session.unwrap_or(empty_session);
        let steps = plan
            .as_ref()
            .map(|plan| {
                plan.steps
                    .iter()
                    .map(|step| BossStepReport {
                        id: step.id,
                        status: step.status,
                        worker_task_id: step.worker_task_id.clone(),
                        attempt_count: step.attempt_count,
                        last_review_summary: step.last_review_summary.clone(),
                        action_required: if step.status == BossPlanStepStatus::ReplanRequired {
                            Some("replan_current_step".into())
                        } else {
                            None
                        },
                        blocker_reason: if step.status == BossPlanStepStatus::ReplanRequired {
                            step.last_correction.as_deref().map(|value| {
                                value
                                    .strip_prefix("replan required: ")
                                    .unwrap_or(value)
                                    .to_string()
                            })
                        } else {
                            None
                        },
                        routed_metadata: routed_step_metadata.get(&step.id).cloned(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let history_summary = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| history_summary_from_app_state(app_state))
            .unwrap_or_else(|| {
                tasks
                    .list()
                    .into_iter()
                    .filter(|task| task.boss_actor_id.is_some())
                    .map(|task| format!("{}:{:?}", task.id, task.status))
                    .collect::<Vec<_>>()
            });

        let step_metrics = status.last_step_metrics.clone();
        let observability_summary =
            Self::build_observability_summary(&steps, Some(tasks), step_metrics.as_ref());

        Ok(BossReportPayload {
            stage: status.stage,
            current_step: status.current_step,
            total_steps: status.total_steps,
            designer_a: session.designer_a,
            executor_b: session.executor_b,
            active_children: session.active_children,
            steps,
            history_summary,
            observability_summary,
            lism_policy: self.lism_policy().await,
        })
    }

    pub async fn stop(
        &self,
        tasks: &TaskManager,
        requester_session_id: &str,
        dispatcher: &NotificationDispatcher,
        deadline_ms: u64,
    ) -> anyhow::Result<BossStopOutcome> {
        let mut stages = vec![BossStopStage::CancelIssued];
        let tracked_task_ids = {
            let session = self.session.read().await;
            session
                .as_ref()
                .map(|snapshot| {
                    tasks
                        .list()
                        .into_iter()
                        .filter(|task| {
                            task.owner.session_id == requester_session_id
                                && task.boss_actor_id.is_some()
                                && (snapshot.executor_b.task_id.as_deref()
                                    == Some(task.id.as_str())
                                    || snapshot.designer_a.task_id.as_deref()
                                        == Some(task.id.as_str())
                                    || task
                                        .boss_actor_id
                                        .as_deref()
                                        .is_some_and(|actor| actor.contains("child")))
                        })
                        .map(|task| task.id)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        let mut killed = Vec::new();
        for task_id in &tracked_task_ids {
            if tasks.kill(task_id, requester_session_id, dispatcher) {
                killed.push(task_id.clone());
            }
        }

        let mut pending_after_cancel = tracked_task_ids
            .iter()
            .filter(|task_id| {
                matches!(
                    tasks.status(task_id),
                    Some(TaskStatus::Pending | TaskStatus::Running)
                )
            })
            .cloned()
            .collect::<Vec<_>>();

        if !pending_after_cancel.is_empty() {
            stages.push(BossStopStage::DeadlineExpired);
            tokio::time::sleep(tokio::time::Duration::from_millis(deadline_ms)).await;
            pending_after_cancel = tracked_task_ids
                .iter()
                .filter(|task_id| {
                    matches!(
                        tasks.status(task_id),
                        Some(TaskStatus::Pending | TaskStatus::Running)
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
        }

        let mut force_drained = false;
        for task_id in pending_after_cancel {
            force_drained = true;
            if tasks.force_kill(&task_id, dispatcher) && !killed.contains(&task_id) {
                killed.push(task_id.clone());
            }
        }
        if force_drained {
            stages.push(BossStopStage::ForceDrain);
        }

        {
            let mut session = self.session.write().await;
            if let Some(snapshot) = session.as_mut() {
                snapshot.designer_a.status = BossActorStatus::Suspended;
                snapshot.executor_b.status = BossActorStatus::Suspended;
                snapshot.active_children.clear();
            }
        }

        // Stop A and B actor runtimes via their mailboxes — await Stopped before returning.
        if let Some(registry) = self.actor_registry.read().await.as_ref() {
            let _ = registry.a_mailbox().request(DesignerACommand::Stop).await;
            let _ = registry.b_mailbox().request(ExecutorBCommand::Stop).await;
        }

        // Abort A/B LLM session tasks directly — these may not have boss_actor_id set
        // (spawned by invoke_agent_tool_with_task_id without a boss_actor_policy), so the
        // tracked_task_ids filter above may have missed them.
        self.abort_a_b_sessions(tasks, dispatcher).await;

        Ok(BossStopOutcome {
            killed_task_ids: killed,
            stages,
        })
    }

    /// Abort A's and B's LLM session tasks by their stored task_id.
    /// Uses force_kill (no session ownership check) because these tasks are owned by the
    /// coordinator's session, not the requester's session.
    async fn abort_a_b_sessions(&self, tasks: &TaskManager, dispatcher: &NotificationDispatcher) {
        let (a_task_id, b_task_id) = {
            let guard = self.session.read().await;
            let (a, b) = guard
                .as_ref()
                .map(|s| (s.designer_a.task_id.clone(), s.executor_b.task_id.clone()))
                .unwrap_or((None, None));
            (a, b)
        };
        if let Some(id) = a_task_id {
            tasks.force_kill(&id, dispatcher);
        }
        if let Some(id) = b_task_id {
            tasks.force_kill(&id, dispatcher);
        }
    }

    /// Processes a task event to update the BossPlan by structured step identity.
    pub async fn on_task_event(&self, event: &TaskEvent) -> anyhow::Result<()> {
        let tasks = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| app_state.permission_context.task_manager.clone());
        // Group fan-in: task_id starts with "group-" and orchestration_group_id is B's task id.
        // Find the step whose worker_task_id matches the group_id (B's task id).
        if event.task_id.starts_with("group-") {
            if let Some(group_id) = &event.orchestration_group_id {
                let mut plan_guard = self.plan.write().await;
                let Some(plan) = plan_guard.as_mut() else {
                    return Ok(());
                };
                let step = plan
                    .steps
                    .iter_mut()
                    .find(|s| s.worker_task_id.as_deref() == Some(group_id.as_str()));
                if let Some(step) = step {
                    let step_id = step.id;
                    match event.status {
                        TaskStatus::Completed => {
                            // Fan-in complete — enter Reviewing, not Completed.
                            // A review gate must accept before the step advances.
                            step.status = BossPlanStepStatus::Reviewing;
                            tracing::info!(
                                "BossPlan: Step {} fan-in complete, entering Reviewing",
                                step_id
                            );
                        }
                        TaskStatus::Failed | TaskStatus::Killed => {
                            step.status = BossPlanStepStatus::Failed;
                            tracing::warn!(
                                "BossPlan: Step {} fan-in failed via group {}",
                                step_id,
                                group_id
                            );
                        }
                        _ => {}
                    }
                    drop(plan_guard);
                    // No auto-advance here — A review must call on_review_event to proceed.
                    return Ok(());
                }
            }
        }

        if event.orchestration_group_id.is_some() {
            tracing::debug!(
                "BossPlan: ignoring non-group child event {} with orchestration group {:?}",
                event.task_id,
                event.orchestration_group_id
            );
            return Ok(());
        }

        let Some(step_id) = event.step_id else {
            return Ok(());
        };

        let mut plan_guard = self.plan.write().await;
        let Some(plan) = plan_guard.as_mut() else {
            return Ok(());
        };

        let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
            return Ok(());
        };

        let review_summary = match event.status {
            TaskStatus::Completed => {
                if let Some(reason) = step_artifact_verification_error(step) {
                    step.completed = false;
                    step.worker_task_id = Some(event.task_id.clone());
                    step.last_review_summary = Some(reason.clone());
                    sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                    append_artifact_verification_runtime_records(
                        step,
                        "missing_or_invalid",
                        &reason,
                    );
                    step.attempt_count += 1;
                    let repair_instruction = build_artifact_repair_instruction(step, &reason);
                    if step.attempt_count >= step.retry_budget {
                        step.status = BossPlanStepStatus::Failed;
                        step.last_correction = repair_instruction;
                    } else {
                        step.status = BossPlanStepStatus::Rejected;
                        step.last_correction = repair_instruction;
                    }
                    tracing::warn!(
                        "BossPlan: Step {} failed artifact verification: {}",
                        step_id,
                        reason
                    );
                    None
                } else {
                    step.worker_task_id = Some(event.task_id.clone());
                    sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                    store_step_result_diff(step, &event.result, Some(&event.summary));
                    if matches!(
                        step.status,
                        BossPlanStepStatus::Completed | BossPlanStepStatus::Reviewing
                    ) {
                        None
                    } else {
                        step.completed = false;
                        step.status = BossPlanStepStatus::Reviewing;
                        tracing::info!(
                            "BossPlan: Step {} completed by worker, entering Reviewing",
                            step_id
                        );
                        Some(build_step_review_summary(
                            step,
                            "Worker task",
                            &[
                                ("Worker task id", event.task_id.as_str()),
                                ("Summary", event.summary.as_str()),
                                ("Result", event.result.as_str()),
                                ("Next action", event.next_action.as_str()),
                            ],
                        ))
                    }
                }
            }
            TaskStatus::Failed | TaskStatus::Killed => {
                step.completed = false;
                step.status = BossPlanStepStatus::Failed;
                step.worker_task_id = Some(event.task_id.clone());
                sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                store_step_result_diff(step, &event.result, Some(&event.summary));
                step.last_review_summary = step_artifact_verification_error(step)
                    .map(|reason| {
                        append_artifact_verification_runtime_records(
                            step,
                            "missing_or_invalid",
                            &reason,
                        );
                        reason
                    })
                    .or_else(|| Some(event.result.clone()).filter(|text| !text.trim().is_empty()))
                    .or_else(|| Some(event.summary.clone()).filter(|text| !text.trim().is_empty()));
                tracing::warn!("BossPlan: Step {} marked as failed", step_id);
                None
            }
            TaskStatus::Running => {
                step.status = BossPlanStepStatus::Running;
                step.worker_task_id = Some(event.task_id.clone());
                sync_step_tool_execution_records(step, tasks.as_deref(), &event.task_id);
                None
            }
            TaskStatus::Pending => None,
        };

        let recovery_status =
            plan.steps
                .iter()
                .find(|s| s.id == step_id)
                .and_then(|step| match step.status {
                    BossPlanStepStatus::Rejected => Some(("repair_dispatched", None)),
                    BossPlanStepStatus::Failed if step.last_correction.is_some() => Some((
                        "terminal_after_repair_exhausted",
                        Some("artifact_verification_failed"),
                    )),
                    _ => None,
                });

        if let Some(summary) = review_summary {
            drop(plan_guard);
            self.trigger_review_for_completed_step(step_id, summary)
                .await?;
            return Ok(());
        }

        let next_step = next_unfinished_step_id(plan);
        drop(plan_guard);
        if let Some((outcome, blocker)) = recovery_status {
            self.mark_routed_metadata_artifact_recovery(step_id, outcome, blocker)
                .await;
            if outcome == "repair_dispatched" {
                self.maybe_auto_advance_after_completion().await?;
            }
        }
        self.update_current_step(next_step).await;

        Ok(())
    }

    /// Called by A's review agent when it has a verdict on a step.
    ///
    /// `accepted = true` → step moves to Completed and auto-advance fires.
    /// `accepted = false` → step moves to Rejected; if under retry budget, correction is stored
    ///   and the step is reset to Pending so the next `advance_plan` re-dispatches B.
    ///   If over budget, step is marked Failed.
    pub async fn on_review_event(
        &self,
        step_id: usize,
        accepted: bool,
        review_summary: &str,
        correction: Option<&str>,
    ) -> anyhow::Result<()> {
        self.ensure_actor_registry_with_a_callbacks_auto().await;

        let has_a_callbacks = self
            .actor_registry
            .read()
            .await
            .as_ref()
            .map(|r| r.has_a_callbacks)
            .unwrap_or(false);

        let fallback_decision = if accepted {
            crate::core::boss_actor_runtime::ReviewDecision::Accept {
                summary: review_summary.to_string(),
            }
        } else if let Some(correction_text) = correction {
            if correction_text.to_uppercase().contains("REPLAN_STEP") {
                Self::parse_a_review_decision(correction_text, review_summary)
            } else {
                crate::core::boss_actor_runtime::ReviewDecision::Correct {
                    summary: review_summary.to_string(),
                    correction: Some(correction_text.to_string()),
                }
            }
        } else {
            crate::core::boss_actor_runtime::ReviewDecision::Correct {
                summary: review_summary.to_string(),
                correction: None,
            }
        };

        let a_mailbox = if has_a_callbacks {
            let guard = self.actor_registry.read().await;
            guard.as_ref().map(|registry| registry.a_mailbox().clone())
        } else {
            None
        };
        let decision = if let Some(a_mailbox) = a_mailbox {
            match a_mailbox
                .request(DesignerACommand::Review {
                    step_id,
                    accepted,
                    summary: review_summary.to_string(),
                    correction: correction.map(str::to_string),
                })
                .await
            {
                Ok(BossActorEvent::ReviewComplete { decision, .. }) => decision,
                _ => fallback_decision,
            }
        } else {
            fallback_decision
        };

        if !has_a_callbacks {
            let designer_a_state = {
                let guard = self.actor_registry.read().await;
                guard
                    .as_ref()
                    .map(|registry| registry.designer_a.state.clone())
            };
            if let Some(a_state) = designer_a_state {
                let mut a_state = a_state.write().await;
                a_state.status = BossActorStatus::Active;
                a_state.current_step = Some(step_id);
            }
            self.apply_review_verdict(step_id, &decision).await?;
        }

        Ok(())
    }

    /// Inner side-effect for A's review callback — called from DesignerARuntime handler.
    /// Mutates plan state and fires auto-advance if accepted.
    pub(crate) async fn apply_review_verdict(
        &self,
        step_id: usize,
        decision: &crate::core::boss_actor_runtime::ReviewDecision,
    ) -> anyhow::Result<()> {
        let (should_auto_advance, artifact_recovery_status) = {
            let mut plan_guard = self.plan.write().await;
            let Some(plan) = plan_guard.as_mut() else {
                return Ok(());
            };
            let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
                return Ok(());
            };
            match decision {
                crate::core::boss_actor_runtime::ReviewDecision::Accept { summary } => {
                    if let Some(reason) = step_artifact_verification_error(step) {
                        append_review_runtime_record(step, "accepted", summary, None);
                        append_artifact_verification_runtime_records(
                            step,
                            "missing_or_invalid",
                            &reason,
                        );
                        step.last_review_summary = Some(reason);
                        step.completed = false;
                        step.attempt_count += 1;
                        let repair_instruction = build_artifact_repair_instruction(
                            step,
                            step.last_review_summary
                                .as_deref()
                                .unwrap_or("artifact verification failed"),
                        );
                        if step.attempt_count >= step.retry_budget {
                            step.status = BossPlanStepStatus::Failed;
                            step.last_correction = repair_instruction;
                            (
                                false,
                                Some((
                                    "terminal_after_repair_exhausted",
                                    Some("artifact_verification_failed"),
                                )),
                            )
                        } else {
                            step.status = BossPlanStepStatus::Rejected;
                            step.last_correction = repair_instruction;
                            (false, Some(("repair_dispatched", None)))
                        }
                    } else {
                        append_review_runtime_record(step, "accepted", summary, None);
                        append_artifact_verification_runtime_records(
                            step,
                            "verified",
                            "artifact verification passed",
                        );
                        step.last_review_summary = Some(summary.clone());
                        step.completed = true;
                        step.status = BossPlanStepStatus::Completed;
                        step.last_correction = None;
                        (true, None)
                    }
                }
                crate::core::boss_actor_runtime::ReviewDecision::Correct {
                    summary,
                    correction,
                } => {
                    append_review_runtime_record(step, "rejected", summary, correction.as_deref());
                    step.last_review_summary = Some(summary.clone());
                    step.attempt_count += 1;
                    if step.attempt_count >= step.retry_budget {
                        step.status = BossPlanStepStatus::Failed;
                    } else {
                        step.status = BossPlanStepStatus::Rejected;
                        step.last_correction = correction.clone();
                    }
                    (false, None)
                }
                crate::core::boss_actor_runtime::ReviewDecision::ReplanStep { summary, reason } => {
                    append_review_runtime_record(
                        step,
                        "replan_required",
                        summary,
                        Some(reason.as_str()),
                    );
                    step.last_review_summary = Some(summary.clone());
                    step.status = BossPlanStepStatus::ReplanRequired;
                    step.last_correction = Some(format!("replan required: {reason}"));
                    (false, None)
                }
            }
        };
        self.persist_plan_if_configured().await?;
        if let Some((outcome, blocker)) = artifact_recovery_status {
            self.mark_routed_metadata_artifact_recovery(step_id, outcome, blocker)
                .await;
        }
        if should_auto_advance {
            let next_step = self
                .plan
                .read()
                .await
                .as_ref()
                .and_then(|p| next_unfinished_step_id(p));
            self.update_current_step(next_step).await;
            self.maybe_auto_advance_after_completion().await?;
        } else if matches!(artifact_recovery_status, Some(("repair_dispatched", None))) {
            self.maybe_auto_advance_after_completion().await?;
        }
        Ok(())
    }

    /// Inner side-effect for A's documentation callback — called from DesignerARuntime handler.
    /// Signal is the raw user input for approval, or "finalize" for documentation loop completion.
    pub(crate) async fn apply_documentation_signal(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        signal: &str,
    ) -> anyhow::Result<()> {
        // "finalize" signal: transition to WaitingForApproval (plan already mutated by caller).
        // Approval signal: "Y"/empty → Execution; anything else → Documentation feedback.
        let stage = self.status.read().await.stage;
        if signal == "finalize" {
            self.transition_to(BossStage::WaitingForApproval).await?;
        } else if stage == BossStage::WaitingForApproval {
            if signal.trim().to_uppercase() == "Y" || signal.trim().is_empty() {
                self.transition_to(BossStage::Execution).await?;
                let _ = self.advance_plan(app_state).await;
            } else {
                self.record_documentation_feedback(signal).await?;
            }
        }
        Ok(())
    }

    /// Simplified entry point for dispatcher updates.
    pub async fn on_notification(
        &self,
        notification: &crate::interaction::notification::Notification,
    ) -> anyhow::Result<()> {
        if notification.notification_type
            != crate::interaction::notification::NotificationType::TaskUpdate
        {
            return Ok(());
        }

        let step_id = match notification.step_id {
            Some(id) => id,
            None => return Ok(()),
        };
        let tasks = self
            .auto_advance_app_state
            .read()
            .await
            .as_ref()
            .and_then(|app_state| app_state.permission_context.task_manager.clone());

        let mut plan_guard = self.plan.write().await;
        let Some(plan) = plan_guard.as_mut() else {
            return Ok(());
        };

        let Some(step) = plan.steps.iter_mut().find(|s| s.id == step_id) else {
            return Ok(());
        };

        let review_summary = match notification.status.as_deref().unwrap_or_default() {
            status if status.eq_ignore_ascii_case("completed") => {
                if let Some(reason) = step_artifact_verification_error(step) {
                    step.completed = false;
                    step.worker_task_id = notification.task_id.clone();
                    step.last_review_summary = Some(reason.clone());
                    if let Some(task_id) = notification.task_id.as_deref() {
                        sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                    }
                    append_artifact_verification_runtime_records(
                        step,
                        "missing_or_invalid",
                        &reason,
                    );
                    step.attempt_count += 1;
                    let repair_instruction = build_artifact_repair_instruction(step, &reason);
                    if step.attempt_count >= step.retry_budget {
                        step.status = BossPlanStepStatus::Failed;
                        step.last_correction = repair_instruction;
                    } else {
                        step.status = BossPlanStepStatus::Rejected;
                        step.last_correction = repair_instruction;
                    }
                    tracing::warn!(
                        "BossPlan: Step {} failed artifact verification via notification: {}",
                        step_id,
                        reason
                    );
                    None
                } else {
                    step.worker_task_id = notification.task_id.clone();
                    if let Some(task_id) = notification.task_id.as_deref() {
                        sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                    }
                    store_step_result_diff(
                        step,
                        notification.output_file.as_deref().unwrap_or_default(),
                        Some(notification.body.as_str()),
                    );
                    if matches!(
                        step.status,
                        BossPlanStepStatus::Completed | BossPlanStepStatus::Reviewing
                    ) {
                        None
                    } else {
                        step.completed = false;
                        step.status = BossPlanStepStatus::Reviewing;
                        tracing::info!(
                            "BossPlan: Step {} completed via notification, entering Reviewing",
                            step_id
                        );
                        Some(build_step_review_summary(
                            step,
                            "Worker notification",
                            &[
                                (
                                    "Worker task id",
                                    notification.task_id.as_deref().unwrap_or(""),
                                ),
                                ("Title", notification.title.as_str()),
                                ("Body", notification.body.as_str()),
                                ("Status", notification.status.as_deref().unwrap_or_default()),
                                (
                                    "Next action",
                                    notification.next_action.as_deref().unwrap_or_default(),
                                ),
                                (
                                    "Output file",
                                    notification.output_file.as_deref().unwrap_or_default(),
                                ),
                            ],
                        ))
                    }
                }
            }
            status
                if status.eq_ignore_ascii_case("failed")
                    || status.eq_ignore_ascii_case("killed") =>
            {
                step.completed = false;
                step.status = BossPlanStepStatus::Failed;
                step.worker_task_id = notification.task_id.clone();
                if let Some(task_id) = notification.task_id.as_deref() {
                    sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                }
                store_step_result_diff(
                    step,
                    notification.output_file.as_deref().unwrap_or_default(),
                    Some(notification.body.as_str()),
                );
                step.last_review_summary = step_artifact_verification_error(step)
                    .map(|reason| {
                        append_artifact_verification_runtime_records(
                            step,
                            "missing_or_invalid",
                            &reason,
                        );
                        reason
                    })
                    .or_else(|| {
                        notification
                            .body
                            .split("Result: ")
                            .nth(1)
                            .map(str::trim)
                            .map(str::to_string)
                            .filter(|text| !text.is_empty())
                    })
                    .or_else(|| {
                        notification
                            .next_action
                            .clone()
                            .filter(|text| !text.trim().is_empty())
                    });
                tracing::warn!(
                    "BossPlan: Step {} marked as failed via notification",
                    step_id
                );
                None
            }
            status if status.eq_ignore_ascii_case("running") => {
                step.status = BossPlanStepStatus::Running;
                step.worker_task_id = notification.task_id.clone();
                if let Some(task_id) = notification.task_id.as_deref() {
                    sync_step_tool_execution_records(step, tasks.as_deref(), task_id);
                }
                None
            }
            _ => None,
        };

        let recovery_status =
            plan.steps
                .iter()
                .find(|s| s.id == step_id)
                .and_then(|step| match step.status {
                    BossPlanStepStatus::Rejected => Some(("repair_dispatched", None)),
                    BossPlanStepStatus::Failed if step.last_correction.is_some() => Some((
                        "terminal_after_repair_exhausted",
                        Some("artifact_verification_failed"),
                    )),
                    _ => None,
                });

        if let Some(summary) = review_summary {
            drop(plan_guard);
            self.trigger_review_for_completed_step(step_id, summary)
                .await?;
            return Ok(());
        }

        let next_step = next_unfinished_step_id(plan);
        drop(plan_guard);
        if let Some((outcome, blocker)) = recovery_status {
            self.mark_routed_metadata_artifact_recovery(step_id, outcome, blocker)
                .await;
            if outcome == "repair_dispatched" {
                self.maybe_auto_advance_after_completion().await?;
            }
        }
        self.update_current_step(next_step).await;

        Ok(())
    }

    async fn maybe_auto_advance_after_completion(&self) -> anyhow::Result<()> {
        let app_state = {
            let guard = self.auto_advance_app_state.read().await;
            guard.clone()
        };
        let Some(app_state) = app_state else {
            return Ok(());
        };
        let _ = self.advance_plan(&app_state).await?;
        Ok(())
    }

    async fn advance_once(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Option<String>> {
        let parent_session_id = app_state.active_session_id.clone();
        let next_action = {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;

            if !plan.auto_sequence {
                return Ok(None);
            }

            if plan.steps.iter().all(|step| step.completed) {
                Some(AdvanceOutcome::PlanComplete)
            } else if let Some(step) = plan
                .steps
                .iter()
                .find(|step| step.status.is_terminal_failure())
            {
                Some(AdvanceOutcome::TerminalFailure(
                    step.last_review_summary
                        .clone()
                        .unwrap_or_else(|| format!("step {} failed", step.id)),
                ))
            } else if plan
                .steps
                .iter()
                .any(|step| step.status == BossPlanStepStatus::Running)
            {
                None
            } else if let Some(step_id) = next_runnable_step(plan).map(|step| step.id) {
                let step = plan
                    .steps
                    .iter_mut()
                    .find(|step| step.id == step_id)
                    .expect("runnable step must exist");
                if step.requires_approval {
                    Some(AdvanceOutcome::ApprovalBarrier(step.id))
                } else {
                    step.status = BossPlanStepStatus::Running;
                    Some(AdvanceOutcome::Dispatch(step_id))
                }
            } else if let Some(step) = plan
                .steps
                .iter()
                .find(|step| step.status == BossPlanStepStatus::ReplanRequired)
            {
                Some(AdvanceOutcome::ReplanRequired(
                    step.id,
                    step.last_correction
                        .as_deref()
                        .map(|value| {
                            value
                                .strip_prefix("replan required: ")
                                .unwrap_or(value)
                                .to_string()
                        })
                        .unwrap_or_else(|| "current step requires replanning".to_string()),
                ))
            } else {
                Some(AdvanceOutcome::NoRunnableStep)
            }
        };

        match next_action {
            Some(AdvanceOutcome::PlanComplete) => {
                self.update_current_step(None).await;
                if self.get_stage().await != BossStage::Completed {
                    self.transition_to(BossStage::Completed).await?;
                }
                let run_id = self.current_run_id().await;
                let lism_enabled = effective_lism_enabled(
                    self.lism_policy().await,
                    app_state.permission_context.lism_enabled(),
                );
                self.emit_lism_sample(&run_id, lism_enabled, BossTestRunOutcome::Completed, 0)
                    .await;
                Ok(Some(
                    "Boss plan complete; no further steps to dispatch.".into(),
                ))
            }
            Some(AdvanceOutcome::TerminalFailure(reason)) => {
                self.update_current_step(None).await;
                if self.get_stage().await != BossStage::Documentation {
                    self.transition_to(BossStage::Documentation).await?;
                }
                let run_id = self.current_run_id().await;
                let lism_enabled = effective_lism_enabled(
                    self.lism_policy().await,
                    app_state.permission_context.lism_enabled(),
                );
                self.emit_lism_sample(&run_id, lism_enabled, BossTestRunOutcome::Aborted, 0)
                    .await;
                Ok(Some(format!(
                    "Boss plan stopped after a terminal step failure; auto-advance halted. Reason: {}",
                    reason
                )))
            }
            Some(AdvanceOutcome::ApprovalBarrier(step_id)) => {
                self.update_step_status(step_id, BossPlanStepStatus::WaitingForApproval)
                    .await?;
                self.update_current_step(Some(step_id)).await;
                Ok(Some(format!(
                    "Boss plan paused before step {} because it requires approval.",
                    step_id
                )))
            }
            Some(AdvanceOutcome::Dispatch(step_id)) => {
                self.update_current_step(Some(step_id)).await;

                let lism_enabled = effective_lism_enabled(
                    self.lism_policy().await,
                    app_state.permission_context.lism_enabled(),
                );

                if lism_enabled {
                    let full_worker_dispatch_fallback_enabled =
                        self.full_worker_dispatch_fallback_enabled().await;
                    let routed_preview = {
                        let plan_guard = self.plan.read().await;
                        let plan = plan_guard
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                        build_routed_state_frame_with_model_route(
                            plan,
                            BossStage::Execution,
                            step_id,
                            ActorRole::Worker,
                        )
                    };
                    if full_worker_dispatch_fallback_enabled
                        && requires_external_tool_execution(
                            &routed_preview.frame,
                            app_state.runtime_tool_registry.is_some(),
                        )
                    {
                        let state_frame_size = serde_json::to_string(&routed_preview.frame)
                            .map(|s| s.len())
                            .ok();
                        let routed_metadata = BossStepRoutedMetadata {
                            toolset_id: routed_preview.frame.toolset_id.clone(),
                            skillset_id: routed_preview.frame.skillset_id.clone(),
                            model_tier: Some(
                                model_tier_label(routed_preview.model_route.tier).to_string(),
                            ),
                            provider_profile_id: routed_preview.model_route.provider_profile_id,
                            state_frame_size,
                            cache_read_tokens: Some(0),
                            cache_write_tokens: Some(0),
                            fallback_count: Some(1),
                            fallback_tier: Some("full_worker_dispatch".into()),
                            fallback_reason: Some("external_tool_execution_required".into()),
                            projection_mismatch_count: Some(
                                routed_preview.projection_mismatch_count,
                            ),
                            hydration_count: Some(0),
                            stale_ref_count: Some(0),
                            hydration_ref_missing: Some(0),
                            tool_dispatch_count: Some(0),
                            tool_dispatch_success_count: Some(0),
                            tool_dispatch_failure_count: Some(0),
                            tool_dispatch_ref_write_count: Some(0),
                            tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                            input_tokens: Some(0),
                            uncached_input_tokens: Some(0),
                            output_tokens: Some(0),
                            original_prompt_chars: Some(0),
                            sent_prompt_chars: Some(0),
                            estimated_cost_micros_usd: Some(0),
                            visible_tools: Vec::new(),
                            allowed_actions: Vec::new(),
                            schema_hash: None,
                            permission_hash: None,
                            actor_role: None,
                            cwd: None,
                            config_root: None,
                            workspace_capabilities: Vec::new(),
                            tool_contract_mismatch_count: Some(0),
                            tool_contract_mismatch: None,
                            last_effective_tool_action: None,
                            last_failure_kind: None,
                            last_failure_recoverable: None,
                            last_recommended_repair: None,
                            last_failure_evidence_ref: None,
                            last_failure_bounded_excerpt: None,
                            last_failure_truncated: None,
                            recovery_attempted: None,
                            recovery_tier: None,
                            recovery_outcome: None,
                            terminal_blocker_kind: None,
                            completion_evidence_status: None,
                            completion_evidence_gaps: Vec::new(),
                            worker_report: None,
                        };
                        let mut routed_step_metadata = self.routed_step_metadata.write().await;
                        routed_step_metadata.insert(step_id, routed_metadata);
                    } else if app_state
                        .permission_context
                        .inherited_active_model_snapshot
                        .is_none()
                    {
                        let state_frame_size = serde_json::to_string(&routed_preview.frame)
                            .map(|s| s.len())
                            .ok();
                        let routed_metadata = BossStepRoutedMetadata {
                            toolset_id: routed_preview.frame.toolset_id.clone(),
                            skillset_id: routed_preview.frame.skillset_id.clone(),
                            model_tier: Some(
                                model_tier_label(routed_preview.model_route.tier).to_string(),
                            ),
                            provider_profile_id: routed_preview.model_route.provider_profile_id,
                            state_frame_size,
                            cache_read_tokens: Some(0),
                            cache_write_tokens: Some(0),
                            fallback_count: Some(1),
                            fallback_tier: Some("full_worker_dispatch".into()),
                            fallback_reason: Some("missing_active_model_snapshot".into()),
                            projection_mismatch_count: Some(
                                routed_preview.projection_mismatch_count,
                            ),
                            hydration_count: Some(0),
                            stale_ref_count: Some(0),
                            hydration_ref_missing: Some(0),
                            tool_dispatch_count: Some(0),
                            tool_dispatch_success_count: Some(0),
                            tool_dispatch_failure_count: Some(0),
                            tool_dispatch_ref_write_count: Some(0),
                            tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                            input_tokens: Some(0),
                            uncached_input_tokens: Some(0),
                            output_tokens: Some(0),
                            original_prompt_chars: Some(0),
                            sent_prompt_chars: Some(0),
                            estimated_cost_micros_usd: Some(0),
                            visible_tools: Vec::new(),
                            allowed_actions: Vec::new(),
                            schema_hash: None,
                            permission_hash: None,
                            actor_role: None,
                            cwd: None,
                            config_root: None,
                            workspace_capabilities: Vec::new(),
                            tool_contract_mismatch_count: Some(0),
                            tool_contract_mismatch: None,
                            last_effective_tool_action: None,
                            last_failure_kind: None,
                            last_failure_recoverable: None,
                            last_recommended_repair: None,
                            last_failure_evidence_ref: None,
                            last_failure_bounded_excerpt: None,
                            last_failure_truncated: None,
                            recovery_attempted: None,
                            recovery_tier: None,
                            recovery_outcome: None,
                            terminal_blocker_kind: None,
                            completion_evidence_status: None,
                            completion_evidence_gaps: Vec::new(),
                            worker_report: None,
                        };
                        let mut routed_step_metadata = self.routed_step_metadata.write().await;
                        routed_step_metadata.insert(step_id, routed_metadata);
                    } else {
                        let (outcome, routed_metadata) = {
                            let plan_guard = self.plan.read().await;
                            let plan = plan_guard
                                .as_ref()
                                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                            let inherited_snapshot = app_state
                                .permission_context
                                .inherited_active_model_snapshot
                                .as_ref()
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "LisM boss path requires an active model snapshot"
                                    )
                                })?;
                            let routed = build_routed_state_frame_with_model_route(
                                plan,
                                BossStage::Execution,
                                step_id,
                                ActorRole::Worker,
                            );
                            let state_frame_size =
                                serde_json::to_string(&routed.frame).map(|s| s.len()).ok();
                            let mut routed_metadata = BossStepRoutedMetadata {
                                toolset_id: routed.frame.toolset_id.clone(),
                                skillset_id: routed.frame.skillset_id.clone(),
                                model_tier: Some(
                                    model_tier_label(routed.model_route.tier).to_string(),
                                ),
                                provider_profile_id: routed.model_route.provider_profile_id.clone(),
                                state_frame_size,
                                cache_read_tokens: Some(0),
                                cache_write_tokens: Some(0),
                                fallback_count: Some(0),
                                fallback_tier: None,
                                fallback_reason: None,
                                projection_mismatch_count: Some(routed.projection_mismatch_count),
                                hydration_count: Some(0),
                                stale_ref_count: Some(0),
                                hydration_ref_missing: Some(0),
                                tool_dispatch_count: Some(0),
                                tool_dispatch_success_count: Some(0),
                                tool_dispatch_failure_count: Some(0),
                                tool_dispatch_ref_write_count: Some(0),
                                tool_dispatch_failure_taxonomy: std::collections::BTreeMap::new(),
                                input_tokens: Some(0),
                                uncached_input_tokens: Some(0),
                                output_tokens: Some(0),
                                original_prompt_chars: Some(0),
                                sent_prompt_chars: Some(0),
                                estimated_cost_micros_usd: Some(0),
                                visible_tools: Vec::new(),
                                allowed_actions: Vec::new(),
                                schema_hash: None,
                                permission_hash: None,
                                actor_role: None,
                                cwd: None,
                                config_root: None,
                                workspace_capabilities: Vec::new(),
                                tool_contract_mismatch_count: Some(0),
                                tool_contract_mismatch: None,
                                last_effective_tool_action: None,
                                last_failure_kind: None,
                                last_failure_recoverable: None,
                                last_recommended_repair: None,
                                last_failure_evidence_ref: None,
                                last_failure_bounded_excerpt: None,
                                last_failure_truncated: None,
                                recovery_attempted: None,
                                recovery_tier: None,
                                recovery_outcome: None,
                                terminal_blocker_kind: None,
                                completion_evidence_status: None,
                                completion_evidence_gaps: Vec::new(),
                                worker_report: None,
                            };
                            let cwd = app_state
                                .session
                                .as_ref()
                                .map(|s| std::path::Path::new(s.cwd.as_str()).to_path_buf())
                                .unwrap_or_else(|| std::path::PathBuf::from("."));
                            let model_registry = resolve_config_root(&cwd).ok().and_then(|root| {
                                load_model_profiles_registry_from_root(&root).ok().flatten()
                            });
                            let tool_runtime = match &app_state.runtime_tool_registry {
                                Some(registry) => {
                                    let mut permissions = app_state.permission_context.clone();
                                    permissions = permissions.with_interactive_tools(true);
                                    Some(StateFrameToolRuntime {
                                        registry: registry.read().await.clone(),
                                        permissions,
                                        cwd: cwd.clone(),
                                        config_root: resolve_config_root(&cwd).ok(),
                                    })
                                }
                                None => None,
                            };
                            let runtime = StepRuntimeResolutionContext {
                                inherited_snapshot,
                                model_registry: model_registry.as_ref(),
                                observability: app_state.service_observability_tracker.clone(),
                                tool_runtime,
                            };
                            let outcome = run_routed_step_with_runtime(
                                routed,
                                DecisionLoopConfig::default(),
                                runtime,
                            )
                            .await?;
                            if let Some(usage) = match &outcome {
                                StepOutcome::Completed { usage, .. } => Some(usage),
                                StepOutcome::Failed {
                                    usage: Some(usage), ..
                                } => Some(usage),
                                StepOutcome::Failed { usage: None, .. } => None,
                            } {
                                Self::apply_loop_usage_to_routed_metadata(
                                    &mut routed_metadata,
                                    usage,
                                );
                            }
                            match &outcome {
                                StepOutcome::Completed {
                                    tool_registry_snapshot: Some(snapshot),
                                    ..
                                }
                                | StepOutcome::Failed {
                                    tool_registry_snapshot: Some(snapshot),
                                    ..
                                } => {
                                    routed_metadata.visible_tools = snapshot.visible_tools.clone();
                                    routed_metadata.allowed_actions =
                                        snapshot.allowed_actions.clone();
                                    routed_metadata.schema_hash =
                                        Some(snapshot.schema_hash.clone());
                                    routed_metadata.permission_hash =
                                        Some(snapshot.permission_hash.clone());
                                    routed_metadata.actor_role = Some(snapshot.actor_role.clone());
                                    routed_metadata.cwd = Some(snapshot.cwd.display().to_string());
                                    routed_metadata.config_root = snapshot
                                        .config_root
                                        .as_ref()
                                        .map(|path| path.display().to_string());
                                    routed_metadata.workspace_capabilities =
                                        snapshot.workspace_capabilities.clone();
                                }
                                _ => {}
                            }
                            if let StepOutcome::Failed {
                                tool_contract_mismatch,
                                ..
                            } = &outcome
                            {
                                routed_metadata.tool_contract_mismatch_count =
                                    Some(tool_contract_mismatch.iter().count());
                                routed_metadata.tool_contract_mismatch =
                                    tool_contract_mismatch.clone();
                            }
                            (outcome, routed_metadata)
                        };
                        {
                            let mut routed_step_metadata = self.routed_step_metadata.write().await;
                            routed_step_metadata.insert(step_id, routed_metadata);
                        }
                        if let Some(usage) = match &outcome {
                            StepOutcome::Completed { usage, .. } => Some(usage),
                            StepOutcome::Failed {
                                usage: Some(usage), ..
                            } => Some(usage),
                            StepOutcome::Failed { usage: None, .. } => None,
                        } {
                            if !usage.tool_execution_records.is_empty() {
                                let mut plan_guard = self.plan.write().await;
                                let plan = plan_guard
                                    .as_mut()
                                    .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                let step = plan
                                    .steps
                                    .iter_mut()
                                    .find(|step| step.id == step_id)
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("Unknown boss step {step_id}")
                                    })?;
                                for record in &usage.tool_execution_records {
                                    append_step_runtime_record(step, record.clone());
                                }
                            }
                        }

                        match outcome {
                            StepOutcome::Completed { .. } => {
                                {
                                    let mut plan_guard = self.plan.write().await;
                                    let plan = plan_guard
                                        .as_mut()
                                        .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                    let step = plan
                                        .steps
                                        .iter_mut()
                                        .find(|step| step.id == step_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!("Unknown boss step {step_id}")
                                        })?;
                                    if let Some(reason) = step_artifact_verification_error(step) {
                                        step.completed = false;
                                        step.status = BossPlanStepStatus::Failed;
                                        step.last_review_summary = Some(reason.clone());
                                        drop(plan_guard);
                                        self.update_current_step(Some(step_id)).await;
                                        if self.get_stage().await != BossStage::Documentation {
                                            self.transition_to(BossStage::Documentation).await?;
                                        }
                                        let run_id = self.current_run_id().await;
                                        let lism_enabled = effective_lism_enabled(
                                            self.lism_policy().await,
                                            app_state.permission_context.lism_enabled(),
                                        );
                                        self.emit_lism_sample(
                                            &run_id,
                                            lism_enabled,
                                            BossTestRunOutcome::Aborted,
                                            0,
                                        )
                                        .await;
                                        return Ok(Some(format!(
                                            "LisM failed boss step {}: {}",
                                            step_id, reason
                                        )));
                                    }
                                    step.completed = true;
                                    step.status = BossPlanStepStatus::Completed;
                                }
                                if let Some(path) = self.status.read().await.planning_file.clone() {
                                    self.save_plan_with_session(std::path::Path::new(&path))
                                        .await?;
                                }
                                let next_step = self
                                    .plan
                                    .read()
                                    .await
                                    .as_ref()
                                    .and_then(|p| next_unfinished_step_id(p));
                                self.update_current_step(next_step).await;
                                if next_step.is_none() {
                                    if self.get_stage().await != BossStage::Completed {
                                        self.transition_to(BossStage::Completed).await?;
                                    }
                                    let run_id = self.current_run_id().await;
                                    let lism_enabled = effective_lism_enabled(
                                        self.lism_policy().await,
                                        app_state.permission_context.lism_enabled(),
                                    );
                                    self.emit_lism_sample(
                                        &run_id,
                                        lism_enabled,
                                        BossTestRunOutcome::Completed,
                                        0,
                                    )
                                    .await;
                                }
                                return Ok(Some(format!(
                                    "LisM executed boss step {} to completion.",
                                    step_id
                                )));
                            }
                            StepOutcome::Failed { reason, .. } => {
                                let reason_clone = reason.clone();
                                {
                                    let mut plan_guard = self.plan.write().await;
                                    let plan = plan_guard
                                        .as_mut()
                                        .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
                                    let step = plan
                                        .steps
                                        .iter_mut()
                                        .find(|step| step.id == step_id)
                                        .ok_or_else(|| {
                                            anyhow::anyhow!("Unknown boss step {step_id}")
                                        })?;
                                    step.completed = false;
                                    step.status = BossPlanStepStatus::Failed;
                                    step.last_review_summary = Some(reason_clone.clone());
                                }
                                self.update_current_step(Some(step_id)).await;
                                if self.get_stage().await != BossStage::Documentation {
                                    self.transition_to(BossStage::Documentation).await?;
                                }
                                if let Some(path) = self.status.read().await.planning_file.clone() {
                                    self.save_plan_with_session(std::path::Path::new(&path))
                                        .await?;
                                }
                                let run_id = self.current_run_id().await;
                                let lism_enabled = effective_lism_enabled(
                                    self.lism_policy().await,
                                    app_state.permission_context.lism_enabled(),
                                );
                                self.emit_lism_sample(
                                    &run_id,
                                    lism_enabled,
                                    BossTestRunOutcome::Aborted,
                                    0,
                                )
                                .await;
                                return Ok(Some(format!(
                                    "LisM failed boss step {}: {}",
                                    step_id, reason
                                )));
                            }
                        }
                    }
                }

                let tasks = app_state
                    .permission_context
                    .task_manager
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;

                let running_b = {
                    let guard = self.session.read().await;
                    guard
                        .as_ref()
                        .and_then(|s| self.find_running_b_task_id(s, tasks))
                };

                let payload = if let Some(b_task_id) = running_b {
                    let continue_build = self
                        .build_step_continue_payload_internal(
                            step_id,
                            &b_task_id,
                            &parent_session_id,
                        )
                        .await?;
                    self.record_b_assignment_contract(
                        &continue_build.assignment_fingerprint,
                        &continue_build.plan_version,
                        &continue_build.step_revision,
                    )
                    .await;
                    let continue_payload = continue_build.payload;

                    self.bootstrap_actor_registry_with_app_state(app_state)
                        .await;
                    if let Some(registry) = self.actor_registry.read().await.as_ref() {
                        let _ = registry
                            .b_mailbox()
                            .request(ExecutorBCommand::ContinueStep {
                                step_id,
                                task_id: b_task_id.clone(),
                                payload: continue_payload.clone(),
                            })
                            .await;
                    }

                    continue_payload
                } else {
                    let b_actor_id = {
                        let guard = self.session.read().await;
                        guard
                            .as_ref()
                            .map(|s| s.executor_b.actor_id.clone())
                            .unwrap_or_else(|| "boss-unknown-b".into())
                    };
                    let spawn_build = self
                        .build_step_spawn_payload_internal(step_id, &parent_session_id, &b_actor_id)
                        .await?;
                    self.record_b_assignment_contract(
                        &spawn_build.assignment_fingerprint,
                        &spawn_build.plan_version,
                        &spawn_build.step_revision,
                    )
                    .await;
                    let spawn_payload = spawn_build.payload;

                    self.bootstrap_actor_registry_with_app_state(app_state)
                        .await;
                    if let Some(registry) = self.actor_registry.read().await.as_ref() {
                        let _ = registry
                            .b_mailbox()
                            .request(ExecutorBCommand::DispatchStep {
                                step_id,
                                payload: spawn_payload.clone(),
                            })
                            .await;
                    }

                    let records = tasks.list();
                    if let Some(task) = records.last() {
                        let mut guard = self.session.write().await;
                        if let Some(session) = guard.as_mut() {
                            session.executor_b.task_id = Some(task.id.clone());
                            session.executor_b.status = BossActorStatus::Active;
                        }
                    }

                    spawn_payload
                };

                Ok(Some(payload))
            }
            Some(AdvanceOutcome::ReplanRequired(step_id, reason)) => {
                self.update_current_step(Some(step_id)).await;
                Ok(Some(format!(
                    "Boss step {} requires replanning before execution can continue. Reason: {}",
                    step_id, reason
                )))
            }
            Some(AdvanceOutcome::NoRunnableStep) | None => Ok(None),
        }
    }

    /// Advances the plan by selecting the next deterministic action.
    pub async fn advance_plan(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<Option<String>> {
        {
            let mut auto_advance_app_state = self.auto_advance_app_state.write().await;
            if auto_advance_app_state.is_none() {
                *auto_advance_app_state = Some(app_state.clone());
            }
        }

        let mut latest_message = None;
        loop {
            let message = self.advance_once(app_state).await?;
            let should_continue = {
                let guard = self.plan.read().await;
                guard.as_ref().is_some_and(|plan| {
                    plan.auto_sequence
                        && plan.steps.iter().any(|step| step.completed)
                        && !plan.steps.iter().all(|step| step.completed)
                        && !plan
                            .steps
                            .iter()
                            .any(|step| step.status.is_terminal_failure())
                        && !plan
                            .steps
                            .iter()
                            .any(|step| step.status == BossPlanStepStatus::Running)
                        && next_runnable_step(plan).is_some()
                })
            };
            latest_message = message;
            if !should_continue {
                break;
            }
        }
        Ok(latest_message)
    }

    /// Returns the running ExecutorB task id if one exists in the task manager.
    /// Returns None if B has no live task (caller must spawn fresh).
    fn find_running_b_task_id(
        &self,
        session: &crate::core::boss_state::BossSession,
        tasks: &crate::task::manager::TaskManager,
    ) -> Option<String> {
        let task_id = session.executor_b.task_id.as_ref()?;
        let record = tasks.get(task_id)?;
        if matches!(
            record.status,
            crate::task::types::TaskStatus::Running | crate::task::types::TaskStatus::Pending
        ) {
            Some(task_id.clone())
        } else {
            None
        }
    }

    async fn build_executor_b_assignment_contract(
        &self,
        step_id: usize,
        parent_session_id: &str,
        include_local_continuity: bool,
    ) -> anyhow::Result<ExecutorBAssignmentContract> {
        let plan_guard = self.plan.read().await;
        let plan = plan_guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter()
            .find(|step| step.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;
        let plan_version = format!("{}:steps={}", plan.plan_id, plan.steps.len());
        let step_revision = format!("step-{}-attempt-{}", step.id, step.attempt_count);
        let relevant_file_handles = extract_relevant_file_handles(step.objective(), &step_revision);
        let target_files = collect_target_files(&relevant_file_handles);
        let target_artifacts = collect_target_artifacts(step, &target_files);
        let recent_decisions = collect_recent_decisions(plan, step.id);
        let open_items = if step.status == BossPlanStepStatus::Completed {
            Vec::new()
        } else {
            step.acceptance.clone()
        };
        let blocked_items = collect_blocked_items(step);
        let recent_local_facts = if include_local_continuity {
            collect_recent_local_facts(step, 5)
        } else {
            Vec::new()
        };
        let allowed_tools = default_allowed_tools();
        let lism_policy = self.worker_lism_policy().await.as_str().to_string();
        let generated_at = format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        );
        let permission_scope = PermissionScopeView {
            lism_policy: lism_policy.clone(),
            inherit_context: false,
            workspace_capability: render_workspace_capability_scope(),
            boss_actor_role: "executor_b".to_string(),
        };
        let brief = BossContextBrief {
            plan_id: plan.plan_id.clone(),
            step_id: step.id,
            plan_version: plan_version.clone(),
            step_revision: step_revision.clone(),
            generated_at,
            objective: step.objective().to_string(),
            acceptance: step.acceptance.clone(),
            last_correction: step.last_correction.clone(),
            recent_decisions: recent_decisions.clone(),
            relevant_file_handles: relevant_file_handles.clone(),
            target_files: target_files.clone(),
            target_artifacts: target_artifacts.clone(),
            allowed_tools: allowed_tools.clone(),
            permission_scope: permission_scope.clone(),
            parent_session_id: parent_session_id.to_string(),
            context_strategy: BossContextStrategy::Brief,
        };
        let state_frame = BossStateFrame {
            step_id: step.id,
            status: step.status,
            open_items,
            blocked_items,
            recent_local_facts,
            allowed_actions: vec!["implement".into()],
            required_output_hint: Some("return a unified diff or file edits".into()),
        };
        let assignment_fingerprint = assignment_fingerprint(&json!({
            "plan_id": plan.plan_id,
            "plan_version": plan_version,
            "plan_shape": plan.steps.iter().map(|s| json!({
                "id": s.id,
                "objective": s.objective(),
                "acceptance": s.acceptance,
                "status": format!("{:?}", s.status),
            })).collect::<Vec<_>>(),
            "step_id": step.id,
            "step_revision": step_revision,
            "objective": step.objective(),
            "acceptance": step.acceptance,
            "last_correction": step.last_correction,
            "recent_decisions": recent_decisions,
            "relevant_file_handles": relevant_file_handles,
            "target_files": target_files,
            "target_artifacts": target_artifacts,
            "allowed_tools": allowed_tools,
            "permission_scope": {
                "lism_policy": permission_scope.lism_policy,
                "inherit_context": permission_scope.inherit_context,
                "workspace_capability": permission_scope.workspace_capability,
                "boss_actor_role": permission_scope.boss_actor_role,
            },
            "parent_session_id": parent_session_id,
        }));

        Ok(ExecutorBAssignmentContract {
            brief,
            state_frame,
            allowed_tools,
            lism_policy,
            assignment_fingerprint,
        })
    }

    async fn record_b_assignment_contract(
        &self,
        assignment_fingerprint: &str,
        plan_version: &str,
        step_revision: &str,
    ) {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_mut() {
            session.executor_b.last_assignment_fingerprint =
                Some(assignment_fingerprint.to_string());
            session.executor_b.last_assignment_plan_version = Some(plan_version.to_string());
            session.executor_b.last_assignment_step_revision = Some(step_revision.to_string());
        }
    }

    async fn build_step_continue_payload_internal(
        &self,
        step_id: usize,
        b_task_id: &str,
        parent_session_id: &str,
    ) -> anyhow::Result<ContinuePayloadBuild> {
        let contract = self
            .build_executor_b_assignment_contract(step_id, parent_session_id, true)
            .await?;
        let prior_assignment = {
            let guard = self.session.read().await;
            guard.as_ref().map(|session| {
                (
                    session.executor_b.last_assignment_fingerprint.clone(),
                    session.executor_b.last_assignment_plan_version.clone(),
                    session.executor_b.last_assignment_step_revision.clone(),
                )
            })
        };
        let needs_refresh = prior_assignment
            .as_ref()
            .map(|(fingerprint, _, _)| {
                fingerprint.as_deref() != Some(contract.assignment_fingerprint.as_str())
            })
            .unwrap_or(true);
        let refresh_reason = if !needs_refresh {
            None
        } else {
            let (prior_plan_version, prior_step_revision) = prior_assignment
                .as_ref()
                .map(|(_, plan_version, step_revision)| {
                    (
                        plan_version.as_deref().unwrap_or("unknown"),
                        step_revision.as_deref().unwrap_or("unknown"),
                    )
                })
                .unwrap_or(("none", "none"));
            Some(format!(
                "stale brief detected: prior plan_version={prior_plan_version} prior step_revision={prior_step_revision}"
            ))
        };
        let message = if needs_refresh {
            format!(
                "Boss assignment refresh for step {step_id}\n\
IMPORTANT: discard any previous brief for this executor session and replace it with the refreshed brief below.\n\
refresh_reason: {}\n\n{}",
                refresh_reason
                    .clone()
                    .unwrap_or_else(|| "assignment contract changed".into()),
                assemble_brief_prompt(&contract.brief, &contract.state_frame),
            )
        } else {
            format!(
                "Boss step {step_id}\nplan_id: {}\nobjective: {}\nacceptance:\n{}{}",
                contract.brief.plan_id,
                contract.brief.objective,
                format_acceptance_from_items(&contract.brief.acceptance),
                render_recent_local_facts_section(&contract.state_frame.recent_local_facts),
            )
        };
        let plan_id = contract.brief.plan_id.clone();
        let objective = contract.brief.objective.clone();
        let acceptance = contract.brief.acceptance.clone();
        let plan_version = contract.brief.plan_version.clone();
        let step_revision = contract.brief.step_revision.clone();
        let assignment_fingerprint = contract.assignment_fingerprint.clone();
        let payload = json!({
            "task_id": b_task_id,
            "message": message,
            "step_id": step_id,
            "boss_plan_id": plan_id,
            "step_objective": objective,
            "step_acceptance": acceptance,
            "parent_session_id": parent_session_id,
            "plan_version": plan_version,
            "step_revision": step_revision,
            "assignment_fingerprint": assignment_fingerprint,
            "stale_brief_action": if needs_refresh { "refresh" } else { "reuse" },
            "refresh_reason": refresh_reason,
            "refresh_task": if needs_refresh {
                Some(assemble_brief_prompt(&contract.brief, &contract.state_frame))
            } else {
                None
            },
            "recent_local_facts": contract.state_frame.recent_local_facts,
            "allowed_tools": contract.allowed_tools,
            "lism_policy": contract.lism_policy,
            "task_contains_boss_context": needs_refresh,
        })
        .to_string();

        Ok(ContinuePayloadBuild {
            payload,
            assignment_fingerprint: contract.assignment_fingerprint,
            plan_version: contract.brief.plan_version,
            step_revision: contract.brief.step_revision,
        })
    }

    /// Builds a Continue payload that sends step context to a running ExecutorB task.
    pub async fn build_step_continue_payload(
        &self,
        step_id: usize,
        b_task_id: &str,
        parent_session_id: &str,
    ) -> anyhow::Result<String> {
        Ok(self
            .build_step_continue_payload_internal(step_id, b_task_id, parent_session_id)
            .await?
            .payload)
    }

    async fn build_step_spawn_payload_internal(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<SpawnPayloadBuild> {
        let contract = self
            .build_executor_b_assignment_contract(step_id, parent_session_id, false)
            .await?;
        let plan_id = contract.brief.plan_id.clone();
        let objective = contract.brief.objective.clone();
        let acceptance = contract.brief.acceptance.clone();
        let plan_version = contract.brief.plan_version.clone();
        let step_revision = contract.brief.step_revision.clone();
        let assignment_fingerprint = contract.assignment_fingerprint.clone();
        let payload = json!({
            "task": assemble_brief_prompt(
                &contract.brief,
                &contract.state_frame,
            ),
            "task_contains_boss_context": true,
            "role": "implement",
            "inherit_context": false,
            "allowed_tools": contract.allowed_tools,
            "lism_policy": contract.lism_policy,
            "context_strategy": "brief",
            "reuse_strategy": "running_only",
            "step_id": contract.brief.step_id,
            "boss_plan_id": plan_id,
            "step_objective": objective,
            "step_acceptance": acceptance,
            "parent_session_id": parent_session_id,
            "parent_runtime_role": "coordinator",
            "orchestration_group_id": b_actor_id,
            "boss_actor_role": "executor_b",
            "boss_lineage_depth": 0,
            "plan_version": plan_version,
            "step_revision": step_revision,
            "assignment_fingerprint": assignment_fingerprint,
        })
        .to_string();

        Ok(SpawnPayloadBuild {
            payload,
            assignment_fingerprint: contract.assignment_fingerprint,
            plan_version: contract.brief.plan_version,
            step_revision: contract.brief.step_revision,
        })
    }

    pub async fn build_step_spawn_payload(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<String> {
        Ok(self
            .build_step_spawn_payload_internal(step_id, parent_session_id, b_actor_id)
            .await?
            .payload)
    }

    async fn invoke_agent_tool(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        payload: &str,
    ) -> anyhow::Result<()> {
        self.invoke_agent_tool_with_task_id(app_state, payload)
            .await
            .map(|_| ())
    }

    /// Like `invoke_agent_tool` but returns the spawned task id extracted from the result text.
    async fn invoke_agent_tool_with_task_id(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        payload: &str,
    ) -> anyhow::Result<String> {
        let agent_tool = crate::tool::builtin::agent::AgentTool;
        let result = agent_tool
            .invoke(
                &ToolCall::new("Agent", payload),
                &app_state.permission_context,
            )
            .await?;

        match result {
            crate::tool::definition::ToolResult::Text(text) => {
                // Result text is "agent task {task_id} respawned for ..." or "agent task {task_id} reused for ..."
                let task_id = text
                    .split_whitespace()
                    .nth(2)
                    .unwrap_or("unknown")
                    .to_string();
                Ok(task_id)
            }
            crate::tool::definition::ToolResult::Denied(reason) => {
                anyhow::bail!("boss worker dispatch denied: {reason}")
            }
            crate::tool::definition::ToolResult::PendingApproval { message, .. } => {
                anyhow::bail!("boss worker dispatch requires approval: {message}")
            }
            crate::tool::definition::ToolResult::Interrupted(reason) => {
                anyhow::bail!("boss worker dispatch interrupted: {reason}")
            }
            crate::tool::definition::ToolResult::Progress(message) => {
                anyhow::bail!(
                    "boss worker dispatch returned progress instead of spawn result: {message}"
                )
            }
            crate::tool::definition::ToolResult::ResultTooLarge(reason) => {
                anyhow::bail!("boss worker dispatch returned oversized result: {reason}")
            }
        }
    }

    /// Ensure Designer A has a real LLM session. On first call, spawns an A session via
    /// AgentTool and writes the task id back to BossSession.designer_a.session_id.
    /// Subsequent calls are no-ops if the session id is already a real task id.
    async fn ensure_a_session(&self, app_state: &Arc<crate::state::app_state::AppState>) {
        // Without a task manager there is no way to track the spawned session — skip.
        if app_state.permission_context.task_manager.is_none() {
            return;
        }
        // Check if already initialized to a real session id (not the deterministic placeholder).
        let placeholder = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| {
                    let placeholder = format!("boss-{}-a", s.plan_id);
                    s.designer_a.session_id == placeholder || s.designer_a.session_id.is_empty()
                })
                .unwrap_or(true)
        };
        if !placeholder {
            return;
        }

        let payload = match self.build_a_session_payload(app_state).await {
            Ok(p) => p,
            Err(_) => return,
        };

        if let Ok(task_id) = self
            .invoke_agent_tool_with_task_id(app_state, &payload)
            .await
        {
            let mut guard = self.session.write().await;
            if let Some(session) = guard.as_mut() {
                session.designer_a.session_id = task_id.clone();
                session.designer_a.task_id = Some(task_id);
                session.designer_a.status = crate::core::boss_state::BossActorStatus::Active;
            }
        }
    }

    /// Send a message to A's running LLM session via AgentTool Continue.
    /// Requires `ensure_a_session` to have been called first so `designer_a.session_id` is real.
    /// Fire-and-forget: enqueues the message into A's mailbox; does not wait for A's reply.
    async fn send_to_a_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.designer_a.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            return;
        }
        {
            let mut guard = self.status.write().await;
            guard.last_a_dispatch_message = Some(message.clone());
        }
        let payload = json!({ "task_id": task_id, "message": message }).to_string();
        let _ = self.invoke_agent_tool(app_state, &payload).await;
    }

    /// Send a message to A's running LLM session and wait for A's response.
    /// Returns A's response text, or an error if A is not running or times out.
    /// Polls `TaskManager::get_output` until new content appears after the message is sent.
    async fn ask_a_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) -> anyhow::Result<String> {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.designer_a.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            anyhow::bail!("A session not initialized");
        }

        let tasks = app_state
            .permission_context
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;
        let session_id = app_state
            .permission_context
            .active_session_id
            .as_deref()
            .unwrap_or("");

        // Record current output offset before sending.
        let offset_before = tasks
            .get_output(&task_id, 0)
            .map(|s| s.next_offset)
            .unwrap_or(0);

        // Record the dispatch message for observability.
        {
            let mut guard = self.status.write().await;
            guard.last_a_dispatch_message = Some(message.clone());
        }

        // send_message uses std::sync::RwLock internally — run it on the blocking pool
        // so it doesn't stall the async runtime thread.
        let tasks_clone = tasks.clone();
        let task_id_clone = task_id.clone();
        let session_id_owned = session_id.to_string();
        let message_clone = message.clone();
        let sent = tokio::task::spawn_blocking(move || {
            tasks_clone.send_message(&task_id_clone, &session_id_owned, message_clone)
        })
        .await
        .unwrap_or(false);
        if !sent {
            anyhow::bail!("A session task {task_id} is not running");
        }

        // Poll for new output with a timeout.
        let timeout_secs = 30u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("A session response timed out after 30s");
            }
            if let Some(slice) = tasks.get_output(&task_id, offset_before) {
                if !slice.content.is_empty() {
                    return Ok(slice.content);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Parse A's LLM response text into a structured review decision.
    fn parse_a_review_decision(
        response: &str,
        summary: &str,
    ) -> crate::core::boss_actor_runtime::ReviewDecision {
        let upper = response.to_uppercase();
        if upper.contains("REPLAN_STEP") {
            let reason = response
                .to_uppercase()
                .find("REASON:")
                .map(|pos| response[pos + "REASON:".len()..].trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "review requested step replanning".to_string());
            return crate::core::boss_actor_runtime::ReviewDecision::ReplanStep {
                summary: summary.to_string(),
                reason,
            };
        }
        if upper.contains("REJECT") {
            let correction = response
                .to_uppercase()
                .find("CORRECTION:")
                .map(|pos| response[pos + "CORRECTION:".len()..].trim().to_string())
                .filter(|s| !s.is_empty());
            return crate::core::boss_actor_runtime::ReviewDecision::Correct {
                summary: summary.to_string(),
                correction,
            };
        }
        crate::core::boss_actor_runtime::ReviewDecision::Accept {
            summary: summary.to_string(),
        }
    }

    /// Summarize `old_part` via A session. Returns A's response string.
    /// If A is unavailable or the ask fails, returns Err — caller must fallback.
    /// Note: this call goes through ask_a_session and may leave a trace in A's runtime history.
    /// It does NOT write to BossPlan or session_snapshot.
    async fn summarize_context_with_a(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        old_part: &str,
    ) -> anyhow::Result<String> {
        let prompt = format!(
            "Summarize the following context concisely so it can replace the original in a continuation prompt. Preserve key decisions, constraints, and outcomes:\n\n{}",
            old_part
        );
        self.ask_a_session(app_state, prompt).await
    }

    /// Summarize `old_part` via a stateless one-shot provider call.
    /// Does NOT go through any session actor — A session history is never touched.
    /// Returns Err if the active model runtime is unavailable or the call fails.
    async fn summarize_context_stateless(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        old_part: &str,
    ) -> anyhow::Result<String> {
        let runtime = app_state
            .active_model_runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("active model runtime not available"))?;
        let snapshot = runtime.snapshot().await;
        let prompt = format!(
            "Summarize the following context concisely so it can replace the original in a continuation prompt. Preserve key decisions, constraints, and outcomes:\n\n{}",
            old_part
        );
        let msg = crate::core::message::Message::user(prompt);
        let events = snapshot.client.stream_message(&msg).await;
        let text: String = events
            .into_iter()
            .filter_map(|e| {
                if let crate::service::api::streaming::StreamEvent::TextDelta(t) = e {
                    Some(t)
                } else {
                    None
                }
            })
            .collect();
        if text.is_empty() {
            anyhow::bail!("stateless summarize returned empty response");
        }
        Ok(text)
    }

    /// Mirrors `ask_a_session` but reads from `executor_b.session_id`.
    async fn ask_b_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) -> anyhow::Result<String> {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.executor_b.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            anyhow::bail!("B session not initialized");
        }

        let tasks = app_state
            .permission_context
            .task_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("task manager not configured"))?;
        let session_id = app_state
            .permission_context
            .active_session_id
            .as_deref()
            .unwrap_or("")
            .to_string();

        // T26.5: dispatch-time budget gate — runs before T25/T25.2 trim/summarize.
        // Reject immediately if even the static prefix exceeds budget.
        if let BudgetDecision::Reject { reason } = evaluate_message_budget(&message) {
            anyhow::bail!("prompt budget exceeded: {reason}");
        }

        // Compress outbound payload before sending — does not modify BossPlan or session_snapshot.
        // Prefer LLM summarize via stateless provider call; fall back to character-truncation trim if unavailable.
        let original_chars = message.len();
        let (message, compression_strategy) = if message.len() > B_CONTEXT_TRIM_THRESHOLD {
            let split = message.len().saturating_sub(B_CONTEXT_KEEP_CHARS);
            let old_part = &message[..split];
            let recent_tail = &message[split..];
            match self.summarize_context_stateless(app_state, old_part).await {
                Ok(summary) => (
                    assemble_summarized_payload(&summary, recent_tail),
                    CompressionStrategy::Summarized,
                ),
                Err(_) => (
                    trim_context_payload(&message, B_CONTEXT_TRIM_THRESHOLD, B_CONTEXT_KEEP_CHARS),
                    CompressionStrategy::Trimmed,
                ),
            }
        } else {
            (message, CompressionStrategy::None)
        };

        // Record the final outbound message and per-dispatch metrics for test observability.
        {
            let mut guard = self.status.write().await;
            guard.last_b_ask_message = Some(message.clone());
            guard.last_step_metrics = Some(BossStepMetrics {
                compression_strategy,
                context_mode: ContextMode::Brief,
                original_chars,
                sent_chars: message.len(),
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
                cache_prefix_instability: false,
            });
        }

        let offset_before = tasks
            .get_output(&task_id, 0)
            .map(|s| s.next_offset)
            .unwrap_or(0);

        let tasks_clone = tasks.clone();
        let task_id_clone = task_id.clone();
        let message_clone = message.clone();
        let sent = tokio::task::spawn_blocking(move || {
            tasks_clone.send_message(&task_id_clone, &session_id, message_clone)
        })
        .await
        .unwrap_or(false);
        if !sent {
            anyhow::bail!("B session task {task_id} is not running");
        }

        let timeout_secs = 30u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("B session response timed out after 30s");
            }
            if let Some(slice) = tasks.get_output(&task_id, offset_before) {
                if !slice.content.is_empty() {
                    return Ok(slice.content);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Fire-and-forget: send a message to B's running LLM session without waiting for a reply.
    async fn send_to_b_session(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) {
        let task_id = {
            let guard = self.session.read().await;
            guard
                .as_ref()
                .map(|s| s.executor_b.session_id.clone())
                .unwrap_or_default()
        };
        if task_id.is_empty() {
            return;
        }
        let payload = json!({ "task_id": task_id, "message": message }).to_string();
        let _ = self.invoke_agent_tool(app_state, &payload).await;
    }

    /// Build the JSON payload for spawning Designer A's LLM session.
    async fn build_a_session_payload(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
    ) -> anyhow::Result<String> {
        let plan_id = self
            .plan
            .read()
            .await
            .as_ref()
            .map(|p| p.plan_id.clone())
            .unwrap_or_default();
        let parent_session_id = app_state.active_session_id.clone();
        Ok(json!({
            "task": format!(
                "Designer A review session for plan {plan_id}. Stay idle until the coordinator sends the actual review/spec content. Do not search the repository, and do not use Glob/Grep just to locate the plan id."
            ),
            "role": "research",
            "allowed_tools": ["Read"],
            "boss_plan_id": plan_id,
            "step_objective": "Review and approve boss plan steps as Designer A. Use only the text provided in-session unless a later message explicitly names a file path to inspect.",
            "parent_session_id": parent_session_id,
            "reuse_strategy": "running_only",
        })
        .to_string())
    }

    async fn update_current_step(&self, current_step: Option<usize>) {
        let mut status = self.status.write().await;
        status.current_step = current_step;
    }

    async fn update_step_status(
        &self,
        step_id: usize,
        next_status: BossPlanStepStatus,
    ) -> anyhow::Result<()> {
        let mut plan_guard = self.plan.write().await;
        let plan = plan_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
        let step = plan
            .steps
            .iter_mut()
            .find(|step| step.id == step_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;
        step.status = next_status;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AdvanceOutcome {
    Dispatch(usize),
    ApprovalBarrier(usize),
    TerminalFailure(String),
    PlanComplete,
    ReplanRequired(usize, String),
    NoRunnableStep,
}

fn model_tier_label(tier: ModelTier) -> &'static str {
    match tier {
        ModelTier::Low => "low",
        ModelTier::Medium => "medium",
        ModelTier::High => "high",
    }
}

fn effective_lism_enabled(policy: BossLisMPolicy, session_lism: bool) -> bool {
    match policy {
        BossLisMPolicy::Inherit => session_lism,
        BossLisMPolicy::ForceOn => true,
        BossLisMPolicy::ForceOff => false,
    }
}

fn next_unfinished_step_id(plan: &BossPlan) -> Option<usize> {
    plan.steps
        .iter()
        .find(|step| !step.completed)
        .map(|step| step.id)
}

fn next_runnable_step(plan: &BossPlan) -> Option<&BossPlanStep> {
    plan.steps.iter().find(|step| {
        !step.completed
            && matches!(
                step.status,
                BossPlanStepStatus::Pending | BossPlanStepStatus::Rejected
            )
    })
}

fn format_acceptance(step: &BossPlanStep) -> String {
    format_acceptance_from_items(&step.acceptance)
}

fn format_acceptance_from_items(items: &[String]) -> String {
    if items.is_empty() {
        "- Complete the step objective.".into()
    } else {
        items
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn history_summary_from_app_state(
    app_state: &crate::state::app_state::AppState,
) -> Option<Vec<String>> {
    if let Some(history) = &app_state.history {
        return Some(history_summary_from_history(history));
    }

    let session_store = app_state.session_store.as_ref()?;
    let session = app_state.session.as_ref()?;
    let request = crate::history::session::SessionRestoreRequest {
        resume: Some(session.session_id.0.clone()),
        continue_session: false,
    };
    let (_, history) = session_store.load(&request)?;
    Some(history_summary_from_history(&history))
}

fn history_summary_from_history(history: &SessionHistory) -> Vec<String> {
    history
        .entries
        .iter()
        .rev()
        .take(3)
        .map(|entry| {
            let text = entry.message.text();
            let text = text.lines().next().unwrap_or("").trim();
            if text.is_empty() {
                "(empty history entry)".into()
            } else {
                text.to_string()
            }
        })
        .collect::<Vec<_>>()
}

impl Default for BossCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Saves a boss plan to a file using atomic write to prevent corruption.
pub async fn save_plan(plan: &BossPlan, path: &std::path::Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let content = serde_json::to_string_pretty(plan)?;
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, content).await?;
    tokio::fs::rename(tmp_path, path).await?;

    Ok(())
}

/// Loads a boss plan from a file (free function, no self needed).
pub async fn load_plan(path: &std::path::Path) -> anyhow::Result<BossPlan> {
    let content = tokio::fs::read_to_string(path).await?;
    let plan = serde_json::from_str(&content)?;
    Ok(plan)
}

/// Default character threshold above which outbound B context payloads are trimmed.
pub const B_CONTEXT_TRIM_THRESHOLD: usize = 80_000;
/// Default number of characters to keep (tail window) when trimming.
pub const B_CONTEXT_KEEP_CHARS: usize = 40_000;

/// Trim an outbound B context payload to at most `keep_chars` characters.
///
/// If `payload.len() <= threshold`, returns the payload unchanged.
/// Otherwise, keeps the last `keep_chars` characters and prepends a fixed notice line
/// so the provider knows earlier context was omitted.
///
/// This operates only on the runtime payload string — it never touches BossPlan,
/// session_snapshot, or any persisted state.
pub fn trim_context_payload(payload: &str, threshold: usize, keep_chars: usize) -> String {
    if payload.len() <= threshold {
        return payload.to_string();
    }
    let omitted = payload.len().saturating_sub(keep_chars);
    let tail = if keep_chars >= payload.len() {
        payload
    } else {
        &payload[payload.len() - keep_chars..]
    };
    format!("[trimmed earlier context: {omitted} chars omitted]\n{tail}")
}

/// Assemble a summarized B context payload from a pre-computed summary and the recent tail.
/// This is the output shape produced by the summarize-first path in `ask_b_session`.
/// Exposed as a pure function so tests can verify the assembly contract without a live A session.
pub fn assemble_summarized_payload(summary: &str, recent_tail: &str) -> String {
    format!("[summary: {summary}]\n{recent_tail}")
}

impl BossCoordinator {
    /// Save the current plan to disk, embedding the current session snapshot so
    /// A/B identity (session_id / task_id) survives a restart.
    /// Liveness of the stored task_id is NOT guaranteed — callers must do a
    /// live-task check before reusing it.
    async fn save_plan_with_session(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let session_snap = self.session.read().await.clone();
        let mut plan_guard = self.plan.write().await;
        if let Some(plan) = plan_guard.as_mut() {
            plan.session_snapshot = session_snap;
            save_plan(plan, path).await?;
        }
        Ok(())
    }

    pub async fn ask_b_session_pub(
        &self,
        app_state: &Arc<crate::state::app_state::AppState>,
        message: String,
    ) -> anyhow::Result<String> {
        self.ask_b_session(app_state, message).await
    }

    pub async fn build_b_step_payload_pub(
        &self,
        step_id: usize,
        parent_session_id: &str,
        b_actor_id: &str,
    ) -> anyhow::Result<String> {
        self.build_step_spawn_payload(step_id, parent_session_id, b_actor_id)
            .await
    }

    pub async fn repair_replan_step(
        &self,
        step_id: usize,
        patched_description: String,
        patched_objective: Option<String>,
        patched_acceptance: Vec<String>,
    ) -> anyhow::Result<()> {
        let plan_path = {
            self.status
                .read()
                .await
                .planning_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("No planning file configured"))?
        };

        {
            let mut plan_guard = self.plan.write().await;
            let plan = plan_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No plan loaded"))?;
            let step = plan
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or_else(|| anyhow::anyhow!("Unknown boss step {step_id}"))?;

            if step.status != BossPlanStepStatus::ReplanRequired {
                anyhow::bail!(
                    "Boss step {} is not awaiting replanning (current status: {:?})",
                    step_id,
                    step.status
                );
            }

            step.description = patched_description;
            step.objective = patched_objective;
            step.acceptance = patched_acceptance;
            step.status = BossPlanStepStatus::Pending;
            step.completed = false;
            step.worker_task_id = None;
            step.review_task_id = None;
            step.attempt_count = 0;
            step.last_correction = None;
        }

        self.update_current_step(Some(step_id)).await;
        self.save_plan_with_session(std::path::Path::new(&plan_path))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::state_frame::{
        AgentState, CompletionEvidenceGap, CompletionEvidenceStatus, WorkerStructuredReport,
    };
    use crate::core::state_frame_loop::LoopUsage;
    use crate::tool::result::{ToolOutcome, ToolOutcomeKind};

    #[tokio::test]
    async fn test_boss_coordinator_initial_stage_is_documentation() {
        let coordinator = BossCoordinator::new();
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn test_state_transition_to_waiting_for_approval() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);
    }

    #[tokio::test]
    async fn test_user_approval_y_transitions_to_execution() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        // set dummy plan to avoid ignoring boolean conversion
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                finalized: true,
                ..BossPlan::default()
            });
        }
        let confirmed = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(confirmed);
        assert_eq!(coordinator.get_stage().await, BossStage::Execution);
        assert!(
            coordinator
                .plan
                .read()
                .await
                .as_ref()
                .unwrap()
                .accepted_by_user
        );
    }

    #[tokio::test]
    async fn test_user_approval_feedback_returns_to_documentation() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan::default());
        }
        let rejected = coordinator
            .handle_user_approval("Wait, this is wrong")
            .await
            .unwrap();
        assert!(!rejected);
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[test]
    fn boss_metadata_records_last_failure_kind_and_recommended_repair() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let mut usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            last_failure_outcome: Some(ToolOutcome {
                kind: ToolOutcomeKind::MissingPath,
                recoverable: true,
                recommended_next_action: Some("create_file".into()),
                evidence_ref: Some("tool_feedback:3".into()),
                bounded_excerpt: Some("No such file or directory".into()),
                truncated: false,
            }),
            ..LoopUsage::default()
        };
        usage.tool_dispatch_failure_count = 1;

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.last_effective_tool_action.as_deref(),
            Some("Read")
        );
        assert_eq!(
            routed_metadata.last_failure_kind.as_deref(),
            Some("missing_path")
        );
        assert_eq!(routed_metadata.last_failure_recoverable, Some(true));
        assert_eq!(
            routed_metadata.last_recommended_repair.as_deref(),
            Some("create_file")
        );
        assert_eq!(
            routed_metadata.last_failure_evidence_ref.as_deref(),
            Some("tool_feedback:3")
        );
    }

    #[test]
    fn boss_metadata_clears_stale_failure_after_later_success() {
        let mut routed_metadata = BossStepRoutedMetadata {
            last_effective_tool_action: Some("Edit".into()),
            last_failure_kind: Some("missing_path".into()),
            last_failure_recoverable: Some(true),
            last_recommended_repair: Some("create_file".into()),
            last_failure_evidence_ref: Some("tool_feedback:7".into()),
            last_failure_bounded_excerpt: Some("stale failure".into()),
            last_failure_truncated: Some(false),
            ..BossStepRoutedMetadata::default()
        };
        let usage = LoopUsage {
            last_effective_tool_action: Some("Read".into()),
            last_failure_outcome: None,
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.last_effective_tool_action.as_deref(),
            Some("Read")
        );
        assert_eq!(routed_metadata.last_failure_kind, None);
        assert_eq!(routed_metadata.last_failure_recoverable, None);
        assert_eq!(routed_metadata.last_recommended_repair, None);
        assert_eq!(routed_metadata.last_failure_evidence_ref, None);
        assert_eq!(routed_metadata.last_failure_bounded_excerpt, None);
        assert_eq!(routed_metadata.last_failure_truncated, None);
    }

    #[test]
    fn boss_metadata_records_recovery_tier_and_outcome() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            recovery_attempted: true,
            recovery_tier: Some("artifact_repair_turn".into()),
            recovery_outcome: Some("repair_turn_injected".into()),
            terminal_blocker_kind: Some("same_invalid_strategy".into()),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(routed_metadata.recovery_attempted, Some(true));
        assert_eq!(
            routed_metadata.recovery_tier.as_deref(),
            Some("artifact_repair_turn")
        );
        assert_eq!(
            routed_metadata.recovery_outcome.as_deref(),
            Some("repair_turn_injected")
        );
        assert_eq!(
            routed_metadata.terminal_blocker_kind.as_deref(),
            Some("same_invalid_strategy")
        );
    }

    #[test]
    fn boss_report_surfaces_worker_structured_report() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            completion_evidence_status: Some(CompletionEvidenceStatus::Sufficient),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("Bash".into()),
                files_changed: vec!["/tmp/report.md".into()],
                tests_run: vec!["cargo_test:passed".into()],
                artifact_status: "verified".into(),
                test_status: "passed".into(),
                verification_status: "verified".into(),
                evidence_refs: vec!["tool_output:1".into(), "artifact:step1:runtime:0".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(
            routed_metadata.completion_evidence_status.as_deref(),
            Some("sufficient")
        );
        let report = routed_metadata.worker_report.expect("worker report");
        assert_eq!(report.worker_state, AgentState::Done);
        assert_eq!(report.artifact_status, "verified");
        assert!(
            report
                .evidence_refs
                .iter()
                .any(|reference| reference == "tool_output:1")
        );
    }

    #[test]
    fn boss_report_identifies_exact_missing_artifact_gap_for_second_target() {
        let mut routed_metadata = BossStepRoutedMetadata::default();
        let usage = LoopUsage {
            completion_evidence_status: Some(CompletionEvidenceStatus::MissingArtifactEvidence),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Executing,
                last_tool_action: Some("Write".into()),
                files_changed: vec!["/tmp/one.md".into()],
                tests_run: Vec::new(),
                artifact_status: "touched".into(),
                test_status: "not_run".into(),
                verification_status: "unverified".into(),
                evidence_refs: vec!["change:1".into()],
                completion_evidence_gaps: vec![CompletionEvidenceGap {
                    target_ref: "artifact:contract:1".into(),
                    target_path: Some("/tmp/two.md".into()),
                    missing_artifact_evidence: true,
                    missing_test_evidence: false,
                    missing_verification_evidence: false,
                    recommended_action: "write_artifact".into(),
                }],
                remaining_risks: vec![
                    "completion_evidence_status=missing_artifact_evidence".into(),
                ],
                completion_evidence_status: CompletionEvidenceStatus::MissingArtifactEvidence,
            }),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);

        assert_eq!(routed_metadata.completion_evidence_gaps.len(), 1);
        let gap = &routed_metadata.completion_evidence_gaps[0];
        assert_eq!(gap.target_ref, "artifact:contract:1");
        assert_eq!(gap.target_path.as_deref(), Some("/tmp/two.md"));
        assert!(gap.missing_artifact_evidence);
        assert_eq!(gap.recommended_action, "write_artifact");
    }

    #[test]
    fn boss_metadata_clears_old_completion_gaps_after_later_success() {
        let mut routed_metadata = BossStepRoutedMetadata {
            completion_evidence_gaps: vec![CompletionEvidenceGap {
                target_ref: "artifact:contract:1".into(),
                target_path: Some("/tmp/two.md".into()),
                missing_artifact_evidence: true,
                missing_test_evidence: false,
                missing_verification_evidence: false,
                recommended_action: "write_artifact".into(),
            }],
            ..BossStepRoutedMetadata::default()
        };
        let usage = LoopUsage {
            completion_evidence_status: Some(CompletionEvidenceStatus::Sufficient),
            worker_report: Some(WorkerStructuredReport {
                worker_state: AgentState::Done,
                last_tool_action: Some("ArtifactVerify".into()),
                files_changed: vec!["/tmp/one.md".into(), "/tmp/two.md".into()],
                tests_run: Vec::new(),
                artifact_status: "verified".into(),
                test_status: "not_required".into(),
                verification_status: "verified".into(),
                evidence_refs: vec!["artifact:verified".into()],
                completion_evidence_gaps: Vec::new(),
                remaining_risks: Vec::new(),
                completion_evidence_status: CompletionEvidenceStatus::Sufficient,
            }),
            ..LoopUsage::default()
        };

        BossCoordinator::apply_loop_usage_to_routed_metadata(&mut routed_metadata, &usage);
        assert!(routed_metadata.completion_evidence_gaps.is_empty());
    }

    #[tokio::test]
    async fn missing_artifact_after_done_escalates_to_repair_instead_of_terminal_success() {
        let coordinator = BossCoordinator::new();
        let target_path = std::env::temp_dir().join(format!(
            "boss_missing_artifact_{}_{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let objective = format!(
            "任务目标：\n- 目标文件：{}\n- 生成一份 markdown 报告",
            target_path.display()
        );
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                steps: vec![BossPlanStep {
                    id: 1,
                    description: "write report".into(),
                    objective: Some(objective),
                    acceptance: vec!["target file exists and is non-empty".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Reviewing,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..BossPlan::default()
            });
        }
        {
            let mut metadata = coordinator.routed_step_metadata.write().await;
            metadata.insert(1, BossStepRoutedMetadata::default());
        }

        coordinator
            .apply_review_verdict(
                1,
                &crate::core::boss_actor_runtime::ReviewDecision::Accept {
                    summary: "worker says done".into(),
                },
            )
            .await
            .expect("apply review verdict");

        let plan = coordinator.plan.read().await;
        let step = plan
            .as_ref()
            .and_then(|plan| plan.steps.iter().find(|step| step.id == 1))
            .expect("step");
        assert_eq!(step.status, BossPlanStepStatus::Rejected);
        assert!(!step.completed);
        assert_eq!(step.attempt_count, 1);
        let correction = step.last_correction.as_deref().expect("repair correction");
        assert!(correction.contains(&format!("target_path={}", target_path.display())));
        assert!(correction.contains("recommended_write_strategy=write_exact_target_file"));

        let routed_metadata = coordinator.routed_step_metadata.read().await;
        let metadata = routed_metadata.get(&1).expect("routed metadata");
        assert_eq!(metadata.recovery_attempted, Some(true));
        assert_eq!(
            metadata.recovery_tier.as_deref(),
            Some("boss_artifact_repair")
        );
        assert_eq!(
            metadata.recovery_outcome.as_deref(),
            Some("repair_dispatched")
        );
        assert_eq!(metadata.terminal_blocker_kind, None);
    }

    #[tokio::test]
    async fn test_user_approval_requires_finalized_documentation() {
        let coordinator = BossCoordinator::new();
        coordinator
            .transition_to(BossStage::WaitingForApproval)
            .await
            .unwrap();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan::default());
        }

        let confirmed = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(!confirmed);
        assert_eq!(coordinator.get_stage().await, BossStage::WaitingForApproval);
        assert!(
            !coordinator
                .plan
                .read()
                .await
                .as_ref()
                .unwrap()
                .accepted_by_user
        );
    }

    #[tokio::test]
    async fn test_handle_user_approval_rejects_call_from_wrong_state() {
        let coordinator = BossCoordinator::new();
        // Still in Documentation (not WaitingForApproval) — should be a no-op and return false
        let result = coordinator.handle_user_approval("Y").await.unwrap();
        assert!(!result);
        // Should remain unchanged
        assert_eq!(coordinator.get_stage().await, BossStage::Documentation);
    }

    #[tokio::test]
    async fn test_boss_plan_persistence() {
        let plan = BossPlan {
            plan_id: "plan-test".into(),
            task_description: "Fix bugs".into(),
            document_spec: "Spec v1".into(),
            pseudo_code: "Code v1".into(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let temp_dir = std::env::temp_dir();
        let plan_path = temp_dir.join("boss_test_plan.json");

        save_plan(&plan, &plan_path).await.unwrap();
        let loaded = load_plan(&plan_path).await.unwrap();

        assert_eq!(loaded.task_description, "Fix bugs");
        assert_eq!(loaded.document_spec, "Spec v1");
        assert!(loaded.accepted_by_user);

        std::fs::remove_file(plan_path).unwrap();
    }

    #[test]
    fn test_default_plan_path_uses_morgo_boss_dir() {
        let root = std::path::Path::new("/home/user/project");
        let path = BossCoordinator::default_plan_path(root);
        assert_eq!(
            path,
            std::path::Path::new("/home/user/project/.morgo/boss/planning.json")
        );
    }

    #[tokio::test]
    async fn test_restore_or_init_handles_state_properly() {
        let temp_dir = std::env::temp_dir();
        let plan_path = temp_dir.join("boss_test_restore_plan.json");

        // 1. Init without file
        let new_coordinator = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
        assert_eq!(new_coordinator.get_stage().await, BossStage::Documentation);
        assert_eq!(
            new_coordinator
                .status
                .read()
                .await
                .planning_file
                .as_ref()
                .unwrap(),
            &plan_path.to_string_lossy().into_owned()
        );

        // 2. Save a plan that is accepted
        let plan = BossPlan {
            plan_id: "plan-restore".into(),
            task_description: "task".into(),
            accepted_by_user: true,
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![crate::core::boss_state::BossPlanStep {
                id: 0,
                description: "".into(),
                objective: None,
                acceptance: Vec::new(),
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
                tool_execution_records: Vec::new(),
            }],
            ..Default::default()
        };
        save_plan(&plan, &plan_path).await.unwrap();

        // 3. Restore and verify it skips straight to Execution
        let restored = BossCoordinator::restore_or_init(&plan_path).await.unwrap();
        assert_eq!(restored.get_stage().await, BossStage::Execution);
        assert_eq!(restored.status.read().await.current_step, Some(0));

        std::fs::remove_file(plan_path).unwrap();
    }

    #[tokio::test]
    async fn test_has_terminal_failure_detects_failed_step() {
        let coordinator = BossCoordinator::new();
        {
            let mut plan = coordinator.plan.write().await;
            *plan = Some(BossPlan {
                accepted_by_user: true,
                auto_sequence: true,
                steps: vec![crate::core::boss_state::BossPlanStep {
                    id: 0,
                    description: "failed".into(),
                    objective: None,
                    acceptance: Vec::new(),
                    requires_approval: false,
                    status: BossPlanStepStatus::Failed,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: None,
                    last_correction: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                }],
                ..Default::default()
            });
        }

        assert!(coordinator.has_terminal_failure().await);
    }

    #[test]
    fn seed_step_acceptance_adds_artifact_expectation_for_target_file() {
        let acceptance = seed_step_acceptance(
            "任务目标：\n- 目标文件：/tmp/example-report.md\n- 生成一份 markdown 报告",
        );
        assert!(
            acceptance
                .iter()
                .any(|item| item == "Task completed successfully.")
        );
        assert!(
            acceptance.iter().any(|item| {
                item == "target file exists and is non-empty: /tmp/example-report.md"
            })
        );
    }

    #[test]
    fn seed_step_acceptance_adds_readme_expectation_for_target_directory_tasks() {
        let acceptance = seed_step_acceptance(
            "任务目标：\n- 目标目录：/tmp/example-agent-site\n- 输出一个简短 README，说明如何打开与查看。",
        );
        assert!(acceptance.iter().any(|item| {
            item == "target directory exists and is non-empty: /tmp/example-agent-site"
        }));
        assert!(acceptance.iter().any(|item| {
            item == "target file exists and is non-empty: /tmp/example-agent-site/README.md"
        }));
    }

    #[test]
    fn extract_relevant_file_handles_normalizes_agent_relative_paths() {
        let repo_root = Path::new("/Users/wangmorgan/MProject/LearnCCfromCC");
        assert_eq!(
            normalize_relevant_file_hint("src/tool/definition.rs", Some(repo_root)).as_deref(),
            Some("RustAgent/Agent/src/tool/definition.rs")
        );
        assert_eq!(
            normalize_relevant_file_hint(
                "../docs/30-boss-mode-and-dual-agent-workflow.md",
                Some(repo_root)
            )
            .as_deref(),
            Some("RustAgent/docs/30-boss-mode-and-dual-agent-workflow.md")
        );
    }

    #[test]
    fn extract_relevant_file_handles_ignores_root_only_tokens() {
        let handles = extract_relevant_file_handles(
            "任务目标：\n- 工具输入：\n  - /\n  - /tmp/example/samples/\n- 目标文件：/tmp/example/report.md",
            "step-1-attempt-0",
        );
        assert!(!handles.iter().any(|handle| handle.path == "/"));
        assert!(
            handles
                .iter()
                .any(|handle| handle.path == "/tmp/example/samples/"
                    && handle.kind == "target_directory")
        );
        assert!(handles.iter().any(|handle| {
            handle.path == "/tmp/example/report.md"
                && handle.kind == "target_file"
                && handle.step_revision == "step-1-attempt-0"
        }));
    }

    #[test]
    fn extract_relevant_file_handles_filters_slash_commands_and_malformed_path_tokens() {
        let handles = extract_relevant_file_handles(
            "任务目标：\n- /boss\n- /mcp\n- 已完成，`/tmp/example/report.md\n- 目标目录：/tmp/example/output/\n- 目标文件：/tmp/example/report.md",
            "step-1-attempt-1",
        );
        assert!(!handles.iter().any(|handle| handle.path == "/boss"));
        assert!(!handles.iter().any(|handle| handle.path == "/mcp"));
        assert!(handles.iter().any(
            |handle| handle.path == "/tmp/example/output/" && handle.kind == "target_directory"
        ));
        assert!(
            handles
                .iter()
                .any(|handle| handle.path == "/tmp/example/report.md"
                    && handle.kind == "target_file")
        );
    }

    #[test]
    fn collect_recent_decisions_keeps_latest_review_summaries() {
        let mut steps = Vec::new();
        for id in 0..5 {
            let mut step = BossPlanStep {
                id,
                description: format!("step {id}"),
                objective: Some(format!("objective {id}")),
                acceptance: vec![format!("acceptance {id}")],
                requires_approval: false,
                status: BossPlanStepStatus::Completed,
                completed: true,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: Some(format!("summary {id}")),
                last_correction: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            };
            if id == 4 {
                step.status = BossPlanStepStatus::Pending;
                step.completed = false;
            }
            steps.push(step);
        }
        let plan = BossPlan {
            plan_id: "plan-alpha".into(),
            task_description: "task".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps,
            accepted_by_user: true,
            auto_sequence: true,
            session_snapshot: None,
        };

        let recent = collect_recent_decisions(&plan, 4);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0], "step 1 review: summary 1");
        assert_eq!(recent[2], "step 3 review: summary 3");
    }

    #[test]
    fn collect_target_artifacts_merges_expectations_and_target_files() {
        let step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some(
                "任务目标：\n- 目标文件：/tmp/report.md\n- 目标目录：/tmp/results/\n- 产出 markdown 报告"
                    .into(),
            ),
            acceptance: Vec::new(),
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
            tool_execution_records: Vec::new(),
        };
        let artifacts =
            collect_target_artifacts(&step, &["/tmp/report.md".into(), "/tmp/results/".into()]);
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/report.md")
        );
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/results/")
        );
    }

    #[test]
    fn collect_blocked_items_uses_review_summary_for_failed_steps() {
        let step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some("objective".into()),
            acceptance: Vec::new(),
            requires_approval: false,
            status: BossPlanStepStatus::Failed,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: Some("tests are still failing".into()),
            last_correction: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        assert_eq!(
            collect_blocked_items(&step),
            vec!["tests are still failing"]
        );
    }

    #[test]
    fn store_step_result_diff_prefers_primary_but_uses_fallback() {
        let mut step = BossPlanStep {
            id: 0,
            description: "step".into(),
            objective: Some("objective".into()),
            acceptance: Vec::new(),
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
            tool_execution_records: Vec::new(),
        };

        store_step_result_diff(&mut step, "", Some("fallback summary"));
        assert_eq!(step.result_diff.as_deref(), Some("fallback summary"));
        store_step_result_diff(&mut step, "primary result", Some("ignored"));
        assert_eq!(step.result_diff.as_deref(), Some("primary result"));
    }
}

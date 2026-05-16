use crate::core::boss_acceptance::extract_artifact_expectations;
use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_fact_ledger::{
    build_blocker_records, build_open_item_records, build_rejected_approach_records,
    build_step_fact_ledgers_with_mode,
};
use crate::core::state_frame::{
    ActorRole, AgentState, DeclaredArtifactContract, ReviewMode, StageContinuationContext,
    StageExecutionContract, StateBudget, StateFrame, TaskProfile, TestContract,
    VerificationContract,
};
use crate::core::state_frame_archive::{
    archive_to_summary, build_accepted_archive, retain_blocked_items, retain_open_items,
};
use crate::core::state_frame_router::{apply_route, route_toolset};

fn current_task_contract_text(text: &str) -> String {
    const HISTORICAL_CONTEXT_MARKERS: &[&str] = &[
        "参考材料摘录",
        "参考材料：",
        "参考背景材料",
        "关键材料摘录",
        "历史材料",
        "历史上下文",
        "背景材料摘录",
        "roadmap 摘录",
        "Roadmap 摘录",
    ];
    let mut lines = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if HISTORICAL_CONTEXT_MARKERS
            .iter()
            .any(|marker| trimmed.starts_with(marker))
        {
            break;
        }
        lines.push(line);
    }
    lines.join("\n")
}

fn projection_objective_text(
    step: Option<&crate::core::boss_state::BossPlanStep>,
    fallback: &str,
) -> String {
    let text = step.map(|step| step.objective()).unwrap_or(fallback);
    if step_is_true_inline_reference_task(step) {
        text.to_string()
    } else {
        current_task_contract_text(text)
    }
}

fn step_is_true_inline_reference_task(
    step: Option<&crate::core::boss_state::BossPlanStep>,
) -> bool {
    let Some(step) = step else {
        return false;
    };
    let contract = &step.stage_execution_contract;
    let profile_keeps_reference = contract.task_profile.is_some_and(|profile| {
        matches!(
            profile,
            TaskProfile::IndependentReview | TaskProfile::ReadOnlyAnalysis
        )
    });
    if !profile_keeps_reference {
        return false;
    }
    let has_durable_contract = !contract.declared_artifacts.is_empty();
    let objective_declares_artifacts =
        !extract_artifact_expectations(&current_task_contract_text(step.objective())).is_empty();
    let requires_write = contract.required_actions.iter().any(|action| {
        matches!(
            action.as_str(),
            "create" | "write" | "write_file" | "edit_file" | "create_file"
        )
    }) || contract.declared_artifacts.iter().any(|artifact| {
        artifact.required_actions.iter().any(|action| {
            matches!(
                action.as_str(),
                "create" | "write" | "write_file" | "edit_file" | "create_file"
            )
        })
    });
    let route_corrected_to_code_change =
        matches!(contract.task_profile, Some(TaskProfile::CodeChange));
    !(has_durable_contract
        || objective_declares_artifacts
        || requires_write
        || route_corrected_to_code_change)
}

fn is_readonly_analysis(plan: &BossPlan, step_id: Option<usize>) -> bool {
    if step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .and_then(|step| step.stage_execution_contract.task_profile)
        .is_some_and(|profile| matches!(profile, TaskProfile::ReadOnlyAnalysis))
    {
        return true;
    }
    false
}

fn sanitize_extracted_artifact_path(path: &str) -> String {
    let end = path
        .char_indices()
        .find_map(|(idx, ch)| {
            (ch.is_whitespace()
                || matches!(
                    ch,
                    '`' | '"' | '\'' | '，' | '。' | '；' | '、' | ')' | '）' | ']' | '】'
                ))
            .then_some(idx)
        })
        .unwrap_or(path.len());
    path[..end]
        .trim()
        .trim_end_matches(['.', ',', ':', ';'])
        .to_string()
}

fn step_looks_like_development_task(
    step: Option<&crate::core::boss_state::BossPlanStep>,
    _declared_artifacts: &[DeclaredArtifactContract],
) -> bool {
    if let Some(task_profile) = step.and_then(|step| step.stage_execution_contract.task_profile) {
        return matches!(task_profile, TaskProfile::CodeChange);
    }
    false
}

fn has_source_evidence_continuation(step: Option<&crate::core::boss_state::BossPlanStep>) -> bool {
    step.and_then(|step| step.stage_continuation_context.as_ref())
        .and_then(|context| context.next_action.as_deref())
        == Some("read_source_evidence")
}

fn has_worker_correction_continuation(
    step: Option<&crate::core::boss_state::BossPlanStep>,
) -> bool {
    step.and_then(|step| step.stage_continuation_context.as_ref())
        .is_some_and(|context| {
            context.next_action.as_deref() == Some("worker_correction")
                || context
                    .repair_intent
                    .as_ref()
                    .and_then(|intent| intent.next_action.as_deref())
                    == Some("worker_correction")
        })
}

fn apply_development_test_policy(contract: &mut StageExecutionContract) {
    if contract.tests.is_empty() {
        contract.tests.push(TestContract {
            name: "st_auto_validation".into(),
            required_actions: vec!["run_test".into()],
            required_evidence: vec!["runtime_test_passed".into()],
        });
    }
    if !contract
        .required_actions
        .iter()
        .any(|action| action == "run_test")
    {
        contract.required_actions.push("run_test".into());
    }
    if !contract
        .required_evidence
        .iter()
        .any(|item| item == "runtime_test_passed")
    {
        contract
            .required_evidence
            .push("runtime_test_passed".into());
    }
}

fn sort_required_actions(actions: &mut Vec<String>) {
    fn order(action: &str) -> usize {
        match action {
            "create" => 0,
            "write" => 1,
            "run_test" => 2,
            "verify" => 3,
            "verify_artifact" => 4,
            _ => 100,
        }
    }
    actions.sort_by(|left, right| order(left).cmp(&order(right)).then_with(|| left.cmp(right)));
    actions.dedup();
}

fn independent_review_requires_runtime_verification(contract: &StageExecutionContract) -> bool {
    contract
        .review_mode
        .is_some_and(|mode| mode.is_independent_review())
        && !contract.verifications.is_empty()
}

fn directory_verification_fallback_child_path(directory: &str) -> String {
    format!("{}/README.md", directory.trim_end_matches('/'))
}

fn push_unique_verification_target(targets: &mut Vec<String>, target: String) {
    if !target.trim().is_empty() && !targets.iter().any(|existing| existing == &target) {
        targets.push(target);
    }
}

fn verification_contract_artifact<'a>(
    contract: &'a StageExecutionContract,
    verification: &VerificationContract,
) -> Option<&'a DeclaredArtifactContract> {
    contract
        .declared_artifacts
        .iter()
        .find(|artifact| artifact.ref_id == verification.target_ref)
        .or_else(|| {
            verification.target_path.as_ref().and_then(|target_path| {
                contract
                    .declared_artifacts
                    .iter()
                    .find(|artifact| artifact.path == *target_path)
            })
        })
}

fn readable_verification_targets(contract: &StageExecutionContract) -> Vec<String> {
    let mut targets = Vec::new();
    for verification in &contract.verifications {
        let raw_target = verification
            .target_path
            .as_deref()
            .unwrap_or(verification.target_ref.as_str())
            .trim();
        if raw_target.is_empty() {
            continue;
        }

        if let Some(artifact) = verification_contract_artifact(contract, verification) {
            if artifact.kind == "directory" {
                let prefix = format!("{}/", artifact.path.trim_end_matches('/'));
                let child_paths = contract
                    .declared_artifacts
                    .iter()
                    .filter(|candidate| {
                        candidate.kind != "directory" && candidate.path.starts_with(&prefix)
                    })
                    .map(|candidate| candidate.path.clone())
                    .collect::<Vec<_>>();
                if child_paths.is_empty() {
                    push_unique_verification_target(
                        &mut targets,
                        directory_verification_fallback_child_path(&artifact.path),
                    );
                } else {
                    for child_path in child_paths {
                        push_unique_verification_target(&mut targets, child_path);
                    }
                }
                continue;
            }
        }

        push_unique_verification_target(&mut targets, raw_target.to_string());
    }
    targets
}

fn infer_review_mode(
    step: Option<&crate::core::boss_state::BossPlanStep>,
    _readonly_analysis: bool,
) -> Option<ReviewMode> {
    if let Some(review_mode) = step.and_then(|step| step.stage_execution_contract.review_mode) {
        return Some(review_mode);
    }
    Some(ReviewMode::IndependentReview)
}

/// Map a `BossStage` to the corresponding `AgentState` for prompt projection.
fn stage_to_agent_state(stage: BossStage) -> AgentState {
    match stage {
        BossStage::Documentation => AgentState::Planning,
        BossStage::WaitingForApproval => AgentState::Blocked,
        BossStage::Execution => AgentState::Executing,
        BossStage::Completed => AgentState::Done,
    }
}

fn fact_line(name: &str, value: impl Into<String>) -> String {
    format!("fact: {name} {}", value.into())
}

fn summarize_list(items: &[String]) -> String {
    if items.is_empty() {
        "none".into()
    } else {
        items.join(" | ")
    }
}

fn build_permission_facts(step_id: usize, objective: &str, readonly_analysis: bool) -> Vec<String> {
    if readonly_analysis {
        return Vec::new();
    }
    let mut facts = Vec::new();
    for (idx, expectation) in extract_artifact_expectations(&current_task_contract_text(objective))
        .into_iter()
        .enumerate()
    {
        let path = sanitize_extracted_artifact_path(&expectation.path.to_string_lossy());
        facts.push(fact_line(
            &format!("permission_to_create_and_write:{path}"),
            format!(
                "ref=permission:step{step_id}:{idx} source=permission_scope source_event_id=permission-scope:{step_id}:{idx} freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=worker may create and write the declared target artifact path {path}"
            ),
        ));
    }
    facts
}

fn format_confidence(confidence_milli: u16) -> String {
    format!("{:.2}", confidence_milli as f32 / 1000.0)
}

fn push_none_recorded_unless_present(facts: &mut Vec<String>, fact_name: &str) {
    if !facts
        .iter()
        .any(|item| item.starts_with(&format!("fact: {fact_name} ")))
    {
        facts.push(fact_line(fact_name, "none recorded"));
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn step_contract_text_with_acceptance(step: &crate::core::boss_state::BossPlanStep) -> String {
    let mut text = current_task_contract_text(step.objective());
    if !step.acceptance.is_empty() {
        text.push('\n');
        text.push_str(&step.acceptance.join("\n"));
    }
    text
}

#[cfg_attr(not(test), allow(dead_code))]
fn first_declared_target_directory(
    declared_artifacts: &[DeclaredArtifactContract],
) -> Option<String> {
    if let Some(directory) = declared_artifacts
        .iter()
        .find(|artifact| artifact.kind == "directory")
    {
        return Some(
            sanitize_extracted_artifact_path(&directory.path)
                .trim_end_matches('/')
                .to_string(),
        );
    }
    declared_artifacts
        .iter()
        .find(|artifact| !artifact.path.trim().is_empty())
        .map(|artifact| {
            let path = sanitize_extracted_artifact_path(&artifact.path);
            path.rsplit_once('/')
                .map(|(parent, _)| parent.to_string())
                .unwrap_or(path)
        })
}

#[cfg_attr(not(test), allow(dead_code))]
fn first_absolute_path_token(text: &str) -> Option<String> {
    let start = text
        .char_indices()
        .find_map(|(idx, ch)| (ch == '/').then_some(idx))?;
    let tail = &text[start..];
    let end = tail
        .char_indices()
        .find_map(|(idx, ch)| {
            (ch.is_whitespace()
                || matches!(
                    ch,
                    '`' | '"' | '\'' | '，' | '。' | '；' | '、' | ')' | '）' | ']' | '】'
                ))
            .then_some(idx)
        })
        .unwrap_or(tail.len());
    let path = tail[..end].trim().trim_end_matches(['.', ',', ':', ';']);
    (!path.is_empty()).then(|| path.to_string())
}

#[cfg_attr(not(test), allow(dead_code))]
fn first_step_target_directory(step: &crate::core::boss_state::BossPlanStep) -> Option<String> {
    let path = first_absolute_path_token(&step_contract_text_with_acceptance(step))?;
    if path
        .rsplit('/')
        .next()
        .is_some_and(|name| name.contains('.'))
    {
        path.rsplit_once('/').map(|(parent, _)| parent.to_string())
    } else {
        Some(path.trim_end_matches('/').to_string())
    }
}

fn join_contract_refs(refs: &[String]) -> String {
    if refs.is_empty() {
        "none".into()
    } else {
        refs.join("|")
    }
}

fn build_completion_contract_fact(
    permission_facts: &[String],
    artifact_ledgers: &[crate::core::state_fact_ledger::ArtifactRecord],
    stage_execution_contract: &StageExecutionContract,
    readonly_analysis: bool,
) -> String {
    let artifact_required = !readonly_analysis
        && (!permission_facts.is_empty()
            || !artifact_ledgers.is_empty()
            || !stage_execution_contract.declared_artifacts.is_empty());
    let mut artifact_refs = artifact_ledgers
        .iter()
        .map(|item| item.ref_id.clone())
        .collect::<Vec<_>>();
    if artifact_refs.is_empty() {
        artifact_refs.extend(
            stage_execution_contract
                .declared_artifacts
                .iter()
                .map(|item| item.ref_id.clone()),
        );
    }
    if artifact_refs.is_empty() {
        artifact_refs.extend(permission_facts.iter().filter_map(|line| {
            line.strip_prefix("fact: permission_to_create_and_write:")
                .map(|rest| rest.split_once(' ').map(|(path, _)| path).unwrap_or(rest))
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_string)
        }));
    }
    let mut test_refs = Vec::new();
    for test in &stage_execution_contract.tests {
        if !test_refs.iter().any(|existing| existing == &test.name) {
            test_refs.push(test.name.clone());
        }
    }
    let st_test_only_mode = stage_execution_contract
        .tests
        .iter()
        .any(|test| test.name == "st_auto_validation");
    let verification_refs = if readonly_analysis || st_test_only_mode {
        Vec::new()
    } else {
        stage_execution_contract
            .verifications
            .iter()
            .map(|item| item.target_ref.clone())
            .collect::<Vec<_>>()
    };
    let test_required = !test_refs.is_empty();
    let verification_required = !verification_refs.is_empty();
    if st_test_only_mode {
        fact_line(
            "completion_contract",
            format!(
                "artifact_evidence={} artifact_refs={} test_evidence={} test_refs={}",
                if artifact_required {
                    "required"
                } else {
                    "not_required"
                },
                join_contract_refs(&artifact_refs),
                if test_required {
                    "required"
                } else {
                    "not_required"
                },
                join_contract_refs(&test_refs),
            ),
        )
    } else {
        fact_line(
            "completion_contract",
            format!(
                "artifact_evidence={} artifact_refs={} test_evidence={} test_refs={} verification_evidence={} verification_refs={}",
                if artifact_required {
                    "required"
                } else {
                    "not_required"
                },
                join_contract_refs(&artifact_refs),
                if test_required {
                    "required"
                } else {
                    "not_required"
                },
                join_contract_refs(&test_refs),
                if verification_required {
                    "required"
                } else {
                    "not_required"
                },
                join_contract_refs(&verification_refs)
            ),
        )
    }
}

fn build_stage_execution_contract(
    step: Option<&crate::core::boss_state::BossPlanStep>,
    permission_facts: &[String],
    file_facts: &[crate::core::state_fact_ledger::FileFactRecord],
    artifact_ledgers: &[crate::core::state_fact_ledger::ArtifactRecord],
    _open_item_ledgers: &[crate::core::state_fact_ledger::OpenItemRecord],
    readonly_analysis: bool,
    st_mode_enabled: bool,
) -> StageExecutionContract {
    let explicit_contract = step.map(|step| &step.stage_execution_contract);
    let mut declared_artifacts = artifact_ledgers
        .iter()
        .map(|item| DeclaredArtifactContract {
            ref_id: item.ref_id.clone(),
            path: item.path.clone(),
            kind: item.kind.clone(),
            required_actions: if readonly_analysis {
                Vec::new()
            } else {
                vec!["create".into(), "write".into()]
            },
            required_evidence: vec![item.ref_id.clone(), item.path.clone(), item.kind.clone()],
        })
        .collect::<Vec<_>>();
    if let Some(step) = step {
        for (idx, expectation) in
            extract_artifact_expectations(&current_task_contract_text(step.objective()))
                .into_iter()
                .enumerate()
        {
            let path = sanitize_extracted_artifact_path(&expectation.path.to_string_lossy());
            if declared_artifacts.iter().any(|item| item.path == path) {
                continue;
            }
            let kind = match expectation.kind {
                crate::core::boss_acceptance::BossArtifactKind::File => "file",
                crate::core::boss_acceptance::BossArtifactKind::Directory => "directory",
            }
            .to_string();
            declared_artifacts.push(DeclaredArtifactContract {
                ref_id: format!("artifact:step{}:{idx}", step.id),
                path: path.clone(),
                kind: kind.clone(),
                required_actions: if readonly_analysis {
                    Vec::new()
                } else {
                    vec!["create".into(), "write".into()]
                },
                required_evidence: vec![format!("artifact:step{}:{idx}", step.id), path, kind],
            });
        }
    }
    for artifact in explicit_contract
        .into_iter()
        .flat_map(|contract| contract.declared_artifacts.iter())
    {
        if declared_artifacts
            .iter()
            .any(|existing| existing.path == artifact.path)
        {
            continue;
        }
        declared_artifacts.push(artifact.clone());
    }
    let st_test_only_mode =
        st_mode_enabled && step_looks_like_development_task(step, &declared_artifacts);
    let review_mode = infer_review_mode(step, readonly_analysis);
    let typed_independent_review_task = review_mode
        .is_some_and(|mode| mode.is_independent_review())
        && step
            .and_then(|step| step.stage_execution_contract.task_profile)
            .is_some_and(|profile| matches!(profile, TaskProfile::IndependentReview));
    let task_profile = step
        .and_then(|step| step.stage_execution_contract.task_profile)
        .or(readonly_analysis.then_some(TaskProfile::ReadOnlyAnalysis));
    let requires_source_evidence = step
        .and_then(|step| step.stage_execution_contract.requires_source_evidence)
        .or(typed_independent_review_task.then_some(false));
    let derived_verifications = || {
        artifact_ledgers
            .iter()
            .map(|item| (item.ref_id.clone(), item.path.clone()))
            .chain(
                declared_artifacts
                    .iter()
                    .map(|item| (item.ref_id.clone(), item.path.clone())),
            )
            .fold(Vec::<(String, String)>::new(), |mut acc, item| {
                if !acc.iter().any(|(ref_id, _)| ref_id == &item.0) {
                    acc.push(item);
                }
                acc
            })
            .into_iter()
            .map(|(target_ref, target_path)| VerificationContract {
                target_ref: target_ref.clone(),
                target_path: Some(target_path.clone()),
                required_actions: vec!["verify".into()],
                required_evidence: vec![target_ref, target_path],
            })
            .collect::<Vec<_>>()
    };
    let verifications = explicit_contract
        .map(|contract| contract.verifications.clone())
        .filter(|items| !items.is_empty())
        .or_else(|| {
            if st_test_only_mode || typed_independent_review_task {
                None
            } else {
                Some(derived_verifications())
            }
        })
        .unwrap_or_default();
    let tests = explicit_contract
        .map(|contract| contract.tests.clone())
        .unwrap_or_default();
    let mut required_actions = explicit_contract
        .map(|contract| contract.required_actions.clone())
        .unwrap_or_default();
    if step.is_some() && !declared_artifacts.is_empty() && !readonly_analysis {
        required_actions.extend(["create".into(), "write".into()]);
    }
    for test in &tests {
        required_actions.extend(test.required_actions.iter().cloned());
    }
    for verification in &verifications {
        required_actions.extend(verification.required_actions.iter().cloned());
    }
    sort_required_actions(&mut required_actions);
    let mut required_evidence = explicit_contract
        .map(|contract| contract.required_evidence.clone())
        .unwrap_or_default();
    required_evidence.extend(permission_facts.iter().cloned());
    required_evidence.extend(
        declared_artifacts
            .iter()
            .flat_map(|item| item.required_evidence.iter().cloned()),
    );
    required_evidence.extend(
        verifications
            .iter()
            .flat_map(|item| item.required_evidence.iter().cloned()),
    );
    required_evidence.extend(
        tests
            .iter()
            .flat_map(|item| item.required_evidence.iter().cloned()),
    );
    required_evidence.sort();
    required_evidence.dedup();
    let artifact_paths = declared_artifacts
        .iter()
        .map(|artifact| artifact.path.as_str())
        .collect::<Vec<_>>();
    let content_evidence_targets = if requires_source_evidence == Some(false) {
        Vec::new()
    } else {
        let mut targets = explicit_contract
            .map(|contract| contract.content_evidence_targets.clone())
            .unwrap_or_default();
        for path in file_facts
            .iter()
            .filter(|item| matches!(item.kind.as_str(), "source_file" | "document"))
            .map(|item| item.path.trim())
            .filter(|path| {
                !path.is_empty()
                    && !path.ends_with('/')
                    && !artifact_paths.iter().any(|artifact| *artifact == *path)
            })
        {
            if !targets.iter().any(|existing| existing == path) {
                targets.push(path.to_string());
            }
        }
        targets
    };
    let mut contract = StageExecutionContract {
        review_mode,
        task_profile,
        requires_source_evidence,
        declared_artifacts,
        verifications,
        tests,
        content_evidence_targets,
        required_actions,
        required_evidence,
    };
    if st_test_only_mode {
        apply_development_test_policy(&mut contract);
    }
    contract
}

fn build_stage_contract_facts(contract: &StageExecutionContract) -> Vec<String> {
    let mut facts = Vec::new();
    if let Some(review_mode) = contract.review_mode {
        facts.push(fact_line("review_mode", review_mode.as_str()));
    }
    if let Some(task_profile) = contract.task_profile {
        facts.push(fact_line("task_profile", task_profile.as_str()));
        if matches!(task_profile, TaskProfile::ReadOnlyAnalysis) {
            facts.push(fact_line(
                "execution_mode",
                "read_only_analysis no_file_edits no_patch",
            ));
        }
    }
    if let Some(requires_source_evidence) = contract.requires_source_evidence {
        facts.push(fact_line(
            "requires_source_evidence",
            if requires_source_evidence {
                "required"
            } else {
                "not_required"
            },
        ));
    }
    for artifact in &contract.declared_artifacts {
        facts.push(fact_line(
            "declared_artifact_contract",
            format!(
                "ref={} path={} kind={} required_actions={} required_evidence={}",
                artifact.ref_id,
                artifact.path,
                artifact.kind,
                summarize_list(&artifact.required_actions),
                summarize_list(&artifact.required_evidence)
            ),
        ));
    }
    for verification in &contract.verifications {
        facts.push(fact_line(
            "verification_contract",
            format!(
                "target_ref={} target_path={} required_actions={} required_evidence={}",
                verification.target_ref,
                verification.target_path.as_deref().unwrap_or("none"),
                summarize_list(&verification.required_actions),
                summarize_list(&verification.required_evidence)
            ),
        ));
    }
    for test in &contract.tests {
        facts.push(fact_line(
            "test_contract",
            format!(
                "name={} required_actions={} required_evidence={}",
                test.name,
                summarize_list(&test.required_actions),
                summarize_list(&test.required_evidence)
            ),
        ));
    }
    if contract
        .tests
        .iter()
        .any(|test| test.name == "st_auto_validation")
    {
        facts.push(fact_line(
            "st_mode",
            "enabled test_first_validation=required",
        ));
    }
    if !contract.required_actions.is_empty() {
        facts.push(fact_line(
            "required_actions",
            summarize_list(&contract.required_actions),
        ));
    }
    if !contract.required_evidence.is_empty() {
        facts.push(fact_line(
            "required_evidence",
            summarize_list(&contract.required_evidence),
        ));
    }
    facts
}

fn build_stage_continuation_fact_lines_with_history(
    context: &StageContinuationContext,
    include_verified_facts: bool,
) -> Vec<String> {
    let next_action = context.next_action.as_deref().unwrap_or("none");
    let failed_target = context.failed_target.as_deref().unwrap_or("none");
    let continuity_mode = context
        .continuity_mode
        .as_ref()
        .map(|mode| format!("{mode:?}").to_ascii_lowercase())
        .unwrap_or_else(|| "none".into());
    let mut facts = vec![fact_line(
        "stage_continuation",
        format!(
            "continuity_mode={} next_action={} failed_target={} verified_facts={}",
            continuity_mode,
            next_action,
            failed_target,
            if include_verified_facts {
                summarize_list(&context.verified_facts)
            } else {
                "none".into()
            }
        ),
    )];
    if next_action == "read_source_evidence" && failed_target != "none" {
        facts.push(fact_line(
            "missing_source_evidence",
            format!(
                "target_path={} required_action=read_source_evidence summary=read this source file before verifying the output artifact",
                failed_target
            ),
        ));
    }
    if let Some(intent) = context.repair_intent.as_ref() {
        let intent_next_action = intent.next_action.as_deref().unwrap_or(next_action);
        let intent_failed_target = intent.failed_target.as_deref().unwrap_or(failed_target);
        if intent_next_action == "worker_correction" {
            facts.push(fact_line(
                "worker_correction",
                format!(
                    "target_path={} required_action=worker_correction verified_facts={} summary=apply Designer A correction and close missing artifact/read evidence",
                    intent_failed_target,
                    summarize_list(&intent.verified_facts)
                ),
            ));
        }
    }
    facts
}

fn build_stage_continuation_open_items(context: &StageContinuationContext) -> Vec<String> {
    if context
        .repair_intent
        .as_ref()
        .and_then(|intent| intent.next_action.as_deref())
        == Some("worker_correction")
        || context.next_action.as_deref() == Some("worker_correction")
    {
        let facts = context
            .repair_intent
            .as_ref()
            .map(|intent| intent.verified_facts.as_slice())
            .unwrap_or(context.verified_facts.as_slice());
        let mut items = Vec::new();
        for fact in facts {
            let Some((_, tail)) = fact.split_once("required_evidence_targets:") else {
                continue;
            };
            for target in tail.split('|') {
                let target = target.trim();
                if target.is_empty()
                    || target == "none"
                    || target.starts_with("failure_reason:")
                    || target.starts_with("modification_direction:")
                    || target.starts_with("remaining_blocker:")
                {
                    continue;
                }
                items.push(format!("create missing artifact: {target}"));
                items.push(format!("provide read evidence: read:{target}"));
            }
        }
        if items.is_empty() {
            if let Some(target) = context
                .repair_intent
                .as_ref()
                .and_then(|intent| intent.failed_target.as_deref())
                .or(context.failed_target.as_deref())
                .filter(|target| !target.trim().is_empty())
            {
                items.push(format!("create missing artifact: {target}"));
                items.push(format!("provide read evidence: read:{target}"));
            }
        }
        items.sort();
        items.dedup();
        return items;
    }
    if context.next_action.as_deref() != Some("read_source_evidence") {
        return Vec::new();
    }
    context
        .failed_target
        .as_deref()
        .filter(|target| !target.trim().is_empty())
        .map(|target| {
            vec![format!(
                "required_action:read_source_evidence target_path={} reason=content evidence source has not been read",
                target
            )]
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionDiagnostics {
    pub mismatch_count: usize,
    pub warnings: Vec<String>,
}

fn has_none_recorded_fact(facts: &[String], fact_name: &str) -> bool {
    facts
        .iter()
        .any(|item| item == &format!("fact: {fact_name} none recorded"))
}

fn has_ref_fact(facts: &[String], fact_name: &str) -> bool {
    facts
        .iter()
        .any(|item| item.starts_with(&format!("fact: {fact_name} ")) && item.contains(" ref="))
}

fn collect_fact_refs(facts: &[String], fact_name: &str) -> Vec<String> {
    facts
        .iter()
        .filter(|item| item.starts_with(&format!("fact: {fact_name} ")))
        .filter_map(|item| {
            item.split_whitespace().find_map(|part| {
                part.strip_prefix("ref=")
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
        .collect()
}

fn collect_fact_field_values(facts: &[String], fact_name: &str, field_name: &str) -> Vec<String> {
    facts
        .iter()
        .filter(|item| item.starts_with(&format!("fact: {fact_name} ")))
        .filter_map(|item| {
            item.split_whitespace().find_map(|part| {
                part.strip_prefix(&format!("{field_name}="))
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty() && value != "none")
            })
        })
        .collect()
}

fn collect_all_ref_ids(facts: &[String]) -> Vec<String> {
    facts
        .iter()
        .filter(|item| item.starts_with("fact: "))
        .filter_map(|item| {
            item.split_whitespace().find_map(|part| {
                part.strip_prefix("ref=")
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
        .collect()
}

fn split_ref_list(value: &str) -> Vec<String> {
    value
        .split('|')
        .map(str::trim)
        .filter(|item| !item.is_empty() && *item != "none" && *item != "none recorded")
        .map(str::to_string)
        .collect()
}

pub fn collect_projection_diagnostics(frame: &StateFrame) -> ProjectionDiagnostics {
    let mut warnings = Vec::new();
    let all_refs = collect_all_ref_ids(&frame.recent_evidence);

    if !frame.open_items.is_empty()
        && has_none_recorded_fact(&frame.recent_evidence, "open_item_refs")
    {
        warnings.push("open_items present but open_item_refs projected as none recorded".into());
    }
    if !frame.blocked_items.is_empty()
        && has_none_recorded_fact(&frame.recent_evidence, "blocker_refs")
    {
        warnings.push("blocked_items present but blocker_refs projected as none recorded".into());
    }
    if has_ref_fact(&frame.recent_evidence, "rejected_approaches") {
        let review_refs = collect_fact_refs(&frame.recent_evidence, "review_verdicts");
        for source_ref in
            collect_fact_field_values(&frame.recent_evidence, "rejected_approaches", "source_ref")
        {
            if !review_refs.iter().any(|item| item == &source_ref) {
                warnings.push(format!(
                    "rejected_approaches source_ref missing in review_verdicts: {source_ref}"
                ));
            }
        }
    }
    for fact_name in [
        "file_facts",
        "test_failures",
        "recent_changes_in_files",
        "review_verdicts",
        "artifact_status",
        "open_item_refs",
        "blocker_refs",
        "rejected_approaches",
    ] {
        for field_name in ["invalidated_by", "supersedes", "conflicts_with"] {
            for value in collect_fact_field_values(&frame.recent_evidence, fact_name, field_name) {
                for ref_id in split_ref_list(&value) {
                    if !all_refs.iter().any(|item| item == &ref_id) {
                        warnings.push(format!(
                            "{fact_name} {field_name} points to missing ref: {ref_id}"
                        ));
                    }
                }
            }
        }
    }
    for fact_name in [
        "file_facts",
        "test_failures",
        "recent_changes_in_files",
        "review_verdicts",
        "artifact_status",
        "open_item_refs",
        "blocker_refs",
        "rejected_approaches",
    ] {
        let has_ref = has_ref_fact(&frame.recent_evidence, fact_name);
        let has_none = has_none_recorded_fact(&frame.recent_evidence, fact_name);
        if has_ref && has_none {
            warnings.push(format!(
                "{fact_name} contains both ref-backed facts and none recorded sentinel"
            ));
        }
    }

    ProjectionDiagnostics {
        mismatch_count: warnings.len(),
        warnings,
    }
}

fn build_fact_ledger(
    plan: &BossPlan,
    stage: BossStage,
    step_id: Option<usize>,
    open_items: &[String],
    blocked_items: &[String],
    readonly_analysis: bool,
    st_mode_enabled: bool,
    blind_review: bool,
) -> Vec<String> {
    let current_step = step_id.and_then(|id| plan.steps.iter().find(|s| s.id == id));
    let mut facts = vec![fact_line(
        "immutable_plan",
        format!(
            "plan_id={} accepted_by_user={} auto_sequence={} step_count={} stage={stage:?}",
            plan.plan_id,
            plan.accepted_by_user,
            plan.auto_sequence,
            plan.steps.len()
        ),
    )];

    if let Some(step) = current_step {
        let ledgers = build_step_fact_ledgers_with_mode(step, blind_review);
        let open_item_ledgers = build_open_item_records(step, open_items);
        let blocker_ledgers = build_blocker_records(Some(step), stage, blocked_items);
        let rejected_ledgers = build_rejected_approach_records(step, &ledgers.review_refs);
        facts.push(fact_line(
            "current_step",
            format!(
                "id={} status={:?} requires_approval={} attempt_count={} retry_budget={}",
                step.id, step.status, step.requires_approval, step.attempt_count, step.retry_budget
            ),
        ));
        facts.push(fact_line(
            "accepted_constraints",
            summarize_list(
                &step
                    .acceptance
                    .iter()
                    .map(|item| current_task_contract_text(item))
                    .collect::<Vec<_>>(),
            ),
        ));
        facts.push(fact_line(
            "reject_correction",
            if blind_review {
                "none recorded"
            } else {
                step.last_correction
                    .as_deref()
                    .or(step.last_review_summary.as_deref())
                    .unwrap_or("none recorded")
            },
        ));
        facts.push(fact_line(
            "recent_diff",
            if blind_review {
                "none recorded"
            } else {
                step.result_diff.as_deref().unwrap_or("none recorded")
            },
        ));
        let permission_facts = build_permission_facts(
            step.id,
            &current_task_contract_text(step.objective()),
            readonly_analysis,
        );
        facts.extend(permission_facts.iter().cloned());
        if !ledgers.file_facts.is_empty() {
            for item in &ledgers.file_facts {
                facts.push(fact_line(
                    "file_facts",
                    format!(
                        "ref={} path={} kind={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={}{} fact={}",
                        item.ref_id,
                        item.path,
                        item.kind,
                        item.source,
                        item.source_event_id,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.lineage.status,
                        summarize_list(&item.lineage.invalidated_by),
                        summarize_list(&item.lineage.supersedes),
                        summarize_list(&item.lineage.conflicts_with),
                        item.symbol
                            .as_deref()
                            .map(|symbol| format!(" symbol={symbol}"))
                            .unwrap_or_default(),
                        item.fact
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "file_facts");
        if !ledgers.test_refs.is_empty() {
            for item in ledgers.test_refs {
                facts.push(fact_line(
                    "test_failures",
                    format!(
                        "ref={} name={} status={} source={} source_event_id={} freshness={} confidence={} lineage_status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
                        item.ref_id,
                        item.name,
                        item.status,
                        item.source,
                        item.source_event_id,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.lineage.status,
                        summarize_list(&item.lineage.invalidated_by),
                        summarize_list(&item.lineage.supersedes),
                        summarize_list(&item.lineage.conflicts_with),
                        item.summary
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "test_failures");
        if !ledgers.change_refs.is_empty() {
            for item in ledgers.change_refs {
                facts.push(fact_line(
                    "recent_changes_in_files",
                    format!(
                        "ref={} path={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
                        item.ref_id,
                        item.path,
                        item.source,
                        item.source_event_id,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.lineage.status,
                        summarize_list(&item.lineage.invalidated_by),
                        summarize_list(&item.lineage.supersedes),
                        summarize_list(&item.lineage.conflicts_with),
                        item.summary
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "recent_changes_in_files");
        if !blind_review && !ledgers.review_refs.is_empty() {
            for item in &ledgers.review_refs {
                facts.push(fact_line(
                    "review_verdicts",
                    format!(
                        "ref={} verdict={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={} summary={}{}",
                        item.ref_id,
                        item.verdict,
                        item.source,
                        item.source_event_id,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.lineage.status,
                        summarize_list(&item.lineage.invalidated_by),
                        summarize_list(&item.lineage.supersedes),
                        summarize_list(&item.lineage.conflicts_with),
                        item.summary,
                        item.correction
                            .as_deref()
                            .map(|correction| format!(" correction={correction}"))
                            .unwrap_or_default()
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "review_verdicts");
        if !blind_review && !ledgers.artifact_refs.is_empty() {
            for item in &ledgers.artifact_refs {
                facts.push(fact_line(
                    "artifact_status",
                    format!(
                        "ref={} path={} kind={} status={} source={} source_event_id={} freshness={} confidence={} lineage_status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
                        item.ref_id,
                        item.path,
                        item.kind,
                        item.status,
                        item.source,
                        item.source_event_id,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.lineage.status,
                        summarize_list(&item.lineage.invalidated_by),
                        summarize_list(&item.lineage.supersedes),
                        summarize_list(&item.lineage.conflicts_with),
                        item.summary
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "artifact_status");
        for item in &open_item_ledgers {
            facts.push(fact_line(
                "open_item_refs",
                format!(
                    "ref={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
                    item.ref_id,
                    item.source,
                    item.source_event_id,
                    item.freshness,
                    format_confidence(item.confidence_milli),
                    item.lineage.status,
                    summarize_list(&item.lineage.invalidated_by),
                    summarize_list(&item.lineage.supersedes),
                    summarize_list(&item.lineage.conflicts_with),
                    item.summary
                ),
            ));
        }
        push_none_recorded_unless_present(&mut facts, "open_item_refs");
        for item in &blocker_ledgers {
            facts.push(fact_line(
                "blocker_refs",
                format!(
                    "ref={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
                    item.ref_id,
                    item.source,
                    item.source_event_id,
                    item.freshness,
                    format_confidence(item.confidence_milli),
                    item.lineage.status,
                    summarize_list(&item.lineage.invalidated_by),
                    summarize_list(&item.lineage.supersedes),
                    summarize_list(&item.lineage.conflicts_with),
                    item.summary
                ),
            ));
        }
        push_none_recorded_unless_present(&mut facts, "blocker_refs");
        if !blind_review {
            for item in &rejected_ledgers {
                facts.push(fact_line(
                "rejected_approaches",
                format!(
                    "ref={} source={}{} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={} summary={}{}",
                    item.ref_id,
                    item.source,
                    item.source_ref
                        .as_deref()
                        .map(|source_ref| format!(" source_ref={source_ref}"))
                        .unwrap_or_default(),
                    item.source_event_id,
                    item.freshness,
                    format_confidence(item.confidence_milli),
                    item.lineage.status,
                    summarize_list(&item.lineage.invalidated_by),
                    summarize_list(&item.lineage.supersedes),
                    summarize_list(&item.lineage.conflicts_with),
                    item.summary,
                    item.correction
                        .as_deref()
                        .map(|correction| format!(" correction={correction}"))
                        .unwrap_or_default()
                ),
            ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "rejected_approaches");
        let stage_execution_contract = build_stage_execution_contract(
            current_step,
            &permission_facts,
            &ledgers.file_facts,
            &ledgers.artifact_refs,
            &open_item_ledgers,
            readonly_analysis,
            st_mode_enabled,
        );
        let completion_contract_readonly = readonly_analysis
            && !independent_review_requires_runtime_verification(&stage_execution_contract);
        facts.extend(build_stage_contract_facts(&stage_execution_contract));
        facts.push(build_completion_contract_fact(
            &permission_facts,
            &ledgers.artifact_refs,
            &stage_execution_contract,
            completion_contract_readonly,
        ));
    } else {
        facts.push(fact_line("accepted_constraints", "none recorded"));
        facts.push(fact_line("reject_correction", "none recorded"));
        facts.push(fact_line("recent_diff", "none recorded"));
        facts.push(fact_line("file_facts", "none recorded"));
        facts.push(fact_line("test_failures", "none recorded"));
        facts.push(fact_line("recent_changes_in_files", "none recorded"));
        facts.push(fact_line("review_verdicts", "none recorded"));
        facts.push(fact_line("artifact_status", "none recorded"));
        facts.push(fact_line("open_item_refs", "none recorded"));
        facts.push(fact_line("blocker_refs", "none recorded"));
        facts.push(fact_line("rejected_approaches", "none recorded"));
        facts.push(fact_line(
            "completion_contract",
            "artifact_evidence=not_required artifact_refs=none test_evidence=not_required test_refs=none verification_evidence=not_required verification_refs=none",
        ));
    }

    facts.push(fact_line(
        "open_blockers",
        if blocked_items.is_empty() {
            "none".into()
        } else {
            summarize_list(blocked_items)
        },
    ));
    facts.push(fact_line(
        "open_items",
        if open_items.is_empty() {
            "none".into()
        } else {
            summarize_list(open_items)
        },
    ));
    facts.push(fact_line("dangerous_assumptions", "none recorded"));
    facts.push(fact_line(
        "review_feedback",
        if blind_review {
            "none recorded"
        } else {
            plan.review_feedback.as_deref().unwrap_or("none recorded")
        },
    ));
    facts.push(fact_line(
        "revision_notes",
        if blind_review {
            "none recorded"
        } else {
            plan.revision_notes.as_deref().unwrap_or("none recorded")
        },
    ));
    facts.push(fact_line(
        "documentation_feedback",
        if blind_review {
            "none recorded".into()
        } else if plan.documentation_feedback.is_empty() {
            "none recorded".into()
        } else {
            summarize_list(&plan.documentation_feedback)
        },
    ));
    if readonly_analysis {
        facts.push(fact_line(
            "execution_mode",
            "read_only_analysis no_file_edits no_patch",
        ));
    }
    facts
}

/// Project a `StateFrame` from a `BossPlan`, the current `BossStage`, an optional step id,
/// and the target actor role.
///
/// Pure function — no side effects, no LLM calls, no state mutation.
/// Uses `state_frame_archive` for accepted_summary / open_items / blocked_items.
pub fn project_state_frame(
    plan: &BossPlan,
    stage: BossStage,
    step_id: Option<usize>,
    role: ActorRole,
) -> StateFrame {
    project_state_frame_with_st_mode(plan, stage, step_id, role, false)
}

pub fn project_state_frame_with_st_mode(
    plan: &BossPlan,
    stage: BossStage,
    step_id: Option<usize>,
    role: ActorRole,
    st_mode_enabled: bool,
) -> StateFrame {
    let mut state = stage_to_agent_state(stage);
    let readonly_analysis = is_readonly_analysis(plan, step_id);
    let current_step = step_id.and_then(|id| plan.steps.iter().find(|s| s.id == id));
    let blind_review_candidate = infer_review_mode(current_step, readonly_analysis)
        .is_some_and(|mode| mode.is_independent_review())
        && !has_source_evidence_continuation(current_step)
        && !has_worker_correction_continuation(current_step);

    // Build archive of completed steps (excluding current step).
    let archive = build_accepted_archive(plan, step_id);

    // objective: current step objective if available, else plan task description.
    let objective = projection_objective_text(current_step, &plan.task_description);

    // open_items: unsatisfied acceptance criteria of the current step.
    let mut open_items = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .filter(|s| !s.completed)
        .map(|s| retain_open_items(&s.acceptance, &archive))
        .unwrap_or_default();
    if !blind_review_candidate {
        if let Some(context) =
            current_step.and_then(|step| step.stage_continuation_context.as_ref())
        {
            for item in build_stage_continuation_open_items(context) {
                if !open_items.iter().any(|existing| existing == &item) {
                    open_items.push(item);
                }
            }
        }
    }

    // blocked_items: stage-driven via archive.
    let blocked_items = retain_blocked_items(stage, &archive);

    // accepted_summary: rendered from archive.
    let mut accepted_summary = archive_to_summary(&archive);
    let permission_facts = current_step
        .map(|step| {
            build_permission_facts(
                step.id,
                &current_task_contract_text(step.objective()),
                readonly_analysis,
            )
        })
        .unwrap_or_default();
    let ledgers =
        current_step.map(|step| build_step_fact_ledgers_with_mode(step, blind_review_candidate));
    let open_item_ledgers = current_step
        .map(|step| build_open_item_records(step, &open_items))
        .unwrap_or_default();
    let stage_execution_contract = build_stage_execution_contract(
        current_step,
        &permission_facts,
        ledgers
            .as_ref()
            .map(|value| value.file_facts.as_slice())
            .unwrap_or(&[]),
        ledgers
            .as_ref()
            .map(|value| value.artifact_refs.as_slice())
            .unwrap_or(&[]),
        &open_item_ledgers,
        readonly_analysis,
        st_mode_enabled,
    );
    let independent_review = stage_execution_contract
        .review_mode
        .is_some_and(|mode| mode.is_independent_review());
    let source_evidence_continuation = has_source_evidence_continuation(current_step);
    let worker_correction_continuation = has_worker_correction_continuation(current_step);
    if independent_review {
        accepted_summary.clear();
    }
    let independent_review_runtime_verification =
        independent_review_requires_runtime_verification(&stage_execution_contract);
    if independent_review_runtime_verification {
        state = AgentState::Verifying;
    }
    let readonly_audit_contract = readonly_analysis && !independent_review_runtime_verification;
    if independent_review_runtime_verification
        && !open_items
            .iter()
            .any(|item| item.starts_with("required_action:verify_artifact"))
    {
        let verification_targets = readable_verification_targets(&stage_execution_contract);
        if !verification_targets.is_empty() {
            open_items.push(format!(
                "required_action:verify_artifact target_refs={}",
                verification_targets.join("|")
            ));
        }
    }

    // recent_evidence doubles as a compact Fact Ledger v1.
    let mut recent_evidence = build_stage_contract_facts(&stage_execution_contract);
    recent_evidence.extend(build_fact_ledger(
        plan,
        stage,
        step_id,
        &open_items,
        &blocked_items,
        readonly_analysis,
        st_mode_enabled,
        independent_review,
    ));
    if let Some(step) = step_id.and_then(|id| plan.steps.iter().find(|s| s.id == id)) {
        if !independent_review {
            if let Some(r) = &step.last_review_summary {
                recent_evidence.push(format!("review: {r}"));
            }
            if let Some(c) = &step.last_correction {
                recent_evidence.push(format!("correction: {c}"));
            }
        }
        if !independent_review || source_evidence_continuation || worker_correction_continuation {
            if let Some(context) = step.stage_continuation_context.as_ref() {
                recent_evidence.extend(build_stage_continuation_fact_lines_with_history(
                    context,
                    !independent_review,
                ));
            }
        }
    }

    let mut frame = StateFrame {
        role,
        state,
        objective,
        stage_execution_contract,
        open_items,
        blocked_items,
        accepted_summary,
        recent_evidence,
        allowed_actions: Vec::new(),
        allowed_tools: Vec::new(),
        toolset_id: None,
        skillset_id: None,
        required_output_schema: Some(if readonly_audit_contract {
            "readonly_audit_4_paragraphs_v1".into()
        } else {
            "state_decision_v1".into()
        }),
        budget: StateBudget::default(),
        runtime_open_items: Vec::new(),
    };
    let route = route_toolset(&frame);
    apply_route(&mut frame, route);
    let diagnostics = collect_projection_diagnostics(&frame);
    frame.recent_evidence.push(fact_line(
        "projection_invariants",
        format!("mismatch_count={}", diagnostics.mismatch_count),
    ));
    for warning in diagnostics.warnings {
        frame.recent_evidence.push(fact_line(
            "projection_invariants",
            format!("warning={warning}"),
        ));
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::{
        collect_projection_diagnostics, project_state_frame, project_state_frame_with_st_mode,
    };
    use crate::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use crate::core::state_frame::{
        ActorRole, AgentState, ContinuityMode, ReviewMode, StageContinuationContext,
        StageExecutionContract, StateBudget, StateFrame, TaskProfile, TestContract,
        VerificationContract,
    };

    #[test]
    fn projection_diagnostics_flags_open_items_without_refs_and_missing_review_source_ref() {
        let frame = StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "fix worker context".into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: open_item_refs none recorded".into(),
                "fact: review_verdicts ref=review:step1:0 verdict=accepted source=tool:BossReview source_event_id=tool-review:1:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=ok".into(),
                "fact: rejected_approaches ref=rejected:step1:0 source=review_correction source_ref=review:step1:missing source_event_id=review-correction:1 freshness=after-review confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=review:step1:missing summary=bad path".into(),
            ],
            allowed_actions: vec![],
            allowed_tools: vec![],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
            runtime_open_items: Vec::new(),
        };

        let diagnostics = collect_projection_diagnostics(&frame);
        assert_eq!(diagnostics.mismatch_count, 3);
        assert!(
            diagnostics
                .warnings
                .iter()
                .any(|item| item.contains("open_items present but open_item_refs"))
        );
        assert!(
            diagnostics
                .warnings
                .iter()
                .any(|item| item.contains("source_ref missing in review_verdicts"))
        );
    }

    #[test]
    fn projection_diagnostics_flags_missing_lineage_refs() {
        let frame = StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "check lineage".into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: Vec::new(),
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: file_facts ref=filefact:1 path=src/lib.rs kind=target_file source=step_objective source_event_id=step-objective:1 freshness=current confidence=1.00 status=active invalidated_by=review:missing supersedes=change:missing conflicts_with=none symbol=Lib fact=target".into(),
                "fact: blocker_refs ref=blocker:1 kind=blocked_by_review source=step_runtime source_event_id=step-runtime:1 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=artifact:missing summary=waiting".into(),
            ],
            allowed_actions: vec![],
            allowed_tools: vec![],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
            runtime_open_items: Vec::new(),
        };

        let diagnostics = collect_projection_diagnostics(&frame);
        assert!(
            diagnostics
                .warnings
                .iter()
                .any(|item| item.contains("invalidated_by points to missing ref: review:missing"))
        );
        assert!(
            diagnostics
                .warnings
                .iter()
                .any(|item| item.contains("supersedes points to missing ref: change:missing"))
        );
        assert!(
            diagnostics
                .warnings
                .iter()
                .any(|item| item.contains("conflicts_with points to missing ref: artifact:missing"))
        );
    }

    #[test]
    fn static_site_entrypoint_must_come_from_explicit_contract_not_objective_prose() {
        let plan = BossPlan {
            plan_id: "plan-1".into(),
            task_description: "build site".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "site".into(),
                objective: Some(
                    "在目标目录创建一个可直接打开的静态网站：\n- 目标目录：/tmp/demo-site".into(),
                ),
                acceptance: vec!["write README".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::CodeChange),
                    declared_artifacts: vec![
                        crate::core::state_frame::DeclaredArtifactContract {
                            ref_id: "artifact:site:dir".into(),
                            path: "/tmp/demo-site".into(),
                            kind: "directory".into(),
                            required_actions: vec!["create".into(), "write".into()],
                            required_evidence: vec!["artifact:site:dir".into()],
                        },
                        crate::core::state_frame::DeclaredArtifactContract {
                            ref_id: "artifact:site:index".into(),
                            path: "/tmp/demo-site/index.html".into(),
                            kind: "file".into(),
                            required_actions: vec!["create".into(), "write".into()],
                            required_evidence: vec!["artifact:site:index".into()],
                        },
                        crate::core::state_frame::DeclaredArtifactContract {
                            ref_id: "artifact:site:readme".into(),
                            path: "/tmp/demo-site/README.md".into(),
                            kind: "file".into(),
                            required_actions: vec!["create".into(), "write".into()],
                            required_evidence: vec!["artifact:site:readme".into()],
                        },
                    ],
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
        assert!(
            !frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("fact: preferred_deployment_mode"))
        );
        assert!(
            frame.recent_evidence.iter().any(|item| {
                item.contains("fact: permission_to_create_and_write:/tmp/demo-site")
            })
        );
        let artifact_paths = frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .map(|artifact| artifact.path.as_str())
            .collect::<Vec<_>>();
        assert!(artifact_paths.contains(&"/tmp/demo-site"));
        assert!(artifact_paths.contains(&"/tmp/demo-site/index.html"));
        assert!(artifact_paths.contains(&"/tmp/demo-site/README.md"));
        assert_eq!(frame.stage_execution_contract.declared_artifacts.len(), 3);
        assert_eq!(frame.stage_execution_contract.verifications.len(), 3);
        assert_eq!(
            frame.stage_execution_contract.required_actions,
            vec!["create", "write", "verify"]
        );
    }

    #[test]
    fn explicit_tool_contract_projects_analyzer_report_and_runtime_test() {
        let plan = BossPlan {
            plan_id: "plan-local-tool".into(),
            task_description: "build jsonl analyzer".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "jsonl analyzer".into(),
                objective: Some(
                    "真实 /boss use case 9：创建一个 JSONL 分析工具。\n\n任务目标：\n- 在目标目录实现一个小工具：\n  - 目标目录：/tmp/lism-jsonl-analyzer\n- 工具输出：\n  - 生成 markdown 报告：/tmp/lism-jsonl-analyzer/report.md\n- 要求：\n  - Python 标准库优先\n  - 需要实际运行一次工具并汇报结果"
                        .into(),
                ),
                acceptance: vec!["tool runs".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::CodeChange),
                    declared_artifacts: vec![
                        crate::core::state_frame::DeclaredArtifactContract {
                            ref_id: "artifact:u9:target_dir".into(),
                            path: "/tmp/lism-jsonl-analyzer".into(),
                            kind: "directory".into(),
                            required_actions: vec!["create".into(), "write".into()],
                            required_evidence: vec!["artifact:u9:target_dir".into()],
                        },
                        crate::core::state_frame::DeclaredArtifactContract {
                            ref_id: "artifact:u9:analyzer".into(),
                            path: "/tmp/lism-jsonl-analyzer/analyze.py".into(),
                            kind: "file".into(),
                            required_actions: vec!["create".into(), "write".into()],
                            required_evidence: vec!["artifact:u9:analyzer".into()],
                        },
                        crate::core::state_frame::DeclaredArtifactContract {
                            ref_id: "artifact:u9:report".into(),
                            path: "/tmp/lism-jsonl-analyzer/report.md".into(),
                            kind: "file".into(),
                            required_actions: vec!["create".into(), "write".into()],
                            required_evidence: vec!["artifact:u9:report".into()],
                        },
                    ],
                    tests: vec![TestContract {
                        name: "u9_analyzer_runtime".into(),
                        required_actions: vec!["run_test".into()],
                        required_evidence: vec!["runtime_test_passed".into()],
                    }],
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
        let artifact_paths = frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .map(|artifact| artifact.path.as_str())
            .collect::<Vec<_>>();
        assert!(artifact_paths.contains(&"/tmp/lism-jsonl-analyzer"));
        assert!(artifact_paths.contains(&"/tmp/lism-jsonl-analyzer/analyze.py"));
        assert!(artifact_paths.contains(&"/tmp/lism-jsonl-analyzer/report.md"));
        assert!(frame.stage_execution_contract.tests.iter().any(|test| {
            test.name == "u9_analyzer_runtime"
                && test
                    .required_evidence
                    .iter()
                    .any(|item| item == "runtime_test_passed")
        }));
        assert!(
            frame
                .stage_execution_contract
                .required_actions
                .iter()
                .any(|action| action == "run_test")
        );
    }

    #[test]
    fn implementation_objective_truncates_historical_reference_even_when_mistyped_review() {
        let plan = BossPlan {
            plan_id: "plan-reference-pollution".into(),
            task_description: "build site".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "site".into(),
                objective: Some(
                    "任务目标：\n- 目标目录：/tmp/demo-site\n- 创建可直接打开的静态网站。\n\n参考材料摘录：\n- u8/u9 历史成功复测材料，不应进入 worker objective。"
                        .into(),
                ),
                acceptance: vec!["site exists".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::IndependentReview),
                    required_actions: vec!["create".into(), "write".into()],
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert!(frame.objective.contains("目标目录：/tmp/demo-site"));
        assert!(!frame.objective.contains("参考材料摘录"));
        assert!(!frame.objective.contains("u8/u9"));
    }

    #[test]
    fn readonly_analysis_objective_may_keep_inline_reference_material() {
        let plan = BossPlan {
            plan_id: "plan-readonly-reference".into(),
            task_description: "audit notes".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "audit".into(),
                objective: Some(
                    "只读审计当前方案。\n\n参考材料摘录：\n- 保留这段材料用于审计。".into(),
                ),
                acceptance: vec!["produce audit".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::ReadOnlyAnalysis),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert!(frame.objective.contains("参考材料摘录"));
        assert!(frame.objective.contains("保留这段材料"));
    }

    #[test]
    fn worker_correction_repair_intent_projects_specific_artifact_open_items() {
        let target = "/tmp/demo-site/index.html";
        let plan = BossPlan {
            plan_id: "plan-worker-correction".into(),
            task_description: "repair site".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "repair site".into(),
                objective: Some("任务目标：\n- 目标目录：/tmp/demo-site\n- 创建静态网站。".into()),
                acceptance: vec!["site exists".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Rejected,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 1,
                retry_budget: 3,
                last_review_summary: Some("missing index".into()),
                last_correction: Some("worker_correction".into()),
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::CodeChange),
                    declared_artifacts: vec![crate::core::state_frame::DeclaredArtifactContract {
                        ref_id: "artifact:index".into(),
                        path: target.into(),
                        kind: "file".into(),
                        required_actions: vec!["create".into(), "write".into()],
                        required_evidence: vec!["artifact:index".into(), target.into()],
                    }],
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: Some(StageContinuationContext {
                    repair_intent: Some(crate::core::state_frame::RepairIntent {
                        failed_target: Some(target.into()),
                        verified_facts: vec![
                            "required_evidence_targets: /tmp/demo-site/index.html".into(),
                            "already verified: /tmp/demo-site/README.md".into(),
                        ],
                        next_action: Some("worker_correction".into()),
                        continuity_mode: Some(ContinuityMode::Repair),
                    }),
                    failed_target: Some(target.into()),
                    verified_facts: vec!["already verified: /tmp/demo-site/README.md".into()],
                    next_action: Some("worker_correction".into()),
                    continuity_mode: Some(ContinuityMode::Repair),
                }),
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert!(
            frame
                .open_items
                .iter()
                .any(|item| item == "create missing artifact: /tmp/demo-site/index.html")
        );
        assert!(
            frame
                .open_items
                .iter()
                .any(|item| item == "provide read evidence: read:/tmp/demo-site/index.html")
        );
        assert!(frame.recent_evidence.iter().any(|item| {
            item.starts_with("fact: worker_correction ")
                && item.contains("/tmp/demo-site/index.html")
        }));
    }

    #[test]
    fn st_mode_projects_test_first_contract_for_demo_report_tasks() {
        let plan = BossPlan {
            plan_id: "plan-st-demo".into(),
            task_description: "build demo".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "build demo".into(),
                objective: Some("在独立目录创建一个最小 Python demo，并报告输出。".into()),
                acceptance: vec!["demo output is available".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::CodeChange),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame_with_st_mode(
            &plan,
            BossStage::Execution,
            Some(0),
            ActorRole::Worker,
            true,
        );

        assert!(
            frame
                .stage_execution_contract
                .tests
                .iter()
                .any(|test| test.name == "st_auto_validation")
        );
        assert!(
            frame
                .stage_execution_contract
                .required_actions
                .iter()
                .any(|action| action == "run_test")
        );
        assert!(frame.stage_execution_contract.verifications.is_empty());
        assert!(
            frame
                .stage_execution_contract
                .required_actions
                .iter()
                .all(|action| action != "verify" && action != "verify_artifact")
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.starts_with("fact: st_mode "))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.starts_with("fact: test_contract "))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.contains("test_evidence=required"))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .filter(|line| line.starts_with("fact: completion_contract "))
                .all(|line| !line.contains("verification_evidence")
                    && !line.contains("verification_refs"))
        );
    }

    #[test]
    fn project_state_frame_defaults_audit_tasks_to_independent_review() {
        let plan = BossPlan {
            plan_id: "plan-review".into(),
            task_description: "audit the output".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "audit".into(),
                objective: Some("audit this report and summarize risks".into()),
                acceptance: vec!["review outcome is clear".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert_eq!(frame.stage_execution_contract.task_profile, None);
        assert_eq!(
            frame.stage_execution_contract.requires_source_evidence,
            None
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line == "fact: review_mode independent_review")
        );
    }

    #[test]
    fn independent_review_projection_hides_history_conclusions() {
        let plan = BossPlan {
            plan_id: "plan-blind-review".into(),
            task_description: "audit the output".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: Some("prior review said use source A".into()),
            revision_notes: Some("previous reviewer recommended a different target".into()),
            finalized: false,
            documentation_feedback: vec!["history says prefer approach B".into()],
            steps: vec![
                BossPlanStep {
                    id: 0,
                    description: "completed target".into(),
                    objective: Some("write the target".into()),
                    acceptance: vec!["completed step".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Completed,
                    completed: true,
                    result_diff: Some("old diff".into()),
                    worker_task_id: None,
                    attempt_count: 1,
                    retry_budget: 3,
                    last_review_summary: Some("old review said keep going".into()),
                    last_correction: Some("old correction said read source first".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: None,
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                },
                BossPlanStep {
                    id: 1,
                    description: "audit".into(),
                    objective: Some("audit this report and summarize risks".into()),
                    acceptance: vec!["review outcome is clear".into()],
                    requires_approval: false,
                    status: BossPlanStepStatus::Running,
                    completed: false,
                    result_diff: None,
                    worker_task_id: None,
                    attempt_count: 0,
                    retry_budget: 3,
                    last_review_summary: Some("do not trust the current result".into()),
                    last_correction: Some("prefer a different path".into()),
                    stage_execution_contract: StageExecutionContract::default(),
                    stage_continuation_context: Some(StageContinuationContext {
                        failed_target: Some("/tmp/report.md".into()),
                        verified_facts: vec!["verified from prior review".into()],
                        next_action: Some("read_source_evidence".into()),
                        continuity_mode: Some(crate::core::state_frame::ContinuityMode::Repair),
                        repair_intent: None,
                    }),
                    executor_b_stage_memory: None,
                    review_task_id: None,
                    tool_execution_records: Vec::new(),
                },
            ],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(1), ActorRole::Worker);

        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert!(
            frame.accepted_summary.is_empty(),
            "blind review should not inherit archive summary"
        );
        assert!(
            frame.recent_evidence.iter().all(|line| {
                !line.starts_with("review: ")
                    && !line.starts_with("correction: ")
                    && (!line.starts_with("fact: review_feedback ")
                        || line == "fact: review_feedback none recorded")
                    && (!line.starts_with("fact: revision_notes ")
                        || line == "fact: revision_notes none recorded")
                    && (!line.starts_with("fact: documentation_feedback ")
                        || line == "fact: documentation_feedback none recorded")
                    && (!line.starts_with("fact: review_verdicts ")
                        || line == "fact: review_verdicts none recorded")
                    && (!line.starts_with("fact: rejected_approaches ")
                        || line == "fact: rejected_approaches none recorded")
                    && (!line.starts_with("fact: artifact_status ")
                        || line == "fact: artifact_status none recorded")
            }),
            "blind review should not inherit conclusion-bearing history"
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.starts_with("fact: accepted_constraints "))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .all(|line| !line.contains("verified from prior review")
                    && !line.contains("do not trust the current result")),
            "blind review should not leak continuation/review conclusions"
        );
    }

    #[test]
    fn readonly_audit_validator_creation_task_is_not_projected_as_readonly_analysis() {
        let plan = BossPlan {
            plan_id: "plan-readonly-audit-validator".into(),
            task_description: "create validator".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "create validator".into(),
                objective: Some(
                    "创建一个 StateDecision/readonly-audit contract 验证器。目标目录：/tmp/state-decision-validator。允许修改文件、创建目录、运行必要命令。".into(),
                ),
                acceptance: vec![
                    "target directory exists and is non-empty: /tmp/state-decision-validator"
                        .into(),
                ],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::IndependentReview),
                    requires_source_evidence: Some(false),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert_eq!(frame.state, AgentState::Executing);
        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line == "fact: review_mode independent_review"),
            "validator-style audit tasks should default to independent_review"
        );
        assert!(
            frame.stage_execution_contract.verifications.is_empty(),
            "independent_review should not inject target_verification evidence requirements"
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line.starts_with("fact: completion_contract ")
                    && line.contains("verification_evidence=not_required")),
            "independent_review completion contract should not require verification evidence"
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .all(|line| !line.contains("execution_mode read_only_analysis")),
            "readonly-audit as a contract name must not remove write capability"
        );
        assert!(frame.recent_evidence.iter().any(|line| {
            line.contains("fact: permission_to_create_and_write:/tmp/state-decision-validator")
        }));
    }

    #[test]
    fn independent_review_runtime_validator_task_does_not_infer_test_gate_from_reference_text() {
        let plan = BossPlan {
            plan_id: "plan-u10-runtime-validator".into(),
            task_description: "create validator".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "create StateDecision validator".into(),
                objective: Some(
                    "创建一个 StateDecision contract 验证器。目标目录：/tmp/state-decision-validator。要求实际执行验证器并给出终端验证摘要。输出一个 markdown 结论文件，说明 ON/OFF 差异。".into(),
                ),
                acceptance: vec![
                    "target directory exists and is non-empty: /tmp/state-decision-validator"
                        .into(),
                ],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::IndependentReview),
                    requires_source_evidence: Some(false),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert_eq!(
            frame.stage_execution_contract.task_profile,
            Some(TaskProfile::IndependentReview)
        );
        assert_eq!(
            frame.stage_execution_contract.requires_source_evidence,
            Some(false)
        );
        assert!(frame.stage_execution_contract.verifications.is_empty());
        assert!(
            frame
                .stage_execution_contract
                .tests
                .iter()
                .all(|test| test.name != "runtime_validation" && test.name != "st_auto_validation")
        );
        assert!(
            frame
                .stage_execution_contract
                .required_actions
                .iter()
                .all(|action| action != "run_test")
        );
        assert!(
            frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .all(|artifact| artifact.path != "/tmp/state-decision-validator/summary.md"),
            "declared_artifacts={:?}",
            frame.stage_execution_contract.declared_artifacts
        );
        assert!(frame.recent_evidence.iter().any(|line| {
            line.starts_with("fact: completion_contract ")
                && line.contains("test_evidence=not_required")
                && line.contains("verification_evidence=not_required")
        }));
    }

    #[test]
    fn independent_review_projection_preserves_inline_reference_material() {
        let plan = BossPlan {
            plan_id: "plan-u2-inline-audit".into(),
            task_description: "audit bash memory backpressure".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "audit bash output limits".into(),
                objective: Some(
                    "真实 /boss A/B use case 2：审计 bash 输出限界与内存背压合同。\n\n任务目标：\n- 判断文档对 bash clamped output 是否准确。\n- 只做只读审计与摘要。\n\n关键材料摘录：\n# 29 - 内存背压与防爆栈设计\n当前 BashTool 两条热路径都已接入有界输出读取。".into(),
                ),
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
                stage_execution_contract: StageExecutionContract {
                    review_mode: Some(ReviewMode::IndependentReview),
                    task_profile: Some(TaskProfile::IndependentReview),
                    requires_source_evidence: Some(false),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert!(
            frame.objective.contains("关键材料摘录"),
            "independent review must keep inline evidence material in objective: {}",
            frame.objective
        );
        assert!(
            frame.objective.contains("当前 BashTool 两条热路径"),
            "inline audit material must remain visible to StateDecision"
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .all(|line| !line.contains("path=/boss ")),
            "slash command mention must not become a file fact: {:?}",
            frame.recent_evidence
        );
    }

    #[test]
    fn explicit_review_mode_overrides_keyword_fallback_for_validator_tasks() {
        let plan = BossPlan {
            plan_id: "plan-explicit-review-mode".into(),
            task_description: "create validator".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "create validator".into(),
                objective: Some(
                    "创建一个 StateDecision/readonly-audit contract 验证器。目标目录：/tmp/state-decision-validator。允许修改文件、创建目录、运行必要命令。".into(),
                ),
                acceptance: vec![
                    "target directory exists and is non-empty: /tmp/state-decision-validator"
                        .into(),
                ],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    review_mode: Some(ReviewMode::IndependentReview),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|line| line == "fact: review_mode independent_review"),
            "explicit model-selected review mode should reach the Fact Ledger"
        );
    }

    #[test]
    fn project_state_frame_routes_independent_review_verification_tasks_to_state_decision() {
        let plan = BossPlan {
            plan_id: "plan-review-runtime".into(),
            task_description: "audit the target file /tmp/report.md and verify the result".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "audit".into(),
                objective: Some(
                    "audit the target file /tmp/report.md and verify the result".into(),
                ),
                acceptance: vec!["target file exists and is non-empty: /tmp/report.md".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::IndependentReview),
                    requires_source_evidence: Some(false),
                    verifications: vec![VerificationContract {
                        target_ref: "artifact:report".into(),
                        target_path: Some("/tmp/report.md".into()),
                        required_actions: vec!["verify".into()],
                        required_evidence: vec!["/tmp/report.md".into()],
                    }],
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert!(!frame.stage_execution_contract.verifications.is_empty());
        assert_eq!(
            frame.required_output_schema.as_deref(),
            Some("state_decision_v1")
        );
        assert_eq!(frame.toolset_id.as_deref(), Some("verifier-readonly"));
    }

    #[test]
    fn project_state_frame_directory_audit_stays_neutral_without_typed_target_verification() {
        let plan = BossPlan {
            plan_id: "plan-review-runtime-directory".into(),
            task_description:
                "audit the target directory /tmp/state-decision-validator and verify it".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "audit".into(),
                objective: Some(
                    "audit the target directory /tmp/state-decision-validator and verify it".into(),
                ),
                acceptance: vec![
                    "target directory exists and is non-empty: /tmp/state-decision-validator"
                        .into(),
                ],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract {
                    task_profile: Some(TaskProfile::IndependentReview),
                    requires_source_evidence: Some(false),
                    ..StageExecutionContract::default()
                },
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert_eq!(
            frame.stage_execution_contract.review_mode,
            Some(ReviewMode::IndependentReview)
        );
        assert!(frame.stage_execution_contract.verifications.is_empty());
        assert!(
            frame
                .open_items
                .iter()
                .all(|item| !item.contains("required_action:verify_artifact"))
        );
    }

    #[test]
    fn project_state_frame_projects_source_evidence_continuation_to_prompt_surface() {
        let source_path = "RustAgent/Agent/src/tool/registry.rs";
        let plan = BossPlan {
            plan_id: "plan-source-continuation".into(),
            task_description: "build source backed report".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "report".into(),
                objective: Some(
                    "write report to /tmp/report.md using RustAgent/Agent/src/tool/registry.rs"
                        .into(),
                ),
                acceptance: vec!["report exists".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Rejected,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 1,
                retry_budget: 3,
                last_review_summary: Some(
                    "completion gate rejected direct completion: verification contract remains unsatisfied"
                        .into(),
                ),
                last_correction: Some("read_source_evidence".into()),
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: Some(StageContinuationContext {
                    failed_target: Some(source_path.into()),
                    next_action: Some("read_source_evidence".into()),
                    continuity_mode: Some(ContinuityMode::Repair),
                    ..StageContinuationContext::default()
                }),
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: true,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);

        assert!(frame.open_items.iter().any(|item| {
            item.contains("required_action:read_source_evidence") && item.contains(source_path)
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("fact: stage_continuation")
                && item.contains("next_action=read_source_evidence")
                && item.contains(source_path)
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("fact: missing_source_evidence")
                && item.contains("required_action=read_source_evidence")
                && item.contains(source_path)
        }));
    }

    #[test]
    fn project_state_frame_declared_artifact_does_not_depend_on_objective_keywords() {
        let plan = BossPlan {
            plan_id: "plan-2".into(),
            task_description: "build report".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "report".into(),
                objective: Some("create output in /tmp/custom-output.txt".into()),
                acceptance: vec!["done".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
        assert!(
            frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/custom-output.txt")
        );
    }

    #[test]
    fn project_state_frame_emits_typed_contract_without_keyword_dependence() {
        let plan = BossPlan {
            plan_id: "plan-4".into(),
            task_description: "build typed contract".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "report".into(),
                objective: Some("write /tmp/typed-contract.txt".into()),
                acceptance: vec!["done".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
        assert!(!frame.stage_execution_contract.declared_artifacts.is_empty());
        assert!(
            frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .all(|artifact| !artifact.path.trim().is_empty())
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.starts_with("fact: declared_artifact_contract "))
        );
        assert!(!frame.recent_evidence.iter().any(|item| {
            item.contains("source=objective") && item.contains("fact: completion_contract ")
        }));
    }

    #[test]
    fn project_state_frame_keeps_multi_artifact_contract_visible() {
        let plan = BossPlan {
            plan_id: "plan-3".into(),
            task_description: "build two artifacts".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "report".into(),
                objective: Some("create /tmp/alpha.txt and also create /tmp/beta.txt".into()),
                acceptance: vec!["done".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
        assert_eq!(frame.stage_execution_contract.declared_artifacts.len(), 2);
        assert!(
            frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/alpha.txt")
        );
        assert!(
            frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .any(|artifact| artifact.path == "/tmp/beta.txt")
        );
    }

    #[test]
    fn project_state_frame_collects_source_and_document_targets_into_content_evidence() {
        let plan = BossPlan {
            plan_id: "plan-5".into(),
            task_description: "analyze tool surface".into(),
            document_spec: String::new(),
            pseudo_code: String::new(),
            draft_spec: None,
            review_feedback: None,
            revision_notes: None,
            finalized: false,
            documentation_feedback: Vec::new(),
            steps: vec![BossPlanStep {
                id: 0,
                description: "write report".into(),
                objective: Some(
                    "任务目标：\n- 目标文件：/tmp/report.md\n- 建议核验路径：\n  - src/tool/definition.rs\n  - ../docs/31-token-efficiency-cost-performance.md"
                        .into(),
                ),
                acceptance: vec!["target file exists and is non-empty: /tmp/report.md".into()],
                requires_approval: false,
                status: BossPlanStepStatus::Running,
                completed: false,
                result_diff: None,
                worker_task_id: None,
                attempt_count: 0,
                retry_budget: 3,
                last_review_summary: None,
                last_correction: None,
                stage_execution_contract: StageExecutionContract::default(),
                stage_continuation_context: None,
                executor_b_stage_memory: None,
                review_task_id: None,
                tool_execution_records: Vec::new(),
            }],
            accepted_by_user: true,
            auto_sequence: false,
            session_snapshot: None,
        };

        let frame = project_state_frame(&plan, BossStage::Execution, Some(0), ActorRole::Worker);
        assert!(
            frame
                .stage_execution_contract
                .content_evidence_targets
                .iter()
                .any(|target| target.ends_with("tool/definition.rs"))
        );
        assert!(
            frame
                .stage_execution_contract
                .content_evidence_targets
                .iter()
                .any(|target| target.ends_with("docs/31-token-efficiency-cost-performance.md"))
        );
        assert!(
            !frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .any(|artifact| artifact.path.ends_with("tool/definition.rs"))
        );
        assert!(
            !frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .any(|artifact| artifact
                    .path
                    .ends_with("docs/31-token-efficiency-cost-performance.md"))
        );
    }
}

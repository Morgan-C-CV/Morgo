use crate::core::boss_acceptance::extract_artifact_expectations;
use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_fact_ledger::{
    build_blocker_records, build_open_item_records, build_rejected_approach_records,
    build_step_fact_ledgers,
};
use crate::core::state_frame::{
    ActorRole, AgentState, DeclaredArtifactContract, StageExecutionContract, StateBudget,
    StateFrame, TestContract, VerificationContract,
};
use crate::core::state_frame_archive::{
    archive_to_summary, build_accepted_archive, retain_blocked_items, retain_open_items,
};
use crate::core::state_frame_router::{apply_route, route_toolset};

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn is_readonly_analysis(plan: &BossPlan, step_id: Option<usize>) -> bool {
    let objective = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .map(|s| s.objective())
        .unwrap_or(plan.task_description.as_str())
        .to_lowercase();
    contains_any(
        &objective,
        &[
            "只读",
            "不改文件",
            "不要改文件",
            "不要改代码",
            "不要写 patch",
            "不提出 patch",
            "只做只读",
            "read-only",
            "readonly",
            "do not modify",
            "no code changes",
            "no patch",
        ],
    )
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

fn infer_preferred_deployment_mode(objective: &str) -> &'static str {
    let lowered = objective.to_lowercase();
    if lowered.contains("静态网站") || lowered.contains("static site") {
        "static_site"
    } else if lowered.contains("python") && lowered.contains("demo") {
        "python_demo"
    } else if lowered.contains("jsonl") || lowered.contains("analyzer") {
        "local_tool"
    } else if lowered.contains("report") || lowered.contains("报告") {
        "local_report_artifact"
    } else {
        "local_artifact"
    }
}

fn build_permission_facts(step_id: usize, objective: &str, readonly_analysis: bool) -> Vec<String> {
    if readonly_analysis {
        return Vec::new();
    }
    let mut facts = Vec::new();
    for (idx, expectation) in extract_artifact_expectations(objective)
        .into_iter()
        .enumerate()
    {
        let path = expectation.path.to_string_lossy().to_string();
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

fn open_item_requires_test(summary: &str) -> bool {
    let lowered = summary.to_ascii_lowercase();
    lowered.contains("cargo test")
        || lowered.contains("run test")
        || lowered.contains("run tests")
        || lowered.contains("pytest")
        || lowered.contains("unit test")
        || lowered.contains("integration test")
        || lowered.contains("pytest")
        || lowered.contains("run verification")
        || summary.contains("运行测试")
        || summary.contains("执行测试")
}

fn open_item_requires_verification(summary: &str) -> bool {
    let lowered = summary.to_ascii_lowercase();
    lowered.contains("verify")
        || lowered.contains("verification")
        || lowered.contains("artifact check")
        || summary.contains("验证")
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
    open_item_ledgers: &[crate::core::state_fact_ledger::OpenItemRecord],
    readonly_analysis: bool,
) -> String {
    let artifact_required =
        !readonly_analysis && (!permission_facts.is_empty() || !artifact_ledgers.is_empty());
    let artifact_refs = artifact_ledgers
        .iter()
        .map(|item| item.ref_id.clone())
        .collect::<Vec<_>>();
    let test_refs = open_item_ledgers
        .iter()
        .filter(|item| open_item_requires_test(&item.summary))
        .map(|item| item.ref_id.clone())
        .collect::<Vec<_>>();
    let verification_refs = if readonly_analysis {
        Vec::new()
    } else if artifact_required {
        artifact_refs.clone()
    } else {
        open_item_ledgers
            .iter()
            .filter(|item| open_item_requires_verification(&item.summary))
            .map(|item| item.ref_id.clone())
            .collect::<Vec<_>>()
    };
    let test_required = !test_refs.is_empty();
    let verification_required = !verification_refs.is_empty();
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

fn build_stage_execution_contract(
    step: Option<&crate::core::boss_state::BossPlanStep>,
    permission_facts: &[String],
    artifact_ledgers: &[crate::core::state_fact_ledger::ArtifactRecord],
    open_item_ledgers: &[crate::core::state_fact_ledger::OpenItemRecord],
    readonly_analysis: bool,
) -> StageExecutionContract {
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
        for (idx, expectation) in extract_artifact_expectations(step.objective())
            .into_iter()
            .enumerate()
        {
            let path = expectation.path.to_string_lossy().to_string();
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
                required_evidence: vec![
                    format!("artifact:step{}:{idx}", step.id),
                    path,
                    kind,
                ],
            });
        }
    }
    let verifications = artifact_ledgers
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
            required_actions: if readonly_analysis {
                Vec::new()
            } else {
                vec!["verify".into()]
            },
            required_evidence: vec![target_ref, target_path],
        })
        .collect::<Vec<_>>();
    let tests = open_item_ledgers
        .iter()
        .filter(|item| open_item_requires_test(&item.summary))
        .map(|item| TestContract {
            name: item.summary.clone(),
            required_actions: vec!["run_test".into()],
            required_evidence: vec![item.ref_id.clone()],
        })
        .collect::<Vec<_>>();
    let mut required_actions = Vec::new();
    if step.is_some() && !declared_artifacts.is_empty() {
        required_actions.extend(["create".into(), "write".into()]);
    }
    if !tests.is_empty() {
        required_actions.push("run_test".into());
    }
    if !verifications.is_empty() {
        required_actions.push("verify".into());
    }
    let mut required_evidence = Vec::new();
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
    StageExecutionContract {
        declared_artifacts,
        verifications,
        tests,
        required_actions,
        required_evidence,
    }
}

fn build_stage_contract_facts(contract: &StageExecutionContract) -> Vec<String> {
    let mut facts = Vec::new();
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
        let ledgers = build_step_fact_ledgers(step);
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
            summarize_list(&step.acceptance),
        ));
        facts.push(fact_line(
            "preferred_deployment_mode",
            format!(
                "ref=deploymode:step{} source=objective_inference source_event_id=deploymode:{} freshness=current confidence=0.85 status=active invalidated_by=none supersedes=none conflicts_with=none summary={}",
                step.id,
                step.id,
                infer_preferred_deployment_mode(step.objective())
            ),
        ));
        facts.push(fact_line(
            "reject_correction",
            step.last_correction
                .as_deref()
                .or(step.last_review_summary.as_deref())
                .unwrap_or("none recorded"),
        ));
        facts.push(fact_line(
            "recent_diff",
            step.result_diff.as_deref().unwrap_or("none recorded"),
        ));
        let permission_facts = build_permission_facts(step.id, step.objective(), readonly_analysis);
        facts.extend(permission_facts.iter().cloned());
        if !ledgers.file_facts.is_empty() {
            for item in ledgers.file_facts {
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
        if !ledgers.review_refs.is_empty() {
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
        if !ledgers.artifact_refs.is_empty() {
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
        push_none_recorded_unless_present(&mut facts, "rejected_approaches");
        let stage_execution_contract = build_stage_execution_contract(
            current_step,
            &permission_facts,
            &ledgers.artifact_refs,
            &open_item_ledgers,
            readonly_analysis,
        );
        facts.extend(build_stage_contract_facts(&stage_execution_contract));
        facts.push(build_completion_contract_fact(
            &permission_facts,
            &ledgers.artifact_refs,
            &open_item_ledgers,
            readonly_analysis,
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
        plan.review_feedback.as_deref().unwrap_or("none recorded"),
    ));
    facts.push(fact_line(
        "revision_notes",
        plan.revision_notes.as_deref().unwrap_or("none recorded"),
    ));
    facts.push(fact_line(
        "documentation_feedback",
        if plan.documentation_feedback.is_empty() {
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
    let state = stage_to_agent_state(stage);
    let readonly_analysis = is_readonly_analysis(plan, step_id);

    // Build archive of completed steps (excluding current step).
    let archive = build_accepted_archive(plan, step_id);

    // objective: current step objective if available, else plan task description.
    let objective = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .map(|s| s.objective().to_string())
        .unwrap_or_else(|| plan.task_description.clone());

    // open_items: unsatisfied acceptance criteria of the current step.
    let open_items = step_id
        .and_then(|id| plan.steps.iter().find(|s| s.id == id))
        .filter(|s| !s.completed)
        .map(|s| retain_open_items(&s.acceptance, &archive))
        .unwrap_or_default();

    // blocked_items: stage-driven via archive.
    let blocked_items = retain_blocked_items(stage, &archive);

    // accepted_summary: rendered from archive.
    let accepted_summary = archive_to_summary(&archive);
    let current_step = step_id.and_then(|id| plan.steps.iter().find(|s| s.id == id));
    let permission_facts = current_step
        .map(|step| build_permission_facts(step.id, step.objective(), readonly_analysis))
        .unwrap_or_default();
    let ledgers = current_step.map(build_step_fact_ledgers);
    let open_item_ledgers = current_step
        .map(|step| build_open_item_records(step, &open_items))
        .unwrap_or_default();
    let stage_execution_contract = build_stage_execution_contract(
        current_step,
        &permission_facts,
        ledgers
            .as_ref()
            .map(|value| value.artifact_refs.as_slice())
            .unwrap_or(&[]),
        &open_item_ledgers,
        readonly_analysis,
    );

    // recent_evidence doubles as a compact Fact Ledger v1.
    let mut recent_evidence = build_stage_contract_facts(&stage_execution_contract);
    recent_evidence.extend(build_fact_ledger(
        plan,
        stage,
        step_id,
        &open_items,
        &blocked_items,
        readonly_analysis,
    ));
    if let Some(step) = step_id.and_then(|id| plan.steps.iter().find(|s| s.id == id)) {
        if let Some(r) = &step.last_review_summary {
            recent_evidence.push(format!("review: {r}"));
        }
        if let Some(c) = &step.last_correction {
            recent_evidence.push(format!("correction: {c}"));
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
        required_output_schema: Some(if readonly_analysis {
            "readonly_audit_4_paragraphs_v1".into()
        } else {
            "state_decision_v1".into()
        }),
        budget: StateBudget::default(),
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
    use super::{collect_projection_diagnostics, project_state_frame};
    use crate::core::boss_state::{BossPlan, BossPlanStep, BossPlanStepStatus, BossStage};
    use crate::core::state_frame::{
        ActorRole, AgentState, StageExecutionContract, StateBudget, StateFrame,
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
    fn project_state_frame_emits_permission_and_deployment_facts_for_artifact_tasks() {
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
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("fact: preferred_deployment_mode") && item.contains("summary=static_site")
        }));
        assert!(
            frame.recent_evidence.iter().any(|item| {
                item.contains("fact: permission_to_create_and_write:/tmp/demo-site")
            })
        );
        assert_eq!(frame.stage_execution_contract.declared_artifacts.len(), 1);
        assert_eq!(frame.stage_execution_contract.verifications.len(), 1);
        assert_eq!(frame.stage_execution_contract.required_actions, vec!["create", "write", "verify"]);
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
        assert!(frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .all(|artifact| !artifact.path.trim().is_empty()));
        assert!(frame
            .recent_evidence
            .iter()
            .any(|item| item.starts_with("fact: declared_artifact_contract ")));
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
                objective: Some(
                    "create /tmp/alpha.txt and also create /tmp/beta.txt".into(),
                ),
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
        assert!(frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .any(|artifact| artifact.path == "/tmp/alpha.txt"));
        assert!(frame
            .stage_execution_contract
            .declared_artifacts
            .iter()
            .any(|artifact| artifact.path == "/tmp/beta.txt"));
    }
}

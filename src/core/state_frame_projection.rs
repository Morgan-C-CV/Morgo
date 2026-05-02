use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_fact_ledger::{
    build_blocker_records, build_open_item_records, build_rejected_approach_records,
    build_step_fact_ledgers,
};
use crate::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};
use crate::core::state_frame_archive::{
    archive_to_summary, build_accepted_archive, retain_blocked_items, retain_open_items,
};

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

/// Static allowed_actions per AgentState.
/// Reviewing / Correcting / Verifying are not yet reachable from BossStage — omitted.
fn allowed_actions_for_state(state: AgentState, readonly_analysis: bool) -> Vec<String> {
    if readonly_analysis {
        return match state {
            AgentState::Planning | AgentState::Executing => {
                vec!["read_file".into(), "summarize_findings".into()]
            }
            AgentState::Blocked | AgentState::Done => vec![],
            _ => vec![],
        };
    }
    match state {
        AgentState::Planning => vec!["read_file".into(), "write_spec".into()],
        AgentState::Executing => vec!["read_file".into(), "edit_file".into(), "run_test".into()],
        AgentState::Blocked | AgentState::Done => vec![],
        _ => vec![],
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

pub fn collect_projection_diagnostics(frame: &StateFrame) -> ProjectionDiagnostics {
    let mut warnings = Vec::new();

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

    // recent_evidence doubles as a compact Fact Ledger v1.
    let mut recent_evidence = build_fact_ledger(
        plan,
        stage,
        step_id,
        &open_items,
        &blocked_items,
        readonly_analysis,
    );
    if let Some(step) = step_id.and_then(|id| plan.steps.iter().find(|s| s.id == id)) {
        if let Some(r) = &step.last_review_summary {
            recent_evidence.push(format!("review: {r}"));
        }
        if let Some(c) = &step.last_correction {
            recent_evidence.push(format!("correction: {c}"));
        }
    }

    let allowed_actions = allowed_actions_for_state(state, readonly_analysis);

    let mut frame = StateFrame {
        role,
        state,
        objective,
        open_items,
        blocked_items,
        accepted_summary,
        recent_evidence,
        allowed_actions,
        toolset_id: None,
        skillset_id: None,
        required_output_schema: Some(if readonly_analysis {
            "readonly_audit_4_paragraphs_v1".into()
        } else {
            "state_decision_v1".into()
        }),
        budget: StateBudget::default(),
    };
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
    use super::collect_projection_diagnostics;
    use crate::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};

    #[test]
    fn projection_diagnostics_flags_open_items_without_refs_and_missing_review_source_ref() {
        let frame = StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "fix worker context".into(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: open_item_refs none recorded".into(),
                "fact: review_verdicts ref=review:step1:0 verdict=accepted source=tool:BossReview source_event_id=tool-review:1:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=ok".into(),
                "fact: rejected_approaches ref=rejected:step1:0 source=review_correction source_ref=review:step1:missing source_event_id=review-correction:1 freshness=after-review confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=review:step1:missing summary=bad path".into(),
            ],
            allowed_actions: vec![],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
        };

        let diagnostics = collect_projection_diagnostics(&frame);
        assert_eq!(diagnostics.mismatch_count, 2);
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
}

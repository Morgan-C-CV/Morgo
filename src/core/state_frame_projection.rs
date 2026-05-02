use crate::core::boss_state::{BossPlan, BossStage};
use crate::core::state_fact_ledger::build_step_fact_ledgers;
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
                        "ref={} path={} kind={} source={} freshness={} confidence={}{} fact={}",
                        item.ref_id,
                        item.path,
                        item.kind,
                        item.source,
                        item.freshness,
                        format_confidence(item.confidence_milli),
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
                        "ref={} name={} status={} source={} freshness={} confidence={} summary={}",
                        item.ref_id,
                        item.name,
                        item.status,
                        item.source,
                        item.freshness,
                        format_confidence(item.confidence_milli),
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
                        "ref={} path={} source={} freshness={} confidence={} summary={}",
                        item.ref_id,
                        item.path,
                        item.source,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.summary
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "recent_changes_in_files");
        if !ledgers.review_refs.is_empty() {
            for item in ledgers.review_refs {
                facts.push(fact_line(
                    "review_verdicts",
                    format!(
                        "ref={} verdict={} source={} freshness={} confidence={} summary={}{}",
                        item.ref_id,
                        item.verdict,
                        item.source,
                        item.freshness,
                        format_confidence(item.confidence_milli),
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
            for item in ledgers.artifact_refs {
                facts.push(fact_line(
                    "artifact_status",
                    format!(
                        "ref={} path={} kind={} status={} source={} freshness={} confidence={} summary={}",
                        item.ref_id,
                        item.path,
                        item.kind,
                        item.status,
                        item.source,
                        item.freshness,
                        format_confidence(item.confidence_milli),
                        item.summary
                    ),
                ));
            }
        }
        push_none_recorded_unless_present(&mut facts, "artifact_status");
    } else {
        facts.push(fact_line("accepted_constraints", "none recorded"));
        facts.push(fact_line("reject_correction", "none recorded"));
        facts.push(fact_line("recent_diff", "none recorded"));
        facts.push(fact_line("file_facts", "none recorded"));
        facts.push(fact_line("test_failures", "none recorded"));
        facts.push(fact_line("recent_changes_in_files", "none recorded"));
        facts.push(fact_line("review_verdicts", "none recorded"));
        facts.push(fact_line("artifact_status", "none recorded"));
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

    StateFrame {
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
    }
}

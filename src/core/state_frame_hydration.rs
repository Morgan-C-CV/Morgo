use crate::core::state_frame::StateFrame;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NeededContextSelector {
    FileSnippet { path: String },
    Symbol { name: String },
    TestFailure { query: Option<String> },
    ChangeRef { path: Option<String> },
    ReviewRef { query: Option<String> },
    ArtifactRef { query: Option<String> },
    OpenItemRef { query: Option<String> },
    BlockerRef { query: Option<String> },
    RejectedApproach { query: Option<String> },
    Artifact { path: Option<String> },
    Fact { name: String },
    Unknown { raw: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HydrationSummary {
    pub changed: bool,
    pub hydrated: Vec<String>,
    pub unavailable: Vec<String>,
    pub deferred: Vec<String>,
}

fn push_unique(items: &mut Vec<String>, value: String) -> bool {
    if items.iter().any(|item| item == &value) {
        return false;
    }
    items.push(value);
    true
}

fn compact_excerpt(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut iter = compact.chars();
    let excerpt = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{excerpt}...")
    } else {
        excerpt
    }
}

fn selector_key(selector: &NeededContextSelector) -> String {
    match selector {
        NeededContextSelector::FileSnippet { path } => format!("file_snippet:{path}"),
        NeededContextSelector::Symbol { name } => format!("symbol:{name}"),
        NeededContextSelector::TestFailure { query: Some(query) } => {
            format!("test_failure:{query}")
        }
        NeededContextSelector::TestFailure { query: None } => "test_failure".into(),
        NeededContextSelector::ChangeRef { path: Some(path) } => format!("change_ref:{path}"),
        NeededContextSelector::ChangeRef { path: None } => "change_ref".into(),
        NeededContextSelector::ReviewRef { query: Some(query) } => format!("review_ref:{query}"),
        NeededContextSelector::ReviewRef { query: None } => "review_ref".into(),
        NeededContextSelector::ArtifactRef { query: Some(query) } => {
            format!("artifact_ref:{query}")
        }
        NeededContextSelector::ArtifactRef { query: None } => "artifact_ref".into(),
        NeededContextSelector::OpenItemRef { query: Some(query) } => {
            format!("open_item_ref:{query}")
        }
        NeededContextSelector::OpenItemRef { query: None } => "open_item_ref".into(),
        NeededContextSelector::BlockerRef { query: Some(query) } => format!("blocker_ref:{query}"),
        NeededContextSelector::BlockerRef { query: None } => "blocker_ref".into(),
        NeededContextSelector::RejectedApproach { query: Some(query) } => {
            format!("rejected_approach:{query}")
        }
        NeededContextSelector::RejectedApproach { query: None } => "rejected_approach".into(),
        NeededContextSelector::Artifact { path: Some(path) } => format!("artifact:{path}"),
        NeededContextSelector::Artifact { path: None } => "artifact".into(),
        NeededContextSelector::Fact { name } => format!("fact:{name}"),
        NeededContextSelector::Unknown { raw } => raw.clone(),
    }
}

fn selector_priority(selector: &NeededContextSelector) -> usize {
    match selector {
        NeededContextSelector::TestFailure { .. } => 0,
        NeededContextSelector::ChangeRef { .. } => 1,
        NeededContextSelector::ReviewRef { .. } => 2,
        NeededContextSelector::ArtifactRef { .. } => 3,
        NeededContextSelector::OpenItemRef { .. } => 4,
        NeededContextSelector::BlockerRef { .. } => 5,
        NeededContextSelector::RejectedApproach { .. } => 6,
        NeededContextSelector::FileSnippet { .. } => 7,
        NeededContextSelector::Artifact { .. } => 8,
        NeededContextSelector::Fact { .. } => 9,
        NeededContextSelector::Symbol { .. } => 10,
        NeededContextSelector::Unknown { .. } => 11,
    }
}

fn selector_estimated_tokens(selector: &NeededContextSelector) -> u64 {
    match selector {
        NeededContextSelector::TestFailure { .. } => 160,
        NeededContextSelector::ChangeRef { .. } => 140,
        NeededContextSelector::ReviewRef { .. } => 120,
        NeededContextSelector::ArtifactRef { .. } => 140,
        NeededContextSelector::OpenItemRef { .. } => 100,
        NeededContextSelector::BlockerRef { .. } => 100,
        NeededContextSelector::RejectedApproach { .. } => 120,
        NeededContextSelector::FileSnippet { .. } => 180,
        NeededContextSelector::Artifact { .. } => 180,
        NeededContextSelector::Fact { .. } => 120,
        NeededContextSelector::Symbol { .. } => 200,
        NeededContextSelector::Unknown { .. } => 80,
    }
}

pub fn parse_needed_context_selector(raw: &str) -> NeededContextSelector {
    let trimmed = raw.trim();
    if let Some(path) = trimmed.strip_prefix("file_snippet:") {
        return NeededContextSelector::FileSnippet {
            path: path.trim().to_string(),
        };
    }
    if let Some(path) = trimmed.strip_prefix("file:") {
        return NeededContextSelector::FileSnippet {
            path: path.trim().to_string(),
        };
    }
    if let Some(name) = trimmed.strip_prefix("symbol:") {
        return NeededContextSelector::Symbol {
            name: name.trim().to_string(),
        };
    }
    if let Some(query) = trimmed.strip_prefix("test_failure:") {
        return NeededContextSelector::TestFailure {
            query: Some(query.trim().to_string()),
        };
    }
    if trimmed == "test_failure" {
        return NeededContextSelector::TestFailure { query: None };
    }
    if let Some(path) = trimmed.strip_prefix("change_ref:") {
        return NeededContextSelector::ChangeRef {
            path: Some(path.trim().to_string()),
        };
    }
    if trimmed == "change_ref" {
        return NeededContextSelector::ChangeRef { path: None };
    }
    if let Some(query) = trimmed.strip_prefix("review_ref:") {
        return NeededContextSelector::ReviewRef {
            query: Some(query.trim().to_string()),
        };
    }
    if trimmed == "review_ref" {
        return NeededContextSelector::ReviewRef { query: None };
    }
    if let Some(query) = trimmed.strip_prefix("artifact_ref:") {
        return NeededContextSelector::ArtifactRef {
            query: Some(query.trim().to_string()),
        };
    }
    if trimmed == "artifact_ref" {
        return NeededContextSelector::ArtifactRef { query: None };
    }
    if let Some(query) = trimmed.strip_prefix("open_item_ref:") {
        return NeededContextSelector::OpenItemRef {
            query: Some(query.trim().to_string()),
        };
    }
    if trimmed == "open_item_ref" {
        return NeededContextSelector::OpenItemRef { query: None };
    }
    if let Some(query) = trimmed.strip_prefix("blocker_ref:") {
        return NeededContextSelector::BlockerRef {
            query: Some(query.trim().to_string()),
        };
    }
    if trimmed == "blocker_ref" {
        return NeededContextSelector::BlockerRef { query: None };
    }
    if let Some(query) = trimmed.strip_prefix("rejected_approach:") {
        return NeededContextSelector::RejectedApproach {
            query: Some(query.trim().to_string()),
        };
    }
    if trimmed == "rejected_approach" {
        return NeededContextSelector::RejectedApproach { query: None };
    }
    if let Some(path) = trimmed.strip_prefix("artifact:") {
        return NeededContextSelector::Artifact {
            path: Some(path.trim().to_string()),
        };
    }
    if trimmed == "artifact" {
        return NeededContextSelector::Artifact { path: None };
    }
    if let Some(name) = trimmed.strip_prefix("fact:") {
        return NeededContextSelector::Fact {
            name: name.trim().to_string(),
        };
    }
    NeededContextSelector::Unknown {
        raw: trimmed.to_string(),
    }
}

fn find_recent_evidence<'a>(frame: &'a StateFrame, prefix: &str) -> impl Iterator<Item = &'a str> {
    frame.recent_evidence.iter().filter_map(move |item| {
        if item.starts_with(prefix) {
            Some(item.as_str())
        } else {
            None
        }
    })
}

fn contains_path(item: &str, path: &str) -> bool {
    item.contains(&format!("path={path}"))
}

fn contains_ref(item: &str, ref_id: &str) -> bool {
    item.contains(&format!("ref={ref_id}"))
}

fn estimate_excerpt_chars(frame: &StateFrame, selected_count: usize) -> usize {
    if frame.budget.max_input_tokens == 0 {
        return 180;
    }
    let total_chars_budget = frame.budget.max_input_tokens.saturating_mul(4) as usize;
    let per_selector = (total_chars_budget / selected_count.max(1)).saturating_sub(48);
    per_selector.clamp(80, 220)
}

fn select_context_requests(
    frame: &StateFrame,
    requested: &[String],
) -> (Vec<NeededContextSelector>, Vec<NeededContextSelector>) {
    let mut selectors = requested
        .iter()
        .map(|raw| parse_needed_context_selector(raw))
        .collect::<Vec<_>>();
    selectors.sort_by_key(selector_priority);
    if frame.budget.max_input_tokens == 0 {
        return (selectors, Vec::new());
    }

    let mut selected = Vec::new();
    let mut deferred = Vec::new();
    let mut used_tokens = 0_u64;
    let cap = frame
        .budget
        .max_input_tokens
        .saturating_mul(35)
        .saturating_div(100);
    for selector in selectors {
        let estimate = selector_estimated_tokens(&selector);
        if !selected.is_empty() && used_tokens.saturating_add(estimate) > cap.max(estimate) {
            deferred.push(selector);
            continue;
        }
        used_tokens = used_tokens.saturating_add(estimate);
        selected.push(selector);
    }
    (selected, deferred)
}

fn selector_matches_symbol(item: &str, name: &str) -> bool {
    item.contains(&format!("symbol={name}")) || item.contains(name)
}

fn hydrate_selector(
    frame: &StateFrame,
    selector: &NeededContextSelector,
    excerpt_chars: usize,
) -> Option<String> {
    match selector {
        NeededContextSelector::FileSnippet { path } => {
            find_recent_evidence(frame, "fact: file_facts")
                .find(|item| contains_path(item, path))
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=fact_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
                .or_else(|| {
                    find_recent_evidence(frame, "fact: recent_changes_in_files")
                        .find(|item| contains_path(item, path))
                        .map(|item| {
                            format!(
                                "hydrated_context: {} source=change_ledger excerpt={}",
                                selector_key(selector),
                                compact_excerpt(item, excerpt_chars)
                            )
                        })
                })
                .or_else(|| {
                    frame.objective.contains(path).then(|| {
                        format!(
                            "hydrated_context: {} source=objective excerpt={}",
                            selector_key(selector),
                            compact_excerpt(&frame.objective, excerpt_chars)
                        )
                    })
                })
        }
        NeededContextSelector::TestFailure { query } => {
            find_recent_evidence(frame, "fact: test_failures")
                .find(|item| {
                    item.contains("status=failed")
                        && query.as_ref().map(|q| item.contains(q)).unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=test_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::ChangeRef { path } => {
            find_recent_evidence(frame, "fact: recent_changes_in_files")
                .find(|item| {
                    path.as_ref()
                        .map(|p| contains_path(item, p))
                        .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=change_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::ReviewRef { query } => {
            find_recent_evidence(frame, "fact: review_verdicts")
                .find(|item| {
                    query
                        .as_ref()
                        .map(|q| contains_ref(item, q) || item.contains(&format!("verdict={q}")))
                        .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=review_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::ArtifactRef { query } => {
            find_recent_evidence(frame, "fact: artifact_status")
                .find(|item| {
                    query
                        .as_ref()
                        .map(|q| contains_ref(item, q) || contains_path(item, q))
                        .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=artifact_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::OpenItemRef { query } => {
            find_recent_evidence(frame, "fact: open_item_refs")
                .find(|item| {
                    query
                        .as_ref()
                        .map(|q| contains_ref(item, q) || item.contains(q))
                        .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=open_item_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::BlockerRef { query } => {
            find_recent_evidence(frame, "fact: blocker_refs")
                .find(|item| {
                    query
                        .as_ref()
                        .map(|q| contains_ref(item, q) || item.contains(q))
                        .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=blocker_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::RejectedApproach { query } => {
            find_recent_evidence(frame, "fact: rejected_approaches")
                .find(|item| {
                    query
                        .as_ref()
                        .map(|q| contains_ref(item, q) || item.contains(q))
                        .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=rejected_approach_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, excerpt_chars)
                    )
                })
        }
        NeededContextSelector::Artifact { path } => {
            let match_in_artifacts = path.as_ref().and_then(|p| {
                find_recent_evidence(frame, "fact: artifact_status")
                    .find(|item| contains_path(item, p))
                    .map(|item| {
                        format!(
                            "hydrated_context: {} source=artifact_ledger excerpt={}",
                            selector_key(selector),
                            compact_excerpt(item, excerpt_chars)
                        )
                    })
            });
            let match_in_changes = path.as_ref().and_then(|p| {
                find_recent_evidence(frame, "fact: recent_changes_in_files")
                    .find(|item| contains_path(item, p))
                    .map(|item| {
                        format!(
                            "hydrated_context: {} source=change_ledger excerpt={}",
                            selector_key(selector),
                            compact_excerpt(item, excerpt_chars)
                        )
                    })
            });
            let match_in_objective = path
                .as_ref()
                .filter(|p| frame.objective.contains(p.as_str()))
                .map(|p| {
                    format!(
                        "hydrated_context: {} source=objective excerpt={}",
                        selector_key(selector),
                        compact_excerpt(
                            &format!(
                                "objective references artifact path {p}; objective={}",
                                frame.objective
                            ),
                            excerpt_chars
                        )
                    )
                });
            match_in_artifacts
                .or(match_in_changes)
                .or(match_in_objective)
                .or_else(|| {
                    find_recent_evidence(frame, "fact: file_facts")
                        .find(|item| {
                            path.as_ref()
                                .map(|p| contains_path(item, p))
                                .unwrap_or(true)
                        })
                        .map(|item| {
                            format!(
                                "hydrated_context: {} source=fact_ledger excerpt={}",
                                selector_key(selector),
                                compact_excerpt(item, excerpt_chars)
                            )
                        })
                })
        }
        NeededContextSelector::Fact { name } => frame
            .recent_evidence
            .iter()
            .find(|item| item.starts_with(&format!("fact: {name} ")))
            .map(|item| {
                format!(
                    "hydrated_context: {} source=fact_ledger excerpt={}",
                    selector_key(selector),
                    compact_excerpt(item, excerpt_chars)
                )
            }),
        NeededContextSelector::Symbol { name } => frame
            .recent_evidence
            .iter()
            .find(|item| selector_matches_symbol(item, name))
            .map(|item| {
                format!(
                    "hydrated_context: {} source=evidence_match excerpt={}",
                    selector_key(selector),
                    compact_excerpt(item, excerpt_chars)
                )
            })
            .or_else(|| {
                frame.objective.contains(name).then(|| {
                    format!(
                        "hydrated_context: {} source=objective excerpt={}",
                        selector_key(selector),
                        compact_excerpt(&frame.objective, excerpt_chars)
                    )
                })
            }),
        NeededContextSelector::Unknown { .. } => None,
    }
}

pub fn hydrate_needed_context(frame: &mut StateFrame, requested: &[String]) -> HydrationSummary {
    let mut summary = HydrationSummary::default();
    let (selected, deferred) = select_context_requests(frame, requested);
    let excerpt_chars = estimate_excerpt_chars(frame, selected.len());

    for selector in deferred {
        let deferred_line = format!(
            "context_deferred: {} reason=budget",
            selector_key(&selector)
        );
        if push_unique(&mut frame.recent_evidence, deferred_line.clone()) {
            summary.changed = true;
        }
        push_unique(&mut summary.deferred, deferred_line);
    }

    for selector in selected {
        if let Some(hydrated) = hydrate_selector(frame, &selector, excerpt_chars) {
            if push_unique(&mut frame.recent_evidence, hydrated.clone()) {
                summary.changed = true;
            }
            push_unique(&mut summary.hydrated, hydrated);
            continue;
        }

        let unavailable = format!("context_unavailable: {}", selector_key(&selector));
        if push_unique(&mut frame.recent_evidence, unavailable.clone()) {
            summary.changed = true;
        }
        push_unique(&mut summary.unavailable, unavailable);
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::{NeededContextSelector, hydrate_needed_context, parse_needed_context_selector};
    use crate::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};

    fn make_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective:
                "update src/core/state_frame_projection.rs around BossCoordinator artifact output"
                    .into(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: file_facts ref=filefact:1 path=src/core/state_frame_projection.rs kind=target_file source=step_objective freshness=current confidence=1.00 symbol=BossCoordinator fact=step objective names this path as concrete context: src/core/state_frame_projection.rs".into(),
                "fact: recent_changes_in_files ref=change:1 path=src/core/state_frame_projection.rs source=worker_result freshness=after-worker-output confidence=0.90 summary=updated src/core/state_frame_projection.rs".into(),
                "fact: test_failures ref=test:1 name=worker_reported_tests status=failed source=worker_result freshness=after-worker-output confidence=0.85 summary=tests failed in boss_flow".into(),
                "fact: review_verdicts ref=review:step1:runtime:0 verdict=accepted source=tool:BossReview freshness=after-runtime-review confidence=1.00 summary=LGTM after targeted review".into(),
                "fact: artifact_status ref=artifact:step1:runtime:0 path=/tmp/report.md kind=file status=verified source=tool:ArtifactVerify freshness=after-runtime-artifact-verify confidence=1.00 summary=artifact verification passed for /tmp/report.md".into(),
                "fact: open_item_refs ref=openitem:step1:0 source=acceptance:0 freshness=current confidence=1.00 summary=tests pass".into(),
                "fact: blocker_refs ref=blocker:step1:0 source=stage:waitingforapproval freshness=current confidence=1.00 summary=waiting for user approval".into(),
                "fact: rejected_approaches ref=rejected:step1:0 source=review_correction source_ref=review:step1:runtime:1 freshness=after-review confidence=1.00 summary=previous patch ignored edge cases correction=preserve the auth guard branch".into(),
            ],
            allowed_actions: vec!["read_file".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
        }
    }

    #[test]
    fn parse_needed_context_selector_supports_typed_keys() {
        assert_eq!(
            parse_needed_context_selector("file_snippet:src/core/boss.rs"),
            NeededContextSelector::FileSnippet {
                path: "src/core/boss.rs".into()
            }
        );
        assert_eq!(
            parse_needed_context_selector("test_failure"),
            NeededContextSelector::TestFailure { query: None }
        );
        assert_eq!(
            parse_needed_context_selector("review_ref:review:step1:runtime:0"),
            NeededContextSelector::ReviewRef {
                query: Some("review:step1:runtime:0".into())
            }
        );
        assert_eq!(
            parse_needed_context_selector("artifact_ref"),
            NeededContextSelector::ArtifactRef { query: None }
        );
        assert_eq!(
            parse_needed_context_selector("open_item_ref"),
            NeededContextSelector::OpenItemRef { query: None }
        );
        assert_eq!(
            parse_needed_context_selector("blocker_ref:blocker:step1:0"),
            NeededContextSelector::BlockerRef {
                query: Some("blocker:step1:0".into())
            }
        );
        assert_eq!(
            parse_needed_context_selector("rejected_approach"),
            NeededContextSelector::RejectedApproach { query: None }
        );
    }

    #[test]
    fn hydrate_needed_context_resolves_file_change_and_test_requests() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "file_snippet:src/core/state_frame_projection.rs".into(),
                "change_ref:src/core/state_frame_projection.rs".into(),
                "test_failure".into(),
            ],
        );

        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert_eq!(summary.hydrated.len(), 3);
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: file_snippet:src/core/state_frame_projection.rs")
        }));
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("hydrated_context: test_failure"))
        );
    }

    #[test]
    fn hydrate_needed_context_marks_unavailable_when_no_match() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(&mut frame, &["symbol:MissingSymbol".into()]);
        assert!(summary.changed);
        assert_eq!(
            summary.unavailable,
            vec!["context_unavailable: symbol:MissingSymbol"]
        );
    }

    #[test]
    fn hydrate_needed_context_resolves_symbol_and_artifact_requests() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "symbol:BossCoordinator".into(),
                "artifact:src/core/state_frame_projection.rs".into(),
            ],
        );
        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("hydrated_context: symbol:BossCoordinator"))
        );
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact:src/core/state_frame_projection.rs")
        }));
    }

    #[test]
    fn hydrate_needed_context_resolves_review_ref_and_artifact_ref_requests() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "review_ref:review:step1:runtime:0".into(),
                "artifact_ref:artifact:step1:runtime:0".into(),
            ],
        );
        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("hydrated_context: review_ref:review:step1:runtime:0"))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("source=review_ledger"))
        );
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact_ref:artifact:step1:runtime:0")
        }));
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("source=artifact_ledger"))
        );
    }

    #[test]
    fn hydrate_needed_context_resolves_open_blocker_and_rejected_refs() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "open_item_ref:openitem:step1:0".into(),
                "blocker_ref:blocker:step1:0".into(),
                "rejected_approach:rejected:step1:0".into(),
            ],
        );
        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("source=open_item_ledger"))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("source=blocker_ledger"))
        );
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("source=rejected_approach_ledger"))
        );
    }

    #[test]
    fn hydrate_needed_context_defers_low_priority_requests_under_budget() {
        let mut frame = make_frame();
        frame.budget.max_input_tokens = 250;
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "test_failure".into(),
                "change_ref:src/core/state_frame_projection.rs".into(),
                "symbol:BossCoordinator".into(),
            ],
        );
        assert!(summary.changed);
        assert!(!summary.deferred.is_empty());
        assert!(
            summary
                .deferred
                .iter()
                .any(|item| item.contains("context_deferred: symbol:BossCoordinator"))
        );
    }
}

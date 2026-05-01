use crate::core::state_frame::StateFrame;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NeededContextSelector {
    FileSnippet { path: String },
    Symbol { name: String },
    TestFailure { query: Option<String> },
    ChangeRef { path: Option<String> },
    Artifact { path: Option<String> },
    Fact { name: String },
    Unknown { raw: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HydrationSummary {
    pub changed: bool,
    pub hydrated: Vec<String>,
    pub unavailable: Vec<String>,
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
        NeededContextSelector::Artifact { path: Some(path) } => format!("artifact:{path}"),
        NeededContextSelector::Artifact { path: None } => "artifact".into(),
        NeededContextSelector::Fact { name } => format!("fact:{name}"),
        NeededContextSelector::Unknown { raw } => raw.clone(),
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

fn hydrate_selector(frame: &StateFrame, selector: &NeededContextSelector) -> Option<String> {
    match selector {
        NeededContextSelector::FileSnippet { path } => find_recent_evidence(frame, "fact: file_facts")
            .find(|item| contains_path(item, path))
            .map(|item| {
                format!(
                    "hydrated_context: {} source=fact_ledger excerpt={}",
                    selector_key(selector),
                    compact_excerpt(item, 180)
                )
            })
            .or_else(|| {
                find_recent_evidence(frame, "fact: recent_changes_in_files")
                    .find(|item| contains_path(item, path))
                    .map(|item| {
                        format!(
                            "hydrated_context: {} source=change_ledger excerpt={}",
                            selector_key(selector),
                            compact_excerpt(item, 180)
                        )
                    })
            }),
        NeededContextSelector::TestFailure { query } => {
            find_recent_evidence(frame, "fact: test_failures")
                .find(|item| {
                    item.contains("status=failed")
                        && query
                            .as_ref()
                            .map(|q| item.contains(q))
                            .unwrap_or(true)
                })
                .map(|item| {
                    format!(
                        "hydrated_context: {} source=test_ledger excerpt={}",
                        selector_key(selector),
                        compact_excerpt(item, 180)
                    )
                })
        }
        NeededContextSelector::ChangeRef { path } => find_recent_evidence(
            frame,
            "fact: recent_changes_in_files",
        )
        .find(|item| path.as_ref().map(|p| contains_path(item, p)).unwrap_or(true))
        .map(|item| {
            format!(
                "hydrated_context: {} source=change_ledger excerpt={}",
                selector_key(selector),
                compact_excerpt(item, 180)
            )
        }),
        NeededContextSelector::Artifact { path } => {
            let match_in_objective = path
                .as_ref()
                .filter(|p| frame.objective.contains(p.as_str()))
                .map(|p| {
                    format!(
                        "hydrated_context: {} source=objective excerpt={}",
                        selector_key(selector),
                        compact_excerpt(&format!("objective references artifact path {p}"), 180)
                    )
                });
            match_in_objective.or_else(|| {
                find_recent_evidence(frame, "fact: file_facts")
                    .find(|item| path.as_ref().map(|p| contains_path(item, p)).unwrap_or(true))
                    .map(|item| {
                        format!(
                            "hydrated_context: {} source=fact_ledger excerpt={}",
                            selector_key(selector),
                            compact_excerpt(item, 180)
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
                    compact_excerpt(item, 180)
                )
            }),
        NeededContextSelector::Symbol { .. } | NeededContextSelector::Unknown { .. } => None,
    }
}

pub fn hydrate_needed_context(frame: &mut StateFrame, requested: &[String]) -> HydrationSummary {
    let mut summary = HydrationSummary::default();

    for raw in requested {
        let selector = parse_needed_context_selector(raw);
        if let Some(hydrated) = hydrate_selector(frame, &selector) {
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
    use super::{hydrate_needed_context, parse_needed_context_selector, NeededContextSelector};
    use crate::core::state_frame::{ActorRole, AgentState, StateBudget, StateFrame};

    fn make_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "update src/core/state_frame_projection.rs".into(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: file_facts ref=filefact:1 path=src/core/state_frame_projection.rs kind=target_file source=step_objective freshness=current confidence=1.00 fact=step objective names this path as concrete context: src/core/state_frame_projection.rs".into(),
                "fact: recent_changes_in_files ref=change:1 path=src/core/state_frame_projection.rs source=worker_result freshness=after-worker-output confidence=0.90 summary=updated src/core/state_frame_projection.rs".into(),
                "fact: test_failures ref=test:1 name=worker_reported_tests status=failed source=worker_result freshness=after-worker-output confidence=0.85 summary=tests failed in boss_flow".into(),
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
        assert!(frame
            .recent_evidence
            .iter()
            .any(|item| item.contains("hydrated_context: file_snippet:src/core/state_frame_projection.rs")));
        assert!(frame
            .recent_evidence
            .iter()
            .any(|item| item.contains("hydrated_context: test_failure")));
    }

    #[test]
    fn hydrate_needed_context_marks_unavailable_when_no_match() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(&mut frame, &["symbol:BossCoordinator".into()]);
        assert!(summary.changed);
        assert_eq!(
            summary.unavailable,
            vec!["context_unavailable: symbol:BossCoordinator"]
        );
    }
}

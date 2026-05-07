use crate::core::state_frame::StateFrame;
use std::collections::HashMap;

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
    pub stale: Vec<String>,
    pub hydration_from_contract_count: usize,
    pub hydration_from_ledger_count: usize,
    pub hydration_miss_unsupported_count: usize,
    pub hydration_miss_stale_count: usize,
    pub hydration_miss_no_match_count: usize,
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
    if let Some(path) = trimmed.strip_prefix("artifact_status:") {
        let normalized = artifact_lookup_query(path);
        if normalized.starts_with("artifact:") {
            return NeededContextSelector::ArtifactRef {
                query: Some(normalized.to_string()),
            };
        }
        return NeededContextSelector::Artifact {
            path: Some(normalized.to_string()),
        };
    }
    if trimmed.starts_with("permission:")
        || trimmed.starts_with("operator_action:")
        || trimmed.starts_with("operator_action_hint:")
    {
        return NeededContextSelector::Unknown {
            raw: trimmed.to_string(),
        };
    }
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
        let canonical = match name.trim() {
            "allow_runtime_tool_calls" => "allow_worker_tool_calls",
            "increase_tool_call_quota" => "increase_max_tool_calls",
            other => other,
        };
        return NeededContextSelector::Fact {
            name: canonical.to_string(),
        };
    }
    NeededContextSelector::Unknown {
        raw: trimmed.to_string(),
    }
}

#[derive(Debug, Clone)]
struct ParsedEvidenceFact {
    fact_name: String,
    raw: String,
    fields: HashMap<String, String>,
    none_recorded: bool,
}

impl ParsedEvidenceFact {
    fn field(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }
}

#[derive(Debug, Clone, Default)]
struct TypedEvidenceIndex {
    facts: Vec<ParsedEvidenceFact>,
}

impl TypedEvidenceIndex {
    fn from_frame(frame: &StateFrame) -> Self {
        Self {
            facts: frame
                .recent_evidence
                .iter()
                .filter_map(|item| parse_fact_line(item))
                .collect(),
        }
    }

    fn facts_named<'a>(&'a self, fact_name: &str) -> impl Iterator<Item = &'a ParsedEvidenceFact> {
        self.facts
            .iter()
            .filter(move |item| item.fact_name == fact_name && !item.none_recorded)
    }
}

#[derive(Debug, Clone)]
struct ContractArtifactEntry {
    ref_id: String,
    path: String,
    kind: String,
}

#[derive(Debug, Clone)]
struct ContractVerificationEntry {
    target_ref: String,
    target_path: Option<String>,
}

#[derive(Debug, Clone)]
struct ContractTestEntry {
    name: String,
}

#[derive(Debug, Clone, Default)]
struct TypedContractIndex {
    artifacts: Vec<ContractArtifactEntry>,
    verifications: Vec<ContractVerificationEntry>,
    tests: Vec<ContractTestEntry>,
}

impl TypedContractIndex {
    fn from_frame(frame: &StateFrame) -> Self {
        Self {
            artifacts: frame
                .stage_execution_contract
                .declared_artifacts
                .iter()
                .map(|item| ContractArtifactEntry {
                    ref_id: item.ref_id.clone(),
                    path: item.path.clone(),
                    kind: item.kind.clone(),
                })
                .collect(),
            verifications: frame
                .stage_execution_contract
                .verifications
                .iter()
                .map(|item| ContractVerificationEntry {
                    target_ref: item.target_ref.clone(),
                    target_path: item.target_path.clone(),
                })
                .collect(),
            tests: frame
                .stage_execution_contract
                .tests
                .iter()
                .map(|item| ContractTestEntry {
                    name: item.name.clone(),
                })
                .collect(),
        }
    }
}

const FACT_FIELD_KEYS: &[&str] = &[
    "ref",
    "path",
    "kind",
    "source",
    "source_ref",
    "source_event_id",
    "freshness",
    "confidence",
    "status",
    "lineage_status",
    "invalidated_by",
    "supersedes",
    "conflicts_with",
    "symbol",
    "name",
    "verdict",
    "required_state",
    "target_path",
    "parent_dir",
    "permission_ref",
    "missing_reason",
    "recommended_write_strategy",
    "summary",
    "fact",
    "correction",
];

fn parse_fact_line(raw: &str) -> Option<ParsedEvidenceFact> {
    let rest = raw.strip_prefix("fact: ")?;
    let (fact_name, body) = match rest.split_once(' ') {
        Some((name, body)) => (name.trim().to_string(), body.trim()),
        None => (rest.trim().to_string(), ""),
    };
    if body == "none recorded" {
        return Some(ParsedEvidenceFact {
            fact_name,
            raw: raw.to_string(),
            fields: HashMap::new(),
            none_recorded: true,
        });
    }

    let mut positions = Vec::new();
    for key in FACT_FIELD_KEYS {
        let needle = format!("{key}=");
        let mut search_from = 0usize;
        while let Some(found) = body[search_from..].find(&needle) {
            let idx = search_from + found;
            let boundary_ok = idx == 0 || body[..idx].ends_with(' ');
            if boundary_ok {
                positions.push((idx, *key));
            }
            search_from = idx + needle.len();
        }
    }
    positions.sort_by_key(|(idx, _)| *idx);
    positions.dedup_by_key(|(idx, _)| *idx);

    let mut fields = HashMap::new();
    for (i, (start, key)) in positions.iter().enumerate() {
        let value_start = start + key.len() + 1;
        let value_end = positions
            .get(i + 1)
            .map(|(next, _)| *next)
            .unwrap_or(body.len());
        let value = body[value_start..value_end].trim();
        if !value.is_empty() {
            fields.insert((*key).to_string(), value.to_string());
        }
    }

    Some(ParsedEvidenceFact {
        fact_name,
        raw: raw.to_string(),
        fields,
        none_recorded: false,
    })
}

fn is_none_like(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim),
        None | Some("") | Some("none") | Some("none recorded")
    )
}

fn field_eq(item: &ParsedEvidenceFact, key: &str, expected: &str) -> bool {
    item.field(key)
        .map(|value| value.trim() == expected.trim())
        .unwrap_or(false)
}

fn matches_any_query(item: &ParsedEvidenceFact, query: &str, keys: &[&str]) -> bool {
    let query = query.trim();
    keys.iter().any(|key| {
        item.field(key)
            .map(|value| value.trim() == query || value.contains(query))
            .unwrap_or(false)
    })
}

fn artifact_lookup_query(raw: &str) -> &str {
    raw.trim()
        .strip_suffix(":exists_confirmation")
        .unwrap_or_else(|| raw.trim())
}

fn matches_artifact_query(item: &ParsedEvidenceFact, query: &str) -> bool {
    let normalized = artifact_lookup_query(query);
    matches_any_query(
        item,
        normalized,
        &["ref", "path", "source_event_id", "summary"],
    )
}

fn trace_string(item: &ParsedEvidenceFact) -> String {
    format!(
        "fact_name={} ref={} source={} source_event_id={} freshness={}",
        item.fact_name,
        item.field("ref").unwrap_or("none"),
        item.field("source").unwrap_or("unknown"),
        item.field("source_event_id").unwrap_or("unknown"),
        item.field("freshness").unwrap_or("unknown"),
    )
}

fn stale_reason(item: &ParsedEvidenceFact) -> Option<String> {
    let lineage_status = item.field("lineage_status").or_else(|| {
        (!matches!(item.fact_name.as_str(), "artifact_status" | "test_failures"))
            .then(|| item.field("status"))
            .flatten()
    });
    if let Some(status) = lineage_status {
        if status != "active" {
            return Some(format!("lineage_status={status}"));
        }
    }
    if !is_none_like(item.field("invalidated_by")) {
        return Some(format!(
            "invalidated_by={}",
            item.field("invalidated_by").unwrap_or("unknown")
        ));
    }
    None
}

fn format_hydrated_line(
    selector: &NeededContextSelector,
    source: &str,
    match_reason: &str,
    item: &ParsedEvidenceFact,
    excerpt_chars: usize,
) -> String {
    let selector = selector_key(selector);
    let mut line = format!(
        "hydrated_context: {} source={} match_reason={} trace={} excerpt={}",
        selector,
        source,
        match_reason,
        trace_string(item),
        compact_excerpt(&item.raw, excerpt_chars)
    );
    if selector.ends_with(":exists_confirmation") && item.fact_name == "artifact_status" {
        line.push_str(" selector_note=existence_confirmation_not_readable_path");
        if item.field("kind") == Some("directory")
            && matches!(
                item.field("status"),
                Some("expected") | Some("missing_or_invalid") | Some("touched")
            )
        {
            line.push_str(" action_hint=create_directory_then_write_files");
        }
    }
    line
}

fn format_stale_line(
    selector: &NeededContextSelector,
    source: &str,
    item: &ParsedEvidenceFact,
    reason: &str,
) -> String {
    format!(
        "context_stale: {} source={} stale_reason={} trace={}",
        selector_key(selector),
        source,
        reason,
        trace_string(item)
    )
}

fn format_unavailable_line(selector: &NeededContextSelector, reason: &str, source: &str) -> String {
    format!(
        "context_unavailable: {} reason={} resolver={}",
        selector_key(selector),
        reason,
        source
    )
}

fn format_contract_line(
    selector: &NeededContextSelector,
    source: &str,
    match_reason: &str,
    excerpt: String,
) -> String {
    format!(
        "hydrated_context: {} source={} match_reason={} excerpt={}",
        selector_key(selector),
        source,
        match_reason,
        excerpt
    )
}

fn runtime_budget_fact_line(frame: &StateFrame) -> String {
    let value = if frame.budget.max_tool_calls == 0 {
        "unlimited".to_string()
    } else {
        frame.budget.max_tool_calls.to_string()
    };
    format!(
        "hydrated_context: fact:budget.max_tool_calls source=state_frame_budget match_reason=runtime_budget excerpt=max_tool_calls={} effective_value={} semantics={}",
        frame.budget.max_tool_calls,
        value,
        if frame.budget.max_tool_calls == 0 {
            "0_means_unlimited"
        } else {
            "hard_cap"
        }
    )
}

fn runtime_allow_worker_tool_calls_line(frame: &StateFrame) -> String {
    format!(
        "hydrated_context: fact:allow_worker_tool_calls source=state_frame_contract match_reason=allowed_actions excerpt=status={} allowed_actions={} allowed_tools={}",
        if frame.allowed_actions.is_empty() {
            "not_allowed"
        } else {
            "allowed"
        },
        if frame.allowed_actions.is_empty() {
            "none".to_string()
        } else {
            frame.allowed_actions.join("|")
        },
        if frame.allowed_tools.is_empty() {
            "none".to_string()
        } else {
            frame.allowed_tools.join("|")
        }
    )
}

fn runtime_increase_max_tool_calls_line(frame: &StateFrame) -> String {
    format!(
        "hydrated_context: fact:increase_max_tool_calls source=state_frame_budget match_reason=runtime_budget excerpt=status={} reason={}",
        if frame.budget.max_tool_calls == 0 {
            "not_needed"
        } else {
            "available_if_cap_exhausted"
        },
        if frame.budget.max_tool_calls == 0 {
            "max_tool_calls_already_unlimited"
        } else {
            "current_budget_is_capped"
        }
    )
}

enum SelectorResolution {
    Hydrated {
        line: String,
        source_kind: HydrationSourceKind,
    },
    Stale(String),
    Unavailable {
        line: String,
        miss_kind: HydrationMissKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HydrationSourceKind {
    Contract,
    Ledger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HydrationMissKind {
    Unsupported,
    NoMatch,
}

fn resolve_fact_match<F>(
    index: &TypedEvidenceIndex,
    fact_name: &str,
    selector: &NeededContextSelector,
    excerpt_chars: usize,
    source: &str,
    match_reason: &str,
    predicate: F,
) -> SelectorResolution
where
    F: Fn(&ParsedEvidenceFact) -> bool,
{
    let matches = index
        .facts_named(fact_name)
        .filter(|item| predicate(item))
        .collect::<Vec<_>>();
    if let Some(item) = matches
        .iter()
        .copied()
        .find(|item| stale_reason(item).is_none())
    {
        return SelectorResolution::Hydrated {
            line: format_hydrated_line(selector, source, match_reason, item, excerpt_chars),
            source_kind: HydrationSourceKind::Ledger,
        };
    }
    if let Some(item) = matches.first().copied() {
        return SelectorResolution::Stale(format_stale_line(
            selector,
            source,
            item,
            &stale_reason(item).unwrap_or_else(|| "stale_match".into()),
        ));
    }
    SelectorResolution::Unavailable {
        line: format_unavailable_line(selector, "no_match", "typed_index"),
        miss_kind: HydrationMissKind::NoMatch,
    }
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

fn hydrate_selector(
    index: &TypedEvidenceIndex,
    contract_index: &TypedContractIndex,
    frame: &StateFrame,
    selector: &NeededContextSelector,
    excerpt_chars: usize,
) -> SelectorResolution {
    match selector {
        NeededContextSelector::FileSnippet { path } => match resolve_fact_match(
            index,
            "file_facts",
            selector,
            excerpt_chars,
            "fact_ledger",
            "path",
            |item| field_eq(item, "path", path),
        ) {
            SelectorResolution::Unavailable { .. } => resolve_fact_match(
                index,
                "recent_changes_in_files",
                selector,
                excerpt_chars,
                "change_ledger",
                "path",
                |item| field_eq(item, "path", path),
            ),
            resolved => resolved,
        },
        NeededContextSelector::TestFailure { query } => {
            let query = query.as_deref().map(str::trim);
            if let Some(contract_test) = contract_index.tests.iter().find(|item| {
                query
                    .map(|q| item.name == q || item.name.contains(q))
                    .unwrap_or(true)
            }) {
                let contract_match = resolve_fact_match(
                    index,
                    "test_failures",
                    selector,
                    excerpt_chars,
                    "test_ledger",
                    "declared_test_contract",
                    |item| {
                        field_eq(item, "status", "failed")
                            && field_eq(item, "name", &contract_test.name)
                    },
                );
                if !matches!(contract_match, SelectorResolution::Unavailable { .. }) {
                    return contract_match;
                }
                return SelectorResolution::Hydrated {
                    line: format_contract_line(
                        selector,
                        "stage_execution_contract",
                        "declared_test_contract",
                        compact_excerpt(
                            &format!("declared test contract name={}", contract_test.name),
                            excerpt_chars,
                        ),
                    ),
                    source_kind: HydrationSourceKind::Contract,
                };
            }
            resolve_fact_match(
                index,
                "test_failures",
                selector,
                excerpt_chars,
                "test_ledger",
                if query.is_some() {
                    "query"
                } else {
                    "latest_failed_test"
                },
                |item| {
                    field_eq(item, "status", "failed")
                        && query
                            .map(|q| {
                                matches_any_query(
                                    item,
                                    q,
                                    &["ref", "name", "source_event_id", "summary"],
                                )
                            })
                            .unwrap_or(true)
                },
            )
        }
        NeededContextSelector::ChangeRef { path } => resolve_fact_match(
            index,
            "recent_changes_in_files",
            selector,
            excerpt_chars,
            "change_ledger",
            if path.is_some() {
                "path_or_source_event"
            } else {
                "latest_change"
            },
            |item| {
                path.as_ref()
                    .map(|query| {
                        matches_any_query(item, query, &["ref", "path", "source_event_id"])
                    })
                    .unwrap_or(true)
            },
        ),
        NeededContextSelector::ReviewRef { query } => resolve_fact_match(
            index,
            "review_verdicts",
            selector,
            excerpt_chars,
            "review_ledger",
            if query.is_some() {
                "ref_or_source_event"
            } else {
                "latest_review"
            },
            |item| {
                query
                    .as_ref()
                    .map(|q| {
                        matches_any_query(
                            item,
                            q,
                            &["ref", "verdict", "source_event_id", "source", "summary"],
                        )
                    })
                    .unwrap_or(true)
            },
        ),
        NeededContextSelector::ArtifactRef { query } => {
            let query = query.as_deref().map(artifact_lookup_query);
            if let Some(contract_artifact) = contract_index.artifacts.iter().find(|item| {
                query
                    .map(|q| {
                        item.ref_id == q
                            || item.path == q
                            || item.ref_id.contains(q)
                            || item.path.contains(q)
                    })
                    .unwrap_or(true)
            }) {
                let contract_match = resolve_fact_match(
                    index,
                    "artifact_status",
                    selector,
                    excerpt_chars,
                    "artifact_ledger",
                    "declared_artifact_contract",
                    |item| {
                        field_eq(item, "ref", &contract_artifact.ref_id)
                            || field_eq(item, "path", &contract_artifact.path)
                    },
                );
                if !matches!(contract_match, SelectorResolution::Unavailable { .. }) {
                    return contract_match;
                }
                return SelectorResolution::Hydrated {
                    line: format_contract_line(
                        selector,
                        "stage_execution_contract",
                        "declared_artifact_contract",
                        compact_excerpt(
                            &format!(
                                "declared artifact contract ref={} path={} kind={}",
                                contract_artifact.ref_id,
                                contract_artifact.path,
                                contract_artifact.kind
                            ),
                            excerpt_chars,
                        ),
                    ),
                    source_kind: HydrationSourceKind::Contract,
                };
            }
            resolve_fact_match(
                index,
                "artifact_status",
                selector,
                excerpt_chars,
                "artifact_ledger",
                if query.is_some() {
                    "ref_path_or_source_event"
                } else {
                    "latest_artifact"
                },
                |item| {
                    query
                        .map(|q| matches_artifact_query(item, q))
                        .unwrap_or(true)
                },
            )
        }
        NeededContextSelector::OpenItemRef { query } => resolve_fact_match(
            index,
            "open_item_refs",
            selector,
            excerpt_chars,
            "open_item_ledger",
            if query.is_some() {
                "ref_or_source_event"
            } else {
                "latest_open_item"
            },
            |item| {
                query
                    .as_ref()
                    .map(|q| matches_any_query(item, q, &["ref", "source_event_id", "summary"]))
                    .unwrap_or(true)
            },
        ),
        NeededContextSelector::BlockerRef { query } => resolve_fact_match(
            index,
            "blocker_refs",
            selector,
            excerpt_chars,
            "blocker_ledger",
            if query.is_some() {
                "ref_or_source_event"
            } else {
                "latest_blocker"
            },
            |item| {
                query
                    .as_ref()
                    .map(|q| matches_any_query(item, q, &["ref", "source_event_id", "summary"]))
                    .unwrap_or(true)
            },
        ),
        NeededContextSelector::RejectedApproach { query } => resolve_fact_match(
            index,
            "rejected_approaches",
            selector,
            excerpt_chars,
            "rejected_approach_ledger",
            if query.is_some() {
                "ref_or_source_event"
            } else {
                "latest_rejected_approach"
            },
            |item| {
                query
                    .as_ref()
                    .map(|q| {
                        matches_any_query(
                            item,
                            q,
                            &[
                                "ref",
                                "source_ref",
                                "source_event_id",
                                "summary",
                                "correction",
                            ],
                        )
                    })
                    .unwrap_or(true)
            },
        ),
        NeededContextSelector::Artifact { path } => {
            let path = path.as_deref().map(artifact_lookup_query);
            if let Some(contract_artifact) = contract_index
                .artifacts
                .iter()
                .find(|item| path.map(|p| item.path == p).unwrap_or(true))
            {
                let contract_match = resolve_fact_match(
                    index,
                    "artifact_status",
                    selector,
                    excerpt_chars,
                    "artifact_ledger",
                    "declared_artifact_contract",
                    |item| field_eq(item, "path", &contract_artifact.path),
                );
                if !matches!(contract_match, SelectorResolution::Unavailable { .. }) {
                    return contract_match;
                }
                return SelectorResolution::Hydrated {
                    line: format_contract_line(
                        selector,
                        "stage_execution_contract",
                        "declared_artifact_contract",
                        compact_excerpt(
                            &format!(
                                "declared artifact contract ref={} path={} kind={}",
                                contract_artifact.ref_id,
                                contract_artifact.path,
                                contract_artifact.kind
                            ),
                            excerpt_chars,
                        ),
                    ),
                    source_kind: HydrationSourceKind::Contract,
                };
            }
            let artifact_resolution = resolve_fact_match(
                index,
                "artifact_status",
                selector,
                excerpt_chars,
                "artifact_ledger",
                if path.is_some() {
                    "path"
                } else {
                    "latest_artifact"
                },
                |item| path.map(|p| field_eq(item, "path", p)).unwrap_or(true),
            );
            match artifact_resolution {
                SelectorResolution::Unavailable { .. } => match resolve_fact_match(
                    index,
                    "recent_changes_in_files",
                    selector,
                    excerpt_chars,
                    "change_ledger",
                    if path.is_some() {
                        "artifact_change_path"
                    } else {
                        "latest_change"
                    },
                    |item| path.map(|p| field_eq(item, "path", p)).unwrap_or(true),
                ) {
                    SelectorResolution::Unavailable { .. } => match resolve_fact_match(
                        index,
                        "file_facts",
                        selector,
                        excerpt_chars,
                        "fact_ledger",
                        if path.is_some() {
                            "artifact_fact_path"
                        } else {
                            "latest_file_fact"
                        },
                        |item| path.map(|p| field_eq(item, "path", p)).unwrap_or(true),
                    ) {
                        SelectorResolution::Unavailable { .. } => SelectorResolution::Unavailable {
                            line: format_unavailable_line(selector, "no_match", "typed_index"),
                            miss_kind: HydrationMissKind::NoMatch,
                        },
                        resolved => resolved,
                    },
                    resolved => resolved,
                },
                resolved => resolved,
            }
        }
        NeededContextSelector::Fact { name } => match name.as_str() {
            "budget.max_tool_calls" => SelectorResolution::Hydrated {
                line: runtime_budget_fact_line(frame),
                source_kind: HydrationSourceKind::Contract,
            },
            "allow_worker_tool_calls" => SelectorResolution::Hydrated {
                line: runtime_allow_worker_tool_calls_line(frame),
                source_kind: HydrationSourceKind::Contract,
            },
            "increase_max_tool_calls" => SelectorResolution::Hydrated {
                line: runtime_increase_max_tool_calls_line(frame),
                source_kind: HydrationSourceKind::Contract,
            },
            _ => resolve_fact_match(
                index,
                name,
                selector,
                excerpt_chars,
                "fact_ledger",
                "fact_name",
                |_| true,
            ),
        },
        NeededContextSelector::Symbol { name } => {
            let symbol_resolution = resolve_fact_match(
                index,
                "file_facts",
                selector,
                excerpt_chars,
                "fact_ledger",
                "symbol",
                |item| field_eq(item, "symbol", name),
            );
            match symbol_resolution {
                SelectorResolution::Unavailable { .. } => SelectorResolution::Unavailable {
                    line: format_unavailable_line(selector, "no_symbol_match", "typed_index"),
                    miss_kind: HydrationMissKind::NoMatch,
                },
                resolved => resolved,
            }
        }
        NeededContextSelector::Unknown { .. } => SelectorResolution::Unavailable {
            line: format_unavailable_line(selector, "unsupported_selector", "typed_index"),
            miss_kind: HydrationMissKind::Unsupported,
        },
    }
}

pub fn hydrate_needed_context(frame: &mut StateFrame, requested: &[String]) -> HydrationSummary {
    let mut summary = HydrationSummary::default();
    let (selected, deferred) = select_context_requests(frame, requested);
    let excerpt_chars = estimate_excerpt_chars(frame, selected.len());
    let index = TypedEvidenceIndex::from_frame(frame);
    let contract_index = TypedContractIndex::from_frame(frame);

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
        match hydrate_selector(&index, &contract_index, frame, &selector, excerpt_chars) {
            SelectorResolution::Hydrated { line, source_kind } => {
                let hydrated = line;
                if push_unique(&mut frame.recent_evidence, hydrated.clone()) {
                    summary.changed = true;
                }
                match source_kind {
                    HydrationSourceKind::Contract => summary.hydration_from_contract_count += 1,
                    HydrationSourceKind::Ledger => summary.hydration_from_ledger_count += 1,
                }
                push_unique(&mut summary.hydrated, hydrated);
            }
            SelectorResolution::Stale(stale) => {
                if push_unique(&mut frame.recent_evidence, stale.clone()) {
                    summary.changed = true;
                }
                summary.hydration_miss_stale_count += 1;
                push_unique(&mut summary.stale, stale);
            }
            SelectorResolution::Unavailable {
                line: unavailable,
                miss_kind,
            } => {
                if push_unique(&mut frame.recent_evidence, unavailable.clone()) {
                    summary.changed = true;
                }
                match miss_kind {
                    HydrationMissKind::Unsupported => {
                        summary.hydration_miss_unsupported_count += 1;
                    }
                    HydrationMissKind::NoMatch => {
                        summary.hydration_miss_no_match_count += 1;
                    }
                }
                push_unique(&mut summary.unavailable, unavailable);
            }
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::{NeededContextSelector, hydrate_needed_context, parse_needed_context_selector};
    use crate::core::state_frame::{
        ActorRole, AgentState, StageExecutionContract, StateBudget, StateFrame,
    };

    fn make_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective:
                "update src/core/state_frame_projection.rs around BossCoordinator artifact output"
                    .into(),
            stage_execution_contract: StageExecutionContract::default(),
            open_items: vec!["tests pass".into()],
            blocked_items: Vec::new(),
            accepted_summary: Vec::new(),
            recent_evidence: vec![
                "fact: file_facts ref=filefact:1 path=src/core/state_frame_projection.rs kind=target_file source=step_objective source_event_id=step-objective:1 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none symbol=BossCoordinator fact=step objective names this path as concrete context: src/core/state_frame_projection.rs".into(),
                "fact: recent_changes_in_files ref=change:1 path=src/core/state_frame_projection.rs source=worker_result source_event_id=worker-result:1 freshness=after-worker-output confidence=0.90 status=active invalidated_by=none supersedes=none conflicts_with=none summary=updated src/core/state_frame_projection.rs".into(),
                "fact: test_failures ref=test:1 name=worker_reported_tests status=failed source=worker_result source_event_id=worker-result:1 freshness=after-worker-output confidence=0.85 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=tests failed in boss_flow".into(),
                "fact: review_verdicts ref=review:step1:runtime:0 verdict=accepted source=tool:BossReview source_event_id=tool-review:1:0 freshness=after-runtime-review confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=LGTM after targeted review".into(),
                "fact: artifact_status ref=artifact:step1:runtime:0 path=/tmp/report.md kind=file status=verified source=tool:ArtifactVerify source_event_id=tool-artifact:1:0 freshness=after-runtime-artifact-verify confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=artifact verification passed for /tmp/report.md".into(),
                "fact: preferred_deployment_mode ref=deploymode:step1 source=objective_inference source_event_id=deploymode:1 freshness=current confidence=0.85 status=active invalidated_by=none supersedes=none conflicts_with=none summary=static_site".into(),
                "fact: permission_to_create_and_write:/tmp/report.md ref=permission:step1:0 source=permission_scope source_event_id=permission-scope:1:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=worker may create and write the declared target artifact path /tmp/report.md".into(),
                "fact: open_item_refs ref=openitem:step1:0 source=acceptance:0 source_event_id=step-acceptance:1:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=tests pass".into(),
                "fact: blocker_refs ref=blocker:step1:0 source=stage:waitingforapproval source_event_id=stage-blocker:1:0 freshness=current confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=none summary=waiting for user approval".into(),
                "fact: rejected_approaches ref=rejected:step1:0 source=review_correction source_ref=review:step1:runtime:1 source_event_id=review-correction:1 freshness=after-review confidence=1.00 status=active invalidated_by=none supersedes=none conflicts_with=review:step1:runtime:1 summary=previous patch ignored edge cases correction=preserve the auth guard branch".into(),
            ],
            allowed_actions: vec!["read_file".into()],
            allowed_tools: vec!["Read".into()],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("state_decision_v1".into()),
            budget: StateBudget::default(),
        }
    }

    fn make_contract_frame() -> StateFrame {
        let mut frame = make_frame();
        frame.objective = "generic objective".into();
        frame.stage_execution_contract.declared_artifacts =
            vec![crate::core::state_frame::DeclaredArtifactContract {
                ref_id: "artifact:declared:0".into(),
                path: "/tmp/declared.txt".into(),
                kind: "file".into(),
                required_actions: vec!["create".into(), "write".into()],
                required_evidence: vec![
                    "artifact:declared:0".into(),
                    "/tmp/declared.txt".into(),
                    "file".into(),
                ],
            }];
        frame.stage_execution_contract.verifications =
            vec![crate::core::state_frame::VerificationContract {
                target_ref: "artifact:declared:0".into(),
                target_path: Some("/tmp/declared.txt".into()),
                required_actions: vec!["verify".into()],
                required_evidence: vec!["artifact:declared:0".into(), "/tmp/declared.txt".into()],
            }];
        frame.stage_execution_contract.tests = vec![crate::core::state_frame::TestContract {
            name: "cargo test -p rust_agent".into(),
            required_actions: vec!["run_test".into()],
            required_evidence: vec!["test:declared:0".into()],
        }];
        frame
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
        assert_eq!(
            parse_needed_context_selector("fact:allow_runtime_tool_calls"),
            NeededContextSelector::Fact {
                name: "allow_worker_tool_calls".into()
            }
        );
        assert_eq!(
            parse_needed_context_selector("fact:increase_tool_call_quota"),
            NeededContextSelector::Fact {
                name: "increase_max_tool_calls".into()
            }
        );
        assert_eq!(
            parse_needed_context_selector("artifact_status:/tmp/declared.txt"),
            NeededContextSelector::Artifact {
                path: Some("/tmp/declared.txt".into())
            }
        );
        assert_eq!(
            parse_needed_context_selector("artifact_status:artifact:declared:0"),
            NeededContextSelector::ArtifactRef {
                query: Some("artifact:declared:0".into())
            }
        );
        assert_eq!(
            parse_needed_context_selector("permission:create:/tmp/report.md"),
            NeededContextSelector::Unknown {
                raw: "permission:create:/tmp/report.md".into()
            }
        );
        assert_eq!(
            parse_needed_context_selector("operator_action:write_artifact"),
            NeededContextSelector::Unknown {
                raw: "operator_action:write_artifact".into()
            }
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
                && item.contains("match_reason=path")
                && item.contains("source_event_id=step-objective:1")
        }));
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("hydrated_context: test_failure")
                    && item.contains("source=test_ledger"))
        );
    }

    #[test]
    fn hydrate_needed_context_marks_unavailable_when_no_match() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(&mut frame, &["symbol:MissingSymbol".into()]);
        assert!(summary.changed);
        assert_eq!(
            summary.unavailable,
            vec![
                "context_unavailable: symbol:MissingSymbol reason=no_symbol_match resolver=typed_index"
            ]
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
    fn hydrate_needed_context_prefers_declared_artifact_contract() {
        let mut frame = make_contract_frame();
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "artifact:/tmp/declared.txt".into(),
                "artifact_ref:artifact:declared:0".into(),
                "test_failure:cargo test -p rust_agent".into(),
            ],
        );

        assert!(summary.changed);
        assert!(summary.unavailable.is_empty());
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact:/tmp/declared.txt")
                && item.contains("source=stage_execution_contract")
                && item.contains("declared_artifact_contract")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact_ref:artifact:declared:0")
                && item.contains("source=stage_execution_contract")
                && item.contains("declared_artifact_contract")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: test_failure:cargo test -p rust_agent")
                && item.contains("source=stage_execution_contract")
                && item.contains("declared_test_contract")
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
                .any(|item| item.contains("source=review_ledger")
                    && item.contains("source_event_id=tool-review:1:0"))
        );
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact_ref:artifact:step1:runtime:0")
        }));
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("source=artifact_ledger")
                    && item.contains("source_event_id=tool-artifact:1:0"))
        );
    }

    #[test]
    fn hydrate_needed_context_resolves_artifact_exists_confirmation_requests() {
        let mut frame = make_frame();
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:step1:target path=/tmp/demo-site kind=directory status=missing_or_invalid source=artifact_expectation source_event_id=artifact-expectation:1:0 freshness=current confidence=1.00 lineage_status=active invalidated_by=none supersedes=none conflicts_with=none summary=target directory missing".into(),
        );
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "artifact:/tmp/report.md:exists_confirmation".into(),
                "artifact_ref:/tmp/demo-site:exists_confirmation".into(),
            ],
        );

        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert_eq!(summary.hydrated.len(), 2);
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact:/tmp/report.md:exists_confirmation")
                && item.contains("source=artifact_ledger")
                && item.contains("match_reason=path")
                && item.contains("selector_note=existence_confirmation_not_readable_path")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: artifact_ref:/tmp/demo-site:exists_confirmation")
                && item.contains("source=artifact_ledger")
                && item.contains("action_hint=create_directory_then_write_files")
        }));
    }

    #[test]
    fn hydrate_needed_context_resolves_permission_and_deployment_fact_requests() {
        let mut frame = make_frame();
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "fact:preferred_deployment_mode".into(),
                "fact:permission_to_create_and_write:/tmp/report.md".into(),
            ],
        );
        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert!(
            frame
                .recent_evidence
                .iter()
                .any(|item| item.contains("hydrated_context: fact:preferred_deployment_mode"))
        );
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: fact:permission_to_create_and_write:/tmp/report.md")
        }));
    }

    #[test]
    fn hydrate_needed_context_resolves_runtime_tool_budget_meta_facts() {
        let mut frame = make_frame();
        frame.allowed_actions = vec!["read_file".into(), "edit_file".into(), "run_test".into()];
        frame.allowed_tools = vec!["Read".into(), "Edit".into(), "Bash".into()];
        frame.budget.max_tool_calls = 0;

        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "fact:budget.max_tool_calls".into(),
                "fact:allow_worker_tool_calls".into(),
                "fact:increase_max_tool_calls".into(),
            ],
        );

        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert_eq!(summary.hydrated.len(), 3);
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: fact:budget.max_tool_calls")
                && item.contains("effective_value=unlimited")
                && item.contains("semantics=0_means_unlimited")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: fact:allow_worker_tool_calls")
                && item.contains("status=allowed")
                && item.contains("allowed_actions=read_file|edit_file|run_test")
        }));
        assert!(frame.recent_evidence.iter().any(|item| {
            item.contains("hydrated_context: fact:increase_max_tool_calls")
                && item.contains("status=not_needed")
                && item.contains("max_tool_calls_already_unlimited")
        }));
    }

    #[test]
    fn hydrate_needed_context_canonicalizes_runtime_tool_permission_aliases() {
        let mut frame = make_frame();
        frame.allowed_actions = vec!["read_file".into(), "edit_file".into()];
        frame.allowed_tools = vec!["Read".into(), "Edit".into()];
        frame.budget.max_tool_calls = 0;

        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "fact:allow_runtime_tool_calls".into(),
                "fact:increase_tool_call_quota".into(),
            ],
        );

        assert!(summary.changed);
        assert_eq!(summary.unavailable.len(), 0);
        assert_eq!(summary.hydrated.len(), 2);
        assert!(
            summary
                .hydrated
                .iter()
                .any(|item| item.contains("hydrated_context: fact:allow_worker_tool_calls"))
        );
        assert!(
            summary
                .hydrated
                .iter()
                .any(|item| item.contains("hydrated_context: fact:increase_max_tool_calls"))
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
    fn hydrate_needed_context_reports_stale_ref_reason() {
        let mut frame = make_frame();
        frame.recent_evidence.push(
            "fact: review_verdicts ref=review:step1:stale verdict=rejected source=tool:BossReview source_event_id=tool-review:1:9 freshness=after-runtime-review confidence=1.00 status=stale invalidated_by=review:step1:runtime:0 supersedes=none conflicts_with=none summary=obsolete review verdict".into(),
        );

        let summary = hydrate_needed_context(&mut frame, &["review_ref:review:step1:stale".into()]);
        assert!(summary.changed);
        assert_eq!(summary.hydrated.len(), 0);
        assert_eq!(summary.unavailable.len(), 0);
        assert_eq!(summary.stale.len(), 1);
        assert!(summary.stale[0].contains("context_stale: review_ref:review:step1:stale"));
        assert!(summary.stale[0].contains("stale_reason=lineage_status=stale"));
        assert!(summary.stale[0].contains("source_event_id=tool-review:1:9"));
    }

    #[test]
    fn hydrate_needed_context_distinguishes_unsupported_from_no_match_and_stale() {
        let mut frame = make_contract_frame();
        frame.recent_evidence.push(
            "fact: artifact_status ref=artifact:declared:0 path=/tmp/declared.txt kind=file status=stale source=tool:ArtifactVerify source_event_id=artifact-verify:stale freshness=after-runtime-review confidence=1.00 lineage_status=stale invalidated_by=artifact:declared:0 supersedes=none conflicts_with=none summary=stale artifact".into(),
        );
        let summary = hydrate_needed_context(
            &mut frame,
            &[
                "artifact_ref:artifact:declared:0".into(),
                "symbol:MissingSymbol".into(),
                "bogus_selector".into(),
            ],
        );

        assert!(summary.stale.iter().any(|item| {
            item.contains("context_stale: artifact_ref:artifact:declared:0")
                && item.contains("stale_reason=lineage_status=stale")
        }));
        assert!(summary.unavailable.iter().any(|item| {
            item == "context_unavailable: symbol:MissingSymbol reason=no_symbol_match resolver=typed_index"
        }));
        assert!(summary.unavailable.iter().any(|item| {
            item == "context_unavailable: bogus_selector reason=unsupported_selector resolver=typed_index"
        }));
    }

    #[test]
    fn legacy_artifact_status_selector_is_canonicalized_to_typed_artifact_selector() {
        let mut frame = make_contract_frame();
        let summary =
            hydrate_needed_context(&mut frame, &["artifact_status:/tmp/declared.txt".into()]);

        assert!(summary.changed);
        assert!(summary.unavailable.is_empty());
        assert_eq!(summary.hydration_from_contract_count, 1);
        assert!(
            summary
                .hydrated
                .iter()
                .any(|item| item.contains("hydrated_context: artifact:/tmp/declared.txt"))
        );
    }

    #[test]
    fn unsupported_legacy_selector_stays_explicitly_unsupported() {
        let mut frame = make_contract_frame();
        let summary =
            hydrate_needed_context(&mut frame, &["operator_action:write_artifact".into()]);

        assert!(summary.changed);
        assert_eq!(summary.hydration_miss_unsupported_count, 1);
        assert_eq!(
            summary.unavailable,
            vec![
                "context_unavailable: operator_action:write_artifact reason=unsupported_selector resolver=typed_index"
            ]
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

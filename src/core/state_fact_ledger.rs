use crate::core::boss_acceptance::{
    BossArtifactKind, extract_artifact_expectations, verify_artifact_expectations,
};
use crate::core::boss_state::{BossPlanStep, BossPlanStepStatus, BossStage};
use crate::tool::result::{ToolExecutionOutcomeKind, ToolExecutionRecord};
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFactRecord {
    pub ref_id: String,
    pub path: String,
    pub kind: String,
    pub fact: String,
    pub symbol: Option<String>,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRecord {
    pub ref_id: String,
    pub path: String,
    pub summary: String,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestRecord {
    pub ref_id: String,
    pub name: String,
    pub status: String,
    pub summary: String,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRecord {
    pub ref_id: String,
    pub verdict: String,
    pub summary: String,
    pub correction: Option<String>,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub ref_id: String,
    pub path: String,
    pub kind: String,
    pub status: String,
    pub summary: String,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LedgerLineage {
    pub status: String,
    pub invalidated_by: Vec<String>,
    pub supersedes: Vec<String>,
    pub conflicts_with: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenItemRecord {
    pub ref_id: String,
    pub summary: String,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockerRecord {
    pub ref_id: String,
    pub summary: String,
    pub source: String,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedApproachRecord {
    pub ref_id: String,
    pub summary: String,
    pub correction: Option<String>,
    pub source: String,
    pub source_ref: Option<String>,
    pub source_event_id: String,
    pub freshness: String,
    pub confidence_milli: u16,
    pub lineage: LedgerLineage,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepFactLedgers {
    pub file_facts: Vec<FileFactRecord>,
    pub change_refs: Vec<ChangeRecord>,
    pub test_refs: Vec<TestRecord>,
    pub review_refs: Vec<ReviewRecord>,
    pub artifact_refs: Vec<ArtifactRecord>,
    pub open_item_refs: Vec<OpenItemRecord>,
    pub blocker_refs: Vec<BlockerRecord>,
    pub rejected_approaches: Vec<RejectedApproachRecord>,
}

fn active_lineage() -> LedgerLineage {
    LedgerLineage {
        status: "active".into(),
        invalidated_by: Vec::new(),
        supersedes: Vec::new(),
        conflicts_with: Vec::new(),
    }
}

pub fn active_ledger_lineage() -> LedgerLineage {
    active_lineage()
}

fn trim_excerpt(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut iter = compact.chars();
    let excerpt = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        format!("{excerpt}...")
    } else {
        excerpt
    }
}

fn classify_path_kind(path: &str, line: &str) -> String {
    if line.contains("目标目录") || path.ends_with('/') {
        "target_directory".into()
    } else if line.contains("目标文件") {
        "target_file".into()
    } else if path.ends_with(".rs") {
        "source_file".into()
    } else if path.ends_with(".md") {
        "document".into()
    } else if path.ends_with(".jsonl") || path.ends_with(".json") || path.ends_with(".log") {
        "data_or_log".into()
    } else {
        "path".into()
    }
}

fn normalize_candidate_path(candidate: &str, cwd: Option<&Path>) -> Option<String> {
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

    Some(candidate.to_string())
}

fn extract_path_candidates_with_mode(text: &str, objective_only: bool) -> Vec<(String, String)> {
    let mut paths = Vec::new();
    let cwd = std::env::current_dir().ok();
    for line in text.lines() {
        let trimmed = line.trim();
        if objective_only
            && !(trimmed.starts_with('-')
                || trimmed.starts_with("目标文件")
                || trimmed.starts_with("目标目录")
                || trimmed.starts_with("Output file")
                || trimmed.starts_with("输出文件"))
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
            let candidate = candidate
                .rsplit_once(['：', ':'])
                .map(|(_, suffix)| suffix)
                .filter(|suffix| suffix.contains('/'))
                .unwrap_or(candidate);
            if candidate.is_empty() || candidate == "/" || !candidate.contains('/') {
                continue;
            }
            if !(candidate.ends_with(".rs")
                || candidate.ends_with(".md")
                || candidate.ends_with(".json")
                || candidate.ends_with(".jsonl")
                || candidate.ends_with(".log")
                || candidate.starts_with('/')
                || candidate.starts_with("./")
                || candidate.starts_with("../")
                || candidate.starts_with("src/"))
            {
                continue;
            }
            if let Some(path) = normalize_candidate_path(candidate, cwd.as_deref()) {
                if !paths.iter().any(|(existing, _)| existing == &path) {
                    paths.push((path, trimmed.to_string()));
                }
            }
        }
    }
    paths
}

fn extract_path_candidates(text: &str) -> Vec<(String, String)> {
    let strict = extract_path_candidates_with_mode(text, true);
    if strict.is_empty() {
        extract_path_candidates_with_mode(text, false)
    } else {
        strict
    }
}

fn extract_path_candidates_anywhere(text: &str) -> Vec<(String, String)> {
    extract_path_candidates_with_mode(text, false)
}

fn collect_symbol_candidates(text: &str) -> Vec<String> {
    let mut symbols = Vec::new();
    for token in
        text.split(|c: char| c.is_whitespace() || [',', ';', ':', '：', '(', ')'].contains(&c))
    {
        let candidate = token
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .trim();
        if candidate.len() < 3
            || !candidate
                .chars()
                .any(|ch| ch.is_ascii_alphabetic() || ch == '_')
            || !candidate
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }
        let has_signal =
            candidate.contains('_') || candidate.chars().any(|ch| ch.is_ascii_uppercase());
        if !has_signal {
            continue;
        }
        if !symbols.iter().any(|existing| existing == candidate) {
            symbols.push(candidate.to_string());
        }
    }
    symbols
}

fn extract_symbol_for_path(path: &str, contexts: &[&str]) -> Option<String> {
    let file_name = Path::new(path)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    contexts
        .iter()
        .flat_map(|text| collect_symbol_candidates(text))
        .find(|symbol| symbol != file_name)
}

fn compact_file_observation(path: &str) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    let lines = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(2)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return Some(
            "workspace snapshot confirms file exists but the sampled lines are empty".into(),
        );
    }
    Some(format!(
        "workspace snapshot confirms file exists; sample={}",
        trim_excerpt(&lines.join(" "), 120)
    ))
}

fn push_file_fact(ledger: &mut StepFactLedgers, record: FileFactRecord) {
    let duplicate = ledger.file_facts.iter().any(|existing| {
        existing.path == record.path
            && existing.kind == record.kind
            && existing.source == record.source
            && existing.fact == record.fact
            && existing.symbol == record.symbol
    });
    if !duplicate {
        ledger.file_facts.push(record);
    }
}

fn observable_input_json(record: &ToolExecutionRecord) -> Option<Value> {
    let raw = record.observable_input.as_ref()?.value.as_str();
    serde_json::from_str(raw).ok()
}

fn observable_path(record: &ToolExecutionRecord) -> Option<String> {
    let json = observable_input_json(record)?;
    json.get("path")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| {
            json.get("file_path")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
}

fn observable_bash_command(record: &ToolExecutionRecord) -> Option<String> {
    let json = observable_input_json(record)?;
    json.get("command")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn observable_string_field(record: &ToolExecutionRecord, key: &str) -> Option<String> {
    let json = observable_input_json(record)?;
    json.get(key)
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn push_review_record(ledger: &mut StepFactLedgers, record: ReviewRecord) {
    let duplicate = ledger.review_refs.iter().any(|existing| {
        existing.verdict == record.verdict
            && existing.summary == record.summary
            && existing.correction == record.correction
            && existing.source == record.source
    });
    if !duplicate {
        ledger.review_refs.push(record);
    }
}

fn push_artifact_record(ledger: &mut StepFactLedgers, record: ArtifactRecord) {
    let duplicate = ledger.artifact_refs.iter().any(|existing| {
        existing.path == record.path
            && existing.kind == record.kind
            && existing.status == record.status
            && existing.source == record.source
            && existing.summary == record.summary
    });
    if !duplicate {
        ledger.artifact_refs.push(record);
    }
}

fn normalize_runtime_path(path: &str) -> String {
    normalize_candidate_path(path, std::env::current_dir().ok().as_deref())
        .unwrap_or_else(|| path.to_string())
}

fn tool_record_summary(record: &ToolExecutionRecord) -> String {
    trim_excerpt(
        record.detail.as_deref().unwrap_or(record.summary.as_str()),
        140,
    )
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

pub fn format_file_fact_line(item: &FileFactRecord) -> String {
    format!(
        "fact: file_facts ref={} path={} kind={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={}{} fact={}",
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
    )
}

pub fn format_change_fact_line(item: &ChangeRecord) -> String {
    format!(
        "fact: recent_changes_in_files ref={} path={} source={} source_event_id={} freshness={} confidence={} status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
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
    )
}

pub fn format_test_fact_line(item: &TestRecord) -> String {
    format!(
        "fact: test_failures ref={} name={} status={} source={} source_event_id={} freshness={} confidence={} lineage_status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
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
    )
}

pub fn format_artifact_fact_line(item: &ArtifactRecord) -> String {
    format!(
        "fact: artifact_status ref={} path={} kind={} status={} source={} source_event_id={} freshness={} confidence={} lineage_status={} invalidated_by={} supersedes={} conflicts_with={} summary={}",
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
    )
}

pub fn fact_lines_from_ledgers(ledgers: &StepFactLedgers) -> Vec<String> {
    let mut facts = Vec::new();
    for item in &ledgers.file_facts {
        facts.push(format_file_fact_line(item));
    }
    for item in &ledgers.change_refs {
        facts.push(format_change_fact_line(item));
    }
    for item in &ledgers.test_refs {
        facts.push(format_test_fact_line(item));
    }
    for item in &ledgers.artifact_refs {
        facts.push(format_artifact_fact_line(item));
    }
    facts
}

fn bash_artifact_status(record: &ToolExecutionRecord) -> &'static str {
    match record.kind {
        ToolExecutionOutcomeKind::Success => "observed",
        ToolExecutionOutcomeKind::Denied => "denied",
        ToolExecutionOutcomeKind::PendingApproval => "pending_approval",
        ToolExecutionOutcomeKind::Interrupted => "interrupted",
        ToolExecutionOutcomeKind::Progress => "in_progress",
        ToolExecutionOutcomeKind::ResultTooLarge => "result_too_large",
    }
}

fn bash_test_status(record: &ToolExecutionRecord) -> &'static str {
    let detail = record.detail.as_deref().unwrap_or_default();
    match record.kind {
        ToolExecutionOutcomeKind::Success if !detail.contains("exit_code:") => "passed",
        ToolExecutionOutcomeKind::Success if detail.contains("exit_code: 0") => "passed",
        ToolExecutionOutcomeKind::PendingApproval => "pending_approval",
        ToolExecutionOutcomeKind::Denied => "denied",
        ToolExecutionOutcomeKind::ResultTooLarge => "result_too_large",
        _ => "failed",
    }
}

pub fn append_runtime_tool_record(
    ledgers: &mut StepFactLedgers,
    record: &ToolExecutionRecord,
    ref_namespace: &str,
) {
    match record.tool_name.as_str() {
        "Read" => {
            if record.kind != ToolExecutionOutcomeKind::Success {
                return;
            }
            let Some(path) = observable_path(record).map(|path| normalize_runtime_path(&path))
            else {
                return;
            };
            push_file_fact(
                ledgers,
                FileFactRecord {
                    ref_id: format!("filefact:{ref_namespace}:read"),
                    path: path.clone(),
                    kind: "read_observation".into(),
                    fact: format!("runtime Read succeeded for {path}"),
                    symbol: extract_symbol_for_path(
                        &path,
                        &[record.detail.as_deref().unwrap_or_default()],
                    ),
                    source: "tool:Read".into(),
                    source_event_id: format!("tool-read:{ref_namespace}"),
                    freshness: "after-runtime-read".into(),
                    confidence_milli: 1000,
                    lineage: active_lineage(),
                },
            );
        }
        "Edit" | "Write" => {
            if record.kind != ToolExecutionOutcomeKind::Success {
                return;
            }
            let Some(path) = observable_path(record).map(|path| normalize_runtime_path(&path))
            else {
                return;
            };
            ledgers.change_refs.push(ChangeRecord {
                ref_id: format!("change:{ref_namespace}:edit"),
                path: path.clone(),
                summary: tool_record_summary(record),
                source: format!("tool:{}", record.tool_name),
                source_event_id: format!("tool-edit:{ref_namespace}"),
                freshness: "after-runtime-edit".into(),
                confidence_milli: 1000,
                lineage: active_lineage(),
            });
            push_file_fact(
                ledgers,
                FileFactRecord {
                    ref_id: format!("filefact:{ref_namespace}:edit"),
                    path,
                    kind: "edited_file".into(),
                    fact: format!("runtime {} succeeded for this file", record.tool_name),
                    symbol: None,
                    source: format!("tool:{}", record.tool_name),
                    source_event_id: format!("tool-edit:{ref_namespace}"),
                    freshness: "after-runtime-edit".into(),
                    confidence_milli: 1000,
                    lineage: active_lineage(),
                },
            );
        }
        "Bash" => {
            let Some(command) = observable_bash_command(record) else {
                return;
            };
            ledgers.artifact_refs.push(ArtifactRecord {
                ref_id: format!("artifact:{ref_namespace}:bash"),
                path: format!("command:{}", trim_excerpt(&command, 80)),
                kind: "command_output".into(),
                status: bash_artifact_status(record).into(),
                summary: tool_record_summary(record),
                source: "tool:Bash".into(),
                source_event_id: format!("tool-bash:{ref_namespace}"),
                freshness: "after-runtime-bash".into(),
                confidence_milli: 1000,
                lineage: active_lineage(),
            });
            if is_test_command(&command) {
                ledgers.test_refs.push(TestRecord {
                    ref_id: format!("test:{ref_namespace}:bash"),
                    name: trim_excerpt(&command, 60),
                    status: bash_test_status(record).into(),
                    summary: tool_record_summary(record),
                    source: "tool:Bash".into(),
                    source_event_id: format!("tool-bash:{ref_namespace}"),
                    freshness: "after-runtime-test".into(),
                    confidence_milli: 1000,
                    lineage: active_lineage(),
                });
            }
        }
        _ => {}
    }
}

fn is_test_command(command: &str) -> bool {
    let lowered = command.to_lowercase();
    lowered.contains("cargo test")
        || lowered.contains("pytest")
        || lowered.contains("pnpm test")
        || lowered.contains("npm test")
        || lowered.contains("yarn test")
        || lowered.contains("go test")
        || lowered.contains("jest")
        || lowered.contains("vitest")
        || lowered.contains("bun test")
        || lowered.contains("uv run pytest")
}

fn apply_runtime_tool_records(ledgers: &mut StepFactLedgers, step: &BossPlanStep) {
    for (idx, record) in step.tool_execution_records.iter().enumerate() {
        match record.tool_name.as_str() {
            "Read" | "Edit" | "Write" | "Bash" => {
                append_runtime_tool_record(ledgers, record, &format!("step{}:{idx}", step.id));
            }
            "BossReview" => {
                let verdict =
                    observable_string_field(record, "verdict").unwrap_or_else(|| "reviewed".into());
                let correction = observable_string_field(record, "correction");
                push_review_record(
                    ledgers,
                    ReviewRecord {
                        ref_id: format!("review:step{}:runtime:{idx}", step.id),
                        verdict,
                        summary: trim_excerpt(
                            record.detail.as_deref().unwrap_or(record.summary.as_str()),
                            180,
                        ),
                        correction,
                        source: "tool:BossReview".into(),
                        source_event_id: format!("tool-review:{}:{idx}", step.id),
                        freshness: "after-runtime-review".into(),
                        confidence_milli: 1000,
                        lineage: active_lineage(),
                    },
                );
            }
            "ArtifactVerify" => {
                let Some(path) = observable_path(record) else {
                    continue;
                };
                let status =
                    observable_string_field(record, "status").unwrap_or_else(|| "verified".into());
                let kind = observable_string_field(record, "kind").unwrap_or_else(|| "file".into());
                push_artifact_record(
                    ledgers,
                    ArtifactRecord {
                        ref_id: format!("artifact:step{}:runtime:{idx}", step.id),
                        path,
                        kind,
                        status,
                        summary: trim_excerpt(
                            record.detail.as_deref().unwrap_or(record.summary.as_str()),
                            180,
                        ),
                        source: "tool:ArtifactVerify".into(),
                        source_event_id: format!("tool-artifact:{}:{idx}", step.id),
                        freshness: "after-runtime-artifact-verify".into(),
                        confidence_milli: 1000,
                        lineage: active_lineage(),
                    },
                );
            }
            _ => {}
        }
    }
}

fn infer_test_status(text: &str) -> Option<&'static str> {
    let lowered = text.to_lowercase();
    if lowered.contains("test") || lowered.contains("测试") {
        if lowered.contains("fail")
            || lowered.contains("failed")
            || lowered.contains("failing")
            || lowered.contains("error")
            || lowered.contains("回归")
            || lowered.contains("失败")
        {
            return Some("failed");
        }
        if lowered.contains("pass")
            || lowered.contains("passed")
            || lowered.contains("green")
            || lowered.contains("通过")
        {
            return Some("passed");
        }
    }
    None
}

fn infer_review_verdict(step: &BossPlanStep, review: &str) -> &'static str {
    match step.status {
        BossPlanStepStatus::Completed => "accepted",
        BossPlanStepStatus::Rejected => "rejected",
        BossPlanStepStatus::ReplanRequired => "replan_required",
        BossPlanStepStatus::Failed => "failed",
        _ => {
            let lowered = review.to_lowercase();
            if step.last_correction.is_some()
                || lowered.contains("correction")
                || lowered.contains("fix ")
                || lowered.contains("not good enough")
                || lowered.contains("failed")
                || lowered.contains("artifact verification failed")
            {
                "rejected"
            } else if lowered.contains("replan") {
                "replan_required"
            } else if lowered.contains("accept")
                || lowered.contains("lgtm")
                || lowered.contains("looks good")
            {
                "accepted"
            } else {
                "reviewed"
            }
        }
    }
}

fn build_review_ledgers(ledgers: &mut StepFactLedgers, step: &BossPlanStep) {
    if ledgers.review_refs.is_empty() {
        if let Some(review) = step
            .last_review_summary
            .as_deref()
            .filter(|text| !text.trim().is_empty())
        {
            push_review_record(
                ledgers,
                ReviewRecord {
                    ref_id: format!("review:step{}:summary", step.id),
                    verdict: infer_review_verdict(step, review).into(),
                    summary: trim_excerpt(review, 180),
                    correction: step.last_correction.clone(),
                    source: "review_summary".into(),
                    source_event_id: format!("review-summary:{}", step.id),
                    freshness: "after-review".into(),
                    confidence_milli: 950,
                    lineage: active_lineage(),
                },
            );
        }
    }

    if ledgers.review_refs.is_empty() {
        if let Some(correction) = step
            .last_correction
            .as_deref()
            .filter(|text| !text.trim().is_empty())
        {
            push_review_record(
                ledgers,
                ReviewRecord {
                    ref_id: format!("review:step{}:correction", step.id),
                    verdict: "rejected".into(),
                    summary: step
                        .last_review_summary
                        .as_deref()
                        .map(|text| trim_excerpt(text, 180))
                        .unwrap_or_else(|| "review requested a correction".into()),
                    correction: Some(trim_excerpt(correction, 180)),
                    source: "review_correction".into(),
                    source_event_id: format!("review-correction:{}", step.id),
                    freshness: "after-review".into(),
                    confidence_milli: 1000,
                    lineage: active_lineage(),
                },
            );
        }
    }
}

fn build_artifact_ledgers(ledgers: &mut StepFactLedgers, step: &BossPlanStep) {
    if !ledgers.artifact_refs.is_empty() {
        return;
    }
    let verification_error = verify_artifact_expectations(step.objective()).err();
    for (idx, expectation) in extract_artifact_expectations(step.objective())
        .into_iter()
        .enumerate()
    {
        let path = expectation.path.to_string_lossy().to_string();
        let kind = match expectation.kind {
            BossArtifactKind::File => "file",
            BossArtifactKind::Directory => "directory",
        }
        .to_string();
        let touched_by_runtime = ledgers.change_refs.iter().any(|item| item.path == path)
            || ledgers.file_facts.iter().any(|item| item.path == path);
        let (status, summary, confidence_milli) = if let Some(reason) = verification_error.as_ref()
        {
            (
                "missing_or_invalid",
                format!("artifact verification failed for {path}: {reason}"),
                1000,
            )
        } else if step.completed {
            (
                "verified",
                format!("artifact expectation verified for {path}"),
                1000,
            )
        } else if touched_by_runtime {
            (
                "touched",
                format!("runtime activity touched artifact candidate {path}"),
                900,
            )
        } else {
            (
                "expected",
                format!("step objective requires artifact {path}"),
                950,
            )
        };
        push_artifact_record(
            ledgers,
            ArtifactRecord {
                ref_id: format!("artifact:step{}:{idx}", step.id),
                path,
                kind,
                status: status.into(),
                summary,
                source: "artifact_expectation".into(),
                source_event_id: format!("artifact-expectation:{}:{idx}", step.id),
                freshness: if step.completed {
                    "after-review".into()
                } else {
                    "current".into()
                },
                confidence_milli,
                lineage: active_lineage(),
            },
        );
    }
}

pub fn build_open_item_records(step: &BossPlanStep, open_items: &[String]) -> Vec<OpenItemRecord> {
    open_items
        .iter()
        .enumerate()
        .map(|(idx, item)| OpenItemRecord {
            ref_id: format!("openitem:step{}:{idx}", step.id),
            summary: item.clone(),
            source: format!("acceptance:{idx}"),
            source_event_id: format!("step-acceptance:{}:{idx}", step.id),
            freshness: "current".into(),
            confidence_milli: 1000,
            lineage: active_lineage(),
        })
        .collect()
}

pub fn build_blocker_records(
    step: Option<&BossPlanStep>,
    stage: BossStage,
    blocked_items: &[String],
) -> Vec<BlockerRecord> {
    let step_id = step.map(|item| item.id).unwrap_or_default();
    blocked_items
        .iter()
        .enumerate()
        .map(|(idx, item)| BlockerRecord {
            ref_id: format!("blocker:step{step_id}:{idx}"),
            summary: item.clone(),
            source: format!("stage:{}", format!("{stage:?}").to_lowercase()),
            source_event_id: format!("stage-blocker:{step_id}:{idx}"),
            freshness: "current".into(),
            confidence_milli: 1000,
            lineage: active_lineage(),
        })
        .collect()
}

pub fn build_rejected_approach_records(
    step: &BossPlanStep,
    review_refs: &[ReviewRecord],
) -> Vec<RejectedApproachRecord> {
    let Some(correction) = step
        .last_correction
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    else {
        return Vec::new();
    };
    let source_ref = review_refs
        .iter()
        .find(|item| item.verdict == "rejected" || item.verdict == "replan_required")
        .map(|item| item.ref_id.clone());
    vec![RejectedApproachRecord {
        ref_id: format!("rejected:step{}:0", step.id),
        summary: step
            .last_review_summary
            .as_deref()
            .unwrap_or("review requested a different approach")
            .to_string(),
        correction: Some(correction.to_string()),
        source: "review_correction".into(),
        source_ref: source_ref.clone(),
        source_event_id: format!("review-correction:{}", step.id),
        freshness: "after-review".into(),
        confidence_milli: 1000,
        lineage: LedgerLineage {
            status: "active".into(),
            invalidated_by: Vec::new(),
            supersedes: Vec::new(),
            conflicts_with: source_ref.into_iter().collect(),
        },
    }]
}

pub fn build_step_fact_ledgers(step: &BossPlanStep) -> StepFactLedgers {
    let mut ledgers = StepFactLedgers::default();
    apply_runtime_tool_records(&mut ledgers, step);

    let objective = step.objective();
    for (idx, (path, line)) in extract_path_candidates(objective).into_iter().enumerate() {
        let kind = classify_path_kind(&path, &line);
        let symbol = extract_symbol_for_path(
            &path,
            &[
                objective,
                step.result_diff.as_deref().unwrap_or_default(),
                step.last_review_summary.as_deref().unwrap_or_default(),
            ],
        );
        push_file_fact(
            &mut ledgers,
            FileFactRecord {
                ref_id: format!("filefact:step{}:{idx}", step.id),
                path: path.clone(),
                kind: kind.clone(),
                fact: if kind == "target_directory" {
                    format!("step objective names this directory as a concrete target: {path}")
                } else {
                    format!("step objective names this path as concrete context: {path}")
                },
                symbol: symbol.clone(),
                source: "step_objective".into(),
                source_event_id: format!("step-objective:{}", step.id),
                freshness: "current".into(),
                confidence_milli: 1000,
                lineage: active_lineage(),
            },
        );
        if kind != "target_directory" {
            let workspace_path = Path::new(&path);
            if workspace_path.exists() {
                push_file_fact(
                    &mut ledgers,
                    FileFactRecord {
                        ref_id: format!("filefact:step{}:snapshot:{idx}", step.id),
                        path: path.clone(),
                        kind: "workspace_snapshot".into(),
                        fact: compact_file_observation(&path)
                            .unwrap_or_else(|| "workspace snapshot confirms file exists".into()),
                        symbol,
                        source: "workspace_snapshot".into(),
                        source_event_id: format!("workspace-snapshot:{}:{idx}", step.id),
                        freshness: "current".into(),
                        confidence_milli: 950,
                        lineage: active_lineage(),
                    },
                );
            }
        }
    }

    if let Some(result_diff) = step
        .result_diff
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    {
        let read_like = result_diff.to_lowercase();
        if read_like.contains("read ")
            || read_like.contains("inspect")
            || read_like.contains("opened ")
            || read_like.contains("viewed ")
            || result_diff.contains("查看")
            || result_diff.contains("阅读")
            || result_diff.contains("读了")
        {
            for (idx, (path, _)) in extract_path_candidates_anywhere(result_diff)
                .into_iter()
                .enumerate()
            {
                push_file_fact(
                    &mut ledgers,
                    FileFactRecord {
                        ref_id: format!("filefact:step{}:read:{idx}", step.id),
                        path: path.clone(),
                        kind: "read_observation".into(),
                        fact: format!(
                            "worker output indicates this file was read or inspected: {path}"
                        ),
                        symbol: extract_symbol_for_path(&path, &[result_diff, objective]),
                        source: "worker_result".into(),
                        source_event_id: format!("worker-read:{}", step.id),
                        freshness: "after-worker-output".into(),
                        confidence_milli: 850,
                        lineage: active_lineage(),
                    },
                );
            }
        }
        let paths = extract_path_candidates_anywhere(result_diff);
        if paths.is_empty() {
            for (idx, file) in ledgers.file_facts.iter().enumerate() {
                ledgers.change_refs.push(ChangeRecord {
                    ref_id: format!("change:step{}:{idx}", step.id),
                    path: file.path.clone(),
                    summary: trim_excerpt(result_diff, 140),
                    source: "worker_result".into(),
                    source_event_id: format!("worker-result:{}", step.id),
                    freshness: "after-worker-output".into(),
                    confidence_milli: 800,
                    lineage: active_lineage(),
                });
            }
        } else {
            for (idx, (path, _)) in paths.into_iter().enumerate() {
                ledgers.change_refs.push(ChangeRecord {
                    ref_id: format!("change:step{}:{idx}", step.id),
                    path,
                    summary: trim_excerpt(result_diff, 140),
                    source: "worker_result".into(),
                    source_event_id: format!("worker-result:{}", step.id),
                    freshness: "after-worker-output".into(),
                    confidence_milli: 900,
                    lineage: active_lineage(),
                });
            }
        }
        if let Some(status) = infer_test_status(result_diff) {
            ledgers.test_refs.push(TestRecord {
                ref_id: format!("test:step{}:worker", step.id),
                name: "worker_reported_tests".into(),
                status: status.into(),
                summary: trim_excerpt(result_diff, 140),
                source: "worker_result".into(),
                source_event_id: format!("worker-result:{}", step.id),
                freshness: "after-worker-output".into(),
                confidence_milli: 850,
                lineage: active_lineage(),
            });
        }
    }

    if let Some(review) = step
        .last_review_summary
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    {
        let review_lowered = review.to_lowercase();
        if review_lowered.contains("read ")
            || review_lowered.contains("inspect")
            || review.contains("查看")
            || review.contains("阅读")
        {
            for (idx, (path, _)) in extract_path_candidates_anywhere(review)
                .into_iter()
                .enumerate()
            {
                push_file_fact(
                    &mut ledgers,
                    FileFactRecord {
                        ref_id: format!("filefact:step{}:review-read:{idx}", step.id),
                        path: path.clone(),
                        kind: "read_observation".into(),
                        fact: format!(
                            "review summary indicates this file was read or inspected: {path}"
                        ),
                        symbol: extract_symbol_for_path(&path, &[review, objective]),
                        source: "review_summary".into(),
                        source_event_id: format!("review-read:{}", step.id),
                        freshness: "after-review".into(),
                        confidence_milli: 800,
                        lineage: active_lineage(),
                    },
                );
            }
        }
        if let Some(status) = infer_test_status(review) {
            ledgers.test_refs.push(TestRecord {
                ref_id: format!("test:step{}:review", step.id),
                name: "review_reported_tests".into(),
                status: status.into(),
                summary: trim_excerpt(review, 140),
                source: "review_summary".into(),
                source_event_id: format!("review-summary:{}", step.id),
                freshness: "after-review".into(),
                confidence_milli: 900,
                lineage: active_lineage(),
            });
        }
    } else if step.completed
        && step
            .acceptance
            .iter()
            .any(|item| item.to_lowercase().contains("test"))
    {
        ledgers.test_refs.push(TestRecord {
            ref_id: format!("test:step{}:acceptance", step.id),
            name: "acceptance_tests".into(),
            status: "passed".into(),
            summary: trim_excerpt(&step.acceptance.join(" | "), 140),
            source: "acceptance".into(),
            source_event_id: format!("acceptance:{}", step.id),
            freshness: "after-accept".into(),
            confidence_milli: 700,
            lineage: active_lineage(),
        });
    }

    build_review_ledgers(&mut ledgers, step);
    build_artifact_ledgers(&mut ledgers, step);

    ledgers
}

#[cfg(test)]
mod tests {
    use super::{
        LedgerLineage, ReviewRecord, build_blocker_records, build_open_item_records,
        build_rejected_approach_records, build_step_fact_ledgers,
    };
    use crate::core::boss_state::{BossPlanStep, BossPlanStepStatus, BossStage};
    use crate::tool::definition::{ObservableInput, ObservableInputSource};
    use crate::tool::result::{
        ToolBatchContext, ToolExecutionOutcomeKind, ToolExecutionRecord, ToolReportModifier,
    };

    #[test]
    fn build_step_fact_ledgers_extracts_target_files_and_worker_changes() {
        let step = BossPlanStep {
            id: 7,
            description: "step".into(),
            objective: Some("任务目标：\n- 目标文件：src/core/boss.rs\n- 更新 worker 路径".into()),
            acceptance: vec!["tests pass".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: Some("updated src/core/boss.rs and tests failed in boss_flow".into()),
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: Some(
                "tests failed because prompt did not include open items".into(),
            ),
            last_correction: None,
            stage_continuation_context: None,
                        executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(
            ledgers
                .file_facts
                .iter()
                .any(|item| item.path == "src/core/boss.rs")
        );
        assert!(
            ledgers
                .change_refs
                .iter()
                .any(|item| item.path == "src/core/boss.rs")
        );
        assert!(ledgers.test_refs.iter().any(|item| item.status == "failed"));
        assert!(
            ledgers
                .file_facts
                .iter()
                .any(|item| item.kind == "workspace_snapshot")
        );
    }

    #[test]
    fn build_step_fact_ledgers_extracts_plain_sentence_objective_paths() {
        let step = BossPlanStep {
            id: 7,
            description: "fix worker ledger".into(),
            objective: Some(
                "修复 src/core/state_frame_projection.rs 中的 worker ledger 映射，并让 boss_flow 测试通过"
                    .into(),
            ),
            acceptance: vec!["tests pass".into()],
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
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(ledgers.file_facts.iter().any(|item| {
            item.path == "RustAgent/Agent/src/core/state_frame_projection.rs"
                || item.path == "src/core/state_frame_projection.rs"
        }));
    }

    #[test]
    fn build_step_fact_ledgers_emits_read_observation_when_worker_reports_file_read() {
        let step = BossPlanStep {
            id: 8,
            description: "read step".into(),
            objective: Some("investigate worker issue".into()),
            acceptance: vec![],
            requires_approval: false,
            status: BossPlanStepStatus::Running,
            completed: false,
            result_diff: Some(
                "read src/core/state_fact_ledger.rs and inspected FileFactRecord".into(),
            ),
            worker_task_id: None,
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_continuation_context: None,
                        executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(ledgers.file_facts.iter().any(|item| {
            item.kind == "read_observation"
                && item.path.ends_with("src/core/state_fact_ledger.rs")
                && item.symbol.as_deref() == Some("FileFactRecord")
        }));
    }

    #[test]
    fn build_step_fact_ledgers_prefers_runtime_tool_records_over_text_inference() {
        let step = BossPlanStep {
            id: 9,
            description: "runtime records".into(),
            objective: Some("update src/core/state_frame_projection.rs".into()),
            acceptance: vec!["tests pass".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Reviewing,
            completed: false,
            result_diff: None,
            worker_task_id: Some("task-runtime-1".into()),
            attempt_count: 0,
            retry_budget: 3,
            last_review_summary: None,
            last_correction: None,
            stage_continuation_context: None,
                        executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "Read".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Read succeeded".into(),
                    detail: Some("pub struct FileFactRecord".into()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"path":"src/core/state_fact_ledger.rs"}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Edit".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Edit succeeded".into(),
                    detail: Some("updated ledger projection".into()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"path":"src/core/state_frame_projection.rs"}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "Bash".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Bash succeeded".into(),
                    detail: Some("command: cargo test -p rust_agent boss_flow\nexit_code: 101\nstderr:\nassert failed".into()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"command":"cargo test -p rust_agent boss_flow"}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
            ],
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(ledgers.file_facts.iter().any(|item| {
            item.source == "tool:Read" && item.path.ends_with("src/core/state_fact_ledger.rs")
        }));
        assert!(ledgers.change_refs.iter().any(|item| {
            item.source == "tool:Edit" && item.path.ends_with("src/core/state_frame_projection.rs")
        }));
        assert!(
            ledgers
                .test_refs
                .iter()
                .any(|item| { item.source == "tool:Bash" && item.status == "failed" })
        );
    }

    #[test]
    fn build_step_fact_ledgers_emits_review_and_artifact_ledgers() {
        let artifact_path = std::env::temp_dir().join(format!(
            "ledger-artifact-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        ));
        std::fs::write(&artifact_path, "artifact").expect("artifact should be written");
        let step = BossPlanStep {
            id: 10,
            description: "artifact review".into(),
            objective: Some(format!(
                "任务目标：\n- 目标文件：{}",
                artifact_path.display()
            )),
            acceptance: vec!["artifact file exists and is non-empty".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Completed,
            completed: true,
            result_diff: Some(format!("wrote {}", artifact_path.display())),
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("ACCEPT: artifact verified".into()),
            last_correction: None,
            stage_continuation_context: None,
                        executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(
            ledgers
                .review_refs
                .iter()
                .any(|item| item.verdict == "accepted")
        );
        assert!(ledgers.artifact_refs.iter().any(|item| {
            item.path == artifact_path.to_string_lossy() && item.status == "verified"
        }));

        let _ = std::fs::remove_file(artifact_path);
    }

    #[test]
    fn build_step_fact_ledgers_prefers_runtime_review_and_artifact_records() {
        let step = BossPlanStep {
            id: 11,
            description: "runtime review artifact".into(),
            objective: Some("任务目标：\n- 目标文件：/tmp/runtime-artifact.txt".into()),
            acceptance: vec!["artifact file exists and is non-empty".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Completed,
            completed: true,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("fallback review summary".into()),
            last_correction: None,
            stage_continuation_context: None,
                        executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: vec![
                ToolExecutionRecord {
                    tool_name: "BossReview".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "Boss review verdict: accepted".into(),
                    detail: Some("LGTM from runtime review".into()),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"step_id":11,"verdict":"accepted","correction":null}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
                ToolExecutionRecord {
                    tool_name: "ArtifactVerify".into(),
                    outcome: "Text".into(),
                    kind: ToolExecutionOutcomeKind::Success,
                    summary: "artifact verification passed: /tmp/runtime-artifact.txt".into(),
                    detail: Some(
                        "artifact verification status=verified path=/tmp/runtime-artifact.txt"
                            .into(),
                    ),
                    pending_approval: None,
                    report_modifier: ToolReportModifier::None,
                    observable_input: Some(ObservableInput {
                        value: r#"{"step_id":11,"path":"/tmp/runtime-artifact.txt","kind":"file","status":"verified"}"#.into(),
                        source: ObservableInputSource::Raw,
                    }),
                    batch_context: ToolBatchContext {
                        batch_index: 0,
                        batch_size: 1,
                        executed_in_batch: false,
                    },
                },
            ],
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(ledgers.review_refs.iter().any(|item| {
            item.source == "tool:BossReview"
                && item.verdict == "accepted"
                && item.summary.contains("runtime review")
        }));
        assert!(ledgers.artifact_refs.iter().any(|item| {
            item.source == "tool:ArtifactVerify"
                && item.path == "/tmp/runtime-artifact.txt"
                && item.status == "verified"
        }));
        assert!(
            ledgers
                .review_refs
                .iter()
                .all(|item| item.source != "review_summary"),
            "runtime review records should suppress fallback inferred review entries"
        );
    }

    #[test]
    fn helper_ledgers_build_open_blocker_and_rejected_records_with_lineage() {
        let step = BossPlanStep {
            id: 12,
            description: "retry".into(),
            objective: Some("fix auth".into()),
            acceptance: vec!["tests pass".into()],
            requires_approval: false,
            status: BossPlanStepStatus::Rejected,
            completed: false,
            result_diff: None,
            worker_task_id: None,
            attempt_count: 1,
            retry_budget: 3,
            last_review_summary: Some("previous patch ignored edge cases".into()),
            last_correction: Some("preserve the auth guard branch".into()),
            stage_continuation_context: None,
                        executor_b_stage_memory: None,
            review_task_id: None,
            tool_execution_records: Vec::new(),
        };
        let open = build_open_item_records(&step, &["tests pass".into()]);
        let blocked = build_blocker_records(
            Some(&step),
            BossStage::WaitingForApproval,
            &["waiting for user approval".into()],
        );
        let rejected = build_rejected_approach_records(
            &step,
            &[ReviewRecord {
                ref_id: "review:step12:summary".into(),
                verdict: "rejected".into(),
                summary: "previous patch ignored edge cases".into(),
                correction: Some("preserve the auth guard branch".into()),
                source: "review_summary".into(),
                source_event_id: "review-summary:12".into(),
                freshness: "after-review".into(),
                confidence_milli: 950,
                lineage: LedgerLineage {
                    status: "active".into(),
                    invalidated_by: Vec::new(),
                    supersedes: Vec::new(),
                    conflicts_with: Vec::new(),
                },
            }],
        );
        assert_eq!(open[0].lineage.status, "active");
        assert_eq!(blocked[0].lineage.status, "active");
        assert_eq!(rejected[0].lineage.status, "active");
        assert_eq!(
            rejected[0].lineage.conflicts_with,
            vec!["review:step12:summary".to_string()]
        );
    }
}

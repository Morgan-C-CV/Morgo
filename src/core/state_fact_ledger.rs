use crate::core::boss_state::BossPlanStep;
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepFactLedgers {
    pub file_facts: Vec<FileFactRecord>,
    pub change_refs: Vec<ChangeRecord>,
    pub test_refs: Vec<TestRecord>,
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
    extract_path_candidates_with_mode(text, true)
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

pub fn build_step_fact_ledgers(step: &BossPlanStep) -> StepFactLedgers {
    let mut ledgers = StepFactLedgers::default();

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
        });
    }

    ledgers
}

#[cfg(test)]
mod tests {
    use super::build_step_fact_ledgers;
    use crate::core::boss_state::{BossPlanStep, BossPlanStepStatus};

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
            review_task_id: None,
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
            review_task_id: None,
        };

        let ledgers = build_step_fact_ledgers(&step);
        assert!(ledgers.file_facts.iter().any(|item| {
            item.kind == "read_observation"
                && item.path.ends_with("src/core/state_fact_ledger.rs")
                && item.symbol.as_deref() == Some("FileFactRecord")
        }));
    }
}

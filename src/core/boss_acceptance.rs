use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BossArtifactKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BossArtifactExpectation {
    pub path: PathBuf,
    pub kind: BossArtifactKind,
}

fn target_file_marker(line: &str) -> Option<usize> {
    let lowered = line.to_lowercase();
    [
        "目标文件",
        "target file",
        "output file",
        "生成 markdown 报告",
    ]
    .iter()
    .filter_map(|marker| lowered.find(marker).map(|idx| idx + marker.len()))
    .min()
}

fn target_dir_marker(line: &str) -> Option<usize> {
    let lowered = line.to_lowercase();
    ["目标目录", "target directory", "output directory"]
        .iter()
        .filter_map(|marker| lowered.find(marker).map(|idx| idx + marker.len()))
        .min()
}

fn clean_path_token(token: &str) -> String {
    token
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .trim_end_matches(['，', ',', '。', '.', ';', '；', ')', '）', ']'])
        .to_string()
}

fn first_absolute_path_after(line: &str, offset: usize) -> Option<PathBuf> {
    let start = line.get(offset..)?.find('/').map(|idx| idx + offset)?;
    let token = line[start..]
        .split_whitespace()
        .next()
        .map(clean_path_token)?;
    (!token.is_empty()).then(|| PathBuf::from(token))
}

fn absolute_artifact_tokens(line: &str) -> Vec<PathBuf> {
    let mut artifacts = Vec::new();
    for raw in line.split_whitespace() {
        let token = clean_path_token(raw);
        if !token.starts_with('/') {
            continue;
        }
        let path = PathBuf::from(&token);
        if !artifacts.iter().any(|item| item == &path) {
            artifacts.push(path);
        }
    }
    artifacts
}

fn is_artifact_scope_boundary(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("参考材料")
        || trimmed.starts_with("关键材料")
        || trimmed.starts_with("参考背景")
        || trimmed.starts_with("参考样本")
        || trimmed.starts_with("建议核验路径")
        || trimmed.starts_with("实现摘录")
}

fn line_requires_artifact_output(line: &str) -> bool {
    let lowered = line.to_lowercase();
    [
        "输出",
        "生成",
        "创建",
        "产出",
        "写入",
        "包含",
        "必须有",
        "必须包含",
        "write",
        "create",
        "generate",
        "produce",
        "output",
        "include",
        "contains",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

fn relative_artifact_tokens(line: &str) -> Vec<PathBuf> {
    let mut artifacts = Vec::new();
    for raw in line.split(|ch: char| {
        ch.is_whitespace()
            || matches!(
                ch,
                '，' | ','
                    | '。'
                    | ';'
                    | '；'
                    | '、'
                    | ':'
                    | '：'
                    | '('
                    | ')'
                    | '（'
                    | '）'
                    | '['
                    | ']'
            )
    }) {
        let token = clean_path_token(raw);
        if token.is_empty() || token.starts_with('/') {
            continue;
        }
        let lowered = token.to_lowercase();
        let is_named_artifact = lowered == "readme"
            || lowered == "readme.md"
            || [
                ".md", ".html", ".css", ".js", ".mjs", ".ts", ".tsx", ".jsx", ".py", ".rs",
                ".json", ".toml", ".yaml", ".yml", ".txt", ".sh",
            ]
            .iter()
            .any(|suffix| lowered.ends_with(suffix));
        if !is_named_artifact {
            continue;
        }
        let path = if lowered == "readme" {
            PathBuf::from("README.md")
        } else {
            PathBuf::from(token)
        };
        if !artifacts.iter().any(|item| item == &path) {
            artifacts.push(path);
        }
    }
    artifacts
}

pub fn extract_artifact_expectations(text: &str) -> Vec<BossArtifactExpectation> {
    let mut expectations = Vec::new();
    let mut relative_file_expectations = Vec::new();
    for line in text.lines() {
        if is_artifact_scope_boundary(line) {
            break;
        }
        let target_file_offset = target_file_marker(line);
        let target_dir_offset = target_dir_marker(line);
        if target_file_offset.is_none()
            && target_dir_offset.is_none()
            && line_requires_artifact_output(line)
        {
            for artifact in absolute_artifact_tokens(line) {
                if !expectations
                    .iter()
                    .any(|item: &BossArtifactExpectation| item.path == artifact)
                {
                    expectations.push(BossArtifactExpectation {
                        path: artifact,
                        kind: BossArtifactKind::File,
                    });
                }
            }
            for artifact in relative_artifact_tokens(line) {
                if !relative_file_expectations
                    .iter()
                    .any(|item| item == &artifact)
                {
                    relative_file_expectations.push(artifact);
                }
            }
        }
        let (kind, path_offset) = if let Some(offset) = target_file_offset {
            (BossArtifactKind::File, offset)
        } else if let Some(offset) = target_dir_offset {
            (BossArtifactKind::Directory, offset)
        } else {
            continue;
        };
        let Some(path) = first_absolute_path_after(line, path_offset) else {
            continue;
        };
        if !expectations
            .iter()
            .any(|item: &BossArtifactExpectation| item.path == path && item.kind == kind)
        {
            expectations.push(BossArtifactExpectation { path, kind });
        }
    }
    if !relative_file_expectations.is_empty() {
        let target_dirs = expectations
            .iter()
            .filter(|item| item.kind == BossArtifactKind::Directory)
            .map(|item| item.path.clone())
            .collect::<Vec<_>>();
        for dir in target_dirs {
            for relative_path in &relative_file_expectations {
                let path = dir.join(relative_path);
                if !expectations
                    .iter()
                    .any(|item| item.path == path && item.kind == BossArtifactKind::File)
                {
                    expectations.push(BossArtifactExpectation {
                        path,
                        kind: BossArtifactKind::File,
                    });
                }
            }
        }
    }
    expectations
}

fn verify_file(path: &Path) -> Result<(), String> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("target file {} is not available: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("target file {} is not a file", path.display()));
    }
    if metadata.len() == 0 {
        return Err(format!("target file {} is empty", path.display()));
    }
    Ok(())
}

fn verify_directory(path: &Path) -> Result<(), String> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        format!(
            "target directory {} is not available: {error}",
            path.display()
        )
    })?;
    if !metadata.is_dir() {
        return Err(format!(
            "target directory {} is not a directory",
            path.display()
        ));
    }
    let mut entries = std::fs::read_dir(path).map_err(|error| {
        format!(
            "target directory {} is not readable: {error}",
            path.display()
        )
    })?;
    if entries.next().is_none() {
        return Err(format!("target directory {} is empty", path.display()));
    }
    Ok(())
}

pub fn verify_artifact_expectations(text: &str) -> Result<(), String> {
    let expectations = extract_artifact_expectations(text);
    for expectation in expectations {
        match expectation.kind {
            BossArtifactKind::File => verify_file(&expectation.path)?,
            BossArtifactKind::Directory => verify_directory(&expectation.path)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_target_file_and_directory_expectations() {
        let text = "\
- 目标目录：/tmp/example-agent-site
- 目标文件：/tmp/example-report.md
- 参考路径：/tmp/not-a-target.md";

        let expectations = extract_artifact_expectations(text);
        assert_eq!(expectations.len(), 2);
        assert!(expectations.iter().any(|item| {
            item.kind == BossArtifactKind::Directory
                && item.path == PathBuf::from("/tmp/example-agent-site")
        }));
        assert!(expectations.iter().any(|item| {
            item.kind == BossArtifactKind::File
                && item.path == PathBuf::from("/tmp/example-report.md")
        }));
    }

    #[test]
    fn ignores_slash_tokens_before_target_marker() {
        let text = "\
- u9 ON/OFF 也已完成，目标目录 `/tmp/lism-jsonl-analyzer` 下生成 `analyze.py` 与 `report.md`
- `/LisM off`：关闭当前 session。";

        let expectations = extract_artifact_expectations(text);
        assert_eq!(expectations.len(), 1);
        assert_eq!(expectations[0].kind, BossArtifactKind::Directory);
        assert_eq!(
            expectations[0].path,
            PathBuf::from("/tmp/lism-jsonl-analyzer")
        );
    }

    #[test]
    fn ignores_artifacts_in_reference_material_sections() {
        let text = "\
任务目标：
- 目标目录：/tmp/agent-site

参考材料摘录：
- u9 ON/OFF 也已完成，目标目录 `/tmp/lism-jsonl-analyzer` 下生成 `analyze.py` 与 `report.md`";

        let expectations = extract_artifact_expectations(text);
        assert_eq!(expectations.len(), 1);
        assert_eq!(expectations[0].kind, BossArtifactKind::Directory);
        assert_eq!(expectations[0].path, PathBuf::from("/tmp/agent-site"));
    }

    #[test]
    fn derives_readme_file_expectation_for_target_directory_tasks() {
        let text = "\
任务目标：
- 目标目录：/tmp/agent-site
- 生成 index.html、styles.css
- 输出一个简短 README，说明如何打开与查看。

参考材料摘录：
- README in reference material should not create another target.";

        let expectations = extract_artifact_expectations(text);
        assert!(expectations.iter().any(|item| {
            item.kind == BossArtifactKind::Directory
                && item.path == PathBuf::from("/tmp/agent-site")
        }));
        assert!(expectations.iter().any(|item| {
            item.kind == BossArtifactKind::File
                && item.path == PathBuf::from("/tmp/agent-site/README.md")
        }));
        assert!(expectations.iter().any(|item| {
            item.kind == BossArtifactKind::File
                && item.path == PathBuf::from("/tmp/agent-site/index.html")
        }));
        assert!(expectations.iter().any(|item| {
            item.kind == BossArtifactKind::File
                && item.path == PathBuf::from("/tmp/agent-site/styles.css")
        }));
        assert!(!expectations.iter().any(|item| {
            item.kind == BossArtifactKind::File
                && item.path == PathBuf::from("/tmp/agent-site/material.md")
        }));
    }

    #[test]
    fn derives_relative_file_expectations_only_from_output_scope() {
        let text = "\
任务目标：
- 目标目录：/tmp/demo
- 必须包含 demo.py README.md config.json

参考材料摘录：
- 旧产物包含 stale.py";

        let expectations = extract_artifact_expectations(text);
        for expected in ["demo.py", "README.md", "config.json"] {
            assert!(expectations.iter().any(|item| {
                item.kind == BossArtifactKind::File
                    && item.path == PathBuf::from("/tmp/demo").join(expected)
            }));
        }
        assert!(!expectations.iter().any(|item| {
            item.kind == BossArtifactKind::File && item.path == PathBuf::from("/tmp/demo/stale.py")
        }));
    }

    #[test]
    fn missing_target_file_fails_verification() {
        let path = std::env::temp_dir().join(format!(
            "missing-boss-artifact-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        ));
        let text = format!("- 目标文件：{}", path.display());
        let err = verify_artifact_expectations(&text).expect_err("missing target should fail");
        assert!(err.contains("target file"));
    }
}

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

fn line_declares_target_file(line: &str) -> bool {
    let lowered = line.to_lowercase();
    lowered.contains("目标文件")
        || lowered.contains("target file")
        || lowered.contains("output file")
        || lowered.contains("生成 markdown 报告")
}

fn line_declares_target_dir(line: &str) -> bool {
    let lowered = line.to_lowercase();
    lowered.contains("目标目录")
        || lowered.contains("target directory")
        || lowered.contains("output directory")
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

fn first_absolute_path(line: &str) -> Option<PathBuf> {
    let start = line.find('/')?;
    let token = line[start..]
        .split_whitespace()
        .next()
        .map(clean_path_token)?;
    (!token.is_empty()).then(|| PathBuf::from(token))
}

pub fn extract_artifact_expectations(text: &str) -> Vec<BossArtifactExpectation> {
    let mut expectations = Vec::new();
    for line in text.lines() {
        let Some(path) = first_absolute_path(line) else {
            continue;
        };
        let kind = if line_declares_target_file(line) {
            Some(BossArtifactKind::File)
        } else if line_declares_target_dir(line) {
            Some(BossArtifactKind::Directory)
        } else {
            None
        };
        if let Some(kind) = kind {
            if !expectations
                .iter()
                .any(|item: &BossArtifactExpectation| item.path == path && item.kind == kind)
            {
                expectations.push(BossArtifactExpectation { path, kind });
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

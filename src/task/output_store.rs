use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::task::types::TaskOutputSlice;

#[derive(Debug, Clone)]
pub struct TaskOutputStore {
    root: PathBuf,
}

impl TaskOutputStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Default for TaskOutputStore {
    fn default() -> Self {
        #[cfg(test)]
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            Self {
                root: std::env::temp_dir().join(format!("rust-agent-test-{now}")),
            }
        }
        #[cfg(not(test))]
        {
            Self {
                root: std::env::current_dir()
                    .unwrap_or_else(|_| std::path::PathBuf::from("."))
                    .join(".rust-agent")
                    .join("task-outputs"),
            }
        }
    }
}

impl TaskOutputStore {
    pub fn init(&self, task_id: &str) -> anyhow::Result<String> {
        fs::create_dir_all(&self.root)?;
        let path = self.root.join(format!("{task_id}.log"));
        fs::write(&path, "")?;
        Ok(path.to_string_lossy().into_owned())
    }

    pub fn append(&self, output_file: &str, chunk: &str) -> anyhow::Result<usize> {
        let mut file = OpenOptions::new().append(true).open(output_file)?;
        file.write_all(chunk.as_bytes())?;
        Ok(chunk.len())
    }

    pub fn read_slice(&self, output_file: &str, offset: usize) -> anyhow::Result<TaskOutputSlice> {
        let content = fs::read_to_string(output_file)?;
        let safe_offset = clamp_to_char_boundary(&content, offset);
        Ok(TaskOutputSlice {
            content: content[safe_offset..].to_string(),
            next_offset: content.len(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn clamp_to_char_boundary(content: &str, offset: usize) -> usize {
    if offset >= content.len() {
        return content.len();
    }
    if content.is_char_boundary(offset) {
        return offset;
    }

    let mut candidate = offset;
    while candidate > 0 && !content.is_char_boundary(candidate) {
        candidate -= 1;
    }
    candidate
}

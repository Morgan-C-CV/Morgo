use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::task::types::TaskOutputSlice;

const MAX_TASK_OUTPUT_READ_BYTES: usize = 256 * 1024;

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
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            // Include a per-call nonce so spawned child processes (which reset the
            // counter to 0) cannot collide with the parent's store directories.
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos();
            Self {
                root: std::env::temp_dir().join(format!("rust-agent-test-{pid}-{id}-{nonce}")),
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
        let mut file = match OpenOptions::new().read(true).open(output_file) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(TaskOutputSlice {
                    content: String::new(),
                    next_offset: 0,
                });
            }
            Err(e) => return Err(e.into()),
        };
        let file_len = file.seek(SeekFrom::End(0))? as usize;

        // If offset is at or past end, nothing to return.
        if offset >= file_len {
            return Ok(TaskOutputSlice {
                content: String::new(),
                next_offset: file_len,
            });
        }

        // Tail-read cap: only read the last MAX_TASK_OUTPUT_READ_BYTES of the file.
        let cap_start = file_len.saturating_sub(MAX_TASK_OUTPUT_READ_BYTES);

        // If the requested offset falls before the cap window, the caller is asking
        // for data we no longer serve — return empty with a truncated marker so the
        // caller knows to advance its offset.
        if offset < cap_start {
            let omitted = cap_start;
            file.seek(SeekFrom::Start(cap_start as u64))?;
            let mut buf = Vec::with_capacity(file_len - cap_start);
            file.read_to_end(&mut buf)?;
            let raw = String::from_utf8_lossy(&buf).into_owned();
            let content = format!("[truncated: {omitted} bytes omitted]\n{raw}");
            return Ok(TaskOutputSlice {
                content,
                next_offset: file_len,
            });
        }

        // Normal path: offset is within the readable window.
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = Vec::with_capacity(file_len - offset);
        file.read_to_end(&mut buf)?;
        let raw = String::from_utf8_lossy(&buf).into_owned();
        Ok(TaskOutputSlice {
            content: raw,
            next_offset: file_len,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

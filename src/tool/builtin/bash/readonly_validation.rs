#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOnlyLevel {
    None,
    ReadOnly,
}

pub fn classify_read_only_level(command: &str) -> ReadOnlyLevel {
    let trimmed = command.trim();
    let read_only = [
        "ls",
        "pwd",
        "git status",
        "git diff",
        "cat",
        "head",
        "tail",
        "which",
        "grep",
        "find",
    ]
    .iter()
    .any(|prefix| trimmed == *prefix || trimmed.starts_with(&format!("{prefix} ")));

    if read_only {
        ReadOnlyLevel::ReadOnly
    } else {
        ReadOnlyLevel::None
    }
}

pub fn is_read_only_command(command: &str) -> bool {
    matches!(classify_read_only_level(command), ReadOnlyLevel::ReadOnly)
}

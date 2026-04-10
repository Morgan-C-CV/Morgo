pub fn is_read_only_command(command: &str) -> bool {
    let trimmed = command.trim();
    [
        "ls",
        "pwd",
        "git status",
        "git diff",
        "cat",
        "head",
        "tail",
        "which",
    ]
    .iter()
    .any(|prefix| trimmed == *prefix || trimmed.starts_with(&format!("{prefix} ")))
}

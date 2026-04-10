pub fn is_plan_mode_safe(command: &str) -> bool {
    let trimmed = command.trim();
    ["ls", "pwd", "git status", "git diff"]
        .iter()
        .any(|prefix| trimmed == *prefix || trimmed.starts_with(&format!("{prefix} ")))
}

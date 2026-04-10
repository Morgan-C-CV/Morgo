pub fn is_plan_mode_safe(command: &str) -> bool {
    let trimmed = command.trim();
    trimmed.starts_with("ls") || trimmed.starts_with("pwd") || trimmed.starts_with("git status")
}

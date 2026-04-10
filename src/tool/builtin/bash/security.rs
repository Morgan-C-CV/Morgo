pub fn contains_destructive_pattern(command: &str) -> bool {
    ["rm -rf", "git reset --hard", "dd if=", "mkfs"]
        .iter()
        .any(|pattern| command.contains(pattern))
}

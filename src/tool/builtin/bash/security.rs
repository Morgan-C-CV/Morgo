pub fn contains_destructive_pattern(command: &str) -> bool {
    ["rm -rf", "git reset --hard", "dd if="]
        .iter()
        .any(|pattern| command.contains(pattern))
}

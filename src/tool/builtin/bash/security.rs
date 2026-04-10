pub fn contains_destructive_pattern(command: &str) -> bool {
    [
        "rm -rf",
        "git reset --hard",
        "dd if=",
        "mkfs",
        "chmod -R 777",
        "> /",
    ]
    .iter()
    .any(|pattern| command.contains(pattern))
}

pub fn contains_shell_operator(command: &str) -> bool {
    ["|", ">", ">>", "&&", ";", "$(", "`"]
        .iter()
        .any(|pattern| command.contains(pattern))
}

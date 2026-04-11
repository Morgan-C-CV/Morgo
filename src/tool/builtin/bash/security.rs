pub fn contains_destructive_pattern(command: &str) -> bool {
    [
        "rm -rf",
        "git reset --hard",
        "dd if=",
        "mkfs",
        "chmod -R 777",
        "> /",
        "mv ",
        "cp -r ",
    ]
    .iter()
    .any(|pattern| command.contains(pattern))
}

pub fn contains_shell_operator(command: &str) -> bool {
    !extract_shell_operators(command).is_empty()
}

pub fn extract_shell_operators(command: &str) -> Vec<String> {
    ["|", ">>", ">", "&&", ";", "$(", "`"]
        .iter()
        .filter(|pattern| command.contains(**pattern))
        .map(|pattern| (*pattern).to_string())
        .collect()
}

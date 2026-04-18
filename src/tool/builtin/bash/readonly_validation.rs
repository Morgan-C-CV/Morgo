use crate::tool::builtin::bash::command_helpers::normalized_command_variants;
use crate::tool::builtin::bash::scanner::scan_bash_command;
use crate::tool::builtin::bash::security::contains_shell_operator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOnlyLevel {
    None,
    ReadOnly,
}

fn is_read_only_variant(command: &str) -> bool {
    let scan = scan_bash_command(command);
    let trimmed = scan.words.join(" ");
    [
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
    .any(|prefix| trimmed == *prefix || trimmed.starts_with(&format!("{prefix} ")))
}

pub fn classify_read_only_level(command: &str) -> ReadOnlyLevel {
    if contains_shell_operator(command) {
        return ReadOnlyLevel::None;
    }

    if normalized_command_variants(command)
        .iter()
        .any(|variant| is_read_only_variant(variant))
    {
        ReadOnlyLevel::ReadOnly
    } else {
        ReadOnlyLevel::None
    }
}

pub fn is_read_only_command(command: &str) -> bool {
    matches!(classify_read_only_level(command), ReadOnlyLevel::ReadOnly)
}

use crate::tool::builtin::bash::scanner::{scan_bash_command, ShellOperator};

pub fn contains_destructive_pattern(command: &str) -> bool {
    let words = scan_bash_command(command).words;
    for window in words.windows(2) {
        if matches!(window, [left, right] if left == "rm" && right == "-rf")
            || matches!(window, [left, right] if left == "mkfs" && right.starts_with('/'))
        {
            return true;
        }
    }
    for window in words.windows(3) {
        if matches!(window, [left, middle, right] if left == "git" && middle == "reset" && right == "--hard")
            || matches!(window, [left, middle, right] if left == "chmod" && middle == "-R" && right == "777")
        {
            return true;
        }
    }
    words.iter().any(|word| word.starts_with("dd") && word.contains("if="))
        || words.first().is_some_and(|word| word == "mv" || word == "cp")
}

pub fn contains_shell_operator(command: &str) -> bool {
    !extract_shell_operators(command).is_empty()
}

pub fn extract_shell_operators(command: &str) -> Vec<String> {
    scan_bash_command(command)
        .operators
        .iter()
        .map(|operator| operator.display().to_string())
        .collect()
}

pub fn shell_operator_reason_codes(command: &str) -> Vec<String> {
    scan_bash_command(command)
        .operators
        .iter()
        .map(|operator| operator.reason_code().to_string())
        .collect()
}

pub fn contains_write_redirection(command: &str) -> bool {
    scan_bash_command(command)
        .operators
        .iter()
        .any(|operator| matches!(operator, ShellOperator::RedirectWrite | ShellOperator::RedirectAppend))
}

use std::path::{Component, Path};

pub fn is_safe_path(path: &str) -> bool {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return true;
    }

    !candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
}

pub fn command_uses_only_safe_paths(command: &str) -> bool {
    command_path_assessment(command)
        .iter()
        .all(|finding| !finding.starts_with("unsafe:"))
}

pub fn command_path_assessment(command: &str) -> Vec<String> {
    command
        .split_whitespace()
        .filter(|token| token.contains('/') || token.starts_with('.'))
        .map(|token| {
            if token.starts_with('/') {
                format!("safe:{token}")
            } else if is_safe_path(token) {
                format!("safe:{token}")
            } else {
                format!("unsafe:{token}")
            }
        })
        .collect()
}

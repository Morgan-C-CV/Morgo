use std::path::{Component, Path};

pub fn is_safe_path(path: &str) -> bool {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return false;
    }

    !candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
}

pub fn command_uses_only_safe_paths(command: &str) -> bool {
    command
        .split_whitespace()
        .filter(|token| token.contains('/') || token.starts_with('.'))
        .all(is_safe_path)
}

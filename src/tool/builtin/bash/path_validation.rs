use std::path::Path;

pub fn is_safe_path(path: &str) -> bool {
    let candidate = Path::new(path);
    !path.contains("..") && !candidate.is_absolute()
}

pub fn is_safe_path(path: &str) -> bool {
    !path.contains("..")
}

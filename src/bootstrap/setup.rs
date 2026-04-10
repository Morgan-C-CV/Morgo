use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SetupContext {
    pub working_directory: PathBuf,
    pub worktree_enabled: bool,
}

impl SetupContext {
    pub fn detect() -> Self {
        let working_directory = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            working_directory,
            worktree_enabled: std::env::var("RUST_AGENT_WORKTREE_ENABLED")
                .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        }
    }
}

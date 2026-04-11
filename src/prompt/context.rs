use crate::context::{git, memory, user_context};
use crate::state::app_state::AppState;

pub fn build_context_prompt(app_state: &AppState) -> String {
    [
        git::describe_git_context(app_state),
        memory::describe_memory_context(app_state),
        user_context::describe_user_context(app_state),
    ]
    .join("\n")
}

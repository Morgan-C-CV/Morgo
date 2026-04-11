use crate::state::app_state::AppState;

pub fn describe_git_context(app_state: &AppState) -> String {
    format!("git_session={}", app_state.active_session_id)
}

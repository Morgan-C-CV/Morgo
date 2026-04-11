use crate::state::app_state::AppState;

pub fn describe_memory_context(app_state: &AppState) -> String {
    format!("history_available={}", app_state.history.is_some())
}

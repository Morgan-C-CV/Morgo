use crate::state::app_state::AppState;

pub fn build_system_prompt(app_state: &AppState) -> String {
    format!(
        "surface={:?}\nsession_mode={:?}\nruntime_role={:?}",
        app_state.surface, app_state.session_mode, app_state.runtime_role
    )
}

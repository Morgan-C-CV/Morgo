use crate::coordinator::mode::is_coordinator_mode;
use crate::coordinator::prompt::build_coordinator_system_prompt;
use crate::state::app_state::AppState;

pub fn build_system_prompt(app_state: &AppState) -> String {
    if is_coordinator_mode()
        || matches!(
            app_state.runtime_role,
            crate::state::app_state::RuntimeRole::Coordinator
        )
    {
        return build_coordinator_system_prompt(app_state);
    }

    format!(
        "surface={:?}\nsession_mode={:?}\nruntime_role={:?}",
        app_state.surface, app_state.session_mode, app_state.runtime_role
    )
}

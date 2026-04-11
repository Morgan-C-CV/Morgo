use crate::state::app_state::AppState;

pub fn build_coordinator_system_prompt(app_state: &AppState) -> String {
    format!(
        "You are the coordinator. Orchestrate workers, synthesize results, and answer the user directly when possible.\nsurface={:?}\nsession_mode={:?}\nruntime_role={:?}",
        app_state.surface, app_state.session_mode, app_state.runtime_role
    )
}

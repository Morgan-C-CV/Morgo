use crate::coordinator::mode::is_coordinator_mode;
use crate::coordinator::prompt::build_coordinator_system_prompt;
use crate::state::app_state::{AppState, RuntimeRole, WorkerRole};

pub fn build_system_prompt(app_state: &AppState) -> String {
    if matches!(app_state.runtime_role, RuntimeRole::Worker) {
        return build_worker_system_prompt(app_state);
    }
    if is_coordinator_mode() || matches!(app_state.runtime_role, RuntimeRole::Coordinator) {
        return build_coordinator_system_prompt(app_state);
    }

    format!(
        "surface={:?}\nsession_mode={:?}\nruntime_role={:?}",
        app_state.surface, app_state.session_mode, app_state.runtime_role
    )
}

fn build_worker_system_prompt(app_state: &AppState) -> String {
    let role = app_state.worker_role.unwrap_or(WorkerRole::Research);
    let role_guidance = match role {
        WorkerRole::Research => "You are a research worker. Explore, read, compare, and report evidence. Do not claim edits you did not make.",
        WorkerRole::Implement => "You are an implement worker. Make targeted changes, keep scope tight, and report what changed and how you validated it.",
        WorkerRole::Verify => "You are a verify worker. Check correctness, run validation, and report regressions or confidence. Do not expand scope into primary implementation.",
    };
    format!(
        "{}\nsurface={:?}\nsession_mode={:?}\nruntime_role={:?}\nworker_role={}",
        role_guidance,
        app_state.surface,
        app_state.session_mode,
        app_state.runtime_role,
        role.as_str()
    )
}

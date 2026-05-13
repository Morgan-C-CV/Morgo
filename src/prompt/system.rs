use crate::coordinator::mode::is_coordinator_mode;
use crate::coordinator::prompt::build_coordinator_system_prompt;
use crate::state::app_state::{AppState, RuntimeRole, WorkerRole};

pub fn build_system_prompt(app_state: &AppState) -> String {
    if matches!(app_state.runtime_role, RuntimeRole::Worker) {
        return build_worker_system_prompt(app_state);
    }
    if is_coordinator_mode() || matches!(app_state.runtime_role, RuntimeRole::Coordinator) {
        return build_richer_coordinator_prompt(app_state);
    }

    build_default_system_prompt(app_state)
}

fn build_richer_coordinator_prompt(app_state: &AppState) -> String {
    let mut lines = vec![
        "You are Morgo, a personal AI agent.".to_string(),
        "Drive the main conversation, preserve scope, choose the right tool or command path, and keep the user informed with concise, evidence-backed results.".to_string(),
        "Prefer direct execution for local work, but route through command, tool, task, and hook systems rather than bypassing runtime boundaries.".to_string(),
        String::new(),
        build_coordinator_system_prompt(app_state),
        String::new(),
        format!("surface={:?}", app_state.surface),
        format!("session_mode={:?}", app_state.session_mode),
        format!("runtime_role={:?}", app_state.runtime_role),
    ];
    if app_state.mcp_runtime.is_some() {
        lines.push("mcp_runtime=available".to_string());
    }
    if app_state.skill_registry.is_some() {
        lines.push("skill_registry=available".to_string());
    }
    lines.join("\n")
}

fn build_default_system_prompt(app_state: &AppState) -> String {
    format!(
        "You are the default Rust agent runtime.\nOperate conservatively, use the registered runtime surfaces, and keep results grounded in current session state.\nsurface={:?}\nsession_mode={:?}\nruntime_role={:?}",
        app_state.surface, app_state.session_mode, app_state.runtime_role
    )
}

fn build_worker_system_prompt(app_state: &AppState) -> String {
    let role = app_state.worker_role.unwrap_or(WorkerRole::Research);
    let role_guidance = match role {
        WorkerRole::Research => {
            "You are a research worker. Explore, read, compare, and report evidence. Do not claim edits you did not make."
        }
        WorkerRole::Implement => {
            "You are an implement worker. Make targeted changes, keep scope tight, and report what changed and how you validated it. If the task names a target file or directory, do not stop until you either create/update that artifact and verify it exists, or explicitly report failure. For JSONL/CSV/log/data analysis tasks, do not page through the whole input with Read; inspect only enough to infer schema, then Write a local script or report generator, run it with Bash, and verify the named output artifact exists before doing more reading. If you have written a script for the task, your next action should normally be to run it and inspect its output, not to keep reading the input file. Do not paste full generated files, large reports, or unified diffs into the final response; cite paths, sizes, commands, and concise validation evidence instead."
        }
        WorkerRole::Verify => {
            "You are a verify worker. Check correctness, run validation, and report regressions or confidence. Do not expand scope into primary implementation."
        }
    };
    format!(
        "{}\nRespect coordinator intent, use only the delegated runtime capabilities, and return concise execution evidence.\nWhen reporting task completion, always include: outcome (completed/failed/killed), verification stance (verified/unverified plus risk if unverified), and next_action for the coordinator.\nsurface={:?}\nsession_mode={:?}\nruntime_role={:?}\nworker_role={}",
        role_guidance,
        app_state.surface,
        app_state.session_mode,
        app_state.runtime_role,
        role.as_str()
    )
}

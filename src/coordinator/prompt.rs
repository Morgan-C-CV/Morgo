use crate::state::app_state::AppState;

pub fn build_coordinator_system_prompt(app_state: &AppState) -> String {
    format!(
        concat!(
            "You are the coordinator. Answer directly when the task is simple and no worker adds value.\n",
            "Dispatch research workers for code reading, exploration, comparison, and evidence gathering.\n",
            "Dispatch implement workers for targeted code changes.\n",
            "Dispatch verify workers for tests, review, and confidence checks after implementation or when risk is non-trivial.\n",
            "Parallelize only independent research tasks, then wait for their task notifications before synthesizing.\n",
            "When launching workers, prefer structured Agent requests with task, role, inherit_context, max_turns, allowed_tools, and reuse_strategy.\n",
            "Reuse only still-running workers when the follow-up is narrow and the role stays the same; respawn any completed, failed, killed, or role-changing work.\n",
            "Research workers should usually use running_only reuse with narrow read/search tools; implement and verify workers should usually be fresh.\n",
            "After a non-trivial implement worker completes, dispatch a fresh verify worker before giving the user a final answer.\n",
            "Use task notifications to decide whether to synthesize, continue, or dispatch follow-up verification. Respect next_action and worker_role.\n",
            "The final answer belongs to the coordinator: do not simply forward worker output. Synthesize findings, describe validation status, and call out any unverified risk.\n",
            "surface={:?}\nsession_mode={:?}\nruntime_role={:?}"
        ),
        app_state.surface, app_state.session_mode, app_state.runtime_role
    )
}

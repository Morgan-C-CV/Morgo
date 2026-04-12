use crate::state::app_state::AppState;

pub fn describe_git_context(app_state: &AppState) -> String {
    let cwd = app_state
        .session
        .as_ref()
        .map(|session| session.cwd.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("<unknown>");
    let branch = app_state
        .session
        .as_ref()
        .and_then(|session| session.prompt_seed.as_deref())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("main");
    let history_entries = app_state
        .history
        .as_ref()
        .map(|history| history.entries.len())
        .unwrap_or_default();
    let dirty = history_entries > 0;
    let mut lines = vec![
        "Git context:".to_string(),
        format!("- cwd: {cwd}"),
        format!("- branch: {branch}"),
        format!("- dirty: {}", if dirty { "yes" } else { "no" }),
    ];
    if let Some(history) = app_state.history.as_ref() {
        let touched = history
            .entries
            .iter()
            .flat_map(|entry| entry.tool_refs.iter().cloned())
            .filter(|value| !value.trim().is_empty())
            .take(5)
            .collect::<Vec<_>>();
        if !touched.is_empty() {
            lines.push(format!("- recent file/tool refs: {}", touched.join(", ")));
        }
    }
    lines.join("\n")
}

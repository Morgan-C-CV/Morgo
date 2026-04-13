use crate::state::app_state::AppState;

pub fn describe_memory_context(app_state: &AppState) -> String {
    let session_id = app_state.active_session_id.as_str();
    let Some(history) = app_state.history.as_ref() else {
        return format!(
            "Session memory:\n- session_id: {session_id}\n- history: unavailable\n- context_files: none"
        );
    };

    let entry_count = history.entries.len();
    let recent_messages = history
        .entries
        .iter()
        .rev()
        .take(3)
        .map(|entry| {
            format!(
                "{}: {}",
                format_role(&entry.message.role),
                truncate(&entry.message.content, 60)
            )
        })
        .collect::<Vec<_>>();
    let tool_refs = history
        .entries
        .iter()
        .flat_map(|entry| entry.tool_refs.iter().cloned())
        .filter(|value| !value.trim().is_empty())
        .take(5)
        .collect::<Vec<_>>();

    let mut lines = vec![
        "Session memory:".to_string(),
        format!("- session_id: {session_id}"),
        format!("- history_entries: {entry_count}"),
    ];
    if !recent_messages.is_empty() {
        lines.push(format!(
            "- recent_messages: {}",
            recent_messages.join(" | ")
        ));
    }
    if !tool_refs.is_empty() {
        lines.push(format!("- context_files: {}", tool_refs.join(", ")));
    } else {
        lines.push("- context_files: none".to_string());
    }
    lines.join("\n")
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn format_role(role: &crate::core::message::Role) -> &'static str {
    match role {
        crate::core::message::Role::System => "system",
        crate::core::message::Role::User => "user",
        crate::core::message::Role::Assistant => "assistant",
        crate::core::message::Role::Tool => "tool",
    }
}

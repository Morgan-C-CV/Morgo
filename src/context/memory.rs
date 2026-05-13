use crate::state::app_state::AppState;
use crate::state::permission_context::{
    sanitize_external_memory_entries, sanitize_nested_memory_lineage,
};
use std::borrow::Cow;

pub fn describe_memory_context(app_state: &AppState) -> String {
    let session_id = app_state.active_session_id.as_str();
    let mut lines = vec![
        "Session memory:".to_string(),
        format!("- session_id: {session_id}"),
    ];

    if let Some(history) = effective_history(app_state) {
        let entry_count = history.entries.len();
        lines.push(format!("- history_entries: {entry_count}"));

        let recent_messages = history
            .entries
            .iter()
            .rev()
            .take(3)
            .map(|entry| {
                format!(
                    "{}: {}",
                    format_role(&entry.message.role),
                    truncate(&entry.message.text(), 60)
                )
            })
            .collect::<Vec<_>>();
        if !recent_messages.is_empty() {
            lines.push(format!(
                "- recent_messages: {}",
                recent_messages.join(" | ")
            ));
        }

        let tool_refs = history
            .entries
            .iter()
            .flat_map(|entry| entry.tool_refs.iter().cloned())
            .filter(|value| !value.trim().is_empty())
            .take(5)
            .collect::<Vec<_>>();
        if !tool_refs.is_empty() {
            lines.push(format!("- context_files: {}", tool_refs.join(", ")));
        } else {
            lines.push("- context_files: none".to_string());
        }
    } else {
        lines.push("- history: unavailable".to_string());
        lines.push("- context_files: none".to_string());
    }

    let external_memory =
        sanitize_external_memory_entries(app_state.permission_context.external_memory_entries());
    lines.push("External memory:".to_string());
    if external_memory.is_empty() {
        lines.push("- entries: none".to_string());
    } else {
        lines.push(format!("- entries: {}", external_memory.len()));
        for (index, entry) in external_memory.iter().enumerate().take(5) {
            lines.push(format!("- [{}] {}", index + 1, truncate(entry, 120)));
        }
    }

    let nested_lineage =
        sanitize_nested_memory_lineage(app_state.permission_context.nested_memory_lineage());
    lines.push("Nested memory lineage:".to_string());
    if nested_lineage.is_empty() {
        lines.push("- lineage: root".to_string());
    } else {
        lines.push(format!("- depth: {}", nested_lineage.len()));
        lines.push(format!("- path: {}", nested_lineage.join(" -> ")));
    }

    lines.join("\n")
}

fn effective_history(
    app_state: &AppState,
) -> Option<Cow<'_, crate::history::session::SessionHistory>> {
    let session_id = app_state.current_session_id();
    if let Some(session_store) = app_state.session_store.as_ref() {
        let request = crate::history::session::SessionRestoreRequest {
            resume: Some(session_id.0.clone()),
            continue_session: false,
        };
        if let Some((snapshot, history)) = session_store.load(&request) {
            if snapshot.session_id == session_id {
                return Some(Cow::Owned(history));
            }
        }
    }

    app_state.history.as_ref().map(Cow::Borrowed)
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

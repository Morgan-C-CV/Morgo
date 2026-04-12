use crate::state::app_state::AppState;

pub fn describe_user_context(app_state: &AppState) -> String {
    let mut lines = vec![
        "Runtime user context:".to_string(),
        format!("- surface: {:?}", app_state.surface),
        format!("- client_type: {:?}", app_state.client_type),
        format!("- session_mode: {:?}", app_state.session_mode),
        format!("- runtime_role: {:?}", app_state.runtime_role),
    ];
    if let Some(worker_role) = app_state.worker_role {
        lines.push(format!("- worker_role: {}", worker_role.as_str()));
    }
    lines.join("\n")
}

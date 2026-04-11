use crate::state::app_state::AppState;

pub fn describe_user_context(app_state: &AppState) -> String {
    format!("client_type={:?}", app_state.client_type)
}

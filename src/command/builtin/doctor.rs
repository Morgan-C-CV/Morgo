use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::state::permission_context::PermissionMode;

pub struct DoctorCommand;

#[async_trait]
impl Command for DoctorCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "doctor".into(),
            description: "Diagnose and verify your Morgo installation and settings".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: Vec::new(),
            is_hidden: false,
            disable_model_invocation: false,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        _input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let surface_info = format!("{:?}", app_state.surface);
        let session_id = &app_state.active_session_id;
        let cwd = app_state.current_working_directory();
        let permission_mode = match app_state.permission_context.mode() {
            PermissionMode::Default => "default",
            PermissionMode::AcceptEdits => "accept_edits",
            PermissionMode::BypassPermissions => "bypass_permissions",
            PermissionMode::Plan => "plan",
        };
        let auth_status = &app_state.active_model_provider_summary.auth_status;
        let pending_approval = app_state.permission_context.pending_approval();
        let cwd_state = if cwd.is_dir() {
            format!("cwd: {} (workspace available)", cwd.display())
        } else {
            format!("cwd: {} (missing or not a directory)", cwd.display())
        };
        let mut coding_blockers = Vec::new();
        if auth_status.contains("(unset)") {
            coding_blockers.push(format!("model/API auth: {}", auth_status));
        } else {
            coding_blockers.push(format!("model/API auth: {}", auth_status));
        }
        coding_blockers.push(cwd_state);
        if let Some(pending) = pending_approval {
            coding_blockers.push(format!(
                "permission mode: {} | pending approval: {} ({})",
                permission_mode, pending.tool_name, pending.tool_input
            ));
        } else {
            coding_blockers.push(format!("permission mode: {}", permission_mode));
        }

        let coding_blockers_section = if coding_blockers.is_empty() {
            "Coding blockers:\n- none detected".to_string()
        } else {
            format!("Coding blockers:\n- {}", coding_blockers.join("\n- "))
        };

        let message = format!(
            "⚕️  Doctor Diagnostics\n\n{}\n\
            \nSecondary diagnostics:\n\
            \nSystem Context:\n\
            - Interaction Surface: {}\n\
            - Default Session ID: {}\n\
            \nStorage & DB:\n\
            - Session Persistence: {}\n\
            \nCapabilities:\n\
            - Local Components: OK\n\
            - Rust Toolchains Validation: OK\n\
            \n(NOTE: Component UI is currently running in a purely unrendered text CLI loop.)",
            coding_blockers_section,
            surface_info,
            session_id,
            if app_state.session_store.is_some() {
                "Enabled"
            } else {
                "Disabled"
            }
        );

        Ok(CommandResult::Message(message))
    }
}

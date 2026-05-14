use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
    SystemTrapAction,
};
use crate::core::message::Role;
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::state::permission_context::PermissionMode;
use crate::task::types::TaskStatus;

pub struct ResumeCommand;

fn interrupted_continuation_target(app_state: &AppState) -> Option<String> {
    let session_id = app_state.current_session_id().0;
    app_state
        .permission_context
        .task_manager
        .as_ref()?
        .list()
        .into_iter()
        .rev()
        .find(|task| {
            task.owner.session_id == session_id
                && matches!(
                    task.status,
                    TaskStatus::Pending | TaskStatus::Running | TaskStatus::Killed
                )
                && !task.description.trim().is_empty()
        })
        .map(|task| task.description)
}

#[async_trait]
impl Command for ResumeCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "resume".into(),
            description: "Resume a previous conversation".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["continue".into()],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.trim();
        if !args.is_empty() {
            return Ok(CommandResult::SystemTrap(SystemTrapAction::ResumeSession(
                args.to_string(),
            )));
        }
        let current_id = &app_state.active_session_id;
        let cwd = app_state.current_working_directory();
        let permission_mode = match app_state.permission_context.mode() {
            PermissionMode::Default => "default",
            PermissionMode::AcceptEdits => "accept_edits",
            PermissionMode::BypassPermissions => "bypass_permissions",
            PermissionMode::Plan => "plan",
        };
        let pending_approval = app_state.permission_context.pending_approval();
        let last_task = interrupted_continuation_target(app_state)
            .or_else(|| {
                app_state
                    .canonical_session_history_entries()
                    .into_iter()
                    .rev()
                    .find(|entry| {
                        entry.message.role == Role::User && entry.message.has_visible_text()
                    })
                    .map(|entry| entry.message.content.trim().to_string())
                    .filter(|text| !text.is_empty())
            })
            .unwrap_or_else(|| "none recorded".into());
        let mode_line = if let Some(pending) = pending_approval {
            format!(
                "mode: {} | pending approval: {}",
                permission_mode, pending.tool_name
            )
        } else {
            format!("mode: {permission_mode}")
        };
        Ok(CommandResult::Message(format!(
            "Resume summary:\n- cwd: {}\n- {}\n- last task: {}\n\nCurrent Session ID: {}\n\nSession auto-saves locally (SQLite/JSON persistence). To restore later:\n  rust-agent --resume <SESSION_ID>\nOptionally use --continue-session to auto-resume the latest one.",
            cwd.display(),
            mode_line,
            last_task,
            current_id
        )))
    }
}

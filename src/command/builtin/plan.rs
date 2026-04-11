use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use crate::tool::definition::ToolResult;

pub struct PlanCommand;

#[async_trait]
impl Command for PlanCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "plan",
            description: "Enable plan mode or view the current session plan",
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: &[],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: false,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.trim();
        if args.is_empty() || args == "status" {
            return Ok(CommandResult::Message(
                crate::state::plan_mode::render_plan_status(&app_state.permission_context),
            ));
        }
        if args == "show" {
            return Ok(CommandResult::Message(
                crate::state::plan_mode::render_plan_show(&app_state.permission_context),
            ));
        }

        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or_default();
        let remainder = parts.collect::<Vec<_>>().join(" ");
        let result = match action {
            "enter" => crate::state::plan_mode::request_enter_plan_mode(
                &app_state.permission_context,
                &remainder,
            ),
            "exit" => crate::state::plan_mode::request_exit_plan_mode(
                &app_state.permission_context,
                &remainder,
            ),
            _ => {
                return Ok(CommandResult::Message(
                    "Usage: /plan [status|show|enter [reason]|exit [summary]]".into(),
                ))
            }
        };

        Ok(match result {
            ToolResult::Text(text) => CommandResult::Message(text),
            ToolResult::Denied(reason) => CommandResult::Denied(reason),
            ToolResult::PendingApproval { tool_name, message } => {
                CommandResult::Message(format!("approval required for {tool_name}: {message}"))
            }
            ToolResult::Interrupted(reason) => CommandResult::Message(format!("Interrupted: {reason}")),
            ToolResult::Progress(progress) => CommandResult::Message(progress),
            ToolResult::ResultTooLarge(reason) => {
                CommandResult::Message(format!("Result too large: {reason}"))
            }
        })
    }
}

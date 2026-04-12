use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::plan::types::PlanStepStatus;
use crate::state::app_state::AppState;
use crate::tool::definition::ToolResult;

pub struct PlanCommand;

#[async_trait]
impl Command for PlanCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "plan".into(),
            description: "Enable plan mode or view the current session plan".into(),
            source: CommandSource::Builtin,
            category: "orchestration".into(),
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
        if args == "history" {
            return Ok(CommandResult::Message(
                crate::state::plan_mode::render_plan_history(&app_state.permission_context),
            ));
        }

        let mut parts = args.split_whitespace();
        let action = parts.next().unwrap_or_default();
        let remainder = parts.collect::<Vec<_>>().join(" ");
        match action {
            "add" => {
                let mut segments = remainder.splitn(2, '|');
                let title = segments.next().unwrap_or_default().trim();
                let details = segments.next().map(str::trim);
                return Ok(CommandResult::Message(crate::state::plan_mode::add_plan_step(
                    &app_state.permission_context,
                    title,
                    details,
                )?));
            }
            "update" => {
                let mut segments = remainder.splitn(4, '|');
                let step_id = segments.next().unwrap_or_default().trim();
                let title = segments.next().map(str::trim).filter(|value| !value.is_empty());
                let details_segment = segments.next().map(str::trim);
                let status = segments
                    .next()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .and_then(PlanStepStatus::from_str);
                let details = details_segment.map(|value| {
                    if value == "-" {
                        None
                    } else {
                        Some(value)
                    }
                });
                return Ok(CommandResult::Message(crate::state::plan_mode::update_plan_step(
                    &app_state.permission_context,
                    step_id,
                    title,
                    details,
                    status,
                )?));
            }
            "done" => {
                let step_id = remainder.trim();
                return Ok(CommandResult::Message(crate::state::plan_mode::complete_plan_step(
                    &app_state.permission_context,
                    step_id,
                )?));
            }
            "enter" => {}
            "exit" => {}
            _ => {
                return Ok(CommandResult::Message(
                    "Usage: /plan [status|show|history|add <title> [| details]|update <step-id>|<title>|<details or ->|<status>|done <step-id>|enter [reason]|exit [summary]]".into(),
                ))
            }
        }

        let result = match action {
            "enter" => crate::state::plan_mode::request_enter_plan_mode(
                &app_state.permission_context,
                &remainder,
            ),
            "exit" => crate::state::plan_mode::request_exit_plan_mode(
                &app_state.permission_context,
                &remainder,
            ),
            _ => unreachable!("validated above"),
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

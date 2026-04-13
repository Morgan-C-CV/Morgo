use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;
use std::process::Command as ProcessCommand;

pub struct DiffCommand;

#[async_trait]
impl Command for DiffCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "diff".into(),
            description: "View uncommitted changes and per-turn diffs".into(),
            source: CommandSource::Coding,
            category: "git".into(),
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
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let output = ProcessCommand::new("git").args(["diff", "HEAD"]).output();

        match output {
            Ok(output) if output.status.success() => {
                let diff_text = String::from_utf8_lossy(&output.stdout);
                if diff_text.trim().is_empty() {
                    Ok(CommandResult::Message(
                        "No uncommitted changes found.".into(),
                    ))
                } else {
                    Ok(CommandResult::Message(format!(
                        "Git Diff (HEAD):\n```diff\n{}\n```",
                        diff_text
                    )))
                }
            }
            Ok(output) => {
                let err_text = String::from_utf8_lossy(&output.stderr);
                Ok(CommandResult::Message(format!(
                    "Failed to get git diff. Are you in a git repository?\n{}",
                    err_text
                )))
            }
            Err(e) => Ok(CommandResult::Message(format!(
                "Failed to execute git command: {}",
                e
            ))),
        }
    }
}

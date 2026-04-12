use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ReviewCommand;

#[async_trait]
impl Command for ReviewCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "review".into(),
            description: "Review a pull request".into(),
            source: CommandSource::Coding,
            category: "git".into(),
            command_type: CommandType::Prompt,
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
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let args = input.command_args.clone();
        
        let prompt = format!(
            r#"You are an expert code reviewer. Follow these steps:

1. If no PR number is provided in the args, run `gh pr list` using your shell tool to show open PRs.
2. If a PR number is provided, run `gh pr view <number>` to get PR details.
3. Run `gh pr diff <number>` to get the diff.
4. Analyze the changes and provide a thorough code review that includes:
   - Overview of what the PR does
   - Analysis of code quality and style
   - Specific suggestions for improvements
   - Any potential issues or risks

Keep your review concise but thorough. Focus on:
- Code correctness
- Following project conventions
- Performance implications
- Test coverage
- Security considerations

Format your review with clear sections and bullet points.

PR number arguments provided by user: {}"#,
            if args.is_empty() { "(none)" } else { &args }
        );

        Ok(CommandResult::Prompt(prompt))
    }
}

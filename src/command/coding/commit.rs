use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct CommitCommand;

#[async_trait]
impl Command for CommitCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "commit",
            description: "Constructs and executes a git commit automatically",
            command_type: CommandType::Prompt,
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
        _input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        let prompt = r#"## Context & Mission

You have been invoked to create a git commit automatically. Please explicitly call your Bash tool to gather context before crafting the commit:
1. Run `git status` to see what is staged/unstaged.
2. Run `git diff HEAD` to analyze the actual code changes.
3. Run `git log --oneline -10` to observe the repository's native commit message style.

## Git Safety Protocol
- NEVER update the git config.
- NEVER skip hooks (--no-verify, --no-gpg-sign, etc) unless explicitly requested.
- CRITICAL: ALWAYS create NEW commits. NEVER use git commit --amend.
- Do not commit files containing secrets (.env, credentials) without explicit warning.
- Do not create empty commits.

## Your Task
1. Analyze your `git diff` outputs.
2. Draft a concise commit message focusing on "why" (e.g. "feat: add user authentication", "fix: resolve memory leak in router").
3. Use the bash tool to formally stage and commit the changes utilizing the HEREDOC syntax precisely:
```bash
git commit -m "$(cat <<'EOF'
<Your Commit Message Here>
EOF
)"
```
Execute the assessment and submit the commit now."#;

        Ok(CommandResult::Prompt(prompt.into()))
    }
}

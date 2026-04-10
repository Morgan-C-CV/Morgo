use async_trait::async_trait;

use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    Prompt,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandAvailability {
    Everywhere,
    CliOnly,
    RemoteSafe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    pub name: &'static str,
    pub description: &'static str,
    pub command_type: CommandType,
    pub availability: CommandAvailability,
    pub aliases: &'static [&'static str],
    pub is_hidden: bool,
    pub disable_model_invocation: bool,
    pub immediate: bool,
    pub is_sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    Message(String),
    ContinueToQuery,
    Prompt(String),
    Denied(String),
}

#[async_trait]
pub trait Command: Send + Sync {
    fn metadata(&self) -> CommandMetadata;
    fn is_enabled(&self) -> bool {
        true
    }
    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult>;
}

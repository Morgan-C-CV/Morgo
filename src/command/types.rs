use async_trait::async_trait;

use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    Message(String),
    ContinueToQuery,
}

#[async_trait]
pub trait Command: Send + Sync {
    fn name(&self) -> &'static str;
    async fn execute(
        &self,
        input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult>;
}

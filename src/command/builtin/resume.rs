use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ResumeCommand;

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
        _input: &NormalizedInput,
        app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        // TODO: 状态留存与外部自动重载机制
        // 当 TUI 日志选择器开发完毕且选中某个 session_id 时，需要能够要求用户输入 /confirm，
        // 将外围循环跳出、应用新装载的 AppState，重入大循环（即实现热重载会话恢复）。
        let current_id = &app_state.active_session_id;
        Ok(CommandResult::Message(format!(
            "Current Session ID: {}\n\nSession auto-saves locally (SQLite/JSON persistence). To restore later:\n  rust-agent --resume <SESSION_ID>\nOptionally use --continue-session to auto-resume the latest one.",
            current_id
        )))
    }
}

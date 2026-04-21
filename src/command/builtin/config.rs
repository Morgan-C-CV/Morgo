use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct ConfigCommand;

#[async_trait]
impl Command for ConfigCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "config".into(),
            description: "Open config panel to change models and settings".into(),
            source: CommandSource::Builtin,
            category: "core".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["settings".into()],
            is_hidden: false,
            disable_model_invocation: false,
            immediate: true,
            is_sensitive: false,
        }
    }

    async fn execute(
        &self,
        input: &NormalizedInput,
        _app_state: &AppState,
    ) -> anyhow::Result<CommandResult> {
        // TODO: 状态留存与外部自动重载机制
        // 目前受到 `app_state` 不可变借用的限制，无法在此热变更查询引擎或模型，
        // 需拉起手动 /confirm，执行状态快照落盘并重启外围大循环以应用改动。
        if input.command_args.is_empty() {
            Ok(CommandResult::Message(
                "Config & Model switching:\nCurrently running in standard mode. \nTo change models (e.g. from Claude-3.5-Sonnet to Haiku for fast tasks), restart the agent with appropriate environment variables.\n(Dynamic TUI model switching requires 'LocalTui' component rendering interface)".into(),
            ))
        } else {
            Ok(CommandResult::Message(format!(
                "Cannot dynamically set model/config to '{}'. TUI component bridge not yet implemented.",
                input.command_args
            )))
        }
    }
}

use async_trait::async_trait;

use crate::command::types::{
    Command, CommandAvailability, CommandMetadata, CommandResult, CommandSource, CommandType,
};
use crate::interaction::envelope::NormalizedInput;
use crate::state::app_state::AppState;

pub struct LisMCommand;

#[async_trait]
impl Command for LisMCommand {
    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            name: "LisM".into(),
            description: "Toggle session-level Less-is-More StateFrame mode".into(),
            source: CommandSource::Builtin,
            category: "system".into(),
            command_type: CommandType::Local,
            availability: CommandAvailability::Everywhere,
            aliases: vec!["lism".into()],
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
        let subcommand = input.command_args.split_whitespace().next().unwrap_or("");
        let permissions = &app_state.permission_context;

        let message = match subcommand {
            "on" => {
                permissions.set_lism_enabled(true);
                "LisM enabled for this session. The /boss production path now switches to the StateFrame execution seam when LisM is on.".to_string()
            }
            "off" => {
                permissions.set_lism_enabled(false);
                "LisM disabled for this session.".to_string()
            }
            "status" => format!(
                "LisM status: {}",
                if permissions.lism_enabled() { "enabled" } else { "disabled" }
            ),
            "explain" => explain(permissions.lism_enabled()),
            _ => usage(),
        };

        Ok(CommandResult::Message(message))
    }
}

fn usage() -> String {
    "usage: /LisM <subcommand>\n  on       enable session-level Less-is-More mode\n  off      disable session-level Less-is-More mode\n  status   show current LisM mode\n  explain  show available building blocks and deferred items".into()
}

fn explain(enabled: bool) -> String {
    let mode = if enabled { "enabled" } else { "disabled" };
    format!(
        "LisM is currently {mode}.\n\nAvailable building blocks:\n- StateFrame schema and StateDecision validation\n- BossPlan -> StateFrame projection\n- Stateless JSON decision loop\n- StateFrame orchestrator seam\n- Toolset / skillset router is attached to the live LisM -> /boss production path\n- Model-tier router metadata is attached to the same seam (metadata-only; no provider/profile switching yet)\n- Archive / retention for accepted and open items\n- Production-path tests for the current StateFrame orchestration pipeline\n\nDeferred items:\n- real provider/profile switching from model-tier routing is not yet connected\n- /LisM persistence is not yet connected\n- fallback ladder expansion is still deferred"
    )
}

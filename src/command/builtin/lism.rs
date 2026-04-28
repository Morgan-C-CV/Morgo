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
            "status" => {
                let enabled = permissions.lism_enabled();
                let base = format!("LisM status: {}", if enabled { "enabled" } else { "disabled" });
                if let Some(coordinator) = app_state.boss_coordinator.as_ref() {
                    let metadata = coordinator.routed_step_metadata_snapshot().await;
                    if metadata.is_empty() {
                        base
                    } else {
                        let mut lines = vec![base];
                        let mut step_ids: Vec<usize> = metadata.keys().copied().collect();
                        step_ids.sort_unstable();
                        let mut total_routed: usize = 0;
                        let mut override_hits: usize = 0;
                        let mut total_cache_r: usize = 0;
                        let mut total_cache_w: usize = 0;
                        let mut total_fallback: usize = 0;
                        let mut total_mismatch: usize = 0;
                        for id in step_ids {
                            let m = &metadata[&id];
                            total_routed += 1;
                            if m.provider_profile_id.is_some() { override_hits += 1; }
                            total_cache_r += m.cache_read_tokens.unwrap_or(0);
                            total_cache_w += m.cache_write_tokens.unwrap_or(0);
                            total_fallback += m.fallback_count.unwrap_or(0);
                            total_mismatch += m.projection_mismatch_count.unwrap_or(0);
                            lines.push(format!(
                                "  step {id}: tier={tier} profile={profile} frame_size={size} cache_r={cr} cache_w={cw} fallback={fb} mismatch={mm}",
                                tier = m.model_tier.as_deref().unwrap_or("-"),
                                profile = m.provider_profile_id.as_deref().unwrap_or("-"),
                                size = m.state_frame_size.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                                cr = m.cache_read_tokens.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                                cw = m.cache_write_tokens.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                                fb = m.fallback_count.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                                mm = m.projection_mismatch_count.map(|n| n.to_string()).unwrap_or_else(|| "-".into()),
                            ));
                        }
                        lines.push(format!(
                            "  total_steps_routed: {total_routed} override_hits: {override_hits} cache_r: {total_cache_r} cache_w: {total_cache_w} fallback: {total_fallback} mismatch: {total_mismatch}"
                        ));
                        lines.join("\n")
                    }
                } else {
                    base
                }
            }
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
        "LisM is currently {mode}.\n\nAvailable building blocks:\n- StateFrame schema and StateDecision validation\n- BossPlan -> StateFrame projection\n- Stateless JSON decision loop\n- StateFrame orchestrator seam\n- Toolset / skillset router is attached to the live LisM -> /boss production path\n- Model-tier router and provider_profile_id routing are connected to the production path\n- Per-step routed metadata (tier, profile, frame_size, cache, fallback) is recorded and visible in /LisM status and /boss report\n- Archive / retention for accepted and open items\n- Production-path tests for the current StateFrame orchestration pipeline\n\nDeferred items:\n- cache_read/write_tokens, fallback_count, projection_mismatch_count are v1 stubs (always 0; real counters not yet wired)\n- /LisM persistence is not yet connected\n- fallback ladder expansion is still deferred"
    )
}

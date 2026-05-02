use crate::core::prompt_segment::{PromptAssembly, PromptSegmentKind};

/// Provider prompt-cache capability declaration.
/// T26.2: pure metadata — does not affect budget decisions or request payload (T26.3 handles that).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptCacheCapability {
    /// Provider does not support prompt caching.
    Unsupported,
    /// Ephemeral cache-control blocks for messages-style APIs (per-segment, TTL ~5 min).
    MessagesApiEphemeral,
    /// OpenAI-compatible prefix caching (automatic, no explicit cache-control).
    OpenAICompatiblePrefix,
    /// Cache capability present but explicitly disabled for this session.
    ManualNone,
}

impl PromptCacheCapability {
    #[allow(non_upper_case_globals)]
    pub const AnthropicEphemeral: PromptCacheCapability =
        PromptCacheCapability::MessagesApiEphemeral;
}

impl Default for PromptCacheCapability {
    fn default() -> Self {
        Self::Unsupported
    }
}

/// Provider context window parameters.
/// v1: conservative defaults for large-context models (200k context, 8k output reserve).
#[derive(Debug, Clone, Copy)]
pub struct ProviderProfile {
    /// Total context window in estimated tokens.
    pub context_window: usize,
    /// Tokens to reserve for model output.
    pub output_reserve: usize,
    /// Minimum chars for a cache block to be eligible (provider-side).
    pub cache_min_size: usize,
    /// Cache capability for this provider. T26.2: metadata only, not used in budget decisions.
    pub prompt_cache: PromptCacheCapability,
}

impl Default for ProviderProfile {
    fn default() -> Self {
        Self {
            context_window: 200_000,
            output_reserve: 8_000,
            cache_min_size: 1_024,
            // Default profile assumes a messages-style provider with ephemeral cache support.
            prompt_cache: PromptCacheCapability::MessagesApiEphemeral,
        }
    }
}

/// Estimated token breakdown for a prompt assembly.
#[derive(Debug, Clone, Copy)]
pub struct BudgetEstimate {
    pub static_prefix_chars: usize,
    pub dynamic_suffix_chars: usize,
    /// Rough token estimate: total chars / 3.5 (conservative for mixed English/code).
    pub estimated_tokens: usize,
    pub output_reserve: usize,
}

impl BudgetEstimate {
    pub fn available_tokens(&self, profile: &ProviderProfile) -> usize {
        profile.context_window.saturating_sub(self.output_reserve)
    }

    pub fn over_budget(&self, profile: &ProviderProfile) -> bool {
        self.estimated_tokens > self.available_tokens(profile)
    }

    pub fn static_prefix_tokens(&self) -> usize {
        (self.static_prefix_chars as f64 / 3.5).ceil() as usize
    }
}

/// Dispatch decision from the budget gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetDecision {
    /// Prompt fits within budget — send as-is.
    Pass,
    /// Dynamic suffix exceeds budget — caller should summarize/trim before sending.
    Degrade { reason: String },
    /// Even the static prefix exceeds budget — dispatch must be rejected.
    Reject { reason: String },
}

/// Estimate token budget for a prompt assembly and return a dispatch decision.
/// Pure function — does not modify `assembly`, BossPlan, or session_snapshot.
pub fn evaluate_prompt_budget(
    assembly: &PromptAssembly,
    profile: &ProviderProfile,
) -> (BudgetEstimate, BudgetDecision) {
    let static_chars: usize = assembly
        .segments()
        .iter()
        .filter(|s| s.is_cacheable())
        .map(|s| s.content.len())
        .sum();
    let dynamic_chars: usize = assembly
        .segments()
        .iter()
        .filter(|s| !s.is_cacheable())
        .map(|s| s.content.len())
        .sum();
    let total_chars = static_chars + dynamic_chars;
    let estimated_tokens = (total_chars as f64 / 3.5).ceil() as usize;

    let estimate = BudgetEstimate {
        static_prefix_chars: static_chars,
        dynamic_suffix_chars: dynamic_chars,
        estimated_tokens,
        output_reserve: profile.output_reserve,
    };

    let available = estimate.available_tokens(profile);
    let static_tokens = estimate.static_prefix_tokens();

    let decision = if static_tokens > available {
        BudgetDecision::Reject {
            reason: format!(
                "static prefix alone ({static_tokens} tokens) exceeds available budget ({available} tokens)"
            ),
        }
    } else if estimated_tokens > available {
        BudgetDecision::Degrade {
            reason: format!(
                "total prompt ({estimated_tokens} tokens) exceeds available budget ({available} tokens); dynamic suffix should be compressed"
            ),
        }
    } else {
        BudgetDecision::Pass
    };

    (estimate, decision)
}

/// Convenience: evaluate a raw message string against the default provider profile.
/// Used by `ask_b_session` before the T25/T25.2 trim/summarize path.
pub fn evaluate_message_budget(message: &str) -> BudgetDecision {
    use crate::core::prompt_segment::PromptSegment;
    let mut assembly = PromptAssembly::new();
    assembly.push(PromptSegment::new(
        "message",
        PromptSegmentKind::DynamicEvidence,
        message,
    ));
    let profile = ProviderProfile::default();
    evaluate_prompt_budget(&assembly, &profile).1
}

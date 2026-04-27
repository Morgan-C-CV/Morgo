use crate::core::state_frame::{ActorRole, AgentState, EffortLevel};

/// Abstract model tier used by T27 routing.
/// Pure metadata — does not resolve a concrete provider/model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTier {
    Low,
    Medium,
    High,
}

/// Result of static model-tier routing.
/// `provider_profile_id` is intentionally `None` in v1 — runtime lookup is deferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRoute {
    pub tier: ModelTier,
    pub provider_profile_id: Option<String>,
}

/// Route `(effort, role, state)` to a model tier.
///
/// Pure static mapping — no runtime lookup, no provider config reads, no side effects.
/// Role/state rules may clamp or raise the effort-derived tier according to explicit safety/cost rules.
pub fn route_model_tier(
    effort: EffortLevel,
    role: ActorRole,
    state: AgentState,
) -> ModelRoute {
    let base = match effort {
        EffortLevel::L => ModelTier::Low,
        EffortLevel::M => ModelTier::Medium,
        EffortLevel::H => ModelTier::High,
    };

    let tier = match (role, state, base) {
        // Planning by DesignerA should never go below Medium.
        (ActorRole::DesignerA, AgentState::Planning, ModelTier::Low) => ModelTier::Medium,
        // Verification should not run on Low tier.
        (ActorRole::Verifier, AgentState::Verifying, ModelTier::Low) => ModelTier::Medium,
        // Summarizer is capped at Medium in v1 — even H downgrades to M.
        (ActorRole::Summarizer, _, ModelTier::High) => ModelTier::Medium,
        // All other combinations use the effort-derived default.
        _ => base,
    };

    ModelRoute {
        tier,
        provider_profile_id: None,
    }
}

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
/// `provider_profile_id` is `Some` only for combinations with an explicit profile rule;
/// all other combinations remain `None` (runtime inherits the session snapshot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRoute {
    pub tier: ModelTier,
    pub provider_profile_id: Option<String>,
}

/// Route `(effort, role, state)` to a model tier.
///
/// Pure static mapping — no runtime lookup, no provider config reads, no side effects.
/// Role/state rules may clamp or raise the effort-derived tier according to explicit safety/cost rules.
pub fn route_model_tier(effort: EffortLevel, role: ActorRole, state: AgentState) -> ModelRoute {
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
        provider_profile_id: match (role, state, tier) {
            (ActorRole::Worker, AgentState::Executing, ModelTier::Medium) => {
                Some("worker-override".into())
            }
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_executing_medium_produces_worker_override_profile() {
        let route = route_model_tier(EffortLevel::M, ActorRole::Worker, AgentState::Executing);
        assert_eq!(route.tier, ModelTier::Medium);
        assert_eq!(
            route.provider_profile_id.as_deref(),
            Some("worker-override")
        );
    }

    #[test]
    fn executor_b_executing_medium_stays_none() {
        let route = route_model_tier(EffortLevel::M, ActorRole::ExecutorB, AgentState::Executing);
        assert_eq!(route.tier, ModelTier::Medium);
        assert_eq!(route.provider_profile_id, None);
    }

    #[test]
    fn designer_a_planning_low_stays_none() {
        let route = route_model_tier(EffortLevel::L, ActorRole::DesignerA, AgentState::Planning);
        assert_eq!(route.tier, ModelTier::Medium); // clamped
        assert_eq!(route.provider_profile_id, None);
    }

    #[test]
    fn verifier_verifying_low_stays_none() {
        let route = route_model_tier(EffortLevel::L, ActorRole::Verifier, AgentState::Verifying);
        assert_eq!(route.tier, ModelTier::Medium); // clamped
        assert_eq!(route.provider_profile_id, None);
    }

    #[test]
    fn summarizer_executing_high_stays_none() {
        let route = route_model_tier(EffortLevel::H, ActorRole::Summarizer, AgentState::Executing);
        assert_eq!(route.tier, ModelTier::Medium); // capped
        assert_eq!(route.provider_profile_id, None);
    }

    #[test]
    fn worker_correcting_high_stays_none() {
        let route = route_model_tier(EffortLevel::H, ActorRole::Worker, AgentState::Correcting);
        assert_eq!(route.tier, ModelTier::High);
        assert_eq!(route.provider_profile_id, None);
    }
}

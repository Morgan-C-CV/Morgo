use crate::bootstrap::InteractionSurface;
use crate::interaction::envelope::ActorIdentity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny { reason: String },
}

pub trait SurfaceAuthorizer: Send + Sync {
    fn authorize(
        &self,
        surface: InteractionSurface,
        actor: &ActorIdentity,
        raw_input: &str,
    ) -> AuthDecision;
}

#[derive(Debug, Clone, Default)]
pub struct DefaultSurfaceAuthorizer;

impl SurfaceAuthorizer for DefaultSurfaceAuthorizer {
    fn authorize(
        &self,
        surface: InteractionSurface,
        actor: &ActorIdentity,
        _raw_input: &str,
    ) -> AuthDecision {
        if matches!(surface, InteractionSurface::Cli) || actor.is_authenticated {
            AuthDecision::Allow
        } else {
            AuthDecision::Deny {
                reason: "unauthenticated actor for remote surface".into(),
            }
        }
    }
}

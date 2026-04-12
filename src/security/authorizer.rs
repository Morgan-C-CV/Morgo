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
        raw_input: &str,
    ) -> AuthDecision {
        if matches!(surface, InteractionSurface::Cli) {
            return AuthDecision::Allow;
        }
        if !actor.is_authenticated {
            return AuthDecision::Deny {
                reason: "unauthenticated actor for remote surface".into(),
            };
        }
        if matches!(surface, InteractionSurface::Remote)
            && matches!(raw_input.trim(), "/permissions" | "/session")
        {
            return AuthDecision::Deny {
                reason: "command is blocked on remote surface".into(),
            };
        }
        AuthDecision::Allow
    }
}

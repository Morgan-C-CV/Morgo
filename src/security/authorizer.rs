use crate::bootstrap::InteractionSurface;
use crate::interaction::envelope::NormalizedInput;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDenyCategory {
    Unauthenticated,
    NotAllowlisted,
    RateLimited,
    AbuseBlocked,
    SurfaceCommandBlocked,
}

impl AuthDenyCategory {
    pub fn code(self) -> &'static str {
        match self {
            AuthDenyCategory::Unauthenticated => "unauthenticated",
            AuthDenyCategory::NotAllowlisted => "not_allowlisted",
            AuthDenyCategory::RateLimited => "rate_limited",
            AuthDenyCategory::AbuseBlocked => "abuse_blocked",
            AuthDenyCategory::SurfaceCommandBlocked => "surface_command_blocked",
        }
    }

    pub fn remote_denial_message(self) -> &'static str {
        match self {
            AuthDenyCategory::Unauthenticated => "Denied: unauthenticated actor for remote surface",
            AuthDenyCategory::NotAllowlisted => {
                "Denied: actor is not allowlisted for remote surface"
            }
            AuthDenyCategory::RateLimited => "Denied: remote request rate limit exceeded",
            AuthDenyCategory::AbuseBlocked => {
                "Denied: actor is temporarily blocked on remote surface"
            }
            AuthDenyCategory::SurfaceCommandBlocked => {
                "Denied: command is blocked on remote surface"
            }
        }
    }

    pub fn reason(self, input: &NormalizedInput) -> String {
        let detail = match self {
            AuthDenyCategory::Unauthenticated => {
                format!("unauthenticated actor for {:?} surface", input.surface)
            }
            AuthDenyCategory::NotAllowlisted => format!(
                "actor {} is not allowlisted for {:?} surface",
                input.actor.actor_id, input.surface
            ),
            AuthDenyCategory::RateLimited => format!(
                "actor {} exceeded request rate for {:?} surface",
                input.actor.actor_id, input.surface
            ),
            AuthDenyCategory::AbuseBlocked => format!(
                "actor {} is temporarily blocked on {:?} surface",
                input.actor.actor_id, input.surface
            ),
            AuthDenyCategory::SurfaceCommandBlocked => {
                "command is blocked on remote surface".to_string()
            }
        };
        format!("{}: {detail}", self.code())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny {
        category: AuthDenyCategory,
        reason: String,
    },
}

pub trait SurfaceAuthorizer: Send + Sync {
    fn authorize(&self, input: &NormalizedInput) -> AuthDecision;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceAdmissionPolicy {
    pub allowlisted_actors: HashSet<String>,
    pub max_requests_per_window: Option<usize>,
    pub window_seconds: u64,
    pub abuse_denial_threshold: Option<usize>,
}

impl Default for SurfaceAdmissionPolicy {
    fn default() -> Self {
        Self {
            allowlisted_actors: HashSet::new(),
            max_requests_per_window: None,
            window_seconds: 60,
            abuse_denial_threshold: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct AdmissionTracker {
    recent_requests: VecDeque<Instant>,
    consecutive_denials: usize,
}

#[derive(Debug, Clone)]
pub struct DefaultSurfaceAuthorizer {
    remote_policy: SurfaceAdmissionPolicy,
    telegram_policy: SurfaceAdmissionPolicy,
    trackers: Arc<Mutex<HashMap<String, AdmissionTracker>>>,
}

impl Default for DefaultSurfaceAuthorizer {
    fn default() -> Self {
        Self {
            remote_policy: SurfaceAdmissionPolicy::default(),
            telegram_policy: SurfaceAdmissionPolicy::default(),
            trackers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl DefaultSurfaceAuthorizer {
    pub fn with_remote_policy(mut self, policy: SurfaceAdmissionPolicy) -> Self {
        self.remote_policy = policy;
        self
    }

    pub fn with_telegram_policy(mut self, policy: SurfaceAdmissionPolicy) -> Self {
        self.telegram_policy = policy;
        self
    }

    fn policy_for_surface(&self, surface: InteractionSurface) -> Option<&SurfaceAdmissionPolicy> {
        match surface {
            InteractionSurface::Cli => None,
            InteractionSurface::Remote => Some(&self.remote_policy),
            InteractionSurface::Telegram => Some(&self.telegram_policy),
        }
    }

    fn tracker_key(input: &NormalizedInput) -> String {
        format!(
            "{:?}:{}:{}",
            input.surface, input.session_id, input.actor.actor_id
        )
    }

    fn note_denial(&self, input: &NormalizedInput) {
        let key = Self::tracker_key(input);
        if let Ok(mut trackers) = self.trackers.lock() {
            let tracker = trackers.entry(key).or_default();
            tracker.consecutive_denials += 1;
        }
    }

    fn note_allowed_request(&self, input: &NormalizedInput, window_seconds: u64) {
        let key = Self::tracker_key(input);
        if let Ok(mut trackers) = self.trackers.lock() {
            let tracker = trackers.entry(key).or_default();
            prune_old_requests(&mut tracker.recent_requests, window_seconds);
            tracker.recent_requests.push_back(Instant::now());
            tracker.consecutive_denials = 0;
        }
    }

    fn deny(&self, input: &NormalizedInput, category: AuthDenyCategory) -> AuthDecision {
        self.note_denial(input);
        AuthDecision::Deny {
            category,
            reason: category.reason(input),
        }
    }
}

impl SurfaceAuthorizer for DefaultSurfaceAuthorizer {
    fn authorize(&self, input: &NormalizedInput) -> AuthDecision {
        if matches!(input.surface, InteractionSurface::Cli) {
            return AuthDecision::Allow;
        }

        let Some(policy) = self.policy_for_surface(input.surface) else {
            return AuthDecision::Allow;
        };

        if !input.actor.is_authenticated {
            return self.deny(input, AuthDenyCategory::Unauthenticated);
        }

        let key = Self::tracker_key(input);
        if let Ok(mut trackers) = self.trackers.lock() {
            let tracker = trackers.entry(key).or_default();
            prune_old_requests(&mut tracker.recent_requests, policy.window_seconds);

            if policy
                .abuse_denial_threshold
                .is_some_and(|threshold| tracker.consecutive_denials >= threshold)
            {
                tracker.consecutive_denials += 1;
                return AuthDecision::Deny {
                    category: AuthDenyCategory::AbuseBlocked,
                    reason: AuthDenyCategory::AbuseBlocked.reason(input),
                };
            }

            if !policy.allowlisted_actors.is_empty()
                && !policy.allowlisted_actors.contains(&input.actor.actor_id)
            {
                tracker.consecutive_denials += 1;
                return AuthDecision::Deny {
                    category: AuthDenyCategory::NotAllowlisted,
                    reason: AuthDenyCategory::NotAllowlisted.reason(input),
                };
            }

            if policy
                .max_requests_per_window
                .is_some_and(|limit| tracker.recent_requests.len() >= limit)
            {
                tracker.consecutive_denials += 1;
                return AuthDecision::Deny {
                    category: AuthDenyCategory::RateLimited,
                    reason: AuthDenyCategory::RateLimited.reason(input),
                };
            }
        }

        if matches!(input.surface, InteractionSurface::Remote)
            && matches!(input.raw.trim(), "/permissions" | "/session")
        {
            return self.deny(input, AuthDenyCategory::SurfaceCommandBlocked);
        }

        self.note_allowed_request(input, policy.window_seconds);
        AuthDecision::Allow
    }
}

fn prune_old_requests(requests: &mut VecDeque<Instant>, window_seconds: u64) {
    let window = Duration::from_secs(window_seconds.max(1));
    let now = Instant::now();
    while requests
        .front()
        .is_some_and(|timestamp| now.duration_since(*timestamp) >= window)
    {
        requests.pop_front();
    }
}

use std::path::Path;
use std::time::{Duration, Instant};

use crate::skills::loader::{SkillLoadResult, SkillLoaderCache};

/// Why a skill cache entry was invalidated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillInvalidationReason {
    /// One or more SKILL.md files changed (fingerprint mismatch).
    FileChanged,
    /// The project config or cwd changed, requiring a fresh load.
    ConfigChanged,
    /// The active model runtime snapshot was switched (profile change).
    RuntimeSnapshotSwitch,
    /// Caller explicitly requested a reload (e.g. `/skills reload`).
    ExplicitReload,
    /// The cache entry exceeded its configured TTL.
    TtlExpired,
}

impl SkillInvalidationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FileChanged => "file_changed",
            Self::ConfigChanged => "config_changed",
            Self::RuntimeSnapshotSwitch => "runtime_snapshot_switch",
            Self::ExplicitReload => "explicit_reload",
            Self::TtlExpired => "ttl_expired",
        }
    }
}

/// Policy controlling when a long-lived skill cache entry expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillCachePolicy {
    /// Maximum age of a cache entry before it is considered stale.
    /// `None` means no TTL — only fingerprint changes trigger reload.
    pub ttl: Option<Duration>,
}

impl Default for SkillCachePolicy {
    fn default() -> Self {
        Self { ttl: None }
    }
}

impl SkillCachePolicy {
    pub fn with_ttl(seconds: u64) -> Self {
        Self {
            ttl: Some(Duration::from_secs(seconds)),
        }
    }

    pub fn no_ttl() -> Self {
        Self { ttl: None }
    }
}

/// A versioned, timestamped cache entry for a skill load result.
#[derive(Debug, Clone)]
pub struct SkillCacheEntry {
    pub result: SkillLoadResult,
    /// Monotonically increasing counter — incremented on every reload.
    pub generation: u64,
    /// Wall-clock time when this entry was populated.
    pub loaded_at: Instant,
    /// Why the previous entry was invalidated to produce this one.
    /// `None` for the initial load.
    pub invalidation_reason: Option<SkillInvalidationReason>,
}

impl SkillCacheEntry {
    fn new(result: SkillLoadResult, generation: u64, reason: Option<SkillInvalidationReason>) -> Self {
        Self {
            result,
            generation,
            loaded_at: Instant::now(),
            invalidation_reason: reason,
        }
    }

    pub fn is_expired(&self, policy: &SkillCachePolicy) -> bool {
        match policy.ttl {
            Some(ttl) => self.loaded_at.elapsed() > ttl,
            None => false,
        }
    }
}

/// Long-lived runtime skill cache.
///
/// Wraps `SkillLoaderCache` (fingerprint-based) with a generation counter, explicit
/// invalidation reasons, and an optional TTL policy. Intended to be held for the lifetime
/// of a runtime session — not recreated per-request.
pub struct SkillRuntimeCache {
    loader_cache: SkillLoaderCache,
    entry: Option<SkillCacheEntry>,
    policy: SkillCachePolicy,
    pending_invalidation: Option<SkillInvalidationReason>,
    next_generation: u64,
}

impl Default for SkillRuntimeCache {
    fn default() -> Self {
        Self::new(SkillCachePolicy::default())
    }
}

impl SkillRuntimeCache {
    pub fn new(policy: SkillCachePolicy) -> Self {
        Self {
            loader_cache: SkillLoaderCache::default(),
            entry: None,
            policy,
            pending_invalidation: None,
            next_generation: 1,
        }
    }

    /// Current generation number. `0` means nothing has been loaded yet.
    pub fn generation(&self) -> u64 {
        self.entry.as_ref().map(|e| e.generation).unwrap_or(0)
    }

    /// The reason the current entry was loaded (i.e. why the previous one was invalidated).
    pub fn last_invalidation_reason(&self) -> Option<SkillInvalidationReason> {
        self.entry.as_ref().and_then(|e| e.invalidation_reason)
    }

    /// Mark the cache as needing reload on the next `load_or_reload` call.
    pub fn invalidate(&mut self, reason: SkillInvalidationReason) {
        self.loader_cache.invalidate();
        self.pending_invalidation = Some(reason);
    }

    /// Load skills from `root`, reloading if:
    /// - no entry exists yet
    /// - a pending explicit invalidation was requested
    /// - the TTL has expired
    /// - the filesystem fingerprint changed (detected by `SkillLoaderCache`)
    ///
    /// Returns `(entry, reloaded)`.
    pub fn load_or_reload(
        &mut self,
        root: &Path,
    ) -> anyhow::Result<(&SkillCacheEntry, bool)> {
        // Check TTL expiry — if expired, mark as pending invalidation
        if let Some(entry) = &self.entry {
            if entry.is_expired(&self.policy) && self.pending_invalidation.is_none() {
                self.pending_invalidation = Some(SkillInvalidationReason::TtlExpired);
                self.loader_cache.invalidate();
            }
        }

        let reason = self.pending_invalidation.take();

        let (result, file_changed) = self.loader_cache.load_or_reload(root)?;

        let is_first_load = self.entry.is_none();

        let effective_reason = reason.or_else(|| {
            // file_changed on first load is the initial population, not a change event
            if file_changed && !is_first_load {
                Some(SkillInvalidationReason::FileChanged)
            } else {
                None
            }
        });

        let reloaded = is_first_load || effective_reason.is_some();

        if reloaded {
            let generation = self.next_generation;
            self.next_generation += 1;
            self.entry = Some(SkillCacheEntry::new(result, generation, effective_reason));
        }

        Ok((self.entry.as_ref().unwrap(), reloaded))
    }

    /// Snapshot of the current entry, if any.
    pub fn snapshot(&self) -> Option<&SkillCacheEntry> {
        self.entry.as_ref()
    }
}

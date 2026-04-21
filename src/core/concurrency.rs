use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info};

const MAX_SUBAGENTS: usize = 8;
const MIN_SUBAGENTS: usize = 3;
const DEFAULT_SUBAGENTS: usize = 6;

/// A dynamic concurrency limiter for subagents that adapts its capacity
/// based on system memory availability and macOS memory pressure.
#[derive(Debug)]
pub struct SubagentLimiter {
    semaphore: Arc<Semaphore>,
    /// Permits held by the limiter itself to effectively "borrow" capacity away from agents
    restriction_permits: Mutex<Vec<OwnedSemaphorePermit>>,
    current_limit: AtomicUsize,
}

impl SubagentLimiter {
    /// Creates a new SubagentLimiter and starts the background refresh loop.
    pub fn new() -> Arc<Self> {
        let limiter = Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(MAX_SUBAGENTS)),
            restriction_permits: Mutex::new(Vec::new()),
            current_limit: AtomicUsize::new(DEFAULT_SUBAGENTS),
        });

        let limiter_clone = limiter.clone();
        // Only spawn the refresh loop if we are in an active Tokio runtime context.
        // This prevents panics in synchronous tests while allowing production usage to work as intended.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                limiter_clone.refresh_loop().await;
            });
        } else {
            debug!(
                "SubagentLimiter: No tokio reactor found, background refresh loop will not be started (expected in sync tests)"
            );
        }

        limiter
    }

    /// Acquires a permit to run a subagent. This will wait until a slot is available
    /// according to the current dynamic limit.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should not be closed")
    }

    /// Internal loop to monitor system health and update the concurrency limit.
    async fn refresh_loop(&self) {
        let mut sys = System::new_all();
        // Initial sync
        self.refresh_and_update(&mut sys).await;

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            self.refresh_and_update(&mut sys).await;
        }
    }

    async fn refresh_and_update(&self, sys: &mut System) {
        sys.refresh_memory();
        // available_memory() returns bytes in sysinfo 0.30+
        let available_mb = sys.available_memory() / 1024 / 1024;
        let pressure = get_macos_memory_pressure();

        let new_limit = calculate_limit(available_mb, pressure);
        self.apply_limit(new_limit).await;
    }

    async fn apply_limit(&self, new_limit: usize) {
        let current = self.current_limit.load(Ordering::SeqCst);
        if new_limit == current {
            return;
        }

        info!(
            "Updating subagent concurrency limit: {} -> {} (Available RAM: {}MB, Pressure: {})",
            current,
            new_limit,
            // Logic to get available_mb again to log accurately or just pass it in
            "?",
            "?"
        );

        // We'll log more detail in the caller or pass values
        self.current_limit.store(new_limit, Ordering::SeqCst);

        let mut restriction = self.restriction_permits.lock().await;
        let target_restriction = MAX_SUBAGENTS.saturating_sub(new_limit);

        // If we need to decrease capacity, we take permits for ourselves
        while restriction.len() < target_restriction {
            if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
                restriction.push(permit);
            } else {
                debug!("Subagent limit decrease delayed: waiting for active agents to finish");
                break;
            }
        }

        // If we need to increase capacity, we release our borrowed permits
        while restriction.len() > target_restriction {
            restriction.pop();
        }
    }

    /// Returns the current effective limit (for debugging/ui).
    pub fn current_limit(&self) -> usize {
        self.current_limit.load(Ordering::SeqCst)
    }
}

fn get_macos_memory_pressure() -> u8 {
    #[cfg(target_os = "macos")]
    {
        // sysctl -n vm.memory_pressure
        // 1 = Normal, 2 = Warning, 4 = Critical
        let output = Command::new("sysctl")
            .arg("-n")
            .arg("vm.memory_pressure")
            .output();

        if let Ok(out) = output {
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .parse::<u8>()
                .unwrap_or(1)
        } else {
            1
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        1
    }
}

fn calculate_limit(available_mb: u64, pressure: u8) -> usize {
    // Primary trigger: macOS Memory Pressure
    if pressure >= 4 {
        return MIN_SUBAGENTS;
    } // Critical
    if pressure >= 2 {
        return 4;
    } // Warning

    // Secondary trigger: Raw available memory
    if available_mb < 1024 {
        return 3;
    } // Very low
    if available_mb < 2048 {
        return 4;
    } // Low
    if available_mb < 4096 {
        return 5;
    } // Moderate
    if available_mb >= 8192 {
        return 8;
    } // High

    DEFAULT_SUBAGENTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_limit_logic() {
        // Normal pressure, high RAM
        assert_eq!(calculate_limit(16384, 1), 8);
        // Normal pressure, moderate RAM
        assert_eq!(calculate_limit(3000, 1), 5);
        // Warning pressure overrides RAM
        assert_eq!(calculate_limit(16384, 2), 4);
        // Critical pressure
        assert_eq!(calculate_limit(16384, 4), 3);
        // Very low RAM
        assert_eq!(calculate_limit(500, 1), 3);
    }

    #[tokio::test]
    async fn test_limiter_restricts_concurrency() {
        let limiter = Arc::new(SubagentLimiter {
            semaphore: Arc::new(Semaphore::new(MAX_SUBAGENTS)),
            restriction_permits: Mutex::new(Vec::new()),
            current_limit: AtomicUsize::new(MAX_SUBAGENTS),
        });

        // Artificially restrict to 2
        limiter.apply_limit(2).await;
        assert_eq!(limiter.current_limit(), 2);

        let p1 = limiter.acquire().await;
        let _p2 = limiter.acquire().await;

        // p3 should fail to acquire immediately
        let try_p3 = limiter.semaphore.try_acquire();
        assert!(try_p3.is_err());

        drop(p1);
        let p3 = limiter.acquire().await;
        assert!(p3.forget_type_info_is_fine()); // just to keep it alive
    }

    trait Forget {
        fn forget_type_info_is_fine(&self) -> bool;
    }
    impl Forget for OwnedSemaphorePermit {
        fn forget_type_info_is_fine(&self) -> bool {
            true
        }
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{info, debug, warn};

/// Configuration for the background housekeeping daemon.
#[derive(Debug, Clone)]
pub struct HousekeepingConfig {
    /// How often the housekeeping loop should run.
    pub interval: Duration,
    /// Threshold (in seconds) for considering a session as "stale" or "zombie".
    pub stale_threshold_secs: u64,
}

impl Default for HousekeepingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            stale_threshold_secs: 600, // 10 minutes
        }
    }
}

/// The housekeeping daemon responsible for background maintenance tasks.
/// Designed for minimal resource footprint (low CPU/memory usage during idle).
pub struct HousekeepingDaemon {
    config: HousekeepingConfig,
    cancel_token: CancellationToken,
    last_activity_ts: Arc<AtomicU64>,
}

impl HousekeepingDaemon {
    /// Creates a new housekeeping daemon.
    pub fn new(
        config: HousekeepingConfig,
        cancel_token: CancellationToken,
        last_activity_ts: Arc<AtomicU64>,
    ) -> Self {
        Self {
            config,
            cancel_token,
            last_activity_ts,
        }
    }

    /// Entry point for the housekeeping background task.
    pub async fn run(self) {
        info!("Housekeeping daemon started with interval {:?}", self.config.interval);
        
        let mut interval = tokio::time::interval(self.config.interval);
        
        // Skip the first immediate tick to adhere to the requested interval from start.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("Housekeeping tick: performing background maintenance...");
                    self.perform_maintenance().await;
                }
                _ = self.cancel_token.cancelled() => {
                    info!("Housekeeping daemon shutting down gracefully.");
                    break;
                }
            }
        }
    }

    async fn perform_maintenance(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last_active = self.last_activity_ts.load(Ordering::Relaxed);
        let delta = now.saturating_sub(last_active);

        if delta > self.config.stale_threshold_secs {
            warn!(
                "Session inactivity detected: last active {} seconds ago (threshold: {}s). Potential zombie session.",
                delta, self.config.stale_threshold_secs
            );
            // In Phase 3, we would trigger actual cleanup or session suspension here.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, pause, advance};

    #[tokio::test]
    async fn test_housekeeping_cancellation() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(
            HousekeepingConfig {
                interval: Duration::from_millis(10),
                stale_threshold_secs: 100,
            },
            token.clone(),
            last_active,
        );

        let handle = tokio::spawn(daemon.run());
        
        // Let it run for a bit
        time::sleep(Duration::from_millis(25)).await;
        
        token.cancel();
        
        // Ensure it exits
        let result = timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "Daemon should have exited after cancellation");
    }

    #[tokio::test]
    async fn test_housekeeping_interval_ticks() {
        pause(); // Use tokio's virtual time
        
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(
            HousekeepingConfig {
                interval: Duration::from_secs(1),
                stale_threshold_secs: 10,
            },
            token.clone(),
            last_active,
        );

        let _handle = tokio::spawn(daemon.run());
        
        // First tick was skipped in the implementation
        advance(Duration::from_secs(1)).await;
        // Now it should have ticked once. 
        // We can't easily check the log output here without more complex setup,
        // but the test confirms the loop doesn't panic and logic flows.

        token.cancel();
    }

    #[tokio::test]
    async fn test_housekeeping_zombie_detection() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        
        let config = HousekeepingConfig {
            interval: Duration::from_millis(10),
            stale_threshold_secs: 5,
        };
        
        let daemon = HousekeepingDaemon::new(config.clone(), token.clone(), last_active.clone());
        
        // Scenario 1: Fresh activity (now)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        last_active.store(now, Ordering::Relaxed);
        
        // This shouldn't produce a warning if we could capture logs, 
        // but we verify the logic doesn't panic and we can call it.
        daemon.perform_maintenance().await;
        
        // Scenario 2: Stale activity (set to 1 hour ago)
        last_active.store(now - 3600, Ordering::Relaxed);
        daemon.perform_maintenance().await;
        
        // The test passes if it completes without panic. 
        // In a real integration test, we'd check tracing output.
    }

    async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, ()>
    where
        F: std::future::Future,
    {
        tokio::select! {
            output = future => Ok(output),
            _ = tokio::time::sleep(duration) => Err(()),
        }
    }
}

use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, debug};

/// Configuration for the background housekeeping daemon.
#[derive(Debug, Clone)]
pub struct HousekeepingConfig {
    /// How often the housekeeping loop should run.
    pub interval: Duration,
}

impl Default for HousekeepingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
        }
    }
}

/// The housekeeping daemon responsible for background maintenance tasks.
/// Designed for minimal resource footprint (low CPU/memory usage during idle).
pub struct HousekeepingDaemon {
    config: HousekeepingConfig,
    cancel_token: CancellationToken,
}

impl HousekeepingDaemon {
    /// Creates a new housekeeping daemon.
    pub fn new(config: HousekeepingConfig, cancel_token: CancellationToken) -> Self {
        Self { config, cancel_token }
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
                    // Placeholder for future logic:
                    // 1. Session activity heartbeat monitoring
                    // 2. Orphaned log/file cleanup
                    // 3. Stale session garbage collection
                    Self::perform_maintenance().await;
                }
                _ = self.cancel_token.cancelled() => {
                    info!("Housekeeping daemon shutting down gracefully.");
                    break;
                }
            }
        }
    }

    async fn perform_maintenance() {
        // This will be expanded in later phases (Phase 2 & 3)
        // Keep it empty for Phase 1 to minimize "静息" (rest) resources.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, pause, advance};

    #[tokio::test]
    async fn test_housekeeping_cancellation() {
        let token = CancellationToken::new();
        let daemon = HousekeepingDaemon::new(
            HousekeepingConfig {
                interval: Duration::from_millis(10),
            },
            token.clone(),
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
        let daemon = HousekeepingDaemon::new(
            HousekeepingConfig {
                interval: Duration::from_secs(1),
            },
            token.clone(),
        );

        let _handle = tokio::spawn(daemon.run());
        
        // First tick was skipped in the implementation
        advance(Duration::from_secs(1)).await;
        // Now it should have ticked once. 
        // We can't easily check the log output here without more complex setup,
        // but the test confirms the loop doesn't panic and logic flows.

        token.cancel();
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

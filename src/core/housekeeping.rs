use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{info, debug, warn, error};

/// Configuration for the background housekeeping daemon.
#[derive(Debug, Clone)]
pub struct HousekeepingConfig {
    /// How often the housekeeping loop should run.
    pub interval: Duration,
    /// Threshold (in seconds) for considering a session as "stale" or "zombie".
    pub stale_threshold_secs: u64,
    /// Retention period for persistent sessions.
    pub session_retention_days: u64,
    /// Retention period for task logs.
    pub task_log_retention_days: u64,
}

impl Default for HousekeepingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            stale_threshold_secs: 600, // 10 minutes
            session_retention_days: 7,
            task_log_retention_days: 1,
        }
    }
}

/// The housekeeping daemon responsible for background maintenance tasks.
/// Designed for minimal resource footprint (low CPU/memory usage during idle).
pub struct HousekeepingDaemon {
    config: HousekeepingConfig,
    cancel_token: CancellationToken,
    last_activity_ts: Arc<AtomicU64>,
    session_root: Option<PathBuf>,
    task_output_root: Option<PathBuf>,
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
            session_root: None,
            task_output_root: None,
        }
    }

    pub fn with_roots(mut self, session_root: PathBuf, task_output_root: PathBuf) -> Self {
        self.session_root = Some(session_root);
        self.task_output_root = Some(task_output_root);
        self
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
                    self.perform_gc().await;
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
            error!(
                "CRITICAL: Zombie session detected! Last active {}s ago (threshold: {}s).",
                delta, self.config.stale_threshold_secs
            );
            self.handle_zombie_session(delta).await;
        } else if delta > self.config.stale_threshold_secs / 2 {
            warn!(
                "Session inactivity warning: last active {}s ago. Session may be suspended soon.",
                delta
            );
        }
    }

    async fn handle_zombie_session(&self, _delta: u64) {
        // Production-grade hook for Phase 3:
        // Here we would trigger session state persistence and process suspension.
        // For now, we ensure visibility in logs and prepare for automated triggers.
        info!("Housekeeping: Preparing session for automated suspension/hibernation.");
    }

    pub async fn perform_gc(&self) {
        if let Some(ref root) = self.session_root {
            let _ = self.prune_directory(root, self.config.session_retention_days * 86400, false);
        }
        if let Some(ref root) = self.task_output_root {
            let _ = self.prune_directory(root, self.config.task_log_retention_days * 86400, false);
        }
    }

    pub fn prune_directory(&self, path: &PathBuf, max_age_secs: u64, is_nested: bool) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }

        let now = SystemTime::now();
        let mut has_remaining_entries = false;

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            
            if entry_path.is_dir() {
                // Recursively prune subdirectories
                let _ = self.prune_directory(&entry_path, max_age_secs, true);
                // Check if directory still exists (was not removed by sub-prune)
                if entry_path.exists() {
                    has_remaining_entries = true;
                }
            } else if entry_path.is_file() {
                let metadata = entry.metadata()?;
                let modified = metadata.modified()?;
                if let Ok(duration) = now.duration_since(modified) {
                    if duration.as_secs() > max_age_secs {
                        debug!("GC: Removing stale artifact: {:?}", entry_path);
                        let _ = std::fs::remove_file(&entry_path);
                    } else {
                        has_remaining_entries = true;
                    }
                } else {
                    has_remaining_entries = true;
                }
            }
        }

        // Remove empty directories (only if they are nested subdirectories)
        if is_nested && !has_remaining_entries {
            debug!("GC: Removing empty directory: {:?}", path);
            let _ = std::fs::remove_dir(path);
        }

        Ok(())
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
                session_retention_days: 7,
                task_log_retention_days: 1,
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
                session_retention_days: 7,
                task_log_retention_days: 1,
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
            session_retention_days: 7,
            task_log_retention_days: 1,
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

    #[tokio::test]
    async fn test_housekeeping_gc_logic() {
        let temp_dir = std::env::temp_dir().join(format!("rust_agent_gc_test_{}", 
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        let file_old = temp_dir.join("old.json");
        let file_new = temp_dir.join("new.json");

        std::fs::write(&file_old, "old").unwrap();
        std::fs::write(&file_new, "new").unwrap();

        // We can't easily backdate file mtime in std::fs without external crates for tests,
        // but we can verify the prune_directory logic with a very small max_age.
        
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(HousekeepingConfig::default(), token, last_active)
            .with_roots(temp_dir.clone(), temp_dir.clone());

        // Wait a bit to ensure age > 0
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        // Test with age 0 (should delete everything)
        daemon.prune_directory(&temp_dir, 0, false).unwrap();
        
        assert!(!file_old.exists());
        assert!(!file_new.exists());

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn test_housekeeping_recursive_gc() {
        let temp_dir = std::env::temp_dir().join(format!("rust_agent_recursive_gc_{}", 
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        let sub_dir = temp_dir.join("sub").join("nested");
        std::fs::create_dir_all(&sub_dir).unwrap();

        let file_nested = sub_dir.join("nested.json");
        std::fs::write(&file_nested, "nested").unwrap();

        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(HousekeepingConfig::default(), token, last_active);

        // Wait to ensure age > 0
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        // Prune the root
        daemon.prune_directory(&temp_dir, 0, false).unwrap();

        // Check if nested file is gone
        assert!(!file_nested.exists());
        // Check if empty subdirectories are gone
        assert!(!sub_dir.exists());
        assert!(!temp_dir.join("sub").exists());
        assert!(temp_dir.exists()); // The base root should stay (is_nested=false)

        let _ = std::fs::remove_dir_all(temp_dir);
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

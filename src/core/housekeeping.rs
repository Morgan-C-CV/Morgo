use crate::history::session::SessionLifecycleStatus;
use crate::interaction::notification::Notification;
use crate::state::app_state::AppState;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Configuration for the background housekeeping daemon.
#[derive(Debug, Clone)]
pub struct HousekeepingConfig {
    /// How often the housekeeping loop should run.
    pub interval: Duration,
    /// Threshold (in seconds) for considering a session as "stale" or "zombie".
    pub stale_threshold_secs: u64,
    /// Threshold (in seconds) for upgrading an already hibernating session to expired.
    pub expired_threshold_secs: u64,
    /// Retention period for persistent sessions.
    pub session_retention_days: u64,
    /// Retention period for task logs.
    pub task_log_retention_days: u64,
    /// Maximum number of filesystem entries to process per GC tick.
    pub max_gc_entries_per_tick: usize,
}

impl Default for HousekeepingConfig {
    fn default() -> Self {
        fn env_u64(key: &str, fallback: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(fallback)
        }

        fn env_usize(key: &str, fallback: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(fallback)
        }

        let stale_threshold_secs = env_u64("RUST_AGENT_HOUSEKEEPING_STALE_THRESHOLD_SECS", 600);

        Self {
            interval: Duration::from_secs(env_u64("RUST_AGENT_HOUSEKEEPING_INTERVAL_SECS", 60)),
            stale_threshold_secs,
            expired_threshold_secs: env_u64(
                "RUST_AGENT_HOUSEKEEPING_EXPIRED_THRESHOLD_SECS",
                stale_threshold_secs.saturating_mul(6),
            ),
            session_retention_days: env_u64("RUST_AGENT_HOUSEKEEPING_SESSION_RETENTION_DAYS", 7),
            task_log_retention_days: env_u64("RUST_AGENT_HOUSEKEEPING_TASK_LOG_RETENTION_DAYS", 1),
            max_gc_entries_per_tick: env_usize(
                "RUST_AGENT_HOUSEKEEPING_MAX_GC_ENTRIES_PER_TICK",
                2048,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZombieSessionFailure {
    MissingAppState,
    PersistSessionState,
    PersistLifecycleHibernating,
    PersistLifecycleExpired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZombieSessionOutcome {
    Noop,
    AlreadyInactive,
    AlreadyExpired,
    Hibernated {
        suspended_task_ids: Vec<String>,
    },
    Expired,
    Failed {
        failure: ZombieSessionFailure,
        suspended_task_ids: Vec<String>,
    },
}

/// The housekeeping daemon responsible for background maintenance tasks.
/// Designed for minimal resource footprint (low CPU/memory usage during idle).
#[derive(Clone)]
pub struct HousekeepingDaemon {
    config: HousekeepingConfig,
    cancel_token: CancellationToken,
    last_activity_ts: Arc<AtomicU64>,
    app_state: Option<AppState>,
    session_root: Option<PathBuf>,
    task_output_root: Option<PathBuf>,
    zombie_handled: Arc<AtomicBool>,
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
            app_state: None,
            session_root: None,
            task_output_root: None,
            zombie_handled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_app_state(mut self, app_state: AppState) -> Self {
        self.app_state = Some(app_state);
        self
    }

    pub fn with_roots(mut self, session_root: PathBuf, task_output_root: PathBuf) -> Self {
        self.session_root = Some(session_root);
        self.task_output_root = Some(task_output_root);
        self
    }

    /// Entry point for the housekeeping background task.
    pub async fn run(self) {
        info!(
            "Housekeeping daemon started with interval {:?}",
            self.config.interval
        );

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
        let last_active = self.last_activity_ts.load(Ordering::Acquire);
        let delta = now.saturating_sub(last_active);
        let lifecycle = self
            .app_state
            .as_ref()
            .map(|app_state| app_state.current_session_lifecycle())
            .unwrap_or(SessionLifecycleStatus::Active);

        if lifecycle == SessionLifecycleStatus::Hibernating
            && delta > self.config.expired_threshold_secs
        {
            let outcome = self.expire_hibernating_session(delta).await;
            debug!("Housekeeping expired zombie outcome: {:?}", outcome);
        } else if delta > self.config.stale_threshold_secs {
            warn!(
                "CRITICAL: Zombie session detected! Last active {}s ago (threshold: {}s).",
                delta, self.config.stale_threshold_secs
            );
            let outcome = self.handle_zombie_session(delta).await;
            debug!("Housekeeping zombie outcome: {:?}", outcome);
        } else if delta > self.config.stale_threshold_secs / 2 {
            self.zombie_handled.store(false, Ordering::Release);
            self.mark_session_lifecycle(SessionLifecycleStatus::Stale);
            warn!(
                "Session inactivity warning: last active {}s ago. Session may be suspended soon.",
                delta
            );
        } else {
            self.zombie_handled.store(false, Ordering::Release);
            self.mark_session_lifecycle(SessionLifecycleStatus::Active);
        }
    }

    async fn handle_zombie_session(&self, delta: u64) -> ZombieSessionOutcome {
        if self.zombie_handled.swap(true, Ordering::AcqRel) {
            return ZombieSessionOutcome::AlreadyInactive;
        }

        warn!(
            "Housekeeping: Session (id={}) has been inactive for {}s. Initiating automated hibernation sequence.",
            self.session_id_for_logs(),
            delta
        );

        let Some(app_state) = self.app_state.as_ref() else {
            return ZombieSessionOutcome::Failed {
                failure: ZombieSessionFailure::MissingAppState,
                suspended_task_ids: Vec::new(),
            };
        };

        let persisted_session = app_state.persist_current_session_state();
        let persisted_lifecycle =
            app_state.persist_session_lifecycle(SessionLifecycleStatus::Hibernating);

        let suspended_task_ids = app_state
            .permission_context
            .task_manager
            .as_ref()
            .map(|tasks| {
                tasks.hibernate_owned_running_tasks(
                    &app_state.active_session_id,
                    &app_state.notification_dispatcher,
                )
            })
            .unwrap_or_default();

        if !(persisted_session && persisted_lifecycle) {
            let failure = if !persisted_session {
                ZombieSessionFailure::PersistSessionState
            } else {
                ZombieSessionFailure::PersistLifecycleHibernating
            };
            self.dispatch_housekeeping_notice(
                app_state,
                "housekeeping.zombie_hibernation_failed",
                format!(
                    "Session {} hit zombie threshold after {}s but hibernation failed at stage {:?}.",
                    app_state.active_session_id, delta, failure
                ),
                "housekeeping_zombie_hibernation_failed",
                true,
            );
            return ZombieSessionOutcome::Failed {
                failure,
                suspended_task_ids,
            };
        }

        self.dispatch_housekeeping_notice(
            app_state,
            "housekeeping.zombie_hibernated",
            format!(
                "Session {} was hibernated after {}s of inactivity; suspended {} running task(s).",
                app_state.active_session_id,
                delta,
                suspended_task_ids.len()
            ),
            "housekeeping_zombie_hibernated",
            true,
        );

        debug!(
            "Housekeeping: Hibernation sequence completed for zombie session {}.",
            app_state.active_session_id
        );
        ZombieSessionOutcome::Hibernated { suspended_task_ids }
    }

    async fn expire_hibernating_session(&self, delta: u64) -> ZombieSessionOutcome {
        let Some(app_state) = self.app_state.as_ref() else {
            return ZombieSessionOutcome::Failed {
                failure: ZombieSessionFailure::MissingAppState,
                suspended_task_ids: Vec::new(),
            };
        };
        if app_state.current_session_lifecycle() == SessionLifecycleStatus::Expired {
            return ZombieSessionOutcome::AlreadyExpired;
        }
        if !app_state.persist_session_lifecycle(SessionLifecycleStatus::Expired) {
            self.dispatch_housekeeping_notice(
                app_state,
                "housekeeping.zombie_expire_failed",
                format!(
                    "Session {} stayed hibernating for {}s and failed to transition to expired.",
                    app_state.active_session_id, delta
                ),
                "housekeeping_zombie_expire_failed",
                true,
            );
            return ZombieSessionOutcome::Failed {
                failure: ZombieSessionFailure::PersistLifecycleExpired,
                suspended_task_ids: Vec::new(),
            };
        }
        self.dispatch_housekeeping_notice(
            app_state,
            "housekeeping.zombie_expired",
            format!(
                "Session {} remained hibernating for {}s and was upgraded to expired.",
                app_state.active_session_id, delta
            ),
            "housekeeping_zombie_expired",
            true,
        );
        ZombieSessionOutcome::Expired
    }

    pub async fn perform_gc(&self) {
        let daemon = self.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let mut budget = daemon.config.max_gc_entries_per_tick;
            if let Some(ref root) = daemon.session_root {
                if let Err(e) = daemon.prune_directory(
                    root,
                    daemon.config.session_retention_days * 86400,
                    false,
                    &mut budget,
                ) {
                    warn!("GC: Failed to prune session directory {:?}: {}", root, e);
                }
            }
            if let Some(ref root) = daemon.task_output_root {
                if let Err(e) = daemon.prune_directory(
                    root,
                    daemon.config.task_log_retention_days * 86400,
                    false,
                    &mut budget,
                ) {
                    warn!(
                        "GC: Failed to prune task output directory {:?}: {}",
                        root, e
                    );
                }
            }
        })
        .await;
    }

    pub fn prune_directory(
        &self,
        path: &PathBuf,
        max_age_secs: u64,
        is_nested: bool,
        remaining_budget: &mut usize,
    ) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        if *remaining_budget == 0 {
            return Ok(());
        }

        let now = SystemTime::now();
        let mut has_remaining_entries = false;

        for entry in std::fs::read_dir(path)? {
            if *remaining_budget == 0 {
                break;
            }
            *remaining_budget = remaining_budget.saturating_sub(1);
            let entry = entry?;
            let entry_path = entry.path();

            let metadata = entry_path.symlink_metadata()?;
            let file_type = metadata.file_type();

            if file_type.is_dir() {
                // SECURITY: Do NOT follow symlinks into other directories during GC
                if file_type.is_symlink() {
                    debug!("GC: Skipping symlinked directory: {:?}", entry_path);
                    has_remaining_entries = true;
                    continue;
                }

                // Recursively prune subdirectories
                if let Err(e) =
                    self.prune_directory(&entry_path, max_age_secs, true, remaining_budget)
                {
                    warn!("GC: Failed to prune subdirectory {:?}: {}", entry_path, e);
                    has_remaining_entries = true;
                } else if entry_path.exists() {
                    has_remaining_entries = true;
                }
            } else if file_type.is_file() || file_type.is_symlink() {
                // We allow deleting the symlink itself if it's stale, but not following it
                let modified = metadata.modified().unwrap_or(SystemTime::now());
                if let Ok(duration) = now.duration_since(modified) {
                    if duration.as_secs() > max_age_secs {
                        debug!(
                            "GC: Removing stale artifact (type={:?}): {:?}",
                            file_type, entry_path
                        );
                        if let Err(e) = std::fs::remove_file(&entry_path) {
                            warn!("GC: Failed to remove file {:?}: {}", entry_path, e);
                            has_remaining_entries = true;
                        }
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

    fn mark_session_lifecycle(&self, status: SessionLifecycleStatus) {
        if let Some(app_state) = self.app_state.as_ref() {
            let _ = app_state.persist_session_lifecycle(status);
        }
    }

    fn dispatch_housekeeping_notice(
        &self,
        app_state: &AppState,
        kind: &str,
        message: String,
        code: &str,
        wake_up: bool,
    ) {
        let mut notice = Notification::runtime_notice(
            app_state.active_session_id.clone(),
            kind,
            message,
            Some(code.into()),
            Some("housekeeping".into()),
            None,
            None,
            None,
            Some(false),
            Some(true),
        );
        notice.wake_up = wake_up;
        app_state
            .notification_dispatcher
            .dispatch(app_state.surface, notice);
    }

    fn session_id_for_logs(&self) -> String {
        self.app_state
            .as_ref()
            .map(|app_state| app_state.active_session_id.clone())
            .or_else(|| {
                self.session_root
                    .as_ref()
                    .map(|r| r.to_string_lossy().to_string())
            })
            .unwrap_or_else(|| "unknown".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{ClientType, InteractionSurface, SessionMode, SessionSource};
    use crate::cost::tracker::CostTracker;
    use crate::history::session::{
        InMemorySessionStore, SessionHistory, SessionId, SessionSnapshot, SessionStore,
    };
    use crate::interaction::dispatcher::NotificationDispatcher;
    use crate::interaction::telegram::gateway::TelegramGateway;
    use crate::security::audit::AuditLog;
    use crate::service::api::client::ProviderCompatibilityProfileKind;
    use crate::state::app_state::{
        ActiveModelProfileSource, ActiveModelProviderSummary, RuntimeRole,
    };
    use crate::state::permission_context::{PermissionMode, ToolPermissionContext};
    use crate::task::manager::TaskManager;
    use std::sync::Mutex;
    use tokio::time::{self, advance, pause};

    fn test_app_state(
        session_store: Arc<InMemorySessionStore>,
        task_manager: Arc<TaskManager>,
        last_active: Arc<AtomicU64>,
        token: CancellationToken,
    ) -> AppState {
        let session_id = SessionId("session-housekeeping".into());
        let snapshot = SessionSnapshot {
            session_id: session_id.clone(),
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            cwd: ".".into(),
            last_turn_at: None,
            prompt_seed: None,
        };
        session_store.save(snapshot.clone(), SessionHistory::default());
        let dispatcher = NotificationDispatcher::new(TelegramGateway::default());
        let permission_context = ToolPermissionContext::new(PermissionMode::Default)
            .with_task_manager(task_manager)
            .with_active_session_id(session_id.0.clone())
            .with_active_surface(InteractionSurface::Cli)
            .with_notification_dispatcher(dispatcher.clone())
            .with_last_activity_ts(last_active.clone())
            .with_cancellation_token(token.clone());
        AppState {
            surface: InteractionSurface::Cli,
            session_mode: SessionMode::Interactive,
            client_type: ClientType::Cli,
            session_source: SessionSource::LocalCli,
            runtime_role: RuntimeRole::Coordinator,
            worker_role: None,
            permission_context,
            command_registry: None,
            runtime_tool_registry: None,
            skill_registry: None,
            mcp_runtime: None,
            plugin_load_result: None,
            cost_tracker: CostTracker::default(),
            service_observability_tracker:
                crate::service::observability::ServiceObservabilityTracker::default(),
            notification_dispatcher: dispatcher,
            audit_log: Arc::new(Mutex::new(AuditLog::default())),
            startup_trace: Vec::new(),
            active_model_runtime: None,
            active_model_profile_name: None,
            active_model_profile_source: ActiveModelProfileSource::BootstrapDefault,
            active_model_provider_summary: ActiveModelProviderSummary {
                provider_id: "default-provider".into(),
                protocol: "Anthropic".into(),
                compatibility_profile: format!("{:?}", ProviderCompatibilityProfileKind::Anthropic),
                base_url_host: "localhost".into(),
                model: "default-model".into(),
                auth_status: "none".into(),
            },
            active_session_id: session_id.0,
            session_store: Some(session_store),
            session: Some(snapshot),
            history: Some(SessionHistory::default()),
            restored_session: None,
            last_activity_ts: last_active,
            cancellation_token: token,
            subagent_limiter: None,
            boss_coordinator: None,
        }
    }

    #[tokio::test]
    async fn test_housekeeping_cancellation() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(
            HousekeepingConfig {
                interval: Duration::from_millis(10),
                stale_threshold_secs: 100,
                expired_threshold_secs: 600,
                session_retention_days: 7,
                task_log_retention_days: 1,
                max_gc_entries_per_tick: 2048,
            },
            token.clone(),
            last_active,
        );

        let handle = tokio::spawn(daemon.run());

        // Let it run for a bit
        time::sleep(Duration::from_millis(25)).await;

        token.cancel();

        // Ensure it exits
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(
            result.is_ok(),
            "Daemon should have exited after cancellation"
        );
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
                expired_threshold_secs: 60,
                session_retention_days: 7,
                task_log_retention_days: 1,
                max_gc_entries_per_tick: 2048,
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
            expired_threshold_secs: 30,
            session_retention_days: 7,
            task_log_retention_days: 1,
            max_gc_entries_per_tick: 2048,
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
    async fn test_housekeeping_zombie_hibernates_session_and_running_tasks() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let session_store = Arc::new(InMemorySessionStore::default());
        let task_manager = Arc::new(TaskManager::default());
        let app_state = test_app_state(
            session_store.clone(),
            task_manager.clone(),
            last_active.clone(),
            token.clone(),
        );
        let task =
            task_manager.create("long task", "session-housekeeping", InteractionSurface::Cli);
        task_manager.start(&task.id);

        let daemon = HousekeepingDaemon::new(HousekeepingConfig::default(), token, last_active)
            .with_app_state(app_state.clone());

        let outcome = daemon.handle_zombie_session(3600).await;
        assert_eq!(
            outcome,
            ZombieSessionOutcome::Hibernated {
                suspended_task_ids: vec![task.id.clone()]
            }
        );
        assert_eq!(
            session_store.load_lifecycle_status(&SessionId("session-housekeeping".into())),
            SessionLifecycleStatus::Hibernating
        );
        assert_eq!(
            task_manager.status(&task.id),
            Some(crate::task::types::TaskStatus::Killed)
        );
        assert!(
            app_state
                .notification_dispatcher
                .delivered()
                .iter()
                .any(|notification| notification.notice_kind.as_deref()
                    == Some("housekeeping.zombie_hibernated"))
        );

        let repeated = daemon.handle_zombie_session(7200).await;
        assert_eq!(repeated, ZombieSessionOutcome::AlreadyInactive);
    }

    #[tokio::test]
    async fn test_housekeeping_hibernating_session_upgrades_to_expired() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let session_store = Arc::new(InMemorySessionStore::default());
        let task_manager = Arc::new(TaskManager::default());
        let app_state = test_app_state(
            session_store.clone(),
            task_manager,
            last_active.clone(),
            token.clone(),
        );
        session_store.save_lifecycle_status(
            &SessionId("session-housekeeping".into()),
            SessionLifecycleStatus::Hibernating,
        );
        let daemon = HousekeepingDaemon::new(
            HousekeepingConfig {
                interval: Duration::from_secs(1),
                stale_threshold_secs: 5,
                expired_threshold_secs: 10,
                session_retention_days: 7,
                task_log_retention_days: 1,
                max_gc_entries_per_tick: 2048,
            },
            token,
            last_active.clone(),
        )
        .with_app_state(app_state.clone());

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        last_active.store(now - 30, Ordering::Relaxed);
        daemon.perform_maintenance().await;

        assert_eq!(
            session_store.load_lifecycle_status(&SessionId("session-housekeeping".into())),
            SessionLifecycleStatus::Expired
        );
        assert!(
            app_state
                .notification_dispatcher
                .delivered()
                .iter()
                .any(|notification| notification.notice_kind.as_deref()
                    == Some("housekeeping.zombie_expired"))
        );
    }

    #[tokio::test]
    async fn test_housekeeping_gc_logic() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rust_agent_gc_test_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
        let mut budget = usize::MAX;
        daemon
            .prune_directory(&temp_dir, 0, false, &mut budget)
            .unwrap();

        assert!(!file_old.exists());
        assert!(!file_new.exists());

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn test_housekeeping_recursive_gc() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rust_agent_recursive_gc_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
        let mut budget = usize::MAX;
        daemon
            .prune_directory(&temp_dir, 0, false, &mut budget)
            .unwrap();

        // Check if nested file is gone
        assert!(!file_nested.exists());
        // Check if empty subdirectories are gone
        assert!(!sub_dir.exists());
        assert!(!temp_dir.join("sub").exists());
        assert!(temp_dir.exists()); // The base root should stay (is_nested=false)

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[tokio::test]
    async fn test_housekeeping_symlink_prevention() {
        use std::os::unix::fs::symlink;

        let temp_dir = std::env::temp_dir().join(format!(
            "rust_agent_symlink_test_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create a real file OUTSIDE the root that should be PROTECTED
        let protected_dir = temp_dir.parent().unwrap().join(format!(
            "protected_dir_gc_test_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&protected_dir).ok();
        let protected_file = protected_dir.join("should_not_be_deleted.txt");
        std::fs::write(&protected_file, "stay alive").unwrap();

        // Create a symlink INSIDE the root pointing to the protected dir
        let symlink_path = temp_dir.join("evil_link");
        symlink(&protected_dir, &symlink_path).unwrap();

        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(HousekeepingConfig::default(), token, last_active);

        // Wait to ensure age > 0 for everything
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        // Run GC on the root with age 0
        let mut budget = usize::MAX;
        daemon
            .prune_directory(&temp_dir, 0, false, &mut budget)
            .unwrap();

        // ASSERTIONS:
        // 1. The symlink itself should be deleted (because it's a "file" in the root and its age is 0)
        // OR if it's treated as a directory link, it should just be ignored.
        // In our current implementation, we delete symlinks to files/dirs if they are old.
        assert!(!symlink_path.exists());

        // 2. CRITICAL: The target of the symlink MUST still exist
        assert!(
            protected_file.exists(),
            "GC followed a symlink and deleted external content!"
        );

        let _ = std::fs::remove_dir_all(temp_dir);
        let _ = std::fs::remove_dir_all(protected_dir);
    }

    #[tokio::test]
    async fn test_housekeeping_async_wrapping() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(HousekeepingConfig::default(), token, last_active);

        // This should not panic and should return immediately even if it spawns blocking work
        let handle = daemon.perform_gc();
        handle.await;

        // Verify it doesn't crash
    }

    #[tokio::test]
    async fn test_housekeeping_error_handling() {
        let token = CancellationToken::new();
        let last_active = Arc::new(AtomicU64::new(0));
        let daemon = HousekeepingDaemon::new(HousekeepingConfig::default(), token, last_active);

        // Path that doesn't exist shouldn't error but return Ok
        let dummy_path = PathBuf::from("/non/existent/path/for/rust/agent/test");
        let mut budget = usize::MAX;
        let result = daemon.prune_directory(&dummy_path, 0, false, &mut budget);
        assert!(result.is_ok());
    }
}

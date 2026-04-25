use crate::state::app_state::WorkerRole;
use crate::task::manager::TaskManager;
use crate::task::types::{TaskRecord, TaskStatus};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info};

const MAX_SUBAGENTS: usize = 8;
const MIN_SUBAGENTS: usize = 3;
const DEFAULT_SUBAGENTS: usize = 6;
const BOSS_ACTIVE_CAP: usize = 6;
const BOSS_IMPLEMENT_CAP: usize = 3;
const BOSS_RESEARCH_CAP: usize = 2;
const BOSS_VERIFY_CAP: usize = 2;
const BOSS_LINEAGE_DEPTH_CAP: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressureLevel {
    Normal,
    Warning,
    Critical,
}

impl MemoryPressureLevel {
    pub fn from_raw_pressure(pressure: u8) -> Self {
        if pressure >= 4 {
            Self::Critical
        } else if pressure >= 2 {
            Self::Warning
        } else {
            Self::Normal
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BossBudgetDecision {
    Allow,
    Queue { reason: String },
    Deny { reason: String },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BossBudgetSnapshot {
    pub active_total: usize,
    pub active_implement: usize,
    pub active_research: usize,
    pub active_verify: usize,
}

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

pub fn current_memory_pressure_level() -> MemoryPressureLevel {
    MemoryPressureLevel::from_raw_pressure(get_macos_memory_pressure())
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

fn is_active_boss_task(task: &TaskRecord) -> bool {
    matches!(task.status, TaskStatus::Pending | TaskStatus::Running) && task.boss_actor_id.is_some()
}

pub fn boss_budget_snapshot(tasks: &TaskManager) -> BossBudgetSnapshot {
    let mut snapshot = BossBudgetSnapshot::default();
    for task in tasks.list().into_iter().filter(is_active_boss_task) {
        snapshot.active_total += 1;
        match task.worker_role {
            Some(WorkerRole::Implement) => snapshot.active_implement += 1,
            Some(WorkerRole::Research) => snapshot.active_research += 1,
            Some(WorkerRole::Verify) => snapshot.active_verify += 1,
            None => {}
        }
    }
    snapshot
}

pub fn evaluate_boss_budget(
    tasks: &TaskManager,
    role: WorkerRole,
    lineage_depth: u32,
    pressure: MemoryPressureLevel,
) -> BossBudgetDecision {
    if lineage_depth > BOSS_LINEAGE_DEPTH_CAP {
        return BossBudgetDecision::Deny {
            reason: format!(
                "boss budget denied: lineage depth {} exceeds cap {}",
                lineage_depth, BOSS_LINEAGE_DEPTH_CAP
            ),
        };
    }

    if pressure == MemoryPressureLevel::Critical && matches!(role, WorkerRole::Research) {
        return BossBudgetDecision::Deny {
            reason: "boss budget denied: critical memory pressure blocks low-priority research children"
                .into(),
        };
    }

    if pressure == MemoryPressureLevel::Critical && matches!(role, WorkerRole::Verify) {
        return BossBudgetDecision::Queue {
            reason: "boss budget queued: critical memory pressure preserves implement capacity before verify children"
                .into(),
        };
    }

    let snapshot = boss_budget_snapshot(tasks);
    if snapshot.active_total >= BOSS_ACTIVE_CAP {
        return BossBudgetDecision::Queue {
            reason: format!(
                "boss budget queued: active boss tasks {} reached total cap {}",
                snapshot.active_total, BOSS_ACTIVE_CAP
            ),
        };
    }

    let (active_for_role, role_cap) = match role {
        WorkerRole::Implement => (snapshot.active_implement, BOSS_IMPLEMENT_CAP),
        WorkerRole::Research => (snapshot.active_research, BOSS_RESEARCH_CAP),
        WorkerRole::Verify => (snapshot.active_verify, BOSS_VERIFY_CAP),
    };
    if active_for_role >= role_cap {
        return BossBudgetDecision::Queue {
            reason: format!(
                "boss budget queued: active {} tasks {} reached role cap {}",
                role.as_str(),
                active_for_role,
                role_cap
            ),
        };
    }

    if pressure == MemoryPressureLevel::Warning && matches!(role, WorkerRole::Research | WorkerRole::Verify)
    {
        return BossBudgetDecision::Queue {
            reason: format!(
                "boss budget queued: warning memory pressure deprioritizes {} children",
                role.as_str()
            ),
        };
    }

    BossBudgetDecision::Allow
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

    #[test]
    fn subagent_limiter_enforces_total_and_role_caps_under_memory_pressure() {
        let manager = TaskManager::default();
        for index in 0..2 {
            let task = manager.create_with_type(
                format!("research-{index}"),
                crate::task::types::TaskType::LocalAgent,
                "boss-session",
                crate::bootstrap::InteractionSurface::Cli,
            );
            manager.set_worker_role(&task.id, WorkerRole::Research);
            manager.set_boss_actor_id(&task.id, Some(format!("implement_child:depth={index}")));
        }

        match evaluate_boss_budget(&manager, WorkerRole::Research, 1, MemoryPressureLevel::Normal) {
            BossBudgetDecision::Queue { reason } => {
                assert!(reason.contains("role cap"));
            }
            other => panic!("expected Queue due to role cap, got {other:?}"),
        }

        for index in 0..4 {
            let task = manager.create_with_type(
                format!("implement-{index}"),
                crate::task::types::TaskType::LocalAgent,
                "boss-session",
                crate::bootstrap::InteractionSurface::Cli,
            );
            manager.set_worker_role(&task.id, WorkerRole::Implement);
            manager.set_boss_actor_id(&task.id, Some(format!("implement_child:depth={index}")));
        }

        match evaluate_boss_budget(&manager, WorkerRole::Implement, 1, MemoryPressureLevel::Normal) {
            BossBudgetDecision::Queue { reason } => {
                assert!(reason.contains("total cap"));
            }
            other => panic!("expected Queue due to total cap, got {other:?}"),
        }
    }

    #[test]
    fn boss_budget_blocks_low_priority_children_when_pressure_is_critical() {
        let manager = TaskManager::default();

        match evaluate_boss_budget(&manager, WorkerRole::Research, 1, MemoryPressureLevel::Critical) {
            BossBudgetDecision::Deny { reason } => {
                assert!(reason.contains("critical memory pressure"));
            }
            other => panic!("expected Deny for research under critical pressure, got {other:?}"),
        }

        match evaluate_boss_budget(&manager, WorkerRole::Verify, 1, MemoryPressureLevel::Critical) {
            BossBudgetDecision::Queue { reason } => {
                assert!(reason.contains("preserves implement capacity"));
            }
            other => panic!("expected Queue for verify under critical pressure, got {other:?}"),
        }

        assert_eq!(
            evaluate_boss_budget(&manager, WorkerRole::Implement, 1, MemoryPressureLevel::Critical),
            BossBudgetDecision::Allow
        );
        assert!(matches!(
            evaluate_boss_budget(&manager, WorkerRole::Implement, 2, MemoryPressureLevel::Normal),
            BossBudgetDecision::Deny { .. }
        ));
    }

}

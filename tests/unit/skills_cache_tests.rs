use std::fs;
use std::time::Duration;

use rust_agent::skills::cache::{
    SkillCachePolicy, SkillInvalidationReason, SkillRuntimeCache,
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_skill_dir(dir: &tempfile::TempDir, name: &str, content: &str) {
    let skill_dir = dir.path().join(".claude").join("skills").join(name);
    fs::create_dir_all(&skill_dir).unwrap();
    fs::write(
        skill_dir.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: test skill\n---\n{content}"),
    )
    .unwrap();
}

// ── SkillCachePolicy ──────────────────────────────────────────────────────────

#[test]
fn r4_1_policy_no_ttl_never_expires() {
    let policy = SkillCachePolicy::no_ttl();
    assert!(policy.ttl.is_none());
}

#[test]
fn r4_1_policy_with_ttl_sets_duration() {
    let policy = SkillCachePolicy::with_ttl(300);
    assert_eq!(policy.ttl, Some(Duration::from_secs(300)));
}

// ── SkillRuntimeCache initial load ───────────────────────────────────────────

#[test]
fn r4_1_initial_load_generation_is_1() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "alpha", "alpha content");

    let mut cache = SkillRuntimeCache::default();
    assert_eq!(cache.generation(), 0, "generation should be 0 before first load");

    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();
    assert!(reloaded, "first load should be reported as reloaded");
    assert_eq!(entry.generation, 1);
    assert!(entry.invalidation_reason.is_none(), "initial load has no invalidation reason");
    assert!(!entry.result.skills.is_empty());
}

#[test]
fn r4_1_second_load_same_fingerprint_returns_cached() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "beta", "beta content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();
    assert!(!reloaded, "second load with same fingerprint should not reload");
    assert_eq!(entry.generation, 1, "generation should stay at 1");
}

// ── file change invalidation ──────────────────────────────────────────────────

#[test]
fn r4_1_file_change_triggers_reload_with_file_changed_reason() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "gamma", "v1 content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    // Modify the skill file to change the fingerprint
    let skill_file = dir
        .path()
        .join(".claude")
        .join("skills")
        .join("gamma")
        .join("SKILL.md");
    fs::write(
        &skill_file,
        "---\nname: gamma\ndescription: updated\n---\nv2 content",
    )
    .unwrap();

    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();
    assert!(reloaded, "file change should trigger reload");
    assert_eq!(entry.generation, 2);
    assert_eq!(
        entry.invalidation_reason,
        Some(SkillInvalidationReason::FileChanged)
    );
}

// ── explicit invalidation ─────────────────────────────────────────────────────

#[test]
fn r4_1_explicit_reload_reason_propagated() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "delta", "delta content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    cache.invalidate(SkillInvalidationReason::ExplicitReload);
    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();

    assert!(reloaded);
    assert_eq!(entry.generation, 2);
    assert_eq!(
        entry.invalidation_reason,
        Some(SkillInvalidationReason::ExplicitReload)
    );
}

#[test]
fn r4_1_config_changed_reason_propagated() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "epsilon", "epsilon content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    cache.invalidate(SkillInvalidationReason::ConfigChanged);
    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();

    assert!(reloaded);
    assert_eq!(
        entry.invalidation_reason,
        Some(SkillInvalidationReason::ConfigChanged)
    );
}

#[test]
fn r4_1_runtime_snapshot_switch_reason_propagated() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "zeta", "zeta content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    cache.invalidate(SkillInvalidationReason::RuntimeSnapshotSwitch);
    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();

    assert!(reloaded);
    assert_eq!(
        entry.invalidation_reason,
        Some(SkillInvalidationReason::RuntimeSnapshotSwitch)
    );
}

// ── TTL expiry ────────────────────────────────────────────────────────────────

#[test]
fn r4_1_ttl_expired_triggers_reload() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "eta", "eta content");

    // TTL of 0 seconds — expires immediately
    let mut cache = SkillRuntimeCache::new(SkillCachePolicy::with_ttl(0));
    cache.load_or_reload(dir.path()).unwrap();

    // Sleep 1ms to ensure elapsed > 0s TTL
    std::thread::sleep(Duration::from_millis(1));

    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();
    assert!(reloaded, "TTL=0 should expire immediately");
    assert_eq!(entry.generation, 2);
    assert_eq!(
        entry.invalidation_reason,
        Some(SkillInvalidationReason::TtlExpired)
    );
}

#[test]
fn r4_1_no_ttl_does_not_expire_without_file_change() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "theta", "theta content");

    let mut cache = SkillRuntimeCache::new(SkillCachePolicy::no_ttl());
    cache.load_or_reload(dir.path()).unwrap();

    std::thread::sleep(Duration::from_millis(1));

    let (entry, reloaded) = cache.load_or_reload(dir.path()).unwrap();
    assert!(!reloaded, "no-TTL cache should not expire without file change");
    assert_eq!(entry.generation, 1);
}

// ── generation counter ────────────────────────────────────────────────────────

#[test]
fn r4_1_generation_increments_on_each_reload() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "iota", "iota content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();
    assert_eq!(cache.generation(), 1);

    cache.invalidate(SkillInvalidationReason::ExplicitReload);
    cache.load_or_reload(dir.path()).unwrap();
    assert_eq!(cache.generation(), 2);

    cache.invalidate(SkillInvalidationReason::ConfigChanged);
    cache.load_or_reload(dir.path()).unwrap();
    assert_eq!(cache.generation(), 3);
}

// ── snapshot ──────────────────────────────────────────────────────────────────

#[test]
fn r4_1_snapshot_returns_none_before_first_load() {
    let cache = SkillRuntimeCache::default();
    assert!(cache.snapshot().is_none());
}

#[test]
fn r4_1_snapshot_returns_entry_after_load() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "kappa", "kappa content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.generation, 1);
    assert!(!snapshot.result.skills.is_empty());
}

// ── last_invalidation_reason ──────────────────────────────────────────────────

#[test]
fn r4_1_last_invalidation_reason_none_on_initial_load() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "lambda", "lambda content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    assert!(cache.last_invalidation_reason().is_none());
}

#[test]
fn r4_1_last_invalidation_reason_reflects_most_recent_reload() {
    let dir = tempfile::tempdir().unwrap();
    make_skill_dir(&dir, "mu", "mu content");

    let mut cache = SkillRuntimeCache::default();
    cache.load_or_reload(dir.path()).unwrap();

    cache.invalidate(SkillInvalidationReason::RuntimeSnapshotSwitch);
    cache.load_or_reload(dir.path()).unwrap();

    assert_eq!(
        cache.last_invalidation_reason(),
        Some(SkillInvalidationReason::RuntimeSnapshotSwitch)
    );
}

// ── SkillInvalidationReason::as_str ──────────────────────────────────────────

#[test]
fn r4_1_invalidation_reason_as_str_values() {
    assert_eq!(SkillInvalidationReason::FileChanged.as_str(), "file_changed");
    assert_eq!(SkillInvalidationReason::ConfigChanged.as_str(), "config_changed");
    assert_eq!(
        SkillInvalidationReason::RuntimeSnapshotSwitch.as_str(),
        "runtime_snapshot_switch"
    );
    assert_eq!(SkillInvalidationReason::ExplicitReload.as_str(), "explicit_reload");
    assert_eq!(SkillInvalidationReason::TtlExpired.as_str(), "ttl_expired");
}

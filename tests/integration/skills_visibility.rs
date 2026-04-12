use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::skills::loader::{SkillLoaderCache, load_skills_with_diagnostics};
use rust_agent::skills::registry::SkillRegistry;

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn skill_visibility_changes_with_cwd_and_cache_invalidation() {
    let root = unique_temp_path("rust-agent-skill-visibility");
    let project_a = root.join("project-a");
    let project_b = root.join("project-b");
    let skill_dir = project_a.join(".claude/skills/contextual");
    fs::create_dir_all(&skill_dir).expect("create skill dir");
    fs::create_dir_all(&project_b).expect("create second project dir");
    fs::write(project_a.join("Cargo.toml"), "[package]\nname='a'\n").expect("write cargo");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\ndescription: contextual skill\npaths: */project-a*\nrequires-files: Cargo.toml\n---\nbody\n",
    )
    .expect("write skill file");

    let result = load_skills_with_diagnostics(&project_a).expect("load skills");
    let registry = SkillRegistry::new(result.skills.clone());
    assert_eq!(registry.list_user_invocable(&project_a).len(), 1);
    assert!(registry.list_user_invocable(&project_b).is_empty());

    let mut cache = SkillLoaderCache::default();
    let (first, reloaded_first) = cache.load_or_reload(&project_a).expect("initial cache load");
    assert!(reloaded_first);
    let (_, reloaded_second) = cache.load_or_reload(&project_a).expect("cached load");
    assert!(!reloaded_second);

    fs::write(
        skill_dir.join("SKILL.md"),
        "---\ndescription: contextual skill updated\npaths: */project-a*\nrequires-files: Cargo.toml\n---\nbody updated\n",
    )
    .expect("rewrite skill file");

    let (second, reloaded_third) = cache.load_or_reload(&project_a).expect("reload after mutation");
    assert!(reloaded_third);
    assert_ne!(first.fingerprint, second.fingerprint);
    assert_eq!(second.skills[0].description, "contextual skill updated");

    fs::remove_dir_all(root).expect("cleanup skill visibility root");
}

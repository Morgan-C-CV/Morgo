use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_agent::skills::frontmatter::parse_frontmatter;
use rust_agent::skills::loader::{SkillLoaderCache, load_skills_with_diagnostics};
use rust_agent::skills::types::{SkillSource, SkillWorkflowExecution};

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[test]
fn frontmatter_parser_supports_richer_skill_metadata() {
    let markdown = r#"---
name: test-skill
description: richer skill
when_to_use: Use when validating skill metadata
argument-hint: target path
workflow-hint: inspect then update
workflow-execution: agent
allowed-tools: Read, Edit
aliases: tskill, test-s
user-invocable: true
disable-model-invocation: false
hidden: false
paths: */project/*
exclude-paths: */vendor/*
requires-files: Cargo.toml, src/main.rs
context: fork
---
Skill body
"#;

    let (frontmatter, content) = parse_frontmatter(markdown).expect("frontmatter should parse");
    assert_eq!(frontmatter.name.as_deref(), Some("test-skill"));
    assert_eq!(
        frontmatter.workflow_hint.as_deref(),
        Some("inspect then update")
    );
    assert_eq!(frontmatter.allowed_tools, vec!["Read", "Edit"]);
    assert_eq!(frontmatter.aliases, vec!["tskill", "test-s"]);
    assert_eq!(
        frontmatter.workflow_execution,
        SkillWorkflowExecution::Agent
    );
    assert_eq!(frontmatter.exclude_paths, vec!["*/vendor/*"]);
    assert_eq!(
        frontmatter.requires_files,
        vec!["Cargo.toml", "src/main.rs"]
    );
    assert_eq!(content.trim(), "Skill body");
}

#[test]
fn skill_loader_builds_normalized_workflow_summary() {
    let root = unique_temp_path("rust-agent-skill-workflow-summary");
    let skill_dir = root.join(".claude/skills/workflow-skill");
    fs::create_dir_all(&skill_dir).expect("create skill dir");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\ndescription: workflow skill\nwhen_to_use:   Use when inspecting state   \nargument-hint:   path target   \nworkflow-hint:   inspect then patch   \n---\nbody\n",
    )
    .expect("write workflow skill");

    let result = load_skills_with_diagnostics(&root).expect("skill load should succeed");
    let skill = &result.skills[0];
    assert_eq!(
        skill.when_to_use.as_deref(),
        Some("Use when inspecting state")
    );
    assert_eq!(skill.argument_hint.as_deref(), Some("path target"));
    assert_eq!(skill.workflow_hint.as_deref(), Some("inspect then patch"));
    assert_eq!(
        skill.workflow_summary.as_deref(),
        Some("inspect then patch | args: path target | use: Use when inspecting state")
    );

    fs::remove_dir_all(root).expect("cleanup workflow summary root");
}

#[test]
fn skill_loader_merges_user_and_project_sources_with_project_override() {
    let root = unique_temp_path("rust-agent-skill-loader");
    let project_skill_dir = root.join(".claude/skills/project-skill");
    let home_root = unique_temp_path("rust-agent-skill-home");
    let user_skill_dir = home_root.join(".claude/skills/project-skill");
    fs::create_dir_all(&project_skill_dir).expect("create project skill dir");
    fs::create_dir_all(&user_skill_dir).expect("create user skill dir");
    fs::write(root.join("Cargo.toml"), "[package]\nname='demo'\n").expect("write cargo file");

    fs::write(
        user_skill_dir.join("SKILL.md"),
        "---\ndescription: user copy\naliases: user-alias\n---\nuser body\n",
    )
    .expect("write user skill");
    fs::write(
        project_skill_dir.join("SKILL.md"),
        "---\ndescription: project copy\nworkflow-hint: local workflow\nrequires-files: Cargo.toml\n---\nproject body\n",
    )
    .expect("write project skill");

    let original_home = std::env::var("HOME").ok();
    unsafe {
        std::env::set_var("HOME", &home_root);
    }
    let result = load_skills_with_diagnostics(&root).expect("skill load should succeed");
    if let Some(home) = original_home {
        unsafe {
            std::env::set_var("HOME", home);
        }
    }

    assert!(result.diagnostics.is_empty());
    assert_eq!(result.skills.len(), 1);
    let skill = &result.skills[0];
    assert_eq!(skill.name, "project-skill");
    assert_eq!(skill.description, "project copy");
    assert_eq!(skill.workflow_hint.as_deref(), Some("local workflow"));
    assert_eq!(skill.workflow_summary.as_deref(), Some("local workflow"));
    assert_eq!(skill.workflow_execution, SkillWorkflowExecution::PromptOnly);
    assert_eq!(skill.source, SkillSource::Filesystem);
    assert_eq!(skill.content, "project body");

    fs::remove_dir_all(root).expect("cleanup project root");
    fs::remove_dir_all(home_root).expect("cleanup home root");
}

#[test]
fn skill_loader_cache_reloads_only_after_fingerprint_changes() {
    let root = unique_temp_path("rust-agent-skill-cache");
    let skill_dir = root.join(".claude/skills/cacheable");
    fs::create_dir_all(&skill_dir).expect("create cache skill dir");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\ndescription: cache skill\n---\nbody\n",
    )
    .expect("write skill file");

    let mut cache = SkillLoaderCache::default();
    let (first, reloaded_first) = cache.load_or_reload(&root).expect("first cache load");
    assert!(reloaded_first);
    assert_eq!(
        first.skills[0].workflow_execution,
        SkillWorkflowExecution::PromptOnly
    );
    let (second, reloaded_second) = cache.load_or_reload(&root).expect("second cache load");
    assert!(!reloaded_second);
    assert_eq!(first.fingerprint, second.fingerprint);

    fs::write(
        skill_dir.join("SKILL.md"),
        "---\ndescription: cache skill updated\nworkflow-execution: agent\n---\nbody\n",
    )
    .expect("rewrite skill file");
    let (third, reloaded_third) = cache.load_or_reload(&root).expect("third cache load");
    assert!(reloaded_third);
    assert_ne!(second.fingerprint, third.fingerprint);
    assert_eq!(
        third.skills[0].workflow_execution,
        SkillWorkflowExecution::Agent
    );

    cache.invalidate();
    let (fourth, reloaded_after_invalidate) = cache
        .load_or_reload(&root)
        .expect("reload after invalidate");
    assert!(reloaded_after_invalidate);
    assert_eq!(
        fourth.skills[0].workflow_execution,
        SkillWorkflowExecution::Agent
    );

    fs::remove_dir_all(root).expect("cleanup cache root");
}

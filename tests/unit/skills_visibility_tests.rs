use std::path::{Path, PathBuf};

use rust_agent::skills::types::{
    SkillDefinition, SkillExecutionContext, SkillSource, SkillWorkflowExecution,
};
use rust_agent::skills::visibility::{
    resolve_skill_visibility, SkillActivationDecision, SkillConflictRecord, SkillPrecedence,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_skill(name: &str, source: SkillSource, file_path: Option<PathBuf>) -> SkillDefinition {
    SkillDefinition {
        name: name.to_string(),
        description: format!("{name} description"),
        when_to_use: None,
        argument_hint: None,
        workflow_hint: None,
        workflow_summary: None,
        allowed_tools: vec![],
        aliases: vec![],
        workflow_execution: SkillWorkflowExecution::PromptOnly,
        user_invocable: true,
        disable_model_invocation: false,
        hidden: false,
        paths: vec![],
        exclude_paths: vec![],
        requires_files: vec![],
        context: SkillExecutionContext::Inline,
        content: String::new(),
        source,
        file_path,
    }
}

fn make_hidden_skill(name: &str, source: SkillSource) -> SkillDefinition {
    let mut s = make_skill(name, source, None);
    s.hidden = true;
    s
}

fn active_names(result: &rust_agent::skills::visibility::SkillVisibilityResult) -> Vec<&str> {
    result
        .active_skills()
        .into_iter()
        .map(|s| s.name.as_str())
        .collect()
}

// ── SkillPrecedence ordering ──────────────────────────────────────────────────

#[test]
fn r4_2_filesystem_beats_user_beats_bundled() {
    let cwd = Path::new("/project/app");

    let bundled = make_skill("s", SkillSource::Bundled, None);
    let user = make_skill("s", SkillSource::User, None);
    let fs = make_skill(
        "s",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/app/.claude/skills/s/SKILL.md")),
    );

    let p_bundled = SkillPrecedence::for_skill(&bundled, cwd);
    let p_user = SkillPrecedence::for_skill(&user, cwd);
    let p_fs = SkillPrecedence::for_skill(&fs, cwd);

    assert!(p_fs > p_user, "Filesystem must beat User");
    assert!(p_user > p_bundled, "User must beat Bundled");
}

#[test]
fn r4_2_deeper_filesystem_scope_beats_shallower() {
    let cwd = Path::new("/project/app/sub");

    let shallow = make_skill(
        "s",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/.claude/skills/s/SKILL.md")),
    );
    let deep = make_skill(
        "s",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/app/.claude/skills/s/SKILL.md")),
    );

    let p_shallow = SkillPrecedence::for_skill(&shallow, cwd);
    let p_deep = SkillPrecedence::for_skill(&deep, cwd);

    assert!(p_deep > p_shallow, "deeper scope must beat shallower");
}

// ── no-conflict cases ─────────────────────────────────────────────────────────

#[test]
fn r4_2_single_skill_is_active() {
    let cwd = Path::new("/project");
    let skills = vec![make_skill("alpha", SkillSource::Bundled, None)];
    let result = resolve_skill_visibility(skills, cwd);

    assert_eq!(active_names(&result), vec!["alpha"]);
    assert!(result.conflicts.is_empty());
}

#[test]
fn r4_2_distinct_names_all_active() {
    let cwd = Path::new("/project");
    let skills = vec![
        make_skill("alpha", SkillSource::Bundled, None),
        make_skill("beta", SkillSource::User, None),
        make_skill(
            "gamma",
            SkillSource::Filesystem,
            Some(PathBuf::from("/project/.claude/skills/gamma/SKILL.md")),
        ),
    ];
    let result = resolve_skill_visibility(skills, cwd);

    let mut names = active_names(&result);
    names.sort();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    assert!(result.conflicts.is_empty());
}

// ── name collision / shadowing ────────────────────────────────────────────────

#[test]
fn r4_2_filesystem_shadows_bundled_on_name_collision() {
    let cwd = Path::new("/project");
    let bundled = make_skill("deploy", SkillSource::Bundled, None);
    let fs = make_skill(
        "deploy",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/.claude/skills/deploy/SKILL.md")),
    );

    let result = resolve_skill_visibility(vec![bundled, fs], cwd);

    let active = result.active_skills();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].source, SkillSource::Filesystem);

    assert_eq!(result.conflicts.len(), 1);
    let conflict = &result.conflicts[0];
    assert_eq!(conflict.name, "deploy");
    assert_eq!(conflict.winner_source, SkillSource::Filesystem);
    assert!(conflict.shadowed_sources.contains(&SkillSource::Bundled));
}

#[test]
fn r4_2_user_shadows_bundled_on_name_collision() {
    let cwd = Path::new("/project");
    let bundled = make_skill("review", SkillSource::Bundled, None);
    let user = make_skill("review", SkillSource::User, None);

    let result = resolve_skill_visibility(vec![bundled, user], cwd);

    let active = result.active_skills();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].source, SkillSource::User);

    assert_eq!(result.conflicts.len(), 1);
    assert_eq!(result.conflicts[0].winner_source, SkillSource::User);
}

#[test]
fn r4_2_deeper_filesystem_shadows_shallower_on_name_collision() {
    let cwd = Path::new("/project/app/sub");

    let shallow = make_skill(
        "build",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/.claude/skills/build/SKILL.md")),
    );
    let deep = make_skill(
        "build",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/app/.claude/skills/build/SKILL.md")),
    );

    let result = resolve_skill_visibility(vec![shallow, deep], cwd);

    let active = result.active_skills();
    assert_eq!(active.len(), 1);
    // deeper scope wins — file_path contains /project/app/
    assert!(active[0]
        .file_path
        .as_ref()
        .unwrap()
        .to_string_lossy()
        .contains("/project/app/"));

    assert_eq!(result.conflicts.len(), 1);
}

#[test]
fn r4_2_shadowed_decision_recorded_in_decisions() {
    let cwd = Path::new("/project");
    let bundled = make_skill("test", SkillSource::Bundled, None);
    let user = make_skill("test", SkillSource::User, None);

    let result = resolve_skill_visibility(vec![bundled, user], cwd);

    let shadowed: Vec<_> = result
        .decisions
        .iter()
        .filter(|d| matches!(d, SkillActivationDecision::Shadowed { .. }))
        .collect();
    assert_eq!(shadowed.len(), 1);
    if let SkillActivationDecision::Shadowed { skill, winner_source } = &shadowed[0] {
        assert_eq!(skill.source, SkillSource::Bundled);
        assert_eq!(*winner_source, SkillSource::User);
    }
}

// ── explicit disable (hidden = true) ─────────────────────────────────────────

#[test]
fn r4_2_hidden_skill_is_disabled_not_active() {
    let cwd = Path::new("/project");
    let hidden = make_hidden_skill("secret", SkillSource::Bundled);

    let result = resolve_skill_visibility(vec![hidden], cwd);

    assert!(result.active_skills().is_empty());
    assert_eq!(result.decisions.len(), 1);
    assert!(matches!(
        result.decisions[0],
        SkillActivationDecision::Disabled(_)
    ));
}

#[test]
fn r4_2_hidden_skill_does_not_shadow_active_skill_with_same_name() {
    let cwd = Path::new("/project");
    let hidden = make_hidden_skill("tool", SkillSource::Filesystem);
    let active = make_skill("tool", SkillSource::Bundled, None);

    let result = resolve_skill_visibility(vec![hidden, active], cwd);

    // The hidden one is Disabled; the bundled one should still be Active
    let active_skills = result.active_skills();
    assert_eq!(active_skills.len(), 1);
    assert_eq!(active_skills[0].source, SkillSource::Bundled);

    // No conflict — hidden doesn't participate in conflict resolution
    assert!(result.conflicts.is_empty());
}

#[test]
fn r4_2_all_hidden_same_name_leaves_no_active() {
    let cwd = Path::new("/project");
    let h1 = make_hidden_skill("x", SkillSource::Bundled);
    let h2 = make_hidden_skill("x", SkillSource::User);

    let result = resolve_skill_visibility(vec![h1, h2], cwd);

    assert!(result.active_skills().is_empty());
    assert!(result.conflicts.is_empty());
    assert_eq!(result.decisions.len(), 2);
    assert!(result
        .decisions
        .iter()
        .all(|d| matches!(d, SkillActivationDecision::Disabled(_))));
}

// ── SkillVisibilityResult helpers ─────────────────────────────────────────────

#[test]
fn r4_2_user_invocable_skills_filters_by_cwd() {
    let cwd = Path::new("/project/app");

    let mut in_scope = make_skill("in", SkillSource::Bundled, None);
    in_scope.user_invocable = true;

    let mut out_of_scope = make_skill("out", SkillSource::Bundled, None);
    out_of_scope.user_invocable = true;
    out_of_scope.paths = vec!["/other/*".to_string()];

    let result = resolve_skill_visibility(vec![in_scope, out_of_scope], cwd);

    let visible = result.user_invocable_skills(cwd);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].name, "in");
}

#[test]
fn r4_2_model_invocable_skills_excludes_disabled_model_invocation() {
    let cwd = Path::new("/project");

    let mut no_model = make_skill("no_model", SkillSource::Bundled, None);
    no_model.disable_model_invocation = true;

    let ok = make_skill("ok", SkillSource::Bundled, None);

    let result = resolve_skill_visibility(vec![no_model, ok], cwd);

    let invocable = result.model_invocable_skills(cwd);
    assert_eq!(invocable.len(), 1);
    assert_eq!(invocable[0].name, "ok");
}

// ── conflict record completeness ──────────────────────────────────────────────

#[test]
fn r4_2_conflict_record_lists_all_shadowed_sources() {
    let cwd = Path::new("/project");

    let bundled = make_skill("cmd", SkillSource::Bundled, None);
    let user = make_skill("cmd", SkillSource::User, None);
    let fs = make_skill(
        "cmd",
        SkillSource::Filesystem,
        Some(PathBuf::from("/project/.claude/skills/cmd/SKILL.md")),
    );

    let result = resolve_skill_visibility(vec![bundled, user, fs], cwd);

    assert_eq!(result.conflicts.len(), 1);
    let record: &SkillConflictRecord = &result.conflicts[0];
    assert_eq!(record.winner_source, SkillSource::Filesystem);
    assert_eq!(record.shadowed_sources.len(), 2);
    assert!(record.shadowed_sources.contains(&SkillSource::Bundled));
    assert!(record.shadowed_sources.contains(&SkillSource::User));
}

// ── is_active / skill() accessors ────────────────────────────────────────────

#[test]
fn r4_2_is_active_returns_true_only_for_active_decision() {
    let cwd = Path::new("/project");
    let bundled = make_skill("x", SkillSource::Bundled, None);
    let user = make_skill("x", SkillSource::User, None);
    let hidden = make_hidden_skill("y", SkillSource::Bundled);

    let result = resolve_skill_visibility(vec![bundled, user, hidden], cwd);

    for d in &result.decisions {
        match d {
            SkillActivationDecision::Active(_) => assert!(d.is_active()),
            SkillActivationDecision::Shadowed { .. } => assert!(!d.is_active()),
            SkillActivationDecision::Disabled(_) => assert!(!d.is_active()),
        }
    }
}

#[test]
fn r4_2_skill_accessor_returns_inner_definition() {
    let cwd = Path::new("/project");
    let s = make_skill("alpha", SkillSource::Bundled, None);
    let result = resolve_skill_visibility(vec![s], cwd);

    assert_eq!(result.decisions[0].skill().name, "alpha");
}

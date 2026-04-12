use std::path::Path;

use rust_agent::skills::registry::SkillRegistry;
use rust_agent::skills::types::{SkillDefinition, SkillExecutionContext, SkillSource};

fn sample_skill(name: &str) -> SkillDefinition {
    SkillDefinition {
        name: name.into(),
        description: format!("{name} description"),
        when_to_use: Some(format!("Use when {name} is relevant")),
        argument_hint: None,
        workflow_hint: None,
        allowed_tools: vec!["Read".into()],
        aliases: Vec::new(),
        user_invocable: true,
        disable_model_invocation: false,
        hidden: false,
        paths: Vec::new(),
        exclude_paths: Vec::new(),
        requires_files: Vec::new(),
        context: SkillExecutionContext::Inline,
        content: format!("instructions for {name}"),
        source: SkillSource::Filesystem,
        file_path: None,
    }
}

#[test]
fn registry_filters_visibility_by_path_hidden_and_required_files() {
    let mut visible = sample_skill("visible");
    visible.paths = vec!["*/demo/project*".into()];
    visible.requires_files = vec!["Cargo.toml".into()];

    let mut hidden = sample_skill("hidden");
    hidden.hidden = true;

    let mut excluded = sample_skill("excluded");
    excluded.exclude_paths = vec!["*/vendor/*".into()];

    let registry = SkillRegistry::new(vec![visible, hidden, excluded]);
    let cwd = Path::new("/tmp/demo/project");

    std::fs::create_dir_all(cwd).expect("create cwd");
    std::fs::write(cwd.join("Cargo.toml"), "[package]\nname='demo'\n").expect("write cargo");

    let user_visible = registry.list_user_invocable(cwd);
    assert_eq!(user_visible.len(), 2);
    assert!(user_visible.iter().any(|skill| skill.name == "visible"));
    assert!(user_visible.iter().any(|skill| skill.name == "excluded"));
    assert!(!user_visible.iter().any(|skill| skill.name == "hidden"));

    std::fs::remove_dir_all(cwd).expect("cleanup cwd");
}

#[test]
fn registry_resolves_aliases_and_model_visibility() {
    let mut skill = sample_skill("analyze");
    skill.aliases = vec!["scan".into()];
    let mut model_hidden = sample_skill("manual-only");
    model_hidden.disable_model_invocation = true;

    let registry = SkillRegistry::new(vec![skill, model_hidden]);
    let cwd = Path::new("/tmp/skills-alias");
    std::fs::create_dir_all(cwd).expect("create cwd");

    assert_eq!(registry.find("scan").expect("alias should resolve").name, "analyze");
    let model_visible = registry.list_model_invocable(cwd);
    assert_eq!(model_visible.len(), 1);
    assert_eq!(model_visible[0].name, "analyze");

    std::fs::remove_dir_all(cwd).expect("cleanup cwd");
}

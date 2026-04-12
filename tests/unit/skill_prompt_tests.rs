use rust_agent::skills::types::{SkillDefinition, SkillExecutionContext, SkillSource};
use rust_agent::tool::builtin::skill::format_skill_prompt;

fn sample_skill() -> SkillDefinition {
    SkillDefinition {
        name: "summarize-skill".into(),
        description: "Summarize repository state".into(),
        when_to_use: Some("Use when triaging repo state".into()),
        argument_hint: Some("target path".into()),
        workflow_hint: Some("raw workflow hint".into()),
        workflow_summary: Some("inspect then summarize | args: target path | use: Use when triaging repo state".into()),
        allowed_tools: vec!["Read".into(), "Glob".into()],
        aliases: vec![],
        user_invocable: true,
        disable_model_invocation: false,
        hidden: false,
        paths: vec![],
        exclude_paths: vec![],
        requires_files: vec![],
        context: SkillExecutionContext::Inline,
        content: "skill body".into(),
        source: SkillSource::Filesystem,
        file_path: None,
    }
}

#[test]
fn format_skill_prompt_uses_augmented_workflow_summary() {
    let prompt = format_skill_prompt(&sample_skill(), "src").expect("skill prompt should render");

    assert!(prompt.contains("Loaded skill: summarize-skill"));
    assert!(prompt.contains("When to use: Use when triaging repo state"));
    assert!(prompt.contains("Argument hint: target path"));
    assert!(prompt.contains(
        "Workflow hint: inspect then summarize | args: target path | use: Use when triaging repo state"
    ));
    assert!(prompt.contains("Allowed tools: Read, Glob"));
    assert!(prompt.contains("Arguments: src"));
    assert!(prompt.contains("Skill instructions:\nskill body"));
    assert!(!prompt.contains("Workflow hint: raw workflow hint"));
}

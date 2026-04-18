use rust_agent::skills::types::{
    SkillDefinition, SkillExecutionContext, SkillSource, SkillWorkflowExecution,
};
use rust_agent::tool::builtin::skill::{format_skill_invocation, format_skill_prompt};

fn sample_skill() -> SkillDefinition {
    SkillDefinition {
        name: "summarize-skill".into(),
        description: "Summarize repository state".into(),
        when_to_use: Some("Use when triaging repo state".into()),
        argument_hint: Some("target path".into()),
        workflow_hint: Some("raw workflow hint".into()),
        workflow_summary: Some(
            "inspect then summarize | args: target path | use: Use when triaging repo state".into(),
        ),
        allowed_tools: vec!["Read".into(), "Glob".into()],
        aliases: vec![],
        workflow_execution: SkillWorkflowExecution::PromptOnly,
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

fn agent_skill() -> SkillDefinition {
    let mut skill = sample_skill();
    skill.workflow_execution = SkillWorkflowExecution::Agent;
    skill
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

#[test]
fn format_skill_invocation_emits_agent_request_for_agent_workflows() {
    let prompt = format_skill_invocation(&agent_skill(), "src/lib.rs")
        .expect("agent skill invocation should render");

    assert!(prompt.contains("Skill workflow: agent"));
    assert!(prompt.contains("\"role\": \"research\""));
    assert!(prompt.contains("\"inherit_context\": true"));
    assert!(prompt.contains("\"reuse_strategy\": \"running_only\""));
    assert!(prompt.contains("\"allowed_tools\": ["));
    assert!(prompt.contains("\"Read\""));
    assert!(prompt.contains("\"Glob\""));
    assert!(prompt.contains("User arguments: src/lib.rs"));
    assert!(prompt.contains("skill body"));
    assert!(!prompt.contains("Loaded skill: summarize-skill"));
}

#[test]
fn format_skill_invocation_rejects_non_model_invocable_skills() {
    let mut skill = agent_skill();
    skill.disable_model_invocation = true;

    let error = format_skill_invocation(&skill, "src/lib.rs")
        .expect_err("disabled skill should be rejected");
    assert!(
        error
            .to_string()
            .contains("skill summarize-skill cannot be invoked by the model")
    );
}

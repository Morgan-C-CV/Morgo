use crate::skills::types::{SkillDefinition, SkillExecutionContext, SkillSource};

pub fn bundled_skills() -> Vec<SkillDefinition> {
    vec![SkillDefinition {
        name: "update-config".to_string(),
        description: "Configure settings.json behavior for Claude Code style automation.".to_string(),
        when_to_use: Some("Use when the user asks to configure persistent automation or settings behavior.".to_string()),
        argument_hint: None,
        workflow_hint: Some("Inspect current settings first, then make the smallest targeted change.".to_string()),
        workflow_summary: Some("Inspect current settings first, then make the smallest targeted change.".to_string()),
        allowed_tools: vec!["Edit".to_string(), "Read".to_string()],
        aliases: vec!["config-update".to_string()],
        user_invocable: true,
        disable_model_invocation: false,
        hidden: false,
        paths: Vec::new(),
        exclude_paths: Vec::new(),
        requires_files: Vec::new(),
        context: SkillExecutionContext::Inline,
        content: "Use the update-config flow to inspect and modify settings-backed automation behavior.".to_string(),
        source: SkillSource::Bundled,
        file_path: None,
    }]
}

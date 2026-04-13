use std::path::Path;

use crate::state::app_state::AppState;

pub fn describe_skills_context(app_state: &AppState) -> String {
    let Some(skill_registry) = app_state.skill_registry.as_ref() else {
        return String::new();
    };
    let cwd = app_state
        .session
        .as_ref()
        .map(|session| Path::new(session.cwd.as_str()))
        .unwrap_or_else(|| Path::new(""));
    let skills = skill_registry.list_model_invocable(cwd);
    if skills.is_empty() {
        return String::new();
    }

    let mut lines = vec!["Available skills:".to_string()];
    for skill in skills {
        let when = skill
            .when_to_use
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!(" — when to use: {}", value.trim()))
            .unwrap_or_default();
        let workflow = skill
            .workflow_summary
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!(" — workflow: {}", value.trim()))
            .unwrap_or_default();
        let source = format!(" [{}]", skill.source.as_str());
        lines.push(format!(
            "- {}{}: {}{}{}",
            skill.name, source, skill.description, when, workflow
        ));
    }
    lines.push("Invoke these via the Skill tool when appropriate.".to_string());
    lines.join("\n")
}

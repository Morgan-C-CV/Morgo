use crate::context::{git, memory, plan, skills, user_context};
use crate::state::app_state::AppState;

pub fn build_context_prompt(app_state: &AppState) -> String {
    let sections = ordered_context_sections(app_state)
        .into_iter()
        .filter(|section| !section.trim().is_empty())
        .collect::<Vec<_>>();

    if sections.is_empty() {
        return String::new();
    }

    let mut lines = vec!["Runtime context summary:".to_string()];
    lines.extend(sections);
    lines.join("\n\n")
}

fn ordered_context_sections(app_state: &AppState) -> [String; 5] {
    [
        git::describe_git_context(app_state),
        memory::describe_memory_context(app_state),
        user_context::describe_user_context(app_state),
        plan::describe_plan_context(app_state),
        skills::describe_skills_context(app_state),
    ]
}

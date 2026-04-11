use std::collections::BTreeMap;

use crate::skills::types::SkillDefinition;

#[derive(Debug, Clone, Default)]
pub struct SkillRegistry {
    skills: BTreeMap<String, SkillDefinition>,
}

impl SkillRegistry {
    pub fn new(skills: Vec<SkillDefinition>) -> Self {
        let mut map = BTreeMap::new();
        for skill in skills {
            map.insert(skill.name.clone(), skill);
        }
        Self { skills: map }
    }

    pub fn list(&self) -> Vec<SkillDefinition> {
        self.skills.values().cloned().collect()
    }

    pub fn list_user_invocable(&self, cwd: &str) -> Vec<SkillDefinition> {
        self.skills
            .values()
            .filter(|skill| skill.user_invocable && skill.matches_project_context(cwd))
            .cloned()
            .collect()
    }

    pub fn list_model_invocable(&self, cwd: &str) -> Vec<SkillDefinition> {
        self.skills
            .values()
            .filter(|skill| skill.is_model_invocable() && skill.matches_project_context(cwd))
            .cloned()
            .collect()
    }

    pub fn find(&self, name: &str) -> Option<SkillDefinition> {
        self.skills.get(name).cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.skills.is_empty()
    }
}

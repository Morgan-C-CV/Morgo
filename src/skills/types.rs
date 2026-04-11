use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillSource {
    Bundled,
    Filesystem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillExecutionContext {
    Inline,
    Fork,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillFrontmatter {
    pub name: Option<String>,
    pub description: Option<String>,
    pub when_to_use: Option<String>,
    pub argument_hint: Option<String>,
    pub allowed_tools: Vec<String>,
    pub user_invocable: bool,
    pub disable_model_invocation: bool,
    pub paths: Vec<String>,
    pub context: SkillExecutionContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDefinition {
    pub name: String,
    pub description: String,
    pub when_to_use: Option<String>,
    pub argument_hint: Option<String>,
    pub allowed_tools: Vec<String>,
    pub user_invocable: bool,
    pub disable_model_invocation: bool,
    pub paths: Vec<String>,
    pub context: SkillExecutionContext,
    pub content: String,
    pub source: SkillSource,
    pub file_path: Option<PathBuf>,
}

impl SkillDefinition {
    pub fn is_model_invocable(&self) -> bool {
        !self.disable_model_invocation
    }

    pub fn matches_project_context(&self, cwd: &str) -> bool {
        if self.paths.is_empty() {
            return true;
        }
        self.paths.iter().any(|pattern| cwd.contains(pattern.trim_matches('*')))
    }
}

impl Default for SkillExecutionContext {
    fn default() -> Self {
        Self::Inline
    }
}

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkillSource {
    Bundled,
    User,
    Filesystem,
}

impl SkillSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bundled => "bundled",
            Self::User => "user",
            Self::Filesystem => "project",
        }
    }
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
    pub workflow_hint: Option<String>,
    pub allowed_tools: Vec<String>,
    pub aliases: Vec<String>,
    pub user_invocable: bool,
    pub disable_model_invocation: bool,
    pub hidden: bool,
    pub paths: Vec<String>,
    pub exclude_paths: Vec<String>,
    pub requires_files: Vec<String>,
    pub context: SkillExecutionContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDefinition {
    pub name: String,
    pub description: String,
    pub when_to_use: Option<String>,
    pub argument_hint: Option<String>,
    pub workflow_hint: Option<String>,
    pub allowed_tools: Vec<String>,
    pub aliases: Vec<String>,
    pub user_invocable: bool,
    pub disable_model_invocation: bool,
    pub hidden: bool,
    pub paths: Vec<String>,
    pub exclude_paths: Vec<String>,
    pub requires_files: Vec<String>,
    pub context: SkillExecutionContext,
    pub content: String,
    pub source: SkillSource,
    pub file_path: Option<PathBuf>,
}

impl SkillDefinition {
    pub fn is_model_invocable(&self) -> bool {
        !self.disable_model_invocation && !self.hidden
    }

    pub fn matches_project_context(&self, cwd: &Path) -> bool {
        let normalized_cwd = normalize_path(cwd);
        if !self.paths.is_empty()
            && !self
                .paths
                .iter()
                .any(|pattern| wildcard_match(&normalize_pattern(pattern), &normalized_cwd))
        {
            return false;
        }
        if self
            .exclude_paths
            .iter()
            .any(|pattern| wildcard_match(&normalize_pattern(pattern), &normalized_cwd))
        {
            return false;
        }
        self.requires_files
            .iter()
            .all(|path| cwd.join(path).exists())
    }

    pub fn is_user_visible(&self, cwd: &Path) -> bool {
        self.user_invocable && !self.hidden && self.matches_project_context(cwd)
    }
}

impl Default for SkillExecutionContext {
    fn default() -> Self {
        Self::Inline
    }
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn normalize_pattern(pattern: &str) -> String {
    pattern.trim().replace('\\', "/")
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut p, mut v) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);

    while v < value.len() {
        if p < pattern.len() && (pattern[p] == value[v] || pattern[p] == b'*') {
            if pattern[p] == b'*' {
                star_idx = Some(p);
                match_idx = v;
                p += 1;
            } else {
                p += 1;
                v += 1;
            }
        } else if let Some(star) = star_idx {
            p = star + 1;
            match_idx += 1;
            v = match_idx;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

use std::collections::BTreeMap;
use std::fs;
use std::hash::Hash;
use std::path::{Path, PathBuf};

use crate::bootstrap::config_root::{PRIMARY_CONFIG_DIR, preferred_workspace_config_root};
use crate::skills::frontmatter::parse_frontmatter;
use crate::skills::types::{SkillDefinition, SkillSource};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillLoadResult {
    pub roots: Vec<PathBuf>,
    pub diagnostics: Vec<String>,
    pub skills: Vec<SkillDefinition>,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SkillLoaderCache {
    cached: Option<SkillLoadResult>,
}

pub fn load_filesystem_skills(root: &Path) -> anyhow::Result<Vec<SkillDefinition>> {
    Ok(load_skills_with_diagnostics(root)?.skills)
}

pub fn load_skills_with_diagnostics(root: &Path) -> anyhow::Result<SkillLoadResult> {
    let roots = skill_roots(root);
    let fingerprint = compute_fingerprint(&roots);
    load_skills_with_fingerprint(&roots, fingerprint)
}

impl SkillLoaderCache {
    pub fn load_or_reload(&mut self, root: &Path) -> anyhow::Result<(SkillLoadResult, bool)> {
        let roots = skill_roots(root);
        let fingerprint = compute_fingerprint(&roots);
        if let Some(cached) = self.cached.as_ref() {
            if cached.fingerprint == fingerprint {
                return Ok((cached.clone(), false));
            }
        }
        let result = load_skills_with_fingerprint(&roots, fingerprint)?;
        self.cached = Some(result.clone());
        Ok((result, true))
    }

    pub fn invalidate(&mut self) {
        self.cached = None;
    }
}

fn load_skills_with_fingerprint(
    roots: &[(PathBuf, SkillSource)],
    fingerprint: u64,
) -> anyhow::Result<SkillLoadResult> {
    let mut diagnostics = Vec::new();
    let mut loaded = BTreeMap::new();

    for (skills_root, source) in roots {
        if !skills_root.exists() {
            continue;
        }
        visit_skill_dirs(skills_root, *source, &mut loaded, &mut diagnostics)?;
    }

    Ok(SkillLoadResult {
        roots: roots.iter().map(|(path, _)| path.clone()).collect(),
        diagnostics,
        skills: loaded.into_values().collect(),
        fingerprint,
    })
}

fn skill_roots(root: &Path) -> Vec<(PathBuf, SkillSource)> {
    let mut roots = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        let user_root = PathBuf::from(home).join(PRIMARY_CONFIG_DIR).join("skills");
        let workspace_root = preferred_workspace_config_root(root).join("skills");
        if user_root != workspace_root {
            roots.push((user_root, SkillSource::User));
        }
    }
    roots.push((
        preferred_workspace_config_root(root).join("skills"),
        SkillSource::Filesystem,
    ));
    roots
}

fn visit_skill_dirs(
    dir: &Path,
    source: SkillSource,
    skills: &mut BTreeMap<String, SkillDefinition>,
    diagnostics: &mut Vec<String>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let skill_file = path.join("SKILL.md");
            if skill_file.is_file() {
                match load_skill_file(&skill_file, source) {
                    Ok(skill) => {
                        skills.insert(skill.name.clone(), skill);
                    }
                    Err(error) => diagnostics.push(format!(
                        "Failed to load skill {}: {error}",
                        skill_file.display()
                    )),
                }
            }
            visit_skill_dirs(&path, source, skills, diagnostics)?;
        }
    }
    Ok(())
}

fn load_skill_file(path: &PathBuf, source: SkillSource) -> anyhow::Result<SkillDefinition> {
    let markdown = fs::read_to_string(path)?;
    let (frontmatter, content) = parse_frontmatter(&markdown)?;
    let default_name = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("skill path is missing a directory name"))?
        .to_string();
    let name = frontmatter.name.unwrap_or(default_name);
    let description = frontmatter
        .description
        .unwrap_or_else(|| format!("Skill loaded from {}", path.display()));
    let when_to_use = normalized_field(frontmatter.when_to_use);
    let argument_hint = normalized_field(frontmatter.argument_hint);
    let workflow_hint = normalized_field(frontmatter.workflow_hint);
    let workflow_summary = build_workflow_summary(
        when_to_use.as_deref(),
        argument_hint.as_deref(),
        workflow_hint.as_deref(),
    );

    Ok(SkillDefinition {
        name,
        description,
        when_to_use,
        argument_hint,
        workflow_hint,
        workflow_summary,
        allowed_tools: frontmatter.allowed_tools,
        aliases: frontmatter.aliases,
        workflow_execution: frontmatter.workflow_execution,
        user_invocable: frontmatter.user_invocable,
        disable_model_invocation: frontmatter.disable_model_invocation,
        hidden: frontmatter.hidden,
        paths: frontmatter.paths,
        exclude_paths: frontmatter.exclude_paths,
        requires_files: frontmatter.requires_files,
        context: frontmatter.context,
        content: content.trim().to_string(),
        source,
        file_path: Some(path.clone()),
    })
}

fn normalized_field(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn build_workflow_summary(
    when_to_use: Option<&str>,
    argument_hint: Option<&str>,
    workflow_hint: Option<&str>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(value) = workflow_hint {
        parts.push(value.trim().to_string());
    }
    if let Some(value) = argument_hint {
        parts.push(format!("args: {}", value.trim()));
    }
    if let Some(value) = when_to_use {
        parts.push(format!("use: {}", value.trim()));
    }
    (!parts.is_empty()).then(|| parts.join(" | "))
}

fn compute_fingerprint(roots: &[(PathBuf, SkillSource)]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for (root, source) in roots {
        source.hash(&mut hasher);
        root.to_string_lossy().hash(&mut hasher);
        collect_skill_file_metadata(root, &mut hasher);
    }
    hasher.finish()
}

fn collect_skill_file_metadata(root: &Path, hasher: &mut impl std::hash::Hasher) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        path.to_string_lossy().hash(hasher);
        if path.is_dir() {
            collect_skill_file_metadata(&path, hasher);
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("SKILL.md") {
            if let Ok(metadata) = fs::metadata(&path) {
                metadata.len().hash(hasher);
                if let Ok(modified) = metadata.modified() {
                    if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                        duration.as_secs().hash(hasher);
                        duration.subsec_nanos().hash(hasher);
                    }
                }
            }
        }
    }
}

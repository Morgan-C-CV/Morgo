use std::fs;
use std::path::{Path, PathBuf};

use crate::skills::frontmatter::parse_frontmatter;
use crate::skills::types::{SkillDefinition, SkillSource};

pub fn load_filesystem_skills(root: &Path) -> anyhow::Result<Vec<SkillDefinition>> {
    let mut skills = Vec::new();
    let skills_root = root.join(".claude").join("skills");
    if !skills_root.exists() {
        return Ok(skills);
    }
    visit_skill_dirs(&skills_root, &mut skills)?;
    Ok(skills)
}

fn visit_skill_dirs(dir: &Path, skills: &mut Vec<SkillDefinition>) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let skill_file = path.join("SKILL.md");
            if skill_file.is_file() {
                if let Ok(skill) = load_skill_file(&skill_file) {
                    skills.push(skill);
                }
            }
            visit_skill_dirs(&path, skills)?;
        }
    }
    Ok(())
}

fn load_skill_file(path: &PathBuf) -> anyhow::Result<SkillDefinition> {
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

    Ok(SkillDefinition {
        name,
        description,
        when_to_use: frontmatter.when_to_use,
        argument_hint: frontmatter.argument_hint,
        allowed_tools: frontmatter.allowed_tools,
        user_invocable: frontmatter.user_invocable,
        disable_model_invocation: frontmatter.disable_model_invocation,
        paths: frontmatter.paths,
        context: frontmatter.context,
        content: content.trim().to_string(),
        source: SkillSource::Filesystem,
        file_path: Some(path.clone()),
    })
}

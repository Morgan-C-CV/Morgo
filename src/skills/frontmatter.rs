use crate::skills::types::{SkillExecutionContext, SkillFrontmatter};

pub fn parse_frontmatter(markdown: &str) -> anyhow::Result<(SkillFrontmatter, String)> {
    let Some(rest) = markdown.strip_prefix("---\n") else {
        return Ok((SkillFrontmatter::default(), markdown.to_string()));
    };
    let Some((frontmatter_block, content)) = rest.split_once("\n---\n") else {
        return Ok((SkillFrontmatter::default(), markdown.to_string()));
    };

    let mut frontmatter = SkillFrontmatter {
        user_invocable: true,
        ..SkillFrontmatter::default()
    };

    for raw_line in frontmatter_block.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('"').trim_matches('\'');
        match key {
            "name" => frontmatter.name = non_empty(value),
            "description" => frontmatter.description = non_empty(value),
            "when_to_use" => frontmatter.when_to_use = non_empty(value),
            "argument-hint" => frontmatter.argument_hint = non_empty(value),
            "allowed-tools" => frontmatter.allowed_tools = split_csv(value),
            "user-invocable" => frontmatter.user_invocable = parse_bool(value).unwrap_or(true),
            "disable-model-invocation" => {
                frontmatter.disable_model_invocation = parse_bool(value).unwrap_or(false)
            }
            "paths" => frontmatter.paths = split_csv(value),
            "context" => {
                frontmatter.context = if value.eq_ignore_ascii_case("fork") {
                    SkillExecutionContext::Fork
                } else {
                    SkillExecutionContext::Inline
                }
            }
            _ => {}
        }
    }

    Ok((frontmatter, content.to_string()))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "1" => Some(true),
        "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

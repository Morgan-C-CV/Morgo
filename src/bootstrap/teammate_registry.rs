use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, bail};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeammateRegistry {
    pub profiles: Vec<TeammateProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeammateProfile {
    pub id: String,
    pub name: String,
    pub description: String,
    pub role: String,
    pub default_model_profile: Option<String>,
    pub allowed_tools: Vec<String>,
    pub max_turns: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TeammateRegistryFile {
    profiles: Vec<TeammateProfile>,
}

pub fn load_teammate_registry_from_root(
    config_root: &Path,
) -> anyhow::Result<Option<TeammateRegistry>> {
    let path = config_root.join("buddies").join("agents.json");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("invalid_configuration: failed to read {}", path.display()))?;
    parse_teammate_registry(&content).map(Some)
}

pub fn parse_teammate_registry(content: &str) -> anyhow::Result<TeammateRegistry> {
    let file: TeammateRegistryFile = serde_json::from_str(content)
        .map_err(|error| anyhow::anyhow!("invalid_configuration: invalid agents.json: {error}"))?;
    validate_registry(file.profiles)
}

fn validate_registry(profiles: Vec<TeammateProfile>) -> anyhow::Result<TeammateRegistry> {
    let mut ids = BTreeSet::new();
    for profile in &profiles {
        validate_required("id", &profile.id, &profile.id)?;
        validate_required("name", &profile.name, &profile.id)?;
        validate_required("description", &profile.description, &profile.id)?;
        validate_required("role", &profile.role, &profile.id)?;
        if profile.max_turns == 0 {
            bail!(
                "invalid_configuration: invalid agents.json: teammate '{}' max_turns must be > 0",
                profile.id.trim()
            );
        }
        if !ids.insert(profile.id.trim().to_string()) {
            bail!(
                "invalid_configuration: invalid agents.json: duplicate teammate id '{}'",
                profile.id.trim()
            );
        }
    }
    Ok(TeammateRegistry { profiles })
}

fn validate_required(field: &str, value: &str, id: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        bail!(
            "invalid_configuration: invalid agents.json: teammate '{}' missing {}",
            id.trim(),
            field
        );
    }
    Ok(())
}

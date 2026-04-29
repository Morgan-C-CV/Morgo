use std::collections::BTreeMap;
use std::path::Path;

use crate::skills::types::{SkillDefinition, SkillSource};

/// Numeric precedence for a skill within a name-collision group.
/// Higher value = wins the conflict.
///
/// Filesystem skills at deeper scope depth beat shallower ones (more specific project context).
/// Filesystem always beats User; User always beats Bundled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SkillPrecedence(u32);

impl SkillPrecedence {
    const BUNDLED_BASE: u32 = 0;
    const USER_BASE: u32 = 1_000_000;
    const FILESYSTEM_BASE: u32 = 2_000_000;

    pub fn for_skill(skill: &SkillDefinition, cwd: &Path) -> Self {
        match skill.source {
            SkillSource::Bundled => Self(Self::BUNDLED_BASE),
            SkillSource::User => Self(Self::USER_BASE),
            SkillSource::Filesystem => {
                let depth = skill
                    .file_path
                    .as_deref()
                    .and_then(|p| p.parent())
                    .map(|skill_root| scope_depth(skill_root, cwd))
                    .unwrap_or(0);
                Self(Self::FILESYSTEM_BASE + depth)
            }
        }
    }
}

/// How many path components of `skill_root` are shared with `cwd`.
/// More shared components = deeper scope = higher precedence.
fn scope_depth(skill_root: &Path, cwd: &Path) -> u32 {
    let skill_components: Vec<_> = skill_root.components().collect();
    let cwd_components: Vec<_> = cwd.components().collect();
    skill_components
        .iter()
        .zip(cwd_components.iter())
        .take_while(|(a, b)| a == b)
        .count() as u32
}

/// The outcome of the visibility resolver for a single skill name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillActivationDecision {
    /// This skill is active and visible.
    Active(SkillDefinition),
    /// This skill was shadowed by a higher-precedence skill with the same name.
    Shadowed {
        skill: SkillDefinition,
        winner_source: SkillSource,
    },
    /// This skill is explicitly disabled (hidden = true).
    Disabled(SkillDefinition),
}

impl SkillActivationDecision {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active(_))
    }

    pub fn skill(&self) -> &SkillDefinition {
        match self {
            Self::Active(s) | Self::Shadowed { skill: s, .. } | Self::Disabled(s) => s,
        }
    }
}

/// Diagnostic record for a name-collision group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillConflictRecord {
    pub name: String,
    pub winner_source: SkillSource,
    pub shadowed_sources: Vec<SkillSource>,
}

/// Result of running the visibility resolver.
#[derive(Debug, Clone, Default)]
pub struct SkillVisibilityResult {
    pub decisions: Vec<SkillActivationDecision>,
    pub conflicts: Vec<SkillConflictRecord>,
}

impl SkillVisibilityResult {
    /// All active skills, filtered to those visible in `cwd`.
    pub fn active_skills(&self) -> Vec<&SkillDefinition> {
        self.decisions
            .iter()
            .filter_map(|d| match d {
                SkillActivationDecision::Active(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    /// Active skills that are user-invocable in `cwd`.
    pub fn user_invocable_skills(&self, cwd: &Path) -> Vec<&SkillDefinition> {
        self.active_skills()
            .into_iter()
            .filter(|s| s.is_user_visible(cwd))
            .collect()
    }

    /// Active skills that are model-invocable in `cwd`.
    pub fn model_invocable_skills(&self, cwd: &Path) -> Vec<&SkillDefinition> {
        self.active_skills()
            .into_iter()
            .filter(|s| s.is_model_invocable() && s.matches_project_context(cwd))
            .collect()
    }
}

/// Resolve visibility and activation for a flat list of skill definitions.
///
/// Rules (in priority order):
/// 1. `hidden = true` → `Disabled`, never active regardless of source.
/// 2. Name collision → highest `SkillPrecedence` wins; others become `Shadowed`.
///    Tie-break: `Filesystem > User > Bundled`; deeper filesystem scope beats shallower.
/// 3. Skills that don't match `cwd` via `matches_project_context` are excluded from
///    the active set but still appear as `Active` in the decision list — callers that
///    need cwd-filtered results should use `user_invocable_skills(cwd)` /
///    `model_invocable_skills(cwd)`.
pub fn resolve_skill_visibility(skills: Vec<SkillDefinition>, cwd: &Path) -> SkillVisibilityResult {
    // Group by canonical name (aliases are not deduplicated here — that's the registry's job)
    let mut groups: BTreeMap<String, Vec<(SkillPrecedence, SkillDefinition)>> = BTreeMap::new();
    for skill in skills {
        let precedence = SkillPrecedence::for_skill(&skill, cwd);
        groups
            .entry(skill.name.clone())
            .or_default()
            .push((precedence, skill));
    }

    let mut decisions = Vec::new();
    let mut conflicts = Vec::new();

    for (name, mut candidates) in groups {
        // Sort descending by precedence — highest first
        candidates.sort_by(|a, b| b.0.cmp(&a.0));

        // Partition disabled (hidden) from active candidates
        let (disabled, mut active): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(|(_, s)| s.hidden);

        for (_, skill) in disabled {
            decisions.push(SkillActivationDecision::Disabled(skill));
        }

        if active.is_empty() {
            continue;
        }

        let (_, winner) = active.remove(0);
        let winner_source = winner.source;

        if !active.is_empty() {
            let shadowed_sources: Vec<_> = active.iter().map(|(_, s)| s.source).collect();
            conflicts.push(SkillConflictRecord {
                name: name.clone(),
                winner_source,
                shadowed_sources,
            });
            for (_, skill) in active {
                decisions.push(SkillActivationDecision::Shadowed {
                    skill,
                    winner_source,
                });
            }
        }

        decisions.push(SkillActivationDecision::Active(winner));
    }

    SkillVisibilityResult { decisions, conflicts }
}

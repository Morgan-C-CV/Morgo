use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Which layer of the prompt assembly this segment belongs to.
/// Determines cache eligibility and assembly order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PromptSegmentKind {
    /// System prompt, safety rules, global agent behavior. Stable across turns.
    StaticSystem,
    /// Tool schema visible to this actor. Stable per actor role.
    ToolSchema,
    /// Repo root, git state, AGENTS, external memory. Stable per cwd fingerprint.
    ProjectContext,
    /// Immutable plan: document_spec, pseudo_code, step contract. Stable per plan.
    BossPlan,
    /// Actor role, lineage, allowed tools. Stable per actor instance.
    ActorBrief,
    /// Current abstract state, open/blocked items, allowed actions, required output schema.
    /// Changes every turn — must be non-cache suffix.
    StateFrame,
    /// Current diff, tool output, review/correction, failed test summary.
    /// Changes every turn — must be non-cache suffix.
    DynamicEvidence,
}

impl PromptSegmentKind {
    /// Whether this segment kind is eligible for provider-side caching.
    /// Dynamic kinds must always be non-cache suffixes.
    pub fn is_cacheable(&self) -> bool {
        matches!(
            self,
            Self::StaticSystem
                | Self::ToolSchema
                | Self::ProjectContext
                | Self::BossPlan
                | Self::ActorBrief
        )
    }
}

/// Stable, repeatable fingerprint derived from segment content and kind.
/// Two segments with identical kind + content produce identical fingerprints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromptSegmentFingerprint(u64);

impl PromptSegmentFingerprint {
    pub fn compute(kind: PromptSegmentKind, content: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        kind.hash(&mut hasher);
        content.hash(&mut hasher);
        Self(hasher.finish())
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

/// A single named slice of a prompt, with its kind, content, and fingerprint.
#[derive(Debug, Clone)]
pub struct PromptSegment {
    pub id: String,
    pub kind: PromptSegmentKind,
    pub content: String,
    pub fingerprint: PromptSegmentFingerprint,
}

impl PromptSegment {
    pub fn new(id: impl Into<String>, kind: PromptSegmentKind, content: impl Into<String>) -> Self {
        let content = content.into();
        let fingerprint = PromptSegmentFingerprint::compute(kind, &content);
        Self { id: id.into(), kind, content, fingerprint }
    }

    pub fn is_cacheable(&self) -> bool {
        self.kind.is_cacheable()
    }
}

/// An ordered collection of prompt segments ready for assembly.
/// Cacheable segments must precede dynamic segments — callers are responsible for ordering.
#[derive(Debug, Default)]
pub struct PromptAssembly {
    segments: Vec<PromptSegment>,
}

impl PromptAssembly {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, segment: PromptSegment) {
        self.segments.push(segment);
    }

    pub fn segments(&self) -> &[PromptSegment] {
        &self.segments
    }

    /// Assemble all segments into a single string, joining with newlines.
    /// This is the fallback path — identical to the existing string-join behavior.
    pub fn assemble(&self) -> String {
        self.segments.iter().map(|s| s.content.as_str()).collect::<Vec<_>>().join("\n")
    }

    /// Fingerprint of the stable (cacheable) prefix only.
    /// Dynamic segments do not contribute to this fingerprint.
    pub fn stable_prefix_fingerprint(&self) -> Option<PromptSegmentFingerprint> {
        let mut hasher = DefaultHasher::new();
        let mut any = false;
        for seg in &self.segments {
            if seg.is_cacheable() {
                seg.fingerprint.value().hash(&mut hasher);
                any = true;
            }
        }
        if any { Some(PromptSegmentFingerprint(hasher.finish())) } else { None }
    }
}

/// Result of a prefix stability check between two consecutive assemblies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixStabilityResult {
    /// Cacheable prefix fingerprint is unchanged — cache is valid.
    Stable { fingerprint: Option<PromptSegmentFingerprint> },
    /// Cacheable prefix fingerprint changed unexpectedly — cache must be invalidated.
    Unstable { prev: Option<PromptSegmentFingerprint>, current: Option<PromptSegmentFingerprint> },
}

/// Compare the current assembly's stable prefix fingerprint against a previously recorded value.
/// Pure function — does not modify assembly or any state.
pub fn check_prefix_stability(
    prev_fingerprint: Option<PromptSegmentFingerprint>,
    assembly: &PromptAssembly,
) -> PrefixStabilityResult {
    let current = assembly.stable_prefix_fingerprint();
    if current == prev_fingerprint {
        PrefixStabilityResult::Stable { fingerprint: current }
    } else {
        PrefixStabilityResult::Unstable { prev: prev_fingerprint, current }
    }
}

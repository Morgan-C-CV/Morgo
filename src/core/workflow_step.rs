use serde::{Deserialize, Serialize};

// ── Step resource kind ────────────────────────────────────────────────────────

/// The primary execution driver for a workflow step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStepKind {
    /// Step is driven by a named skill (PromptOnly or Agent execution).
    Skill,
    /// Step is driven by a plugin command or tool.
    Plugin,
    /// Step is driven by an MCP tool invocation.
    Mcp,
    /// Step composes multiple resource kinds (parallel or sequential sub-steps).
    Composite,
    /// Step has no specific resource assignment — falls back to bare LLM dispatch.
    Unassigned,
}

impl WorkflowStepKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Skill => "skill",
            Self::Plugin => "plugin",
            Self::Mcp => "mcp",
            Self::Composite => "composite",
            Self::Unassigned => "unassigned",
        }
    }
}

// ── Step resource reference ───────────────────────────────────────────────────

/// A reference to a specific named resource assigned to a step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStepResourceRef {
    pub kind: WorkflowStepKind,
    /// Name of the resource: skill name, plugin name, or MCP server name.
    pub name: String,
    /// Optional sub-tool or sub-command within the resource (e.g. plugin tool name).
    pub sub_name: Option<String>,
}

impl WorkflowStepResourceRef {
    pub fn skill(name: impl Into<String>) -> Self {
        Self {
            kind: WorkflowStepKind::Skill,
            name: name.into(),
            sub_name: None,
        }
    }

    pub fn plugin(name: impl Into<String>, sub_name: impl Into<String>) -> Self {
        Self {
            kind: WorkflowStepKind::Plugin,
            name: name.into(),
            sub_name: Some(sub_name.into()),
        }
    }

    pub fn mcp(server_name: impl Into<String>) -> Self {
        Self {
            kind: WorkflowStepKind::Mcp,
            name: server_name.into(),
            sub_name: None,
        }
    }

    pub fn render_line(&self) -> String {
        match &self.sub_name {
            Some(sub) => format!("{}:{}:{}", self.kind.as_str(), self.name, sub),
            None => format!("{}:{}", self.kind.as_str(), self.name),
        }
    }
}

// ── Step contract ─────────────────────────────────────────────────────────────

/// Typed contract for a single workflow step.
///
/// Describes what resources are required, what outputs the step declares,
/// which prior steps it depends on, and its retry/diagnostic budget.
/// This is the canonical description callers use to validate a step before dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStepContract {
    pub step_id: usize,
    pub kind: WorkflowStepKind,
    /// Resources assigned to this step (may be empty for Unassigned steps).
    pub resources: Vec<WorkflowStepResourceRef>,
    /// Logical output tags this step is expected to produce (e.g. "diff", "test_report").
    /// Used for downstream dependency validation.
    pub expected_outputs: Vec<String>,
    /// Step ids that must be completed before this step can start.
    pub depends_on: Vec<usize>,
    /// Maximum number of dispatch attempts before the step is marked failed.
    pub retry_budget: u32,
    /// Whether the step requires explicit user approval before marking completed.
    pub requires_approval: bool,
}

impl WorkflowStepContract {
    pub fn has_resource_of_kind(&self, kind: WorkflowStepKind) -> bool {
        self.resources.iter().any(|r| r.kind == kind)
    }

    pub fn resource_names_for_kind(&self, kind: WorkflowStepKind) -> Vec<&str> {
        self.resources
            .iter()
            .filter(|r| r.kind == kind)
            .map(|r| r.name.as_str())
            .collect()
    }

    pub fn is_satisfied_by(&self, available: &WorkflowResourceAvailability) -> bool {
        for resource in &self.resources {
            match resource.kind {
                WorkflowStepKind::Skill => {
                    if !available.skill_names.contains(&resource.name) {
                        return false;
                    }
                }
                WorkflowStepKind::Plugin => {
                    if !available.plugin_names.contains(&resource.name) {
                        return false;
                    }
                }
                WorkflowStepKind::Mcp => {
                    if !available.mcp_server_names.contains(&resource.name) {
                        return false;
                    }
                }
                WorkflowStepKind::Composite | WorkflowStepKind::Unassigned => {}
            }
        }
        true
    }

    /// Render a one-line summary for diagnostic/observability output.
    pub fn render_summary(&self) -> String {
        let resources: Vec<String> = self.resources.iter().map(|r| r.render_line()).collect();
        format!(
            "step={} kind={} resources=[{}] depends_on={:?} outputs={:?} retry={}",
            self.step_id,
            self.kind.as_str(),
            resources.join(", "),
            self.depends_on,
            self.expected_outputs,
            self.retry_budget,
        )
    }
}

// ── Available resources snapshot ──────────────────────────────────────────────

/// Snapshot of available resources at step dispatch time.
/// Used by `is_satisfied_by` and `resolve_step_contract`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkflowResourceAvailability {
    pub skill_names: Vec<String>,
    pub plugin_names: Vec<String>,
    pub mcp_server_names: Vec<String>,
}

impl WorkflowResourceAvailability {
    pub fn with_skills(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.skill_names.extend(names.into_iter().map(Into::into));
        self
    }

    pub fn with_plugins(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.plugin_names.extend(names.into_iter().map(Into::into));
        self
    }

    pub fn with_mcp(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.mcp_server_names
            .extend(names.into_iter().map(Into::into));
        self
    }
}

// ── Cross-step state handoff ──────────────────────────────────────────────────

/// Completed output produced by a step — carried forward to dependent steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStepOutput {
    pub step_id: usize,
    /// Tags declared in the contract's `expected_outputs` that were actually produced.
    pub produced_tags: Vec<String>,
    /// Optional human-readable summary of this step's result (e.g. diff summary).
    pub result_summary: Option<String>,
    /// Whether the step completed successfully.
    pub succeeded: bool,
}

impl WorkflowStepOutput {
    pub fn success(step_id: usize, tags: Vec<String>, summary: Option<String>) -> Self {
        Self {
            step_id,
            produced_tags: tags,
            result_summary: summary,
            succeeded: true,
        }
    }

    pub fn failure(step_id: usize) -> Self {
        Self {
            step_id,
            produced_tags: vec![],
            result_summary: None,
            succeeded: false,
        }
    }

    pub fn produces_tag(&self, tag: &str) -> bool {
        self.produced_tags.iter().any(|t| t == tag)
    }
}

/// Cross-step state envelope passed from completed steps to their dependents.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkflowStepState {
    /// Outputs from all completed upstream steps, keyed by step_id order.
    pub completed_outputs: Vec<WorkflowStepOutput>,
    /// Diagnostic observations accumulated across prior steps.
    pub observations: Vec<WorkflowStepObservation>,
}

impl WorkflowStepState {
    pub fn output_for_step(&self, step_id: usize) -> Option<&WorkflowStepOutput> {
        self.completed_outputs.iter().find(|o| o.step_id == step_id)
    }

    pub fn all_succeeded(&self) -> bool {
        self.completed_outputs.iter().all(|o| o.succeeded)
    }

    pub fn any_failed(&self) -> bool {
        self.completed_outputs.iter().any(|o| !o.succeeded)
    }

    /// Returns true if all tags required by `contract.depends_on` steps are present.
    pub fn satisfies_dependencies(&self, contract: &WorkflowStepContract) -> bool {
        contract.depends_on.iter().all(|dep_id| {
            self.completed_outputs
                .iter()
                .any(|o| o.step_id == *dep_id && o.succeeded)
        })
    }

    /// Observation messages from upstream — used as context for the next step.
    pub fn observation_lines(&self) -> Vec<String> {
        self.observations.iter().map(|o| o.render_line()).collect()
    }
}

// ── Step observations (diagnostic cross-step signal) ─────────────────────────

/// A typed diagnostic observation emitted by a step, forwarded to dependents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStepObservation {
    pub step_id: usize,
    pub kind: WorkflowObservationKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowObservationKind {
    /// A skill name collision was observed during dispatch.
    SkillConflict,
    /// A plugin capability was blocked (governance or lifecycle).
    PluginBlocked,
    /// An MCP server was unavailable during dispatch.
    McpUnavailable,
    /// The step hit its retry budget.
    RetryBudgetExhausted,
    /// The step required user approval that was not pre-granted.
    ApprovalGate,
    /// Informational note — no action required.
    Info,
}

impl WorkflowObservationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SkillConflict => "skill_conflict",
            Self::PluginBlocked => "plugin_blocked",
            Self::McpUnavailable => "mcp_unavailable",
            Self::RetryBudgetExhausted => "retry_budget_exhausted",
            Self::ApprovalGate => "approval_gate",
            Self::Info => "info",
        }
    }
}

impl WorkflowStepObservation {
    pub fn render_line(&self) -> String {
        format!(
            "step={} [{}] {}",
            self.step_id,
            self.kind.as_str(),
            self.message
        )
    }
}

// ── Dependency resolution ─────────────────────────────────────────────────────

/// Why a step cannot be dispatched yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowStepBlocker {
    /// One or more upstream dependency steps have not completed.
    DependencyNotCompleted { pending_step_ids: Vec<usize> },
    /// A required resource is not in the available set.
    ResourceNotAvailable { resource: WorkflowStepResourceRef },
    /// Upstream state includes failed steps that block this one.
    UpstreamFailure { failed_step_ids: Vec<usize> },
}

impl WorkflowStepBlocker {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DependencyNotCompleted { .. } => "dependency_not_completed",
            Self::ResourceNotAvailable { .. } => "resource_not_available",
            Self::UpstreamFailure { .. } => "upstream_failure",
        }
    }

    pub fn render_line(&self) -> String {
        match self {
            Self::DependencyNotCompleted { pending_step_ids } => {
                format!("dependency_not_completed: pending={pending_step_ids:?}")
            }
            Self::ResourceNotAvailable { resource } => {
                format!("resource_not_available: {}", resource.render_line())
            }
            Self::UpstreamFailure { failed_step_ids } => {
                format!("upstream_failure: failed={failed_step_ids:?}")
            }
        }
    }
}

/// Outcome of checking whether a step is ready to dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowStepReadiness {
    Ready,
    Blocked(Vec<WorkflowStepBlocker>),
}

impl WorkflowStepReadiness {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn blockers(&self) -> &[WorkflowStepBlocker] {
        match self {
            Self::Ready => &[],
            Self::Blocked(blockers) => blockers,
        }
    }
}

// ── Dispatch readiness check ──────────────────────────────────────────────────

/// Check whether `contract` can be dispatched given `state` and `available` resources.
pub fn check_step_readiness(
    contract: &WorkflowStepContract,
    state: &WorkflowStepState,
    available: &WorkflowResourceAvailability,
) -> WorkflowStepReadiness {
    let mut blockers = Vec::new();

    // Dependency check
    let pending: Vec<usize> = contract
        .depends_on
        .iter()
        .filter(|dep_id| {
            !state
                .completed_outputs
                .iter()
                .any(|o| o.step_id == **dep_id && o.succeeded)
        })
        .copied()
        .collect();
    if !pending.is_empty() {
        blockers.push(WorkflowStepBlocker::DependencyNotCompleted {
            pending_step_ids: pending,
        });
    }

    // Upstream failure check — only block if a *required* dependency failed
    let failed: Vec<usize> = contract
        .depends_on
        .iter()
        .filter(|dep_id| {
            state
                .completed_outputs
                .iter()
                .any(|o| o.step_id == **dep_id && !o.succeeded)
        })
        .copied()
        .collect();
    if !failed.is_empty() {
        blockers.push(WorkflowStepBlocker::UpstreamFailure {
            failed_step_ids: failed,
        });
    }

    // Resource availability check
    for resource in &contract.resources {
        let available_names = match resource.kind {
            WorkflowStepKind::Skill => &available.skill_names,
            WorkflowStepKind::Plugin => &available.plugin_names,
            WorkflowStepKind::Mcp => &available.mcp_server_names,
            WorkflowStepKind::Composite | WorkflowStepKind::Unassigned => continue,
        };
        if !available_names.contains(&resource.name) {
            blockers.push(WorkflowStepBlocker::ResourceNotAvailable {
                resource: resource.clone(),
            });
        }
    }

    if blockers.is_empty() {
        WorkflowStepReadiness::Ready
    } else {
        WorkflowStepReadiness::Blocked(blockers)
    }
}

// ── State handoff builder ─────────────────────────────────────────────────────

/// Produce the `WorkflowStepState` to pass into a step by collecting all
/// upstream completed outputs and observations that the step's `depends_on` list references.
pub fn build_handoff_state(
    all_outputs: &[WorkflowStepOutput],
    all_observations: &[WorkflowStepObservation],
    contract: &WorkflowStepContract,
) -> WorkflowStepState {
    let relevant_step_ids: std::collections::BTreeSet<usize> =
        contract.depends_on.iter().copied().collect();

    let completed_outputs = all_outputs
        .iter()
        .filter(|o| relevant_step_ids.contains(&o.step_id))
        .cloned()
        .collect();

    let observations = all_observations
        .iter()
        .filter(|o| relevant_step_ids.contains(&o.step_id))
        .cloned()
        .collect();

    WorkflowStepState {
        completed_outputs,
        observations,
    }
}

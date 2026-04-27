use crate::core::state_frame::{ActorRole, AgentState, StateFrame};

/// Resolved toolset and skillset identifiers for a given StateFrame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolsetRoute {
    pub toolset_id: Option<String>,
    pub skillset_id: Option<String>,
    pub allowed_actions: Vec<String>,
}

/// Route a StateFrame to its minimal toolset and skillset.
///
/// Pure static mapping on role + state — no runtime calls, no side effects.
/// Callers should apply the result by setting `frame.toolset_id`, `frame.skillset_id`,
/// and `frame.allowed_actions` before dispatching to `run_decision_loop`.
pub fn route_toolset(frame: &StateFrame) -> ToolsetRoute {
    // Terminal / blocked states: no tools regardless of role.
    if matches!(frame.state, AgentState::Blocked | AgentState::Done) {
        return ToolsetRoute {
            toolset_id: None,
            skillset_id: None,
            allowed_actions: vec![],
        };
    }

    match frame.role {
        ActorRole::DesignerA => route_designer_a(frame.state),
        ActorRole::ExecutorB => route_executor_b(frame.state),
        ActorRole::Worker => route_worker(frame.state),
        ActorRole::Verifier => route_verifier(frame.state),
        ActorRole::Summarizer => route_summarizer(frame.state),
    }
}

fn route_designer_a(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Planning => ToolsetRoute {
            toolset_id: Some("designer-planning".into()),
            skillset_id: Some("spec-writer".into()),
            allowed_actions: vec!["read_file".into(), "write_spec".into(), "list_files".into()],
        },
        AgentState::Reviewing => ToolsetRoute {
            toolset_id: Some("designer-review".into()),
            skillset_id: Some("code-reviewer".into()),
            allowed_actions: vec!["read_file".into(), "run_test".into()],
        },
        _ => conservative_fallback(),
    }
}

fn route_executor_b(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Executing => ToolsetRoute {
            toolset_id: Some("executor-edit".into()),
            skillset_id: Some("implementer".into()),
            allowed_actions: vec![
                "read_file".into(),
                "edit_file".into(),
                "run_test".into(),
                "list_files".into(),
            ],
        },
        AgentState::Correcting => ToolsetRoute {
            toolset_id: Some("executor-edit".into()),
            skillset_id: Some("implementer".into()),
            allowed_actions: vec!["read_file".into(), "edit_file".into(), "run_test".into()],
        },
        _ => conservative_fallback(),
    }
}

fn route_worker(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Executing | AgentState::Correcting => ToolsetRoute {
            toolset_id: Some("worker-minimal".into()),
            skillset_id: None,
            allowed_actions: vec!["read_file".into(), "edit_file".into(), "run_test".into()],
        },
        AgentState::Planning => ToolsetRoute {
            toolset_id: Some("worker-minimal".into()),
            skillset_id: None,
            allowed_actions: vec!["read_file".into(), "list_files".into()],
        },
        _ => conservative_fallback(),
    }
}

fn route_verifier(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Verifying | AgentState::Reviewing => ToolsetRoute {
            toolset_id: Some("verifier-readonly".into()),
            skillset_id: Some("acceptance-checker".into()),
            allowed_actions: vec!["read_file".into(), "run_test".into()],
        },
        _ => conservative_fallback(),
    }
}

fn route_summarizer(state: AgentState) -> ToolsetRoute {
    // Summarizer never needs write tools.
    match state {
        AgentState::Planning | AgentState::Executing | AgentState::Reviewing => ToolsetRoute {
            toolset_id: Some("summarizer-readonly".into()),
            skillset_id: Some("context-summarizer".into()),
            allowed_actions: vec!["read_file".into()],
        },
        _ => conservative_fallback(),
    }
}

/// Conservative fallback for unrecognized role+state combinations.
/// Returns read-only access — never write tools.
fn conservative_fallback() -> ToolsetRoute {
    ToolsetRoute {
        toolset_id: None,
        skillset_id: None,
        allowed_actions: vec!["read_file".into()],
    }
}

/// Apply a `ToolsetRoute` back onto a `StateFrame` in place.
pub fn apply_route(frame: &mut StateFrame, route: ToolsetRoute) {
    frame.toolset_id = route.toolset_id;
    frame.skillset_id = route.skillset_id;
    frame.allowed_actions = route.allowed_actions;
}

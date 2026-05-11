use crate::core::state_frame::{ActorRole, AgentState, StateFrame};

/// Resolved toolset and skillset identifiers for a given StateFrame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolsetRoute {
    pub toolset_id: Option<String>,
    pub skillset_id: Option<String>,
}

fn allowed_actions_for_route(route: &ToolsetRoute) -> Vec<String> {
    match route.toolset_id.as_deref() {
        Some("designer-planning") => vec!["read_file".into(), "write_spec".into()],
        Some("designer-review") => vec!["read_file".into(), "summarize_findings".into()],
        Some("executor-edit") => vec!["read_file".into(), "edit_file".into(), "run_test".into()],
        Some("worker-minimal") => vec!["read_file".into(), "edit_file".into(), "run_test".into()],
        Some("verifier-readonly") => vec!["read_file".into(), "summarize_findings".into()],
        Some("summarizer-readonly") => vec!["read_file".into(), "summarize_findings".into()],
        None => vec!["read_file".into()],
        Some(_) => vec!["read_file".into()],
    }
}

fn independent_review_requires_runtime_verification(frame: &StateFrame) -> bool {
    frame
        .stage_execution_contract
        .review_mode
        .is_some_and(|mode| mode.is_independent_review())
        && !frame.stage_execution_contract.verifications.is_empty()
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
        };
    }
    if independent_review_requires_runtime_verification(frame) {
        return ToolsetRoute {
            toolset_id: Some("verifier-readonly".into()),
            skillset_id: Some("acceptance-checker".into()),
        };
    }
    if matches!(
        frame.required_output_schema.as_deref(),
        Some("readonly_audit_4_paragraphs_v1")
    ) {
        return ToolsetRoute {
            toolset_id: Some("summarizer-readonly".into()),
            skillset_id: Some("context-summarizer".into()),
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
        },
        AgentState::Reviewing => ToolsetRoute {
            toolset_id: Some("designer-review".into()),
            skillset_id: Some("code-reviewer".into()),
        },
        _ => conservative_fallback(),
    }
}

fn route_executor_b(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Executing => ToolsetRoute {
            toolset_id: Some("executor-edit".into()),
            skillset_id: Some("implementer".into()),
        },
        AgentState::Correcting => ToolsetRoute {
            toolset_id: Some("executor-edit".into()),
            skillset_id: Some("implementer".into()),
        },
        _ => conservative_fallback(),
    }
}

fn route_worker(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Executing | AgentState::Correcting => ToolsetRoute {
            toolset_id: Some("worker-minimal".into()),
            skillset_id: None,
        },
        AgentState::Planning => ToolsetRoute {
            toolset_id: Some("worker-minimal".into()),
            skillset_id: None,
        },
        _ => conservative_fallback(),
    }
}

fn route_verifier(state: AgentState) -> ToolsetRoute {
    match state {
        AgentState::Verifying | AgentState::Reviewing => ToolsetRoute {
            toolset_id: Some("verifier-readonly".into()),
            skillset_id: Some("acceptance-checker".into()),
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
    }
}

/// Apply a `ToolsetRoute` back onto a `StateFrame` in place.
pub fn apply_route(frame: &mut StateFrame, route: ToolsetRoute) {
    let allowed_actions = if matches!(frame.state, AgentState::Blocked | AgentState::Done) {
        Vec::new()
    } else {
        allowed_actions_for_route(&route)
    };
    frame.toolset_id = route.toolset_id;
    frame.skillset_id = route.skillset_id;
    frame.allowed_actions = allowed_actions;
}

#[cfg(test)]
mod tests {
    use super::route_toolset;
    use crate::core::state_frame::{
        ActorRole, AgentState, DeclaredArtifactContract, ReviewMode, StageExecutionContract,
        StateBudget, StateFrame, VerificationContract,
    };

    fn verification_review_frame() -> StateFrame {
        StateFrame {
            role: ActorRole::Worker,
            state: AgentState::Executing,
            objective: "audit verification".into(),
            stage_execution_contract: StageExecutionContract {
                review_mode: Some(ReviewMode::IndependentReview),
                declared_artifacts: vec![DeclaredArtifactContract {
                    ref_id: "artifact:step0:0".into(),
                    path: "/tmp/report.md".into(),
                    kind: "file".into(),
                    required_actions: vec![],
                    required_evidence: vec![],
                }],
                verifications: vec![VerificationContract {
                    target_ref: "artifact:step0:0".into(),
                    target_path: Some("/tmp/report.md".into()),
                    required_actions: vec!["verify".into()],
                    required_evidence: vec!["artifact:step0:0".into()],
                }],
                ..StageExecutionContract::default()
            },
            open_items: vec![],
            blocked_items: vec![],
            accepted_summary: vec![],
            recent_evidence: vec![],
            allowed_actions: vec![],
            allowed_tools: vec![],
            toolset_id: None,
            skillset_id: None,
            required_output_schema: Some("readonly_audit_4_paragraphs_v1".into()),
            budget: StateBudget::default(),
        }
    }

    #[test]
    fn independent_review_with_verification_contract_routes_to_verifier_readonly() {
        let frame = verification_review_frame();
        let route = route_toolset(&frame);
        assert_eq!(route.toolset_id.as_deref(), Some("verifier-readonly"));
        assert_eq!(route.skillset_id.as_deref(), Some("acceptance-checker"));
    }
}

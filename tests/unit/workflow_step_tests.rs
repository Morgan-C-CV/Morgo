use rust_agent::core::workflow_step::{
    WorkflowObservationKind, WorkflowResourceAvailability, WorkflowStepBlocker,
    WorkflowStepContract, WorkflowStepKind, WorkflowStepObservation, WorkflowStepOutput,
    WorkflowStepResourceRef, WorkflowStepState, build_handoff_state,
    check_step_readiness,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_contract(
    step_id: usize,
    resources: Vec<WorkflowStepResourceRef>,
    depends_on: Vec<usize>,
) -> WorkflowStepContract {
    WorkflowStepContract {
        step_id,
        kind: if resources.is_empty() {
            WorkflowStepKind::Unassigned
        } else {
            resources[0].kind
        },
        resources,
        expected_outputs: vec!["result".to_string()],
        depends_on,
        retry_budget: 3,
        requires_approval: false,
    }
}

fn make_availability() -> WorkflowResourceAvailability {
    WorkflowResourceAvailability::default()
        .with_skills(["deploy", "review"])
        .with_plugins(["ci-plugin"])
        .with_mcp(["github", "linear"])
}

fn succeeded_output(step_id: usize) -> WorkflowStepOutput {
    WorkflowStepOutput::success(step_id, vec!["result".to_string()], None)
}

fn failed_output(step_id: usize) -> WorkflowStepOutput {
    WorkflowStepOutput::failure(step_id)
}

// ── WorkflowStepKind::as_str ──────────────────────────────────────────────────

#[test]
fn r4_3_step_kind_as_str_values() {
    assert_eq!(WorkflowStepKind::Skill.as_str(), "skill");
    assert_eq!(WorkflowStepKind::Plugin.as_str(), "plugin");
    assert_eq!(WorkflowStepKind::Mcp.as_str(), "mcp");
    assert_eq!(WorkflowStepKind::Composite.as_str(), "composite");
    assert_eq!(WorkflowStepKind::Unassigned.as_str(), "unassigned");
}

// ── WorkflowStepResourceRef constructors ─────────────────────────────────────

#[test]
fn r4_3_resource_ref_skill_constructor() {
    let r = WorkflowStepResourceRef::skill("deploy");
    assert_eq!(r.kind, WorkflowStepKind::Skill);
    assert_eq!(r.name, "deploy");
    assert!(r.sub_name.is_none());
}

#[test]
fn r4_3_resource_ref_plugin_constructor() {
    let r = WorkflowStepResourceRef::plugin("ci-plugin", "run-tests");
    assert_eq!(r.kind, WorkflowStepKind::Plugin);
    assert_eq!(r.name, "ci-plugin");
    assert_eq!(r.sub_name.as_deref(), Some("run-tests"));
}

#[test]
fn r4_3_resource_ref_mcp_constructor() {
    let r = WorkflowStepResourceRef::mcp("github");
    assert_eq!(r.kind, WorkflowStepKind::Mcp);
    assert_eq!(r.name, "github");
}

#[test]
fn r4_3_resource_ref_render_line_with_sub_name() {
    let r = WorkflowStepResourceRef::plugin("ci", "build");
    assert_eq!(r.render_line(), "plugin:ci:build");
}

#[test]
fn r4_3_resource_ref_render_line_without_sub_name() {
    let r = WorkflowStepResourceRef::skill("deploy");
    assert_eq!(r.render_line(), "skill:deploy");
}

// ── WorkflowStepContract::is_satisfied_by ────────────────────────────────────

#[test]
fn r4_3_contract_satisfied_when_all_resources_available() {
    let contract = make_contract(
        0,
        vec![
            WorkflowStepResourceRef::skill("deploy"),
            WorkflowStepResourceRef::mcp("github"),
        ],
        vec![],
    );
    let avail = make_availability();
    assert!(contract.is_satisfied_by(&avail));
}

#[test]
fn r4_3_contract_not_satisfied_when_skill_missing() {
    let contract = make_contract(
        0,
        vec![WorkflowStepResourceRef::skill("unknown-skill")],
        vec![],
    );
    let avail = make_availability();
    assert!(!contract.is_satisfied_by(&avail));
}

#[test]
fn r4_3_contract_not_satisfied_when_mcp_server_missing() {
    let contract = make_contract(0, vec![WorkflowStepResourceRef::mcp("jira")], vec![]);
    let avail = make_availability();
    assert!(!contract.is_satisfied_by(&avail));
}

#[test]
fn r4_3_contract_satisfied_for_unassigned_step() {
    let contract = make_contract(0, vec![], vec![]);
    let avail = WorkflowResourceAvailability::default();
    assert!(contract.is_satisfied_by(&avail));
}

#[test]
fn r4_3_contract_has_resource_of_kind() {
    let contract = make_contract(
        0,
        vec![
            WorkflowStepResourceRef::skill("deploy"),
            WorkflowStepResourceRef::mcp("github"),
        ],
        vec![],
    );
    assert!(contract.has_resource_of_kind(WorkflowStepKind::Skill));
    assert!(contract.has_resource_of_kind(WorkflowStepKind::Mcp));
    assert!(!contract.has_resource_of_kind(WorkflowStepKind::Plugin));
}

#[test]
fn r4_3_contract_resource_names_for_kind() {
    let contract = make_contract(
        0,
        vec![
            WorkflowStepResourceRef::skill("deploy"),
            WorkflowStepResourceRef::skill("review"),
        ],
        vec![],
    );
    let names = contract.resource_names_for_kind(WorkflowStepKind::Skill);
    assert_eq!(names, vec!["deploy", "review"]);
}

#[test]
fn r4_3_contract_render_summary_contains_key_fields() {
    let contract = make_contract(2, vec![WorkflowStepResourceRef::skill("deploy")], vec![1]);
    let line = contract.render_summary();
    assert!(line.contains("step=2"), "summary: {line}");
    assert!(line.contains("skill"), "summary: {line}");
    assert!(line.contains("deploy"), "summary: {line}");
}

// ── WorkflowResourceAvailability builder ──────────────────────────────────────

#[test]
fn r4_3_availability_builder_with_skills() {
    let avail = WorkflowResourceAvailability::default().with_skills(["a", "b"]);
    assert!(avail.skill_names.contains(&"a".to_string()));
    assert!(avail.skill_names.contains(&"b".to_string()));
}

// ── check_step_readiness ──────────────────────────────────────────────────────

#[test]
fn r4_3_ready_when_no_deps_and_resources_available() {
    let contract = make_contract(0, vec![WorkflowStepResourceRef::skill("deploy")], vec![]);
    let state = WorkflowStepState::default();
    let avail = make_availability();

    assert!(check_step_readiness(&contract, &state, &avail).is_ready());
}

#[test]
fn r4_3_blocked_when_dependency_not_completed() {
    let contract = make_contract(1, vec![], vec![0]);
    let state = WorkflowStepState::default(); // step 0 not in completed_outputs
    let avail = make_availability();

    let readiness = check_step_readiness(&contract, &state, &avail);
    assert!(!readiness.is_ready());
    assert!(readiness.blockers().iter().any(|b| matches!(
        b,
        WorkflowStepBlocker::DependencyNotCompleted { pending_step_ids }
        if pending_step_ids.contains(&0)
    )));
}

#[test]
fn r4_3_ready_when_dependency_completed_successfully() {
    let contract = make_contract(1, vec![], vec![0]);
    let state = WorkflowStepState {
        completed_outputs: vec![succeeded_output(0)],
        observations: vec![],
    };
    let avail = make_availability();

    assert!(check_step_readiness(&contract, &state, &avail).is_ready());
}

#[test]
fn r4_3_blocked_by_upstream_failure() {
    let contract = make_contract(2, vec![], vec![1]);
    let state = WorkflowStepState {
        completed_outputs: vec![failed_output(1)],
        observations: vec![],
    };
    let avail = make_availability();

    let readiness = check_step_readiness(&contract, &state, &avail);
    assert!(!readiness.is_ready());
    assert!(readiness.blockers().iter().any(|b| matches!(
        b,
        WorkflowStepBlocker::UpstreamFailure { failed_step_ids }
        if failed_step_ids.contains(&1)
    )));
}

#[test]
fn r4_3_blocked_when_resource_not_available() {
    let contract = make_contract(
        0,
        vec![WorkflowStepResourceRef::skill("missing-skill")],
        vec![],
    );
    let state = WorkflowStepState::default();
    let avail = make_availability();

    let readiness = check_step_readiness(&contract, &state, &avail);
    assert!(!readiness.is_ready());
    assert!(readiness.blockers().iter().any(|b| matches!(
        b,
        WorkflowStepBlocker::ResourceNotAvailable { resource }
        if resource.name == "missing-skill"
    )));
}

#[test]
fn r4_3_multiple_blockers_collected() {
    // Missing dep + missing resource
    let contract = make_contract(2, vec![WorkflowStepResourceRef::mcp("jira")], vec![1]);
    let state = WorkflowStepState::default(); // dep 1 not completed
    let avail = make_availability(); // "jira" not in mcp list

    let readiness = check_step_readiness(&contract, &state, &avail);
    assert!(!readiness.is_ready());
    assert!(readiness.blockers().len() >= 2);
}

// ── WorkflowStepBlocker::as_str / render_line ─────────────────────────────────

#[test]
fn r4_3_step_blocker_as_str_values() {
    assert_eq!(
        WorkflowStepBlocker::DependencyNotCompleted {
            pending_step_ids: vec![1]
        }
        .as_str(),
        "dependency_not_completed"
    );
    assert_eq!(
        WorkflowStepBlocker::ResourceNotAvailable {
            resource: WorkflowStepResourceRef::skill("x")
        }
        .as_str(),
        "resource_not_available"
    );
    assert_eq!(
        WorkflowStepBlocker::UpstreamFailure {
            failed_step_ids: vec![0]
        }
        .as_str(),
        "upstream_failure"
    );
}

#[test]
fn r4_3_step_blocker_render_line_includes_values() {
    let line = WorkflowStepBlocker::DependencyNotCompleted {
        pending_step_ids: vec![3, 4],
    }
    .render_line();
    assert!(line.contains("3"));
    assert!(line.contains("4"));
}

// ── WorkflowStepState ─────────────────────────────────────────────────────────

#[test]
fn r4_3_state_all_succeeded_true_when_all_outputs_succeeded() {
    let state = WorkflowStepState {
        completed_outputs: vec![succeeded_output(0), succeeded_output(1)],
        observations: vec![],
    };
    assert!(state.all_succeeded());
    assert!(!state.any_failed());
}

#[test]
fn r4_3_state_any_failed_true_when_one_output_failed() {
    let state = WorkflowStepState {
        completed_outputs: vec![succeeded_output(0), failed_output(1)],
        observations: vec![],
    };
    assert!(!state.all_succeeded());
    assert!(state.any_failed());
}

#[test]
fn r4_3_state_satisfies_dependencies_true_when_all_deps_succeeded() {
    let contract = make_contract(2, vec![], vec![0, 1]);
    let state = WorkflowStepState {
        completed_outputs: vec![succeeded_output(0), succeeded_output(1)],
        observations: vec![],
    };
    assert!(state.satisfies_dependencies(&contract));
}

#[test]
fn r4_3_state_satisfies_dependencies_false_when_dep_failed() {
    let contract = make_contract(2, vec![], vec![0, 1]);
    let state = WorkflowStepState {
        completed_outputs: vec![succeeded_output(0), failed_output(1)],
        observations: vec![],
    };
    assert!(!state.satisfies_dependencies(&contract));
}

#[test]
fn r4_3_state_output_for_step_found() {
    let state = WorkflowStepState {
        completed_outputs: vec![succeeded_output(3)],
        observations: vec![],
    };
    assert!(state.output_for_step(3).is_some());
    assert!(state.output_for_step(99).is_none());
}

// ── WorkflowStepOutput ────────────────────────────────────────────────────────

#[test]
fn r4_3_step_output_produces_tag() {
    let output =
        WorkflowStepOutput::success(0, vec!["diff".to_string(), "test_report".to_string()], None);
    assert!(output.produces_tag("diff"));
    assert!(output.produces_tag("test_report"));
    assert!(!output.produces_tag("deploy_log"));
}

// ── WorkflowStepObservation ───────────────────────────────────────────────────

#[test]
fn r4_3_observation_kind_as_str_values() {
    assert_eq!(
        WorkflowObservationKind::SkillConflict.as_str(),
        "skill_conflict"
    );
    assert_eq!(
        WorkflowObservationKind::PluginBlocked.as_str(),
        "plugin_blocked"
    );
    assert_eq!(
        WorkflowObservationKind::McpUnavailable.as_str(),
        "mcp_unavailable"
    );
    assert_eq!(
        WorkflowObservationKind::RetryBudgetExhausted.as_str(),
        "retry_budget_exhausted"
    );
    assert_eq!(
        WorkflowObservationKind::ApprovalGate.as_str(),
        "approval_gate"
    );
    assert_eq!(WorkflowObservationKind::Info.as_str(), "info");
}

#[test]
fn r4_3_observation_render_line_includes_step_and_kind() {
    let obs = WorkflowStepObservation {
        step_id: 2,
        kind: WorkflowObservationKind::McpUnavailable,
        message: "github timed out".to_string(),
    };
    let line = obs.render_line();
    assert!(line.contains("step=2"), "line: {line}");
    assert!(line.contains("mcp_unavailable"), "line: {line}");
    assert!(line.contains("github timed out"), "line: {line}");
}

#[test]
fn r4_3_state_observation_lines_renders_all() {
    let state = WorkflowStepState {
        completed_outputs: vec![],
        observations: vec![
            WorkflowStepObservation {
                step_id: 0,
                kind: WorkflowObservationKind::Info,
                message: "first obs".to_string(),
            },
            WorkflowStepObservation {
                step_id: 1,
                kind: WorkflowObservationKind::PluginBlocked,
                message: "ci plugin blocked".to_string(),
            },
        ],
    };
    let lines = state.observation_lines();
    assert_eq!(lines.len(), 2);
    assert!(lines[1].contains("plugin_blocked"));
}

// ── build_handoff_state ───────────────────────────────────────────────────────

#[test]
fn r4_3_handoff_state_includes_only_dep_outputs() {
    // Step 3 depends on steps 1 and 2 only (not step 0)
    let contract = make_contract(3, vec![], vec![1, 2]);

    let all_outputs = vec![
        succeeded_output(0),
        succeeded_output(1),
        succeeded_output(2),
    ];
    let all_obs = vec![];

    let handoff = build_handoff_state(&all_outputs, &all_obs, &contract);

    assert_eq!(handoff.completed_outputs.len(), 2);
    assert!(handoff.completed_outputs.iter().any(|o| o.step_id == 1));
    assert!(handoff.completed_outputs.iter().any(|o| o.step_id == 2));
    assert!(!handoff.completed_outputs.iter().any(|o| o.step_id == 0));
}

#[test]
fn r4_3_handoff_state_includes_only_dep_observations() {
    let contract = make_contract(2, vec![], vec![1]);

    let all_outputs = vec![succeeded_output(1)];
    let all_obs = vec![
        WorkflowStepObservation {
            step_id: 0, // not a dep — should be excluded
            kind: WorkflowObservationKind::Info,
            message: "irrelevant".to_string(),
        },
        WorkflowStepObservation {
            step_id: 1, // is a dep — should be included
            kind: WorkflowObservationKind::SkillConflict,
            message: "deploy shadowed".to_string(),
        },
    ];

    let handoff = build_handoff_state(&all_outputs, &all_obs, &contract);
    assert_eq!(handoff.observations.len(), 1);
    assert_eq!(handoff.observations[0].step_id, 1);
}

#[test]
fn r4_3_handoff_state_empty_when_no_deps() {
    let contract = make_contract(0, vec![], vec![]);
    let handoff = build_handoff_state(&[succeeded_output(99)], &[], &contract);

    assert!(handoff.completed_outputs.is_empty());
    assert!(handoff.observations.is_empty());
}

// ── serde round-trip ──────────────────────────────────────────────────────────

#[test]
fn r4_3_contract_serde_round_trip() {
    let contract = WorkflowStepContract {
        step_id: 4,
        kind: WorkflowStepKind::Composite,
        resources: vec![
            WorkflowStepResourceRef::skill("deploy"),
            WorkflowStepResourceRef::mcp("github"),
        ],
        expected_outputs: vec!["diff".to_string()],
        depends_on: vec![1, 2, 3],
        retry_budget: 2,
        requires_approval: true,
    };

    let json = serde_json::to_string(&contract).unwrap();
    let restored: WorkflowStepContract = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.step_id, 4);
    assert_eq!(restored.kind, WorkflowStepKind::Composite);
    assert_eq!(restored.resources.len(), 2);
    assert!(restored.requires_approval);
}

#[test]
fn r4_3_step_state_serde_round_trip() {
    let state = WorkflowStepState {
        completed_outputs: vec![WorkflowStepOutput::success(
            1,
            vec!["diff".to_string()],
            Some("3 files changed".to_string()),
        )],
        observations: vec![WorkflowStepObservation {
            step_id: 1,
            kind: WorkflowObservationKind::Info,
            message: "done".to_string(),
        }],
    };

    let json = serde_json::to_string(&state).unwrap();
    let restored: WorkflowStepState = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.completed_outputs.len(), 1);
    assert_eq!(restored.observations.len(), 1);
    assert_eq!(restored.observations[0].kind, WorkflowObservationKind::Info);
}

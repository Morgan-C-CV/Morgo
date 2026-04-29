use rust_agent::bootstrap::InteractionSurface;
use rust_agent::security::approval_protocol::{
    ApprovalDecision, ApprovalResolutionRecord, ApprovalSurface, BossStepApprovalGate,
    BossStepApprovalOutcome, PendingApprovalStatus, parse_approval_input,
    resolve_boss_step_approval,
};
use rust_agent::state::permission_context::PendingApproval;

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_pending(tool_name: &str, code: Option<&str>) -> PendingApproval {
    PendingApproval {
        tool_name: tool_name.to_string(),
        tool_input: r#"{"command":"rm -rf /tmp/test"}"#.to_string(),
        message: format!("bash command requires approval [{tool_name}]"),
        code: code.map(str::to_string),
        summary: Some("Bash pending approval".into()),
        detail: Some("destructive pattern detected".into()),
        approval_kind: Some("tool_permission".into()),
        escalation_reasons: vec!["destructive_pattern".into()],
    }
}

// ── ApprovalDecision ──────────────────────────────────────────────────────────

#[test]
fn r0_2_approval_decision_as_str() {
    assert_eq!(ApprovalDecision::Approved.as_str(), "approved");
    assert_eq!(ApprovalDecision::Denied.as_str(), "denied");
}

#[test]
fn r0_2_approval_decision_from_bool() {
    assert_eq!(
        ApprovalDecision::from_bool(true),
        ApprovalDecision::Approved
    );
    assert_eq!(ApprovalDecision::from_bool(false), ApprovalDecision::Denied);
}

// ── parse_approval_input ──────────────────────────────────────────────────────

#[test]
fn r0_2_parse_approval_input_yes_variants() {
    for input in &["yes", "YES", "Yes", "y", "Y", "approve", "APPROVE"] {
        assert_eq!(
            parse_approval_input(input),
            Some(ApprovalDecision::Approved),
            "input: {input}"
        );
    }
}

#[test]
fn r0_2_parse_approval_input_no_variants() {
    for input in &["no", "NO", "No", "n", "N", "deny", "DENY"] {
        assert_eq!(
            parse_approval_input(input),
            Some(ApprovalDecision::Denied),
            "input: {input}"
        );
    }
}

#[test]
fn r0_2_parse_approval_input_unrecognized_returns_none() {
    assert!(parse_approval_input("maybe").is_none());
    assert!(parse_approval_input("").is_none());
    assert!(parse_approval_input("ok").is_none());
    assert!(parse_approval_input("sure").is_none());
}

// ── ApprovalSurface ───────────────────────────────────────────────────────────

#[test]
fn r0_2_approval_surface_as_str() {
    assert_eq!(ApprovalSurface::Cli.as_str(), "cli");
    assert_eq!(ApprovalSurface::Telegram.as_str(), "telegram");
    assert_eq!(ApprovalSurface::Remote.as_str(), "remote");
    assert_eq!(ApprovalSurface::Unknown.as_str(), "unknown");
}

#[test]
fn r0_2_approval_surface_from_interaction_surface() {
    assert_eq!(
        ApprovalSurface::from_interaction_surface(InteractionSurface::Cli),
        ApprovalSurface::Cli
    );
    assert_eq!(
        ApprovalSurface::from_interaction_surface(InteractionSurface::Telegram),
        ApprovalSurface::Telegram
    );
    assert_eq!(
        ApprovalSurface::from_interaction_surface(InteractionSurface::Remote),
        ApprovalSurface::Remote
    );
}

// ── ApprovalResolutionRecord ──────────────────────────────────────────────────

#[test]
fn r0_2_resolution_record_from_pending_approved() {
    let pending = make_pending("Bash", Some("capability_escalation"));
    let record =
        ApprovalResolutionRecord::new(&pending, ApprovalDecision::Approved, ApprovalSurface::Cli);
    assert_eq!(record.tool_name, "Bash");
    assert_eq!(record.decision, ApprovalDecision::Approved);
    assert_eq!(record.surface, ApprovalSurface::Cli);
    assert_eq!(record.code.as_deref(), Some("capability_escalation"));
    assert_eq!(record.approval_kind.as_deref(), Some("tool_permission"));
    assert_eq!(record.escalation_reasons, vec!["destructive_pattern"]);
}

#[test]
fn r0_2_resolution_record_from_pending_denied() {
    let pending = make_pending("Bash", None);
    let record = ApprovalResolutionRecord::new(
        &pending,
        ApprovalDecision::Denied,
        ApprovalSurface::Telegram,
    );
    assert_eq!(record.decision, ApprovalDecision::Denied);
    assert_eq!(record.surface, ApprovalSurface::Telegram);
}

#[test]
fn r0_2_resolution_record_render_line_contains_key_fields() {
    let pending = make_pending("Bash", Some("policy_escalation"));
    let record = ApprovalResolutionRecord::new(
        &pending,
        ApprovalDecision::Approved,
        ApprovalSurface::Remote,
    );
    let line = record.render_line();
    assert!(line.contains("Bash"), "line: {line}");
    assert!(line.contains("approved"), "line: {line}");
    assert!(line.contains("remote"), "line: {line}");
    assert!(line.contains("policy_escalation"), "line: {line}");
}

// ── PendingApprovalStatus ─────────────────────────────────────────────────────

#[test]
fn r0_2_pending_approval_status_from_pending() {
    let pending = make_pending("Bash", Some("capability_escalation"));
    let status = PendingApprovalStatus::from_pending(&pending);
    assert_eq!(status.tool_name, "Bash");
    assert_eq!(status.code.as_deref(), Some("capability_escalation"));
    assert_eq!(status.approval_kind.as_deref(), Some("tool_permission"));
    assert!(!status.escalation_reasons.is_empty());
}

#[test]
fn r0_2_pending_approval_status_render_prompt_line_contains_yes_no() {
    let pending = make_pending("Bash", Some("capability_escalation"));
    let status = PendingApprovalStatus::from_pending(&pending);
    let line = status.render_prompt_line();
    assert!(line.contains("yes"), "line: {line}");
    assert!(line.contains("no"), "line: {line}");
    assert!(line.contains("capability_escalation"), "line: {line}");
}

#[test]
fn r0_2_pending_approval_status_render_telegram_prompt_contains_tool_and_reply() {
    let pending = make_pending("Bash", None);
    let status = PendingApprovalStatus::from_pending(&pending);
    let prompt = status.render_telegram_prompt();
    assert!(prompt.contains("Bash"), "prompt: {prompt}");
    assert!(prompt.contains("yes"), "prompt: {prompt}");
    assert!(prompt.contains("no"), "prompt: {prompt}");
    assert!(prompt.contains("destructive"), "prompt: {prompt}");
}

// ── BossStepApprovalGate ──────────────────────────────────────────────────────

#[test]
fn r0_2_boss_step_approval_gate_from_pending() {
    let pending = make_pending("Bash", Some("capability_escalation"));
    let gate = BossStepApprovalGate::new("step-3", &pending);
    assert_eq!(gate.step_id, "step-3");
    assert_eq!(gate.tool_name, "Bash");
    assert_eq!(gate.code.as_deref(), Some("capability_escalation"));
    assert!(!gate.escalation_reasons.is_empty());
}

#[test]
fn r0_2_boss_step_approval_gate_render_line() {
    let pending = make_pending("Bash", Some("policy_escalation"));
    let gate = BossStepApprovalGate::new("step-7", &pending);
    let line = gate.render_line();
    assert!(line.contains("step-7"), "line: {line}");
    assert!(line.contains("Bash"), "line: {line}");
    assert!(line.contains("policy_escalation"), "line: {line}");
}

// ── resolve_boss_step_approval ────────────────────────────────────────────────

#[test]
fn r0_2_resolve_boss_step_approval_approved() {
    let pending = make_pending("Bash", None);
    let gate = BossStepApprovalGate::new("step-1", &pending);
    let outcome = resolve_boss_step_approval(&gate, ApprovalDecision::Approved);
    assert_eq!(outcome.as_str(), "approved");
    assert_eq!(outcome.step_id(), "step-1");
    assert!(matches!(outcome, BossStepApprovalOutcome::Approved { .. }));
}

#[test]
fn r0_2_resolve_boss_step_approval_denied() {
    let pending = make_pending("Bash", None);
    let gate = BossStepApprovalGate::new("step-2", &pending);
    let outcome = resolve_boss_step_approval(&gate, ApprovalDecision::Denied);
    assert_eq!(outcome.as_str(), "denied");
    assert_eq!(outcome.step_id(), "step-2");
    if let BossStepApprovalOutcome::Denied { reason, .. } = &outcome {
        assert!(reason.contains("Bash"), "reason: {reason}");
    } else {
        panic!("expected Denied outcome");
    }
}

#[test]
fn r0_2_boss_step_approval_outcome_render_line_approved() {
    let outcome = BossStepApprovalOutcome::approved("step-5");
    let line = outcome.render_line();
    assert!(line.contains("step-5"), "line: {line}");
    assert!(line.contains("approved"), "line: {line}");
}

#[test]
fn r0_2_boss_step_approval_outcome_render_line_denied() {
    let outcome = BossStepApprovalOutcome::denied("step-6", "user denied");
    let line = outcome.render_line();
    assert!(line.contains("step-6"), "line: {line}");
    assert!(line.contains("denied"), "line: {line}");
    assert!(line.contains("user denied"), "line: {line}");
}

// ── serde round-trip ──────────────────────────────────────────────────────────

#[test]
fn r0_2_approval_resolution_record_serde_round_trip() {
    let pending = make_pending("Bash", Some("capability_escalation"));
    let record = ApprovalResolutionRecord::new(
        &pending,
        ApprovalDecision::Approved,
        ApprovalSurface::Telegram,
    );
    let json = serde_json::to_string(&record).unwrap();
    let restored: ApprovalResolutionRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.decision, ApprovalDecision::Approved);
    assert_eq!(restored.surface, ApprovalSurface::Telegram);
    assert_eq!(restored.tool_name, "Bash");
}

#[test]
fn r0_2_boss_step_approval_outcome_serde_round_trip() {
    let outcome = BossStepApprovalOutcome::denied("step-9", "user denied approval for Bash");
    let json = serde_json::to_string(&outcome).unwrap();
    let restored: BossStepApprovalOutcome = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.step_id(), "step-9");
    assert_eq!(restored.as_str(), "denied");
}

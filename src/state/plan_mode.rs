use crate::plan::types::{PlanState, PlanStepStatus};
use crate::state::permission_context::{PendingApproval, PermissionMode, ToolPermissionContext};
use crate::task::manager::TaskGroupSummary;
use crate::task::types::TaskRecord;
use crate::tool::definition::ToolResult;

pub fn render_plan_status(permissions: &ToolPermissionContext) -> String {
    let mode_line = if matches!(permissions.mode(), PermissionMode::Plan) {
        "Plan mode is on. Use /plan exit [summary] when ready to leave.".to_string()
    } else {
        "Plan mode is off. Use /plan enter [reason] to start planning.".to_string()
    };

    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        return mode_line;
    };
    let Some(state) = plan_manager.state() else {
        return format!("{mode_line}\nNo plan object exists for this session yet.");
    };

    format!("{mode_line}\n{}", summarize_plan_state(&state))
}

pub fn render_plan_show(permissions: &ToolPermissionContext) -> String {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        return "No plan manager is available in this session.".into();
    };
    let Some(state) = plan_manager.state() else {
        return "No plan object exists for this session yet.".into();
    };

    let linked_tasks = permissions
        .task_list_manager
        .as_ref()
        .map(|manager| manager.tasks_grouped_by_plan_step())
        .unwrap_or_default();
    let runtime_groups = permissions
        .task_manager
        .as_ref()
        .map(|manager| {
            if let Some(draft) = state.draft.as_ref() {
                draft
                    .steps
                    .iter()
                    .filter_map(|step| {
                        manager
                            .group_summary(&step.id)
                            .map(|group| (step.id.clone(), group))
                    })
                    .collect::<std::collections::BTreeMap<_, _>>()
            } else {
                std::collections::BTreeMap::new()
            }
        })
        .unwrap_or_default();
    let mut lines = vec![format!("Plan status: {}", state.status.as_str())];
    if let Some(execution) = state.execution.as_ref() {
        lines.push(format!(
            "Execution: {}/{} completed ({}%)",
            execution.completed_steps, execution.total_steps, execution.progress_percent
        ));
        if let Some(active_step_id) = execution.active_step_id.as_ref() {
            lines.push(format!("Active step: {active_step_id}"));
        }
    }
    if let Some(draft) = state.draft.as_ref() {
        let total_steps = draft.steps.len();
        let completed = draft
            .steps
            .iter()
            .filter(|step| step.status == PlanStepStatus::Completed)
            .count();
        let in_progress = draft
            .steps
            .iter()
            .filter(|step| step.status == PlanStepStatus::InProgress)
            .count();
        let pending = total_steps.saturating_sub(completed + in_progress);
        let linked_count = draft
            .steps
            .iter()
            .filter(|step| linked_tasks.contains_key(step.id.as_str()))
            .count();
        lines.push(format!(
            "Step summary: total={}, completed={}, in_progress={}, pending={}, linked={}, unlinked={}",
            total_steps,
            completed,
            in_progress,
            pending,
            linked_count,
            total_steps.saturating_sub(linked_count)
        ));
        if !runtime_groups.is_empty() {
            lines.push(format!(
                "Runtime orchestration: groups={}, waiting_for_verification={}, ready_for_synthesis={}, still_in_progress={}",
                runtime_groups.len(),
                runtime_groups
                    .values()
                    .filter(|group| group.hint.contains("waiting for verification"))
                    .count(),
                runtime_groups
                    .values()
                    .filter(|group| group.hint.contains("ready for synthesis"))
                    .count(),
                runtime_groups
                    .values()
                    .filter(|group| group.hint.contains("still in progress"))
                    .count()
            ));
        }
        if !draft.summary.trim().is_empty() {
            lines.push(format!("Summary: {}", draft.summary.trim()));
        }
        if let Some(notes) = draft
            .notes
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            lines.push(format!("Notes: {notes}"));
        }
        if draft.steps.is_empty() {
            lines.push("Steps: none".into());
        } else {
            lines.push("Steps:".into());
            for step in &draft.steps {
                lines.push(format_plan_step(
                    step,
                    linked_tasks.get(step.id.as_str()),
                    runtime_groups.get(step.id.as_str()),
                    permissions,
                ));
            }
        }
        let duplicate_links = linked_tasks
            .values()
            .filter(|tasks| tasks.len() > 1)
            .count();
        if duplicate_links > 0 {
            lines.push(format!(
                "Warnings: {} duplicated plan-step link(s) detected",
                duplicate_links
            ));
        }
    } else {
        lines.push("Draft: none".into());
    }

    if let Some(summary) = state
        .approval_summary
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Approval summary: {summary}"));
    }
    if let Some(approved_at) = state.approved_at.as_ref() {
        lines.push(format!("Approved at: {approved_at}"));
    }

    lines.join("\n")
}

pub fn render_plan_history(permissions: &ToolPermissionContext) -> String {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        return "No plan manager is available in this session.".into();
    };
    let history = plan_manager.history();
    if history.is_empty() {
        return "Plan history: none".into();
    }

    let mut lines = vec!["Plan history:".into()];
    for entry in history.iter().rev().take(10) {
        let step_count = entry
            .draft
            .as_ref()
            .map(|draft| draft.steps.len())
            .unwrap_or(0);
        let completed = entry
            .execution
            .as_ref()
            .map(|execution| execution.completed_steps)
            .unwrap_or(0);
        let active_step = entry
            .execution
            .as_ref()
            .and_then(|execution| execution.active_step_id.as_deref())
            .unwrap_or("none");
        let approval = entry
            .draft
            .as_ref()
            .map(|draft| draft.summary.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("(no summary)");
        lines.push(format!(
            "- {} [{}] {} — {}",
            entry.timestamp,
            entry.status.as_str(),
            entry.action,
            entry.summary
        ));
        lines.push(format!(
            "  snapshot: steps={}, completed={}, active_step={}, summary={}",
            step_count, completed, active_step, approval
        ));
    }
    if let Some(runtime_overlay) = render_history_runtime_overlay(permissions) {
        lines.push("Current runtime overlay:".into());
        lines.extend(runtime_overlay.into_iter().map(|line| format!("  {line}")));
    }
    lines.join("\n")
}

fn format_plan_step(
    step: &crate::plan::types::PlanStep,
    linked_tasks: Option<&Vec<crate::task::list_types::TaskListItem>>,
    runtime_group: Option<&TaskGroupSummary>,
    permissions: &ToolPermissionContext,
) -> String {
    let details = step
        .details
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| format!(" — {value}"))
        .unwrap_or_default();
    let mut lines = vec![format!(
        "- {}: {} [{}]{}",
        step.id,
        step.title,
        step.status.as_str(),
        details
    )];
    match linked_tasks {
        Some(tasks) if !tasks.is_empty() => {
            for task in tasks {
                let owner = task.owner.as_deref().unwrap_or("none");
                let blocks = if task.blocks.is_empty() {
                    "none".to_string()
                } else {
                    task.blocks.join(", ")
                };
                let blocked_by = if task.blocked_by.is_empty() {
                    "none".to_string()
                } else {
                    task.blocked_by.join(", ")
                };
                lines.push(format!(
                    "  linked task: {} [{}] owner={} blocked_by={} blocks={}",
                    task.id,
                    task_list_status_label(&task.status),
                    owner,
                    blocked_by,
                    blocks
                ));
                if !plan_task_status_matches(step.status, task.status.clone()) {
                    lines.push("  warning: plan/task status mismatch".into());
                }
            }
            if tasks.len() > 1 {
                lines.push("  warning: duplicate linked tasks for this step".into());
            }
        }
        _ => lines.push("  linked task: none".into()),
    }
    match runtime_group {
        Some(group) => {
            lines.push(format!(
                "  runtime group: {} — {}",
                group.group_id, group.hint
            ));
            for task in &group.tasks {
                lines.extend(
                    format_runtime_task_lines(task, permissions)
                        .into_iter()
                        .map(|line| format!("  {line}")),
                );
            }
        }
        None => lines.push("  runtime group: none".into()),
    }
    lines.join("\n")
}

fn format_runtime_task_lines(
    task: &TaskRecord,
    permissions: &ToolPermissionContext,
) -> Vec<String> {
    let hint = permissions
        .task_manager
        .as_ref()
        .map(|manager| manager.task_hint(task))
        .unwrap_or_else(|| "runtime hint unavailable".to_string());
    let mut lines = vec![format!(
        "runtime task: {} [{:?}] role={} phase={} validation_state={}",
        task.id,
        task.status,
        task.worker_role.map(|role| role.as_str()).unwrap_or("none"),
        task.phase.map(|phase| phase.as_str()).unwrap_or("none"),
        task.validation_state
            .map(|state| state.as_str())
            .unwrap_or("none")
    )];
    lines.push(format!("hint: {hint}"));
    if let Some(parent_task_id) = task.parent_task_id.as_deref() {
        lines.push(format!("parent_task_id: {parent_task_id}"));
    }
    lines
}

fn render_history_runtime_overlay(permissions: &ToolPermissionContext) -> Option<Vec<String>> {
    let plan_manager = permissions.plan_manager.as_ref()?;
    let state = plan_manager.state()?;
    let draft = state.draft.as_ref()?;
    let task_manager = permissions.task_manager.as_ref()?;
    let groups = draft
        .steps
        .iter()
        .filter_map(|step| task_manager.group_summary(&step.id))
        .collect::<Vec<_>>();
    if groups.is_empty() {
        return None;
    }
    Some(vec![
        format!("active_runtime_groups={}", groups.len()),
        format!(
            "waiting_for_verification_groups={}",
            groups
                .iter()
                .filter(|group| group.hint.contains("waiting for verification"))
                .count()
        ),
        format!(
            "ready_for_synthesis_groups={}",
            groups
                .iter()
                .filter(|group| group.hint.contains("ready for synthesis"))
                .count()
        ),
        format!(
            "still_in_progress_groups={}",
            groups
                .iter()
                .filter(|group| group.hint.contains("still in progress"))
                .count()
        ),
    ])
}

fn plan_task_status_matches(
    plan_status: PlanStepStatus,
    task_status: crate::task::list_types::TaskListStatus,
) -> bool {
    matches!(
        (plan_status, task_status),
        (
            PlanStepStatus::Pending,
            crate::task::list_types::TaskListStatus::Pending
        ) | (
            PlanStepStatus::InProgress,
            crate::task::list_types::TaskListStatus::InProgress
        ) | (
            PlanStepStatus::Completed,
            crate::task::list_types::TaskListStatus::Completed
        )
    )
}

fn task_list_status_label(status: &crate::task::list_types::TaskListStatus) -> &'static str {
    match status {
        crate::task::list_types::TaskListStatus::Pending => "pending",
        crate::task::list_types::TaskListStatus::InProgress => "in_progress",
        crate::task::list_types::TaskListStatus::Completed => "completed",
    }
}

pub fn add_plan_step(
    permissions: &ToolPermissionContext,
    title: &str,
    details: Option<&str>,
) -> anyhow::Result<String> {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        anyhow::bail!("No plan manager is available in this session.");
    };
    let step = plan_manager.add_step(title, details)?;
    Ok(format!("Added plan step {}: {}", step.id, step.title))
}

pub fn update_plan_step(
    permissions: &ToolPermissionContext,
    step_id: &str,
    title: Option<&str>,
    details: Option<Option<&str>>,
    status: Option<PlanStepStatus>,
) -> anyhow::Result<String> {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        anyhow::bail!("No plan manager is available in this session.");
    };
    plan_manager.update_step(step_id, title, details, status)?;
    Ok(format!("Updated plan step {step_id}"))
}

pub fn complete_plan_step(
    permissions: &ToolPermissionContext,
    step_id: &str,
) -> anyhow::Result<String> {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        anyhow::bail!("No plan manager is available in this session.");
    };
    plan_manager.mark_step_status(step_id, PlanStepStatus::Completed)?;
    Ok(format!("Completed plan step {step_id}"))
}

pub fn reorder_plan_steps(
    permissions: &ToolPermissionContext,
    ordered_ids: &[String],
) -> anyhow::Result<String> {
    let Some(plan_manager) = permissions.plan_manager.as_ref() else {
        anyhow::bail!("No plan manager is available in this session.");
    };
    plan_manager.reorder_steps(ordered_ids)?;
    Ok(format!("Reordered {} plan steps", ordered_ids.len()))
}

pub fn request_enter_plan_mode(permissions: &ToolPermissionContext, reason: &str) -> ToolResult {
    if matches!(permissions.mode(), PermissionMode::Plan) {
        return ToolResult::Text("Already in plan mode.".into());
    }

    let message = if reason.trim().is_empty() {
        "approve entering plan mode".to_string()
    } else {
        format!("approve entering plan mode: {}", reason.trim())
    };
    let approval = crate::tool::result::PendingApprovalPayload {
        code: Some("enter_plan_mode".into()),
        summary: "EnterPlanMode pending approval".into(),
        detail: Some(message.clone()),
        approval_kind: Some("plan_mode_transition".into()),
        escalation_reasons: Vec::new(),
    };
    permissions.set_pending_approval(Some(PendingApproval {
        tool_name: "EnterPlanMode".into(),
        tool_input: reason.trim().to_string(),
        message: message.clone(),
        code: approval.code.clone(),
        summary: Some(approval.summary.clone()),
        detail: approval.detail.clone(),
        approval_kind: approval.approval_kind.clone(),
        escalation_reasons: approval.escalation_reasons.clone(),
    }));

    ToolResult::PendingApproval {
        tool_name: "EnterPlanMode".into(),
        message,
        approval,
    }
}

pub fn request_exit_plan_mode(permissions: &ToolPermissionContext, summary: &str) -> ToolResult {
    if !matches!(permissions.mode(), PermissionMode::Plan) {
        return ToolResult::Denied("Plan mode is not active.".into());
    }

    if let Some(plan_manager) = permissions.plan_manager.as_ref() {
        let Some(state) = plan_manager.state() else {
            return ToolResult::Denied("No plan draft exists to approve.".into());
        };
        let is_empty = state
            .draft
            .as_ref()
            .map(|draft| draft.summary.trim().is_empty() && draft.steps.is_empty())
            .unwrap_or(true);
        if is_empty {
            return ToolResult::Denied("Cannot approve an empty plan draft.".into());
        }
    }

    let message = if summary.trim().is_empty() {
        "approve exiting plan mode".to_string()
    } else {
        format!("approve exiting plan mode: {}", summary.trim())
    };
    let approval = crate::tool::result::PendingApprovalPayload {
        code: Some("exit_plan_mode".into()),
        summary: "ExitPlanMode pending approval".into(),
        detail: Some(message.clone()),
        approval_kind: Some("plan_mode_transition".into()),
        escalation_reasons: Vec::new(),
    };
    permissions.set_pending_approval(Some(PendingApproval {
        tool_name: "ExitPlanMode".into(),
        tool_input: summary.trim().to_string(),
        message: message.clone(),
        code: approval.code.clone(),
        summary: Some(approval.summary.clone()),
        detail: approval.detail.clone(),
        approval_kind: approval.approval_kind.clone(),
        escalation_reasons: approval.escalation_reasons.clone(),
    }));

    ToolResult::PendingApproval {
        tool_name: "ExitPlanMode".into(),
        message,
        approval,
    }
}

pub fn apply_enter_plan_mode(permissions: &ToolPermissionContext, reason: &str) -> String {
    permissions.set_mode(PermissionMode::Plan);
    if let Some(plan_manager) = permissions.plan_manager.as_ref() {
        plan_manager.ensure_draft(non_empty(reason));
    }
    if reason.trim().is_empty() {
        "entered plan mode".into()
    } else {
        format!("entered plan mode: {}", reason.trim())
    }
}

pub fn apply_exit_plan_mode(
    permissions: &ToolPermissionContext,
    summary: &str,
) -> anyhow::Result<String> {
    if let Some(plan_manager) = permissions.plan_manager.as_ref() {
        let approved = plan_manager.approve(non_empty(summary))?;
        if let Some(task_list_manager) = permissions.task_list_manager.as_ref() {
            task_list_manager.sync_plan_state(&approved);
        }
    }
    permissions.set_mode(PermissionMode::Default);
    Ok(if summary.trim().is_empty() {
        "plan approved; exited plan mode".into()
    } else {
        format!("plan approved; exited plan mode: {}", summary.trim())
    })
}

fn summarize_plan_state(state: &PlanState) -> String {
    let summary = state
        .draft
        .as_ref()
        .map(|draft| draft.summary.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("(no summary yet)");
    let steps = state
        .draft
        .as_ref()
        .map(|draft| draft.steps.len())
        .unwrap_or(0);
    let progress = state
        .execution
        .as_ref()
        .map(|execution| execution.progress_percent)
        .unwrap_or(0);
    format!(
        "Plan object: status={}, summary={}, steps={}, progress={}%%",
        state.status.as_str(),
        summary,
        steps,
        progress
    )
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

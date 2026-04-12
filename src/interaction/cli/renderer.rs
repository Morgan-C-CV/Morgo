use crate::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};
use crate::task::types::TaskEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderDocument {
    blocks: Vec<RenderBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RenderBlock {
    PrimaryText(String),
    Panel(RenderPanel),
    RawRuntime(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderPanel {
    kind: PanelKind,
    title: String,
    lines: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PanelKind {
    Approval,
    TaskSummary,
    ToolResult,
}

pub fn render_output(output: &str) -> String {
    output.to_string()
}

pub fn render_turn_output(turn: &CliTurnOutput) -> String {
    render_document_to_text(&build_render_document(turn))
}

fn build_render_document(turn: &CliTurnOutput) -> RenderDocument {
    let mut blocks = Vec::new();
    if !turn.primary_text.is_empty() {
        blocks.push(RenderBlock::PrimaryText(turn.primary_text.clone()));
    }
    for event in &turn.events {
        blocks.push(render_block_for_event(event));
    }
    RenderDocument { blocks }
}

fn render_block_for_event(event: &CliDisplayEvent) -> RenderBlock {
    match event {
        CliDisplayEvent::TaskEvent(task_event) => RenderBlock::Panel(render_task_panel(task_event)),
        CliDisplayEvent::RuntimeEvent(runtime_event) => match runtime_event {
            CliRuntimeEvent::PendingApproval { tool_name, message } => {
                RenderBlock::Panel(RenderPanel {
                    kind: PanelKind::Approval,
                    title: "Approval required".into(),
                    lines: vec![format!("Tool: {tool_name}"), message.clone()],
                })
            }
            CliRuntimeEvent::ToolResult { tool_name, content } => {
                let mut lines = vec![format!("Tool: {tool_name}")];
                lines.extend(content.lines().map(|line| line.to_string()));
                RenderBlock::Panel(RenderPanel {
                    kind: PanelKind::ToolResult,
                    title: "Tool result".into(),
                    lines,
                })
            }
            other => RenderBlock::RawRuntime(other.to_legacy_line()),
        },
    }
}

fn render_task_panel(task_event: &TaskEvent) -> RenderPanel {
    RenderPanel {
        kind: PanelKind::TaskSummary,
        title: "Task update".into(),
        lines: vec![
            format!("[task] id: {}", task_event.task_id),
            format!("[task] summary: {}", task_event.summary),
            format!("[task] status: {:?}", task_event.status),
            format!("[task] result: {}", task_event.result),
            format!(
                "[task] worker_role: {}",
                task_event.worker_role.map(|role| role.as_str()).unwrap_or("none")
            ),
            format!(
                "[task] orchestration_group: {}",
                task_event.orchestration_group_id.as_deref().unwrap_or("none")
            ),
            format!(
                "[task] phase: {}",
                task_event.phase.map(|phase| phase.as_str()).unwrap_or("none")
            ),
            format!(
                "[task] validation_state: {}",
                task_event
                    .validation_state
                    .map(|state| state.as_str())
                    .unwrap_or("none")
            ),
            format!("[task] output: {}", task_event.output_file),
            format!("[task] next_action: {}", task_event.next_action),
        ],
    }
}

fn render_document_to_text(document: &RenderDocument) -> String {
    document
        .blocks
        .iter()
        .map(render_block_to_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn render_block_to_text(block: &RenderBlock) -> String {
    match block {
        RenderBlock::PrimaryText(text) => text.clone(),
        RenderBlock::RawRuntime(text) => text.clone(),
        RenderBlock::Panel(panel) => render_panel_to_text(panel),
    }
}

fn render_panel_to_text(panel: &RenderPanel) -> String {
    let mut lines = vec![format!("== {} ==", panel.title)];
    lines.extend(panel.lines.iter().map(|line| format!("  {line}")));
    lines.join("\n")
}

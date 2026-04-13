use crate::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};
use crate::task::types::TaskEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderDocument {
    pub blocks: Vec<RenderBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderBlock {
    PrimaryText(String),
    Panel(RenderPanel),
    RawRuntime(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderPanel {
    pub kind: PanelKind,
    pub title: String,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanelKind {
    Approval,
    Notice,
    TaskSummary,
    ToolResult,
}

pub fn render_output(output: &str) -> String {
    output.to_string()
}

pub fn render_turn_output(turn: &CliTurnOutput) -> String {
    render_document_to_text(&build_render_document(turn))
}

pub fn render_turn_document(turn: &CliTurnOutput) -> RenderDocument {
    build_render_document(turn)
}

pub fn render_document_output(document: &RenderDocument) -> String {
    render_document_to_text(document)
}

pub fn render_turn_tui_output(turn: &CliTurnOutput) -> String {
    render_document_to_tui_text(&build_render_document(turn))
}

pub fn render_document_tui_output(document: &RenderDocument) -> String {
    render_document_to_tui_text(document)
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
                RenderBlock::Panel(render_panel(
                    PanelKind::Approval,
                    "Approval required",
                    vec![format!("Tool: {tool_name}"), message.clone()],
                ))
            }
            CliRuntimeEvent::Notice { kind, message } => RenderBlock::Panel(render_panel(
                PanelKind::Notice,
                format!("Notice: {kind}"),
                vec![message.clone()],
            )),
            CliRuntimeEvent::ToolResult { tool_name, content } => {
                let mut lines = vec![format!("Tool: {tool_name}")];
                lines.extend(content.lines().map(|line| line.to_string()));
                RenderBlock::Panel(render_panel(PanelKind::ToolResult, "Tool result", lines))
            }
            other => RenderBlock::RawRuntime(other.to_legacy_line()),
        },
    }
}

fn render_task_panel(task_event: &TaskEvent) -> RenderPanel {
    render_panel(
        PanelKind::TaskSummary,
        "Task update",
        vec![
            format!("[task] id: {}", task_event.task_id),
            format!("[task] summary: {}", task_event.summary),
            format!("[task] status: {:?}", task_event.status),
            format!("[task] result: {}", task_event.result),
            format!(
                "[task] worker_role: {}",
                task_event
                    .worker_role
                    .map(|role| role.as_str())
                    .unwrap_or("none")
            ),
            format!(
                "[task] orchestration_group: {}",
                task_event
                    .orchestration_group_id
                    .as_deref()
                    .unwrap_or("none")
            ),
            format!(
                "[task] phase: {}",
                task_event
                    .phase
                    .map(|phase| phase.as_str())
                    .unwrap_or("none")
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
    )
}

fn render_panel(kind: PanelKind, title: impl Into<String>, lines: Vec<String>) -> RenderPanel {
    RenderPanel {
        kind,
        title: title.into(),
        lines,
    }
}

fn panel_marker(kind: PanelKind) -> &'static str {
    match kind {
        PanelKind::Approval => "approval",
        PanelKind::Notice => "notice",
        PanelKind::TaskSummary => "task",
        PanelKind::ToolResult => "tool",
    }
}

fn panel_header(title: &str) -> String {
    format!("== {title} ==")
}

fn panel_body_lines(lines: &[String]) -> Vec<String> {
    lines.iter().map(|line| format!("  {line}")).collect()
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

fn render_document_to_tui_text(document: &RenderDocument) -> String {
    let sections = document
        .blocks
        .iter()
        .map(render_block_to_tui_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>();

    if sections.is_empty() {
        return String::new();
    }

    let mut lines = vec!["╔════════════════ CLI TUI ════════════════".to_string()];
    lines.extend(sections.into_iter().flat_map(|section| {
        section
            .lines()
            .map(|line| format!("║ {line}"))
            .collect::<Vec<_>>()
    }));
    lines.push("╚═════════════════════════════════════════".to_string());
    lines.join("\n")
}

fn render_block_to_text(block: &RenderBlock) -> String {
    match block {
        RenderBlock::PrimaryText(text) => text.clone(),
        RenderBlock::RawRuntime(text) => text.clone(),
        RenderBlock::Panel(panel) => render_panel_to_text(panel),
    }
}

fn render_block_to_tui_text(block: &RenderBlock) -> String {
    match block {
        RenderBlock::PrimaryText(text) => render_tui_section("Primary", text.lines().collect()),
        RenderBlock::RawRuntime(text) => render_tui_section("Runtime", text.lines().collect()),
        RenderBlock::Panel(panel) => render_tui_section(
            &panel.title,
            panel.lines.iter().map(|line| line.as_str()).collect(),
        ),
    }
}

fn render_panel_to_text(panel: &RenderPanel) -> String {
    let mut lines = vec![panel_header(&panel.title)];
    lines.push(format!("  [panel:{}]", panel_marker(panel.kind)));
    lines.extend(panel_body_lines(&panel.lines));
    lines.join("\n")
}

fn render_tui_section(title: &str, lines: Vec<&str>) -> String {
    let mut section_lines = vec![format!("[{}]", title)];
    section_lines.extend(lines.into_iter().map(|line| format!("  {line}")));
    section_lines.join("\n")
}

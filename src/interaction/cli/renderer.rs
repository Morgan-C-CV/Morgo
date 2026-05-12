use crate::core::output::{OutputBlock, blocks_to_plain_text};
use crate::interaction::cli::repl::CliTurnOutput;
use crate::interaction::view::{SurfaceItem, SurfaceView, TaskView, build_surface_view};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderDocument {
    pub blocks: Vec<RenderBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuiScreen {
    pub main: Vec<String>,
    pub panels: Vec<TuiPanelSection>,
    pub prompt: Vec<String>,
    pub footer: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuiPanelSection {
    pub title: String,
    pub lines: Vec<String>,
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

pub fn render_output_blocks(blocks: &[OutputBlock]) -> String {
    blocks_to_plain_text(blocks)
}

pub fn render_turn_output(turn: &CliTurnOutput) -> String {
    render_document_to_text(&build_render_document(&build_surface_view(turn)))
}

pub fn render_turn_document(turn: &CliTurnOutput) -> RenderDocument {
    build_render_document(&build_surface_view(turn))
}

pub fn render_document_output(document: &RenderDocument) -> String {
    render_document_to_text(document)
}

pub fn render_turn_tui_output(turn: &CliTurnOutput) -> String {
    render_document_to_tui_text(&build_render_document(&build_surface_view(turn)))
}

pub fn render_document_tui_output(document: &RenderDocument) -> String {
    render_document_to_tui_text(document)
}

pub fn render_tui_screen_output(screen: &TuiScreen) -> String {
    render_tui_screen_to_text(screen)
}

pub fn build_tui_screen(document: &RenderDocument) -> TuiScreen {
    let mut main = Vec::new();
    let mut panels = Vec::new();

    for block in &document.blocks {
        match block {
            RenderBlock::PrimaryText(text) => {
                if !text.is_empty() {
                    main.extend(text.lines().map(|line| line.to_string()));
                }
            }
            RenderBlock::RawRuntime(text) => {
                if let Some(lines) = raw_runtime_lines_for_tui(text) {
                    panels.push(TuiPanelSection {
                        title: "Runtime".into(),
                        lines,
                    });
                }
            }
            RenderBlock::Panel(panel) => panels.push(TuiPanelSection {
                title: panel.title.clone(),
                lines: panel.lines.clone(),
            }),
        }
    }

    if main.is_empty() && panels.is_empty() {
        main = vec![
            "Welcome to RustAgent TUI.".into(),
            "Run a command or ask for help to populate this screen.".into(),
            "Try /help to inspect the current command surface.".into(),
        ];
    }

    TuiScreen {
        main,
        panels,
        prompt: vec![
            "Prompt".into(),
            "  > enter a request and press return".into(),
        ],
        footer: vec!["Controls: /exit, exit, or quit leaves the TUI.".into()],
    }
}

fn build_render_document(view: &SurfaceView) -> RenderDocument {
    let mut blocks = Vec::new();
    if !view.primary_text.is_empty() {
        blocks.push(RenderBlock::PrimaryText(view.primary_text.clone()));
    }
    for item in &view.items {
        blocks.push(render_block_for_surface_item(item));
    }
    RenderDocument { blocks }
}

fn render_block_for_surface_item(item: &SurfaceItem) -> RenderBlock {
    match item {
        SurfaceItem::TaskUpdate(task) => RenderBlock::Panel(render_task_panel(task)),
        SurfaceItem::ApprovalRequired {
            tool_name, message, ..
        } => RenderBlock::Panel(render_panel(
            PanelKind::Approval,
            "Approval required",
            vec![format!("Tool: {tool_name}"), message.clone()],
        )),
        SurfaceItem::RuntimeNotice { kind, message, .. } => RenderBlock::Panel(render_panel(
            PanelKind::Notice,
            format!("Notice: {kind}"),
            vec![message.clone()],
        )),
        SurfaceItem::ToolResult {
            tool_name, content, ..
        } => {
            let mut lines = vec![format!("Tool: {tool_name}")];
            lines.extend(content.lines().map(|line| line.to_string()));
            RenderBlock::Panel(render_panel(PanelKind::ToolResult, "Tool result", lines))
        }
        other => RenderBlock::RawRuntime(other.to_legacy_line()),
    }
}

fn raw_runtime_lines_for_tui(text: &str) -> Option<Vec<String>> {
    if text.is_empty() {
        return None;
    }

    let lines = text.lines().map(|line| line.to_string()).collect::<Vec<_>>();
    if lines.is_empty() || lines.iter().all(|line| line.starts_with("[delta]")) {
        return None;
    }

    Some(lines)
}

fn render_task_panel(task_event: &TaskView) -> RenderPanel {
    render_panel(
        PanelKind::TaskSummary,
        "Task update",
        vec![
            format!("[task] id: {}", task_event.task_id),
            format!("[task] summary: {}", task_event.summary),
            format!("[task] status: {}", title_case_label(task_event.status)),
            format!("[task] result: {}", task_event.result),
            format!(
                "[task] worker_role: {}",
                task_event.worker_role.unwrap_or("none")
            ),
            format!(
                "[task] orchestration_group: {}",
                task_event
                    .orchestration_group_id
                    .as_deref()
                    .unwrap_or("none")
            ),
            format!("[task] phase: {}", task_event.phase.unwrap_or("none")),
            format!(
                "[task] validation_state: {}",
                task_event.validation_state.unwrap_or("none")
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
    render_tui_screen_to_text(&build_tui_screen(document))
}

fn render_tui_screen_to_text(screen: &TuiScreen) -> String {
    let sections = render_tui_screen_sections(screen);

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

fn render_panel_to_text(panel: &RenderPanel) -> String {
    let mut lines = vec![panel_header(&panel.title)];
    lines.push(format!("  [panel:{}]", panel_marker(panel.kind)));
    lines.extend(panel_body_lines(&panel.lines));
    lines.join("\n")
}

fn render_tui_screen_sections(screen: &TuiScreen) -> Vec<String> {
    let mut sections = Vec::new();
    if !screen.main.is_empty() {
        sections.push(render_tui_section(
            "Main",
            screen.main.iter().map(|line| line.as_str()).collect(),
        ));
    }
    for panel in &screen.panels {
        sections.push(render_tui_section(
            &panel.title,
            panel.lines.iter().map(|line| line.as_str()).collect(),
        ));
    }
    if !screen.prompt.is_empty() {
        sections.push(render_tui_section(
            "Prompt",
            screen.prompt.iter().map(|line| line.as_str()).collect(),
        ));
    }
    if !screen.footer.is_empty() {
        sections.push(render_tui_section(
            "Footer",
            screen.footer.iter().map(|line| line.as_str()).collect(),
        ));
    }
    sections
}

fn render_tui_section(title: &str, lines: Vec<&str>) -> String {
    let mut section_lines = vec![format!("[{}]", title)];
    section_lines.extend(lines.into_iter().map(|line| format!("  {line}")));
    section_lines.join("\n")
}

fn title_case_label(label: &str) -> String {
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

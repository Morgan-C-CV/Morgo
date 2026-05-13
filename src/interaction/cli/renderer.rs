use crate::core::output::{OutputBlock, blocks_to_plain_text};
use crate::interaction::cli::repl::CliTurnOutput;
use crate::interaction::view::{SurfaceItem, SurfaceView, TaskView, build_surface_view};
use serde_json::Value;

const MAX_TOOL_DETAIL_LINES: usize = 8;
const MAX_TOOL_DETAIL_WIDTH: usize = 100;

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
    ToolActivity,
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

pub fn build_tui_loading_screen(request: &str, _frame_index: usize) -> TuiScreen {
    let request = truncate_for_tui(request, 96);

    TuiScreen {
        main: vec!["Working...".into(), "The agent is processing your request.".into()],
        panels: vec![TuiPanelSection {
            title: "Status".into(),
            lines: vec![
                "State: waiting for model response".into(),
                format!("Request: {request}"),
            ],
        }],
        prompt: vec![],
        footer: vec![],
    }
}

pub fn build_tui_screen(document: &RenderDocument) -> TuiScreen {
    let mut main = Vec::new();
    let mut panel_entries = Vec::new();

    for (index, block) in document.blocks.iter().enumerate() {
        match block {
            RenderBlock::PrimaryText(text) => {
                if !text.is_empty() {
                    main.extend(text.lines().map(|line| line.to_string()));
                }
            }
            RenderBlock::RawRuntime(text) => {
                if let Some(lines) = raw_runtime_lines_for_tui(text) {
                    panel_entries.push((
                        panel_priority(None),
                        index,
                        TuiPanelSection {
                            title: "Runtime".into(),
                            lines,
                        },
                    ));
                }
            }
            RenderBlock::Panel(panel) => panel_entries.push((
                panel_priority(Some(panel.kind)),
                index,
                TuiPanelSection {
                    title: panel.title.clone(),
                    lines: panel.lines.clone(),
                },
            )),
        }
    }

    panel_entries.sort_by_key(|(priority, index, _)| (*priority, *index));
    let panels = panel_entries
        .into_iter()
        .map(|(_, _, panel)| panel)
        .collect::<Vec<_>>();

    if main.is_empty() && panels.is_empty() {
        main = vec![
            "Morgo is ready for coding tasks.".into(),
            "Ask me to inspect code, edit files, or run verification commands.".into(),
            "Use /help to see commands if needed, or /exit to leave the TUI.".into(),
        ];
    }

    TuiScreen {
        main,
        panels,
        prompt: vec![],
        footer: vec![],
    }
}

fn panel_priority(kind: Option<PanelKind>) -> u8 {
    match kind {
        Some(PanelKind::Approval) => 0,
        Some(PanelKind::ToolActivity) => 1,
        Some(PanelKind::ToolResult) => 2,
        Some(PanelKind::TaskSummary) => 3,
        Some(PanelKind::Notice) => 4,
        None => 5,
    }
}

fn build_render_document(view: &SurfaceView) -> RenderDocument {
    let mut blocks = Vec::new();
    if !view.primary_text.is_empty() {
        blocks.push(RenderBlock::PrimaryText(view.primary_text.clone()));
    }
    if let Some(activity_panel) = build_tool_activity_panel(&view.items) {
        blocks.push(RenderBlock::Panel(activity_panel));
    }
    for item in &view.items {
        if let Some(block) = render_block_for_surface_item(item) {
            blocks.push(block);
        }
    }
    RenderDocument { blocks }
}

fn render_block_for_surface_item(item: &SurfaceItem) -> Option<RenderBlock> {
    match item {
        SurfaceItem::TaskUpdate(task) => Some(RenderBlock::Panel(render_task_panel(task))),
        SurfaceItem::ApprovalRequired {
            tool_name,
            message,
            detail,
            ..
        } => Some(RenderBlock::Panel(render_approval_panel(
            tool_name,
            message,
            detail.as_deref(),
        ))),
        SurfaceItem::RuntimeNotice { .. } => None,
        SurfaceItem::ToolCallStarted { .. }
        | SurfaceItem::ToolResult { .. }
        | SurfaceItem::AssistantDelta { .. } => None,
        SurfaceItem::Transition { .. }
        | SurfaceItem::Terminal { .. }
        | SurfaceItem::SessionMilestone { .. } => None,
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

fn render_approval_panel(tool_name: &str, message: &str, detail: Option<&str>) -> RenderPanel {
    let mut lines = vec![format!("Tool: {tool_name}")];
    let mut reason = None;
    let mut action = None;

    if let Some(detail) = detail {
        for raw_line in detail.lines().map(str::trim).filter(|line| !line.is_empty()) {
            if raw_line.starts_with("Reason:") {
                reason = Some(raw_line.to_string());
            } else if raw_line.starts_with("Choose ") || raw_line.starts_with("Action:") {
                action = Some(format!(
                    "Action: {}",
                    raw_line
                        .trim_start_matches("Action:")
                        .trim_start_matches("Choose ")
                        .trim()
                ));
            }
        }
    }

    lines.push(reason.unwrap_or_else(|| format!("Reason: {message}")));
    lines.push(action.unwrap_or_else(|| "Action: approve or deny".into()));

    render_panel(PanelKind::Approval, "Approval required", lines)
}

fn build_tool_activity_panel(items: &[SurfaceItem]) -> Option<RenderPanel> {
    let mut exploration = Vec::new();
    let mut lines = Vec::new();

    for item in items {
        match item {
            SurfaceItem::ToolCallStarted { tool_name, input } => {
                if let Some(line) = tool_call_activity_line(tool_name, input) {
                    if is_exploration_tool(tool_name) {
                        if exploration.last() != Some(&line) {
                            exploration.push(line);
                        }
                    } else {
                        lines.push(format!("• {line}"));
                    }
                }
            }
            SurfaceItem::ToolResult {
                tool_name,
                content,
                summary,
                detail,
            } => {
                if let Some((headline, detail_lines)) =
                    tool_result_activity_block(tool_name, content, summary.as_deref(), detail.as_deref())
                {
                    let detail_lines = detail_lines
                        .into_iter()
                        .filter(|line| !is_low_signal_tool_detail(line))
                        .collect::<Vec<_>>();
                    if !headline.trim().is_empty() {
                        lines.push(format!("• {headline}"));
                    }
                    for detail_line in detail_lines {
                        lines.push(format!("  └ {detail_line}"));
                    }
                }
            }
            _ => {}
        }
    }

    if !exploration.is_empty() {
        let exploration_len = exploration.len();
        let mut prefixed = vec!["Explored".into()];
        for (index, line) in exploration.into_iter().enumerate() {
            let branch = if index + 1 == exploration_len { "└" } else { "├" };
            prefixed.push(format!("  {branch} {line}"));
        }
        prefixed.extend(lines);
        return Some(render_panel(PanelKind::ToolActivity, "Activity", prefixed));
    }

    if lines.is_empty() {
        None
    } else {
        Some(render_panel(PanelKind::ToolActivity, "Activity", lines))
    }
}

fn is_exploration_tool(tool_name: &str) -> bool {
    matches!(tool_name, "Read" | "Grep" | "Glob" | "ToolSearch" | "WebSearch" | "WebFetch")
}

fn tool_call_activity_line(tool_name: &str, input: &str) -> Option<String> {
    let parsed = serde_json::from_str::<Value>(input).ok();
    match tool_name {
        "Read" => {
            let path = json_string_field(parsed.as_ref(), &["path", "file_path"])?;
            Some(format!("Read {}", short_path(&path)))
        }
        "Grep" => {
            let pattern = json_string_field(parsed.as_ref(), &["pattern", "query"])?;
            let path = json_string_field(parsed.as_ref(), &["path"])
                .map(|value| format!(" in {}", short_path(&value)))
                .unwrap_or_default();
            Some(format!("Search {}{}", truncate_for_tui(&pattern, 72), path))
        }
        "Glob" => {
            let pattern = json_string_field(parsed.as_ref(), &["pattern", "glob"])
                .or_else(|| json_string_field(parsed.as_ref(), &["path"]))?;
            Some(format!("List {}", truncate_for_tui(&pattern, 72)))
        }
        "ToolSearch" | "WebSearch" => {
            let query = json_string_field(parsed.as_ref(), &["query", "q"])?;
            Some(format!("Search {}", truncate_for_tui(&query, 72)))
        }
        "WebFetch" => {
            let url = json_string_field(parsed.as_ref(), &["url"])?;
            Some(format!("Fetched {}", truncate_for_tui(&url, 72)))
        }
        "Bash" => {
            let command = json_string_field(parsed.as_ref(), &["command", "cmd"])?;
            Some(format!("Ran {}", truncate_for_tui(&command, 72)))
        }
        "Edit" | "Write" | "FileEdit" | "FileWrite" => {
            let path = json_string_field(parsed.as_ref(), &["path", "file_path"])?;
            Some(format!("Updated {}", short_path(&path)))
        }
        _ => Some(format!("Used {tool_name}")),
    }
}

fn tool_result_activity_block(
    tool_name: &str,
    content: &str,
    summary: Option<&str>,
    detail: Option<&str>,
) -> Option<(String, Vec<String>)> {
    let summary = summary.map(str::trim).filter(|value| !value.is_empty())?;
    if is_exploration_tool(tool_name) {
        return None;
    }

    let headline = match tool_name {
        "Bash" => summary
            .strip_suffix(" succeeded")
            .map(|value| format!("Ran {}", truncate_for_tui(value, 72)))
            .unwrap_or_else(|| truncate_for_tui(summary, 72)),
        "Edit" | "Write" | "FileEdit" | "FileWrite" => truncate_for_tui(summary, 72),
        _ => truncate_for_tui(summary, 72),
    };

    let detail_source = detail.unwrap_or(content);
    let detail_lines = if tool_name == "Bash" {
        summarize_bash_activity_detail(detail_source)
    } else {
        compact_tool_detail_lines(detail_source.lines().map(|line| line.to_string()).collect())
    };

    Some((headline, detail_lines))
}

fn summarize_bash_activity_detail(content: &str) -> Vec<String> {
    let lines = render_bash_result_lines(content)
        .into_iter()
        .filter(|line| !line.starts_with("Command:"))
        .collect::<Vec<_>>();
    compact_tool_detail_lines(lines)
}

fn is_low_signal_tool_detail(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty()
        || trimmed == "..."
        || trimmed.starts_with("Path: ")
        || trimmed.starts_with("Offset: ")
        || trimmed.starts_with("Returned chars: ")
        || trimmed.starts_with("Replacements: ")
        || trimmed.starts_with("Replace all: ")
        || trimmed.starts_with("Old text: ")
        || trimmed.starts_with("New text: ")
}

fn render_bash_result_lines(content: &str) -> Vec<String> {
    content
        .lines()
        .map(|line| {
            if let Some(command) = line.strip_prefix("command:") {
                format!("Command: {}", command.trim())
            } else if let Some(exit_code) = line.strip_prefix("exit_code:") {
                format!("Exit code: {}", exit_code.trim())
            } else {
                line.to_string()
            }
        })
        .collect()
}

fn compact_tool_detail_lines(lines: Vec<String>) -> Vec<String> {
    let cleaned = lines
        .into_iter()
        .map(|line| truncate_for_tui(line.trim(), MAX_TOOL_DETAIL_WIDTH))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();

    if cleaned.len() <= MAX_TOOL_DETAIL_LINES {
        return cleaned;
    }

    let mut truncated = cleaned
        .into_iter()
        .take(MAX_TOOL_DETAIL_LINES)
        .collect::<Vec<_>>();
    truncated.push("...".into());
    truncated
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
        PanelKind::ToolActivity => "activity",
        PanelKind::ToolResult => "tool",
    }
}

fn build_tui_footer(document: &RenderDocument) -> Vec<String> {
    let cwd = std::env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| ".".into());

    let mut footer = vec![format!("Status: cwd: {cwd} | mode: default")];

    if let Some(tool_name) = pending_approval_tool_name(document) {
        footer.push(format!("Pending approval: {tool_name}"));
    }

    footer.push("Controls: /exit, exit, or quit leaves the TUI.".into());
    footer
}

fn pending_approval_tool_name(document: &RenderDocument) -> Option<String> {
    document.blocks.iter().find_map(|block| match block {
        RenderBlock::Panel(panel) if panel.kind == PanelKind::Approval => panel
            .lines
            .iter()
            .find_map(|line| line.strip_prefix("Tool: ").map(|tool| tool.trim().to_string())),
        _ => None,
    })
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
    let mut sections = Vec::new();

    if !screen.main.is_empty() {
        sections.push(screen.main.join("\n"));
    }

    let boxed_sections = render_tui_boxed_sections(screen);
    if !boxed_sections.is_empty() {
        let mut lines = vec!["╔════════════════ CLI TUI ════════════════".to_string()];
        lines.extend(boxed_sections.into_iter().flat_map(|section| {
            section
                .lines()
                .map(|line| format!("║ {line}"))
                .collect::<Vec<_>>()
        }));
        lines.push("╚═════════════════════════════════════════".to_string());
        sections.push(lines.join("\n"));
    }

    sections.join("\n\n")
}

fn truncate_for_tui(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn json_string_field(value: Option<&Value>, keys: &[&str]) -> Option<String> {
    let object = value?.as_object()?;
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
    })
}

fn short_path(path: &str) -> String {
    std::path::Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| truncate_for_tui(path, 72))
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

fn render_tui_boxed_sections(screen: &TuiScreen) -> Vec<String> {
    let mut sections = Vec::new();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};

    #[test]
    fn tui_output_omits_streaming_delta_noise() {
        let turn = CliTurnOutput {
            primary_text: "final answer".into(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
                    text: "morg".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
                    text: "o".into(),
                }),
            ],
        };

        let rendered = render_turn_tui_output(&turn);
        assert!(rendered.contains("final answer"));
        assert!(rendered.starts_with("final answer"));
        assert!(!rendered.contains("[delta]"));
        assert!(!rendered.contains("  morg"));
        assert!(!rendered.contains("  o"));
        assert!(!rendered.contains("[Prompt]"));
        assert!(!rendered.contains("[Footer]"));
    }

    #[test]
    fn tui_tool_result_uses_summary_and_truncates_detail() {
        let long_detail = [
            "command: cargo test --package agent --lib -- interaction::cli::renderer",
            "exit_code: 0",
            "line-1",
            "line-2",
            "line-3",
            "line-4",
            "line-5",
            "line-6",
            "line-7",
            "line-8",
            "line-9",
        ]
        .join("\n");
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Bash".into(),
                    input: r#"{"command":"cargo test -- --nocapture","timeout_ms":120000}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                    tool_name: "Bash".into(),
                    content: long_detail.clone(),
                    summary: Some("cargo test passed".into()),
                    detail: Some(long_detail),
                }),
            ],
        };

        let rendered = render_turn_tui_output(&turn);
        assert!(rendered.contains("[Activity]"));
        assert!(rendered.contains("• Ran cargo test -- --nocapture"));
        assert!(rendered.contains("Exit code: 0"));
        assert!(!rendered.contains("\"timeout_ms\":120000"));
    }

    #[test]
    fn tui_groups_exploration_activity() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"/tmp/renderer.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"/tmp/renderer.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"delta|tool use","path":"/tmp/reference"}"#.into(),
                }),
            ],
        };

        let rendered = render_turn_tui_output(&turn);
        assert!(rendered.contains("Explored"));
        assert!(rendered.contains("Read renderer.rs"));
        assert!(rendered.contains("Search delta|tool use in reference"));
        assert_eq!(rendered.matches("Read renderer.rs").count(), 1);
    }

    #[test]
    fn tui_filters_runtime_notices() {
        let turn = CliTurnOutput {
            primary_text: "answer".into(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Notice {
                kind: "usage".into(),
                message: "recorded usage".into(),
                code: None,
                runtime_kind: None,
                service_failure_code: None,
                provider_kind: None,
                status_code: None,
                retryable: None,
                surface_visible: None,
            })],
        };

        let rendered = render_turn_tui_output(&turn);
        assert!(rendered.contains("answer"));
        assert!(!rendered.contains("recorded usage"));
        assert!(!rendered.contains("Notice:"));
    }
}

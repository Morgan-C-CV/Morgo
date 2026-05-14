use crate::core::message::is_legacy_hidden_primary_line;
use crate::core::output::{OutputBlock, blocks_to_plain_text};
use crate::interaction::cli::repl::CliTurnOutput;
use crate::interaction::view::{SurfaceItem, SurfaceView, TaskView, build_surface_view};
use serde_json::Value;
use std::path::PathBuf;

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
        main: vec![
            "Working...".into(),
            "The agent is processing your request.".into(),
        ],
        panels: vec![TuiPanelSection {
            title: "Status".into(),
            lines: vec![
                "State: waiting for model response".into(),
                format!("Request: {request}"),
            ],
        }],
        prompt: vec![format!("> waiting for response: {request}")],
        footer: vec![],
    }
}

pub fn build_tui_screen(document: &RenderDocument) -> TuiScreen {
    let mut main = Vec::new();
    let mut panel_entries = Vec::new();

    for (index, block) in document.blocks.iter().enumerate() {
        match block {
            RenderBlock::PrimaryText(text) => {
                let visible_lines = visible_tui_primary_lines(text);
                if !visible_lines.is_empty() {
                    main.extend(visible_lines);
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
        prompt: vec!["> ".into()],
        footer: vec![],
    }
}

fn visible_tui_primary_lines(text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut previous_blank = true;

    for raw_line in text.lines() {
        if is_hidden_tui_primary_line(raw_line) {
            continue;
        }

        let line = raw_line.to_string();
        let is_blank = line.trim().is_empty();
        if is_blank && previous_blank {
            continue;
        }

        previous_blank = is_blank;
        lines.push(line);
    }

    while lines
        .last()
        .map(|line| line.trim().is_empty())
        .unwrap_or(false)
    {
        lines.pop();
    }

    lines
}

fn is_hidden_tui_primary_line(line: &str) -> bool {
    is_legacy_hidden_primary_line(line)
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

    let lines = text
        .lines()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
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
    let mut run = None;
    let mut reason = None;
    let mut action = None;

    if let Some(detail) = detail {
        for raw_line in detail
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            if let Some(value) = approval_detail_value(raw_line, &["Run", "Command"]) {
                run = Some(format!("Run: {value}"));
            } else if let Some(value) = approval_detail_value(raw_line, &["Reason"]) {
                reason = Some(format!("Reason: {value}"));
            } else if let Some(value) = approval_detail_value(raw_line, &["Action", "NextStep"]) {
                action = Some(format!("Action: {value}"));
            } else if raw_line.starts_with("Choose ") {
                action = Some(format!(
                    "Action: {}",
                    raw_line.trim_start_matches("Choose ").trim()
                ));
            }
        }
    }

    if let Some(run) = run {
        lines.push(run);
    }
    lines.push(reason.unwrap_or_else(|| format!("Reason: {message}")));
    lines.push(action.unwrap_or_else(|| "Action: choose an approval option below".into()));

    render_panel(PanelKind::Approval, "Approval required", lines)
}

fn approval_detail_value<'a>(line: &'a str, keys: &[&str]) -> Option<&'a str> {
    let (raw_key, value) = line.split_once(':')?;
    let normalized_key = raw_key
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .collect::<String>();
    keys.iter()
        .any(|key| normalized_key.eq_ignore_ascii_case(key))
        .then_some(value.trim())
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
                if let Some((headline, detail_lines)) = tool_result_activity_block(
                    tool_name,
                    content,
                    summary.as_deref(),
                    detail.as_deref(),
                ) {
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
        let mut prefixed = vec![style_activity_action("EXPLORED")];
        for (index, line) in exploration.into_iter().enumerate() {
            let branch = if index + 1 == exploration_len {
                "└"
            } else {
                "├"
            };
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
    matches!(
        tool_name,
        "Read" | "Grep" | "Glob" | "ToolSearch" | "WebSearch" | "WebFetch"
    )
}

fn tool_call_activity_line(tool_name: &str, input: &str) -> Option<String> {
    let parsed = serde_json::from_str::<Value>(input).ok();
    match tool_name {
        "Read" => {
            let path = json_string_field(parsed.as_ref(), &["path", "file_path"])?;
            Some(format!(
                "{} {}",
                style_activity_action("READ"),
                short_path(&path)
            ))
        }
        "Grep" => {
            let pattern = json_string_field(parsed.as_ref(), &["pattern", "query"])?;
            let path = json_string_field(parsed.as_ref(), &["path"])
                .map(|value| format!(" in {}", short_path(&value)))
                .unwrap_or_default();
            Some(format!(
                "{} {}{}",
                style_activity_action("SEARCH"),
                truncate_for_tui(&pattern, 72),
                path
            ))
        }
        "Glob" => {
            let pattern = json_string_field(parsed.as_ref(), &["pattern", "glob"])
                .or_else(|| json_string_field(parsed.as_ref(), &["path"]))?;
            Some(format!(
                "{} {}",
                style_activity_action("LIST"),
                truncate_for_tui(&pattern, 72)
            ))
        }
        "ToolSearch" | "WebSearch" => {
            let query = json_string_field(parsed.as_ref(), &["query", "q"])?;
            Some(format!(
                "{} {}",
                style_activity_action("SEARCH"),
                truncate_for_tui(&query, 72)
            ))
        }
        "WebFetch" => {
            let url = json_string_field(parsed.as_ref(), &["url"])?;
            Some(format!(
                "{} {}",
                style_activity_action("FETCHED"),
                truncate_for_tui(&url, 72)
            ))
        }
        "Bash" => {
            let command = json_string_field(parsed.as_ref(), &["command", "cmd"])?;
            Some(format!(
                "{} {}",
                style_activity_action("RAN"),
                truncate_for_tui(&command, 72)
            ))
        }
        "Edit" | "Write" | "FileEdit" | "FileWrite" => {
            let path = json_string_field(parsed.as_ref(), &["path", "file_path"])?;
            Some(format!(
                "{} {}",
                style_activity_action("UPDATED"),
                short_path(&path)
            ))
        }
        _ => Some(format!("{} {tool_name}", style_activity_action("USED"))),
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

    if matches!(tool_name, "Edit" | "FileEdit") {
        return render_edit_activity_block(content, detail);
    }

    let headline = match tool_name {
        "Bash" => summary
            .strip_suffix(" succeeded")
            .map(|value| {
                format!(
                    "{} {}",
                    style_activity_action("RAN"),
                    truncate_for_tui(value, 72)
                )
            })
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

fn render_edit_activity_block(
    content: &str,
    detail: Option<&str>,
) -> Option<(String, Vec<String>)> {
    let detail_source = detail.unwrap_or(content);
    let fields = parse_key_value_lines(detail_source);
    let path = fields.get("path")?;
    let old_text =
        decode_tool_preview_text(fields.get("old_text").map(String::as_str).unwrap_or(""));
    let new_text =
        decode_tool_preview_text(fields.get("new_text").map(String::as_str).unwrap_or(""));

    let old_count = count_nonempty_lines(&old_text);
    let new_count = count_nonempty_lines(&new_text);
    let display_path = display_activity_path(path);
    let headline = format!(
        "{} {} ({} {})",
        style_activity_action("EDITED"),
        display_path,
        style_activity_added_count(new_count),
        style_activity_removed_count(old_count),
    );

    let detail_lines = render_edit_diff_lines(path, &old_text, &new_text);
    Some((headline, detail_lines))
}

fn render_edit_diff_lines(path: &str, old_text: &str, new_text: &str) -> Vec<String> {
    let file_text = std::fs::read_to_string(path).ok();
    let new_lines = split_preserve_empty(new_text);
    let old_lines = split_preserve_empty(old_text);
    let start_line = file_text
        .as_deref()
        .and_then(|text| locate_line_number(text, new_text))
        .unwrap_or(1);

    let width = (start_line + old_lines.len().max(new_lines.len()) + 1)
        .to_string()
        .len()
        .max(3);
    let mut rendered = Vec::new();

    for (idx, line) in old_lines.iter().enumerate() {
        rendered.push(style_removed_diff_line(start_line + idx, width, line));
    }
    for (idx, line) in new_lines.iter().enumerate() {
        rendered.push(style_added_diff_line(start_line + idx, width, line));
    }

    if rendered.is_empty() {
        rendered.push(style_added_diff_line(
            start_line,
            width,
            &truncate_for_tui(new_text, 96),
        ));
    }

    rendered
}

fn parse_key_value_lines(text: &str) -> std::collections::BTreeMap<String, String> {
    let mut fields = std::collections::BTreeMap::new();
    for line in text.lines() {
        if let Some((key, value)) = line.split_once('=') {
            fields.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    fields
}

fn decode_tool_preview_text(value: &str) -> String {
    value.replace("\\n", "\n")
}

fn split_preserve_empty(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    text.lines().map(|line| line.to_string()).collect()
}

fn locate_line_number(file_text: &str, snippet: &str) -> Option<usize> {
    if snippet.trim().is_empty() {
        return None;
    }

    let byte_index = file_text.find(snippet)?;
    Some(
        file_text[..byte_index]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1,
    )
}

fn count_nonempty_lines(text: &str) -> usize {
    let count = text.lines().count();
    if count == 0 {
        usize::from(!text.is_empty())
    } else {
        count
    }
}

fn display_activity_path(path: &str) -> String {
    current_dir_relative_path(path).unwrap_or_else(|| path.to_string())
}

fn current_dir_relative_path(path: &str) -> Option<String> {
    let current_dir = std::env::current_dir().ok()?;
    let absolute = PathBuf::from(path);
    absolute
        .strip_prefix(current_dir)
        .ok()
        .map(|relative| relative.display().to_string())
}

fn style_activity_added_count(count: usize) -> String {
    format!("\x1b[32m+{count}\x1b[0m")
}

fn style_activity_removed_count(count: usize) -> String {
    format!("\x1b[31m-{count}\x1b[0m")
}

fn style_added_diff_line(line_number: usize, width: usize, line: &str) -> String {
    format!(
        "\x1b[48;5;120m{:>width$} + {}\x1b[0m",
        line_number,
        truncate_for_tui(line, 96),
        width = width
    )
}

fn style_removed_diff_line(line_number: usize, width: usize, line: &str) -> String {
    format!(
        "\x1b[48;5;224m{:>width$} - {}\x1b[0m",
        line_number,
        truncate_for_tui(line, 96),
        width = width
    )
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
        RenderBlock::Panel(panel) if panel.kind == PanelKind::Approval => {
            panel.lines.iter().find_map(|line| {
                line.strip_prefix("Tool: ")
                    .map(|tool| tool.trim().to_string())
            })
        }
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

    let activity_sections = render_activity_sections(screen);
    if !activity_sections.is_empty() {
        sections.extend(activity_sections);
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

    if !screen.prompt.is_empty() {
        sections.push(screen.prompt.join("\n"));
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

fn style_activity_action(label: &str) -> String {
    format!("\x1b[1;30m{label}\x1b[0m")
}

fn style_activity_title(label: &str) -> String {
    format!("\x1b[1;34m[{label}]\x1b[0m")
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
        if panel.title == "Activity" {
            continue;
        }
        sections.push(render_tui_section(
            &panel.title,
            panel.lines.iter().map(|line| line.as_str()).collect(),
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

fn render_activity_sections(screen: &TuiScreen) -> Vec<String> {
    screen
        .panels
        .iter()
        .filter(|panel| panel.title == "Activity")
        .map(|panel| {
            let mut lines = vec![style_activity_title("Activity")];
            lines.extend(panel.lines.iter().map(|line| format!("  {line}")));
            lines.join("\n")
        })
        .collect()
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

    fn strip_ansi(text: &str) -> String {
        let mut cleaned = String::new();
        let mut chars = text.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '\u{1b}' && chars.peek() == Some(&'[') {
                chars.next();
                for next in chars.by_ref() {
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
                continue;
            }
            cleaned.push(ch);
        }

        cleaned
    }

    #[test]
    fn tui_output_omits_streaming_delta_noise() {
        let turn = CliTurnOutput {
            primary_text: "final answer".into(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
                    text: "morg".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta { text: "o".into() }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
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

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("[Activity]"));
        assert!(rendered.contains("• RAN cargo test -- --nocapture"));
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

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("[Activity]"));
        assert!(rendered.contains("EXPLORED"));
        assert!(rendered.contains("READ renderer.rs"));
        assert!(rendered.contains("SEARCH delta|tool use in reference"));
        assert_eq!(rendered.matches("READ renderer.rs").count(), 1);
    }

    #[test]
    fn tui_renders_edit_activity_as_colored_diff_preview() {
        let path = std::env::temp_dir().join("renderer_edit_activity_preview.rs");
        std::fs::write(
            &path,
            "fn before() {\n    println!(\"old\");\n}\nfn after() {}\n",
        )
        .expect("write temp preview file");
        let path_text = path.display().to_string();

        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Edit".into(),
                content: format!(
                    "path={path_text}\nreplacements=1\nreplace_all=false\nold_text=    println!(\"todo\");\nnew_text=    println!(\"old\");"
                ),
                summary: Some("Edit succeeded".into()),
                detail: Some(format!(
                    "path={path_text}\nreplacements=1\nreplace_all=false\nold_text=    println!(\"todo\");\nnew_text=    println!(\"old\");"
                )),
            })],
        };

        let rendered = render_turn_tui_output(&turn);
        let plain = strip_ansi(&rendered);
        assert!(plain.contains("[Activity]"));
        assert!(plain.contains("EDITED"));
        assert!(plain.contains("(+1 -1)"));
        assert!(plain.contains("renderer_edit_activity_preview.rs"));
        assert!(
            plain.contains("+     println!(\"old\");") || plain.contains("+ println!(\"old\");")
        );
        assert!(
            plain.contains("-     println!(\"todo\");") || plain.contains("- println!(\"todo\");")
        );
        assert!(rendered.contains("\x1b[48;5;120m"));
        assert!(rendered.contains("\x1b[48;5;224m"));

        let _ = std::fs::remove_file(path);
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

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("answer"));
        assert!(!rendered.contains("recorded usage"));
        assert!(!rendered.contains("Notice:"));
    }

    #[test]
    fn tui_filters_tool_result_follow_up_text_from_primary_message_area() {
        let turn = CliTurnOutput {
            primary_text: [
                "tool Read result: Read succeeded (5313 chars)",
                "tool Grep result: Grep succeeded (0 chars)",
                "tool batch result:",
                "verified_target: /tmp/report.md",
                "verification_result: verified",
                "minimal_evidence: Read succeeded",
                "remaining_blocker: none",
                "",
                "Final answer",
            ]
            .join("\n"),
            events: vec![],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("Final answer"));
        assert!(!rendered.contains("tool Read result:"));
        assert!(!rendered.contains("tool Grep result:"));
        assert!(!rendered.contains("tool batch result:"));
        assert!(!rendered.contains("verified_target:"));
        assert!(!rendered.contains("verification_result:"));
        assert!(!rendered.contains("minimal_evidence:"));
        assert!(!rendered.contains("remaining_blocker:"));
    }

    #[test]
    fn tui_approval_panel_prefers_run_reason_and_action_fields() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                tool_name: "Bash".into(),
                message: "raw fallback".into(),
                code: Some("policy_escalation".into()),
                summary: Some("Bash pending approval".into()),
                detail: Some(
                    "Run: find . -type f | head\nReason: This command uses a pipe.\nAction: choose an approval option below".into(),
                ),
                approval_kind: Some("tool_permission".into()),
                escalation_reasons: vec!["shell_operator.pipe".into()],
            })],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("[Approval required]"));
        assert!(rendered.contains("Tool: Bash"));
        assert!(rendered.contains("Run: find . -type f | head"));
        assert!(rendered.contains("Reason: This command uses a pipe."));
        assert!(rendered.contains("Action: choose an approval option below"));
        assert!(!rendered.contains("Reason: raw fallback"));
    }

    #[test]
    fn tui_prompt_renders_outside_box() {
        let screen = build_tui_screen(&RenderDocument { blocks: vec![] });
        let rendered = strip_ansi(&render_tui_screen_output(&screen));
        assert!(rendered.contains("\n\n> "));
        assert!(!rendered.contains("[Prompt]"));
        assert!(!rendered.contains("║ [Prompt]"));
    }
}

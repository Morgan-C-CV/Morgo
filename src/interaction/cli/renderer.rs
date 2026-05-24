use crate::core::message::is_legacy_hidden_primary_line;
use crate::core::output::{OutputBlock, blocks_to_plain_text};
use crate::interaction::cli::repl::CliTurnOutput;
use crate::interaction::view::{SurfaceItem, SurfaceView, TaskView, build_surface_view};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use std::path::PathBuf;

const MAX_TOOL_DETAIL_LINES: usize = 8;
const MAX_TOOL_DETAIL_WIDTH: usize = 100;
const DIFF_CONTEXT_LINES: usize = 3;
const MAX_EXACT_DIFF_CELLS: usize = 2_000_000;
pub const CONVERSATION_INTERRUPTED_MESSAGE: &str =
    "■ Conversation interrupted - tell the model what to do differently.";
const APPROVAL_CONTINUATION_PREFIX: &str = "Approval resolved for tool ";
const APPROVAL_CONTINUATION_MIDDLE: &str = "\n\nTool input:\n";
const APPROVAL_CONTINUATION_RESULT: &str = "\n\nTool result:\n";
const APPROVAL_CONTINUATION_SUFFIX: &str =
    "\n\nContinue the interrupted user task using this tool result.";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TuiMainFlowKind {
    Text,
    Activity,
    Divider,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderBlock {
    PrimaryText(String),
    Panel(RenderPanel),
    RawRuntime(String),
    Divider,
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
    let mut last_main_flow_kind = None::<TuiMainFlowKind>;

    for (index, block) in document.blocks.iter().enumerate() {
        match block {
            RenderBlock::PrimaryText(text) => {
                let visible_lines = visible_tui_primary_lines(text);
                if !visible_lines.is_empty() {
                    main.extend(visible_lines);
                    last_main_flow_kind = Some(TuiMainFlowKind::Text);
                }
            }
            RenderBlock::Divider => {
                if !main.is_empty() {
                    main.push(activity_stage_divider_line());
                    last_main_flow_kind = Some(TuiMainFlowKind::Divider);
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
            RenderBlock::Panel(panel) if panel.kind == PanelKind::ToolActivity => {
                if !panel.lines.is_empty() {
                    if last_main_flow_kind == Some(TuiMainFlowKind::Activity) && !main.is_empty() {
                        main.push(activity_stage_divider_line());
                    }
                    main.extend(panel.lines.clone());
                    last_main_flow_kind = Some(TuiMainFlowKind::Activity);
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

        let line = style_tui_primary_line(raw_line);
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

fn style_tui_primary_line(line: &str) -> String {
    if line == CONVERSATION_INTERRUPTED_MESSAGE {
        format!("\x1b[31m{line}\x1b[0m")
    } else {
        line.to_string()
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
    if should_render_interleaved_activity(view) {
        return build_interleaved_render_document(view);
    }

    let has_pending_approval = surface_items_have_pending_approval(&view.items);
    let mut blocks = Vec::new();
    let primary_text = if view.primary_text.is_empty() {
        streaming_delta_text(&view.items)
    } else {
        view.primary_text.clone()
    };
    if !primary_text.is_empty() {
        if let Some(lines) = approval_continuation_activity_lines(&primary_text) {
            blocks.push(RenderBlock::Panel(render_panel(
                PanelKind::ToolActivity,
                "Activity",
                lines,
            )));
        } else {
            blocks.push(RenderBlock::PrimaryText(primary_text));
        }
    }
    for activity_panel in build_tool_activity_panels(&view.items) {
        blocks.push(RenderBlock::Panel(activity_panel));
    }
    for item in &view.items {
        if let Some(block) = render_block_for_surface_item(item, has_pending_approval) {
            blocks.push(block);
        }
    }
    RenderDocument { blocks }
}

fn should_render_interleaved_activity(view: &SurfaceView) -> bool {
    let has_pending_approval = surface_items_have_pending_approval(&view.items);
    view.items.iter().any(|item| {
        matches!(
            item,
            SurfaceItem::ToolCallStarted { .. } | SurfaceItem::ToolResult { .. }
        )
    }) && (!view.primary_text.is_empty()
        || view
            .items
            .iter()
            .any(|item| matches!(item, SurfaceItem::AssistantDelta { text } if !text.is_empty()))
        || view.items.iter().any(|item| {
            matches!(
                item,
                SurfaceItem::Terminal { kind, .. }
                    if terminal_interrupt_message(kind, has_pending_approval).is_some()
            )
        }))
}

fn build_interleaved_render_document(view: &SurfaceView) -> RenderDocument {
    let mut builder = InterleavedRenderBuilder::default();
    let has_pending_approval = surface_items_have_pending_approval(&view.items);

    for item in &view.items {
        match item {
            SurfaceItem::AssistantDelta { text } => builder.push_text(text),
            SurfaceItem::Terminal { kind, .. }
                if terminal_interrupt_message(kind, has_pending_approval).is_some() =>
            {
                if let Some(message) = terminal_interrupt_message(kind, has_pending_approval) {
                    builder.push_text(message);
                }
            }
            SurfaceItem::ToolCallStarted { tool_name, input } => {
                builder.flush_text();
                builder.push_tool_call(tool_name, input);
            }
            SurfaceItem::ToolResult {
                tool_name,
                content,
                summary,
                detail,
            } => {
                builder.flush_text();
                builder.push_tool_result(tool_name, content, summary.as_deref(), detail.as_deref());
            }
            _ => {
                if let Some(block) = render_block_for_surface_item(item, has_pending_approval) {
                    builder.flush_activity_before_non_activity();
                    builder.push_non_activity_block(block);
                }
            }
        }
    }

    builder.finish_with_fallback_primary(&view.primary_text)
}

#[derive(Default)]
struct InterleavedRenderBuilder {
    blocks: Vec<RenderBlock>,
    text_buffer: String,
    activity: ActivityStageBuilder,
    last_flushed_activity: bool,
    saw_text: bool,
}

impl InterleavedRenderBuilder {
    fn push_text(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        if self.activity.has_pending_activity() {
            self.flush_activity();
        }
        if self.last_flushed_activity && !self.blocks.is_empty() {
            self.blocks.push(RenderBlock::Divider);
        }
        self.last_flushed_activity = false;
        self.text_buffer.push_str(text);
    }

    fn push_tool_call(&mut self, tool_name: &str, input: &str) {
        self.activity.push_tool_call(tool_name, input);
        self.last_flushed_activity = false;
    }

    fn push_tool_result(
        &mut self,
        tool_name: &str,
        content: &str,
        summary: Option<&str>,
        detail: Option<&str>,
    ) {
        self.activity
            .push_tool_result(tool_name, content, summary, detail);
        self.last_flushed_activity = false;
    }

    fn push_non_activity_block(&mut self, block: RenderBlock) {
        self.flush_text();
        self.blocks.push(block);
        self.last_flushed_activity = false;
    }

    fn flush_text(&mut self) {
        if self.text_buffer.is_empty() {
            return;
        }
        self.blocks.push(RenderBlock::PrimaryText(std::mem::take(
            &mut self.text_buffer,
        )));
        self.saw_text = true;
        self.last_flushed_activity = false;
    }

    fn flush_activity(&mut self) {
        for panel in self.activity.take_panels() {
            self.blocks.push(RenderBlock::Panel(panel));
            self.last_flushed_activity = true;
        }
    }

    fn flush_activity_before_non_activity(&mut self) {
        self.flush_text();
        self.flush_activity();
    }

    fn finish_with_fallback_primary(mut self, primary_text: &str) -> RenderDocument {
        self.flush_text();
        self.flush_activity();

        if !primary_text.is_empty() && !self.saw_text {
            self.push_text(primary_text);
            self.flush_text();
        }

        RenderDocument {
            blocks: self.blocks,
        }
    }
}

fn streaming_delta_text(items: &[SurfaceItem]) -> String {
    items
        .iter()
        .filter_map(|item| match item {
            SurfaceItem::AssistantDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_block_for_surface_item(
    item: &SurfaceItem,
    has_pending_approval: bool,
) -> Option<RenderBlock> {
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
        SurfaceItem::RuntimeNotice { .. }
        | SurfaceItem::ToolCallStarted { .. }
        | SurfaceItem::AssistantDelta { .. } => None,
        SurfaceItem::ToolResult { .. } => None,
        SurfaceItem::Terminal { kind, .. } => {
            terminal_interrupt_message(kind, has_pending_approval)
                .map(|message| RenderBlock::PrimaryText(message.into()))
        }
        SurfaceItem::Transition { .. } | SurfaceItem::SessionMilestone { .. } => None,
    }
}

fn surface_items_have_pending_approval(items: &[SurfaceItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, SurfaceItem::ApprovalRequired { .. }))
}

fn terminal_interrupt_message(kind: &str, has_pending_approval: bool) -> Option<&'static str> {
    match kind {
        "aborted_streaming" => Some(CONVERSATION_INTERRUPTED_MESSAGE),
        "aborted_tools" if !has_pending_approval => Some(CONVERSATION_INTERRUPTED_MESSAGE),
        _ => None,
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExplorationEntry {
    Read { paths: Vec<String> },
    Line(String),
}

impl ExplorationEntry {
    fn from_tool_call(tool_name: &str, input: &str) -> Option<Self> {
        let parsed = serde_json::from_str::<Value>(input).ok();
        match tool_name {
            "Read" => {
                let path = json_string_field(parsed.as_ref(), &["path", "file_path"])?;
                Some(Self::Read {
                    paths: vec![short_path(&path)],
                })
            }
            "Grep" => {
                let pattern = json_string_field(parsed.as_ref(), &["pattern", "query"])?;
                let path = json_string_field(parsed.as_ref(), &["path"])
                    .map(|value| format!(" in {}", short_path(&value)))
                    .unwrap_or_default();
                Some(Self::Line(format!(
                    "{} {}{}",
                    style_activity_action("SEARCH"),
                    truncate_for_tui(&pattern, 72),
                    path
                )))
            }
            "Glob" => {
                let pattern = json_string_field(parsed.as_ref(), &["pattern", "glob"])
                    .or_else(|| json_string_field(parsed.as_ref(), &["path"]))?;
                Some(Self::Line(format!(
                    "{} {}",
                    style_activity_action("LIST"),
                    truncate_for_tui(&pattern, 72)
                )))
            }
            "ToolSearch" | "WebSearch" => {
                let query = json_string_field(parsed.as_ref(), &["query", "q"])?;
                Some(Self::Line(format!(
                    "{} {}",
                    style_activity_action("SEARCH"),
                    truncate_for_tui(&query, 72)
                )))
            }
            "WebFetch" => {
                let url = json_string_field(parsed.as_ref(), &["url"])?;
                Some(Self::Line(format!(
                    "{} {}",
                    style_activity_action("FETCHED"),
                    truncate_for_tui(&url, 72)
                )))
            }
            _ => None,
        }
    }

    fn merge_into(self, entries: &mut Vec<ExplorationEntry>) {
        match self {
            Self::Read { paths } => {
                if let Some(Self::Read { paths: existing }) = entries.last_mut() {
                    for path in paths {
                        if !existing.contains(&path) {
                            existing.push(path);
                        }
                    }
                } else {
                    entries.push(Self::Read { paths });
                }
            }
            Self::Line(line) => entries.push(Self::Line(line)),
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Read { paths } => {
                format!("{} {}", style_activity_action("READ"), paths.join(", "))
            }
            Self::Line(line) => line.clone(),
        }
    }
}

fn activity_stage_divider_line() -> String {
    "─".repeat(120)
}

fn render_exploration_stage(entries: &[ExplorationEntry]) -> Vec<String> {
    let mut lines = vec![activity_status_line(ActivityStatus::Succeeded, "Explored")];
    for (index, entry) in entries.iter().enumerate() {
        if index == 0 {
            lines.push(format!("  └ {}", entry.render()));
        } else {
            lines.push(format!("    {}", entry.render()));
        }
    }
    lines
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityStatus {
    Running,
    Succeeded,
    Failed,
}

fn activity_status_line(status: ActivityStatus, text: &str) -> String {
    let bullet_color = match status {
        ActivityStatus::Running => "2;37",
        ActivityStatus::Succeeded => "32",
        ActivityStatus::Failed => "31",
    };
    format!("{} {text}", colorize_activity_bullet(bullet_color))
}

fn colorize_activity_bullet(color: &str) -> String {
    format!("\x1b[{color}m•\x1b[0m")
}

#[derive(Default)]
struct ActivityStageBuilder {
    lines: Vec<String>,
    exploration_entries: Vec<ExplorationEntry>,
    in_exploration_stage: bool,
    pending_tool_call: Option<PendingActivityToolCall>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingActivityToolCall {
    tool_name: String,
    input: String,
}

impl ActivityStageBuilder {
    fn has_pending_activity(&self) -> bool {
        self.in_exploration_stage
            || !self.exploration_entries.is_empty()
            || !self.lines.is_empty()
            || self.pending_tool_call.is_some()
    }

    fn push_tool_call(&mut self, tool_name: &str, input: &str) {
        if is_exploration_tool(tool_name) {
            self.flush_pending_tool_call();
            self.in_exploration_stage = true;
            if let Some(entry) = ExplorationEntry::from_tool_call(tool_name, input) {
                entry.merge_into(&mut self.exploration_entries);
            }
        } else if tool_call_activity_line(tool_name, input).is_some() {
            self.finish_exploration_stage();
            self.flush_pending_tool_call();
            self.pending_tool_call = Some(PendingActivityToolCall {
                tool_name: tool_name.to_string(),
                input: input.to_string(),
            });
        }
    }

    fn push_tool_result(
        &mut self,
        tool_name: &str,
        content: &str,
        summary: Option<&str>,
        detail: Option<&str>,
    ) {
        let pending = self.pending_tool_call.take();
        let (pending_input, restore_pending) = match pending {
            Some(pending) if pending.tool_name == tool_name => (Some(pending.input), None),
            other => (None, other),
        };
        self.pending_tool_call = restore_pending;
        if let Some((headline, status, detail_lines)) = tool_result_activity_block(
            tool_name,
            pending_input.as_deref(),
            content,
            summary,
            detail,
        ) {
            self.finish_exploration_stage();
            let detail_lines = detail_lines
                .into_iter()
                .filter(|line| !is_low_signal_tool_detail(line))
                .collect::<Vec<_>>();
            if !headline.trim().is_empty() {
                self.lines.push(activity_status_line(status, &headline));
            }
            for detail_line in detail_lines {
                self.lines.push(format!("  └ {detail_line}"));
            }
        }
    }

    fn take_panels(&mut self) -> Vec<RenderPanel> {
        self.finish_exploration_stage();
        self.flush_pending_tool_call();
        if self.lines.is_empty() {
            Vec::new()
        } else {
            vec![render_panel(
                PanelKind::ToolActivity,
                "Activity",
                std::mem::take(&mut self.lines),
            )]
        }
    }

    fn finish_exploration_stage(&mut self) {
        if !self.in_exploration_stage {
            return;
        }
        if !self.exploration_entries.is_empty() {
            self.lines
                .extend(render_exploration_stage(&self.exploration_entries));
            self.exploration_entries.clear();
        }
        self.in_exploration_stage = false;
    }

    fn flush_pending_tool_call(&mut self) {
        let Some(pending) = self.pending_tool_call.take() else {
            return;
        };
        if let Some(line) = tool_call_activity_line(&pending.tool_name, &pending.input) {
            self.lines
                .push(activity_status_line(ActivityStatus::Running, &line));
        }
    }
}

fn build_tool_activity_panels(items: &[SurfaceItem]) -> Vec<RenderPanel> {
    let mut builder = ActivityStageBuilder::default();

    for item in items {
        match item {
            SurfaceItem::ToolCallStarted { tool_name, input } => {
                builder.push_tool_call(tool_name, input);
            }
            SurfaceItem::ToolResult {
                tool_name,
                content,
                summary,
                detail,
            } => {
                builder.push_tool_result(tool_name, content, summary.as_deref(), detail.as_deref());
            }
            _ => {}
        }
    }

    builder.take_panels()
}

pub fn approval_continuation_activity_lines(text: &str) -> Option<Vec<String>> {
    let text = text.trim();
    let after_prefix = text.strip_prefix(APPROVAL_CONTINUATION_PREFIX)?;
    let (tool_name, after_tool) = after_prefix.split_once(".\n")?;
    let (_, after_middle) = after_tool.split_once(APPROVAL_CONTINUATION_MIDDLE)?;
    let (tool_input, after_result) = after_middle.split_once(APPROVAL_CONTINUATION_RESULT)?;
    let (tool_result, _) = after_result.split_once(APPROVAL_CONTINUATION_SUFFIX)?;

    let mut builder = ActivityStageBuilder::default();
    builder.push_tool_call(tool_name.trim(), tool_input.trim());
    let summary = approval_continuation_summary(tool_name.trim(), tool_input.trim(), tool_result);
    builder.push_tool_result(
        tool_name.trim(),
        tool_result.trim(),
        Some(&summary),
        Some(tool_result.trim()),
    );
    builder
        .take_panels()
        .into_iter()
        .next()
        .map(|panel| panel.lines)
}

fn approval_continuation_summary(tool_name: &str, tool_input: &str, tool_result: &str) -> String {
    if tool_name == "Bash" {
        let status = activity_status_from_tool_result("Bash succeeded", tool_result);
        let command = bash_activity_command(Some(tool_input), tool_result, "Bash succeeded")
            .unwrap_or_else(|| "Bash".into());
        return match status {
            ActivityStatus::Failed => format!("{command} failed"),
            _ => format!("{command} succeeded"),
        };
    }
    if tool_result.to_ascii_lowercase().contains("failed") {
        format!("{tool_name} failed")
    } else {
        format!("{tool_name} succeeded")
    }
}

fn is_exploration_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "Read" | "Grep" | "Glob" | "ToolSearch" | "WebSearch" | "WebFetch"
    )
}

fn tool_call_activity_line(tool_name: &str, input: &str) -> Option<String> {
    match tool_name {
        "Bash" => {
            let parsed = serde_json::from_str::<Value>(input).ok();
            let command = json_string_field(parsed.as_ref(), &["command", "cmd"])?;
            Some(format!("Running {}", truncate_for_tui(&command, 72)))
        }
        "Edit" | "Write" | "FileEdit" | "FileWrite" => {
            let parsed = serde_json::from_str::<Value>(input).ok();
            let path = json_string_field(parsed.as_ref(), &["path", "file_path"])?;
            Some(format!("Running update {}", short_path(&path)))
        }
        _ => Some(format!("Running {tool_name}")),
    }
}

fn tool_result_activity_block(
    tool_name: &str,
    input: Option<&str>,
    content: &str,
    summary: Option<&str>,
    detail: Option<&str>,
) -> Option<(String, ActivityStatus, Vec<String>)> {
    let summary = summary.map(str::trim).filter(|value| !value.is_empty())?;
    if is_exploration_tool(tool_name) {
        return None;
    }

    if matches!(tool_name, "Edit" | "FileEdit") {
        return render_edit_activity_block(content, detail)
            .map(|(headline, detail)| (headline, activity_status_from_summary(summary), detail));
    }

    let detail_source = detail.unwrap_or(content);
    let status = activity_status_from_tool_result(summary, detail_source);
    let headline = match tool_name {
        "Bash" => bash_activity_command(input, detail_source, summary)
            .map(|command| format!("Ran {}", truncate_for_tui(&command, 72)))
            .unwrap_or_else(|| format!("Ran {}", truncate_for_tui(summary, 72))),
        "Edit" | "Write" | "FileEdit" | "FileWrite" => truncate_for_tui(summary, 72),
        _ => truncate_for_tui(summary, 72),
    };

    let detail_lines = if tool_name == "Bash" {
        summarize_bash_activity_detail(detail_source)
    } else {
        compact_tool_detail_lines(detail_source.lines().map(|line| line.to_string()).collect())
    };

    Some((headline, status, detail_lines))
}

fn summarize_bash_activity_detail(content: &str) -> Vec<String> {
    let parsed = parse_bash_result(content);
    let mut lines = Vec::new();
    if let Some(exit_code) = parsed.exit_code.as_deref().filter(|code| *code != "0") {
        lines.push(format!("Exit code: {exit_code}"));
    }
    if !parsed.stdout.is_empty() {
        lines.extend(compact_tool_detail_lines(parsed.stdout));
    }
    if !parsed.stderr.is_empty() {
        if !lines.is_empty() {
            lines.push("stderr:".into());
        }
        lines.extend(compact_tool_detail_lines(parsed.stderr));
    }
    if lines.is_empty() && !parsed.body.is_empty() {
        lines.extend(compact_tool_detail_lines(parsed.body));
    }
    lines
}

fn render_edit_activity_block(
    content: &str,
    detail: Option<&str>,
) -> Option<(String, Vec<String>)> {
    let detail_source = detail.unwrap_or(content);
    let fields = parse_key_value_lines(detail_source);
    let path = fields.get("path")?;
    let old_text = decode_edit_payload_text(&fields, "old_text_b64", "old_text");
    let new_text = decode_edit_payload_text(&fields, "new_text_b64", "new_text");
    let replace_all = fields
        .get("replace_all")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let replacements = fields
        .get("replacements")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);

    let rendered_diff =
        render_edit_diff_lines(path, &old_text, &new_text, replace_all, replacements);
    let display_path = display_activity_path(path);
    let headline = format!(
        "{} {} ({} {})",
        style_activity_action("EDITED"),
        display_path,
        style_activity_added_count(rendered_diff.additions),
        style_activity_removed_count(rendered_diff.removals),
    );

    Some((headline, rendered_diff.lines))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderedEditDiff {
    lines: Vec<String>,
    additions: usize,
    removals: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffLineKind {
    Context,
    Add,
    Remove,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiffLine {
    kind: DiffLineKind,
    old_number: Option<usize>,
    new_number: Option<usize>,
    text: String,
}

fn decode_edit_payload_text(
    fields: &std::collections::BTreeMap<String, String>,
    full_key: &str,
    preview_key: &str,
) -> String {
    fields
        .get(full_key)
        .and_then(|value| STANDARD.decode(value).ok())
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_else(|| {
            decode_tool_preview_text(fields.get(preview_key).map(String::as_str).unwrap_or(""))
        })
}

fn render_edit_diff_lines(
    path: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
    replacements: usize,
) -> RenderedEditDiff {
    render_structured_edit_diff(path, old_text, new_text, replace_all, replacements)
        .unwrap_or_else(|| render_legacy_edit_diff_lines(path, old_text, new_text))
}

fn render_structured_edit_diff(
    path: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
    replacements: usize,
) -> Option<RenderedEditDiff> {
    let current_file = std::fs::read_to_string(path).ok();
    let (before, after) = if let Some(after) = current_file.as_deref() {
        reconstruct_before_edit(after, old_text, new_text, replace_all, replacements)
            .map(|before| (before, after.to_string()))
            .unwrap_or_else(|| (old_text.to_string(), new_text.to_string()))
    } else {
        (old_text.to_string(), new_text.to_string())
    };

    let diff_lines = build_numbered_diff(&before, &after);
    let additions = diff_lines
        .iter()
        .filter(|line| line.kind == DiffLineKind::Add)
        .count();
    let removals = diff_lines
        .iter()
        .filter(|line| line.kind == DiffLineKind::Remove)
        .count();
    let hunks = diff_hunks(&diff_lines, DIFF_CONTEXT_LINES);
    if hunks.is_empty() {
        return None;
    }

    let mut rendered = vec![style_diff_frame_line()];
    for (index, hunk) in hunks.iter().enumerate() {
        if index > 0 {
            rendered.push(style_diff_ellipsis());
        }
        rendered.extend(render_diff_hunk(hunk));
    }
    rendered.push(style_diff_frame_line());

    Some(RenderedEditDiff {
        lines: rendered,
        additions,
        removals,
    })
}

fn reconstruct_before_edit(
    after: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
    replacements: usize,
) -> Option<String> {
    if new_text.is_empty() || !after.contains(new_text) {
        return None;
    }

    let replacement_limit = replacements.max(1);
    let before = if replace_all {
        after.replacen(new_text, old_text, replacement_limit)
    } else {
        after.replacen(new_text, old_text, 1)
    };

    (before != after).then_some(before)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiffOp {
    Equal(String),
    Add(String),
    Remove(String),
}

fn build_numbered_diff(before: &str, after: &str) -> Vec<DiffLine> {
    let old_lines = split_preserve_empty(before);
    let new_lines = split_preserve_empty(after);
    let ops = build_line_diff(&old_lines, &new_lines);
    let mut old_number = 1;
    let mut new_number = 1;
    let mut lines = Vec::with_capacity(ops.len());

    for op in ops {
        match op {
            DiffOp::Equal(text) => {
                lines.push(DiffLine {
                    kind: DiffLineKind::Context,
                    old_number: Some(old_number),
                    new_number: Some(new_number),
                    text,
                });
                old_number += 1;
                new_number += 1;
            }
            DiffOp::Remove(text) => {
                lines.push(DiffLine {
                    kind: DiffLineKind::Remove,
                    old_number: Some(old_number),
                    new_number: None,
                    text,
                });
                old_number += 1;
            }
            DiffOp::Add(text) => {
                lines.push(DiffLine {
                    kind: DiffLineKind::Add,
                    old_number: None,
                    new_number: Some(new_number),
                    text,
                });
                new_number += 1;
            }
        }
    }

    lines
}

fn build_line_diff(old_lines: &[String], new_lines: &[String]) -> Vec<DiffOp> {
    let cell_count = old_lines.len().saturating_mul(new_lines.len());
    if cell_count > MAX_EXACT_DIFF_CELLS {
        return build_prefix_suffix_diff(old_lines, new_lines);
    }

    let rows = old_lines.len() + 1;
    let cols = new_lines.len() + 1;
    let mut dp = vec![0usize; rows * cols];
    for i in (0..old_lines.len()).rev() {
        for j in (0..new_lines.len()).rev() {
            let index = i * cols + j;
            dp[index] = if old_lines[i] == new_lines[j] {
                1 + dp[(i + 1) * cols + j + 1]
            } else {
                dp[(i + 1) * cols + j].max(dp[i * cols + j + 1])
            };
        }
    }

    let mut i = 0;
    let mut j = 0;
    let mut ops = Vec::new();
    while i < old_lines.len() && j < new_lines.len() {
        if old_lines[i] == new_lines[j] {
            ops.push(DiffOp::Equal(old_lines[i].clone()));
            i += 1;
            j += 1;
        } else if dp[(i + 1) * cols + j] >= dp[i * cols + j + 1] {
            ops.push(DiffOp::Remove(old_lines[i].clone()));
            i += 1;
        } else {
            ops.push(DiffOp::Add(new_lines[j].clone()));
            j += 1;
        }
    }
    while i < old_lines.len() {
        ops.push(DiffOp::Remove(old_lines[i].clone()));
        i += 1;
    }
    while j < new_lines.len() {
        ops.push(DiffOp::Add(new_lines[j].clone()));
        j += 1;
    }

    ops
}

fn build_prefix_suffix_diff(old_lines: &[String], new_lines: &[String]) -> Vec<DiffOp> {
    let mut prefix = 0;
    while prefix < old_lines.len()
        && prefix < new_lines.len()
        && old_lines[prefix] == new_lines[prefix]
    {
        prefix += 1;
    }

    let mut suffix = 0;
    while suffix + prefix < old_lines.len()
        && suffix + prefix < new_lines.len()
        && old_lines[old_lines.len() - suffix - 1] == new_lines[new_lines.len() - suffix - 1]
    {
        suffix += 1;
    }

    let mut ops = Vec::new();
    ops.extend(old_lines[..prefix].iter().cloned().map(DiffOp::Equal));
    ops.extend(
        old_lines[prefix..old_lines.len() - suffix]
            .iter()
            .cloned()
            .map(DiffOp::Remove),
    );
    ops.extend(
        new_lines[prefix..new_lines.len() - suffix]
            .iter()
            .cloned()
            .map(DiffOp::Add),
    );
    ops.extend(
        old_lines[old_lines.len() - suffix..]
            .iter()
            .cloned()
            .map(DiffOp::Equal),
    );
    ops
}

fn diff_hunks(lines: &[DiffLine], context: usize) -> Vec<Vec<DiffLine>> {
    let changed = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| (line.kind != DiffLineKind::Context).then_some(index))
        .collect::<Vec<_>>();
    if changed.is_empty() {
        return Vec::new();
    }

    let mut hunks = Vec::new();
    let mut cursor = 0;
    while cursor < changed.len() {
        let mut start = changed[cursor].saturating_sub(context);
        let mut end = (changed[cursor] + context).min(lines.len() - 1);
        cursor += 1;

        while cursor < changed.len() && changed[cursor].saturating_sub(context) <= end + 1 {
            start = start.min(changed[cursor].saturating_sub(context));
            end = end.max((changed[cursor] + context).min(lines.len() - 1));
            cursor += 1;
        }

        hunks.push(lines[start..=end].to_vec());
    }

    hunks
}

fn render_diff_hunk(hunk: &[DiffLine]) -> Vec<String> {
    let gutter_digits = hunk
        .iter()
        .filter_map(diff_display_number)
        .max()
        .unwrap_or(1)
        .to_string()
        .len()
        .max(1);
    hunk.iter()
        .map(|line| render_diff_line(line, gutter_digits))
        .collect()
}

fn render_diff_line(line: &DiffLine, gutter_digits: usize) -> String {
    let marker = match line.kind {
        DiffLineKind::Context => " ",
        DiffLineKind::Add => "+",
        DiffLineKind::Remove => "-",
    };
    let number = diff_display_number(line).unwrap_or(0);
    let gutter_width = gutter_digits + 3;
    let content_width = MAX_TOOL_DETAIL_WIDTH.saturating_sub(gutter_width).max(20);
    let rendered = format!(
        "{marker} {number:>gutter_digits$} {}",
        truncate_for_tui(&line.text, content_width)
    );

    match line.kind {
        DiffLineKind::Add => format!("\x1b[48;5;120m{rendered}\x1b[0m"),
        DiffLineKind::Remove => format!("\x1b[48;5;224m{rendered}\x1b[0m"),
        DiffLineKind::Context => rendered,
    }
}

fn diff_display_number(line: &DiffLine) -> Option<usize> {
    match line.kind {
        DiffLineKind::Add => line.new_number,
        DiffLineKind::Remove => line.old_number,
        DiffLineKind::Context => line.new_number.or(line.old_number),
    }
}

fn style_diff_frame_line() -> String {
    format!("\x1b[2m{}\x1b[0m", "-".repeat(MAX_TOOL_DETAIL_WIDTH))
}

fn style_diff_ellipsis() -> String {
    "\x1b[2m...\x1b[0m".into()
}

fn render_legacy_edit_diff_lines(path: &str, old_text: &str, new_text: &str) -> RenderedEditDiff {
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

    RenderedEditDiff {
        lines: rendered,
        additions: count_nonempty_lines(new_text),
        removals: count_nonempty_lines(old_text),
    }
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BashResultParts {
    command: Option<String>,
    exit_code: Option<String>,
    stdout: Vec<String>,
    stderr: Vec<String>,
    body: Vec<String>,
}

fn parse_bash_result(content: &str) -> BashResultParts {
    let mut parsed = BashResultParts::default();
    let mut section = None::<&str>;

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("command:") {
            parsed.command = Some(value.trim().to_string());
            section = None;
            continue;
        }
        if let Some(value) = line.strip_prefix("exit_code:") {
            parsed.exit_code = Some(value.trim().to_string());
            section = None;
            continue;
        }
        if line.trim() == "stdout:" {
            section = Some("stdout");
            continue;
        }
        if line.trim() == "stderr:" {
            section = Some("stderr");
            continue;
        }

        match section {
            Some("stdout") => parsed.stdout.push(line.to_string()),
            Some("stderr") => parsed.stderr.push(line.to_string()),
            _ => parsed.body.push(line.to_string()),
        }
    }

    parsed
}

fn bash_activity_command(input: Option<&str>, detail: &str, summary: &str) -> Option<String> {
    input
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| json_string_field(Some(&value), &["command", "cmd"]))
        .or_else(|| parse_bash_result(detail).command)
        .or_else(|| {
            summary
                .strip_suffix(" succeeded")
                .or_else(|| summary.strip_suffix(" failed"))
                .map(str::to_string)
        })
}

fn activity_status_from_summary(summary: &str) -> ActivityStatus {
    let lowered = summary.to_ascii_lowercase();
    if lowered.contains("failed") || lowered.contains("denied") || lowered.contains("error") {
        ActivityStatus::Failed
    } else {
        ActivityStatus::Succeeded
    }
}

fn activity_status_from_tool_result(summary: &str, detail: &str) -> ActivityStatus {
    let parsed = parse_bash_result(detail);
    if parsed
        .exit_code
        .as_deref()
        .is_some_and(|code| code.trim() != "0")
    {
        ActivityStatus::Failed
    } else {
        activity_status_from_summary(summary)
    }
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
    format!("\x1b[34m{label}\x1b[0m")
}

fn style_activity_title(label: &str) -> String {
    format!("\x1b[34m[{label}]\x1b[0m")
}

fn render_block_to_text(block: &RenderBlock) -> String {
    match block {
        RenderBlock::PrimaryText(text) => text.clone(),
        RenderBlock::RawRuntime(text) => text.clone(),
        RenderBlock::Divider => activity_stage_divider_line(),
        RenderBlock::Panel(panel) if panel.kind == PanelKind::ToolActivity => {
            panel.lines.join("\n")
        }
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

    fn edit_detail(path: &std::path::Path, old_text: &str, new_text: &str) -> String {
        edit_detail_with_options(path, old_text, new_text, false, 1)
    }

    fn edit_detail_with_options(
        path: &std::path::Path,
        old_text: &str,
        new_text: &str,
        replace_all: bool,
        replacements: usize,
    ) -> String {
        format!(
            "path={}\nreplacements={replacements}\nreplace_all={replace_all}\nold_text={}\nnew_text={}\nold_text_b64={}\nnew_text_b64={}",
            path.display(),
            truncate_for_tui(old_text, 40).replace('\n', "\\n"),
            truncate_for_tui(new_text, 40).replace('\n', "\\n"),
            STANDARD.encode(old_text),
            STANDARD.encode(new_text)
        )
    }

    #[test]
    fn tui_output_renders_streaming_delta_text_without_legacy_noise() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
                    text: "morg".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta { text: "o".into() }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("morgo"));
        assert!(!rendered.contains("[delta]"));
        assert!(!rendered.contains("[Prompt]"));
        assert!(!rendered.contains("[Footer]"));
    }

    #[test]
    fn tui_final_text_supersedes_streaming_delta_text() {
        let turn = CliTurnOutput {
            primary_text: "final answer".into(),
            events: vec![CliDisplayEvent::RuntimeEvent(
                CliRuntimeEvent::AssistantDelta {
                    text: "partial".into(),
                },
            )],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("final answer"));
        assert!(!rendered.contains("partial"));
        assert!(!rendered.contains("[delta]"));
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
        assert!(!rendered.contains("[Activity]"));
        assert!(rendered.contains("• Ran cargo test -- --nocapture"));
        assert!(
            !rendered.contains(
                "Command: cargo test --package agent --lib -- interaction::cli::renderer"
            )
        );
        assert!(!rendered.contains("Exit code: 0"));
        assert!(!rendered.contains("\"timeout_ms\":120000"));
        assert!(!rendered.contains("[Tool result]"));
    }

    #[test]
    fn tui_bash_activity_uses_status_colored_bullet() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Bash".into(),
                content: "command: cargo test\nexit_code: 1\nstderr:\nfailed".into(),
                summary: Some("cargo test failed".into()),
                detail: Some("command: cargo test\nexit_code: 1\nstderr:\nfailed".into()),
            })],
        };

        let rendered = render_turn_tui_output(&turn);
        let plain = strip_ansi(&rendered);
        assert!(plain.contains("• Ran cargo test"));
        assert!(plain.contains("Exit code: 1"));
        assert!(plain.contains("failed"));
        assert!(rendered.contains("\x1b[31m•\x1b[0m"));
    }

    #[test]
    fn tui_running_activity_uses_gray_bullet_and_running_label() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(
                CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Bash".into(),
                    input: r#"{"command":"cargo build --bin morgo"}"#.into(),
                },
            )],
        };

        let rendered = render_turn_tui_output(&turn);
        let plain = strip_ansi(&rendered);
        assert!(plain.contains("• Running cargo build --bin morgo"));
        assert!(rendered.contains("\x1b[2;37m•\x1b[0m"));
    }

    #[test]
    fn tui_approval_continuation_renders_as_activity_summary() {
        let prompt = [
            "Approval resolved for tool Bash.",
            "The approved tool has now run.",
            "",
            "Tool input:",
            r#"{"command":"pwd && git status --short && ls -la","description":"Inspect repo root and status"}"#,
            "",
            "Tool result:",
            "description: Inspect repo root and status",
            "command: pwd && git status --short && ls -la",
            "normalized_variants: [\"pwd && git status --short && ls -la\"]",
            "cwd: /Users/wangmorgan/MProject/LearnCCfromCC",
            "sandbox_policy: WorkspaceWrite",
            "exit_code: 0",
            "stdout:",
            "/Users/wangmorgan/MProject/LearnCCfromCC",
            " M RustAgent/Agent/src/bootstrap/runtime.rs",
            "",
            "Continue the interrupted user task using this tool result. Do not repeat the same approved tool call unless more evidence is needed.",
        ]
        .join("\n");
        let turn = CliTurnOutput {
            primary_text: prompt,
            events: vec![],
        };

        let rendered = render_turn_tui_output(&turn);
        let plain = strip_ansi(&rendered);
        assert!(plain.contains("• Ran pwd && git status --short && ls -la"));
        assert!(plain.contains("└ /Users/wangmorgan/MProject/LearnCCfromCC"));
        assert!(plain.contains("└ M RustAgent/Agent/src/bootstrap/runtime.rs"));
        assert!(!plain.contains("Approval resolved for tool Bash"));
        assert!(!plain.contains("Continue the interrupted user task"));
        assert!(rendered.contains("\x1b[32m•\x1b[0m"));
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
        assert!(!rendered.contains("[Activity]"));
        assert!(rendered.contains("• Explored"));
        assert!(rendered.contains("READ renderer.rs"));
        assert!(rendered.contains("SEARCH delta|tool use in reference"));
        assert_eq!(rendered.matches("READ renderer.rs").count(), 1);
    }

    #[test]
    fn tui_places_primary_text_after_activity_when_message_events_are_missing() {
        let turn = CliTurnOutput {
            primary_text: "### 方案 B：直接给你一个“改造优先级清单”".into(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"createBridgeLogger|bridgeUI|BridgeLogger|spawnMode|sessionDisplayInfo|qr","path":"src"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"export function createBridgeLogger|function renderConnectingLine|function renderStatusLine","path":"src"}"#.into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        let answer_pos = rendered
            .find("### 方案 B：直接给你一个“改造优先级清单”")
            .expect("final answer text");
        let activity_pos = rendered.find("• Explored").expect("activity section");
        let divider_pos = rendered.find("────────────────").expect("divider");
        assert!(activity_pos < divider_pos, "{rendered}");
        assert!(divider_pos < answer_pos, "{rendered}");
        assert_eq!(rendered.matches("• Explored").count(), 1, "{rendered}");
        assert!(rendered.contains(
            "SEARCH createBridgeLogger|bridgeUI|BridgeLogger|spawnMode|sessionDisplayInfo|qr in src"
        ));
    }

    #[test]
    fn tui_merges_consecutive_activity_without_dividers() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"src/state/active_model_runtime.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"src/bootstrap/model_profiles.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"src/bootstrap/model_profiles.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"struct ModelProviderConfig","path":"src/service/api/client.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"src/service/api/client.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Bash".into(),
                    input: r#"{"command":"cargo test --lib","timeout_ms":120000}"#.into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("• Explored"));
        assert!(rendered.contains("READ active_model_runtime.rs, model_profiles.rs"));
        assert!(rendered.contains("SEARCH struct ModelProviderConfig in client.rs"));
        assert!(rendered.contains("READ client.rs"));
        assert!(rendered.contains("Running cargo test --lib"));
        assert!(!rendered.contains("────────────────"), "{rendered}");
        assert_eq!(rendered.matches("• Explored").count(), 1, "{rendered}");
    }

    #[test]
    fn tui_interleaves_activity_between_streamed_assistant_messages() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
                    text: "我先核对 TUI 启动链路。\n".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Glob".into(),
                    input: r#"{"pattern":"logs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"tui-runtime.log"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantDelta {
                    text: "日志已经把关键点钉住了。\n".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"PTY Host|sighup","path":"runtime.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"runtime.rs"}"#.into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        let first_text_pos = rendered.find("我先核对 TUI 启动链路。").unwrap();
        let first_activity_pos = rendered.find("LIST logs").unwrap();
        let divider_pos = rendered.find("────────────────").unwrap();
        let second_text_pos = rendered.find("日志已经把关键点钉住了。").unwrap();
        let second_activity_pos = rendered
            .find("SEARCH PTY Host|sighup in runtime.rs")
            .unwrap();
        let second_read_pos = rendered.find("READ runtime.rs").unwrap();

        assert!(first_text_pos < first_activity_pos, "{rendered}");
        assert!(first_activity_pos < divider_pos, "{rendered}");
        assert!(divider_pos < second_text_pos, "{rendered}");
        assert!(second_text_pos < second_activity_pos, "{rendered}");
        assert!(second_activity_pos < second_read_pos, "{rendered}");
        assert!(!rendered.contains("[Activity]"), "{rendered}");
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.chars().all(|ch| ch == '─') && line.len() >= 80)
                .count(),
            1,
            "{rendered}"
        );
        assert_eq!(rendered.matches("• Explored").count(), 2, "{rendered}");
    }

    #[test]
    fn tui_interleaves_committed_messages_in_completed_turn_output() {
        let turn = CliTurnOutput {
            primary_text: [
                "我先确认 TUI 渲染入口。",
                "现在根因已经定位：最终态丢了事件顺序。",
            ]
            .join("\n"),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantMessageCommitted {
                    text: "我先确认 TUI 渲染入口。\n".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"renderer.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"build_render_document","path":"renderer.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantMessageCommitted {
                    text: "现在根因已经定位：最终态丢了事件顺序。\n".into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        let first_text_pos = rendered.find("我先确认 TUI 渲染入口。").unwrap();
        let activity_pos = rendered.find("READ renderer.rs").unwrap();
        let divider_pos = rendered.find("────────────────").unwrap();
        let second_text_pos = rendered
            .find("现在根因已经定位：最终态丢了事件顺序。")
            .unwrap();

        assert!(first_text_pos < activity_pos, "{rendered}");
        assert!(activity_pos < divider_pos, "{rendered}");
        assert!(divider_pos < second_text_pos, "{rendered}");
        assert_eq!(rendered.matches("• Explored").count(), 1, "{rendered}");
        assert_eq!(rendered.matches("我先确认 TUI 渲染入口。").count(), 1);
        assert_eq!(
            rendered
                .matches("现在根因已经定位：最终态丢了事件顺序。")
                .count(),
            1
        );
    }

    #[test]
    fn tui_merges_consecutive_activity_after_committed_message_with_user_context() {
        let turn = CliTurnOutput {
            primary_text: "我先直接定位 TUI/计时器/流式输出相关代码。".into(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Glob".into(),
                    input: r#"{"pattern":"**/*"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"stream|streaming|timer","path":"LearnCCfromCC"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantMessageCommitted {
                    text: "我先直接定位 TUI/计时器/流式输出相关代码。\n".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Glob".into(),
                    input: r#"{"pattern":"**/*tui*"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Glob".into(),
                    input: r#"{"pattern":"**/*stream*"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"src/service/api/streaming.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"StreamEvent|TextDelta","path":"src"}"#.into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        let first_activity_pos = rendered
            .find("SEARCH stream|streaming|timer in LearnCCfromCC")
            .unwrap();
        let text_pos = rendered
            .find("我先直接定位 TUI/计时器/流式输出相关代码。")
            .unwrap();
        let second_activity_pos = rendered.find("LIST **/*tui*").unwrap();
        let read_pos = rendered.find("READ streaming.rs").unwrap();
        let second_search_pos = rendered
            .find("SEARCH StreamEvent|TextDelta in src")
            .unwrap();

        assert!(first_activity_pos < text_pos, "{rendered}");
        assert!(text_pos < second_activity_pos, "{rendered}");
        assert!(second_activity_pos < read_pos, "{rendered}");
        assert!(read_pos < second_search_pos, "{rendered}");
        assert_eq!(rendered.matches("• Explored").count(), 2, "{rendered}");
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.chars().all(|ch| ch == '─') && line.len() >= 80)
                .count(),
            1,
            "{rendered}"
        );
    }

    #[test]
    fn tui_invisible_runtime_events_do_not_split_activity_groups() {
        let turn = CliTurnOutput {
            primary_text: "我继续只读关键路径。".into(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::AssistantMessageCommitted {
                    text: "我继续只读关键路径。\n".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"tick|frame|render","path":"runtime.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::SessionMilestone {
                    kind: "tool_result_committed".into(),
                    text: "Tool result committed".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Transition {
                    kind: "continue".into(),
                    text: "Continuing".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"stream|delta|partial","path":"runtime.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::SessionMilestone {
                    kind: "tool_result_committed".into(),
                    text: "Tool result committed".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"runtime.rs"}"#.into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        let first_search_pos = rendered
            .find("SEARCH tick|frame|render in runtime.rs")
            .unwrap();
        let second_search_pos = rendered
            .find("SEARCH stream|delta|partial in runtime.rs")
            .unwrap();
        let read_pos = rendered.find("READ runtime.rs").unwrap();

        assert!(first_search_pos < second_search_pos, "{rendered}");
        assert!(second_search_pos < read_pos, "{rendered}");
        assert_eq!(rendered.matches("• Explored").count(), 1, "{rendered}");
        assert!(!rendered.contains("────────────────"), "{rendered}");
    }

    #[test]
    fn tui_keeps_activity_before_primary_text_when_message_events_are_missing() {
        let turn = CliTurnOutput {
            primary_text: "我已经把关键路径收敛到 runtime.rs。".into(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"tick|render|stream","path":"runtime.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::SessionMilestone {
                    kind: "tool_result_committed".into(),
                    text: "Tool result committed".into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Read".into(),
                    input: r#"{"file_path":"runtime.rs"}"#.into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        let activity_pos = rendered
            .find("SEARCH tick|render|stream in runtime.rs")
            .unwrap();
        let read_pos = rendered.find("READ runtime.rs").unwrap();
        let divider_pos = rendered.find("────────────────").unwrap();
        let text_pos = rendered
            .find("我已经把关键路径收敛到 runtime.rs。")
            .unwrap();

        assert!(activity_pos < read_pos, "{rendered}");
        assert!(read_pos < divider_pos, "{rendered}");
        assert!(divider_pos < text_pos, "{rendered}");
        assert_eq!(rendered.matches("• Explored").count(), 1, "{rendered}");
    }

    #[test]
    fn tui_renders_interrupted_terminal_events_but_hides_completed_terminal() {
        let interrupted = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolCallStarted {
                    tool_name: "Grep".into(),
                    input: r#"{"pattern":"stream","path":"runtime.rs"}"#.into(),
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Terminal {
                    kind: "aborted_streaming".into(),
                    text: "aborted_streaming".into(),
                }),
            ],
        };
        let completed = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Terminal {
                kind: "completed".into(),
                text: "completed".into(),
            })],
        };

        let raw_interrupted_rendered = render_turn_tui_output(&interrupted);
        assert!(
            raw_interrupted_rendered.contains(&format!(
                "\x1b[31m{CONVERSATION_INTERRUPTED_MESSAGE}\x1b[0m"
            )),
            "{raw_interrupted_rendered}"
        );

        let interrupted_rendered = strip_ansi(&raw_interrupted_rendered);
        let activity_pos = interrupted_rendered
            .find("SEARCH stream in runtime.rs")
            .unwrap();
        let divider_pos = interrupted_rendered.find("────────────────").unwrap();
        let message_pos = interrupted_rendered
            .find(CONVERSATION_INTERRUPTED_MESSAGE)
            .unwrap();
        assert!(activity_pos < divider_pos, "{interrupted_rendered}");
        assert!(divider_pos < message_pos, "{interrupted_rendered}");

        let completed_rendered = strip_ansi(&render_turn_tui_output(&completed));
        assert!(
            !completed_rendered.contains("completed"),
            "{completed_rendered}"
        );
        assert!(
            !completed_rendered.contains(CONVERSATION_INTERRUPTED_MESSAGE),
            "{completed_rendered}"
        );
    }

    #[test]
    fn tui_renders_edit_activity_as_colored_diff_preview() {
        let path = std::env::temp_dir().join("renderer_edit_activity_preview.rs");
        std::fs::write(
            &path,
            "fn before() {\n    println!(\"old\");\n}\nfn after() {}\n",
        )
        .expect("write temp preview file");
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Edit".into(),
                content: edit_detail(&path, "    println!(\"todo\");", "    println!(\"old\");"),
                summary: Some("Edit succeeded".into()),
                detail: Some(edit_detail(
                    &path,
                    "    println!(\"todo\");",
                    "    println!(\"old\");",
                )),
            })],
        };

        let rendered = render_turn_tui_output(&turn);
        let plain = strip_ansi(&rendered);
        assert!(!plain.contains("[Activity]"));
        assert!(plain.contains("EDITED"));
        assert!(plain.contains("(+1 -1)"));
        assert!(plain.contains("renderer_edit_activity_preview.rs"));
        assert!(plain.contains("+ 2     println!(\"old\");"));
        assert!(plain.contains("- 2     println!(\"todo\");"));
        assert!(plain.contains("----------------------------------------------------------------------------------------------------"));
        assert!(rendered.contains("\x1b[48;5;120m"));
        assert!(rendered.contains("\x1b[48;5;224m"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn tui_renders_full_edit_payload_beyond_preview_length() {
        let path = std::env::temp_dir().join("renderer_edit_activity_long_payload.rs");
        let old_line = "let value = \"old payload keeps visible content after the forty character preview limit\";";
        let new_line = "let value = \"new payload keeps visible content after the forty character preview limit\";";
        std::fs::write(&path, format!("fn demo() {{\n    {new_line}\n}}\n"))
            .expect("write temp preview file");
        let detail = edit_detail(&path, old_line, new_line);
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Edit".into(),
                content: detail.clone(),
                summary: Some("Edit succeeded".into()),
                detail: Some(detail),
            })],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("after the forty character preview limit"));
        assert!(rendered.contains("+1 -1"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn tui_renders_replace_all_as_separate_diff_hunks() {
        let path = std::env::temp_dir().join("renderer_edit_activity_replace_all.rs");
        std::fs::write(
            &path,
            [
                "line 1",
                "target_new",
                "line 3",
                "line 4",
                "line 5",
                "line 6",
                "line 7",
                "line 8",
                "line 9",
                "line 10",
                "line 11",
                "line 12",
                "line 13",
                "target_new",
                "line 15",
                "line 16",
            ]
            .join("\n"),
        )
        .expect("write temp preview file");
        let detail = edit_detail_with_options(&path, "target_old", "target_new", true, 2);
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Edit".into(),
                content: detail.clone(),
                summary: Some("Edit succeeded".into()),
                detail: Some(detail),
            })],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("(+2 -2)"));
        assert!(rendered.contains("- 2 target_old"));
        assert!(rendered.contains("+ 2 target_new"));
        assert!(rendered.contains("- 14 target_old"));
        assert!(rendered.contains("+ 14 target_new"));
        assert!(rendered.contains("..."));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn tui_edit_diff_falls_back_to_preview_fields_when_full_payload_is_missing() {
        let path = std::env::temp_dir().join("renderer_edit_activity_legacy_payload.rs");
        let detail = format!(
            "path={}\nreplacements=1\nreplace_all=false\nold_text=legacy old\nnew_text=legacy new\nold_text_b64=not-valid-base64\nnew_text_b64=not-valid-base64",
            path.display()
        );
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::ToolResult {
                tool_name: "Edit".into(),
                content: detail.clone(),
                summary: Some("Edit succeeded".into()),
                detail: Some(detail),
            })],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("(+1 -1)"));
        assert!(rendered.contains("- 1 legacy old"));
        assert!(rendered.contains("+ 1 legacy new"));
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
        assert!(!rendered.contains("[Notice:"));
        assert!(!rendered.contains("recorded usage"));
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
    fn tui_approval_pause_does_not_render_interrupted_message() {
        let turn = CliTurnOutput {
            primary_text: String::new(),
            events: vec![
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::PendingApproval {
                    tool_name: "Bash".into(),
                    message: "approval required".into(),
                    code: None,
                    summary: Some("Bash pending approval".into()),
                    detail: Some("Run: cargo build\nReason: requires approval".into()),
                    approval_kind: Some("tool_permission".into()),
                    escalation_reasons: vec!["shell_operator.pipe".into()],
                }),
                CliDisplayEvent::RuntimeEvent(CliRuntimeEvent::Terminal {
                    kind: "aborted_tools".into(),
                    text: "aborted_tools".into(),
                }),
            ],
        };

        let rendered = strip_ansi(&render_turn_tui_output(&turn));
        assert!(rendered.contains("[Approval required]"));
        assert!(!rendered.contains(CONVERSATION_INTERRUPTED_MESSAGE));
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

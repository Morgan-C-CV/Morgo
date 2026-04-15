use crate::interaction::cli::repl::{CliDisplayEvent, CliRuntimeEvent, CliTurnOutput};
use crate::task::types::{TaskEvent, TaskUsageSummary};

fn leak_task_type(task_type: &'static str) -> &'static str {
    task_type
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceView {
    pub primary_text: String,
    pub items: Vec<SurfaceItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurfaceItem {
    TaskUpdate(TaskView),
    ApprovalRequired {
        tool_name: String,
        message: String,
        summary: Option<String>,
        detail: Option<String>,
    },
    RuntimeNotice {
        kind: String,
        message: String,
    },
    ToolCallStarted {
        tool_name: String,
        input: String,
    },
    ToolResult {
        tool_name: String,
        content: String,
        summary: Option<String>,
        detail: Option<String>,
    },
    AssistantDelta {
        text: String,
    },
    Transition {
        kind: String,
        text: String,
    },
    Terminal {
        kind: String,
        text: String,
    },
    SessionMilestone {
        kind: String,
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskView {
    pub task_id: String,
    pub task_type: &'static str,
    pub status: &'static str,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<&'static str>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<&'static str>,
    pub validation_state: Option<&'static str>,
    pub output_file: String,
    pub usage: Option<TaskUsageSummary>,
}

pub fn build_surface_view(turn: &CliTurnOutput) -> SurfaceView {
    SurfaceView {
        primary_text: turn.primary_text.clone(),
        items: turn
            .events
            .iter()
            .map(surface_item_from_cli_event)
            .collect(),
    }
}

pub fn surface_item_from_cli_event(event: &CliDisplayEvent) -> SurfaceItem {
    match event {
        CliDisplayEvent::TaskEvent(task_event) => {
            SurfaceItem::TaskUpdate(TaskView::from(task_event))
        }
        CliDisplayEvent::RuntimeEvent(runtime_event) => {
            surface_item_from_runtime_event(runtime_event)
        }
    }
}

fn surface_item_from_runtime_event(event: &CliRuntimeEvent) -> SurfaceItem {
    match event {
        CliRuntimeEvent::AssistantDelta { text } => {
            SurfaceItem::AssistantDelta { text: text.clone() }
        }
        CliRuntimeEvent::ToolCallStarted { tool_name, input } => SurfaceItem::ToolCallStarted {
            tool_name: tool_name.clone(),
            input: input.clone(),
        },
        CliRuntimeEvent::ToolResult {
            tool_name,
            content,
            summary,
            detail,
        } => SurfaceItem::ToolResult {
            tool_name: tool_name.clone(),
            content: content.clone(),
            summary: summary.clone(),
            detail: detail.clone(),
        },
        CliRuntimeEvent::PendingApproval {
            tool_name,
            message,
            summary,
            detail,
        } => SurfaceItem::ApprovalRequired {
            tool_name: tool_name.clone(),
            message: message.clone(),
            summary: summary.clone(),
            detail: detail.clone(),
        },
        CliRuntimeEvent::Notice { kind, message } => SurfaceItem::RuntimeNotice {
            kind: kind.clone(),
            message: message.clone(),
        },
        CliRuntimeEvent::Transition { text } => SurfaceItem::Transition {
            kind: stable_transition_kind(text).to_string(),
            text: text.clone(),
        },
        CliRuntimeEvent::Terminal { text } => SurfaceItem::Terminal {
            kind: stable_terminal_kind(text).to_string(),
            text: text.clone(),
        },
        CliRuntimeEvent::SessionMilestone { text } => SurfaceItem::SessionMilestone {
            kind: stable_session_milestone_kind(text).to_string(),
            text: text.clone(),
        },
    }
}

impl SurfaceItem {
    pub fn to_legacy_line(&self) -> String {
        match self {
            Self::TaskUpdate(task) => {
                format!(
                    "[task:{}] {} {}",
                    task.task_type, task.task_id, task.summary
                )
            }
            Self::ApprovalRequired {
                tool_name, message, ..
            } => format!("[approval] {tool_name}: {message}"),
            Self::RuntimeNotice { kind, message } => format!("[notice:{kind}] {message}"),
            Self::ToolCallStarted { tool_name, input } => {
                format!("[tool-start] {tool_name}: {input}")
            }
            Self::ToolResult {
                tool_name, content, ..
            } => format!("[tool-result] {tool_name}: {content}"),
            Self::AssistantDelta { text } => format!("[delta] {text}"),
            Self::Transition { text, .. } => format!("[transition] {text}"),
            Self::Terminal { text, .. } => format!("[terminal] {text}"),
            Self::SessionMilestone { text, .. } => format!("[milestone] {text}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramView {
    pub primary_text: String,
    pub items: Vec<TelegramItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramItem {
    TaskUpdate(TelegramTaskItem),
    ApprovalRequired { tool_name: String, message: String },
    RuntimeNotice { kind: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramTaskItem {
    pub task_id: String,
    pub task_type: &'static str,
    pub status: &'static str,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<&'static str>,
    pub phase: Option<&'static str>,
    pub validation_state: Option<&'static str>,
    pub output_file: String,
    pub usage: Option<TaskUsageSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebView {
    pub primary_text: String,
    pub items: Vec<WebItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebItem {
    TaskUpdate(WebTaskItem),
    ApprovalRequired {
        tool_name: String,
        message: String,
        summary: Option<String>,
        detail: Option<String>,
    },
    RuntimeNotice {
        notice_kind: String,
        message: String,
    },
    ToolCallStarted {
        tool_name: String,
        input: String,
    },
    ToolResult {
        tool_name: String,
        content: String,
        summary: Option<String>,
        detail: Option<String>,
    },
    AssistantDelta {
        text: String,
    },
    Transition {
        transition_kind: String,
        text: String,
    },
    Terminal {
        terminal_kind: String,
        text: String,
    },
    SessionMilestone {
        milestone_kind: String,
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebTaskItem {
    pub task_id: String,
    pub task_type: &'static str,
    pub status: &'static str,
    pub summary: String,
    pub result: String,
    pub next_action: String,
    pub worker_role: Option<&'static str>,
    pub orchestration_group_id: Option<String>,
    pub phase: Option<&'static str>,
    pub validation_state: Option<&'static str>,
    pub output_file: String,
    pub usage: Option<TaskUsageSummary>,
}

pub fn build_telegram_view(view: &SurfaceView) -> TelegramView {
    TelegramView {
        primary_text: view.primary_text.clone(),
        items: view
            .items
            .iter()
            .filter_map(telegram_item_from_surface_item)
            .collect(),
    }
}

pub fn telegram_item_from_surface_item(item: &SurfaceItem) -> Option<TelegramItem> {
    match item {
        SurfaceItem::TaskUpdate(task) => Some(TelegramItem::TaskUpdate(TelegramTaskItem {
            task_id: task.task_id.clone(),
            task_type: task.task_type,
            status: task.status,
            summary: task.summary.clone(),
            result: task.result.clone(),
            next_action: task.next_action.clone(),
            worker_role: task.worker_role,
            phase: task.phase,
            validation_state: task.validation_state,
            output_file: task.output_file.clone(),
            usage: task.usage.clone(),
        })),
        SurfaceItem::ApprovalRequired {
            tool_name, message, ..
        } => Some(TelegramItem::ApprovalRequired {
            tool_name: tool_name.clone(),
            message: message.clone(),
        }),
        SurfaceItem::RuntimeNotice { kind, message } => telegram_runtime_notice_item(kind, message),
        SurfaceItem::ToolCallStarted { .. }
        | SurfaceItem::ToolResult { .. }
        | SurfaceItem::AssistantDelta { .. }
        | SurfaceItem::Transition { .. }
        | SurfaceItem::Terminal { .. }
        | SurfaceItem::SessionMilestone { .. } => None,
    }
}

fn telegram_runtime_notice_item(kind: &str, message: &str) -> Option<TelegramItem> {
    if kind == "runtime" {
        return None;
    }
    Some(TelegramItem::RuntimeNotice {
        kind: kind.to_string(),
        message: message.to_string(),
    })
}

pub fn build_web_view(view: &SurfaceView) -> WebView {
    WebView {
        primary_text: view.primary_text.clone(),
        items: view.items.iter().map(web_item_from_surface_item).collect(),
    }
}

pub fn web_item_from_surface_item(item: &SurfaceItem) -> WebItem {
    match item {
        SurfaceItem::TaskUpdate(task) => WebItem::TaskUpdate(WebTaskItem {
            task_id: task.task_id.clone(),
            task_type: task.task_type,
            status: task.status,
            summary: task.summary.clone(),
            result: task.result.clone(),
            next_action: task.next_action.clone(),
            worker_role: task.worker_role,
            orchestration_group_id: task.orchestration_group_id.clone(),
            phase: task.phase,
            validation_state: task.validation_state,
            output_file: task.output_file.clone(),
            usage: task.usage.clone(),
        }),
        SurfaceItem::ApprovalRequired {
            tool_name,
            message,
            summary,
            detail,
        } => WebItem::ApprovalRequired {
            tool_name: tool_name.clone(),
            message: message.clone(),
            summary: summary.clone(),
            detail: detail.clone(),
        },
        SurfaceItem::RuntimeNotice { kind, message } => WebItem::RuntimeNotice {
            notice_kind: kind.clone(),
            message: message.clone(),
        },
        SurfaceItem::ToolCallStarted { tool_name, input } => WebItem::ToolCallStarted {
            tool_name: tool_name.clone(),
            input: input.clone(),
        },
        SurfaceItem::ToolResult {
            tool_name,
            content,
            summary,
            detail,
        } => WebItem::ToolResult {
            tool_name: tool_name.clone(),
            content: content.clone(),
            summary: summary.clone(),
            detail: detail.clone(),
        },
        SurfaceItem::AssistantDelta { text } => WebItem::AssistantDelta { text: text.clone() },
        SurfaceItem::Transition { kind, text } => WebItem::Transition {
            transition_kind: kind.clone(),
            text: text.clone(),
        },
        SurfaceItem::Terminal { kind, text } => WebItem::Terminal {
            terminal_kind: kind.clone(),
            text: text.clone(),
        },
        SurfaceItem::SessionMilestone { kind, text } => WebItem::SessionMilestone {
            milestone_kind: kind.clone(),
            text: text.clone(),
        },
    }
}

impl From<&TaskEvent> for TaskView {
    fn from(value: &TaskEvent) -> Self {
        Self {
            task_id: value.task_id.clone(),
            task_type: leak_task_type(value.task_type.as_str()),
            status: value.status.as_str(),
            summary: value.summary.clone(),
            result: value.result.clone(),
            next_action: value.next_action.clone(),
            worker_role: value.worker_role.map(|role| role.as_str()),
            orchestration_group_id: value.orchestration_group_id.clone(),
            phase: value.phase.map(|phase| phase.as_str()),
            validation_state: value.validation_state.map(|state| state.as_str()),
            output_file: value.output_file.clone(),
            usage: value.usage.clone(),
        }
    }
}

pub fn stable_transition_kind(text: &str) -> &str {
    match text {
        "next_turn" => "next_turn",
        "tool_use_follow_up" => "tool_use_follow_up",
        "max_output_tokens_escalate" => "max_output_tokens_escalate",
        "max_output_tokens_recovery" => "max_output_tokens_recovery",
        "collapse_drain_retry" => "collapse_drain_retry",
        "reactive_compact_retry" => "reactive_compact_retry",
        "stop_hook_blocking" => "stop_hook_blocking",
        "token_budget_continuation" => "token_budget_continuation",
        "model_fallback_retry" => "model_fallback_retry",
        _ => "unknown_transition",
    }
}

pub fn stable_terminal_kind(text: &str) -> &str {
    match text {
        "completed" => "completed",
        "max_turns" => "max_turns",
        "max_budget" => "max_budget",
        "stop_hook_prevented" => "stop_hook_prevented",
        "aborted_streaming" => "aborted_streaming",
        "aborted_tools" => "aborted_tools",
        "model_error" => "model_error",
        _ => "unknown_terminal",
    }
}

pub fn stable_session_milestone_kind(text: &str) -> &str {
    match text {
        "user_input_committed" => "user_input_committed",
        "assistant_message_committed" => "assistant_message_committed",
        "tool_result_committed" => "tool_result_committed",
        "turn_completed" => "turn_completed",
        _ => "unknown_milestone",
    }
}

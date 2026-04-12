use crate::interaction::cli::repl::{CliDisplayEvent, CliTurnOutput};
use crate::task::types::TaskEvent;

pub fn render_output(output: &str) -> String {
    output.to_string()
}

pub fn render_turn_output(turn: &CliTurnOutput) -> String {
    let mut sections = Vec::new();
    if !turn.primary_text.is_empty() {
        sections.push(turn.primary_text.clone());
    }
    for event in &turn.events {
        sections.push(render_event(event));
    }
    sections.join("\n")
}

fn render_event(event: &CliDisplayEvent) -> String {
    match event {
        CliDisplayEvent::TaskEvent(task_event) => render_task_event(task_event),
        CliDisplayEvent::RuntimeEvent(text) => text.clone(),
    }
}

fn render_task_event(task_event: &TaskEvent) -> String {
    [
        format!("[task] id: {}", task_event.task_id),
        format!("[task] summary: {}", task_event.summary),
        format!("[task] status: {:?}", task_event.status),
        format!("[task] result: {}", task_event.result),
        format!(
            "[task] worker_role: {}",
            task_event.worker_role.map(|role| role.as_str()).unwrap_or("none")
        ),
        format!("[task] output: {}", task_event.output_file),
        format!("[task] next_action: {}", task_event.next_action),
    ]
    .join("\n")
}

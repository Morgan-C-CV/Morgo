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
    }
}

fn render_task_event(task_event: &TaskEvent) -> String {
    [
        format!("[task] {}", task_event.summary),
        format!("[task] status: {:?}", task_event.status),
        format!("[task] output: {}", task_event.output_file),
    ]
    .join("\n")
}

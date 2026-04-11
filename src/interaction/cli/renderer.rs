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
    let next_action = match task_event.status {
        crate::task::types::TaskStatus::Running => {
            format!(
                "use SendMessage with input '{}:<message>'",
                task_event.task_id
            )
        }
        _ => format!("use TaskOutput with input '{}:0'", task_event.task_id),
    };
    [
        format!("[task] id: {}", task_event.task_id),
        format!("[task] summary: {}", task_event.summary),
        format!("[task] status: {:?}", task_event.status),
        format!("[task] output: {}", task_event.output_file),
        format!("[task] next_action: {}", next_action),
    ]
    .join("\n")
}

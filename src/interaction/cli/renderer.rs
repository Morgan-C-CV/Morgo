pub fn render_output(output: &str) -> String {
    output
        .lines()
        .map(render_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_line(line: &str) -> String {
    if line.starts_with("<task-notification>")
        || line.starts_with("<task-id>")
        || line.starts_with("<status>")
        || line.starts_with("<summary>")
        || line.starts_with("<output-file>")
        || line.starts_with("</task-notification>")
    {
        format!("[task] {line}")
    } else {
        line.to_string()
    }
}

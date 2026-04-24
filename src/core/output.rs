#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputBlock {
    Text(String),
    Section {
        title: String,
        items: Vec<OutputBlock>,
    },
    KeyValue {
        key: String,
        value: String,
    },
    Table {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

impl OutputBlock {
    pub fn text(s: impl Into<String>) -> Self {
        OutputBlock::Text(s.into())
    }

    pub fn section(title: impl Into<String>, items: Vec<OutputBlock>) -> Self {
        OutputBlock::Section {
            title: title.into(),
            items,
        }
    }

    pub fn kv(key: impl Into<String>, value: impl Into<String>) -> Self {
        OutputBlock::KeyValue {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn table(headers: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        OutputBlock::Table { headers, rows }
    }

    pub fn to_plain_text(&self) -> String {
        match self {
            OutputBlock::Text(s) => s.clone(),
            OutputBlock::KeyValue { key, value } => format!("- {key}: {value}"),
            OutputBlock::Section { title, items } => {
                let mut out = vec![format!("{title}:")];
                for item in items {
                    for line in item.to_plain_text().lines() {
                        out.push(format!("  {line}"));
                    }
                }
                out.join("\n")
            }
            OutputBlock::Table { headers, rows } => {
                if headers.is_empty() && rows.is_empty() {
                    return String::new();
                }
                let col_count = headers
                    .len()
                    .max(rows.iter().map(|r| r.len()).max().unwrap_or(0));
                let mut widths = vec![0usize; col_count];
                for (i, h) in headers.iter().enumerate() {
                    widths[i] = widths[i].max(h.len());
                }
                for row in rows {
                    for (i, cell) in row.iter().enumerate() {
                        if i < col_count {
                            widths[i] = widths[i].max(cell.len());
                        }
                    }
                }
                let mut lines = Vec::new();
                if !headers.is_empty() {
                    let header_line = headers
                        .iter()
                        .enumerate()
                        .map(|(i, h)| format!("{:<width$}", h, width = widths[i]))
                        .collect::<Vec<_>>()
                        .join("  ");
                    let sep = widths
                        .iter()
                        .map(|w| "-".repeat(*w))
                        .collect::<Vec<_>>()
                        .join("  ");
                    lines.push(header_line);
                    lines.push(sep);
                }
                for row in rows {
                    let row_line = (0..col_count)
                        .map(|i| {
                            let cell = row.get(i).map(String::as_str).unwrap_or("");
                            format!("{:<width$}", cell, width = widths[i])
                        })
                        .collect::<Vec<_>>()
                        .join("  ");
                    lines.push(row_line);
                }
                lines.join("\n")
            }
        }
    }
}

pub fn blocks_to_plain_text(blocks: &[OutputBlock]) -> String {
    blocks
        .iter()
        .map(|b| b.to_plain_text())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

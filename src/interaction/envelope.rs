use crate::bootstrap::InteractionSurface;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedInput {
    pub surface: InteractionSurface,
    pub raw: String,
    pub command_name: Option<String>,
    pub command_args: String,
}

impl NormalizedInput {
    pub fn from_raw(surface: InteractionSurface, raw: impl Into<String>) -> Self {
        let raw = raw.into();
        if let Some(stripped) = raw.strip_prefix('/') {
            let mut parts = stripped.splitn(2, char::is_whitespace);
            let command_name = parts.next().map(str::to_string);
            let command_args = parts.next().unwrap_or_default().trim().to_string();
            Self {
                surface,
                raw,
                command_name,
                command_args,
            }
        } else {
            Self {
                surface,
                raw,
                command_name: None,
                command_args: String::new(),
            }
        }
    }
}

use crate::bootstrap::InteractionSurface;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorIdentity {
    pub actor_id: String,
    pub is_authenticated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputMetadata {
    pub from_trusted_surface: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedInput {
    pub session_id: String,
    pub surface: InteractionSurface,
    pub actor: ActorIdentity,
    pub raw: String,
    pub command_name: Option<String>,
    pub command_args: String,
    pub attachments: Vec<String>,
    pub metadata: InputMetadata,
}

impl NormalizedInput {
    pub fn from_raw(surface: InteractionSurface, raw: impl Into<String>) -> Self {
        let raw = raw.into();
        if let Some(stripped) = raw.strip_prefix('/') {
            let mut parts = stripped.splitn(2, char::is_whitespace);
            let command_name = parts.next().map(str::to_string);
            let command_args = parts.next().unwrap_or_default().trim().to_string();
            Self {
                session_id: "local-session".into(),
                surface,
                actor: ActorIdentity {
                    actor_id: "local-user".into(),
                    is_authenticated: true,
                },
                raw,
                command_name,
                command_args,
                attachments: Vec::new(),
                metadata: InputMetadata {
                    from_trusted_surface: true,
                },
            }
        } else {
            Self {
                session_id: "local-session".into(),
                surface,
                actor: ActorIdentity {
                    actor_id: "local-user".into(),
                    is_authenticated: true,
                },
                raw,
                command_name: None,
                command_args: String::new(),
                attachments: Vec::new(),
                metadata: InputMetadata {
                    from_trusted_surface: true,
                },
            }
        }
    }
}

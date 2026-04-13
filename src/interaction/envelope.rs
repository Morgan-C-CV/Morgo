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
        Self::from_session_raw(surface, "local-session", raw)
    }

    pub fn from_session_raw(
        surface: InteractionSurface,
        session_id: impl Into<String>,
        raw: impl Into<String>,
    ) -> Self {
        Self::from_actor_session_raw(surface, session_id, "local-user", true, true, raw)
    }

    pub fn from_remote_raw(
        session_id: impl Into<String>,
        actor_id: impl Into<String>,
        is_authenticated: bool,
        from_trusted_surface: bool,
        raw: impl Into<String>,
    ) -> Self {
        Self::from_actor_session_raw(
            InteractionSurface::Remote,
            session_id,
            actor_id,
            is_authenticated,
            from_trusted_surface,
            raw,
        )
    }

    fn from_actor_session_raw(
        surface: InteractionSurface,
        session_id: impl Into<String>,
        actor_id: impl Into<String>,
        is_authenticated: bool,
        from_trusted_surface: bool,
        raw: impl Into<String>,
    ) -> Self {
        let session_id = session_id.into();
        let actor_id = actor_id.into();
        let raw = raw.into();
        let actor = ActorIdentity {
            actor_id,
            is_authenticated,
        };
        let metadata = InputMetadata {
            from_trusted_surface,
        };
        if let Some(stripped) = raw.strip_prefix('/') {
            let mut parts = stripped.splitn(2, char::is_whitespace);
            let command_name = parts.next().map(str::to_string);
            let command_args = parts.next().unwrap_or_default().trim().to_string();
            Self {
                session_id,
                surface,
                actor,
                raw,
                command_name,
                command_args,
                attachments: Vec::new(),
                metadata,
            }
        } else {
            Self {
                session_id,
                surface,
                actor,
                raw,
                command_name: None,
                command_args: String::new(),
                attachments: Vec::new(),
                metadata,
            }
        }
    }
}

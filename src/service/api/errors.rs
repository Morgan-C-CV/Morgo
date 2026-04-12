use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiErrorKind {
    HttpStatus(u16),
    RequestBuild,
    Transport,
    Timeout,
    InvalidResponse,
    SseProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub kind: ApiErrorKind,
    pub message: String,
}

impl ApiError {
    pub fn http_status(status: u16, message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::HttpStatus(status),
            message: message.into(),
        }
    }

    pub fn request_build(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::RequestBuild,
            message: message.into(),
        }
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::Transport,
            message: message.into(),
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::Timeout,
            message: message.into(),
        }
    }

    pub fn invalid_response(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::InvalidResponse,
            message: message.into(),
        }
    }

    pub fn sse_protocol(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::SseProtocol,
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ApiErrorKind::Transport | ApiErrorKind::Timeout)
            || matches!(self.kind, ApiErrorKind::HttpStatus(status) if status == 429 || (500..=599).contains(&status))
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

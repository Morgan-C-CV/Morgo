use std::fmt;

use crate::service::api::streaming::{ProviderFailureDisposition, StreamError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiErrorKind {
    HttpStatus(u16),
    RequestBuild,
    Transport,
    ConnectionReset,
    Timeout,
    EmptyBody,
    BadContentType,
    InvalidResponse,
    SseProtocol,
    ToolUseProtocol,
    StructuredOutputInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub kind: ApiErrorKind,
    pub message: String,
    pub disposition: ProviderFailureDisposition,
    pub retry_after_ms: Option<u64>,
}

impl ApiError {
    pub fn http_status(status: u16, message: impl Into<String>) -> Self {
        let disposition = if status == 429 || (500..=599).contains(&status) {
            ProviderFailureDisposition::PreStreamRetryable
        } else {
            ProviderFailureDisposition::PreStreamTerminal
        };
        Self {
            kind: ApiErrorKind::HttpStatus(status),
            message: message.into(),
            disposition,
            retry_after_ms: None,
        }
    }

    pub fn request_build(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::RequestBuild,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamTerminal,
            retry_after_ms: None,
        }
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::Transport,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamRetryable,
            retry_after_ms: None,
        }
    }

    pub fn connection_reset(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::ConnectionReset,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamRetryable,
            retry_after_ms: None,
        }
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::Timeout,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamRetryable,
            retry_after_ms: None,
        }
    }

    pub fn empty_body(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::EmptyBody,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamTerminal,
            retry_after_ms: None,
        }
    }

    pub fn bad_content_type(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::BadContentType,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamTerminal,
            retry_after_ms: None,
        }
    }

    pub fn invalid_response(message: impl Into<String>) -> Self {
        Self {
            kind: ApiErrorKind::InvalidResponse,
            message: message.into(),
            disposition: ProviderFailureDisposition::PreStreamTerminal,
            retry_after_ms: None,
        }
    }

    pub fn sse_protocol(message: impl Into<String>) -> Self {
        Self::sse_protocol_with_disposition(
            message,
            ProviderFailureDisposition::PreStreamTerminal,
        )
    }

    pub fn sse_protocol_with_disposition(
        message: impl Into<String>,
        disposition: ProviderFailureDisposition,
    ) -> Self {
        Self {
            kind: ApiErrorKind::SseProtocol,
            message: message.into(),
            disposition,
            retry_after_ms: None,
        }
    }

    pub fn tool_use_protocol(message: impl Into<String>) -> Self {
        Self::tool_use_protocol_with_disposition(
            message,
            ProviderFailureDisposition::PreStreamTerminal,
        )
    }

    pub fn tool_use_protocol_with_disposition(
        message: impl Into<String>,
        disposition: ProviderFailureDisposition,
    ) -> Self {
        Self {
            kind: ApiErrorKind::ToolUseProtocol,
            message: message.into(),
            disposition,
            retry_after_ms: None,
        }
    }

    pub fn structured_output_invalid(message: impl Into<String>) -> Self {
        Self::structured_output_invalid_with_disposition(
            message,
            ProviderFailureDisposition::PreStreamTerminal,
        )
    }

    pub fn structured_output_invalid_with_disposition(
        message: impl Into<String>,
        disposition: ProviderFailureDisposition,
    ) -> Self {
        Self {
            kind: ApiErrorKind::StructuredOutputInvalid,
            message: message.into(),
            disposition,
            retry_after_ms: None,
        }
    }

    pub fn with_disposition(mut self, disposition: ProviderFailureDisposition) -> Self {
        self.disposition = disposition;
        self
    }

    pub fn with_retry_after_ms(mut self, retry_after_ms: Option<u64>) -> Self {
        self.retry_after_ms = retry_after_ms;
        self
    }

    pub fn is_retryable(&self) -> bool {
        self.disposition.is_pre_stream_retryable()
    }

    pub fn kind_label(&self) -> &'static str {
        match self.kind {
            ApiErrorKind::HttpStatus(_) => "http_status",
            ApiErrorKind::RequestBuild => "request_build",
            ApiErrorKind::Transport => "transport",
            ApiErrorKind::ConnectionReset => "connection_reset",
            ApiErrorKind::Timeout => "timeout",
            ApiErrorKind::EmptyBody => "empty_body",
            ApiErrorKind::BadContentType => "bad_content_type",
            ApiErrorKind::InvalidResponse => "invalid_response",
            ApiErrorKind::SseProtocol => "sse_protocol",
            ApiErrorKind::ToolUseProtocol => "tool_use_protocol",
            ApiErrorKind::StructuredOutputInvalid => "structured_output_invalid",
        }
    }

    pub fn to_stream_error(&self, provider_id: &str) -> StreamError {
        StreamError {
            provider_id: provider_id.to_string(),
            kind: self.kind_label().to_string(),
            message: self.message.clone(),
            retryable: self.is_retryable(),
            disposition: match self.disposition {
                ProviderFailureDisposition::PreStreamRetryable => {
                    ProviderFailureDisposition::PreStreamRetryable
                }
                ProviderFailureDisposition::PreStreamTerminal => {
                    ProviderFailureDisposition::PreStreamTerminal
                }
                ProviderFailureDisposition::StreamInterrupted => {
                    ProviderFailureDisposition::StreamInterrupted
                }
                ProviderFailureDisposition::StreamTerminal => {
                    ProviderFailureDisposition::StreamTerminal
                }
            },
            status_code: match self.kind {
                ApiErrorKind::HttpStatus(status) => Some(status),
                _ => None,
            },
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

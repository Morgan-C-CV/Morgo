use crate::core::engine::QueryEngine;
use crate::interaction::remote::{
    RemoteRequest, RemoteResponse, RemoteResponseMeta, RemoteResponseOutcome,
    handle_remote_request,
};
use crate::interaction::router::CommandRouter;
use crate::state::app_state::AppState;

/// Minimal HTTP-oriented request envelope for the web transport adapter.
///
/// Translates to a `RemoteRequest` before dispatch — no new surface variant needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebRequest {
    pub session_id: String,
    pub actor_id: String,
    pub is_authenticated: bool,
    pub raw: String,
    pub correlation_id: Option<String>,
    /// Optional HTTP method hint (GET/POST/…) — carried through for future routing, not used by v1.
    pub http_method: Option<String>,
    /// Optional request path — carried through for future routing, not used by v1.
    pub path: Option<String>,
}

impl WebRequest {
    pub fn new(
        session_id: impl Into<String>,
        actor_id: impl Into<String>,
        is_authenticated: bool,
        raw: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            actor_id: actor_id.into(),
            is_authenticated,
            raw: raw.into(),
            correlation_id: None,
            http_method: None,
            path: None,
        }
    }

    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub fn with_http_method(mut self, method: impl Into<String>) -> Self {
        self.http_method = Some(method.into());
        self
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

/// HTTP status code semantics derived from `RemoteResponseOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebStatusCode {
    Ok200,
    Forbidden403,
    InternalServerError500,
}

impl WebStatusCode {
    pub fn as_u16(self) -> u16 {
        match self {
            Self::Ok200 => 200,
            Self::Forbidden403 => 403,
            Self::InternalServerError500 => 500,
        }
    }
}

impl From<RemoteResponseOutcome> for WebStatusCode {
    fn from(outcome: RemoteResponseOutcome) -> Self {
        match outcome {
            RemoteResponseOutcome::Ok => Self::Ok200,
            RemoteResponseOutcome::Denied => Self::Forbidden403,
            RemoteResponseOutcome::RuntimeError => Self::InternalServerError500,
        }
    }
}

/// Web transport response — wraps `RemoteResponse` with HTTP-oriented fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebResponse {
    pub status: WebStatusCode,
    pub body: String,
    pub meta: RemoteResponseMeta,
}

impl WebResponse {
    pub fn status_code(&self) -> u16 {
        self.status.as_u16()
    }

    pub fn is_ok(&self) -> bool {
        self.status == WebStatusCode::Ok200
    }
}

impl From<RemoteResponse> for WebResponse {
    fn from(remote: RemoteResponse) -> Self {
        let status = WebStatusCode::from(remote.meta.outcome);
        Self {
            status,
            body: remote.primary_text,
            meta: remote.meta,
        }
    }
}

/// Web transport entry point — translates `WebRequest` to `RemoteRequest`, dispatches through
/// the existing remote runtime, and wraps the result as `WebResponse`.
pub async fn handle_web_request(
    router: &CommandRouter,
    engine: &QueryEngine,
    app_state: &AppState,
    request: WebRequest,
) -> anyhow::Result<WebResponse> {
    let remote_request = RemoteRequest {
        session_id: request.session_id,
        actor_id: request.actor_id,
        is_authenticated: request.is_authenticated,
        from_trusted_surface: true,
        raw: request.raw,
        correlation_id: request.correlation_id,
    };
    let remote_response = handle_remote_request(router, engine, app_state, remote_request).await?;
    Ok(WebResponse::from(remote_response))
}

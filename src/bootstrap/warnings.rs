use std::path::Path;

use crate::service::api::client::redact_proxy_url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupWarning {
    /// RUST_AGENT_PROVIDER_BASE_URL is unset; using http://localhost which will fail in production.
    ProviderBaseUrlIsLocalhost,
    /// One or more MCP server configs failed to parse.
    McpConfigParseFailure { count: usize, messages: Vec<String> },
    /// RUST_AGENT_CONFIG_ROOT is unset; using the default managed config root.
    ConfigRootIsDefault { path: String },
    /// No filesystem policy found; running with no-policy (all paths allowed).
    FilesystemPolicyMissing,
    /// Provider pricing is default (all zeros); cost tracking will show $0.00.
    ProviderPricingIsDefault { provider_id: String },
    /// RUST_AGENT_PROXY_URL is set but not a valid proxy URL; proxy will be ignored.
    InvalidProxyUrl { redacted_url: String },
}

impl StartupWarning {
    pub fn message(&self) -> String {
        match self {
            StartupWarning::ProviderBaseUrlIsLocalhost => {
                "RUST_AGENT_PROVIDER_BASE_URL is unset; using http://localhost — \
                 set this env var to point at a real provider endpoint"
                    .into()
            }
            StartupWarning::McpConfigParseFailure { count, messages } => {
                format!(
                    "{count} MCP server config(s) failed to parse and were skipped: {}",
                    messages.join("; ")
                )
            }
            StartupWarning::ConfigRootIsDefault { path } => {
                format!(
                    "RUST_AGENT_CONFIG_ROOT is unset; using default config root: {path} — \
                     set RUST_AGENT_CONFIG_ROOT to override"
                )
            }
            StartupWarning::FilesystemPolicyMissing => {
                "No filesystem-policy.json found; running with no filesystem policy \
                 (all paths are allowed)"
                    .into()
            }
            StartupWarning::ProviderPricingIsDefault { provider_id } => {
                format!(
                    "Provider '{provider_id}' has no pricing configured; \
                     cost tracking will show $0.00"
                )
            }
            StartupWarning::InvalidProxyUrl { redacted_url } => {
                format!(
                    "RUST_AGENT_PROXY_URL '{redacted_url}' is not a valid proxy URL — \
                     proxy will be ignored"
                )
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StartupWarnings {
    pub warnings: Vec<StartupWarning>,
}

impl StartupWarnings {
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty()
    }

    pub fn push(&mut self, warning: StartupWarning) {
        self.warnings.push(warning);
    }

    /// Emit all warnings via `tracing::warn!`. No-op if empty.
    pub fn emit_tracing(&self) {
        for warning in &self.warnings {
            tracing::warn!("[startup] {}", warning.message());
        }
    }

    pub fn has(&self, predicate: impl Fn(&StartupWarning) -> bool) -> bool {
        self.warnings.iter().any(predicate)
    }
}

/// Collect startup warnings from the results of all config loaders.
pub fn collect_startup_warnings(
    base_url: &str,
    mcp_config_diagnostics: &[String],
    config_root: &Path,
    filesystem_policy_missing: bool,
    provider_id: &str,
    pricing_is_default: bool,
) -> StartupWarnings {
    let mut warnings = StartupWarnings::default();

    if base_url.trim_end_matches('/') == "http://localhost"
        || base_url.trim_end_matches('/') == "https://localhost"
    {
        warnings.push(StartupWarning::ProviderBaseUrlIsLocalhost);
    }

    if !mcp_config_diagnostics.is_empty() {
        warnings.push(StartupWarning::McpConfigParseFailure {
            count: mcp_config_diagnostics.len(),
            messages: mcp_config_diagnostics.to_vec(),
        });
    }

    if std::env::var("RUST_AGENT_CONFIG_ROOT").is_err() {
        warnings.push(StartupWarning::ConfigRootIsDefault {
            path: config_root.display().to_string(),
        });
    }

    if filesystem_policy_missing {
        warnings.push(StartupWarning::FilesystemPolicyMissing);
    }

    if pricing_is_default {
        warnings.push(StartupWarning::ProviderPricingIsDefault {
            provider_id: provider_id.to_string(),
        });
    }

    if let Ok(proxy_url) = std::env::var("RUST_AGENT_PROXY_URL") {
        if !proxy_url.trim().is_empty() && !is_valid_proxy_url(&proxy_url) {
            warnings.push(StartupWarning::InvalidProxyUrl {
                redacted_url: redact_proxy_url(&proxy_url),
            });
        }
    }

    warnings
}

fn is_valid_proxy_url(url: &str) -> bool {
    match reqwest::Url::parse(url) {
        Ok(parsed) => matches!(parsed.scheme(), "http" | "https" | "socks5"),
        Err(_) => false,
    }
}

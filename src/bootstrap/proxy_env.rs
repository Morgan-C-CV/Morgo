#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxySource {
    RustAgentEnv,
    SystemEnv,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyEnvResolution {
    pub proxy_url: Option<String>,
    pub no_proxy: Option<String>,
    pub source: ProxySource,
}

/// Resolve proxy env according to the T19.3 system proxy contract.
///
/// Priority:
/// 1. `RUST_AGENT_PROXY_URL` / `RUST_AGENT_NO_PROXY`
/// 2. `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY`
/// 3. none
pub fn resolve_proxy_env_contract() -> ProxyEnvResolution {
    let rust_agent_proxy = std::env::var("RUST_AGENT_PROXY_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    if let Some(proxy_url) = rust_agent_proxy {
        let no_proxy = std::env::var("RUST_AGENT_NO_PROXY")
            .ok()
            .filter(|v| !v.trim().is_empty());
        return ProxyEnvResolution {
            proxy_url: Some(proxy_url),
            no_proxy,
            source: ProxySource::RustAgentEnv,
        };
    }

    let system_proxy = std::env::var("HTTPS_PROXY")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var("HTTP_PROXY")
                .ok()
                .filter(|v| !v.trim().is_empty())
        });
    if let Some(proxy_url) = system_proxy {
        let no_proxy = std::env::var("NO_PROXY")
            .ok()
            .filter(|v| !v.trim().is_empty());
        return ProxyEnvResolution {
            proxy_url: Some(proxy_url),
            no_proxy,
            source: ProxySource::SystemEnv,
        };
    }

    ProxyEnvResolution {
        proxy_url: None,
        no_proxy: None,
        source: ProxySource::None,
    }
}

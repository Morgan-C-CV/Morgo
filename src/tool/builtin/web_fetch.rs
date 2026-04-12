use async_trait::async_trait;
use reqwest::Url;
use serde::Deserialize;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct WebFetchTool;

#[derive(Debug, Deserialize)]
struct WebFetchInput {
    url: String,
}

fn parse_url_input(call: &ToolCall) -> anyhow::Result<String> {
    if let Some(json) = call.json_input() {
        let input: WebFetchInput = serde_json::from_value(json)
            .map_err(|error| anyhow::anyhow!("invalid web fetch input: {error}"))?;
        return Ok(input.url);
    }
    Ok(call.input.trim().to_string())
}

pub async fn fetch_text_with<F, Fut>(raw_url: &str, fetcher: F) -> anyhow::Result<String>
where
    F: FnOnce(Url) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<(u16, String)>>,
{
    let url = parse_url(raw_url)?;
    let (status, body) = fetcher(url.clone()).await?;
    if !(200..300).contains(&status) {
        anyhow::bail!("fetch failed for {url}: HTTP {status}")
    }
    Ok(body)
}

async fn fetch_text(raw_url: &str) -> anyhow::Result<String> {
    fetch_text_with(raw_url, |url| async move {
        let response = reqwest::get(url.clone())
            .await
            .map_err(|error| anyhow::anyhow!("failed to fetch {url}: {error}"))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|error| anyhow::anyhow!("failed to read {url}: {error}"))?;
        Ok((status, body))
    })
    .await
}

#[async_trait]
impl Tool for WebFetchTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "WebFetch".into(),
            description: "Fetch remote web content".into(),
            aliases: &[],
            search_hint: Some("fetch url"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: false,
            should_defer: true,
            requires_auth: true,
            requires_user_interaction: false,
            is_open_world: true,
            is_search_or_read_command: true,
        }
    }

    fn input_schema(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": {"type": "string"}
            }
        }))
    }

    async fn validate_input(&self, call: &ToolCall) -> anyhow::Result<()> {
        let url = parse_url(&parse_url_input(call)?)?;
        match url.scheme() {
            "http" | "https" => Ok(()),
            scheme => anyhow::bail!("unsupported URL scheme: {scheme}"),
        }
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let body = fetch_text(&parse_url_input(call)?).await?;
        Ok(ToolResult::Text(body))
    }
}

fn parse_url(raw: &str) -> anyhow::Result<Url> {
    Url::parse(raw.trim()).map_err(|error| anyhow::anyhow!("invalid URL: {error}"))
}

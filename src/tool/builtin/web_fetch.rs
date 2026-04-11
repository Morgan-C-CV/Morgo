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

#[async_trait]
impl Tool for WebFetchTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "WebFetch",
            description: "Fetch remote web content",
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
        let url = parse_url(&parse_url_input(call)?)?;
        let response = reqwest::get(url.clone())
            .await
            .map_err(|error| anyhow::anyhow!("failed to fetch {url}: {error}"))?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("fetch failed for {url}: HTTP {status}")
        }

        let body = response
            .text()
            .await
            .map_err(|error| anyhow::anyhow!("failed to read {url}: {error}"))?;
        Ok(ToolResult::Text(body))
    }
}

fn parse_url(raw: &str) -> anyhow::Result<Url> {
    Url::parse(raw.trim()).map_err(|error| anyhow::anyhow!("invalid URL: {error}"))
}

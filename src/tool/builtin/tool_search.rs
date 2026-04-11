use async_trait::async_trait;

use crate::state::permission_context::ToolPermissionContext;
use crate::tool::builtin::{
    agent::AgentTool, file_edit::FileEditTool, file_read::FileReadTool, glob::GlobTool,
    grep::GrepTool, web_fetch::WebFetchTool,
};
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};

pub struct ToolSearchTool;

#[async_trait]
impl Tool for ToolSearchTool {
    fn metadata(&self) -> ToolMetadata {
        ToolMetadata {
            name: "ToolSearch",
            description: "Search the available tool catalog",
            aliases: &[],
            search_hint: Some("search tool catalog"),
            read_only: true,
            destructive: false,
            concurrency_safe: true,
            always_load: true,
            should_defer: false,
            requires_auth: false,
            requires_user_interaction: false,
            is_open_world: false,
            is_search_or_read_command: true,
        }
    }

    async fn validate_input(&self, _call: &ToolCall) -> anyhow::Result<()> {
        Ok(())
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let query = call.input.trim().to_ascii_lowercase();
        let catalog = _permissions
            .inherited_tool_registry
            .as_ref()
            .map(|registry| registry.all_metadata())
            .unwrap_or_else(|| {
                vec![
                    AgentTool.metadata(),
                    FileEditTool.metadata(),
                    FileReadTool.metadata(),
                    GlobTool.metadata(),
                    GrepTool.metadata(),
                    self.metadata(),
                    WebFetchTool.metadata(),
                ]
            });

        let mut matches = catalog
            .into_iter()
            .filter(|tool| {
                query.is_empty()
                    || tool.name.to_ascii_lowercase().contains(&query)
                    || tool.description.to_ascii_lowercase().contains(&query)
                    || tool
                        .search_hint
                        .map(|hint| hint.to_ascii_lowercase().contains(&query))
                        .unwrap_or(false)
                    || tool
                        .aliases
                        .iter()
                        .any(|alias| alias.to_ascii_lowercase().contains(&query))
            })
            .map(|tool| format!("{} - {}", tool.name, tool.description))
            .collect::<Vec<_>>();
        matches.sort();

        Ok(ToolResult::Text(matches.join("\n")))
    }
}

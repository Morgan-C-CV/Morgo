use std::sync::Arc;

use async_trait::async_trait;

use crate::hook::registry::HookRegistry;
use crate::plugins::types::{
    PluginDiagnostic, PluginDiagnosticSeverity, PluginLoadResult, PluginToolDefinition,
};
use crate::state::permission_context::ToolPermissionContext;
use crate::tool::definition::{Tool, ToolCall, ToolMetadata, ToolResult};
use crate::tool::registry::ToolRegistry;

pub fn augment_hook_registry_with_plugins(
    mut registry: HookRegistry,
    plugin_load_result: &PluginLoadResult,
) -> HookRegistry {
    for hook in plugin_load_result
        .plugins
        .iter()
        .flat_map(|plugin| plugin.active_hooks().into_iter())
    {
        registry = registry.register_rule(hook.to_rule());
    }
    registry
}

pub fn augment_tool_registry_with_plugins(
    mut registry: ToolRegistry,
    plugin_load_result: &PluginLoadResult,
) -> (ToolRegistry, Vec<PluginDiagnostic>) {
    let mut diagnostics = Vec::new();

    for tool in plugin_load_result
        .plugins
        .iter()
        .flat_map(|plugin| plugin.active_tools().into_iter())
    {
        match PluginPromptTool::new(tool.clone()) {
            Ok(tool) => {
                registry = registry.register(Arc::new(tool));
            }
            Err(message) => diagnostics.push(PluginDiagnostic {
                plugin_name: Some(tool.plugin_name.clone()),
                manifest_path: Some(tool.manifest_path.clone()),
                severity: PluginDiagnosticSeverity::Error,
                code: "plugin-tool-registration-failed".into(),
                message: message.to_string(),
            }),
        }
    }

    (registry, diagnostics)
}

struct PluginPromptTool {
    metadata: ToolMetadata,
    plugin_name: String,
    prompt: String,
    manifest_path: String,
}

impl PluginPromptTool {
    fn new(definition: PluginToolDefinition) -> anyhow::Result<Self> {
        let display_name = definition.name.clone();
        let qualified_name = definition.qualified_tool_name();
        let description: &'static str = Box::leak(definition.description.clone().into_boxed_str());
        let aliases: &'static [&'static str] = Box::leak(
            definition
                .aliases
                .iter()
                .cloned()
                .map(|alias| Box::leak(alias.into_boxed_str()) as &'static str)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let search_hint = definition
            .search_hint
            .clone()
            .map(|hint| Box::leak(hint.into_boxed_str()) as &'static str);
        Ok(Self {
            metadata: ToolMetadata {
                name: Box::leak(qualified_name.into_boxed_str()),
                description,
                aliases,
                search_hint,
                read_only: definition.read_only,
                destructive: definition.destructive,
                concurrency_safe: true,
                always_load: true,
                should_defer: false,
                requires_auth: definition.requires_auth,
                requires_user_interaction: definition.requires_user_interaction,
                is_open_world: false,
                is_search_or_read_command: definition.read_only,
            },
            plugin_name: format!("{} ({display_name})", definition.plugin_name),
            prompt: definition.prompt,
            manifest_path: definition.manifest_path.display().to_string(),
        })
    }
}

#[async_trait]
impl Tool for PluginPromptTool {
    fn metadata(&self) -> ToolMetadata {
        self.metadata.clone()
    }

    async fn invoke(
        &self,
        call: &ToolCall,
        _permissions: &ToolPermissionContext,
    ) -> anyhow::Result<ToolResult> {
        let args = call.raw_input().trim();
        let args_line = if args.is_empty() {
            "Arguments: (none)".to_string()
        } else {
            format!("Arguments: {args}")
        };
        Ok(ToolResult::Text(format!(
            "Loaded plugin tool: {}\nPlugin: {}\n{}\nManifest: {}\n\nPlugin tool instructions:\n{}",
            self.metadata.name, self.plugin_name, args_line, self.manifest_path, self.prompt
        )))
    }
}
